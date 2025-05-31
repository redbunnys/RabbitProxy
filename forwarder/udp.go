package forwarder

import (
	"fmt"
	"net"
	"sync"
	"time"

	"rabbitproxy/config" // For ParsedPort
	"rabbitproxy/logging"
)

const udpSessionTimeout = 2 * time.Minute // Default timeout for UDP sessions

type clientSession struct {
	clientAddr *net.UDPAddr
	targetConn *net.UDPConn
	lastActivity time.Time
}

// UDPForwarder handles UDP forwarding for a single listen port to a single target port.
type UDPForwarder struct {
	ListenHost string
	ListenPort int
	TargetHost string
	TargetPort int
	RuleDesc   string

	MaxConnections int // Max active sessions (0 for unlimited)

	listenerConn *net.UDPConn
	sessions     map[string]*clientSession
	sessionsLock sync.Mutex
	stopChan     chan struct{}
	timeout      time.Duration // Could be made configurable per rule
}

// NewUDPForwarder creates a new UDPForwarder instance.
func NewUDPForwarder(listenP config.ParsedPort, targetP config.ParsedPort, description string, maxConns int) (*UDPForwarder, error) {
	if listenP.Port <= 0 || listenP.Port > 65535 {
		return nil, fmt.Errorf("invalid listen port %d for UDP forwarder (rule: %s)", listenP.Port, description)
	}
	if targetP.Port <= 0 || targetP.Port > 65535 {
		return nil, fmt.Errorf("invalid target port %d for UDP forwarder (rule: %s)", targetP.Port, description)
	}
	if targetP.Host == "" {
		return nil, fmt.Errorf("target host cannot be empty for UDP forwarder (rule: %s)", description)
	}
	if maxConns < 0 { // Should be normalized by config loader
		logging.Logger.Warnf("NewUDPForwarder (rule '%s'): received negative maxConns (%d), normalizing to 0 (unlimited).", description, maxConns)
		maxConns = 0
	}

	return &UDPForwarder{
		ListenHost:     listenP.Host,
		ListenPort:     listenP.Port,
		TargetHost:     targetP.Host,
		TargetPort:     targetP.Port,
		RuleDesc:       description,
		MaxConnections: maxConns,
		sessions:       make(map[string]*clientSession),
		stopChan:       make(chan struct{}),
		timeout:        udpSessionTimeout, // Default for now
	}, nil
}

// Start begins listening for UDP packets.
func (f *UDPForwarder) Start() error {
	listenAddrStr := fmt.Sprintf("%s:%d", f.ListenHost, f.ListenPort)
	listenUDPAddr, err := net.ResolveUDPAddr("udp", listenAddrStr)
	if err != nil {
		return fmt.Errorf("UDP: [%s] Failed to resolve listen address %s: %w", f.RuleDesc, listenAddrStr, err)
	}

	conn, err := net.ListenUDP("udp", listenUDPAddr)
	if err != nil {
		return fmt.Errorf("UDP: [%s] Failed to listen on %s: %w", f.RuleDesc, listenAddrStr, err)
	}
	f.listenerConn = conn

	maxSessStr := "unlimited"
	if f.MaxConnections > 0 {
		maxSessStr = fmt.Sprintf("%d", f.MaxConnections)
	}
	logging.Logger.Infof("UDP Forwarder: [%s] Listening on %s, forwarding to %s:%d. Max Sessions: %s. Session Timeout: %s",
		f.RuleDesc, listenAddrStr, f.TargetHost, f.TargetPort, maxSessStr, f.timeout.String())

	go f.cleanupLoop() // Start session cleanup loop
	go func() {
		buffer := make([]byte, 4096) // Adjust buffer size as needed
		for {
			select {
			case <-f.stopChan:
				return
			default:
				// Set a read deadline to allow checking stopChan periodically
				f.listenerConn.SetReadDeadline(time.Now().Add(1 * time.Second))
				n, clientAddr, err := f.listenerConn.ReadFromUDP(buffer)
				if err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						continue // Deadline reached, loop to check stopChan
					}
					// Check if error is due to closed connection
					if err.Error() == "use of closed network connection" || err.Error() == "read udp: use of closed network connection" {
						logging.Logger.Infof("UDP Listener for rule '%s' on %s closed.", f.RuleDesc, f.listenerConn.LocalAddr())
						return
					}
					logging.Logger.Errorf("UDP: [%s] Error reading from listener: %v", f.RuleDesc, err)
					continue // Consider if this type of error should stop the listener
				}
				// Ensure data is copied before passing buffer to another goroutine or reusing
				data := make([]byte, n)
				copy(data, buffer[:n])
				go f.handleClientPacket(clientAddr, data)
			}
		}
	}()
	return nil
}

// handleClientPacket processes an incoming packet from a client.
func (f *UDPForwarder) handleClientPacket(clientAddr *net.UDPAddr, data []byte) {
	clientKey := clientAddr.String()
	f.sessionsLock.Lock()

	session, found := f.sessions[clientKey]
	if !found {
		if f.MaxConnections > 0 && len(f.sessions) >= f.MaxConnections {
			f.sessionsLock.Unlock()
			logging.Logger.Warnf("UDP: [%s] Max active sessions (%d) reached. Dropping packet from %s (%d bytes). Current sessions: %d",
				f.RuleDesc, f.MaxConnections, clientAddr, len(data), len(f.sessions)) // len(f.sessions) might be slightly off due to no lock, but ok for log.
			return
		}

		targetUDPAddrStr := fmt.Sprintf("%s:%d", f.TargetHost, f.TargetPort)
		targetUDPAddr, err := net.ResolveUDPAddr("udp", targetUDPAddrStr)
		if err != nil {
			f.sessionsLock.Unlock()
			logging.Logger.Errorf("UDP: [%s] Failed to resolve target %s: %v", f.RuleDesc, targetUDPAddrStr, err)
			return
		}

		targetConn, err := net.DialUDP("udp", nil, targetUDPAddr) // Can also use f.listenerConn.WriteToUDP if no specific source port needed for target comms
		if err != nil {
			f.sessionsLock.Unlock()
			logging.Logger.Errorf("UDP: [%s] Failed to dial target %s: %v", f.RuleDesc, targetUDPAddrStr, err)
			return
		}

		session = &clientSession{
			clientAddr:   clientAddr,
			targetConn:   targetConn,
			lastActivity: time.Now(),
		}
		f.sessions[clientKey] = session
		logging.Logger.Infof("UDP: [%s] New session for %s, forwarding to %s. Active sessions: %d",
			f.RuleDesc, clientAddr, targetConn.RemoteAddr(), len(f.sessions))

		// Start a goroutine to listen for responses from the target for this session
		go f.listenFromTarget(session)
	}
	session.lastActivity = time.Now()
	f.sessionsLock.Unlock() // Unlock before writing to target or reading from client

	_, err := session.targetConn.Write(data)
	if err != nil {
		logging.Logger.Warnf("UDP: [%s] Failed to write to target %s for client %s: %v",
			f.RuleDesc, session.targetConn.RemoteAddr(), clientAddr, err)
		// Optionally, remove session if write fails consistently
	}
}

// listenFromTarget reads packets from the target and forwards them to the client.
func (f *UDPForwarder) listenFromTarget(s *clientSession) {
	buffer := make([]byte, 4096) // Adjust buffer size
	defer func() {
		// This defer does not remove the session from the map. That's handled by cleanupLoop.
		// However, closing the targetConn here is important.
		s.targetConn.Close()
		// Logging for when this specific goroutine ends.
		// logging.Logger.Debugf("UDP: [%s] Goroutine listening from target %s for client %s ended.", f.RuleDesc, s.targetConn.RemoteAddr(), s.clientAddr)
	}()

	for {
		select {
		case <-f.stopChan: // Forwarder is stopping
			return
		default:
			// Set a read deadline to allow checking stopChan periodically and session timeout
			s.targetConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, _, err := s.targetConn.ReadFromUDP(buffer) // We don't care about target's remote addr here
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// Check for session inactivity inside the timeout block
					f.sessionsLock.Lock()
					if time.Since(s.lastActivity) > f.timeout {
						// Session has timed out, clean it up
						// Note: actual removal from map is by cleanupLoop, but we can stop this goroutine.
						logging.Logger.Infof("UDP: [%s] Session for client %s timed out. Closing target connection.", f.RuleDesc, s.clientAddr)
						f.sessionsLock.Unlock()
						return // Exit goroutine, session will be removed by cleanupLoop
					}
					f.sessionsLock.Unlock()
					continue // Deadline reached, loop to check stopChan/activity
				}
				// Check if error is due to closed connection (e.g. by Stop() or cleanup)
				if err.Error() == "use of closed network connection" ||  err.Error() == "read udp: use of closed network connection"{
					// logging.Logger.Debugf("UDP: [%s] Target connection for client %s closed (likely by cleanup or stop).", f.RuleDesc, s.clientAddr)
					return
				}
				logging.Logger.Warnf("UDP: [%s] Error reading from target %s for client %s: %v", f.RuleDesc, s.targetConn.RemoteAddr(), s.clientAddr, err)
				return // Exit goroutine on other errors
			}

			// Send data back to the original client via the main listener connection
			_, err = f.listenerConn.WriteToUDP(buffer[:n], s.clientAddr)
			if err != nil {
				logging.Logger.Warnf("UDP: [%s] Failed to write to client %s from target %s: %v", f.RuleDesc, s.clientAddr, s.targetConn.RemoteAddr(), err)
				// If we can't write to client, the session might be effectively dead.
			} else {
				// Update last activity on successful write to client
				f.sessionsLock.Lock()
				s.lastActivity = time.Now() // Should this be only on client packet? Or any activity?
				f.sessionsLock.Unlock()
			}
		}
	}
}

// cleanupLoop periodically checks for and removes timed-out sessions.
func (f *UDPForwarder) cleanupLoop() {
	ticker := time.NewTicker(f.timeout / 2) // Check more frequently than timeout
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.sessionsLock.Lock()
			now := time.Now()
			for key, session := range f.sessions {
				if now.Sub(session.lastActivity) > f.timeout {
					logging.Logger.Infof("UDP: [%s] Cleaning up timed-out session for client %s (target %s). Last activity: %s",
						f.RuleDesc, session.clientAddr, session.targetConn.RemoteAddr(), session.lastActivity.Format(time.RFC3339))
					session.targetConn.Close() // Close the connection to the target
					delete(f.sessions, key)
				}
			}
			f.sessionsLock.Unlock()
		case <-f.stopChan:
			return // Forwarder is stopping
		}
	}
}

// Stop closes the listener and cleans up resources.
func (f *UDPForwarder) Stop() error {
	logging.Logger.Infof("Stopping UDP forwarder for rule '%s' (listening on %s:%d)", f.RuleDesc, f.ListenHost, f.ListenPort)
	close(f.stopChan) // Signal all goroutines to stop

	var err error
	if f.listenerConn != nil {
		err = f.listenerConn.Close()
		f.listenerConn = nil
	}

	f.sessionsLock.Lock()
	defer f.sessionsLock.Unlock()
	for key, session := range f.sessions {
		session.targetConn.Close()
		delete(f.sessions, key)
	}
	logging.Logger.Infof("Successfully stopped UDP forwarder for rule '%s'. All sessions closed.", f.RuleDesc)
	return err
}
