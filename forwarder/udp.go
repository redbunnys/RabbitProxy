package forwarder

import (
	"fmt"
	"net"
	"os"     // Required for os.SyscallError
	"strings" // Required for strings.Contains
	"sync"
	"time"

	"rabbitproxy/config"
	"rabbitproxy/logging"
	"rabbitproxy/ratelimit"
)

const udpSessionTimeoutDefault = 2 * time.Minute
const udpCopyBufferSize = 4096

type clientSession struct {
	clientAddr   *net.UDPAddr
	targetConn   *net.UDPConn
	lastActivity time.Time
}

type UDPForwarder struct {
	ListenHost string
	ListenPort int
	TargetHost string
	TargetPort int
	RuleDesc   string

	MaxConnections int

	listenerConn *net.UDPConn
	sessions     map[string]*clientSession
	sessionsLock sync.Mutex
	stopChan     chan struct{}
	sessionTimeout time.Duration

	ingressBucket *ratelimit.TokenBucket
	egressBucket  *ratelimit.TokenBucket
	bandwidthSettings config.BandwidthSetting
}

func NewUDPForwarder(listenP config.ParsedPort, targetP config.ParsedPort, description string, maxConns int, bwSettings config.BandwidthSetting) (*UDPForwarder, error) {
	if listenP.Port <= 0 || listenP.Port > 65535 {
		return nil, fmt.Errorf("invalid listen port %d for UDP forwarder (rule: %s)", listenP.Port, description)
	}
	if targetP.Host == "" || targetP.Port <= 0 || targetP.Port > 65535 {
		return nil, fmt.Errorf("invalid target address '%s:%d' for UDP forwarder (rule: %s)", targetP.Host, targetP.Port, description)
	}
	if maxConns < 0 {
		logging.S.Warnf("NewUDPForwarder (rule '%s'): received negative maxConns (%d), normalizing to 0 (unlimited).", description, maxConns)
		maxConns = 0
	}

	fwd := &UDPForwarder{
		ListenHost:     listenP.Host,
		ListenPort:     listenP.Port,
		TargetHost:     targetP.Host,
		TargetPort:     targetP.Port,
		RuleDesc:       description,
		MaxConnections: maxConns,
		sessions:       make(map[string]*clientSession),
		stopChan:       make(chan struct{}),
		sessionTimeout: udpSessionTimeoutDefault,
		bandwidthSettings: bwSettings,
	}

	if bwSettings.IsSet && bwSettings.RateBPS > 0 {
		fwd.ingressBucket = ratelimit.NewTokenBucket(bwSettings.RateBPS, bwSettings.BurstBPS)
		fwd.egressBucket = ratelimit.NewTokenBucket(bwSettings.RateBPS, bwSettings.BurstBPS)
		logging.S.Debugf("UDP: [%s] Initialized token buckets. Rate: %.2f Bps, Burst: %.2f Bps (each direction)", description, bwSettings.RateBPS, bwSettings.BurstBPS)
	} else if bwSettings.IsSet && bwSettings.RateBPS == 0 {
		logging.S.Debugf("UDP: [%s] Bandwidth is explicitly set to unlimited.", description)
	} else {
		logging.S.Debugf("UDP: [%s] No bandwidth limit configured for this forwarder.", description)
	}
	return fwd, nil
}

func (f *UDPForwarder) Start() error {
	listenAddrStr := fmt.Sprintf("%s:%d", f.ListenHost, f.ListenPort)
	listenUDPAddr, errResolve := net.ResolveUDPAddr("udp", listenAddrStr)
	if errResolve != nil {
		return fmt.Errorf("UDP: [%s] Failed to resolve listen address %s: %w", f.RuleDesc, listenAddrStr, errResolve)
	}

	conn, errListen := net.ListenUDP("udp", listenUDPAddr)
	if errListen != nil {
		if f.ListenPort < 1024 {
			if opErr, ok := errListen.(*net.OpError); ok {
				errMsgLower := strings.ToLower(opErr.Err.Error())
				isPermissionError := false
				if sysErr, okSys := opErr.Err.(*os.SyscallError); okSys {
					if strings.Contains(errMsgLower, "permission denied") || strings.Contains(errMsgLower, "access denied") || sysErr.Err.Error() == "errno 13" {
						isPermissionError = true
					}
				} else if strings.Contains(errMsgLower, "permission denied") || strings.Contains(errMsgLower, "access denied") {
					isPermissionError = true
				}
				if isPermissionError {
					logging.S.Warnf("UDP: [%s] Permission denied when trying to listen on privileged port %s. Try running with root/administrator privileges or use a port >= 1024.", f.RuleDesc, listenAddrStr)
				}
			}
		}
		return fmt.Errorf("UDP: [%s] failed to listen on %s: %w", f.RuleDesc, listenAddrStr, errListen)
	}
	f.listenerConn = conn

	maxSessStr := "unlimited"
	if f.MaxConnections > 0 {
		maxSessStr = fmt.Sprintf("%d", f.MaxConnections)
	}
	bwLog := "disabled"
	if f.bandwidthSettings.IsSet {
		if f.bandwidthSettings.RateBPS > 0 {
			bwLog = fmt.Sprintf("Rate: %.2f Bps, Burst: %.2f Bps (per direction)", f.bandwidthSettings.RateBPS, f.bandwidthSettings.BurstBPS)
		} else {
			bwLog = "unlimited"
		}
	}

	logging.S.Infof("UDP Forwarder: [%s] Listening on %s, targetting %s:%d. Max Sessions: %s. Session Timeout: %s. Bandwidth: %s",
		f.RuleDesc, listenAddrStr, f.TargetHost, f.TargetPort, maxSessStr, f.sessionTimeout.String(), bwLog)

	go f.cleanupLoop()
	go func() {
		buffer := make([]byte, udpCopyBufferSize)
		for {
			select {
			case <-f.stopChan:
				return
			default:
			}

			f.listenerConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, clientAddr, readErr := f.listenerConn.ReadFromUDP(buffer)

			if readErr != nil {
				if netErr, ok := readErr.(net.Error); ok && netErr.Timeout() {
					continue
				}
				// Use strings.Contains for broader compatibility with "use of closed network connection"
				if strings.Contains(readErr.Error(), "use of closed network connection") {
					logging.S.Infof("UDP Listener for rule '%s' on %s closed.", f.RuleDesc, f.listenerConn.LocalAddr())
					return
				}
				logging.S.Warnf("UDP: [%s] Error reading from listener: %v", f.RuleDesc, readErr)
				continue
			}

			if n > 0 {
				if f.ingressBucket != nil && !f.ingressBucket.Consume(n) {
					logging.S.Debugf("UDP: [%s] Ingress rate limit exceeded for client %s. Dropping %d bytes.", f.RuleDesc, clientAddr.String(), n)
					continue
				}

				data := make([]byte, n)
				copy(data, buffer[:n])
				go f.handleClientPacket(clientAddr, data)
			}
		}
	}()
	return nil
}

func (f *UDPForwarder) handleClientPacket(clientAddr *net.UDPAddr, data []byte) {
	clientKey := clientAddr.String()
	f.sessionsLock.Lock()

	session, found := f.sessions[clientKey]
	if !found {
		if f.MaxConnections > 0 && len(f.sessions) >= f.MaxConnections {
			f.sessionsLock.Unlock()
			logging.S.Warnf("UDP: [%s] Max active sessions (%d) reached. Dropping %d byte packet from %s. Current sessions: %d",
				f.RuleDesc, f.MaxConnections, len(data), clientAddr, len(f.sessions))
			return
		}

		targetUDPAddrStr := fmt.Sprintf("%s:%d", f.TargetHost, f.TargetPort)
		targetUDPAddr, errResolve := net.ResolveUDPAddr("udp", targetUDPAddrStr)
		if errResolve != nil {
			f.sessionsLock.Unlock()
			logging.S.Errorf("UDP: [%s] Failed to resolve target %s for client %s: %v", f.RuleDesc, targetUDPAddrStr, clientAddr, errResolve)
			return
		}

		targetConn, errDial := net.DialUDP("udp", nil, targetUDPAddr)
		if errDial != nil {
			f.sessionsLock.Unlock()
			logging.S.Errorf("UDP: [%s] Failed to dial target %s for client %s: %v", f.RuleDesc, targetUDPAddrStr, clientAddr, errDial)
			return
		}

		session = &clientSession{
			clientAddr:   clientAddr,
			targetConn:   targetConn,
			lastActivity: time.Now(),
		}
		f.sessions[clientKey] = session
		logging.S.Infof("UDP: [%s] New session for %s -> %s. Active sessions: %d",
			f.RuleDesc, clientAddr, targetConn.RemoteAddr(), len(f.sessions))

		go f.listenFromTarget(session)
	}
	session.lastActivity = time.Now()
	f.sessionsLock.Unlock()

	_, errWrite := session.targetConn.Write(data)
	if errWrite != nil {
		logging.S.Warnf("UDP: [%s] Failed to write %d bytes to target %s for client %s: %v",
			f.RuleDesc, len(data), session.targetConn.RemoteAddr(), clientAddr, errWrite)
	} else {
		logging.S.Debugf("UDP: [%s] Forwarded %d bytes from %s to %s", f.RuleDesc, len(data), clientAddr, session.targetConn.RemoteAddr())
	}
}

func (f *UDPForwarder) listenFromTarget(s *clientSession) {
	buffer := make([]byte, udpCopyBufferSize)
	defer func() {
		s.targetConn.Close()
		logging.S.Debugf("UDP: [%s] Goroutine listening from target %s for client %s ended.", f.RuleDesc, s.targetConn.RemoteAddr(), s.clientAddr)
	}()

	for {
		select {
		case <-f.stopChan:
			return
		default:
		}

		s.targetConn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, _, readErr := s.targetConn.ReadFromUDP(buffer)

		if readErr != nil {
			if netErr, ok := readErr.(net.Error); ok && netErr.Timeout() {
				f.sessionsLock.Lock()
				sessionStillExists := false
				if currentSession, exists := f.sessions[s.clientAddr.String()]; exists && currentSession == s {
					sessionStillExists = true
					if time.Since(s.lastActivity) > f.sessionTimeout {
						logging.S.Infof("UDP: [%s] Session for client %s (target %s) inactive for %v, stopping listener. Will be cleaned up.",
							f.RuleDesc, s.clientAddr, s.targetConn.RemoteAddr(), f.sessionTimeout)
						f.sessionsLock.Unlock()
						return
					}
				}
				if !sessionStillExists {
					logging.S.Debugf("UDP: [%s] Session for client %s (target %s) already removed by cleanup. Stopping listener.", f.RuleDesc, s.clientAddr, s.targetConn.RemoteAddr())
					f.sessionsLock.Unlock()
					return
				}
				f.sessionsLock.Unlock()
				continue
			}
			if strings.Contains(readErr.Error(), "use of closed network connection") {
				logging.S.Debugf("UDP: [%s] Target connection for client %s closed (likely by Stop() or cleanup).", f.RuleDesc, s.clientAddr)
				return
			}
			logging.S.Warnf("UDP: [%s] Error reading from target %s for client %s: %v", f.RuleDesc, s.targetConn.RemoteAddr(), s.clientAddr, readErr)
			return
		}

		if n > 0 {
			if f.egressBucket != nil && !f.egressBucket.Consume(n) {
				logging.S.Debugf("UDP: [%s] Egress rate limit exceeded for target %s to client %s. Dropping %d bytes.", f.RuleDesc, s.targetConn.RemoteAddr(), s.clientAddr, n)
				continue
			}

			_, writeErr := f.listenerConn.WriteToUDP(buffer[:n], s.clientAddr)
			if writeErr != nil {
				logging.S.Warnf("UDP: [%s] Failed to write %d bytes to client %s from target %s: %v", f.RuleDesc, n, s.clientAddr, s.targetConn.RemoteAddr(), writeErr)
			} else {
				logging.S.Debugf("UDP: [%s] Relayed %d bytes from target %s to client %s", f.RuleDesc, n, s.targetConn.RemoteAddr(), s.clientAddr)
				f.sessionsLock.Lock()
				s.lastActivity = time.Now()
				f.sessionsLock.Unlock()
			}
		}
	}
}

func (f *UDPForwarder) cleanupLoop() {
	tickerInterval := f.sessionTimeout / 2
	if tickerInterval < 1*time.Second { // Ensure ticker interval is reasonable
		tickerInterval = 1*time.Second
	}
	if f.sessionTimeout == 0 { // If session timeout is disabled (0), effectively disable cleanup loop by using a very long interval
		tickerInterval = 24 * time.Hour
	}
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if f.sessionTimeout == 0 { continue } // Skip cleanup if session timeout is disabled

			f.sessionsLock.Lock()
			now := time.Now()
			cleanedCount := 0
			for key, session := range f.sessions {
				if now.Sub(session.lastActivity) > f.sessionTimeout {
					logging.S.Infof("UDP: [%s] Cleaning up timed-out session for client %s (target %s). Last activity: %s. Active sessions before cleanup: %d",
						f.RuleDesc, session.clientAddr, session.targetConn.RemoteAddr(), session.lastActivity.Format(time.RFC3339), len(f.sessions))
					session.targetConn.Close()
					delete(f.sessions, key)
					cleanedCount++
				}
			}
			if cleanedCount > 0 {
				logging.S.Infof("UDP: [%s] Cleaned up %d timed-out session(s). Active sessions: %d", f.RuleDesc, cleanedCount, len(f.sessions))
			}
			f.sessionsLock.Unlock()
		case <-f.stopChan:
			return
		}
	}
}

func (f *UDPForwarder) Stop() error {
	logging.S.Infof("Stopping UDP forwarder for rule '%s' (listening on %s:%d)", f.RuleDesc, f.ListenHost, f.ListenPort)
	close(f.stopChan)

	var err error
	if f.listenerConn != nil {
		err = f.listenerConn.Close()
		f.listenerConn = nil
	}

	f.sessionsLock.Lock()
	defer f.sessionsLock.Unlock()
	if len(f.sessions) > 0 {
		logging.S.Debugf("UDP: [%s] Closing %d active session(s) due to stop.", f.RuleDesc, len(f.sessions))
		for key, session := range f.sessions {
			session.targetConn.Close()
			delete(f.sessions, key)
		}
	}
	logging.S.Infof("Successfully stopped UDP forwarder for rule '%s'. All sessions closed.", f.RuleDesc)
	return err
}
