package forwarder

import (
	"fmt"
	"io"
	"net"
	"rabbitproxy/config"
	"rabbitproxy/logging"
	"strings"
	"sync/atomic" // For atomic operations on activeConnections
	"time"        // For ParsedTimeout
)

// TCPForwarder handles TCP forwarding for a single listen port to a single target port.
type TCPForwarder struct {
	ListenHost string // Specific host this forwarder listens on (usually empty for all interfaces)
	ListenPort int    // Specific port this forwarder listens on

	TargetHost string // Target host
	TargetPort int    // Target port

	RuleDesc          string        // From rule.Description for logging
	Timeout           time.Duration // From rule.ParsedTimeout
	listener          net.Listener  // Active listener for this forwarder
	MaxConnections    int           // From rule.MaxConnections (0 means unlimited)
	activeConnections int64         // Current number of active connections
}

// NewTCPForwarder creates a new TCPForwarder instance for a specific listen/target pair.
func NewTCPForwarder(listenP config.ParsedPort, targetP config.ParsedPort, description string, timeout time.Duration, maxConns int) (*TCPForwarder, error) {
	if listenP.Port <= 0 || listenP.Port > 65535 {
		return nil, fmt.Errorf("invalid listen port %d for TCP forwarder (rule: %s)", listenP.Port, description)
	}
	if targetP.Port <= 0 || targetP.Port > 65535 {
		return nil, fmt.Errorf("invalid target port %d for TCP forwarder (rule: %s)", targetP.Port, description)
	}
	if strings.TrimSpace(targetP.Host) == "" {
		return nil, fmt.Errorf("target host cannot be empty for TCP forwarder (rule: %s)", description)
	}
	if maxConns < 0 { // Should be normalized by loader, but safeguard here.
		logging.Logger.Warnf("NewTCPForwarder (rule '%s'): received negative maxConns (%d), normalizing to 0 (unlimited).", description, maxConns)
		maxConns = 0
	}

	return &TCPForwarder{
		ListenHost:     listenP.Host,
		ListenPort:     listenP.Port,
		TargetHost:     strings.TrimSpace(targetP.Host),
		TargetPort:     targetP.Port,
		RuleDesc:       description,
		Timeout:        timeout,
		MaxConnections: maxConns,
		// activeConnections will default to 0
	}, nil
}

// Start begins listening and forwarding connections.
func (f *TCPForwarder) Start() error {
	listenAddr := fmt.Sprintf("%s:%d", f.ListenHost, f.ListenPort)
	targetAddr := fmt.Sprintf("%s:%d", f.TargetHost, f.TargetPort) // For logging, not used directly in handleTCPConnection's signature anymore

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s for rule '%s' (target %s): %w", listenAddr, f.RuleDesc, targetAddr, err)
	}
	f.listener = listener
	maxConnsStr := "unlimited"
	if f.MaxConnections > 0 {
		maxConnsStr = fmt.Sprintf("%d", f.MaxConnections)
	}
	logging.Logger.Infof("TCP Forwarder: [%s] Listening on %s, forwarding to %s. Max Connections: %s. Timeout: %s",
		f.RuleDesc, listenAddr, targetAddr, maxConnsStr, f.Timeout.String())


	go func() {
		for {
			clientConn, err := f.listener.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
					logging.Logger.Infof("TCP Listener for rule '%s' (on %s) closed.", f.RuleDesc, listenAddr)
					return
				}
				logging.Logger.Errorf("Failed to accept TCP connection for rule '%s' on %s: %v", f.RuleDesc, listenAddr, err)
				if clientConn != nil {
					clientConn.Close()
				}
				continue
			}

			// Check connection limit
			if f.MaxConnections > 0 {
				currentConns := atomic.LoadInt64(&f.activeConnections)
				if currentConns >= int64(f.MaxConnections) {
					logging.Logger.Warnf("TCP: [%s] Max connections (%d) reached. Rejecting new connection from %s", f.RuleDesc, f.MaxConnections, clientConn.RemoteAddr())
					clientConn.Close()
					continue
				}
			}

			atomic.AddInt64(&f.activeConnections, 1)
			// logging.Logger.Debugf("TCP: [%s] New connection from %s. Active connections: %d/%s", f.RuleDesc, clientConn.RemoteAddr(), atomic.LoadInt64(&f.activeConnections), maxConnsStr)
			go f.handleTCPConnection(clientConn) // Pass only clientConn
		}
	}()
	return nil
}

// handleTCPConnection forwards data between the client and the configured target.
func (f *TCPForwarder) handleTCPConnection(clientConn net.Conn) {
	targetAddr := fmt.Sprintf("%s:%d", f.TargetHost, f.TargetPort)
	defer func() {
		atomic.AddInt64(&f.activeConnections, -1)
		// logging.Logger.Debugf("TCP: [%s] Connection closed from %s. Active connections: %d/%s", f.RuleDesc, clientConn.RemoteAddr(), atomic.LoadInt64(&f.activeConnections), maxConnsStr)
		clientConn.Close()
	}()

	logging.Logger.Debugf("TCP: [%s] Handling connection from %s to target %s", f.RuleDesc, clientConn.RemoteAddr(), targetAddr)
	targetConn, err := net.DialTimeout("tcp", targetAddr, f.Timeout) // Use DialTimeout
	if err != nil {
		logging.Logger.Errorf("TCP: [%s] Failed to connect to target %s (client %s): %v", f.RuleDesc, targetAddr, clientConn.RemoteAddr(), err)
		return
	}
		return
	}
	defer targetConn.Close()

	logging.Logger.Debugf("TCP: [%s] Established connection from %s to target %s", f.RuleDesc, clientConn.RemoteAddr(), targetAddr)

	// Timeout for io.Copy can be handled by setting deadlines on connections if f.Timeout > 0
	// This is a more complex implementation involving selecting on multiple channels or using context.
	// For now, io.Copy will run until EOF or an error. The DialTimeout handles initial connection timeout.
	// If f.Timeout is meant to be an inactivity timeout, that requires setting Read/Write deadlines
	// before each Read/Write operation, which is intricate.

	errChan := make(chan error, 2)

	// Bidirectional copy
	go func() {
		// logging.Logger.Debugf("TCP: [%s] Starting copy from client %s to target %s", f.RuleDesc, clientConn.RemoteAddr(), targetAddr)
		_, errCopy := io.Copy(targetConn, clientConn)
		// logging.Logger.Debugf("TCP: [%s] Finished copy from client %s to target %s, err: %v", f.RuleDesc, clientConn.RemoteAddr(), targetAddr, errCopy)
		errChan <- errCopy
		if tcpTargetConn, ok := targetConn.(*net.TCPConn); ok {
			tcpTargetConn.CloseWrite()
		}
	}()

	go func() {
		// logging.Logger.Debugf("TCP: [%s] Starting copy from target %s to client %s", f.RuleDesc, targetAddr, clientConn.RemoteAddr())
		_, errCopy := io.Copy(clientConn, targetConn)
		// logging.Logger.Debugf("TCP: [%s] Finished copy from target %s to client %s, err: %v", f.RuleDesc, targetAddr, clientConn.RemoteAddr(), errCopy)
		errChan <- errCopy
		if tcpClientConn, ok := clientConn.(*net.TCPConn); ok {
			tcpClientConn.CloseWrite()
		}
	}()

	for i := 0; i < 2; i++ {
		if errCopy := <-errChan; errCopy != nil {
			// Avoid logging EOF or "use of closed network connection" as errors, they are normal.
			if errCopy != io.EOF && !strings.Contains(errCopy.Error(), "use of closed network connection") {
				logging.Logger.Warnf("TCP: [%s] Error during data copy between client %s and target %s: %v", f.RuleDesc, clientConn.RemoteAddr(), targetAddr, errCopy)
			}
		}
	}
	// logging.Logger.Debugf("TCP: [%s] Bidirectional copy finished for client %s, target %s", f.RuleDesc, clientConn.RemoteAddr(), targetAddr)
}

// Stop closes the listener.
func (f *TCPForwarder) Stop() error {
	if f.listener != nil {
		originalListenAddr := f.listener.Addr().String() // Get address before closing
		logging.Logger.Infof("Stopping TCP forwarder for rule '%s' (listening on %s, target %s:%d)", f.RuleDesc, originalListenAddr, f.TargetHost, f.TargetPort)
		err := f.listener.Close()
		f.listener = nil // Mark as nil to prevent further operations
		if err != nil {
			logging.Logger.Errorf("Error while stopping TCP forwarder for rule '%s' (listening on %s): %v", f.RuleDesc, originalListenAddr, err)
			return err
		}
		logging.Logger.Infof("Successfully stopped TCP forwarder for rule '%s' (was listening on %s)", f.RuleDesc, originalListenAddr)
		return nil
	}
	// logging.Logger.Debugf("TCP forwarder for rule '%s' (target %s:%d) already stopped or never started.", f.RuleDesc, f.TargetHost, f.TargetPort)
	return nil
}
