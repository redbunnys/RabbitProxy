package forwarder

import (
	"fmt"
	"io"
	"net"
	"os" // Required for os.SyscallError
	"strings" // Required for strings.Contains
	"sync/atomic"
	"time"

	"rabbitproxy/config"
	"rabbitproxy/logging"
	"rabbitproxy/ratelimit"
)

const copyBufferSize = 32 * 1024
const bucketRetryDelay = 5 * time.Millisecond
const maxDialRetries = 3
const dialRetryDelay = 2 * time.Second
const dialAttemptTimeout = 5 * time.Second // Timeout for each individual dial attempt

// Custom error type for context deadline exceeded simulation in customCopyWithRateLimit
type contextDeadlineExceededError struct{}
func (e contextDeadlineExceededError) Error() string { return "context deadline exceeded (simulated for bucket wait)" }
func (e contextDeadlineExceededError) Timeout() bool   { return true }
func (e contextDeadlineExceededError) Temporary() bool { return false }


type TCPForwarder struct {
	ListenHost        string
	ListenPort        int
	TargetHost        string
	TargetPort        int
	RuleDesc          string
	Timeout           time.Duration
	MaxConnections    int
	activeConnections int64
	listener          net.Listener

	ingressBucket     *ratelimit.TokenBucket
	egressBucket      *ratelimit.TokenBucket
	bandwidthSettings config.BandwidthSetting
}

func NewTCPForwarder(listenP config.ParsedPort, targetP config.ParsedPort, description string, timeout time.Duration, maxConns int, bwSettings config.BandwidthSetting) (*TCPForwarder, error) {
	if listenP.Port <= 0 || listenP.Port > 65535 {
		return nil, fmt.Errorf("invalid listen port %d for TCP forwarder (rule: %s)", listenP.Port, description)
	}
	if targetP.Port <= 0 || targetP.Port > 65535 {
		return nil, fmt.Errorf("invalid target port %d for TCP forwarder (rule: %s)", targetP.Port, description)
	}
	if targetP.Host == "" {
		return nil, fmt.Errorf("target host cannot be empty for TCP forwarder (rule: %s)", description)
	}
	if maxConns < 0 {
		logging.S.Warnf("NewTCPForwarder (rule '%s'): received negative maxConns (%d), normalizing to 0 (unlimited).", description, maxConns)
		maxConns = 0
	}

	fwd := &TCPForwarder{
		ListenHost:        listenP.Host,
		ListenPort:        listenP.Port,
		TargetHost:        targetP.Host,
		TargetPort:        targetP.Port,
		RuleDesc:          description,
		Timeout:           timeout,
		MaxConnections:    maxConns,
		bandwidthSettings: bwSettings,
	}

	if bwSettings.IsSet && bwSettings.RateBPS > 0 {
		fwd.ingressBucket = ratelimit.NewTokenBucket(bwSettings.RateBPS, bwSettings.BurstBPS)
		fwd.egressBucket = ratelimit.NewTokenBucket(bwSettings.RateBPS, bwSettings.BurstBPS)
		logging.S.Debugf("TCP: [%s] Initialized token buckets. Rate: %.2f Bps, Burst: %.2f Bps (each direction)", description, bwSettings.RateBPS, bwSettings.BurstBPS)
	} else if bwSettings.IsSet && bwSettings.RateBPS == 0 {
		logging.S.Debugf("TCP: [%s] Bandwidth is explicitly set to unlimited.", description)
	} else {
		logging.S.Debugf("TCP: [%s] No bandwidth limit configured.", description)
	}
	return fwd, nil
}

func (f *TCPForwarder) Start() error {
	listenAddrStr := fmt.Sprintf("%s:%d", f.ListenHost, f.ListenPort)
	listener, err := net.Listen("tcp", listenAddrStr)
	if err != nil {
		if f.ListenPort < 1024 {
			if opErr, ok := err.(*net.OpError); ok {
				errMsgLower := strings.ToLower(opErr.Err.Error())
				isPermissionError := false
				if sysErr, okSys := opErr.Err.(*os.SyscallError); okSys {
					// syscall.EACCES is 13 on many Unix-like systems
					// This check is more robust if syscall.EACCES can be directly used,
					// but string matching is a common fallback for broader compatibility.
					if strings.Contains(errMsgLower, "permission denied") || strings.Contains(errMsgLower, "access denied") || sysErr.Err.Error() == "errno 13" {
						isPermissionError = true
					}
				} else if strings.Contains(errMsgLower, "permission denied") || strings.Contains(errMsgLower, "access denied") {
					isPermissionError = true
				}
				if isPermissionError {
					logging.S.Warnf("TCP: [%s] Permission denied when trying to listen on privileged port %s. Try running with root/administrator privileges or use a port >= 1024.", f.RuleDesc, listenAddrStr)
				}
			}
		}
		return fmt.Errorf("TCP: [%s] failed to listen on %s: %w", f.RuleDesc, listenAddrStr, err)
	}
	f.listener = listener

	rateLimitLog := "disabled"
	if f.bandwidthSettings.IsSet {
		if f.bandwidthSettings.RateBPS > 0 {
			rateLimitLog = fmt.Sprintf("Rate: %.2f Bps, Burst: %.2f Bps (per direction)", f.bandwidthSettings.RateBPS, f.bandwidthSettings.BurstBPS)
		} else {
			rateLimitLog = "unlimited"
		}
	}
	logging.S.Infof("TCP Forwarder: [%s] Listening on %s, forwarding to %s:%d. MaxConns: %d, Timeout: %s, Bandwidth: %s",
		f.RuleDesc, listenAddrStr, f.TargetHost, f.TargetPort, f.MaxConnections, f.Timeout, rateLimitLog)

	maxConnsStr := "unlimited"
	if f.MaxConnections > 0 {
		maxConnsStr = fmt.Sprintf("%d", f.MaxConnections)
	}

	go func() {
		for {
			clientConn, errAccept := f.listener.Accept()
			if errAccept != nil {
				if opErr, ok := errAccept.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
					logging.S.Infof("TCP Listener for rule '%s' (on %s) closed.", f.RuleDesc, listenAddrStr)
					return
				}
				logging.S.Errorf("TCP: [%s] Failed to accept connection on %s: %v", f.RuleDesc, listenAddrStr, errAccept)
				if clientConn != nil {
					clientConn.Close()
				}
				continue
			}

			if f.MaxConnections > 0 {
				currentConns := atomic.LoadInt64(&f.activeConnections)
				if currentConns >= int64(f.MaxConnections) {
					logging.S.Warnf("TCP: [%s] Max connections (%d) reached. Rejecting new connection from %s", f.RuleDesc, f.MaxConnections, clientConn.RemoteAddr())
					clientConn.Close()
					continue
				}
			}

			atomic.AddInt64(&f.activeConnections, 1)
			logging.S.Debugf("TCP: [%s] New connection from %s. Active: %d/%s", f.RuleDesc, clientConn.RemoteAddr(), atomic.LoadInt64(&f.activeConnections), maxConnsStr)
			go f.handleTCPConnection(clientConn)
		}
	}()
	return nil
}

func customCopyWithRateLimit(dst net.Conn, src net.Conn, bucket *ratelimit.TokenBucket, effectiveTimeout time.Duration, ruleDesc string, direction string, errChan chan error, connClosedChan chan struct{}) {
	buf := make([]byte, copyBufferSize)
	var totalBytesCopied int64
	var lastActivityTime time.Time = time.Now() // Track activity for timeout during bucket wait

	defer func() {
		if tcpDst, ok := dst.(*net.TCPConn); ok {
			tcpDst.CloseWrite()
		}
		logging.S.Debugf("TCP: [%s] Finished %s copy, total bytes: %d", ruleDesc, direction, totalBytesCopied)
		// errChan is signaled explicitly based on outcome
	}()

	for {
		select {
		case <-connClosedChan:
			logging.S.Debugf("TCP: [%s] %s copy: Connection closing signal received, aborting copy.", ruleDesc, direction)
			errChan <- io.ErrClosedPipe
			return
		default:
		}

		if effectiveTimeout > 0 {
			src.SetReadDeadline(time.Now().Add(effectiveTimeout))
		}
		readN, readErr := src.Read(buf)
		if effectiveTimeout > 0 {
			src.SetReadDeadline(time.Time{})
		}

		if readN > 0 {
			lastActivityTime = time.Now()
			if bucket != nil {
				needed := readN
				for !bucket.Consume(needed) {
					logging.S.Debugf("TCP: [%s] %s bucket empty (need %d, have ~%.0f), delaying for %v", ruleDesc, direction, needed, bucket.CurrentTokens(), bucketRetryDelay)
					select {
					case <-time.After(bucketRetryDelay):
					case <-connClosedChan:
						logging.S.Debugf("TCP: [%s] %s copy: Connection closing signal received during bucket delay.", ruleDesc, direction)
						errChan <- io.ErrClosedPipe
						return
					}
					if effectiveTimeout > 0 && time.Since(lastActivityTime) > effectiveTimeout {
						 logging.S.Warnf("TCP: [%s] %s copy: Timeout (%s) exceeded while waiting for token bucket.", ruleDesc, direction, effectiveTimeout)
                         errChan <- contextDeadlineExceededError{}
                         return
					}
				}
			}

			if effectiveTimeout > 0 {
				dst.SetWriteDeadline(time.Now().Add(effectiveTimeout))
			}
			writeN, writeErr := dst.Write(buf[:readN])
			if effectiveTimeout > 0 {
				dst.SetWriteDeadline(time.Time{})
			}

			if writeErr != nil {
				logging.S.Warnf("TCP: [%s] Error writing %s data to %s from %s: %v", ruleDesc, direction, dst.RemoteAddr(), src.RemoteAddr(), writeErr)
				errChan <- writeErr
				return
			}
			if readN != writeN {
				logging.S.Warnf("TCP: [%s] Short write %s to %s from %s: read %d, wrote %d", ruleDesc, direction, dst.RemoteAddr(), src.RemoteAddr(), readN, writeN)
				errChan <- io.ErrShortWrite
				return
			}
			totalBytesCopied += int64(writeN)
		}

		if readErr != nil {
			if readErr == io.EOF {
				logging.S.Debugf("TCP: [%s] EOF reached on %s read from %s.", ruleDesc, direction, src.RemoteAddr())
				errChan <- nil
			} else {
				if netErr, ok := readErr.(net.Error); ok && netErr.Timeout() {
					logging.S.Warnf("TCP: [%s] Timeout reading %s data from %s: %v", ruleDesc, direction, src.RemoteAddr(), readErr)
				} else if readErr != io.ErrClosedPipe && !strings.Contains(readErr.Error(), "use of closed network connection"){
					logging.S.Warnf("TCP: [%s] Error reading %s data from %s: %v", ruleDesc, direction, src.RemoteAddr(), readErr)
                } else {
                    logging.S.Debugf("TCP: [%s] Read loop ended for %s from %s due to closed pipe/connection: %v", ruleDesc, direction, src.RemoteAddr(), readErr)
                }
				errChan <- readErr
			}
			return
		}
	}
}

func (f *TCPForwarder) handleTCPConnection(clientConn net.Conn) {
	maxConnsStr := "unlimited"
	if f.MaxConnections > 0 {
		maxConnsStr = fmt.Sprintf("%d", f.MaxConnections)
	}
	closedEarly := false

	defer func() {
		atomic.AddInt64(&f.activeConnections, -1)
		if closedEarly {
		    logging.S.Debugf("TCP: [%s] Conn from %s closed early. Active: %d/%s", f.RuleDesc, clientConn.RemoteAddr(), atomic.LoadInt64(&f.activeConnections), maxConnsStr)
		} else {
		    logging.S.Debugf("TCP: [%s] Conn from %s fully closed. Active: %d/%s", f.RuleDesc, clientConn.RemoteAddr(), atomic.LoadInt64(&f.activeConnections), maxConnsStr)
		}
		clientConn.Close()
	}()

	var targetConn net.Conn
	var errDial error
	targetAddrStr := fmt.Sprintf("%s:%d", f.TargetHost, f.TargetPort)

	for i := 0; i < maxDialRetries; i++ {
		logging.S.Debugf("TCP: [%s] Attempting to connect to target %s (attempt %d/%d, timeout %s)", f.RuleDesc, targetAddrStr, i+1, maxDialRetries, dialAttemptTimeout)
		targetConn, errDial = net.DialTimeout("tcp", targetAddrStr, dialAttemptTimeout)
		if errDial == nil {
			break // Connection successful
		}
		logging.S.Warnf("TCP: [%s] Failed to connect to target %s (attempt %d/%d): %v.", f.RuleDesc, targetAddrStr, i+1, maxDialRetries, errDial)
		if i < maxDialRetries-1 {
			logging.S.Infof("TCP: [%s] Retrying target connection to %s in %v...", f.RuleDesc, targetAddrStr, dialRetryDelay)
			time.Sleep(dialRetryDelay)
		}
	}

	if errDial != nil {
		logging.S.Errorf("TCP: [%s] Failed to connect to target %s after %d attempts: %v. Closing client connection %s.",
			f.RuleDesc, targetAddrStr, maxDialRetries, errDial, clientConn.RemoteAddr())
		closedEarly = true
		return
	}
	defer targetConn.Close()

	logging.S.Infof("TCP: [%s] Accepted connection from %s, successfully connected to target %s", f.RuleDesc, clientConn.RemoteAddr(), targetAddrStr)
	if f.bandwidthSettings.IsSet {
		if f.bandwidthSettings.RateBPS > 0 {
			logging.S.Infof("TCP: [%s] Applying bandwidth limit - Rate: %.2f Bps, Burst: %.2f Bps (per direction)", f.RuleDesc, f.bandwidthSettings.RateBPS, f.bandwidthSettings.BurstBPS)
		} else {
			logging.S.Infof("TCP: [%s] Bandwidth is explicitly unlimited for this rule.", f.RuleDesc)
		}
	}

	connClosedChan := make(chan struct{})
	errChan := make(chan error, 2)
	copyTimeout := f.Timeout

	go customCopyWithRateLimit(targetConn, clientConn, f.ingressBucket, copyTimeout, f.RuleDesc, "client->target", errChan, connClosedChan)
	go customCopyWithRateLimit(clientConn, targetConn, f.egressBucket, copyTimeout, f.RuleDesc, "target->client", errChan, connClosedChan)

	completedCopies := 0
	for i := 0; i < 2; i++ {
		select {
		case copyErr := <-errChan:
			if copyErr != nil {
				if netErr, ok := copyErr.(net.Error); ok && netErr.Timeout() {
					logging.S.Warnf("TCP: [%s] Connection %s <-> %s timed out during copy: %v", f.RuleDesc, clientConn.RemoteAddr(), targetAddrStr, copyErr)
				} else if copyErr != io.EOF && copyErr != io.ErrClosedPipe && !strings.Contains(copyErr.Error(), "use of closed network connection") && !strings.Contains(copyErr.Error(), "broken pipe") {
					logging.S.Debugf("TCP: [%s] Error during copy for %s <-> %s: %v", f.RuleDesc, clientConn.RemoteAddr(), targetAddrStr, copyErr)
				} else {
                     logging.S.Debugf("TCP: [%s] Copy routine for %s finished with expected EOF/close: %v", f.RuleDesc, clientConn.RemoteAddr(), copyErr)
                }
			}
		// Optional: Add a timeout here for waiting on errChan if copy routines could get stuck indefinitely without erroring.
		// case <-time.After(someOverallCopyTimeout):
		//    logging.S.Errorf("TCP: [%s] Overall copy timeout for %s", f.RuleDesc, clientConn.RemoteAddr())
		//    close(connClosedChan) // Forcefully signal copy routines
		//    return // Exit handler
		}
		completedCopies++
	}

	// Both copy goroutines have finished (either cleanly or with an error signaled on errChan).
	// Signal them to stop if they are somehow stuck in bucket retry loop waiting on connClosedChan.
	close(connClosedChan)

	logging.S.Debugf("TCP: [%s] Both copy routines finished for %s", f.RuleDesc, clientConn.RemoteAddr())
}

func (f *TCPForwarder) Stop() error {
	if f.listener != nil {
		originalListenAddr := f.listener.Addr().String()
		logging.S.Infof("Stopping TCP forwarder for rule '%s' (listening on %s, target %s:%d)", f.RuleDesc, originalListenAddr, f.TargetHost, f.TargetPort)
		err := f.listener.Close()
		f.listener = nil
		if err != nil {
			logging.S.Errorf("Error while stopping TCP forwarder for rule '%s' (listening on %s): %v", f.RuleDesc, originalListenAddr, err)
			return err
		}
		logging.S.Infof("Successfully stopped TCP forwarder for rule '%s' (was listening on %s)", f.RuleDesc, originalListenAddr)
		return nil
	}
	logging.S.Debugf("TCP forwarder for rule '%s' (target %s:%d) already stopped or never started.", f.RuleDesc, f.TargetHost, f.TargetPort)
	return nil
}
