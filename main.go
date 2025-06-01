package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"rabbitproxy/pkg/ratelimit"
	"rabbitproxy/pkg/trafficstats"
)

const (
	defaultRateLimitMBps    = 1.0 // MB/s
	defaultBurstLimitMB     = 2.0 // MB
	statsCollectionInterval = 5 * time.Second
	statsMaxAge             = 60 * time.Second
	defaultStatMultiplier   = 1.0
	copyChunkSize           = 4 * 1024 // 4KB
)

// rateLimitedCopy reads from src, waits for tokens from bucket, writes to dst, and records traffic.
func rateLimitedCopy(dst io.Writer, src io.Reader, bucket *ratelimit.TokenBucket, sc *trafficstats.StatsCollector, clientIP string, streamLabel string, remotePort int) (written int64, err error) {
	buf := make([]byte, copyChunkSize)
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			// Wait for tokens BEFORE attempting to write
			// The cost is per byte read from source
			waitErr := bucket.Wait(int64(nr))
			if waitErr != nil {
				// This could happen if TokenBucket.Wait is modified to be interruptible or returns errors.
				// For now, it blocks or returns nil if count <=0.
				log.Printf("[%s %s] Error waiting for tokens: %v. Aborting copy.", clientIP, streamLabel, waitErr)
				return written, waitErr // Abort if waiting fails
			}

			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw { // Check for anomalous writes
				nw = 0
				if ew == nil {
					ew = io.ErrInvalidWrite
				}
			}
			written += int64(nw)
			if ew != nil {
				log.Printf("[%s %s] Write error: %v", clientIP, streamLabel, ew)
				err = ew
				break
			}
			if nr != nw {
				log.Printf("[%s %s] Short write", clientIP, streamLabel)
				err = io.ErrShortWrite
				break
			}

			// Record traffic after successful write
			// Using '1' for packets here is an approximation; could be refined if packet data is available
			sc.RecordTraffic(int64(nw), 1, clientIP, streamLabel, remotePort)
		}
		if er != nil {
			if er != io.EOF { // Don't log EOF as an error
				log.Printf("[%s %s] Read error: %v", clientIP, streamLabel, er)
				err = er
			} else {
				// If EOF, ensure any written data from the last chunk is accounted for, then break.
				// This case is implicitly handled as 'written' is updated before this check.
			}
			break
		}
	}
	return written, err
}

func handleConnection(clientConn net.Conn, targetAddr string, bm *ratelimit.BucketManager, sc *trafficstats.StatsCollector) {
	clientRemoteAddr := clientConn.RemoteAddr().String()
	clientIP, clientSourcePortStr, err := net.SplitHostPort(clientRemoteAddr)
	if err != nil {
		log.Printf("Failed to parse client remote address %s: %v", clientRemoteAddr, err)
		clientIP = clientRemoteAddr // Use full address as fallback identifier
	}
	clientSourcePort, _ := strconv.Atoi(clientSourcePortStr) // Ignore error for port if not needed for rate limiting key

	log.Printf("[%s] Accepted connection from %s", clientIP, clientConn.LocalAddr())
	defer func() {
		log.Printf("[%s] Closing client connection.", clientIP)
		clientConn.Close()
	}()

	serverConn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		log.Printf("[%s] Failed to connect to target %s: %v", clientIP, targetAddr, err)
		return
	}
	log.Printf("[%s] Connected to target %s", clientIP, targetAddr)
	defer func() {
		log.Printf("[%s] Closing server connection to %s.", clientIP, targetAddr)
		serverConn.Close()
	}()

	_, targetPortStr, err := net.SplitHostPort(targetAddr)
	if err != nil {
		log.Printf("[%s] Failed to parse target address %s for port: %v", clientIP, targetAddr, err)
		// Use a default or handle error if port is critical for stats
	}
	targetPort, _ := strconv.Atoi(targetPortStr)

	// Get token buckets for this client IP (up and down streams)
	// Identifiers could be more sophisticated, e.g., include service name or port.
	upBucketID := clientIP + "_up"
	downBucketID := clientIP + "_down"

	// Assuming default rate/burst are handled by BucketManager if not specified per bucket
	clientUploadBucket := bm.GetBucket(upBucketID)    // Rate limit client uploads
	clientDownloadBucket := bm.GetBucket(downBucketID) // Rate limit client downloads

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine to copy data from client to server (upload)
	go func() {
		defer wg.Done()
		if tcpConn, ok := serverConn.(*net.TCPConn); ok {
			defer tcpConn.CloseWrite()
		}
		log.Printf("[%s] Starting copy from client to server (upload, %s -> %s)", clientIP, clientRemoteAddr, serverConn.RemoteAddr())
		copiedBytes, err := rateLimitedCopy(serverConn, clientConn, clientUploadBucket, sc, clientIP, "TCP_UP", targetPort)
		if err != nil && err != io.EOF { // EOF is expected and not an application error
			log.Printf("[%s] Error in client->server copy: %v", clientIP, err)
		}
		log.Printf("[%s] Finished copying from client to server (upload). Bytes: %d", clientIP, copiedBytes)
	}()

	// Goroutine to copy data from server to client (download)
	go func() {
		defer wg.Done()
		if tcpConn, ok := clientConn.(*net.TCPConn); ok {
			defer tcpConn.CloseWrite()
		}
		log.Printf("[%s] Starting copy from server to client (download, %s -> %s)", clientIP, serverConn.RemoteAddr(), clientRemoteAddr)
		copiedBytes, err := rateLimitedCopy(clientConn, serverConn, clientDownloadBucket, sc, clientIP, "TCP_DOWN", clientSourcePort)
		if err != nil && err != io.EOF {
			log.Printf("[%s] Error in server->client copy: %v", clientIP, err)
		}
		log.Printf("[%s] Finished copying from server to client (download). Bytes: %d", clientIP, copiedBytes)
	}()

	log.Printf("[%s] Waiting for data transfer to complete...", clientIP)
	wg.Wait()
	log.Printf("[%s] Data transfer complete. Closing connections.", clientIP)
}

func main() {
	listenAddr := flag.String("listen", "localhost:8080", "Address for the proxy to listen on")
	targetAddr := flag.String("target", "localhost:8000", "Address of the target server to proxy to")
	rateMBps := flag.Float64("rate", defaultRateLimitMBps, "Default rate limit in MB/s per client stream (up/down)")
	burstMB := flag.Float64("burst", defaultBurstLimitMB, "Default burst limit in MB per client stream (up/down)")
	flag.Parse()

	// Initialize BucketManager and StatsCollector
	defaultRateBytes := int64(*rateMBps * 1024 * 1024)
	defaultBurstBytes := int64(*burstMB * 1024 * 1024)

	bucketManager := ratelimit.NewBucketManager(defaultRateBytes, defaultBurstBytes)
	statsCollector := trafficstats.NewStatsCollector(statsCollectionInterval, statsMaxAge, defaultStatMultiplier)
	defer statsCollector.Stop() // Ensure stats collector's run() goroutine is stopped on exit

	log.Printf("Starting TCP proxy. Listening on %s, Targeting %s", *listenAddr, *targetAddr)
	log.Printf("Default rate limit: %.2f MB/s, Default burst: %.2f MB per stream", *rateMBps, *burstMB)

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Failed to start listener on %s: %v", *listenAddr, err)
		os.Exit(1)
	}
	defer listener.Close()

	log.Printf("Proxy listening for incoming connections...")

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			if opError, ok := err.(*net.OpError); ok && !opError.Temporary() {
				log.Printf("Non-temporary accept error. Exiting accept loop.")
				break
			}
			continue
		}
		go handleConnection(clientConn, *targetAddr, bucketManager, statsCollector)
	}

	log.Println("Proxy shutting down.")
}
