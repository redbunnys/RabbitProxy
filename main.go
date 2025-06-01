// Package main implements RabbitProxy, a TCP proxy with rate limiting and traffic statistics.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"rabbitproxy/pkg/ratelimit"
	"rabbitproxy/pkg/trafficstats"
)

const (
	defaultRateLimitMBps    = 1.0         // defaultRateLimitMBps is the default rate limit in Megabytes per second for new streams.
	defaultBurstLimitMB     = 2.0         // defaultBurstLimitMB is the default burst limit in Megabytes for new streams.
	statsCollectionInterval = 5 * time.Second // statsCollectionInterval is the frequency for periodic stats processing (e.g., pruning).
	statsMaxAge             = 60 * time.Second // statsMaxAge is the maximum age for recent traffic items used in real-time bandwidth calculation.
	defaultStatMultiplier   = 1.0         // defaultStatMultiplier is the default statistical multiplier for traffic counts.
	copyChunkSize           = 4 * 1024    // copyChunkSize is the buffer size used in rateLimitedCopy.
	defaultHTTPAPIListen    = "localhost:8081" // defaultHTTPAPIListen is the default address for the HTTP API server.
)

// RateLimitConfigPayload defines the structure for JSON request and response bodies
// when dealing with rate limit configurations (both default and per-identifier).
type RateLimitConfigPayload struct {
	RateBps  int64 `json:"rate_bps"`  // RateBps is the rate in Bytes per second.
	BurstBps int64 `json:"burst_bps"` // BurstBps is the burst size in Bytes.
}

// handleGetDefaultRateLimit creates an HTTP handler function that returns the current
// default rate and burst limits from the provided BucketManager.
// It responds with a JSON RateLimitConfigPayload.
// Path: GET /config/ratelimit/default
func handleGetDefaultRateLimit(bm *ratelimit.BucketManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		rate, burst := bm.GetDefaultRateAndBurst()
		config := RateLimitConfigPayload{RateBps: rate, BurstBps: burst}
		respondWithJSON(w, http.StatusOK, config)
	}
}

// handleSetDefaultRateLimit creates an HTTP handler function that updates the
// default rate and burst limits in the provided BucketManager based on a JSON request body.
// Request Body: RateLimitConfigPayload
// Path: PUT /config/ratelimit/default
func handleSetDefaultRateLimit(bm *ratelimit.BucketManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		var config RateLimitConfigPayload
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
			return
		}
		defer r.Body.Close()

		if config.RateBps <= 0 || config.BurstBps <= 0 {
			respondWithError(w, http.StatusBadRequest, "Rate and burst must be positive values")
			return
		}

		bm.SetDefaultRateAndBurst(config.RateBps, config.BurstBps)
		log.Printf("Default rate limit updated: RateBps=%d, BurstBps=%d", config.RateBps, config.BurstBps)
		respondWithJSON(w, http.StatusOK, map[string]string{"message": "Default rate limit updated successfully."})
	}
}

// handleGetIdentifierRateLimit creates an HTTP handler function that returns the
// rate limit settings for a specific identifier from the BucketManager.
// The identifier is extracted from the URL path.
// Path: GET /config/ratelimit/identifier/{id}
func handleGetIdentifierRateLimit(bm *ratelimit.BucketManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		identifier := r.PathValue("id") // Requires Go 1.22+
		if identifier == "" {
			respondWithError(w, http.StatusBadRequest, "Identifier not provided in path")
			return
		}

		rate, burst, exists := bm.GetBucketSettings(identifier)
		if !exists {
			respondWithError(w, http.StatusNotFound, "Rate limit settings not found for identifier: "+identifier)
			return
		}

		config := RateLimitConfigPayload{RateBps: rate, BurstBps: burst}
		respondWithJSON(w, http.StatusOK, config)
	}
}

// handleSetIdentifierRateLimit creates an HTTP handler function that sets or updates
// the rate limit for a specific identifier in the BucketManager.
// The identifier is extracted from the URL path, and settings from a JSON request body.
// Request Body: RateLimitConfigPayload
// Path: PUT /config/ratelimit/identifier/{id}
func handleSetIdentifierRateLimit(bm *ratelimit.BucketManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		identifier := r.PathValue("id") // Requires Go 1.22+
		if identifier == "" {
			respondWithError(w, http.StatusBadRequest, "Identifier not provided in path")
			return
		}

		var config RateLimitConfigPayload
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
			return
		}
		defer r.Body.Close()

		if config.RateBps <= 0 || config.BurstBps <= 0 {
			respondWithError(w, http.StatusBadRequest, "Rate and burst must be positive values")
			return
		}

		bm.SetBucketRateAndBurst(identifier, config.RateBps, config.BurstBps)
		log.Printf("Rate limit for identifier '%s' updated: RateBps=%d, BurstBps=%d", identifier, config.RateBps, config.BurstBps)
		respondWithJSON(w, http.StatusOK, map[string]string{"message": "Rate limit for identifier '" + identifier + "' updated successfully."})
	}
}

// rateLimitedCopy implements a rate-limited data copy from src to dst.
// It reads data from src in chunks, waits for tokens from the provided TokenBucket,
// writes the chunk to dst, and records the transferred traffic using StatsCollector.
// clientIP, streamLabel, and remotePort are used for tagging traffic statistics.
// Returns the total number of bytes written and any error encountered.
func rateLimitedCopy(dst io.Writer, src io.Reader, bucket *ratelimit.TokenBucket, sc *trafficstats.StatsCollector, clientIP string, streamLabel string, remotePort int) (written int64, err error) {
	buf := make([]byte, copyChunkSize) // Buffer for reading chunks.
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

// handleConnection manages a single client TCP connection, proxying data to/from the target address.
// It applies rate limiting using the BucketManager and records traffic statistics via StatsCollector.
// clientConn: The accepted connection from a client.
// targetAddr: The address (host:port) of the backend server to proxy to.
// bm: The global BucketManager for retrieving or creating rate limiters.
// sc: The global StatsCollector for recording traffic data.
func handleConnection(clientConn net.Conn, targetAddr string, bm *ratelimit.BucketManager, sc *trafficstats.StatsCollector) {
	clientRemoteAddr := clientConn.RemoteAddr().String()
	clientIP, clientSourcePortStr, err := net.SplitHostPort(clientRemoteAddr)
	if err != nil {
		log.Printf("Error parsing client remote address %s: %v. Using full address as IP identifier.", clientRemoteAddr, err)
		clientIP = clientRemoteAddr // Fallback to using the full remote address string as an identifier.
	}
	clientSourcePort, err := strconv.Atoi(clientSourcePortStr)
	if err != nil {
		log.Printf("Error parsing client source port from %s: %v. Using 0 for stats.", clientSourcePortStr, err)
		clientSourcePort = 0 // Default to 0 if parsing fails.
	}

	log.Printf("[%s] Accepted connection from %s to proxy %s", clientIP, clientConn.LocalAddr(), targetAddr)
	defer func() {
		log.Printf("[%s] Closing client connection from %s.", clientIP, clientRemoteAddr)
		clientConn.Close()
	}()

	serverConn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		log.Printf("[%s] Failed to connect to target %s: %v", clientIP, targetAddr, err)
		return
	}
	log.Printf("[%s] Connected to target %s for client %s", clientIP, targetAddr, clientRemoteAddr)
	defer func() {
		log.Printf("[%s] Closing server connection to %s for client %s.", clientIP, targetAddr, clientRemoteAddr)
		serverConn.Close()
	}()

	_, targetPortStr, err := net.SplitHostPort(targetAddr)
	targetPort := 0 // Default target port for stats if parsing fails.
	if err != nil {
		log.Printf("[%s] Failed to parse target address %s for port: %v. Using 0 for stats.", clientIP, targetAddr, err)
	} else {
		targetPort, err = strconv.Atoi(targetPortStr)
		if err != nil {
			log.Printf("[%s] Failed to convert target port '%s' to int: %v. Using 0 for stats.", clientIP, targetPortStr, err)
			targetPort = 0
		}
	}

	// Create unique identifiers for upstream (client->server) and downstream (server->client) rate limit buckets.
	upBucketID := clientIP + "_up"
	downBucketID := clientIP + "_down"

	// Retrieve or create token buckets for the client's IP, specific to upload and download streams.
	clientUploadBucket := bm.GetBucket(upBucketID)
	clientDownloadBucket := bm.GetBucket(downBucketID)

	var wg sync.WaitGroup
	wg.Add(2) // Wait for two copy goroutines to complete.

	// Goroutine for client -> server (upload) traffic.
	go func() {
		defer wg.Done()
		// Attempt to half-close the write side of the server connection when client stops sending.
		if tcpConn, ok := serverConn.(*net.TCPConn); ok {
			defer tcpConn.CloseWrite()
		}
		log.Printf("[%s] Starting data copy from client %s to target %s (upload stream)", clientIP, clientRemoteAddr, serverConn.RemoteAddr())
		copiedBytes, copyErr := rateLimitedCopy(serverConn, clientConn, clientUploadBucket, sc, clientIP, "TCP_UP", targetPort)
		if copyErr != nil && copyErr != io.EOF {
			log.Printf("[%s] Error during client->server copy (upload): %v", clientIP, copyErr)
		}
		log.Printf("[%s] Finished copying from client %s to target %s (upload stream). Bytes: %d", clientIP, clientRemoteAddr, serverConn.RemoteAddr(), copiedBytes)
	}()

	// Goroutine for server -> client (download) traffic.
	go func() {
		defer wg.Done()
		// Attempt to half-close the write side of the client connection when server stops sending.
		if tcpConn, ok := clientConn.(*net.TCPConn); ok {
			defer tcpConn.CloseWrite()
		}
		log.Printf("[%s] Starting data copy from target %s to client %s (download stream)", clientIP, serverConn.RemoteAddr(), clientRemoteAddr)
		copiedBytes, copyErr := rateLimitedCopy(clientConn, serverConn, clientDownloadBucket, sc, clientIP, "TCP_DOWN", clientSourcePort)
		if copyErr != nil && copyErr != io.EOF {
			log.Printf("[%s] Error during server->client copy (download): %v", clientIP, copyErr)
		}
		log.Printf("[%s] Finished copying from target %s to client %s (download stream). Bytes: %d", clientIP, serverConn.RemoteAddr(), clientRemoteAddr, copiedBytes)
	}()

	log.Printf("[%s] Data transfer initiated for client %s. Waiting for completion...", clientIP, clientRemoteAddr)
	wg.Wait()
	log.Printf("[%s] Data transfer complete for client %s.", clientIP, clientRemoteAddr)
}

// main is the entry point for RabbitProxy.
// It initializes configurations, starts the TCP proxy listener, and an HTTP API server.
func main() {
	// Define and parse command-line flags.
	listenAddr := flag.String("listen", "localhost:8080", "Address and port for the TCP proxy to listen on.")
	targetAddr := flag.String("target", "localhost:8000", "Address and port of the target server to proxy connections to.")
	apiAddr := flag.String("api", defaultHTTPAPIListen, "Address and port for the HTTP API server.")
	rateMBps := flag.Float64("rate", defaultRateLimitMBps, "Default rate limit in Megabytes per second (MB/s) per client stream (up/down).")
	burstMB := flag.Float64("burst", defaultBurstLimitMB, "Default burst limit in Megabytes (MB) per client stream (up/down).")
	flag.Parse()

	// Initialize core components: BucketManager for rate limiting and StatsCollector for statistics.
	defaultRateBytes := int64(*rateMBps * 1024 * 1024)
	defaultBurstBytes := int64(*burstMB * 1024 * 1024)

	bucketManager := ratelimit.NewBucketManager(defaultRateBytes, defaultBurstBytes)
	statsCollector := trafficstats.NewStatsCollector(statsCollectionInterval, statsMaxAge, defaultStatMultiplier)
	defer statsCollector.Stop() // Ensure stats collector's run() goroutine is stopped on exit

	// Setup HTTP API server
	mux := http.NewServeMux()
	// Default rate limit handlers
	mux.HandleFunc("GET /config/ratelimit/default", handleGetDefaultRateLimit(bucketManager))
	mux.HandleFunc("PUT /config/ratelimit/default", handleSetDefaultRateLimit(bucketManager))
	// Specific identifier rate limit handlers
	mux.HandleFunc("GET /config/ratelimit/identifier/{id}", handleGetIdentifierRateLimit(bucketManager))
	mux.HandleFunc("PUT /config/ratelimit/identifier/{id}", handleSetIdentifierRateLimit(bucketManager))
	// Add more handlers here for specific bucket configs, etc.

	log.Printf("Starting HTTP API server on %s.", *apiAddr)
	go func() {
		// Start the HTTP server. http.ListenAndServe blocks until an error occurs.
		if err := http.ListenAndServe(*apiAddr, mux); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Fatal error starting HTTP API server on %s: %v", *apiAddr, err)
		}
	}()

	log.Printf("Starting TCP proxy. Listening on %s, Targeting %s", *listenAddr, *targetAddr)
	log.Printf("Default rate limit: %.2f MB/s, Default burst: %.2f MB per stream (used for new buckets)", *rateMBps, *burstMB)

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Failed to start TCP listener on %s: %v", *listenAddr, err)
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

	log.Println("Proxy shutting down.") // This log might only be reached if the accept loop exits.
}

// --- HTTP Helper Functions ---

// respondWithError is a utility function to send a JSON error response.
// It takes a ResponseWriter, an HTTP status code, and an error message string.
func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

// respondWithJSON is a utility function to send a JSON response.
// It takes a ResponseWriter, an HTTP status code, and a payload to be marshalled into JSON.
// It handles marshalling errors and sets the appropriate headers.
func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		// If marshalling fails, log the error and send a generic internal server error response.
		log.Printf("Error marshalling JSON response: %v", err)
		// Avoid calling respondWithError here to prevent potential infinite loop if map marshalling fails.
		http.Error(w, `{"error":"Internal server error during JSON marshalling"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response) // Send the marshalled JSON response.
}

// --- Stats API Handlers ---

// handleGetGlobalStats creates an HTTP handler function that retrieves and returns
// the global traffic statistics from the provided StatsCollector.
// Path: GET /stats/global
func handleGetGlobalStats(sc *trafficstats.StatsCollector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		stats := sc.GetGlobalStats()
		respondWithJSON(w, http.StatusOK, stats)
	}
}

// handleGetIPStats creates an HTTP handler function that retrieves and returns
// traffic statistics for a specific source IP address.
// The IP address is extracted from the URL path.
// Path: GET /stats/ip/{ip}
func handleGetIPStats(sc *trafficstats.StatsCollector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		ip := r.PathValue("ip") // Requires Go 1.22+
		if ip == "" {
			respondWithError(w, http.StatusBadRequest, "IP address not provided in path")
			return
		}
		stats, exists := sc.GetStatsBySourceIP(ip)
		if !exists {
			respondWithError(w, http.StatusNotFound, "No statistics found for IP: "+ip)
			return
		}
		respondWithJSON(w, http.StatusOK, stats)
	}
}

// handleGetProtocolStats creates an HTTP handler function that retrieves and returns
// traffic statistics for a specific protocol or stream label (e.g., "TCP_UP").
// The protocol label is extracted from the URL path.
// Path: GET /stats/protocol/{protocol}
func handleGetProtocolStats(sc *trafficstats.StatsCollector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		protocol := r.PathValue("protocol") // Requires Go 1.22+
		if protocol == "" {
			respondWithError(w, http.StatusBadRequest, "Protocol not provided in path")
			return
		}
		stats, exists := sc.GetStatsByProtocol(protocol)
		if !exists {
			respondWithError(w, http.StatusNotFound, "No statistics found for protocol: "+protocol)
			return
		}
		respondWithJSON(w, http.StatusOK, stats)
	}
}

// handleGetPortStats creates an HTTP handler function that retrieves and returns
// traffic statistics for a specific port number.
// The port number is extracted from the URL path and converted to an integer.
// Path: GET /stats/port/{port}
func handleGetPortStats(sc *trafficstats.StatsCollector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		portStr := r.PathValue("port") // Requires Go 1.22+
		if portStr == "" {
			respondWithError(w, http.StatusBadRequest, "Port not provided in path")
			return
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid port number: "+err.Error())
			return
		}
		stats, exists := sc.GetStatsByPort(port)
		if !exists {
			respondWithError(w, http.StatusNotFound, fmt.Sprintf("No statistics found for port: %d", port))
			return
		}
		respondWithJSON(w, http.StatusOK, stats)
	}
}

// handleGetTopNPorts creates an HTTP handler function that retrieves and returns
// a list of the top N ports, ranked by total bytes transferred.
// The number 'N' can be specified via a query parameter (e.g., "?n=5"), defaulting to 10.
// Path: GET /stats/ports/topn
func handleGetTopNPorts(sc *trafficstats.StatsCollector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respondWithError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		nStr := r.URL.Query().Get("n")
		n := 10 // Default to Top 10 if 'n' is not specified or invalid.
		if nStr != "" {
			var err error
			parsedN, err := strconv.Atoi(nStr)
			if err != nil || parsedN <= 0 {
				respondWithError(w, http.StatusBadRequest, "Invalid 'n' query parameter. Must be a positive integer.")
				return
			}
			n = parsedN
		}
		topPorts := sc.GetTopNPortsByBytes(n)
		// It's okay to return an empty list if no traffic or no ports meet criteria.
		respondWithJSON(w, http.StatusOK, topPorts)
	}
}

// Note: Usage of r.PathValue("id") / r.PathValue("ip") etc. requires Go 1.22+.
// If using an older Go version, path parameter extraction would need a different approach
// (e.g., string manipulation on r.URL.Path or a third-party router).
}
