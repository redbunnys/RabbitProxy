package trafficstats

import (
	"sort" // Added for GetTopNPortsByBytes
	"sync"
	"time"
	// "log" // Will add if logging is needed in run()
)

// StatItem represents a single recorded traffic event.
// It includes timing, volume, and optional metadata like source IP, protocol, and port.
type StatItem struct {
	Timestamp time.Time // Timestamp of when the traffic was recorded.
	Bytes     int64     // Number of bytes in this event (potentially adjusted by statisticalMultiplier).
	Packets   int64     // Number of packets in this event (potentially adjusted by statisticalMultiplier).
	SourceIP  string    // Source IP address of the traffic.
	Protocol  string    // Protocol or stream label (e.g., "TCP_UP", "TCP_DOWN").
	Port      int       // Destination or source port associated with the traffic.
}

// GlobalStats holds aggregated traffic statistics for a particular scope (e.g., global, per-IP, per-protocol).
type GlobalStats struct {
	TotalBytesForwarded   int64     // Total number of bytes forwarded.
	TotalPacketsForwarded int64     // Total number of packets forwarded.
	PeakBandwidth         float64   // Highest observed bandwidth in bytes per second.
	PeakBandwidthTimestamp time.Time // Timestamp when the peak bandwidth was observed.
	// Could add StartTime for uptime or average calculations.
}

// StatsCollector is responsible for collecting, aggregating, and providing access to traffic statistics.
// It handles global statistics as well as statistics broken down by source IP, protocol, and port.
// It also calculates real-time and peak bandwidth and manages a list of recent traffic events.
// The StatsCollector is thread-safe.
type StatsCollector struct {
	mu sync.RWMutex // Protects all fields below.

	globalStats     GlobalStats             // Aggregated statistics for all traffic.
	statsBySourceIP map[string]*GlobalStats // Statistics per source IP address.
	statsByProtocol map[string]*GlobalStats // Statistics per protocol/stream label.
	statsByPort     map[int]*GlobalStats    // Statistics per port number.

	// recentTraffic stores StatItems for a defined period, used for real-time bandwidth calculation.
	// It is periodically pruned.
	recentTraffic []StatItem

	statisticalMultiplier float64       // Multiplier applied to byte/packet counts (e.g., for sampled traffic).
	collectionInterval  time.Duration // Interval for periodic background tasks (pruning, peak bandwidth update).
	maxRecentTrafficAge time.Duration // Maximum age for items in recentTraffic before they are pruned.

	stopChan chan struct{}    // Signals the background goroutine to stop.
	wg       sync.WaitGroup // Waits for the background goroutine to finish.
}

// NewStatsCollector creates and starts a new StatsCollector.
// collectionInterval: Specifies how often background tasks (like pruning recent traffic and
//                     recalculating peak bandwidth) should run. If non-positive, a default is used.
// maxRecentAge: Defines the maximum age of traffic events to keep in the `recentTraffic` slice
//               for real-time bandwidth calculations. Events older than this are pruned.
//               If non-positive, a default is used.
// multiplier: A statistical multiplier applied to recorded byte and packet counts. Useful if
//             traffic is sampled. If non-positive, it defaults to 1.0 (no multiplication).
// Returns an initialized StatsCollector with its background processing goroutine started.
func NewStatsCollector(collectionInterval, maxRecentAge time.Duration, multiplier float64) *StatsCollector {
	if collectionInterval <= 0 {
		collectionInterval = 10 * time.Second // Default collection interval.
	}
	if maxRecentAge <= 0 {
		maxRecentAge = 1 * time.Minute // Default maximum age for recent traffic items.
	}
	if multiplier <= 0 {
		multiplier = 1.0 // Default multiplier (no change to raw counts).
	}

	sc := &StatsCollector{
		globalStats:         GlobalStats{},
		statsBySourceIP:     make(map[string]*GlobalStats),
		statsByProtocol:     make(map[string]*GlobalStats),
		statsByPort:         make(map[int]*GlobalStats),
		recentTraffic:       make([]StatItem, 0, 1024), // Pre-allocate capacity.
		statisticalMultiplier: multiplier,
		collectionInterval:  collectionInterval,
		maxRecentTrafficAge: maxRecentAge,
		stopChan:            make(chan struct{}),
	}

	sc.wg.Add(1)
	go sc.run() // Start the background goroutine.

	return sc
}

// RecordTraffic records a traffic event, updating relevant global and dimensional statistics.
// bytes: Number of bytes for this event (raw count before statistical multiplier).
// packets: Number of packets for this event (raw count before statistical multiplier).
// sourceIP: Source IP address string. If empty, not recorded for IP-specific stats.
// protocol: Protocol or stream label (e.g., "TCP_UP"). If empty, not recorded for protocol-specific stats.
// port: Port number. If non-positive, not recorded for port-specific stats.
// All byte and packet counts stored and aggregated are adjusted by the statisticalMultiplier.
// This method is thread-safe.
func (sc *StatsCollector) RecordTraffic(bytes int64, packets int64, sourceIP string, protocol string, port int) {
	sc.mu.Lock() // Ensure exclusive access for updating all stats fields.
	defer sc.mu.Unlock()

	now := time.Now()

	// Apply statistical multiplier
	adjustedBytes := int64(float64(bytes) * sc.statisticalMultiplier)
	adjustedPackets := int64(float64(packets) * sc.statisticalMultiplier)

	sc.globalStats.TotalBytesForwarded += adjustedBytes
	sc.globalStats.TotalPacketsForwarded += adjustedPackets

	// Note: recentTraffic stores raw (unmultiplied) bytes/packets for accurate
	// real-time bandwidth calculation if multiplier changes.
	// Or, store adjusted, but then GetRealTimeBandwidth needs to be aware.
	// For now, let's store raw in StatItem and apply multiplier for aggregated stats.
	// Re-evaluating: For consistency, perhaps StatItem should store the *adjusted* values
	// if all counters use adjusted values. Let's store adjusted values in StatItem as well.
	// This means GetRealTimeBandwidth will also reflect multiplied values.
	item := StatItem{
		Timestamp: now,
		Bytes:     adjustedBytes, // Storing adjusted bytes
		Packets:   adjustedPackets, // Storing adjusted packets
		SourceIP:  sourceIP,
		Protocol:  protocol,
		Port:      port,
	}
	sc.recentTraffic = append(sc.recentTraffic, item)

	// Update statsBySourceIP
	if sourceIP != "" {
		ipStats, exists := sc.statsBySourceIP[sourceIP]
		if !exists {
			ipStats = &GlobalStats{}
			sc.statsBySourceIP[sourceIP] = ipStats
		}
		ipStats.TotalBytesForwarded += adjustedBytes
		ipStats.TotalPacketsForwarded += adjustedPackets
	}

	// Update statsByProtocol
	if protocol != "" {
		protoStats, exists := sc.statsByProtocol[protocol]
		if !exists {
			protoStats = &GlobalStats{}
			sc.statsByProtocol[protocol] = protoStats
		}
		protoStats.TotalBytesForwarded += adjustedBytes
		protoStats.TotalPacketsForwarded += adjustedPackets
	}

	// Update statsByPort
	if port > 0 { // Assuming port 0 is not a valid port for tracking detailed stats.
		portStats, exists := sc.statsByPort[port]
		if !exists {
			portStats = &GlobalStats{} // Initialize if new.
			sc.statsByPort[port] = portStats
		}
		portStats.TotalBytesForwarded += adjustedBytes
		portStats.TotalPacketsForwarded += adjustedPackets
	}
}

// GetGlobalStats returns a copy of the current global traffic statistics.
// This method is thread-safe.
func (sc *StatsCollector) GetGlobalStats() GlobalStats {
	sc.mu.RLock() // Lock for reading globalStats.
	defer sc.mu.RUnlock()
	return sc.globalStats // Return a copy.
}

// GetRealTimeBandwidth calculates the current bandwidth in bytes per second based on
// traffic recorded in `recentTraffic` within the specified `window` duration from the present moment.
// window: The duration to look back for calculating bandwidth.
// Returns the calculated bandwidth in Bps.
// This method is thread-safe.
func (sc *StatsCollector) GetRealTimeBandwidth(window time.Duration) float64 {
	sc.mu.RLock() // Lock for reading recentTraffic.
	defer sc.mu.RUnlock()

	var totalBytesInWindow int64 = 0
	windowStartTime := time.Now().Add(-window)

	// Iterate from the end of recentTraffic
	for i := len(sc.recentTraffic) - 1; i >= 0; i-- {
		item := sc.recentTraffic[i]
		if item.Timestamp.Before(windowStartTime) {
			break // Item is older than the window
		}
		totalBytesInWindow += item.Bytes
	}

	if window.Seconds() == 0 {
		return 0 // Avoid division by zero
	}
	return float64(totalBytesInWindow) / window.Seconds()
}

// pruneRecentTraffic removes items from `recentTraffic` that are older than `sc.maxRecentTrafficAge`.
// This method must be called while holding sc.mu write lock.
func (sc *StatsCollector) pruneRecentTraffic() {
	cutoffTime := time.Now().Add(-sc.maxRecentTrafficAge)

	// Find the first index of an item that is NOT older than the cutoffTime.
	// This means all items before this index ARE older and should be pruned.
	firstValidIndex := 0
	for i, item := range sc.recentTraffic {
		if !item.Timestamp.Before(cutoffTime) {
			firstValidIndex = i
			break
		}
		// If all items are older, firstValidIndex will become len(sc.recentTraffic)
		if i == len(sc.recentTraffic)-1 && item.Timestamp.Before(cutoffTime) {
			firstValidIndex = len(sc.recentTraffic)
		}
	}

	if firstValidIndex > 0 {
		sc.recentTraffic = sc.recentTraffic[firstValidIndex:]
	}
}

// calculateAndUpdatePeakBandwidth recalculates the peak bandwidth based on the current
// `recentTraffic` data within the `maxRecentTrafficAge` window and updates
// `sc.globalStats.PeakBandwidth` if a new peak is found.
// This method must be called while holding sc.mu write lock.
func (sc *StatsCollector) calculateAndUpdatePeakBandwidth() {
	if len(sc.recentTraffic) == 0 {
		// Cannot calculate peak bandwidth if there's no recent traffic.
		return
	}

	var totalBytesInRecentWindow int64 = 0
	var actualWindowDuration time.Duration

	now := time.Now()
	windowStartTime := now.Add(-sc.maxRecentTrafficAge)

	firstItemTime := sc.recentTraffic[0].Timestamp
	if firstItemTime.After(windowStartTime) {
		// If the oldest item is newer than the start of our desired window,
		// the actual duration is from the oldest item to now.
		actualWindowDuration = now.Sub(firstItemTime)
	} else {
		actualWindowDuration = sc.maxRecentTrafficAge
	}

	for _, item := range sc.recentTraffic {
		// We only sum items that are within the maxRecentTrafficAge from 'now'.
		// Pruning should handle this, but this is an extra safeguard / handles the edge case
		// where pruning hasn't run yet for items slightly older than maxRecentTrafficAge.
		if !item.Timestamp.Before(windowStartTime) {
			totalBytesInRecentWindow += item.Bytes
		}
	}

	if actualWindowDuration.Seconds() > 0 {
		currentBandwidth := float64(totalBytesInRecentWindow) / actualWindowDuration.Seconds()
		if currentBandwidth > sc.globalStats.PeakBandwidth {
			sc.globalStats.PeakBandwidth = currentBandwidth
			sc.globalStats.PeakBandwidthTimestamp = now
		}
	}
}


// run is the background goroutine that performs periodic tasks such as pruning old traffic
// data and recalculating peak bandwidth. It stops when a signal is received on stopChan.
func (sc *StatsCollector) run() {
	defer sc.wg.Done()
	ticker := time.NewTicker(sc.collectionInterval) // Ticker for periodic execution.
	defer ticker.Stop()

	for {
		select {
		case <-sc.stopChan: // Stop signal received.
			return
		case <-ticker.C: // Timer ticked for periodic task execution.
			sc.mu.Lock()
			sc.pruneRecentTraffic()
			sc.calculateAndUpdatePeakBandwidth()
			sc.mu.Unlock()
			// log.Printf("StatsCollector: Performed periodic tasks. Recent traffic size: %d", len(sc.recentTraffic))
		}
	}
}

// Stop signals the StatsCollector's background goroutine to terminate and waits for it to complete.
// This should be called to ensure graceful shutdown.
func (sc *StatsCollector) Stop() {
	close(sc.stopChan) // Signal the run goroutine to stop.
	sc.wg.Wait()       // Wait for the run goroutine to finish.
}

// GetStatsBySourceIP returns a copy of the statistics for a specific source IP address.
// ip: The source IP address string to query.
// Returns the GlobalStats for the IP and a boolean indicating if the IP was found.
// This method is thread-safe.
func (sc *StatsCollector) GetStatsBySourceIP(ip string) (GlobalStats, bool) {
	sc.mu.RLock() // Lock for reading statsBySourceIP map.
	defer sc.mu.RUnlock()
	stats, exists := sc.statsBySourceIP[ip]
	if !exists {
		return GlobalStats{}, false
	}
	return *stats, true // Return a copy of the stats struct.
}

// GetAllStatsBySourceIP returns a map containing copies of statistics for all source IPs tracked.
// The keys of the map are IP addresses.
// This method is thread-safe.
func (sc *StatsCollector) GetAllStatsBySourceIP() map[string]GlobalStats {
	sc.mu.RLock() // Lock for reading statsBySourceIP map.
	defer sc.mu.RUnlock()
	allStats := make(map[string]GlobalStats, len(sc.statsBySourceIP))
	for ip, statsPtr := range sc.statsBySourceIP {
		allStats[ip] = *statsPtr // Dereference pointer to make a copy.
	}
	return allStats
}

// GetStatsByProtocol returns a copy of the statistics for a specific protocol or stream label.
// protocol: The protocol/stream label string to query (e.g., "TCP_UP").
// Returns the GlobalStats for the protocol and a boolean indicating if the protocol was found.
// This method is thread-safe.
func (sc *StatsCollector) GetStatsByProtocol(protocol string) (GlobalStats, bool) {
	sc.mu.RLock() // Lock for reading statsByProtocol map.
	defer sc.mu.RUnlock()
	stats, exists := sc.statsByProtocol[protocol]
	if !exists {
		return GlobalStats{}, false
	}
	return *stats, true // Return a copy of the stats struct.
}

// GetAllStatsByProtocol returns a map containing copies of statistics for all protocols/stream labels tracked.
// The keys of the map are protocol/stream label strings.
// This method is thread-safe.
func (sc *StatsCollector) GetAllStatsByProtocol() map[string]GlobalStats {
	sc.mu.RLock() // Lock for reading statsByProtocol map.
	defer sc.mu.RUnlock()
	allStats := make(map[string]GlobalStats, len(sc.statsByProtocol))
	for protocol, statsPtr := range sc.statsByProtocol {
		allStats[protocol] = *statsPtr // Dereference pointer to make a copy.
	}
	return allStats
}

// GetStatsByPort returns a copy of the statistics for a specific port number.
// port: The port number to query.
// Returns the GlobalStats for the port and a boolean indicating if the port was found.
// This method is thread-safe.
func (sc *StatsCollector) GetStatsByPort(port int) (GlobalStats, bool) {
	sc.mu.RLock() // Lock for reading statsByPort map.
	defer sc.mu.RUnlock()
	stats, exists := sc.statsByPort[port]
	if !exists {
		return GlobalStats{}, false
	}
	return *stats, true // Return a copy of the stats struct.
}

// GetAllStatsByPort returns a map containing copies of statistics for all port numbers tracked.
// The keys of the map are port numbers.
// This method is thread-safe.
func (sc *StatsCollector) GetAllStatsByPort() map[int]GlobalStats {
	sc.mu.RLock() // Lock for reading statsByPort map.
	defer sc.mu.RUnlock()
	allStats := make(map[int]GlobalStats, len(sc.statsByPort))
	for port, statsPtr := range sc.statsByPort {
		allStats[port] = *statsPtr // Dereference pointer to make a copy.
	}
	return allStats
}

// PortStat is a helper struct used for returning Top-N port statistics.
// It pairs a port number with its corresponding GlobalStats.
type PortStat struct {
	Port  int         // The port number.
	Stats GlobalStats // The statistics associated with this port.
}

// GetTopNPortsByBytes returns a slice of PortStat for the top N ports,
// sorted in descending order by their TotalBytesForwarded.
// n: The maximum number of top ports to return. If n is non-positive, an empty slice is returned.
//    If n is larger than the number of tracked ports, all ports are returned, sorted.
// This method is thread-safe.
func (sc *StatsCollector) GetTopNPortsByBytes(n int) []PortStat {
	sc.mu.RLock() // Lock for reading statsByPort map.
	defer sc.mu.RUnlock()

	if n <= 0 {
		return []PortStat{}
	}

	allPortStats := make([]PortStat, 0, len(sc.statsByPort))
	for port, stats := range sc.statsByPort {
		allPortStats = append(allPortStats, PortStat{Port: port, Stats: *stats})
	}

	// Sort slice in descending order of TotalBytesForwarded
	// sort.Slice is available in Go 1.8+
	// For older versions, sort.Sort with a custom type implementing sort.Interface would be needed.
	// Assuming Go 1.8+ for sort.Slice.
	// Let's import "sort"
	// import "sort" // Will need to add this to imports at the top of the file.
	// For now, I'll write the sort logic manually for the diff, then add import later.
	// This is a placeholder for actual sort call.
	// sort.Slice(allPortStats, func(i, j int) bool {
	// 	return allPortStats[i].Stats.TotalBytesForwarded > allPortStats[j].Stats.TotalBytesForwarded
	// })

	// Manual sort for now (less efficient than sort.Slice but avoids import issue mid-flow)
	// This is just to illustrate the logic, will be replaced by sort.Slice.
	// This will be very inefficient for large number of ports.
	// For the sake of this step, I will assume sort.Slice can be used and will adjust imports later.
	// The actual sorting will be done using sort.Slice from the "sort" package.
	// This requires adding "sort" to the imports.
	// For now, the tool doesn't allow adding imports directly mid-subtask easily with replace_with_git_merge_diff to the top.
	// I will add the sort logic conceptually, and it should be understood that `sort.Slice` is the way.
	// The following is a simplified representation of getting top N.
	// A proper sort is needed.
	// For the purpose of this tool, I will skip implementing the sort directly in this diff
	// and assume it will be added when imports can be handled.
	// The important part is the structure and intent.

	// To make this runnable, I will use a simple bubble sort for now.
	// Using sort.Slice for efficient sorting.
	sort.Slice(allPortStats, func(i, j int) bool {
		return allPortStats[i].Stats.TotalBytesForwarded > allPortStats[j].Stats.TotalBytesForwarded
	})

	if n > len(allPortStats) {
		n = len(allPortStats) // Adjust n if it's larger than the number of available stats.
	}
	return allPortStats[:n] // Return the top N (or fewer) items.
}

// SetStatisticalMultiplier updates the statistical multiplier used for adjusting recorded
// byte and packet counts.
// multiplier: The new statistical multiplier. If non-positive, it defaults to 1.0.
// This method is thread-safe.
func (sc *StatsCollector) SetStatisticalMultiplier(multiplier float64) {
	sc.mu.Lock() // Lock for writing statisticalMultiplier.
	defer sc.mu.Unlock()
	if multiplier <= 0 {
		sc.statisticalMultiplier = 1.0 // Default to 1.0 if an invalid multiplier is provided.
		return
	}
	sc.statisticalMultiplier = multiplier
}
