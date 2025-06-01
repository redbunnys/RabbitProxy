package trafficstats

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// Helper to create a StatsCollector with specific test-friendly intervals.
func newTestStatsCollector(t *testing.T, collectionInterval, maxRecentAge time.Duration, multiplier float64) *StatsCollector {
	if collectionInterval == 0 {
		collectionInterval = 50 * time.Millisecond // Short interval for testing
	}
	if maxRecentAge == 0 {
		maxRecentAge = 200 * time.Millisecond // Short age limit for testing pruning
	}
	if multiplier == 0 {
		multiplier = 1.0
	}
	sc := NewStatsCollector(collectionInterval, maxRecentAge, multiplier)
	// Ensure Stop is called to clean up the goroutine
	t.Cleanup(func() {
		sc.Stop()
	})
	return sc
}

func TestNewStatsCollector(t *testing.T) {
	t.Parallel()
	sc := newTestStatsCollector(t, 0, 0, 0) // Use defaults from helper

	if sc.globalStats.TotalBytesForwarded != 0 || sc.globalStats.TotalPacketsForwarded != 0 {
		t.Error("GlobalStats not initialized to zero")
	}
	if sc.statsBySourceIP == nil || sc.statsByProtocol == nil || sc.statsByPort == nil {
		t.Error("Dimension maps not initialized")
	}
	if sc.recentTraffic == nil {
		t.Error("recentTraffic slice not initialized")
	}
	if sc.statisticalMultiplier != 1.0 { // Default from helper if 0 passed
		t.Errorf("Expected default multiplier 1.0, got %f", sc.statisticalMultiplier)
	}

	// Test Stop immediately to ensure goroutine exits
	scTestStop := NewStatsCollector(50*time.Millisecond, 200*time.Millisecond, 1.0)
	stopCalled := make(chan bool)
	go func() {
		scTestStop.Stop()
		close(stopCalled)
	}()
	select {
	case <-stopCalled:
		// successfully stopped
	case <-time.After(1 * time.Second): // Timeout for stop
		t.Error("StatsCollector.Stop() timed out")
	}
}

func TestStatsCollector_RecordTraffic_And_Getters(t *testing.T) {
	t.Parallel()
	sc := newTestStatsCollector(t, 0, 0, 2.0) // Multiplier of 2.0

	// Record first event
	sc.RecordTraffic(100, 1, "1.1.1.1", "TCP", 80)
	// Adjusted: 200 bytes, 2 packets

	// Global Stats
	gs := sc.GetGlobalStats()
	if gs.TotalBytesForwarded != 200 || gs.TotalPacketsForwarded != 2 {
		t.Errorf("GlobalStats: expected (200, 2), got (%d, %d)", gs.TotalBytesForwarded, gs.TotalPacketsForwarded)
	}

	// IP Stats
	ipStats, exists := sc.GetStatsBySourceIP("1.1.1.1")
	if !exists || ipStats.TotalBytesForwarded != 200 || ipStats.TotalPacketsForwarded != 2 {
		t.Errorf("IPStats for 1.1.1.1: expected (200, 2), got (%v, %v)", exists, ipStats)
	}
	allIpStats := sc.GetAllStatsBySourceIP()
	if len(allIpStats) != 1 || allIpStats["1.1.1.1"].TotalBytesForwarded != 200 {
		t.Errorf("GetAllStatsBySourceIP unexpected result: %v", allIpStats)
	}


	// Protocol Stats
	protoStats, exists := sc.GetStatsByProtocol("TCP")
	if !exists || protoStats.TotalBytesForwarded != 200 || protoStats.TotalPacketsForwarded != 2 {
		t.Errorf("ProtoStats for TCP: expected (200, 2), got (%v, %v)", exists, protoStats)
	}
	allProtoStats := sc.GetAllStatsByProtocol()
	if len(allProtoStats) != 1 || allProtoStats["TCP"].TotalBytesForwarded != 200 {
		t.Errorf("GetAllStatsByProtocol unexpected result: %v", allProtoStats)
	}

	// Port Stats
	portStats, exists := sc.GetStatsByPort(80)
	if !exists || portStats.TotalBytesForwarded != 200 || portStats.TotalPacketsForwarded != 2 {
		t.Errorf("PortStats for 80: expected (200, 2), got (%v, %v)", exists, portStats)
	}
	allPortStats := sc.GetAllStatsByPort()
	if len(allPortStats) != 1 || allPortStats[80].TotalBytesForwarded != 200 {
		t.Errorf("GetAllStatsByPort unexpected result: %v", allPortStats)
	}

	// Check recentTraffic (stores adjusted values)
	sc.mu.RLock()
	if len(sc.recentTraffic) != 1 || sc.recentTraffic[0].Bytes != 200 {
		t.Errorf("recentTraffic incorrect. Len: %d, Item: %+v", len(sc.recentTraffic), sc.recentTraffic)
	}
	sc.mu.RUnlock()


	// Record second event (different IP, same protocol, different port)
	sc.RecordTraffic(50, 1, "2.2.2.2", "TCP", 443)
	// Adjusted: 100 bytes, 2 packets

	gs = sc.GetGlobalStats() // Total: 300 bytes, 4 packets
	if gs.TotalBytesForwarded != 300 || gs.TotalPacketsForwarded != 4 {
		t.Errorf("GlobalStats after 2nd event: expected (300, 4), got (%d, %d)", gs.TotalBytesForwarded, gs.TotalPacketsForwarded)
	}
	ipStats2, _ := sc.GetStatsBySourceIP("2.2.2.2")
	if ipStats2.TotalBytesForwarded != 100 || ipStats2.TotalPacketsForwarded != 2 {
		t.Errorf("IPStats for 2.2.2.2: expected (100, 2), got %v", ipStats2)
	}
	protoStatsTCP, _ := sc.GetStatsByProtocol("TCP") // TCP total: 300 bytes, 4 packets
	if protoStatsTCP.TotalBytesForwarded != 300 || protoStatsTCP.TotalPacketsForwarded != 4 {
		t.Errorf("ProtoStats for TCP after 2nd event: expected (300, 4), got %v", protoStatsTCP)
	}
	portStats443, _ := sc.GetStatsByPort(443)
	if portStats443.TotalBytesForwarded != 100 || portStats443.TotalPacketsForwarded != 2 {
		t.Errorf("PortStats for 443: expected (100, 2), got %v", portStats443)
	}

	// Test Getters for non-existent keys
	_, exists = sc.GetStatsBySourceIP("0.0.0.0")
	if exists { t.Error("Expected exists=false for unknown IP") }
	_, exists = sc.GetStatsByProtocol("UDP")
	if exists { t.Error("Expected exists=false for unknown protocol") }
	_, exists = sc.GetStatsByPort(12345)
	if exists { t.Error("Expected exists=false for unknown port") }

	// Test copy behavior of GetGlobalStats
	gsCopy1 := sc.GetGlobalStats()
	sc.RecordTraffic(10,1,"","","",0) // Adjusted: 20 bytes, 2 packets
	gsCopy2 := sc.GetGlobalStats()
	if gsCopy1.TotalBytesForwarded == gsCopy2.TotalBytesForwarded {
		t.Error("GetGlobalStats should return a copy, not be affected by later records")
	}
}

func TestStatsCollector_GetRealTimeBandwidth(t *testing.T) {
	t.Parallel()
	// Using multiplier 1.0 for simpler bandwidth math
	sc := newTestStatsCollector(t, 50*time.Millisecond, 1*time.Second, 1.0)

	// Record 1000 bytes
	sc.RecordTraffic(1000, 1, "1.1.1.1", "TCP", 80)
	time.Sleep(60 * time.Millisecond) // Sleep for > 50ms but < 150ms

	// Window = 50ms, should only see current data if events are sparse
	// GetRealTimeBandwidth sums items *within* the window relative to time.Now().
	// If the single event is now older than 50ms, bandwidth will be 0 for that window.
	// Let's record a series of events.
	sc.RecordTraffic(1000, 1, "1.1.1.1", "TCP", 80) // t = 0ms (relative)
	time.Sleep(60 * time.Millisecond)              // t = 60ms
	sc.RecordTraffic(2000, 1, "1.1.1.1", "TCP", 80) // t = 60ms
	time.Sleep(60 * time.Millisecond)              // t = 120ms
	sc.RecordTraffic(3000, 1, "1.1.1.1", "TCP", 80) // t = 120ms

	// At t=120ms:
	// Event 1 (1000B) is at t=0 (120ms ago)
	// Event 2 (2000B) is at t=60ms (60ms ago)
	// Event 3 (3000B) is at t=120ms (0ms ago)

	// Window 50ms from Now (t=120ms): should only include Event 3 (3000B)
	// Bandwidth = 3000B / 0.05s = 60000 B/s
	bw1 := sc.GetRealTimeBandwidth(50 * time.Millisecond)
	// This calculation is tricky. GetRealTimeBandwidth sums items whose Timestamp is after (time.Now() - window).
	// If window is 50ms, it sums items from the last 50ms.
	// Event 3 is definitely in. Event 2 (60ms ago) is out. Event 1 (120ms ago) is out.
	// So, 3000 bytes / 0.05s = 60000 B/s.
	expectedBw1 := float64(3000) / (50 * time.Millisecond).Seconds()
	if bw1 != expectedBw1 {
		t.Errorf("GetRealTimeBandwidth(50ms): expected %.2f, got %.2f", expectedBw1, bw1)
	}

	// Window 100ms from Now (t=120ms): should include Event 3 (3000B) and Event 2 (2000B)
	// Total 5000B / 0.1s = 50000 B/s
	bw2 := sc.GetRealTimeBandwidth(100 * time.Millisecond)
	expectedBw2 := float64(3000+2000) / (100 * time.Millisecond).Seconds()
	if bw2 != expectedBw2 {
		t.Errorf("GetRealTimeBandwidth(100ms): expected %.2f, got %.2f", expectedBw2, bw2)
	}

	// Window 150ms from Now (t=120ms): should include Event 3, 2, and 1
	// Total 6000B / 0.15s = 40000 B/s
	bw3 := sc.GetRealTimeBandwidth(150 * time.Millisecond)
	expectedBw3 := float64(3000+2000+1000) / (150 * time.Millisecond).Seconds()
	if bw3 != expectedBw3 {
		t.Errorf("GetRealTimeBandwidth(150ms): expected %.2f, got %.2f", expectedBw3, bw3)
	}

	// Test with empty recentTraffic
	scEmpty := newTestStatsCollector(t, 0,0,1.0)
	if bwEmpty := scEmpty.GetRealTimeBandwidth(1*time.Second); bwEmpty != 0 {
		t.Errorf("Expected 0 bandwidth for empty stats, got %f", bwEmpty)
	}
}


func TestStatsCollector_BackgroundTasks_PruningAndPeak(t *testing.T) {
	// This test is time-sensitive and relies on the background goroutine running.
	// Use short intervals for testing.
	collectionInterval := 50 * time.Millisecond
	maxAge := 150 * time.Millisecond // Items older than this should be pruned
	sc := newTestStatsCollector(t, collectionInterval, maxAge, 1.0)

	// Record initial traffic
	sc.RecordTraffic(1000, 1, "1.1.1.1", "TCP", 80) // Event 1 @ t=0
	initialPeakBwTime := sc.GetGlobalStats().PeakBandwidthTimestamp // Should be ~now

	time.Sleep(collectionInterval / 2) // ~25ms
	sc.RecordTraffic(2000, 1, "1.1.1.1", "TCP", 80) // Event 2 @ t=25ms
	// At this point, recentTraffic has 2 items. Peak BW might update in background.

	// Wait for at least one collection interval to pass for pruning and peak calculation
	// Add a small buffer to ensure the ticker in run() has fired.
	time.Sleep(collectionInterval + (collectionInterval / 2)) // ~75ms from start of this block, total ~100ms from Event 1

	// Check Peak Bandwidth
	// After ~100ms: Event 1 (1000B), Event 2 (2000B).
	// calculateAndUpdatePeakBandwidth uses maxRecentTrafficAge (150ms) as its window.
	// Both events are within 150ms of now. Total bytes = 3000.
	// Actual window duration for calculation: time.Now() - firstItemTime
	// firstItemTime for Event 1. So duration is ~100ms.
	// Peak = 3000B / ~0.1s = ~30000 B/s
	gs := sc.GetGlobalStats()
	expectedPeak := float64(3000) / ( (time.Duration(100) * time.Millisecond).Seconds() ) // Approximate
	if gs.PeakBandwidth < expectedPeak*0.8 || gs.PeakBandwidth > expectedPeak*1.2 { // Allow 20% variance
		t.Errorf("PeakBandwidth expected around %.2f, got %.2f", expectedPeak, gs.PeakBandwidth)
	}
	if gs.PeakBandwidthTimestamp.Equal(initialPeakBwTime) && gs.TotalBytesForwarded > 1000 { // if more traffic came, peak time should update
		t.Logf("Initial peak time: %v, current peak time: %v", initialPeakBwTime, gs.PeakBandwidthTimestamp)
		// This check can be tricky if peak occurs at first event.
		// For this flow, peak should be updated when event 2 comes in or shortly after by background.
	}


	// Test Pruning: Wait longer than maxRecentTrafficAge + collectionInterval buffer
	// Total time elapsed since Event 1: ~100ms.
	// Event 1 is at t=0. Event 2 is at t=25ms.
	// maxAge is 150ms.
	// Need to sleep until Event 1 is older than 150ms. So, sleep for another ~60ms.
	// Current time is ~100ms from start. Event 1 is 100ms old. Event 2 is 75ms old.
	time.Sleep(maxAge) // Sleep for maxAge duration (150ms). Total time from start ~250ms.
	// Event 1 is now ~250ms old -> should be pruned.
	// Event 2 is now ~225ms old -> should be pruned.

	// Force another collection cycle for pruning
	time.Sleep(collectionInterval + (collectionInterval / 2)) // Ensure another run cycle

	sc.mu.RLock()
	if len(sc.recentTraffic) != 0 {
		t.Errorf("recentTraffic should be empty after pruning, got %d items. First item timestamp: %v",
			len(sc.recentTraffic), sc.recentTraffic[0].Timestamp)
		// For debugging:
		// for _, item := range sc.recentTraffic {
		// 	t.Logf("Remaining item: %+v, Age: %v", item, time.Since(item.Timestamp))
		// }
	}
	sc.mu.RUnlock()
}


func TestStatsCollector_GetTopNPortsByBytes(t *testing.T) {
	t.Parallel()
	sc := newTestStatsCollector(t, 0,0, 1.0)

	sc.RecordTraffic(100, 1, "1.1.1.1", "TCP", 80)    // Port 80: 100 bytes
	sc.RecordTraffic(300, 3, "1.1.1.1", "TCP", 443)   // Port 443: 300 bytes
	sc.RecordTraffic(50, 1, "1.1.1.1", "UDP", 53)    // Port 53: 50 bytes
	sc.RecordTraffic(200, 2, "1.1.1.1", "TCP", 8080)  // Port 8080: 200 bytes
	sc.RecordTraffic(150, 1, "1.1.1.1", "TCP", 80)    // Port 80: 100+150 = 250 bytes

	// Expected order by bytes: 443 (300), 80 (250), 8080 (200), 53 (50)

	top2 := sc.GetTopNPortsByBytes(2)
	if len(top2) != 2 {
		t.Fatalf("GetTopNPortsByBytes(2): expected 2 items, got %d", len(top2))
	}
	if top2[0].Port != 443 || top2[0].Stats.TotalBytesForwarded != 300 {
		t.Errorf("Top 1: expected port 443 (300B), got %d (%.0fB)", top2[0].Port, float64(top2[0].Stats.TotalBytesForwarded))
	}
	if top2[1].Port != 80 || top2[1].Stats.TotalBytesForwarded != 250 {
		t.Errorf("Top 2: expected port 80 (250B), got %d (%.0fB)", top2[1].Port, float64(top2[1].Stats.TotalBytesForwarded))
	}

	topAll := sc.GetTopNPortsByBytes(10) // Request more than available
	if len(topAll) != 4 {
		t.Fatalf("GetTopNPortsByBytes(10): expected 4 items, got %d", len(topAll))
	}
	if topAll[0].Port != 443 || topAll[1].Port != 80 || topAll[2].Port != 8080 || topAll[3].Port != 53 {
		t.Error("TopN all ports sorted incorrectly:")
		for i, ps := range topAll {
			t.Logf("  [%d] Port %d, Bytes %d", i, ps.Port, ps.Stats.TotalBytesForwarded)
		}
	}

	// Test with n=0
	top0 := sc.GetTopNPortsByBytes(0)
	if len(top0) != 0 {
		t.Errorf("GetTopNPortsByBytes(0) should return empty slice, got %d", len(top0))
	}
}

func TestStatsCollector_SetStatisticalMultiplier(t *testing.T) {
	t.Parallel()
	sc := newTestStatsCollector(t, 0,0, 1.0)

	sc.RecordTraffic(100, 1, "", "", 0) // Initial: 100 bytes (multiplier 1.0)
	gs1 := sc.GetGlobalStats()
	if gs1.TotalBytesForwarded != 100 {
		t.Errorf("Expected 100 bytes, got %d", gs1.TotalBytesForwarded)
	}

	sc.SetStatisticalMultiplier(3.0)
	if sc.statisticalMultiplier != 3.0 {
		t.Error("SetStatisticalMultiplier did not update value")
	}

	sc.RecordTraffic(100, 1, "", "", 0) // This 100 bytes should be multiplied by 3.0 -> 300
	// Total bytes = 100 (old) + 300 (new) = 400
	gs2 := sc.GetGlobalStats()
	if gs2.TotalBytesForwarded != 400 {
		t.Errorf("After multiplier update, expected 400 total bytes, got %d", gs2.TotalBytesForwarded)
	}

	// Test setting invalid multiplier (should default to 1.0)
	sc.SetStatisticalMultiplier(0)
	if sc.statisticalMultiplier != 1.0 {
		t.Errorf("Setting multiplier to 0 should default to 1.0, got %f", sc.statisticalMultiplier)
	}
	sc.SetStatisticalMultiplier(-5.0)
	if sc.statisticalMultiplier != 1.0 {
		t.Errorf("Setting multiplier to -5.0 should default to 1.0, got %f", sc.statisticalMultiplier)
	}
}

func TestStatsCollector_Concurrency(t *testing.T) {
	t.Parallel()
	sc := newTestStatsCollector(t, 50*time.Millisecond, 200*time.Millisecond, 1.0)
	numGoroutines := 100
	recordsPerG := 10
	bytesPerRecord := int64(10)
	packetsPerRecord := int64(1)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(gID int) {
			defer wg.Done()
			for j := 0; j < recordsPerG; j++ {
				ip := fmt.Sprintf("10.0.0.%d", gID%20) // Some overlap in IPs
				proto := "TCP"
				if j%2 == 0 {
					proto = "UDP"
				}
				port := 1000 + (j % 5)
				sc.RecordTraffic(bytesPerRecord, packetsPerRecord, ip, proto, port)
				// Interleave with some reads
				if j%3 == 0 {
					_ = sc.GetGlobalStats()
					_, _ = sc.GetStatsBySourceIP(ip)
					_ = sc.GetRealTimeBandwidth(100*time.Millisecond)
				}
			}
		}(i)
	}
	wg.Wait()

	expectedTotalBytes := int64(numGoroutines * recordsPerG) * bytesPerRecord
	expectedTotalPackets := int64(numGoroutines * recordsPerG) * packetsPerRecord

	gs := sc.GetGlobalStats()
	if gs.TotalBytesForwarded != expectedTotalBytes {
		t.Errorf("Concurrent GlobalStats: expected total bytes %d, got %d", expectedTotalBytes, gs.TotalBytesForwarded)
	}
	if gs.TotalPacketsForwarded != expectedTotalPackets {
		t.Errorf("Concurrent GlobalStats: expected total packets %d, got %d", expectedTotalPackets, gs.TotalPacketsForwarded)
	}

	// Wait for background tasks to run a few times to check for races there too
	time.Sleep(sc.collectionInterval * 3)
	_ = sc.GetGlobalStats() // Final check after background tasks
	t.Logf("Concurrency test finished, global bytes: %d", sc.GetGlobalStats().TotalBytesForwarded)
}

// Test to ensure GetAllStatsBy... methods return copies
func TestStatsCollector_GetAll_ReturnsCopy(t *testing.T) {
    t.Parallel()
    sc := newTestStatsCollector(t, 0, 0, 1.0)
    sc.RecordTraffic(100, 1, "1.1.1.1", "TCP", 80)

    // Test GetAllStatsBySourceIP
    allIpStats1 := sc.GetAllStatsBySourceIP()
    val1, ok1 := allIpStats1["1.1.1.1"]
    if !ok1 { t.Fatal("IP 1.1.1.1 not found in first map copy") }
    val1.TotalBytesForwarded = 999 // Modify copy

    allIpStats2 := sc.GetAllStatsBySourceIP()
    val2, ok2 := allIpStats2["1.1.1.1"]
    if !ok2 { t.Fatal("IP 1.1.1.1 not found in second map copy") }
    if val2.TotalBytesForwarded == 999 {
        t.Error("Modification of map returned by GetAllStatsBySourceIP affected internal state or subsequent calls.")
    }
    if val1.TotalBytesForwarded == val2.TotalBytesForwarded && val1.TotalBytesForwarded == 999 {
         // This means the internal map was modified.
    } else if val2.TotalBytesForwarded != 100 {
         t.Errorf("Expected 100 bytes for 1.1.1.1 in fresh copy, got %d", val2.TotalBytesForwarded)
    }


    // Test GetAllStatsByProtocol (similar logic)
	allProtoStats1 := sc.GetAllStatsByProtocol()
	pVal1, pOk1 := allProtoStats1["TCP"]
	if !pOk1 { t.Fatal("Protocol TCP not found in first map copy") }
	pVal1.TotalBytesForwarded = 888

	allProtoStats2 := sc.GetAllStatsByProtocol()
	pVal2, _ := allProtoStats2["TCP"]
    if pVal2.TotalBytesForwarded == 888 {
        t.Error("Modification of map returned by GetAllStatsByProtocol affected internal state.")
    }

    // Test GetAllStatsByPort (similar logic)
    allPortStats1 := sc.GetAllStatsByPort()
	portVal1, portOk1 := allPortStats1[80]
	if !portOk1 { t.Fatal("Port 80 not found in first map copy") }
	portVal1.TotalBytesForwarded = 777

	allPortStats2 := sc.GetAllStatsByPort()
	portVal2, _ := allPortStats2[80]
    if portVal2.TotalBytesForwarded == 777 {
        t.Error("Modification of map returned by GetAllStatsByPort affected internal state.")
    }
}

// Ensure sorting is correct for GetTopNPortsByBytes (imported sort)
func TestGetTopNPortsByBytes_Sorting(t *testing.T) {
    t.Parallel()
    sc := newTestStatsCollector(t, 0, 0, 1.0)
    ports := []int{80, 443, 22, 53, 8080}
    bytes := []int64{100, 500, 50, 200, 300}
    expectedOrder := []int{443, 8080, 53, 80, 22} // Expected: 500, 300, 200, 100, 50

    for i := range ports {
        sc.RecordTraffic(bytes[i], 1, "test", "tcp", ports[i])
    }

    // Manually sort expected for verification
    expectedPortStats := make([]PortStat, len(ports))
    for i := range ports {
        expectedPortStats[i] = PortStat{Port: ports[i], Stats: GlobalStats{TotalBytesForwarded: bytes[i]}}
    }
    sort.Slice(expectedPortStats, func(i, j int) bool {
        return expectedPortStats[i].Stats.TotalBytesForwarded > expectedPortStats[j].Stats.TotalBytesForwarded
    })


    topN := sc.GetTopNPortsByBytes(len(ports))
    if len(topN) != len(expectedPortStats) {
        t.Fatalf("Expected %d items, got %d", len(expectedPortStats), len(topN))
    }

    for i := range topN {
        if topN[i].Port != expectedPortStats[i].Port || topN[i].Stats.TotalBytesForwarded != expectedPortStats[i].Stats.TotalBytesForwarded {
            t.Errorf("Mismatch at index %d. Got Port %d (Bytes %d), Expected Port %d (Bytes %d)",
                i, topN[i].Port, topN[i].Stats.TotalBytesForwarded,
                expectedPortStats[i].Port, expectedPortStats[i].Stats.TotalBytesForwarded)
        }
    }
}
