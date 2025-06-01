package ratelimit

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

const (
	bmDefaultRate  int64 = 100
	bmDefaultBurst int64 = 200
	bmTestID1            = "test_id_1"
	bmTestID2            = "test_id_2"
)

// TestNewBucketManager tests the constructor.
func TestNewBucketManager(t *testing.T) {
	t.Parallel()

	// Test with valid inputs
	bm := NewBucketManager(bmDefaultRate, bmDefaultBurst)
	if bm == nil {
		t.Fatal("NewBucketManager returned nil for valid inputs")
	}
	if bm.defaultRate != bmDefaultRate || bm.defaultBurst != bmDefaultBurst {
		t.Errorf("Expected default rate/burst (%d, %d), got (%d, %d)",
			bmDefaultRate, bmDefaultBurst, bm.defaultRate, bm.defaultBurst)
	}
	if bm.buckets == nil {
		t.Error("Buckets map not initialized")
	}

	// Test with invalid inputs (0 or negative)
	// Constructor defaults: rate=10, burst=rate*2 if invalid
	bmInvalid1 := NewBucketManager(0, 0)
	if bmInvalid1.defaultRate != 10 || bmInvalid1.defaultBurst != 20 {
		t.Errorf("Expected fallback defaults (10, 20) for (0,0), got (%d, %d)",
			bmInvalid1.defaultRate, bmInvalid1.defaultBurst)
	}
	bmInvalid2 := NewBucketManager(-1, -50)
	if bmInvalid2.defaultRate != 10 || bmInvalid2.defaultBurst != 20 {
		t.Errorf("Expected fallback defaults (10, 20) for (-1,-50), got (%d, %d)",
			bmInvalid2.defaultRate, bmInvalid2.defaultBurst)
	}
	bmInvalid3 := NewBucketManager(50, 0) // Valid rate, invalid burst
	if bmInvalid3.defaultRate != 50 || bmInvalid3.defaultBurst != 100 { // burst = rate * 2
		t.Errorf("Expected defaults (50, 100) for (50,0), got (%d, %d)",
			bmInvalid3.defaultRate, bmInvalid3.defaultBurst)
	}
}

// TestBucketManager_GetBucket tests retrieving/creating buckets with default settings.
func TestBucketManager_GetBucket(t *testing.T) {
	t.Parallel()
	bm := NewBucketManager(bmDefaultRate, bmDefaultBurst)

	// 1. Get bucket for new identifier
	b1 := bm.GetBucket(bmTestID1)
	if b1 == nil {
		t.Fatalf("GetBucket for new ID %s returned nil", bmTestID1)
	}
	r, bu := b1.GetRateAndBurst()
	if r != bmDefaultRate || bu != bmDefaultBurst {
		t.Errorf("Bucket for new ID %s: expected rate/burst (%d,%d), got (%d,%d)",
			bmTestID1, bmDefaultRate, bmDefaultBurst, r, bu)
	}

	// 2. Get bucket for existing identifier
	b1Again := bm.GetBucket(bmTestID1)
	if b1Again != b1 {
		t.Errorf("GetBucket for existing ID %s returned a different instance", bmTestID1)
	}

	// 3. Concurrent calls
	var wg sync.WaitGroup
	numGoroutines := 50
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id string) {
			defer wg.Done()
			bucket := bm.GetBucket(id)
			if bucket == nil {
				t.Errorf("Concurrent GetBucket for ID %s returned nil", id)
			}
			// All goroutines asking for bmTestID2 should get the same instance
			if id == bmTestID2 {
				// To verify they get same instance, we'd need to store first instance of bmTestID2
				// This is harder to check without external synchronization.
				// What we can check is that settings are consistent.
				r, bu := bucket.GetRateAndBurst()
				if r != bmDefaultRate || bu != bmDefaultBurst {
					t.Errorf("Concurrent GetBucket for ID %s: expected settings (%d,%d), got (%d,%d)",
						id, bmDefaultRate, bmDefaultBurst, r, bu)
				}
			}
		}(fmt.Sprintf("concurrent_id_%d", i%5)) // Mix of new and potentially same IDs
	}
	wg.Wait()
}

// TestBucketManager_GetOrCreateBucket tests retrieving/creating with specific settings.
func TestBucketManager_GetOrCreateBucket(t *testing.T) {
	t.Parallel()
	bm := NewBucketManager(bmDefaultRate, bmDefaultBurst)
	specRate, specBurst := int64(50), int64(75)

	// 1. Create bucket with specific rate/burst for new ID
	b1 := bm.GetOrCreateBucket(bmTestID1, specRate, specBurst)
	if b1 == nil {
		t.Fatalf("GetOrCreateBucket for new ID %s returned nil", bmTestID1)
	}
	r, bu := b1.GetRateAndBurst()
	if r != specRate || bu != specBurst {
		t.Errorf("Bucket for new ID %s with specific settings: expected (%d,%d), got (%d,%d)",
			bmTestID1, specRate, specBurst, r, bu)
	}

	// 2. Get existing bucket - should NOT update rate/burst
	b1Again := bm.GetOrCreateBucket(bmTestID1, bmDefaultRate, bmDefaultBurst) // Try with different rates
	if b1Again != b1 {
		t.Errorf("GetOrCreateBucket for existing ID %s returned a different instance", bmTestID1)
	}
	rExisting, buExisting := b1Again.GetRateAndBurst()
	if rExisting != specRate || buExisting != specBurst { // Should still have original specRate/specBurst
		t.Errorf("Existing bucket for ID %s: settings changed. Expected (%d,%d), got (%d,%d)",
			bmTestID1, specRate, specBurst, rExisting, buExisting)
	}

	// 3. Create with invalid specific rate/burst (should use defaults)
	b2 := bm.GetOrCreateBucket(bmTestID2, 0, 0)
	r2, bu2 := b2.GetRateAndBurst()
	if r2 != bmDefaultRate || bu2 != bmDefaultBurst {
		t.Errorf("Bucket for ID %s with invalid specific settings: expected defaults (%d,%d), got (%d,%d)",
			bmTestID2, bmDefaultRate, bmDefaultBurst, r2, bu2)
	}
}

// TestBucketManager_DefaultRateAndBurstSettings tests Get/Set for default rate/burst.
func TestBucketManager_DefaultRateAndBurstSettings(t *testing.T) {
	t.Parallel()
	bm := NewBucketManager(bmDefaultRate, bmDefaultBurst)

	// 1. Get initial defaults
	r, bu := bm.GetDefaultRateAndBurst()
	if r != bmDefaultRate || bu != bmDefaultBurst {
		t.Errorf("Initial GetDefaultRateAndBurst: expected (%d,%d), got (%d,%d)",
			bmDefaultRate, bmDefaultBurst, r, bu)
	}

	// 2. Set new defaults
	newDefRate, newDefBurst := int64(150), int64(300)
	bm.SetDefaultRateAndBurst(newDefRate, newDefBurst)
	r, bu = bm.GetDefaultRateAndBurst()
	if r != newDefRate || bu != newDefBurst {
		t.Errorf("After SetDefaultRateAndBurst: expected (%d,%d), got (%d,%d)",
			newDefRate, newDefBurst, r, bu)
	}

	// 3. Verify new buckets use new defaults
	bNew := bm.GetBucket("new_after_set_default")
	rNew, buNew := bNew.GetRateAndBurst()
	if rNew != newDefRate || buNew != newDefBurst {
		t.Errorf("New bucket after SetDefault: expected rates (%d,%d), got (%d,%d)",
			newDefRate, newDefBurst, rNew, buNew)
	}

	// 4. Verify existing buckets are NOT affected
	bm.GetBucket(bmTestID1) // Ensure bmTestID1 exists with old defaults
	bExisting := bm.GetBucket(bmTestID1)
	// This test setup is slightly flawed: bmTestID1 wasn't created BEFORE SetDefaultRateAndBurst.
	// Let's fix this:
	bm2 := NewBucketManager(10, 20)
	bPre := bm2.GetBucket("pre_existing_bucket")
	rPreOrig, buPreOrig := bPre.GetRateAndBurst() // Should be 10, 20

	bm2.SetDefaultRateAndBurst(500, 1000) // Change defaults

	rPreAfter, buPreAfter := bPre.GetRateAndBurst() // Check pre_existing_bucket again
	if rPreAfter != rPreOrig || buPreAfter != buPreOrig {
		t.Errorf("Existing bucket settings changed after SetDefaultRateAndBurst. Expected (%d,%d), got (%d,%d)",
			rPreOrig, buPreOrig, rPreAfter, buPreAfter)
	}


	// 5. Test SetDefaultRateAndBurst with invalid inputs (should not change)
	bm.SetDefaultRateAndBurst(newDefRate, newDefBurst) // reset to known state
	bm.SetDefaultRateAndBurst(0, 0)
	r, bu = bm.GetDefaultRateAndBurst()
	if r != newDefRate || bu != newDefBurst {
		t.Errorf("SetDefaultRateAndBurst(0,0) changed values. Expected (%d,%d), got (%d,%d)",
			newDefRate, newDefBurst, r, bu)
	}
	bm.SetDefaultRateAndBurst(-1, newDefBurst) // invalid rate
	r, bu = bm.GetDefaultRateAndBurst()
	if r != newDefRate || bu != newDefBurst {
		t.Errorf("SetDefaultRateAndBurst(-1, valid) changed values. Expected (%d,%d), got (%d,%d)",
			newDefRate, newDefBurst, r, bu)
	}
}

// TestBucketManager_SpecificBucketSettings tests Get/Set for specific bucket's rate/burst.
func TestBucketManager_SpecificBucketSettings(t *testing.T) {
	t.Parallel()
	bm := NewBucketManager(bmDefaultRate, bmDefaultBurst)

	// 1. Set rate/burst for a new identifier
	newID := "specific_new_id"
	specRate, specBurst := int64(55), int64(110)
	bm.SetBucketRateAndBurst(newID, specRate, specBurst)

	r, b, exists := bm.GetBucketSettings(newID)
	if !exists {
		t.Fatalf("Bucket %s should exist after SetBucketRateAndBurst", newID)
	}
	if r != specRate || b != specBurst {
		t.Errorf("Bucket %s: expected settings (%d,%d), got (%d,%d)", newID, specRate, specBurst, r, b)
	}

	// 2. Update rate/burst for an existing identifier
	updatedRate, updatedBurst := int64(66), int64(132)
	bm.SetBucketRateAndBurst(newID, updatedRate, updatedBurst)
	r, b, exists = bm.GetBucketSettings(newID)
	if !exists {
		t.Fatalf("Bucket %s should still exist after update", newID)
	}
	if r != updatedRate || b != updatedBurst {
		t.Errorf("Bucket %s after update: expected (%d,%d), got (%d,%d)", newID, updatedRate, updatedBurst, r, b)
	}

	// 3. Verify default settings are not affected
	defR, defB := bm.GetDefaultRateAndBurst()
	if defR != bmDefaultRate || defB != bmDefaultBurst {
		t.Errorf("Default settings changed. Expected (%d,%d), got (%d,%d)", bmDefaultRate, bmDefaultBurst, defR, defB)
	}

	// 4. Test SetBucketRateAndBurst with invalid values (should not apply)
	bm.SetBucketRateAndBurst(newID, specRate, specBurst) // Known state
	bm.SetBucketRateAndBurst(newID, 0, 0) // Invalid set
	r, b, _ = bm.GetBucketSettings(newID)
	if r != specRate || b != specBurst { // Should remain unchanged from specRate, specBurst
		t.Errorf("SetBucketRateAndBurst(0,0) for ID %s changed settings. Expected (%d,%d), got (%d,%d)",
			newID, specRate, specBurst, r, b)
	}

	// 5. Test GetBucketSettings for non-existent bucket
	_, _, exists = bm.GetBucketSettings("non_existent_id")
	if exists {
		t.Error("GetBucketSettings for non-existent ID should return exists == false")
	}
}

// TestBucketManager_Concurrency tests mixed concurrent operations.
func TestBucketManager_Concurrency(t *testing.T) {
	t.Parallel()
	bm := NewBucketManager(10, 20) // Low defaults to see if they are overridden

	var wg sync.WaitGroup
	numOps := 200 // Number of operations in total

	wg.Add(numOps)
	for i := 0; i < numOps; i++ {
		opType := i % 5
		idNum := i % 10 // Create some contention on same IDs
		identifier := fmt.Sprintf("concurrent_id_%d", idNum)

		switch opType {
		case 0: // GetBucket
			go func(id string) {
				defer wg.Done()
				b := bm.GetBucket(id)
				if b == nil {
					t.Errorf("GetBucket(%s) returned nil", id)
					return
				}
				// Further checks could be done, but might make test too complex if settings change
			}(identifier)
		case 1: // GetOrCreateBucket with specific settings
			go func(id string, opIdx int) {
				defer wg.Done()
				// Use opIdx to vary rates a bit
				rate := int64(100 + opIdx)
				burst := int64(200 + opIdx)
				b := bm.GetOrCreateBucket(id, rate, burst)
				if b == nil {
					t.Errorf("GetOrCreateBucket(%s, %d, %d) returned nil", id, rate, burst)
				}
			}(identifier, i)
		case 2: // SetBucketRateAndBurst
			go func(id string, opIdx int) {
				defer wg.Done()
				rate := int64(50 + opIdx)
				burst := int64(100 + opIdx)
				bm.SetBucketRateAndBurst(id, rate, burst)
				// Check if settings took, or if it was created
				r,b,exists := bm.GetBucketSettings(id)
				if !exists {
					t.Errorf("SetBucketRateAndBurst(%s) failed, bucket does not exist", id)
				} else if r != rate || b != burst {
					// This check can be racy if another SetBucketRateAndBurst runs for same ID.
					// For a basic check, we just ensure it runs without panicking due to race.
					// log.Printf("Possible race in check for SetBucketRateAndBurst: ID %s, expected (%d,%d), got (%d,%d)", id, rate, burst, r,b)
				}
			}(identifier, i)
		case 3: // GetBucketSettings
			go func(id string) {
				defer wg.Done()
				bm.GetBucketSettings(id) // Just call it, ensure no race
			}(identifier)
		case 4: // Set/Get DefaultRateAndBurst
			go func(opIdx int) {
				defer wg.Done()
				if opIdx % 2 == 0 {
					bm.SetDefaultRateAndBurst(int64(10+opIdx), int64(20+opIdx*2))
				} else {
					bm.GetDefaultRateAndBurst()
				}
			}(i)
		}
	}
	wg.Wait()

	// Final check: try to get one of the buckets and see if it has plausible settings
	// This is not a strict check due to concurrency, but ensures manager is in a sane state.
	r, b, exists := bm.GetBucketSettings("concurrent_id_0")
	if !exists {
		t.Error("Expected bucket concurrent_id_0 to exist after concurrent ops")
	} else {
		t.Logf("Settings for concurrent_id_0 after test: rate=%d, burst=%d (expect varied values)", r, b)
		if r <=0 || b <=0 {
			t.Errorf("Bucket concurrent_id_0 has invalid rate/burst: %d/%d", r,b)
		}
	}
}
