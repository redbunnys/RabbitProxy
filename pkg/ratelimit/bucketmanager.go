package ratelimit

import (
	"sync"
	"time"
)

// BucketManager manages a collection of TokenBucket instances.
type BucketManager struct {
	buckets      map[string]*TokenBucket
	defaultRate  int64
	defaultBurst int64
	mu           sync.RWMutex
}

// NewBucketManager creates a new BucketManager.
func NewBucketManager(defaultRate, defaultBurst int64) *BucketManager {
	if defaultRate <= 0 || defaultBurst <= 0 {
		// Sensible defaults if invalid values are provided, or panic/error
		defaultRate = 10  // e.g., 10 tokens per second
		defaultBurst = 20 // e.g., burst of 20
	}
	return &BucketManager{
		buckets:      make(map[string]*TokenBucket),
		defaultRate:  defaultRate,
		defaultBurst: defaultBurst,
	}
}

// GetBucket retrieves an existing bucket for the given identifier.
// If the bucket doesn't exist, it creates a new one using the default rate and burst.
func (bm *BucketManager) GetBucket(identifier string) *TokenBucket {
	bm.mu.RLock()
	bucket, exists := bm.buckets[identifier]
	bm.mu.RUnlock()

	if exists {
		return bucket
	}

	// Bucket doesn't exist, acquire write lock to create it
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Double-check if another goroutine created it while we were waiting for the write lock
	bucket, exists = bm.buckets[identifier]
	if exists {
		return bucket
	}

	// Create and store the new bucket
	newBucket := NewTokenBucket(bm.defaultRate, bm.defaultBurst)
	bm.buckets[identifier] = newBucket
	return newBucket
}

// GetOrCreateBucket retrieves an existing bucket or creates one with specific rate and burst.
func (bm *BucketManager) GetOrCreateBucket(identifier string, rate, burst int64) *TokenBucket {
	if rate <= 0 || burst <= 0 {
		// Use defaults if provided specific rate/burst are invalid
		rate = bm.defaultRate
		burst = bm.defaultBurst
	}

	bm.mu.RLock()
	bucket, exists := bm.buckets[identifier]
	bm.mu.RUnlock()

	if exists {
		// Check if the existing bucket's rate and burst match the desired ones.
		// This part can be tricky: what if it exists but with different settings?
		// For now, we'll assume if it exists, we use it.
		// A more advanced version might update the existing bucket if settings differ.
		// Or, the identifier should perhaps incorporate rate/burst if they are immutable per identifier.
		// Let's check and if different, we might need to create a new one or update.
		// For now, if it exists, return it. If specific rate/burst are desired for an existing
		// bucket, SetRateAndBurst should be called explicitly by the user.
		// This simplifies GetOrCreateBucket to "get, or create if not present at all".
		return bucket
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Double-check, as in GetBucket
	bucket, exists = bm.buckets[identifier]
	if exists {
		// Similar to above, if it exists now, return it.
		// The decision of whether to update its rate/burst here is complex.
		// Let's assume we don't update if it was created by another goroutine in the meantime.
		return bucket
	}

	newBucket := NewTokenBucket(rate, burst)
	bm.buckets[identifier] = newBucket
	return newBucket
}

// CleanupStaleBuckets removes buckets that haven't been used for maxIdleTime.
// This is a placeholder. A full implementation requires TokenBucket to track its last access time.
func (bm *BucketManager) CleanupStaleBuckets(maxIdleTime time.Duration) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// For a real implementation:
	// now := time.Now()
	// for id, bucket := range bm.buckets {
	//   if bucket.IsIdle(now, maxIdleTime) { // IsIdle would be a new method on TokenBucket
	//     delete(bm.buckets, id)
	//   }
	// }
	//
	// As TokenBucket does not have IsIdle or last access tracking, this is a no-op for now.
	// fmt.Printf("CleanupStaleBuckets called, but not implemented due to missing TokenBucket.lastAccessTime. Max idle: %v\n", maxIdleTime)
}
