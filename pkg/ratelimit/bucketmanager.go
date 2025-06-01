package ratelimit

import (
	"sync"
	"time"
)

// BucketManager manages a collection of TokenBucket instances, identified by unique strings.
// It provides methods to get existing buckets or create new ones with default or specific settings.
// It is thread-safe.
type BucketManager struct {
	// buckets stores the TokenBucket instances, keyed by identifier.
	// Access to this map is protected by mu.
	buckets map[string]*TokenBucket

	// defaultRate is the rate (tokens per second) used for new buckets if not otherwise specified.
	// Protected by mu.
	defaultRate int64

	// defaultBurst is the burst size (max tokens) used for new buckets if not otherwise specified.
	// Protected by mu.
	defaultBurst int64

	// mu protects concurrent access to the buckets map and default rate/burst settings.
	mu sync.RWMutex
}

// NewBucketManager creates a new, empty BucketManager with specified default rate and burst settings.
// defaultRate: The default token generation rate per second for new buckets.
//              If non-positive, a fallback default (e.g., 10 tps) is used.
// defaultBurst: The default maximum token capacity for new buckets.
//               If non-positive, a fallback default (e.g., 2x the effective rate) is used.
func NewBucketManager(defaultRate, defaultBurst int64) *BucketManager {
	if defaultRate <= 0 {
		defaultRate = 10 // Fallback default rate if an invalid value is provided.
	}
	if defaultBurst <= 0 {
		defaultBurst = defaultRate * 2 // e.g., burst of 2x rate (adjust as needed)
	}
	return &BucketManager{
		buckets:      make(map[string]*TokenBucket),
		defaultRate:  defaultRate,
		defaultBurst: defaultBurst,
	}
}

// GetBucket retrieves an existing TokenBucket for the given identifier.
// If a bucket for the identifier does not exist, a new one is created using the manager's
// current defaultRate and defaultBurst settings, stored, and then returned.
// This method is thread-safe.
// identifier: A unique string key for the token bucket.
// Returns a pointer to the TokenBucket.
func (bm *BucketManager) GetBucket(identifier string) *TokenBucket {
	bm.mu.RLock() // Lock for reading from the buckets map.
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

	// Create and store the new bucket with the manager's default settings.
	newBucket := NewTokenBucket(bm.defaultRate, bm.defaultBurst)
	// NewTokenBucket can return nil if its inputs are invalid, though bm.defaultRate/Burst should be valid.
	// However, if it somehow was nil, storing it would be problematic.
	// For robustness, one might check `if newBucket != nil`. Given current NewTokenBucket logic, it won't be nil here.
	bm.buckets[identifier] = newBucket
	return newBucket
}

// GetOrCreateBucket retrieves an existing TokenBucket for the given identifier,
// or creates a new one if it doesn't exist, using the provided specific rate and burst.
// If the bucket already exists, its settings are *not* modified by this call; the existing bucket is returned.
// To modify an existing bucket's settings, use SetBucketRateAndBurst.
// If the provided rate or burst are non-positive, the manager's current defaultRate and defaultBurst are used instead for creation.
// This method is thread-safe.
// identifier: A unique string key for the token bucket.
// rate: The desired token generation rate if creating a new bucket.
// burst: The desired maximum token capacity if creating a new bucket.
// Returns a pointer to the TokenBucket.
func (bm *BucketManager) GetOrCreateBucket(identifier string, rate, burst int64) *TokenBucket {
	// Use manager's defaults if the provided specific rate/burst are invalid.
	effectiveRate := rate
	effectiveBurst := burst
	if effectiveRate <= 0 || effectiveBurst <= 0 {
		bm.mu.RLock() // Need to read defaults safely
		effectiveRate = bm.defaultRate
		effectiveBurst = bm.defaultBurst
		bm.mu.RUnlock()
	}

	bm.mu.RLock() // Lock for reading from the buckets map.
	bucket, exists := bm.buckets[identifier]
	bm.mu.RUnlock()

	if exists {
		// Bucket exists, return it as is. Its settings are not changed by this function.
		return bucket
	}

	// Bucket doesn't exist, acquire write lock to create it.
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Double-check, as in GetBucket
	bucket, exists = bm.buckets[identifier]
	if exists {
		// Similar to above, if it exists now, return it.
		// The decision of whether to update its rate/burst if it was created by another goroutine
		// in the meantime is handled by returning the existing one. The specific rate/burst passed
		// to this function are only used if *this* goroutine is the one creating the bucket.
		return bucket
	}

	// Create and store the new bucket with the (potentially defaulted) specific settings.
	newBucket := NewTokenBucket(effectiveRate, effectiveBurst)
	bm.buckets[identifier] = newBucket
	return newBucket
}

// CleanupStaleBuckets is intended to remove buckets that have been idle for longer than maxIdleTime.
// NOTE: This is currently a placeholder. A full implementation requires TokenBucket instances
// to track their last access time, which is not part of the current TokenBucket design.
// maxIdleTime: The maximum duration a bucket can be idle before being eligible for removal.
func (bm *BucketManager) CleanupStaleBuckets(maxIdleTime time.Duration) {
	bm.mu.Lock() // Lock for modifying the buckets map.
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
	// To implement this, TokenBucket would need a GetLastAccessTime() method,
	// and this function would iterate bm.buckets, check times, and delete old ones.
}

// SetDefaultRateAndBurst updates the default rate and burst size that will be used for
// subsequently created buckets (if GetBucket is called, or GetOrCreateBucket with invalid specific rates).
// Existing buckets are not affected. This method is thread-safe.
// rate: The new default token generation rate. If non-positive, the rate is not updated.
// burst: The new default maximum token capacity. If non-positive, the burst is not updated.
func (bm *BucketManager) SetDefaultRateAndBurst(rate, burst int64) {
	bm.mu.Lock() // Lock for writing defaultRate and defaultBurst.
	defer bm.mu.Unlock()
	if rate > 0 {
		bm.defaultRate = rate
	}
	if burst > 0 {
		bm.defaultBurst = burst
	}
}

// GetDefaultRateAndBurst returns the current default rate and burst size used for creating new buckets.
// This method is thread-safe.
// Returns the default rate (tokens per second) and default burst size (max tokens).
func (bm *BucketManager) GetDefaultRateAndBurst() (rate int64, burst int64) {
	bm.mu.RLock() // Lock for reading defaultRate and defaultBurst.
	defer bm.mu.RUnlock()
	return bm.defaultRate, bm.defaultBurst
}

// GetBucketSettings retrieves the current rate and burst settings of a specific, existing token bucket.
// This method is thread-safe.
// identifier: The unique string key for the token bucket.
// Returns the rate, burst, and a boolean indicating if the bucket exists.
// If the bucket does not exist, rate and burst will be 0 and exists will be false.
func (bm *BucketManager) GetBucketSettings(identifier string) (rate int64, burst int64, exists bool) {
	bm.mu.RLock() // Lock for reading from the buckets map.
	defer bm.mu.RUnlock()

	bucket, found := bm.buckets[identifier]
	if !found {
		return 0, 0, false // Bucket does not exist.
	}
	// TokenBucket's GetRateAndBurst method is internally thread-safe.
	r, b := bucket.GetRateAndBurst()
	return r, b, true
}

// SetBucketRateAndBurst sets or updates the rate and burst settings for a token bucket
// associated with the given identifier.
// If a bucket for the identifier does not exist, a new one is created with the specified settings.
// If a bucket exists, its settings are updated.
// This method is thread-safe.
// identifier: A unique string key for the token bucket.
// rate: The desired token generation rate. Must be positive.
// burst: The desired maximum token capacity. Must be positive.
// If rate or burst are non-positive, the operation is a no-op (bucket is not created or updated).
func (bm *BucketManager) SetBucketRateAndBurst(identifier string, rate int64, burst int64) {
	if rate <= 0 || burst <= 0 {
		// Do not create or update with invalid settings. Consider logging or returning an error.
		return
	}

	bm.mu.Lock() // Lock for writing to the buckets map or modifying an existing bucket.
	defer bm.mu.Unlock()

	bucket, exists := bm.buckets[identifier]
	if exists {
		// Bucket exists, update its settings.
		// TokenBucket's SetRateAndBurst is internally thread-safe.
		bucket.SetRateAndBurst(rate, burst)
	} else {
		// Bucket doesn't exist, create a new one with the specified settings.
		newBucket := NewTokenBucket(rate, burst)
		// NewTokenBucket already checks if rate/burst are positive.
		// It returns nil if they aren't, but we've checked above.
		bm.buckets[identifier] = newBucket
	}
}
