package ratelimit

import (
	"sort"
	"sync"
	// No specific sub-package import needed if all files are in package ratelimit
	// "rabbitproxy/pkg/ratelimit/bucketmanager"
	// "rabbitproxy/pkg/ratelimit/tokenbucket"
)

// PriorityLevel defines the level of priority.
type PriorityLevel int

const (
	// Low is the lowest priority.
	Low PriorityLevel = iota
	// Medium is the medium priority.
	Medium
	// High is the highest priority.
	High
)

// String returns the string representation of PriorityLevel.
func (pl PriorityLevel) String() string {
	switch pl {
	case High:
		return "High"
	case Medium:
		return "Medium"
	case Low:
		return "Low"
	default:
		return "Unknown"
	}
}

// PriorityLimiter manages token buckets with different priority levels.
type PriorityLimiter struct {
	priorityBuckets map[PriorityLevel]*BucketManager // Use direct type name
	priorityOrder   []PriorityLevel // Stored from highest to lowest
	defaultRate     int64
	defaultBurst    int64
	mu              sync.RWMutex // Protects priorityBuckets if levels can be added/removed dynamically
}

// NewPriorityLimiter creates a new PriorityLimiter.
// levels should be provided, and they will be sorted internally from highest to lowest.
func NewPriorityLimiter(defaultRate, defaultBurst int64, levels []PriorityLevel) *PriorityLimiter {
	if defaultRate <= 0 {
		defaultRate = 10 // Default to 10 tps
	}
	if defaultBurst <= 0 {
		defaultBurst = 20 // Default to burst of 20
	}

	pl := &PriorityLimiter{
		priorityBuckets: make(map[PriorityLevel]*bucketmanager.BucketManager),
		priorityOrder:   make([]PriorityLevel, len(levels)),
		defaultRate:     defaultRate,
		defaultBurst:    defaultBurst,
	}

	copy(pl.priorityOrder, levels)
	// Sort levels from High (largest int value) to Low (smallest int value)
	// Ensure High > Medium > Low numerically for this sort to work as intended.
	sort.Slice(pl.priorityOrder, func(i, j int) bool {
		return pl.priorityOrder[i] > pl.priorityOrder[j]
	})

	for _, level := range pl.priorityOrder {
		pl.priorityBuckets[level] = NewBucketManager(defaultRate, defaultBurst) // Use direct type name
	}

	return pl
}

// Take attempts to consume requestedTokens for the identifier, trying from requestedPriority downwards.
func (pl *PriorityLimiter) Take(identifier string, requestedTokens int64, requestedPriority PriorityLevel) bool {
	pl.mu.RLock() // Lock for reading priorityOrder and priorityBuckets map
	defer pl.mu.RUnlock()

	startIndex := -1
	for i, p := range pl.priorityOrder {
		if p == requestedPriority {
			startIndex = i
			break
		}
	}

	if startIndex == -1 {
		// Requested priority level doesn't exist or isn't configured in this limiter
		return false
	}

	// Try from the requestedPriority down to the lowest configured priority
	for i := startIndex; i < len(pl.priorityOrder); i++ {
		currentPriority := pl.priorityOrder[i]
		manager, ok := pl.priorityBuckets[currentPriority]
		if !ok {
			// Should not happen if constructor correctly initializes all levels in priorityOrder
			continue
		}

		bucket := manager.GetBucket(identifier) // Gets or creates bucket with default rate/burst for this manager
		// bucket is of type *TokenBucket from the same package
		if bucket.Take(requestedTokens) {
			return true
		}
	}

	return false
}

// SetRateAndBurst allows updating the rate and burst for a specific identifier
// within its designated priority bucket manager.
func (pl *PriorityLimiter) SetRateAndBurst(identifier string, priority PriorityLevel, newRate, newBurst int64) {
	pl.mu.RLock() // Lock for reading priorityBuckets map
	manager, ok := pl.priorityBuckets[priority]
	pl.mu.RUnlock()

	if !ok {
		// Priority level doesn't exist
		return // Or handle error
	}

	// Get or create the bucket first to ensure it exists
	bucket := manager.GetOrCreateBucket(identifier, newRate, newBurst)
	// Then apply the new rate and burst. If GetOrCreateBucket created it, these are already set.
	// If it existed, this will update it.
	bucket.SetRateAndBurst(newRate, newBurst)
}

// Helper function to get available priority levels (optional)
func (pl *PriorityLimiter) GetAvailablePriorities() []PriorityLevel {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	priorities := make([]PriorityLevel, len(pl.priorityOrder))
	copy(priorities, pl.priorityOrder)
	return priorities
}

// GetManagerForPriority returns the bucket manager for a specific priority.
// Useful if direct access to a manager is needed.
func (pl *PriorityLimiter) GetManagerForPriority(priority PriorityLevel) (*BucketManager, bool) { // Use direct type name
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	manager, ok := pl.priorityBuckets[priority]
	return manager, ok
}
