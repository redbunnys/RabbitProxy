package ratelimit

import (
	"math"
	"sync"
	"time"
)

// TokenBucket represents a token bucket rate limiter.
type TokenBucket struct {
	mu         sync.Mutex
	rate       float64 // tokens (bytes) per second
	burst      float64 // bucket capacity in tokens (bytes)
	tokens     float64 // current number of tokens
	lastRefill time.Time
}

// NewTokenBucket creates a new TokenBucket.
// rateBPS is tokens (bytes) per second. A rate of 0 means no refill, effectively limiting to initial burst if burst > 0.
// burstBPS is the maximum capacity of the bucket in tokens (bytes).
func NewTokenBucket(rateBPS, burstBPS float64) *TokenBucket {
	if rateBPS < 0 {
		rateBPS = 0 // Rate cannot be negative
	}
	if burstBPS <= 0 {
		// If burst is not positive, default it. If rate is 0, burst should also be 0.
		// If rate > 0, burst should be at least rate (e.g. to allow one second of data).
		if rateBPS > 0 {
			burstBPS = rateBPS
		} else {
			burstBPS = 0 // No rate, no burst
		}
	}
	// Ensure burst is at least the rate, if rate is positive.
	// This allows at least one second worth of tokens to accumulate.
	if rateBPS > 0 && burstBPS < rateBPS {
		burstBPS = rateBPS
	}

	return &TokenBucket{
		rate:       rateBPS,
		burst:      burstBPS,
		tokens:     burstBPS, // Start with a full bucket
		lastRefill: time.Now(),
	}
}

// refill recalculates the number of tokens in the bucket based on elapsed time.
// Must be called with the mutex held.
func (tb *TokenBucket) refill() {
	if tb.rate <= 0 { // If rate is zero, no tokens are ever added.
		tb.lastRefill = time.Now() // Keep lastRefill updated to prevent large elapsed times if rate changes later.
		return
	}
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	if elapsed <= 0 { // Avoid issues if time goes backwards or very short intervals
		return
	}

	tokensToAdd := elapsed * tb.rate
	tb.tokens += tokensToAdd
	if tb.tokens > tb.burst {
		tb.tokens = tb.burst // Cap tokens at burst size
	}
	tb.lastRefill = now
}

// Allow checks if 'n' tokens (bytes) can be consumed.
// It consumes the tokens if available and returns true. Otherwise, returns false.
// This is non-blocking.
func (tb *TokenBucket) Allow(n int) bool {
	if n <= 0 { // Consuming zero or negative tokens is always allowed.
		return true
	}
	if tb.rate == 0 && tb.burst == 0 { // If rate and burst are zero, nothing is allowed.
		return false
	}


	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill() // Refill tokens based on time passed

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return true
	}
	return false
}

// Consume is an alias for Allow for clarity in usage (e.g. tb.Consume(bytes)).
func (tb *TokenBucket) Consume(n int) bool {
	return tb.Allow(n)
}

// SetRate updates the rate and burst size of the token bucket.
// This can be used for dynamic configuration reloads.
func (tb *TokenBucket) SetRate(newRateBPS, newBurstBPS float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Validate and normalize new rates and bursts
	if newRateBPS < 0 {
		newRateBPS = 0
	}
	if newBurstBPS <= 0 {
		if newRateBPS > 0 {
			newBurstBPS = newRateBPS
		} else {
			newBurstBPS = 0
		}
	}
	if newRateBPS > 0 && newBurstBPS < newRateBPS {
		newBurstBPS = newRateBPS
	}

	// Before changing rate, refill with the old rate to account for elapsed time
	tb.refill()

	tb.rate = newRateBPS
	tb.burst = newBurstBPS
	// Ensure current tokens do not exceed new burst size.
	if tb.tokens > tb.burst {
		tb.tokens = tb.burst
	}
	// lastRefill is already updated by the refill() call.
}

// CurrentTokens returns the current number of tokens in the bucket.
// Useful for metrics/monitoring. Not guaranteed to be perfectly accurate without holding the lock externally
// if there are concurrent Consume operations, but good for a snapshot.
func (tb *TokenBucket) CurrentTokens() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	// Refill before reporting to give the most up-to-date number for an instantaneous check.
	tb.refill()
	return tb.tokens
}

// Rate returns the current rate in bytes/sec.
func (tb *TokenBucket) Rate() float64 {
    tb.mu.Lock()
    defer tb.mu.Unlock()
    return tb.rate
}

// Burst returns the current burst size in bytes.
func (tb *TokenBucket) Burst() float64 {
    tb.mu.Lock()
    defer tb.mu.Unlock()
    return tb.burst
}
