package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

// TokenBucket implements a thread-safe token bucket rate limiting algorithm.
// It allows controlling the rate at which events (e.g., data transfer) can occur.
type TokenBucket struct {
	rate  int64 // rate defines the number of tokens generated per second.
	burst int64 // burst is the maximum number of tokens the bucket can hold (i.e., its capacity).

	// tokens stores the current number of available tokens in the bucket.
	// It is accessed atomically.
	tokens int64

	// lastTokenTime records the timestamp when tokens were last updated.
	// Protected by mu.
	lastTokenTime time.Time

	// mu protects access to lastTokenTime and coordinates token generation logic.
	// It also ensures that addTokens is called serially.
	mu sync.Mutex
}

// NewTokenBucket creates a new TokenBucket with a specified rate and burst size.
// rate: The number of tokens to generate per second. Must be positive.
// burst: The maximum capacity of the bucket. Must be positive.
// Returns a pointer to the initialized TokenBucket, or nil if rate or burst are not positive.
// The bucket starts full of tokens.
func NewTokenBucket(rate, burst int64) *TokenBucket {
	if rate <= 0 || burst <= 0 {
		return nil
	}
	return &TokenBucket{
		rate:          rate,
		burst:         burst,
		tokens:        burst, // Start with a full bucket
		lastTokenTime: time.Now(),
	}
}

// addTokens calculates and adds new tokens to the bucket based on the elapsed time
// since the last token update. It's called internally by methods like Take and Wait.
// This method must be called while holding tb.mu lock.
func (tb *TokenBucket) addTokens() {
	now := time.Now()
	elapsed := now.Sub(tb.lastTokenTime)

	if elapsed <= 0 {
		return // No time has passed, or clock went backwards
	}

	tokensToAddFloat := float64(elapsed.Nanoseconds()) * float64(tb.rate) / float64(time.Second.Nanoseconds())
	tokensToAdd := int64(tokensToAddFloat)

	if tokensToAdd > 0 {
		currentTokens := atomic.LoadInt64(&tb.tokens)
		newTokens := currentTokens + tokensToAdd
		if newTokens > tb.burst {
			newTokens = tb.burst
		}
		// If CAS succeeds, tokens were updated, so update lastTokenTime.
		// If CAS fails, it implies a concurrent modification under the same lock (which is unexpected for tb.tokens)
		// or newTokens == currentTokens (e.g. already at burst, or tokensToAdd was rounded to 0 effectively).
		// Not updating lastTokenTime upon CAS failure for an increase means the token generation for this period
		// might be re-attempted, which can be a safe fallback.
		// However, if newTokens == currentTokens (e.g. at burst), lastTokenTime should still be updated.
		if atomic.CompareAndSwapInt64(&tb.tokens, currentTokens, newTokens) {
			tb.lastTokenTime = now
		} else if newTokens == currentTokens { // e.g. already at burst, or no real change
			tb.lastTokenTime = now
		}
		// If CAS failed and newTokens != currentTokens, it's an odd state under lock.
		// For now, we prioritize updating lastTokenTime if tokens were effectively added or no change was needed.
	} else {
		// No new full tokens generated, but time has passed. Update lastTokenTime.
		tb.lastTokenTime = now
	}
}

// SetRateAndBurst updates the rate and burst size of the token bucket.
// newRate: The new rate of token generation per second. Must be positive.
// newBurst: The new maximum capacity of the bucket. Must be positive.
// If non-positive values are provided for newRate or newBurst, the existing values are retained for that parameter.
// It ensures that the changes are applied safely and that current token count respects the new burst limit.
func (tb *TokenBucket) SetRateAndBurst(newRate, newBurst int64) {
	tb.mu.Lock() // Ensure exclusive access for updating rate, burst, and tokens.
	defer tb.mu.Unlock()

	tb.addTokens() // Account for tokens generated based on the old rate.

	if newRate > 0 {
		tb.rate = newRate
	}
	if newBurst > 0 {
		tb.burst = newBurst
	}

	currentTokens := atomic.LoadInt64(&tb.tokens)
	if currentTokens > tb.burst { // Use tb.burst as it might have just been updated.
		atomic.StoreInt64(&tb.tokens, tb.burst)
	}
}

// GetRateAndBurst returns the current rate and burst settings of the token bucket.
// It is thread-safe.
func (tb *TokenBucket) GetRateAndBurst() (rate int64, burst int64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.rate, tb.burst
}

// Take attempts to consume 'count' tokens from the bucket.
// count: The number of tokens to consume. If count is zero or negative, Take returns true without action.
// Returns true if enough tokens were available and consumed, false otherwise.
// This method is thread-safe.
func (tb *TokenBucket) Take(count int64) bool {
	if count <= 0 {
		return true // No tokens requested or invalid count, operation is trivially successful.
	}

	tb.mu.Lock()
	tb.addTokens() // Replenish tokens based on elapsed time.
	currentTokens := atomic.LoadInt64(&tb.tokens)

	if currentTokens >= count {
		if atomic.CompareAndSwapInt64(&tb.tokens, currentTokens, currentTokens-count) {
			tb.mu.Unlock()
			return true
		}
		// If CAS failed, another goroutine (unexpectedly, due to outer lock) or logic error.
	}
	tb.mu.Unlock()
	return false
}

// Wait blocks until 'count' tokens are available in the bucket and then consumes them.
// count: The number of tokens to wait for and consume. If count is zero or negative, Wait returns nil immediately.
// Returns nil when tokens are successfully consumed.
// This method is thread-safe.
// Note: This implementation involves sleeping; a context-aware version might be preferable for cancellable waits.
func (tb *TokenBucket) Wait(count int64) error {
	if count <= 0 {
		return nil // No tokens requested or invalid count.
	}

	for {
		tb.mu.Lock()
		tb.addTokens() // Replenish tokens.
		currentTokens := atomic.LoadInt64(&tb.tokens)

		if currentTokens >= count {
			if atomic.CompareAndSwapInt64(&tb.tokens, currentTokens, currentTokens-count) {
				tb.mu.Unlock()
				return nil // Successfully consumed tokens.
			}
			// If CAS failed, another goroutine consumed tokens (unexpectedly, due to outer lock). Loop to re-evaluate.
			tb.mu.Unlock()
			continue
		}
		tb.mu.Unlock() // Release lock before sleeping.

		needed := count - currentTokens
		if needed <= 0 { needed = 1 } // Should not happen if logic is correct, but as a fallback.

		// Read rate for wait calculation. It's read outside the lock here.
		// This is generally fine as rate changes are infrequent and protected by SetRateAndBurst's lock.
		// A very brief stale read of tb.rate is acceptable for calculating sleep duration.
		currentRate := tb.rate // Direct read, or atomic.LoadInt64(&tb.rate) if rate was atomic.

		var waitDuration time.Duration
		if currentRate > 0 {
			waitDuration = time.Duration(float64(needed) * float64(time.Second) / float64(currentRate))
			waitDuration += 10 * time.Millisecond // Add a small buffer to avoid waking up too soon.
		} else {
			// If rate is zero (or negative, though constructor/setter should prevent this),
			// and tokens are needed, this would wait indefinitely without intervention.
			// Use a fixed, reasonably small polling interval to re-check.
			waitDuration = 100 * time.Millisecond
		}
		time.Sleep(waitDuration)
	}
}
