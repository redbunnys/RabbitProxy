package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

// TokenBucket represents a token bucket rate limiter.
type TokenBucket struct {
	rate          int64 // tokens per second
	burst         int64 // maximum number of tokens the bucket can hold
	tokens        int64 // current number of tokens
	lastTokenTime time.Time
	mu            sync.Mutex
}

// NewTokenBucket creates a new TokenBucket.
func NewTokenBucket(rate, burst int64) *TokenBucket {
	if rate <= 0 || burst <= 0 {
		return nil // Or handle error appropriately
	}
	return &TokenBucket{
		rate:          rate,
		burst:         burst,
		tokens:        burst, // Start with a full bucket
		lastTokenTime: time.Now(),
	}
}

// addTokens calculates and adds new tokens based on the elapsed time.
// This method is not thread-safe by itself and expects to be called
// from within a critical section protected by tb.mu.
func (tb *TokenBucket) addTokens() {
	now := time.Now()
	elapsed := now.Sub(tb.lastTokenTime)

	if elapsed <= 0 {
		return // No time has passed, or clock went backwards
	}

	// Calculate tokens to add
	// Use float64 for precision in intermediate calculation
	tokensToAddFloat := float64(elapsed.Nanoseconds()) * float64(tb.rate) / float64(time.Second.Nanoseconds())

	tokensToAdd := int64(tokensToAddFloat)

	if tokensToAdd > 0 {
		currentTokens := atomic.LoadInt64(&tb.tokens)
		newTokens := currentTokens + tokensToAdd
		if newTokens > tb.burst {
			newTokens = tb.burst
		}
		// Atomically update tokens only if it changed.
		// This CAS is not strictly necessary here because we are under tb.mu.Lock(),
		// but it's a good practice if we were to refactor tb.tokens update.
		// A simple atomic.StoreInt64(&tb.tokens, newTokens) or even tb.tokens = newTokens would also work here.
		if atomic.CompareAndSwapInt64(&tb.tokens, currentTokens, newTokens) {
			// Only update lastTokenTime if tokens were successfully updated.
			tb.lastTokenTime = now
		} else {
			// If CAS failed, it means tokens were updated by another operation (e.g. Take/Wait)
			// concurrently, which shouldn't happen if mu is correctly used by callers.
			// For robustness, we can re-fetch current time if we were to retry.
			// However, since addTokens is called within a lock, this path indicates a potential logic error
			// in how the lock is managed. For now, assume the lock prevents this.
			// If tokens were not updated, we should not update lastTokenTime to prevent losing generated tokens.
		}

	} else {
		// No full tokens generated, but still update lastTokenTime to now to keep it current.
		// This prevents a large elapsed time accumulation if tokens are generated very slowly.
		tb.lastTokenTime = now
	}
}

// SetRateAndBurst updates the rate and burst size of the token bucket.
// It ensures that the changes are applied safely.
func (tb *TokenBucket) SetRateAndBurst(newRate, newBurst int64) {
	if newRate <= 0 || newBurst <= 0 {
		// Or handle error appropriately, e.g., return an error
		return
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Add tokens based on the old rate before changing it
	tb.addTokens()

	tb.rate = newRate
	tb.burst = newBurst

	// If current tokens exceed the new burst size, cap them.
	currentTokens := atomic.LoadInt64(&tb.tokens)
	if currentTokens > newBurst {
		atomic.StoreInt64(&tb.tokens, newBurst)
	}
	// Note: lastTokenTime is updated by addTokens.
	// The next call to addTokens will use the new rate.
}

// Take attempts to consume count tokens. Returns true if successful, false otherwise.
func (tb *TokenBucket) Take(count int64) bool {
	if count <= 0 {
		return true // No tokens requested, always successful
	}

	tb.mu.Lock()
	tb.addTokens() // Recalculate tokens before attempting to take
	currentTokens := atomic.LoadInt64(&tb.tokens)

	if currentTokens >= count {
		// Try to decrement tokens
		if atomic.CompareAndSwapInt64(&tb.tokens, currentTokens, currentTokens-count) {
			tb.mu.Unlock()
			return true
		}
		// If CAS failed, another goroutine took tokens. Fall through to return false.
	}
	tb.mu.Unlock()
	return false
}

// Wait waits until count tokens are available and then consumes them.
// This is a simplified implementation.
func (tb *TokenBucket) Wait(count int64) error {
	if count <= 0 {
		return nil // No tokens requested
	}

	for {
		tb.mu.Lock()
		tb.addTokens() // Recalculate tokens
		currentTokens := atomic.LoadInt64(&tb.tokens)

		if currentTokens >= count {
			if atomic.CompareAndSwapInt64(&tb.tokens, currentTokens, currentTokens-count) {
				tb.mu.Unlock()
				return nil
			}
			// If CAS failed, another goroutine modified tokens. Loop again.
			tb.mu.Unlock()
			continue // Re-evaluate
		}
		tb.mu.Unlock()

		// Not enough tokens, calculate how many more are needed and estimate wait time
		needed := count - currentTokens
		if needed < 0 { // Should not happen if logic is correct
			needed = 1
		}

		// Estimate wait time.
		var waitDuration time.Duration
		if tb.rate > 0 {
			// Calculate time needed to generate the remaining tokens
			waitDuration = time.Duration(float64(needed) * float64(time.Second) / float64(tb.rate))
			// Add a small buffer (e.g., 10ms) to avoid waking up too soon due to precision or timing issues.
			waitDuration += 10 * time.Millisecond
		} else {
			// If rate is 0 (or negative, though constructor should prevent this),
			// and we need tokens, we'll wait indefinitely if not careful.
			// For now, use a fixed, reasonably small polling interval.
			// This part might need a more sophisticated handling like a channel/signal
			// if the rate can change, or if indefinite wait for 0 rate is not desired.
			waitDuration = 100 * time.Millisecond
		}

		time.Sleep(waitDuration)
	}
}
