// ratelimit/bucket_test.go
package ratelimit

import (
	"math"
	"testing"
	"time"
)

const testEpsilon = 1e-9 // For float comparisons, using a slightly larger epsilon for time-based tests might be needed if flakiness occurs

func TestTokenBucket_InitialState(t *testing.T) {
	rate, burst := 100.0, 200.0
	tb := NewTokenBucket(rate, burst)
	if tb.Rate() != rate {
		t.Errorf("Initial rate: got %f, want %f", tb.Rate(), rate)
	}
	if tb.Burst() != burst {
		t.Errorf("Initial burst: got %f, want %f", tb.Burst(), burst)
	}
	if tb.CurrentTokens() != burst { // Bucket should start full
		t.Errorf("Initial tokens: got %f, want %f (full burst)", tb.CurrentTokens(), burst)
	}
}

func TestTokenBucket_Allow_BasicConsumption(t *testing.T) {
	tb := NewTokenBucket(100, 200) // Rate: 100 Bps, Burst: 200 B

	if !tb.Allow(100) {
		t.Errorf("Should allow initial consumption of 100 tokens")
	}
	if tokens := tb.CurrentTokens(); math.Abs(tokens-100.0) > testEpsilon { // Manually check after consumption
		t.Errorf("Tokens after consuming 100: got %f, want 100", tokens)
	}

	if !tb.Allow(100) {
		t.Errorf("Should allow second consumption of 100 tokens")
	}
	if tokens := tb.CurrentTokens(); math.Abs(tokens-0.0) > testEpsilon {
		t.Errorf("Tokens after consuming another 100: got %f, want 0", tokens)
	}

	if tb.Allow(1) { // Try to consume 1, should fail
		t.Errorf("Should not allow consumption of 1 token when bucket is empty")
	}
	if tokens := tb.CurrentTokens(); math.Abs(tokens-0.0) > testEpsilon { // Should still be 0
		t.Errorf("Tokens after trying to consume from empty bucket: got %f, want 0", tokens)
	}
}

func TestTokenBucket_RefillOverTime(t *testing.T) {
	rate := 100.0
	burst := 200.0
	tb := NewTokenBucket(rate, burst)

	tb.Allow(200) // Empty the bucket

	time.Sleep(500 * time.Millisecond) // Wait for 0.5 seconds
	// Expected tokens: 0.5s * 100 Bps = 50 B
	// CurrentTokens() calls refill()
	tokensAfterSleep1 := tb.CurrentTokens()
	if tokensAfterSleep1 < rate*0.5-5 || tokensAfterSleep1 > rate*0.5+5 { // Allow +/- 5 tokens slack for timing
		t.Errorf("After 0.5s, expected ~50 tokens, got %f", tokensAfterSleep1)
	}
	if !tb.Allow(int(rate * 0.5 * 0.8)) { // Try to consume 80% of what should have refilled
		t.Errorf("Should allow consumption after 0.5s refill")
	}

	time.Sleep(2 * time.Second) // Wait for 2 more seconds
	// Expected tokens: current + 2s * 100 Bps, capped at burst (200)
	// Bucket had ~10 tokens left (50 * 0.2), so 10 + 200 = 210, capped at 200.
	tokensAfterSleep2 := tb.CurrentTokens()
	if tokensAfterSleep2 < burst-5 || tokensAfterSleep2 > burst { // Should be close to full burst
		t.Errorf("After 2s more, expected tokens to be near burst (%f), got %f", burst, tokensAfterSleep2)
	}
}

func TestTokenBucket_BurstConsumption(t *testing.T) {
	tb := NewTokenBucket(10, 100) // Rate 10 Bps, Burst 100 B
	// Bucket starts full (100 tokens)
	if !tb.Consume(100) { // Consume alias for Allow
		t.Fatalf("Initial burst consumption of 100 failed")
	}
	if tb.Consume(1) {
		t.Fatalf("Should not allow consumption of 1 token beyond burst")
	}
	// Wait 2 seconds, should add 2 * 10 = 20 tokens
	time.Sleep(2 * time.Second)
	if !tb.Consume(20) {
		t.Errorf("Failed to consume 20 tokens after 2s refill (current: %f)", tb.CurrentTokens())
	}
	if tb.Consume(1) { // Should be empty now
		t.Errorf("Bucket should be empty after consuming refilled tokens (current: %f)", tb.CurrentTokens())
	}
}

func TestTokenBucket_SetRate(t *testing.T) {
	tb := NewTokenBucket(100, 200) // Rate 100, Burst 200
	tb.Allow(150)                  // Consume 150, tokens should be 50

	// Change rate
	tb.SetRate(10, 50) // New Rate 10 Bps, New Burst 50 B.
	// After SetRate, refill() is called with old rate, but tokens are then capped by new burst.
	// Initial tokens: 200. Consumed 150. Left: 50.
	// SetRate calls refill: lastRefill was ~now. So elapsed ~0. tokensToAdd ~0. tokens still 50.
	// Then tokens (50) capped at newBurst (50). So tokens should be 50.

	if math.Abs(tb.Rate()-10.0) > testEpsilon {
		t.Errorf("SetRate() failed to update rate. got %f, want %f", tb.Rate(), 10.0)
	}
	if math.Abs(tb.Burst()-50.0) > testEpsilon {
		t.Errorf("SetRate() failed to update burst. got %f, want %f", tb.Burst(), 50.0)
	}
	// Current tokens should be capped at new burst size.
	// CurrentTokens() itself calls refill. So, tokens = oldTokens (50) + elapsed(tiny)*oldRate(100) capped at newBurst(50)
	// then after setting new rate/burst, it's tokens (50) capped at newBurst (50).
	// Then CurrentTokens() calls refill with new rate for tiny elapsed time.
	// Effectively, it should be very close to 50.
	tokensAfterSetRate := tb.CurrentTokens()
	if tokensAfterSetRate > 50.0+testEpsilon || tokensAfterSetRate < 50.0-5 { // Allow some slack if SetRate took time
		t.Errorf("SetRate() failed to cap/maintain tokens. got %f, want ~50.0", tokensAfterSetRate)
	}

	// Consume all current tokens (should be ~50)
	if !tb.Allow(50) { t.Fatalf("Could not consume 50 tokens after SetRate (current: %f)", tb.CurrentTokens()) }
	if tokens := tb.CurrentTokens(); math.Abs(tokens-0.0) > testEpsilon {
		t.Errorf("Tokens after consuming 50 post-SetRate: got %f, want 0", tokens)
	}


	time.Sleep(1 * time.Second) // Refill at 10 Bps for 1 sec = 10 tokens
	tokensAfterSleep := tb.CurrentTokens()
	if tokensAfterSleep < 9.0 || tokensAfterSleep > 11.0 { // Allow some slack
		t.Errorf("After SetRate and 1s sleep, expected ~10 tokens, got %f", tokensAfterSleep)
	}
}

func TestTokenBucket_ZeroRate(t *testing.T) {
	tb := NewTokenBucket(0, 100) // Zero rate, Burst 100
	if !tb.Allow(100) {
		t.Fatalf("Failed to consume initial burst with zero rate")
	}
	if tb.Allow(1) {
		t.Fatalf("Should not allow any more tokens with zero rate (bucket empty)")
	}
	time.Sleep(100 * time.Millisecond) // Wait a bit
	if tb.Allow(1) {                   // Should still not allow, no refill
		t.Fatalf("Should not refill with zero rate (current: %f)", tb.CurrentTokens())
	}
	// Test SetRate on a zero-rate bucket
	tb.SetRate(10,20) // Rate 10, Burst 20
	if math.Abs(tb.Rate() - 10.0) > testEpsilon || math.Abs(tb.Burst() - 20.0) > testEpsilon {
		t.Errorf("SetRate on zero-rate bucket failed to update params.")
	}
	// Tokens should be 0 (from previous state) + elapsed*old_rate(0) = 0. Capped at new burst 20.
	// Then CurrentTokens() calls refill with new rate.
	tokensAfterSetRate := tb.CurrentTokens()
	if tokensAfterSetRate > 1.0 { // Should be close to 0, allow small amount for timing of SetRate call itself
		t.Errorf("Tokens immediately after SetRate from 0 to 10 (burst 20): got %f, want ~0", tokensAfterSetRate)
	}
	time.Sleep(500 * time.Millisecond) // 0.5 sec * 10 Bps = 5 tokens
	tokensAfterSleep := tb.CurrentTokens()
	if tokensAfterSleep < 4.0 || tokensAfterSleep > 6.0 {
		t.Errorf("After SetRate and 0.5s sleep, expected ~5 tokens, got %f", tokensAfterSleep)
	}
}

func TestTokenBucket_AllowZeroOrNegative(t *testing.T) {
    tb := NewTokenBucket(10, 10)
    if !tb.Allow(0) {
        t.Errorf("Allow(0) should always return true")
    }
    if !tb.Allow(-5) {
        t.Errorf("Allow(-5) should always return true")
    }
    // Bucket state should be unchanged
    if math.Abs(tb.CurrentTokens() - 10.0) > testEpsilon {
        t.Errorf("Bucket tokens changed after Allow(0) or Allow(-5)")
    }
}

func TestTokenBucket_RateZeroBurstZero(t *testing.T) {
    tb := NewTokenBucket(0, 0)
    if tb.Allow(1) {
        t.Errorf("Allow(1) should return false for 0 rate and 0 burst bucket")
    }
}

func TestTokenBucket_RefillDoesNotExceedBurst(t *testing.T) {
    tb := NewTokenBucket(1000, 100) // High rate, small burst
    time.Sleep(1 * time.Second) // Enough time to generate 1000 tokens
    // CurrentTokens calls refill
    if tokens := tb.CurrentTokens(); tokens > 100.0 + testEpsilon {
        t.Errorf("Refill exceeded burst. Got %f, want max 100", tokens)
    }
}

func TestTokenBucket_SetRate_BurstAdjustment(t *testing.T) {
    tb := NewTokenBucket(100, 200)
    // Scenario 1: New burst is smaller than current tokens (after old rate refill)
    tb.Allow(50) // Tokens: 150
    tb.SetRate(10, 100) // New rate 10, new burst 100.
    // Refill with old rate (100) for tiny elapsed: tokens ~150.
    // Then capped at new burst 100.
    if ct := tb.CurrentTokens(); ct > 100 + testEpsilon || ct < 100 - 5 { // Allow small slack for timing
         t.Errorf("SetRate with smaller burst: current tokens got %f, want ~100", ct)
    }

    // Scenario 2: New burst is larger, tokens should not change beyond refill
    tb.SetRate(10, 300) // New rate 10, new burst 300
    // Refill with old rate (10) for tiny elapsed: tokens ~100 (from previous state).
    // Not capped by new burst.
    if ct := tb.CurrentTokens(); ct > 100 + 5 || ct < 100 - 5 {
         t.Errorf("SetRate with larger burst: current tokens got %f, want ~100", ct)
    }
}
