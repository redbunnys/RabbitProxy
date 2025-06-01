# Standalone Rate Limiting Module (Token Bucket)

This Go module provides a token bucket implementation for rate limiting operations. It was originally part of a larger project but has been isolated here for focused study and usage.

## Features

*   **Token Bucket Algorithm:** Classic and flexible rate limiting.
*   **Configurable Rate and Burst:** Set sustained rate and maximum burst capacity.
*   **Dynamic Updates:** Rate and burst can be changed on an existing bucket instance.
*   **Bandwidth String Parsing:** Utility to convert human-readable bandwidth strings (e.g., "10Mbps", "500K", "1G", "unlimited") into bytes per second.
*   **Thread-Safe:** The token bucket implementation is safe for concurrent use.

## Package: `ratelimit`

### `parser.go`

*   `ParseBandwidthString(bwStr string) (float64, error)`:
    *   Parses strings like "10M" (Mbps), "500K" (Kbps), "1G" (Gbps), "1024bps", or "1024" (plain number for bps).
    *   Also accepts "unlimited" or "0" for no limit.
    *   Returns the rate in **Bytes Per Second** (float64).

### `bucket.go`

*   `type TokenBucket`
    *   The core rate limiter struct.
*   `NewTokenBucket(rateBPS, burstBPS float64) *TokenBucket`:
    *   Creates a new token bucket.
    *   `rateBPS`: Tokens (bytes) to add per second.
    *   `burstBPS`: Maximum capacity of the bucket in tokens (bytes). Defaults to `rateBPS` if not adequately specified.
*   `Allow(n int) bool` (aliased as `Consume(n int) bool`):
    *   Non-blocking. Checks if `n` tokens (bytes) can be consumed.
    *   Returns `true` and consumes tokens if available, `false` otherwise.
*   `SetRate(newRateBPS, newBurstBPS float64)`:
    *   Updates the rate and burst size of an existing bucket.
*   `CurrentTokens() float64`:
    *   Returns the current number of available tokens.
*   `Rate() float64`:
    *   Returns the current configured rate in BPS.
*   `Burst() float64`:
    *   Returns the current configured burst size in BPS.


## Basic Usage Example

```go
package main

import (
	"fmt"
	"time"
	"ratelimit-module-example/ratelimit" // Assuming go.mod is 'ratelimit-module-example'
)

func main() {
	// Example: Rate limit to 100 bytes/sec with a burst of 200 bytes
	rateBPS, _ := ratelimit.ParseBandwidthString("800bps") // 800 bits/sec = 100 bytes/sec
	burstBPS := 200.0

	bucket := ratelimit.NewTokenBucket(rateBPS, burstBPS)

	for i := 0; i < 5; i++ {
		// Try to consume 50 bytes
		if bucket.Consume(50) {
			fmt.Printf("[%s] Action %d: Allowed (50 bytes consumed)\n", time.Now().Format("15:04:05.000"), i+1)
		} else {
			fmt.Printf("[%s] Action %d: Denied (not enough tokens for 50 bytes)\n", time.Now().Format("15:04:05.000"), i+1)
		}
		time.Sleep(200 * time.Millisecond) // Wait a bit before next action
	}

    // Example of CurrentTokens
    fmt.Printf("Current tokens after operations: %.2f\n", bucket.CurrentTokens())

    // Example of changing rate
    newRateBPS, _ := ratelimit.ParseBandwidthString("1.6kbps") // 1600 bits/sec = 200 bytes/sec
    bucket.SetRate(newRateBPS, 300.0) // New rate, new burst
    fmt.Printf("Rate updated to %.2f Bps, Burst updated to %.2f Bps\n", bucket.Rate(), bucket.Burst())
}
```

This module is intended for educational purposes as a standalone example of a token bucket rate limiter.
