package ratelimit

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

const (
	BytesPerKilobit = 125.0       // 1000 / 8
	BytesPerMegabit = 125000.0    // 1000 * 1000 / 8
	BytesPerGigabit = 125000000.0 // 1000 * 1000 * 1000 / 8
	BitsInByte      = 8.0
)

// ParseBandwidthString converts a bandwidth string (e.g., "10M", "500k", "1g", "1024bps")
// into bytes per second (float64 for precision).
// "0", "", or "unlimited" means unlimited rate (represented as 0.0 bytes/sec).
func ParseBandwidthString(bwStr string) (float64, error) {
	trimmedBwStr := strings.ToLower(strings.TrimSpace(bwStr))
	if trimmedBwStr == "" || trimmedBwStr == "unlimited" || trimmedBwStr == "0" {
		return 0.0, nil // 0.0 means unlimited rate
	}

	var multiplier float64
	var numStr string

	if strings.HasSuffix(trimmedBwStr, "kbps") {
		multiplier = BytesPerKilobit
		numStr = strings.TrimSuffix(trimmedBwStr, "kbps")
	} else if strings.HasSuffix(trimmedBwStr, "k") {
		multiplier = BytesPerKilobit
		numStr = strings.TrimSuffix(trimmedBwStr, "k")
	} else if strings.HasSuffix(trimmedBwStr, "mbps") {
		multiplier = BytesPerMegabit
		numStr = strings.TrimSuffix(trimmedBwStr, "mbps")
	} else if strings.HasSuffix(trimmedBwStr, "m") {
		multiplier = BytesPerMegabit
		numStr = strings.TrimSuffix(trimmedBwStr, "m")
	} else if strings.HasSuffix(trimmedBwStr, "gbps") {
		multiplier = BytesPerGigabit
		numStr = strings.TrimSuffix(trimmedBwStr, "gbps")
	} else if strings.HasSuffix(trimmedBwStr, "g") {
		multiplier = BytesPerGigabit
		numStr = strings.TrimSuffix(trimmedBwStr, "g")
	} else if strings.HasSuffix(trimmedBwStr, "bps") {
		multiplier = 1.0 / BitsInByte // Convert bits per second to bytes per second
		numStr = strings.TrimSuffix(trimmedBwStr, "bps")
	} else {
		// Check if the string is purely numeric (interpreting as bits per second)
		isNumeric := true
		for _, r := range trimmedBwStr {
			if !unicode.IsDigit(r) && r != '.' { // Allow decimal point
				isNumeric = false
				break
			}
		}
		if isNumeric {
			multiplier = 1.0 / BitsInByte // Treat plain numbers as bits per second
			numStr = trimmedBwStr
		} else {
			return 0, fmt.Errorf("invalid bandwidth string format: unknown suffix in '%s'", bwStr)
		}
	}

	numStr = strings.TrimSpace(numStr)
	if numStr == "" {
		return 0, fmt.Errorf("invalid bandwidth string format: no numeric value found in '%s'", bwStr)
	}

	value, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric value '%s' in bandwidth string '%s': %w", numStr, bwStr, err)
	}

	if value < 0 {
		return 0, fmt.Errorf("bandwidth value cannot be negative: %s (from %s)", numStr, bwStr)
	}

	// If value is extremely small but positive, it's still a valid rate.
	// If value is 0 after parsing a non-"0" string (e.g. "0k"), it's a specific 0 rate, treated as unlimited.
	if value == 0 {
	    return 0.0, nil // Explicit 0 rate means unlimited
	}

	return value * multiplier, nil
}
