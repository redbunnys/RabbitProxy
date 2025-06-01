// ratelimit/parser_test.go
package ratelimit

import (
	"math"
	"testing"
)

func TestParseBandwidthString(t *testing.T) {
	tests := []struct {
		name    string
		bwStr   string
		wantBPS float64
		wantErr bool
	}{
		{"empty", "", 0, false},
		{"unlimited", "unlimited", 0, false},
		{"zero", "0", 0, false},
		{"100bps_plain_num_as_bps", "100", 100.0 / BitsInByte, false}, // Plain numbers are bps
		{"100bps_suffix", "100bps", 100.0 / BitsInByte, false},
		{"1K_uppercase", "1K", BytesPerKilobit, false},
		{"1k_lowercase", "1k", BytesPerKilobit, false},
		{"1kbps_suffix", "1kbps", BytesPerKilobit, false},
		{"2.5M_uppercase", "2.5M", 2.5 * BytesPerMegabit, false},
		{"2.5m_lowercase", "2.5m", 2.5 * BytesPerMegabit, false},
		{"2.5mbps_suffix", "2.5mbps", 2.5 * BytesPerMegabit, false},
		{"0.5G_uppercase", "0.5G", 0.5 * BytesPerGigabit, false},
		{"0.5g_lowercase", "0.5g", 0.5 * BytesPerGigabit, false},
		{"0.5gbps_suffix", "0.5gbps", 0.5 * BytesPerGigabit, false},
		{"invalid_suffix", "100xx", 0, true},
		{"invalid_number_char", "abM", 0, true},
		{"number_with_invalid_char", "1.2xM", 0, true},
		{"negative_value", "-10M", 0, true},
		{"space_around_num_suffix", "  10 M  ", 10 * BytesPerMegabit, false},
		{"space_around_num_only_as_bps", "  1200  ", 1200.0 / BitsInByte, false},
		{"space_in_number", "1 000 K", 0, true}, // Invalid: space within number part
		{"empty_numeric_part_k", "k", 0, true},
		{"empty_numeric_part_kbps", "kbps", 0, true},
		{"just_decimal_point_as_bps", ".", 0, true},
		{"zero_rate_with_suffix", "0M", 0.0, false}, // Explicit zero rate is unlimited
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBPS, err := ParseBandwidthString(tt.bwStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseBandwidthString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Use a small epsilon for float comparison
			if !tt.wantErr && math.Abs(gotBPS-tt.wantBPS) > 1e-9 {
				t.Errorf("ParseBandwidthString() gotBPS = %v, want %v", gotBPS, tt.wantBPS)
			}
		})
	}
}
