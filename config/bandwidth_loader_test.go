// config/bandwidth_loader_test.go
package config

import (
	"math"
	"reflect" // Used only if comparing complex structs directly, not needed for BandwidthSetting if checking fields
	"testing"

	"rabbitproxy/ratelimit" // For constants like BytesPerMegabit
	// "rabbitproxy/logging" // Not strictly needed for this test, as we check return values
)

const testEpsilon = 1e-9 // For float comparisons

func TestParseBandwidthEntryHelper(t *testing.T) {
	// This tests the parseBandwidthEntry helper directly.
	// It simulates how ValidateConfig would use this for TCPRule.ParsedBandwidth
	// or for entries in UDPRule.Bandwidth if it's a list/map.
	tests := []struct {
		name            string
		bwInput         interface{} // Simulates rule.Bandwidth field
		ruleDescription string      // For logging context in parse helpers
		wantParsed      BandwidthSetting
		wantErr         bool
	}{
		{
			name:            "simple_string_10M",
			bwInput:         "10M",
			ruleDescription: "tcp_simple_10M",
			wantParsed: BandwidthSetting{
				RateBPS:  10 * ratelimit.BytesPerMegabit,
				BurstBPS: 10 * ratelimit.BytesPerMegabit * defaultBurstFactor,
				IsSet:    true,
			},
			wantErr: false,
		},
		{
			name:            "advanced_map_200K_300K",
			bwInput:         map[string]interface{}{"rate": "200K", "burst": "300K"},
			ruleDescription: "tcp_adv_200K_300K",
			wantParsed: BandwidthSetting{
				RateBPS:  200 * ratelimit.BytesPerKilobit,
				BurstBPS: 300 * ratelimit.BytesPerKilobit,
				IsSet:    true,
			},
			wantErr: false,
		},
		{
			name:            "advanced_map_50m_no_burst", // 'm' for megabits
			bwInput:         map[string]interface{}{"rate": "50m"},
			ruleDescription: "tcp_adv_50m_no_burst",
			wantParsed: BandwidthSetting{
				RateBPS:  50 * ratelimit.BytesPerMegabit,
				BurstBPS: 50 * ratelimit.BytesPerMegabit * defaultBurstFactor,
				IsSet:    true,
			},
			wantErr: false,
		},
		{
			name:            "unlimited_string",
			bwInput:         "unlimited",
			ruleDescription: "tcp_unlimited_str",
			wantParsed:      BandwidthSetting{RateBPS: 0, BurstBPS: 0, IsSet: true},
			wantErr:         false,
		},
		{
			name:            "zero_string_rate",
			bwInput:         "0kbps",
			ruleDescription: "tcp_zero_str_rate",
			wantParsed:      BandwidthSetting{RateBPS: 0, BurstBPS: 0, IsSet: true},
			wantErr:         false,
		},
		{
			name:            "nil_bandwidth_input", // Simulates bandwidth key omitted
			bwInput:         nil,
			ruleDescription: "tcp_nil_bw",
			wantParsed:      BandwidthSetting{RateBPS: 0, BurstBPS: 0, IsSet: false},
			wantErr:         false,
		},
		{
			name:            "invalid_rate_string_in_map",
			bwInput:         map[string]interface{}{"rate": "200X"},
			ruleDescription: "tcp_err_invalid_rate_map",
			wantErr:         true,
		},
		{
			name:            "map_missing_rate_key",
			bwInput:         map[string]interface{}{"burst": "200K"},
			ruleDescription: "tcp_err_map_no_rate",
			wantErr:         true,
		},
		{
			name:            "map_rate_not_string",
			bwInput:         map[string]interface{}{"rate": 123},
			ruleDescription: "tcp_err_map_rate_not_str",
			wantErr:         true,
		},
		{
			name:            "map_burst_not_string",
			bwInput:         map[string]interface{}{"rate": "1M", "burst": 123},
			ruleDescription: "tcp_err_map_burst_not_str",
			wantErr:         true,
		},
		{
			name:            "invalid_type_for_bandwidth",
			bwInput:         12345, // e.g. int instead of string or map
			ruleDescription: "tcp_err_invalid_bw_type",
			wantErr:         true,
		},
		{
			name:            "burst_smaller_than_rate_adjust",
			bwInput:         map[string]interface{}{"rate": "100M", "burst": "50M"},
			ruleDescription: "adjust_burst",
			wantParsed: BandwidthSetting{
				RateBPS:  100 * ratelimit.BytesPerMegabit,
				BurstBPS: 100 * ratelimit.BytesPerMegabit, // Adjusted to rate
				IsSet:    true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotParsed, err := parseBandwidthEntry(tt.bwInput, tt.ruleDescription)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseBandwidthEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if math.Abs(gotParsed.RateBPS-tt.wantParsed.RateBPS) > testEpsilon ||
					math.Abs(gotParsed.BurstBPS-tt.wantParsed.BurstBPS) > testEpsilon ||
					gotParsed.IsSet != tt.wantParsed.IsSet {
					t.Errorf("parseBandwidthEntry() mismatch: got %+v, want %+v", gotParsed, tt.wantParsed)
				}
			}
		})
	}
}

// Note: Testing the full ValidateConfig for UDPRule's per-port bandwidth list
// would be more involved, requiring construction of []interface{} with maps.
// The TestParseBandwidthEntryHelper above covers the core logic used for each entry.
// A dedicated test for UDPRule's list processing in ValidateConfig could be added
// for completeness if desired, by creating a dummy Config struct and calling ValidateConfig.
// For example:
/*
func TestValidateConfig_UDPRule_PerPortBandwidth(t *testing.T) {
	rule := UDPRule{
		Description: "udp_per_port_test",
		Listen: []interface{}{int64(9000), int64(9001)}, // Need some listen ports for context
		Target: "127.0.0.1:8000",
		Bandwidth: []interface{}{
			map[string]interface{}{"port": int64(9000), "rate": "1M"},
			map[string]interface{}{"port": int64(9001), "rate": "2M", "burst": "3M"},
			map[string]interface{}{"port": int64(9002), "rate": "invalid"}, // This port isn't in Listen
		},
	}
	cfg := Config{UDP: []UDPRule{rule}}

	// Need to initialize logging.S for potential warnings inside parse helpers,
	// or ensure those warnings are also tested/suppressed.
	// logging.Init(GlobalSettings{LogLevel: "debug"}, false, "") // Example init

	err := ValidateConfig(&cfg) // This will modify cfg.UDP[0].ParsedPortBandwidths

	if err == nil { // Expecting error due to "invalid" rate for port 9002
		// Or, if the test case was valid, check cfg.UDP[0].ParsedPortBandwidths
		// t.Errorf("Expected validation error for invalid rate in per-port UDP bandwidth, got nil")
	}
    // Example check for valid case (if the "invalid" was removed):
	// want9000 := BandwidthSetting{RateBPS: 1*ratelimit.BytesPerMegabit, BurstBPS: 1*ratelimit.BytesPerMegabit*defaultBurstFactor, IsSet: true}
	// got9000, ok9000 := cfg.UDP[0].ParsedPortBandwidths[9000]
	// if !ok9000 || !reflect.DeepEqual(got9000, want9000) { // Custom compare for floats needed
	//    t.Errorf("Port 9000 bandwidth: got %+v, want %+v", got9000, want9000)
	// }
}
*/
