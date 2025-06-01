package config

import (
	"fmt"
	"os"
	"rabbitproxy/logging" // Using Zap
	"rabbitproxy/ratelimit" // For ParseBandwidthString
	"strings"
	"time" // For time.ParseDuration

	"github.com/BurntSushi/toml"
	"go.uber.org/zap/zapcore" // For log level parsing with Zap
)

const defaultBurstFactor = 1.5

// parseSingleBandwidthConfig parses a rate string and an optional burst string
func parseSingleBandwidthConfig(rateStr, burstStr string, ruleDescForLog string) (BandwidthSetting, error) {
	setting := BandwidthSetting{IsSet: true} // Mark as set if we attempt parsing
	var err error

	setting.RateBPS, err = ratelimit.ParseBandwidthString(rateStr)
	if err != nil {
		return BandwidthSetting{}, fmt.Errorf("invalid rate string '%s': %w", rateStr, err)
	}

	if burstStr != "" {
		setting.BurstBPS, err = ratelimit.ParseBandwidthString(burstStr)
		if err != nil {
			return BandwidthSetting{}, fmt.Errorf("invalid burst string '%s': %w", burstStr, err)
		}
	} else {
		if setting.RateBPS > 0 {
			setting.BurstBPS = setting.RateBPS * defaultBurstFactor
		} else {
			setting.BurstBPS = 0 // Unlimited rate implies effectively unlimited burst
		}
	}

	// Validate that burst is not smaller than rate (if rate is positive)
	if setting.RateBPS > 0 && setting.BurstBPS < setting.RateBPS {
		logging.S.Warnf("Rule '%s': Bandwidth burst (%.2f Bps) is less than rate (%.2f Bps) for rate string '%s'. Adjusting burst to be equal to the rate.",
			ruleDescForLog, setting.BurstBPS, setting.RateBPS, rateStr)
		setting.BurstBPS = setting.RateBPS // Adjust burst to be at least the rate
	}
	if setting.RateBPS == 0 { // Normalize for "unlimited" or explicit "0" rate
		setting.BurstBPS = 0
		// IsSet remains true if "unlimited" or "0" was explicitly provided.
	}

	return setting, nil
}

// parseBandwidthEntry parses a bandwidth configuration entry which can be a string or a map.
func parseBandwidthEntry(entry interface{}, ruleDescForLog string) (BandwidthSetting, error) {
	if entry == nil {
		return BandwidthSetting{RateBPS: 0, BurstBPS: 0, IsSet: false}, nil // Not set, effectively unlimited
	}

	switch v := entry.(type) {
	case string: // Simple rate string, e.g., "10M"
		return parseSingleBandwidthConfig(v, "", ruleDescForLog)
	case map[string]interface{}: // Advanced config: { rate="10M", burst="15M" }
		// TOML decoding often results in map[string]interface{}.
		// We need to safely access "rate" and "burst".
		rateValStr, rateOk := v["rate"].(string)
		if !rateOk {
			return BandwidthSetting{}, fmt.Errorf("bandwidth config map %v for rule '%s' missing 'rate' field or it's not a string", v, ruleDescForLog)
		}

		var burstValStr string
		if burstInterface, burstExists := v["burst"]; burstExists {
			burstValStr, burstOk = burstInterface.(string)
			if !burstOk {
				return BandwidthSetting{}, fmt.Errorf("bandwidth config map %v for rule '%s' has 'burst' field that is not a string", v, ruleDescForLog)
			}
		}
		return parseSingleBandwidthConfig(rateValStr, burstValStr, ruleDescForLog)
	default:
		// This case might also be hit if TOML parsing results in a different map type, e.g. map[interface{}]interface{}
		// For robust handling, one might use reflection or a library like mapstructure.
		// However, BurntSushi/toml usually gives map[string]interface{} for tables.
		return BandwidthSetting{}, fmt.Errorf("invalid bandwidth type for rule '%s': expected string or map, got %T", ruleDescForLog, entry)
	}
}


// LoadConfig reads a TOML file from filePath, parses it into a Config struct, and validates it.
func LoadConfig(filePath string) (*Config, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	var cfg Config
	if _, err := toml.Decode(string(content), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse TOML config from %s: %w", filePath, err)
	}

	if err := ValidateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}
	return &cfg, nil
}

// ValidateConfig checks the integrity and correctness of the loaded configuration.
func ValidateConfig(cfg *Config) error {
	// Validate GlobalSettings
	if cfg.Global.LogLevel == "" {
		logging.S.Info("Global LogLevel in config is empty, defaulting to 'info'.")
		cfg.Global.LogLevel = "info"
	}

	var parsedLevel zapcore.Level
	err := parsedLevel.Set(strings.ToLower(cfg.Global.LogLevel))
	if err != nil {
		originalLevel := cfg.Global.LogLevel
		cfg.Global.LogLevel = "info"
		return fmt.Errorf("invalid global.log_level: '%s', defaulted to 'info'. Valid levels (case-insensitive): debug, info, warn, error, dpanic, panic, fatal. Parse error: %w",
			originalLevel, err)
	}

	// Validate TCPRules
	for i := range cfg.TCP {
		rule := &cfg.TCP[i]
		ruleDesc := rule.Description
		if ruleDesc == "" {
			ruleDesc = fmt.Sprintf("TCP Rule #%d (Listen: %v, Target: %s)", i+1, rule.Listen, rule.Target)
		}
		if rule.Listen == nil {
			return fmt.Errorf("tcp rule '%s': 'listen' field cannot be empty", ruleDesc)
		}
		if rule.Target == "" {
			return fmt.Errorf("tcp rule '%s': 'target' field cannot be empty", ruleDesc)
		}
		if rule.MaxConnections < 0 {
			logging.S.Warnf("TCP rule '%s': max_connections is negative (%d), treating as unlimited (0).", ruleDesc, rule.MaxConnections)
			rule.MaxConnections = 0
		}
		if rule.Timeout != "" {
			parsedDur, errTimeout := time.ParseDuration(rule.Timeout)
			if errTimeout != nil {
				return fmt.Errorf("tcp rule '%s': invalid timeout string '%s': %w", ruleDesc, rule.Timeout, errTimeout)
			}
			rule.ParsedTimeout = parsedDur
		} else {
			rule.ParsedTimeout = 30 * time.Second // Example default TCP timeout
		}

		// Parse Bandwidth for TCPRule
		if rule.Bandwidth != nil {
			bs, errBw := parseBandwidthEntry(rule.Bandwidth, ruleDesc)
			if errBw != nil {
				return fmt.Errorf("failed to parse bandwidth for TCP rule '%s': %w", ruleDesc, errBw)
			}
			rule.ParsedBandwidth = bs
		} else {
			rule.ParsedBandwidth = BandwidthSetting{IsSet: false} // Explicitly mark as not set
		}
	}

	// Validate UDPRules
	for i := range cfg.UDP {
		rule := &cfg.UDP[i]
		ruleDesc := rule.Description
		if ruleDesc == "" {
			ruleDesc = fmt.Sprintf("UDP Rule #%d (Listen: %v, Target: %s)", i+1, rule.Listen, rule.Target)
		}
		rule.ParsedPortBandwidths = make(map[int]BandwidthSetting) // Initialize map

		if rule.Listen == nil {
			return fmt.Errorf("udp rule '%s': 'listen' field cannot be empty", ruleDesc)
		}
		if rule.Target == "" {
			return fmt.Errorf("udp rule '%s': 'target' field cannot be empty", ruleDesc)
		}
		if rule.MaxConnections < 0 {
			logging.S.Warnf("UDP rule '%s': max_connections is negative (%d), treating as unlimited (0).", ruleDesc, rule.MaxConnections)
			rule.MaxConnections = 0
		}

		// Parse Bandwidth for UDPRule
		if rule.Bandwidth != nil {
			switch bwConf := rule.Bandwidth.(type) {
			case string, map[string]interface{}: // Rule-wide config
				bs, errBw := parseBandwidthEntry(bwConf, ruleDesc)
				if errBw != nil {
					return fmt.Errorf("failed to parse rule-wide bandwidth for UDP rule '%s': %w", ruleDesc, errBw)
				}
				rule.ParsedRuleBandwidth = bs
			case []interface{}: // Per-port config list
				for itemIdx, item := range bwConf {
					portConfMap, ok := item.(map[string]interface{})
					if !ok {
						return fmt.Errorf("udp rule '%s': item #%d in bandwidth list is not a map (TOML table)", ruleDesc, itemIdx+1)
					}

					portValInterface, portOk := portConfMap["port"]
					if !portOk {
						return fmt.Errorf("udp rule '%s': item #%d in bandwidth list missing 'port' field", ruleDesc, itemIdx+1)
					}
					portInt64, typeOk := portValInterface.(int64) // TOML integers are often int64
					if !typeOk {
						// Try float64 then convert, as TOML numbers can sometimes be float
						portFloat64, fOk := portValInterface.(float64)
						if fOk {
							portInt64 = int64(portFloat64)
							if float64(portInt64) != portFloat64 { // Check if it was a whole number
								return fmt.Errorf("udp rule '%s': port '%v' in bandwidth list item #%d is a non-integer float", ruleDesc, portValInterface, itemIdx+1)
							}
						} else {
							return fmt.Errorf("udp rule '%s': port '%v' in bandwidth list item #%d is not a valid integer type (expected int64 or float64)", ruleDesc, portValInterface, itemIdx+1)
						}
					}
					if portInt64 <= 0 || portInt64 > 65535 {
						return fmt.Errorf("udp rule '%s': port %d in bandwidth list item #%d is out of valid range (1-65535)", ruleDesc, portInt64, itemIdx+1)
					}
					portInt := int(portInt64)

					rateStr, rateOk := portConfMap["rate"].(string)
					if !rateOk {
						return fmt.Errorf("udp rule '%s' port %d: missing 'rate' string in bandwidth config item #%d", ruleDesc, portInt, itemIdx+1)
					}
					burstStr, _ := portConfMap["burst"].(string) // Burst is optional

					pbs, errPbs := parseSingleBandwidthConfig(rateStr, burstStr, fmt.Sprintf("%s port %d", ruleDesc, portInt))
					if errPbs != nil {
						return fmt.Errorf("udp rule '%s' port %d bandwidth config item #%d: %w", ruleDesc, portInt, itemIdx+1, errPbs)
					}
					if _, exists := rule.ParsedPortBandwidths[portInt]; exists {
						logging.S.Warnf("UDP rule '%s': duplicate bandwidth configuration for port %d. Overwriting with last entry.", ruleDesc, portInt)
					}
					rule.ParsedPortBandwidths[portInt] = pbs
				}
			default:
				return fmt.Errorf("invalid bandwidth configuration type for UDP rule '%s': %T", ruleDesc, rule.Bandwidth)
			}
		} else { // No bandwidth key for UDP rule
			rule.ParsedRuleBandwidth = BandwidthSetting{IsSet: false} // Default to unlimited for the rule
		}
	}
	return nil
}
