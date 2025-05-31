package config

import (
	"fmt"
	"os"
	"rabbitproxy/logging" // Added for logging
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/sirupsen/logrus" // Added for log level parsing
)

// LoadConfig reads a TOML file from filePath, parses it into a Config struct,
// and validates it.
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
// It uses logging.Logger (once initialized) for reporting non-critical issues like defaulting.
// Returns an error for critical validation failures that should prevent startup.
func ValidateConfig(cfg *Config) error {
	// Validate GlobalSettings
	if cfg.Global.LogLevel == "" {
		// This informational message will be logged by the default logrus instance if logger isn't fully init'd yet,
		// or by the configured logger if called after Init.
		logging.Logger.Info("Global LogLevel in config is empty, defaulting to 'info'.")
		cfg.Global.LogLevel = "info" // Default log level
	}
	// Validate the log level string using logrus's parser.
	// This ensures it's a level logrus understands.
	_, err := logrus.ParseLevel(strings.ToLower(cfg.Global.LogLevel))
	if err != nil {
		originalLevel := cfg.Global.LogLevel
		// If the provided log level is invalid, default to "info" and return an error
		// as this is a configuration error that the user should fix.
		// The logger initialization in main.go will also catch this and default,
		// but having it here ensures the cfg struct is corrected early.
		cfg.Global.LogLevel = "info" // Correct the level in the struct for safety
		return fmt.Errorf("invalid global.log_level: '%s', defaulted to 'info'. Valid levels: trace, debug, info, warn, error, fatal, panic. Parse error: %w",
			originalLevel, err)
	}

	// Validate TCPRules - Basic presence checks. Detailed parsing is done elsewhere.
	for i := range cfg.TCP {
		rule := &cfg.TCP[i] // Use pointer to modify the rule directly
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
			logging.Logger.Warnf("TCP rule '%s': max_connections is negative (%d), treating as unlimited (0).", ruleDesc, rule.MaxConnections)
			rule.MaxConnections = 0 // Normalize to 0 for unlimited
		}
		// Timeout parsing can also be done here if needed, e.g.
		if rule.Timeout != "" {
			parsedDur, err := time.ParseDuration(rule.Timeout)
			if err != nil {
				return fmt.Errorf("tcp rule '%s': invalid timeout string '%s': %w", ruleDesc, rule.Timeout, err)
			}
			rule.ParsedTimeout = parsedDur
		}
	}

	// Validate UDPRules - Basic presence checks
	for i := range cfg.UDP {
		rule := &cfg.UDP[i] // Use pointer to modify the rule directly
		ruleDesc := rule.Description
		if ruleDesc == "" {
			ruleDesc = fmt.Sprintf("UDP Rule #%d (Listen: %v, Target: %s)", i+1, rule.Listen, rule.Target)
		}
		if rule.Listen == nil {
			return fmt.Errorf("udp rule '%s': 'listen' field cannot be empty", ruleDesc)
		}
		if rule.Target == "" {
			return fmt.Errorf("udp rule '%s': 'target' field cannot be empty", ruleDesc)
		}
		if rule.MaxConnections < 0 {
			logging.Logger.Warnf("UDP rule '%s': max_connections is negative (%d), treating as unlimited (0).", ruleDesc, rule.MaxConnections)
			rule.MaxConnections = 0 // Normalize to 0 for unlimited
		}
	}
	// logging.Logger.Debug("Config validation successful.")
	return nil
}
