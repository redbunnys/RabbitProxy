package config

import (
	"rabbitproxy/ratelimit" // Import your new ratelimit package
	"time"
)

// BandwidthSetting holds parsed rate and burst values in Bytes Per Second.
type BandwidthSetting struct {
	RateBPS  float64 // Bytes per second
	BurstBPS float64 // Bytes
	IsSet    bool    // True if bandwidth was configured for this rule/port
}

// PortSpecificBandwidth allows defining bandwidth for a specific port in a multi-port UDP rule.
// This struct is primarily for TOML parsing.
type PortSpecificBandwidth struct {
	Port  int    `toml:"port"`
	Rate  string `toml:"rate"`            // e.g., "10M", "500K", "unlimited"
	Burst string `toml:"burst,omitempty"` // Optional, e.g., "15M"
}

// AdvancedBandwidthConfig allows specifying rate and burst explicitly.
// This struct is primarily for TOML parsing.
type AdvancedBandwidthConfig struct {
	Rate  string `toml:"rate"`
	Burst string `toml:"burst,omitempty"`
}

// Config represents the overall configuration structure.
type Config struct {
	Global GlobalSettings `toml:"global"`
	TCP    []TCPRule      `toml:"tcp"`
	UDP    []UDPRule      `toml:"udp"`
}

// GlobalSettings defines global application settings.
type GlobalSettings struct {
	LogLevel    string `toml:"log_level"`
	LogFilePath string `toml:"log_file_path,omitempty"` // Path for file-based logging
	// EnableSyslog bool `toml:"enable_syslog,omitempty"` // If made configurable
}

// TCPRule defines a forwarding rule for TCP traffic.
type TCPRule struct {
	Listen         interface{} `toml:"listen"`
	Target         string      `toml:"target"`
	Description    string      `toml:"description,omitempty"`
	Timeout        string      `toml:"timeout,omitempty"`
	ParsedTimeout  time.Duration `toml:"-"` // Populated by loader
	MaxConnections int         `toml:"max_connections,omitempty"`
	Bandwidth      interface{} `toml:"bandwidth,omitempty"` // e.g., "10M" OR { rate="10M", burst="12M" }
	ParsedBandwidth BandwidthSetting `toml:"-"` // Populated by loader
}

// UDPRule defines a forwarding rule for UDP traffic.
type UDPRule struct {
	Listen         interface{} `toml:"listen"`
	Target         string      `toml:"target"`
	Description    string      `toml:"description,omitempty"`
	MaxConnections int         `toml:"max_connections,omitempty"`
	// Bandwidth can be simple string, advanced config, or list of per-port configs.
	Bandwidth             interface{}            `toml:"bandwidth,omitempty"` // "10M" OR {rate="10M"} OR [ {port=5000, rate="1M"}, ... ]
	ParsedRuleBandwidth   BandwidthSetting       `toml:"-"` // Populated by loader if rule-wide bw is set
	ParsedPortBandwidths map[int]BandwidthSetting `toml:"-"` // Populated by loader if per-port bw is set
}
