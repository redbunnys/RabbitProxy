package config

// Config represents the overall configuration structure.
type Config struct {
	Global GlobalSettings `toml:"global"`
	TCP    []TCPRule      `toml:"tcp"`
	UDP    []UDPRule      `toml:"udp"`
}

// GlobalSettings defines global application settings.
type GlobalSettings struct {
	LogLevel string `toml:"log_level"`
}

import "time"

// TCPRule defines a forwarding rule for TCP traffic.
type TCPRule struct {
	Listen         interface{}   `toml:"listen"` // Can be int, string (for range), or []int
	Target         string        `toml:"target"`
	Description    string        `toml:"description,omitempty"`
	Timeout        string        `toml:"timeout,omitempty"` // e.g., "30s", "1m"
	ParsedTimeout  time.Duration `toml:"-"`                 // Ignored by TOML, populated after parsing Timeout string
	MaxConnections int           `toml:"max_connections,omitempty"` // 0 or negative means unlimited
}

// UDPRule defines a forwarding rule for UDP traffic.
type UDPRule struct {
	Listen         interface{} `toml:"listen"` // Can be int, string (for range), or []int
	Target         string      `toml:"target"`
	Description    string      `toml:"description,omitempty"`
	MaxConnections int         `toml:"max_connections,omitempty"` // Max active sessions; 0 or negative means unlimited
}
