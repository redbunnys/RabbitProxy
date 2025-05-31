package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// ParsedPort represents a single port to listen on or target.
// Host is typically empty for listen, meaning all interfaces.
type ParsedPort struct {
	Host string
	Port int
}

// ParseListenEntry takes a listen entry (int, string, or []interface{}) from a rule
// and returns a slice of ParsedPort and an error if parsing fails.
// ruleType is "tcp" or "udp", used for informative error messages.
func ParseListenEntry(listenEntry interface{}, ruleType string) ([]ParsedPort, error) {
	ports := []ParsedPort{}

	switch v := listenEntry.(type) {
	case int: // Handles TOML integer, often decoded as int by some libraries
		if v <= 0 || v > 65535 {
			return nil, fmt.Errorf("invalid port %d for %s rule listen entry", v, ruleType)
		}
		ports = append(ports, ParsedPort{Port: v})
	case int64: // Common for TOML integers
		portVal := int(v)
		if v <= 0 || v > 65535 {
			return nil, fmt.Errorf("invalid port %d for %s rule listen entry", v, ruleType)
		}
		ports = append(ports, ParsedPort{Port: portVal})
	case float64: // TOML numbers might be parsed as float64
		portInt := int(v)
		if float64(portInt) != v || portInt <= 0 || portInt > 65535 {
			return nil, fmt.Errorf("invalid or non-integer port %f for %s rule listen entry", v, ruleType)
		}
		ports = append(ports, ParsedPort{Port: portInt})
	case string:
		trimmedVal := strings.TrimSpace(v)
		if strings.Contains(trimmedVal, "-") { // Port range "start-end"
			parts := strings.SplitN(trimmedVal, "-", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid port range format '%s' for %s rule listen entry", v, ruleType)
			}
			startPort, err := strconv.Atoi(strings.TrimSpace(parts[0]))
			if err != nil {
				return nil, fmt.Errorf("invalid start port in range '%s' for %s rule listen entry: %w", v, ruleType, err)
			}
			endPort, err := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err != nil {
				return nil, fmt.Errorf("invalid end port in range '%s' for %s rule listen entry: %w", v, ruleType, err)
			}
			if startPort <= 0 || startPort > 65535 || endPort <= 0 || endPort > 65535 || startPort > endPort {
				return nil, fmt.Errorf("invalid port range %d-%d for %s rule listen entry", startPort, endPort, ruleType)
			}
			for i := startPort; i <= endPort; i++ {
				ports = append(ports, ParsedPort{Port: i})
			}
		} else { // Single port as string "port"
			port, err := strconv.Atoi(trimmedVal)
			if err != nil {
				return nil, fmt.Errorf("invalid port string '%s' for %s rule listen entry: %w", v, ruleType, err)
			}
			if port <= 0 || port > 65535 {
				return nil, fmt.Errorf("invalid port %d for %s rule listen entry", port, ruleType)
			}
			ports = append(ports, ParsedPort{Port: port})
		}
	case []interface{}: // Array of ports [port1, port2, port3]
		if len(v) == 0 {
			return nil, fmt.Errorf("empty port array for %s rule listen entry", ruleType)
		}
		for i, item := range v {
			var portInt int
			switch pVal := item.(type) {
			case int:
				portInt = pVal
			case int64:
				portInt = int(pVal)
			case float64:
				fPortInt := int(pVal)
				if float64(fPortInt) != pVal {
					return nil, fmt.Errorf("non-integer port %f in array (item %d) for %s rule listen entry", pVal, i, ruleType)
				}
				portInt = fPortInt
			default:
				return nil, fmt.Errorf("invalid type %T in port array (item %d) for %s rule listen entry, expected integer", item, i, ruleType)
			}

			if portInt <= 0 || portInt > 65535 {
				return nil, fmt.Errorf("invalid port %d in array (item %d) for %s rule listen entry", portInt, i, ruleType)
			}
			ports = append(ports, ParsedPort{Port: portInt})
		}
	default:
		return nil, fmt.Errorf("unsupported listen type '%T' for %s rule listen entry: %v", listenEntry, ruleType, listenEntry)
	}

	if len(ports) == 0 {
		return nil, fmt.Errorf("no valid listen ports found for %s rule with listen entry: %v", ruleType, listenEntry)
	}
	return ports, nil
}

// ParseTarget takes a target string (e.g., "host:port", "host:start-end")
// and the number of listen ports (relevant for range-to-range or multi-to-range mapping).
// ruleType is "tcp" or "udp", used for informative error messages.
// Returns a slice of ParsedPort.
func ParseTarget(targetStr string, numListenPorts int, ruleType string) ([]ParsedPort, error) {
	host, portStr, err := net.SplitHostPort(targetStr)
	if err != nil {
		return nil, fmt.Errorf("invalid target format '%s' for %s rule: %w", targetStr, ruleType, err)
	}
	if strings.TrimSpace(host) == "" { // Allow host to be empty if it means localhost or similar contextually, but for remote targets it must be specified.
									   // For this proxy, target host must be explicit.
		return nil, fmt.Errorf("target host cannot be empty in '%s' for %s rule", targetStr, ruleType)
	}
	host = strings.TrimSpace(host)


	targets := []ParsedPort{}
	trimmedPortStr := strings.TrimSpace(portStr)
	if strings.Contains(trimmedPortStr, "-") { // Port range "host:start-end"
		parts := strings.SplitN(trimmedPortStr, "-", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid target port range format '%s' for %s rule", portStr, ruleType)
		}
		startPort, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid start port in target range '%s' for %s rule: %w", portStr, ruleType, err)
		}
		endPort, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid end port in target range '%s' for %s rule: %w", portStr, ruleType, err)
		}
		if startPort <= 0 || startPort > 65535 || endPort <= 0 || endPort > 65535 || startPort > endPort {
			return nil, fmt.Errorf("invalid target port range %d-%d for %s rule", startPort, endPort, ruleType)
		}

		numTargetPorts := endPort - startPort + 1
		// If listen side has multiple ports (range or array) and target is a range, counts must match for N:N mapping.
		if numListenPorts > 1 && numTargetPorts > 1 && numTargetPorts != numListenPorts {
			return nil, fmt.Errorf("listen port count (%d) does not match target port count (%d) for %s rule with target range '%s'", numListenPorts, numTargetPorts, ruleType, targetStr)
		}
		// If listen side has single port, but target is a range, this is an error (1:N is not supported this way)
		if numListenPorts == 1 && numTargetPorts > 1 {
			return nil, fmt.Errorf("single listen port cannot map to a target port range ('%s') for %s rule; target must be a single port", targetStr, ruleType)
		}


		for i := startPort; i <= endPort; i++ {
			targets = append(targets, ParsedPort{Host: host, Port: i})
		}
	} else { // Single port "host:port"
		port, err := strconv.Atoi(trimmedPortStr)
		if err != nil {
			return nil, fmt.Errorf("invalid target port string '%s' for %s rule: %w", portStr, ruleType, err)
		}
		if port <= 0 || port > 65535 {
			return nil, fmt.Errorf("invalid target port %d for %s rule", port, ruleType)
		}
		targets = append(targets, ParsedPort{Host: host, Port: port})
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("no valid target ports found for %s rule with target entry: %s", ruleType, targetStr)
	}
	return targets, nil
}
