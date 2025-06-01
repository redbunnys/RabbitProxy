// config/portparser_test.go
package config

import (
	"reflect"
	"testing"
)

func TestParseListenEntry(t *testing.T) {
	tests := []struct {
		name      string
		listenArg interface{}
		ruleType  string
		wantPorts []ParsedPort
		wantErr   bool
	}{
		{name: "single_int_tcp", listenArg: int(80), ruleType: "tcp", wantPorts: []ParsedPort{{Port: 80}}, wantErr: false},
		{name: "single_int64_udp", listenArg: int64(8080), ruleType: "udp", wantPorts: []ParsedPort{{Port: 8080}}, wantErr: false},
		{name: "single_float64_tcp", listenArg: float64(53.0), ruleType: "tcp", wantPorts: []ParsedPort{{Port: 53}}, wantErr: false},
		{name: "single_float64_decimal_error", listenArg: float64(53.5), ruleType: "udp", wantErr: true},
		{name: "single_str_port_tcp", listenArg: "8081", ruleType: "tcp", wantPorts: []ParsedPort{{Port: 8081}}, wantErr: false},
		{name: "range_str_udp", listenArg: "9000-9002", ruleType: "udp", wantPorts: []ParsedPort{{Port: 9000}, {Port: 9001}, {Port: 9002}}, wantErr: false},
		{name: "list_int_tcp", listenArg: []interface{}{int64(5000), int64(5001)}, ruleType: "tcp", wantPorts: []ParsedPort{{Port: 5000}, {Port: 5001}}, wantErr: false},
		{name: "list_mixed_types_udp", listenArg: []interface{}{int(5002), int64(5003), float64(5004.0)}, ruleType: "udp", wantPorts: []ParsedPort{{Port: 5002}, {Port: 5003}, {Port: 5004}}, wantErr: false},
		{name: "empty_str_error_tcp", listenArg: "", ruleType: "tcp", wantErr: true}, // ParseListenEntry expects non-empty if string
		{name: "invalid_range_udp", listenArg: "9000-8000", ruleType: "udp", wantErr: true},
		{name: "invalid_range_format_tcp", listenArg: "9000--9002", ruleType: "tcp", wantErr: true},
		{name: "invalid_port_in_range_udp", listenArg: "9000-900a", ruleType: "udp", wantErr: true},
		{name: "invalid_type_in_list_tcp", listenArg: []interface{}{"abc"}, ruleType: "tcp", wantErr: true},
		{name: "empty_list_udp", listenArg: []interface{}{}, ruleType: "udp", wantErr: true},
		{name: "port_zero_error_tcp", listenArg: int64(0), ruleType: "tcp", wantErr: true},
		{name: "port_too_high_error_udp", listenArg: "65536", ruleType: "udp", wantErr: true},
		{name: "unsupported_type_tcp", listenArg: true, ruleType: "tcp", wantErr: true},
		{name: "list_with_invalid_float_udp", listenArg: []interface{}{float64(5005.5)}, ruleType: "udp", wantErr: true},
		{name: "single_str_port_with_spaces_tcp", listenArg: " 8082 ", ruleType: "tcp", wantPorts: []ParsedPort{{Port: 8082}}, wantErr: false},
		{name: "range_str_with_spaces_udp", listenArg: " 9003 - 9005 ", ruleType: "udp", wantPorts: []ParsedPort{{Port: 9003}, {Port: 9004}, {Port: 9005}}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseListenEntry(tt.listenArg, tt.ruleType)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseListenEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.wantPorts) {
				t.Errorf("ParseListenEntry() = %v, want %v", got, tt.wantPorts)
			}
		})
	}
}

func TestParseTarget(t *testing.T) {
	tests := []struct {
		name            string
		targetStr       string
		numListenPorts  int
		ruleType        string
		wantParsedPorts []ParsedPort
		wantErr         bool
	}{
		{"single_host_port", "127.0.0.1:8000", 1, "tcp", []ParsedPort{{Host: "127.0.0.1", Port: 8000}}, false},
		{"single_host_port_ipv6", "[::1]:8001", 1, "udp", []ParsedPort{{Host: "::1", Port: 8001}}, false},
		{"host_port_range_1_to_1", "example.com:9000-9000", 1, "tcp", []ParsedPort{{Host: "example.com", Port: 9000}}, false},
		{"host_port_range_N_to_N", "example.com:9000-9002", 3, "tcp", []ParsedPort{{Host: "example.com", Port: 9000}, {Host: "example.com", Port: 9001}, {Host: "example.com", Port: 9002}}, false},
		{"host_port_range_N_to_M_mismatch_error", "example.com:9000-9002", 2, "udp", nil, true}, // 3 target ports, 2 listen ports
		{"single_listen_to_target_range_error", "example.com:9000-9002", 1, "tcp", nil, true}, // 1 listen port, 3 target ports
		{"invalid_target_format_no_port", "127.0.0.1", 1, "tcp", nil, true},
		{"invalid_target_format_no_host", ":8000", 1, "udp", nil, true},
		{"invalid_port_in_range", "host:800a-8002", 1, "tcp", nil, true},
		{"invalid_range_numbers", "host:9000-8000", 1, "udp", nil, true},
		{"target_port_zero", "host:0", 1, "tcp", nil, true},
		{"target_port_too_high", "host:65536", 1, "udp", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTarget(tt.targetStr, tt.numListenPorts, tt.ruleType)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTarget() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.wantParsedPorts) {
				t.Errorf("ParseTarget() = %v, want %v", got, tt.wantParsedPorts)
			}
		})
	}
}
