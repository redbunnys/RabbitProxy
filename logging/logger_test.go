// logging/logger_test.go
package logging

import (
	"os"
	"testing"
	"rabbitproxy/config" // Assuming module path
	"go.uber.org/zap" // For re-initializing default logger if needed
)

func TestInit_BasicInitialization(t *testing.T) {
	// Store original loggers to restore them later if needed, though these tests modify globals.
	originalL := L
	originalS := S
	defer func() {
		L = originalL
		S = originalS
	}()

	cfg := config.GlobalSettings{
		LogLevel:    "info",
		LogFilePath: "", // Test without file logging for this case
	}
	enableSyslog := false // Test without syslog for basic init test

	err := Init(cfg, enableSyslog, cfg.LogFilePath)
	if err != nil {
		// Init currently doesn't return errors, it logs them.
		// This check is more for future-proofing if Init's signature changes.
		t.Fatalf("Init() error = %v, wantErr false", err)
	}

	if L == nil {
		t.Error("Global logger L is nil after Init()")
	}
	if S == nil {
		t.Error("Global sugared logger S is nil after Init()")
	}

	// Test if logging a message works (optional, but good smoke test)
	// This primarily checks if the call itself panics.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("S.Info() after Init() panicked: %v", r)
			}
		}()
		S.Info("Test log message after basic initialization (S.Info).")
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("L.Info() after Init() panicked: %v", r)
			}
		}()
		L.Info("Test structured log message after basic initialization (L.Info).")
	}()
}

func TestInit_WithFileAndSyslog(t *testing.T) {
	originalL := L
	originalS := S
	defer func() {
		L = originalL
		S = originalS
	}()

	// This test is more about checking if Init runs without error with these enabled,
	// not about verifying actual file/syslog output content.
	testLogFileName := "test_init_output.log"
	cfg := config.GlobalSettings{
		LogLevel:    "debug",
		LogFilePath: testLogFileName,
	}
	// Attempt to init syslog. Syslog connection can fail in CI environments.
	// The Init function itself prints to os.Stderr if syslog connection fails,
	// but doesn't return an error for that specific case.
	enableSyslog := true

	err := Init(cfg, enableSyslog, cfg.LogFilePath)
	if err != nil {
		// This would be for errors Init decides are fatal to its own operation,
		// not necessarily for syslog/file write issues which it logs.
		t.Fatalf("Init() with file and syslog returned an unexpected error: %v", err)
	}

	if L == nil {
		t.Error("Global logger L is nil after Init() with file/syslog")
	}
	if S == nil {
		t.Error("Global sugared logger S is nil after Init() with file/syslog")
	}

	// Log some messages to test usability
	S.Debugf("Test debug message after init with file/syslog. Log file: %s", testLogFileName)
	L.Info("Test structured info message with file/syslog", zap.String("file", testLogFileName))

	// Clean up test log file
	// Check if file exists before attempting to remove
	if _, statErr := os.Stat(testLogFileName); statErr == nil {
		removeErr := os.Remove(testLogFileName)
		if removeErr != nil {
			t.Logf("Warning: could not remove test log file '%s': %v", testLogFileName, removeErr)
		}
	} else if !os.IsNotExist(statErr) {
		t.Logf("Warning: could not stat test log file '%s' before removal: %v", testLogFileName, statErr)
	}
}

func TestInit_InvalidLogLevel(t *testing.T) {
	originalL := L
	originalS := S
	defer func() {
		L = originalL
		S = originalS
	}()

	cfg := config.GlobalSettings{
		LogLevel: "invalidlevel", // This should be defaulted by Init
	}
	// Redirect os.Stderr to check for the warning message from Init
	// This is a bit more involved; for now, we'll just check if the level defaults to Info.
	// A simpler check is that Init doesn't error out and sets a default level.

	err := Init(cfg, false, "")
	if err != nil {
		t.Fatalf("Init() with invalid log level errored: %v", err)
	}
	if L.Core().Enabled(zap.DebugLevel) { // Default is Info, so Debug should be disabled
		t.Errorf("With invalid level, expected InfoLevel (Debug disabled), but Debug is enabled.")
	}
	if !L.Core().Enabled(zap.InfoLevel) { // Info should be enabled
		t.Errorf("With invalid level, expected InfoLevel, but Info is disabled.")
	}
}
