package logging

import (
	"io"
	"log/syslog" // For Syslog hook
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
	"rabbitproxy/config" // Assuming this is your module path
)

// Logger is the global logger instance
var Logger = logrus.New()

// Init initializes the global logger based on configuration.
func Init(cfg config.GlobalSettings, enableSyslog bool) error {
	// Default to a TextFormatter, could be made configurable (e.g., JSONFormatter)
	Logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		TimestampFormat: "2006-01-02 15:04:05", // More standard timestamp
	})

	// Set output to stdout by default
	Logger.SetOutput(os.Stdout)

	// Parse and set log level
	level, err := logrus.ParseLevel(strings.ToLower(cfg.LogLevel))
	if err != nil || cfg.LogLevel == "" { // Also treat empty log level string as needing default
		// Default to InfoLevel if parsing fails or level is empty, and log a warning
		if cfg.LogLevel != "" { // Only warn if a value was provided but was invalid
			Logger.Warnf("Invalid log_level '%s' in config, defaulting to 'info'. Error: %v", cfg.LogLevel, err)
		} else {
			Logger.Infof("log_level is empty in config, defaulting to 'info'.")
		}
		Logger.SetLevel(logrus.InfoLevel)
	} else {
		Logger.SetLevel(level)
	}

	// Initial log message using the configured level (if possible)
	// This message might not appear if the level is set higher than INFO and this is the first message.
	// Logger.Infof("Logger set to level: %s", Logger.GetLevel().String())


	if enableSyslog {
		// The third parameter to NewSyslogHook is a syslog.Priority.
		// This is ORed with the syslog severity of the message.
		// We pass 0 here to let logrus determine the priority based on the message level.
		// The last parameter is the tag, if empty, os.Args[0] is used.
		hook, err_syslog := logrus_syslog.NewSyslogHook("", "", 0, "")
		if err_syslog != nil {
			Logger.Errorf("Unable to connect to local syslog daemon: %v", err_syslog)
		} else {
			Logger.AddHook(hook)
			// This message will go to both stdout and syslog if connection was successful.
			Logger.Info("Syslog hook added. Application logs may also be sent to Syslog.")
			// If you want to disable stdout logging when syslog is active:
			// Logger.SetOutput(io.Discard)
		}
	}
	// A definitive message that logger is initialized, will respect the set level.
	Logger.Logf(Logger.GetLevel(), "Logger initialized. Current log level: %s. Syslog enabled: %t", Logger.GetLevel().String(), enableSyslog && hook != nil)
	return nil // Init itself doesn't return an error from its own logic, only logs them.
}
