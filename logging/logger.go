package logging

import (
	"fmt"
	"os"
	"strings"
	"log/syslog" // For standard syslog levels for zapsyslog

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/exp/zapsyslog" // For syslog hook
	"gopkg.in/natefinch/lumberjack.v2" // For file rotation
	"rabbitproxy/config" // Assuming this is your module path
)

var L *zap.Logger // Global logger instance (structured)
var S *zap.SugaredLogger // Global sugared logger instance for convenience

func init() {
	// Initialize with a default fallback logger until Init() is called.
	// This prevents nil pointer dereference if L or S are used before Init.
	var err error
	L, err = zap.NewDevelopment(zap.ErrorOutput(zapcore.AddSync(os.Stderr))) // Log internal Zap errors to stderr
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize fallback Zap logger: %v\n", err)
		// As an absolute fallback, make L a no-op logger
		L = zap.NewNop()
	}
	S = L.Sugar()
}

// Init initializes the global logger based on configuration.
func Init(cfg config.GlobalSettings, enableSyslog bool, logFilePath string) error {
	var level zapcore.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = zapcore.DebugLevel
	case "info":
		level = zapcore.InfoLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	case "fatal":
		level = zapcore.FatalLevel
	case "panic":
		level = zapcore.PanicLevel
	default:
		level = zapcore.InfoLevel // Default level
		// Use fmt.Fprintf for this pre-initialization warning as the logger isn't fully set up.
		fmt.Fprintf(os.Stderr, "Warning: Invalid log_level '%s' in config, defaulting to 'info'.\n", cfg.LogLevel)
	}

	// --- Console Output (stdout) ---
	consoleEncoderCfg := zap.NewProductionEncoderConfig()
	consoleEncoderCfg.TimeKey = "timestamp"
	consoleEncoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	consoleEncoderCfg.EncodeLevel = zapcore.CapitalLevelEncoder // e.g., INFO, ERROR
	consoleEncoderCfg.ConsoleSeparator = " " // Use space as separator for console
	consoleCore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(consoleEncoderCfg),
		zapcore.Lock(os.Stdout), // Lock for concurrent writes
		level,
	)

	cores := []zapcore.Core{consoleCore}
	syslogSuccessfullyInitialized := false

	// --- File Output (Rotated Logs) ---
	if logFilePath != "" {
		fileWriter := zapcore.AddSync(&lumberjack.Logger{
			Filename:   logFilePath,
			MaxSize:    100, // megabytes
			MaxBackups: 3,
			MaxAge:     28, // days
			Compress:   true,
		})
		fileEncoderCfg := zap.NewProductionEncoderConfig() // Use Production for structured JSON
		fileEncoderCfg.TimeKey = "timestamp"
		fileEncoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		fileEncoderCfg.EncodeLevel = zapcore.CapitalLevelEncoder
		fileCore := zapcore.NewCore(
			zapcore.NewJSONEncoder(fileEncoderCfg), // JSON output for files
			fileWriter,
			level,
		)
		cores = append(cores, fileCore)
		// This message will go to console if it's the only core so far, or to the initial fallback logger.
		fmt.Fprintf(os.Stderr, "Info: Logging to file will be enabled: %s\n", logFilePath)
	}

    // --- Syslog Output ---
    if enableSyslog {
        // zapsyslog.NewPlayer returns (io.WriteCloser, error)
        syslogWriter, err := zapsyslog.NewPlayer(
            zapsyslog.Network(""), // Default to local syslog (UDP usually)
            zapsyslog.Address(""), // Default to local syslog address
            syslog.LOG_DAEMON,    // Example facility, could be another like LOG_LOCAL0
            "rabbitproxy",        // Syslog tag
        )
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: Failed to connect to syslog: %v. Syslog logging disabled.\n", err)
        } else {
            syslogEncoderCfg := zap.NewProductionEncoderConfig()
            syslogEncoderCfg.TimeKey = "timestamp"
            syslogEncoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
            syslogEncoderCfg.EncodeLevel = zapcore.CapitalLevelEncoder

            syslogZapCore := zapcore.NewCore(
                zapcore.NewJSONEncoder(syslogEncoderCfg), // JSON for syslog is common for structured logging
                zapcore.AddSync(syslogWriter), // This makes io.WriteCloser a WriteSyncer
                level, // Zap's level filtering applies before sending to syslog
            )
            cores = append(cores, syslogZapCore)
            syslogSuccessfullyInitialized = true
            fmt.Fprintf(os.Stderr, "Info: Syslog logging will be enabled.\n")
        }
    }

	// Combine cores: console, and optionally file and syslog
	combinedCore := zapcore.NewTee(cores...)

	// Create the logger with options
	// AddCallerSkip(1) to make caller point to the actual logging site, not this wrapper.
	// AddStacktrace for ErrorLevel and above.
	L = zap.New(combinedCore, zap.AddCaller(), zap.AddCallerSkip(1), zap.AddStacktrace(zapcore.ErrorLevel))
	S = L.Sugar()

	// Log the final status using the newly configured logger
	finalLogFilePath := "disabled"
	if logFilePath != "" {
		finalLogFilePath = logFilePath
	}
	S.Infof("Logger initialized. Effective Level: %s. Syslog Enabled: %t. File Logging: %s", level.String(), syslogSuccessfullyInitialized, finalLogFilePath)
	return nil
}
