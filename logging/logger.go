package logging

import (
	"fmt"
	"os"
	"strings"
	stdSyslog "log/syslog" // For standard syslog Facility constants

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2" // For file rotation

	zapSyslog "github.com/stephanesan/zap-syslog" // New syslog package

	"rabbitproxy/config" // Assuming this is your module path
)

var L *zap.Logger // Global logger instance (structured)
var S *zap.SugaredLogger // Global sugared logger instance for convenience

func init() {
	var err error
	L, err = zap.NewDevelopment(zap.ErrorOutput(zapcore.AddSync(os.Stderr)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize fallback Zap logger: %v\n", err)
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
		level = zapcore.InfoLevel
		fmt.Fprintf(os.Stderr, "Warning: Invalid log_level '%s' in config, defaulting to 'info'.\n", cfg.LogLevel)
	}

	// --- Console Output (stdout) ---
	consoleEncoderCfg := zap.NewProductionEncoderConfig()
	consoleEncoderCfg.TimeKey = "timestamp"
	consoleEncoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	consoleEncoderCfg.EncodeLevel = zapcore.CapitalLevelEncoder
	consoleEncoderCfg.ConsoleSeparator = " "
	consoleCore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(consoleEncoderCfg),
		zapcore.Lock(os.Stdout),
		level,
	)
	cores := []zapcore.Core{consoleCore}

	var fileCore zapcore.Core // Declare here to check in final log message
	if logFilePath != "" {
		fileWriter := zapcore.AddSync(&lumberjack.Logger{
			Filename:   logFilePath, MaxSize: 100, MaxBackups: 3, MaxAge: 28, Compress: true,
		})
		fileEncoderCfg := zap.NewProductionEncoderConfig()
		fileEncoderCfg.TimeKey = "timestamp"
		fileEncoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		fileEncoderCfg.EncodeLevel = zapcore.CapitalLevelEncoder
		fileCore = zapcore.NewCore(
			zapcore.NewJSONEncoder(fileEncoderCfg),
			fileWriter,
			level,
		)
		cores = append(cores, fileCore)
		fmt.Fprintf(os.Stderr, "Info: Logging to file will be enabled: %s\n", logFilePath)
	}

    // --- Syslog Output using stephanesan/zap-syslog ---
    var syslogCore zapcore.Core
    syslogSuccessfullyInitialized := false
    if enableSyslog {
        // Base Zap encoder config for syslog messages
        syslogEncoderDetails := zap.NewProductionEncoderConfig()
        syslogEncoderDetails.TimeKey = "timestamp"
        syslogEncoderDetails.EncodeTime = zapcore.ISO8601TimeEncoder
        syslogEncoderDetails.EncodeLevel = zapcore.CapitalLevelEncoder
        // Remove fields that syslog might add itself or that are not standard in RFC5424 via this encoder
        // For example, logger name if not desired, or customize caller key.
        // syslogEncoderDetails.CallerKey = "" // Example: disable caller key if syslog adds it
        // syslogEncoderDetails.NameKey = ""   // Example: disable logger name key

        syslogEncCfg := zapSyslog.SyslogEncoderConfig{
            EncoderConfig: syslogEncoderDetails,
            Facility:      stdSyslog.LOG_DAEMON, // Example: LOG_DAEMON or LOG_LOCAL0, etc.
            Hostname:      "",                  // Optional: override hostname, empty uses os.Hostname()
            PID:           os.Getpid(),
            App:           "rabbitproxy",       // Application name tag
        }

        syslogEncoder := zapSyslog.NewSyslogEncoder(syslogEncCfg)

        // Empty network and raddr usually mean local syslog daemon
        syslogSyncer, err := zapSyslog.NewConnSyncer("", "")
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: Failed to connect to syslog with stephanesan/zap-syslog: %v. Syslog logging disabled.\n", err)
        } else {
            syslogCore = zapcore.NewCore(syslogEncoder, syslogSyncer, level)
            cores = append(cores, syslogCore)
            syslogSuccessfullyInitialized = true // Mark success
            fmt.Fprintf(os.Stderr, "Info: Syslog logging enabled via stephanesan/zap-syslog.\n")
        }
    }

	combinedCore := zapcore.NewTee(cores...)
	L = zap.New(combinedCore, zap.AddCaller(), zap.AddCallerSkip(1), zap.AddStacktrace(zapcore.ErrorLevel))
	S = L.Sugar()

    finalLogMsgParts := []string{fmt.Sprintf("Logger initialized. Effective Level: %s", level.String())}
    if logFilePath != "" && fileCore != nil { // Check if fileCore was actually added
        finalLogMsgParts = append(finalLogMsgParts, fmt.Sprintf("File: %s", logFilePath))
    } else if logFilePath != "" {
        finalLogMsgParts = append(finalLogMsgParts, "File: (logging disabled due to error or empty path)")
    } else {
        finalLogMsgParts = append(finalLogMsgParts, "File: disabled")
    }

    if enableSyslog {
        if syslogSuccessfullyInitialized {
            finalLogMsgParts = append(finalLogMsgParts, "Syslog: enabled")
        } else {
            finalLogMsgParts = append(finalLogMsgParts, "Syslog: failed to initialize")
        }
    } else {
        finalLogMsgParts = append(finalLogMsgParts, "Syslog: disabled")
    }
	S.Info(strings.Join(finalLogMsgParts, ". "))
	return nil
}
