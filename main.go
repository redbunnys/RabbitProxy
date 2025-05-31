package main

import (
	"fmt"
	"log" // Standard log for pre-logger errors
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall" // For SIGHUP & other signals

	"github.com/fsnotify/fsnotify"
	"rabbitproxy/config"
	"rabbitproxy/forwarder"
	"rabbitproxy/logging"
)

var (
	activeForwarders []forwarder.Forwarder
	forwardersLock   sync.Mutex     // Protects activeForwarders and currentConfig
	currentConfig    *config.Config // Holds the currently active configuration
	configFile       string         = "config.toml" // Default config file path
)

// setupForwarders configures and starts forwarders based on the provided application configuration.
// It returns a slice of successfully started forwarders and an error if critical setup steps fail.
func setupForwarders(cfg *config.Config) ([]forwarder.Forwarder, error) {
	newlyStartedForwarders := []forwarder.Forwarder{}
	logging.Logger.Info("--- Setting up forwarders based on current configuration ---")

	// TCP Rules
	logging.Logger.Debugf("Processing %d TCP rules from configuration", len(cfg.TCP))
	for i, rule := range cfg.TCP {
		ruleDesc := rule.Description
		if ruleDesc == "" {
			ruleDesc = fmt.Sprintf("TCP Rule #%d (Listen: %v, Target: %s)", i+1, rule.Listen, rule.Target)
		}
		logging.Logger.Infof("Processing %s", ruleDesc)

		listenPorts, err := config.ParseListenEntry(rule.Listen, "tcp")
		if err != nil {
			logging.Logger.Warnf("Skipping %s: Error parsing listen entry: %v", ruleDesc, err)
			continue
		}

		targetPorts, err := config.ParseTarget(rule.Target, len(listenPorts), "tcp")
		if err != nil {
			logging.Logger.Warnf("Skipping %s: Error parsing target entry: %v", ruleDesc, err)
			continue
		}

		if len(listenPorts) == 1 && len(targetPorts) == 1 { // 1:1 mapping
			lp := listenPorts[0]
			tp := targetPorts[0]
			fwdDesc := fmt.Sprintf("%s (L:%s%d -> T:%s:%d)", ruleDesc, lp.Host, lp.Port, tp.Host, tp.Port)

			tcpFwd, err := forwarder.NewTCPForwarder(lp, tp, fwdDesc, rule.ParsedTimeout, rule.MaxConnections)
			if err != nil {
				logging.Logger.Errorf("Failed to create TCP forwarder for %s: %v", fwdDesc, err)
				continue
			}
			if err := tcpFwd.Start(); err != nil {
				logging.Logger.Errorf("Failed to start TCP forwarder for %s: %v", fwdDesc, err)
				continue
			}
			newlyStartedForwarders = append(newlyStartedForwarders, tcpFwd)
			logging.Logger.Infof("Successfully started TCP forwarder: %s", fwdDesc)

		} else if len(listenPorts) > 1 && len(targetPorts) == len(listenPorts) { // N:N mapping
			for j, lp := range listenPorts {
				tp := targetPorts[j]
				fwdDesc := fmt.Sprintf("%s [map %d/%d] (L:%s%d -> T:%s:%d)", ruleDesc, j+1, len(listenPorts), lp.Host, lp.Port, tp.Host, tp.Port)
				tcpFwd, err := forwarder.NewTCPForwarder(lp, tp, fwdDesc, rule.ParsedTimeout, rule.MaxConnections)
				tcpFwd, err := forwarder.NewTCPForwarder(lp, tp, fwdDesc, rule.ParsedTimeout, rule.MaxConnections)
				if err != nil {
					logging.Logger.Errorf("Failed to create TCP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				if err := tcpFwd.Start(); err != nil {
					logging.Logger.Errorf("Failed to start TCP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				newlyStartedForwarders = append(newlyStartedForwarders, tcpFwd)
				logging.Logger.Infof("Successfully started TCP forwarder: %s", fwdDesc)
			}
		} else if len(listenPorts) > 1 && len(targetPorts) == 1 { // N:1 mapping
			tp := targetPorts[0]
			for j, lp := range listenPorts {
				fwdDesc := fmt.Sprintf("%s [map %d/%d] (L:%s%d -> T:%s:%d)", ruleDesc, j+1, len(listenPorts), lp.Host, lp.Port, tp.Host, tp.Port)
				tcpFwd, err := forwarder.NewTCPForwarder(lp, tp, fwdDesc)
				if err != nil {
					logging.Logger.Errorf("Failed to create TCP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				if err := tcpFwd.Start(); err != nil {
					logging.Logger.Errorf("Failed to start TCP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				newlyStartedForwarders = append(newlyStartedForwarders, tcpFwd)
				logging.Logger.Infof("Successfully started TCP forwarder: %s", fwdDesc)
			}
		} else {
			logging.Logger.Warnf("Skipping %s: Invalid port mapping combination. Listen ports (%d) vs Target ports (%d). Requires 1:1, N:N, or N:1 mapping.",
				ruleDesc, len(listenPorts), len(targetPorts))
		}
	}

	// UDP Rules
	if len(cfg.UDP) > 0 {
		logging.Logger.Debugf("Processing %d UDP rules from configuration", len(cfg.UDP))
		for i, rule := range cfg.UDP {
			ruleDesc := rule.Description
			if ruleDesc == "" {
				ruleDesc = fmt.Sprintf("UDP Rule #%d (Listen: %v, Target: %s)", i+1, rule.Listen, rule.Target)
			}
			logging.Logger.Infof("Processing %s", ruleDesc)

			listenPorts, err := config.ParseListenEntry(rule.Listen, "udp")
			if err != nil {
				logging.Logger.Warnf("Skipping %s: Error parsing listen entry: %v", ruleDesc, err)
				continue
			}

			targetPorts, err := config.ParseTarget(rule.Target, len(listenPorts), "udp")
			if err != nil {
				logging.Logger.Warnf("Skipping %s: Error parsing target entry: %v", ruleDesc, err)
				continue
			}

			if len(listenPorts) == 1 && len(targetPorts) == 1 { // 1:1 mapping
				lp := listenPorts[0]
				tp := targetPorts[0]
				fwdDesc := fmt.Sprintf("%s (L:%s%d -> T:%s:%d)", ruleDesc, lp.Host, lp.Port, tp.Host, tp.Port)

				udpFwd, err := forwarder.NewUDPForwarder(lp, tp, fwdDesc, rule.MaxConnections)
				if err != nil {
					logging.Logger.Errorf("Failed to create UDP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				if err := udpFwd.Start(); err != nil {
					logging.Logger.Errorf("Failed to start UDP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				newlyStartedForwarders = append(newlyStartedForwarders, udpFwd)
				logging.Logger.Infof("Successfully started UDP forwarder: %s", fwdDesc)

			} else if len(listenPorts) > 1 && len(targetPorts) == len(listenPorts) { // N:N mapping
				for j, lp := range listenPorts {
					tp := targetPorts[j]
					fwdDesc := fmt.Sprintf("%s [map %d/%d] (L:%s%d -> T:%s:%d)", ruleDesc, j+1, len(listenPorts), lp.Host, lp.Port, tp.Host, tp.Port)
					udpFwd, err := forwarder.NewUDPForwarder(lp, tp, fwdDesc, rule.MaxConnections)
					if err != nil {
						logging.Logger.Errorf("Failed to create UDP forwarder for %s: %v", fwdDesc, err)
						continue
					}
					if err := udpFwd.Start(); err != nil {
						logging.Logger.Errorf("Failed to start UDP forwarder for %s: %v", fwdDesc, err)
						continue
					}
					newlyStartedForwarders = append(newlyStartedForwarders, udpFwd)
					logging.Logger.Infof("Successfully started UDP forwarder: %s", fwdDesc)
				}
			} else if len(listenPorts) > 1 && len(targetPorts) == 1 { // N:1 mapping
				tp := targetPorts[0]
				for j, lp := range listenPorts {
					fwdDesc := fmt.Sprintf("%s [map %d/%d] (L:%s%d -> T:%s:%d)", ruleDesc, j+1, len(listenPorts), lp.Host, lp.Port, tp.Host, tp.Port)
					udpFwd, err := forwarder.NewUDPForwarder(lp, tp, fwdDesc, rule.MaxConnections)
					if err != nil {
						logging.Logger.Errorf("Failed to create UDP forwarder for %s: %v", fwdDesc, err)
						continue
					}
					if err := udpFwd.Start(); err != nil {
						logging.Logger.Errorf("Failed to start UDP forwarder for %s: %v", fwdDesc, err)
						continue
					}
					newlyStartedForwarders = append(newlyStartedForwarders, udpFwd)
					logging.Logger.Infof("Successfully started UDP forwarder: %s", fwdDesc)
				}
			} else {
				logging.Logger.Warnf("Skipping %s: Invalid port mapping combination. Listen ports (%d) vs Target ports (%d). Requires 1:1, N:N, or N:1 mapping.",
					ruleDesc, len(listenPorts), len(targetPorts))
			}
		}
	}
	logging.Logger.Info("--- Finished setting up forwarders ---")
	return newlyStartedForwarders, nil
}

func reloadConfiguration() {
	forwardersLock.Lock()
	defer forwardersLock.Unlock()

	logging.Logger.Info("Reloading configuration from file: " + configFile)
	newCfg, err := config.LoadConfig(configFile)
	if err != nil {
		logging.Logger.Errorf("Error reloading configuration from '%s': %v. No changes applied.", configFile, err)
		return
	}

	// Update logger settings if GlobalSettings changed (e.g., log level)
	// Re-initializing the logger will apply new settings.
	// We need to decide if enableSyslog can change on reload or is fixed at startup. Assuming fixed for now.
	currentEnableSyslogState := true // This should ideally come from a persistent state or initial config
	if err := logging.Init(newCfg.Global, currentEnableSyslogState); err != nil {
		logging.Logger.Errorf("Error re-initializing logger with new settings: %v", err)
		// Continue with reload despite logger re-init error, as old settings would persist.
	}
	logging.Logger.Infof("Logger re-initialized with new settings from reloaded config. Effective LogLevel: %s", logging.Logger.GetLevel().String())


	logging.Logger.Info("Stopping all current forwarders before applying new configuration...")
	for _, fwd := range activeForwarders {
		if err := fwd.Stop(); err != nil {
			logging.Logger.Errorf("Error stopping a forwarder: %v", err)
		}
	}
	activeForwarders = []forwarder.Forwarder{} // Clear the slice

	logging.Logger.Info("Setting up forwarders with new configuration...")
	newlyStartedForwarders, setupErr := setupForwarders(newCfg)
	if setupErr != nil {
		// This error implies a global issue with setting up forwarders, not just one rule.
		logging.Logger.Errorf("Critical error during setupForwarders with new configuration: %v. Service might be degraded or unresponsive.", setupErr)
		// Depending on policy, we might try to restore old config or exit.
		// For now, we proceed with whatever got started.
	}
	activeForwarders = newlyStartedForwarders
	currentConfig = newCfg // IMPORTANT: Update the global currentConfig

	logging.Logger.Infof("Configuration reloaded. Total active forwarders: %d", len(activeForwarders))
	logging.Logger.Debugf("Full new configuration after reload: %+v", newCfg)
}

func main() {
	var err error // To be used for errors before logger is fully up

	// Initial configuration load
	currentConfig, err = config.LoadConfig(configFile)
	if err != nil {
		log.Fatalf("CRITICAL: Failed to load initial configuration from '%s': %v. Exiting.", configFile, err)
	}

	// Initialize logger with settings from the initial configuration
	// Assuming enableSyslog is true for now, could be from currentConfig.Global.EnableSyslog if added
	enableSyslog := true
	if err_logging := logging.Init(currentConfig.Global, enableSyslog); err_logging != nil {
		// Use standard log as logger.Init might have failed in a way that Logger itself is not reliable
		log.Printf("WARNING: Error during logger initialization: %v. Logging may not be fully functional.", err_logging)
	}

	logging.Logger.Infof("Loaded initial configuration from '%s'. Initial LogLevel: '%s'. Effective LogLevel: '%s'",
		configFile, currentConfig.Global.LogLevel, logging.Logger.GetLevel().String())
	logging.Logger.Debugf("Initial full configuration details: %+v", currentConfig)

	// Initial setup of forwarders
	// forwardersLock is not strictly needed here as it's pre-goroutines, but good practice if setupForwarders was ever concurrent.
	forwardersLock.Lock()
	activeForwarders, err = setupForwarders(currentConfig)
	if err != nil {
		// If setupForwarders returns a critical error (not just errors for individual rules)
		logging.Logger.Fatalf("Failed to complete initial setup of forwarders: %v. Exiting.", err)
	}
	if len(activeForwarders) == 0 {
		logging.Logger.Warn("Initial setup: No active forwarders were started. Check configuration and logs.")
	} else {
		logging.Logger.Infof("Initial setup: Successfully started %d forwarder(s).", len(activeForwarders))
	}
	forwardersLock.Unlock()

	// --- Signal and File Watcher Setup ---
	sighupChan := make(chan os.Signal, 1)
	signal.Notify(sighupChan, syscall.SIGHUP)

	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		logging.Logger.Errorf("Failed to create fsnotify watcher: %v. Config file changes will not be automatically reloaded.", err)
	} else {
		// Note: defer fsWatcher.Close() would be called when main exits.
		// The signal handling goroutine below will close it when it exits.

		absConfigFile, configErr := filepath.Abs(configFile)
		if configErr != nil {
			logging.Logger.Errorf("Failed to get absolute path for config file '%s': %v. Watcher not started.", configFile, configErr)
			fsWatcher.Close() // Close the watcher if we can't use it
			fsWatcher = nil   // Mark as nil so the event loop doesn't try to use it
		} else {
			// It's generally better to watch the directory containing the config file
			// to handle atomic saves (create new -> rename old -> rename new to old).
			// Watching the directory: err = fsWatcher.Add(filepath.Dir(absConfigFile))
			// Then in event handler, check if event.Name == absConfigFile
			err = fsWatcher.Add(absConfigFile) // Watch the specific file for simplicity here
			if err != nil {
				logging.Logger.Errorf("Failed to add config file '%s' to fsnotify watcher: %v.", absConfigFile, err)
				fsWatcher.Close()
				fsWatcher = nil
			} else {
				logging.Logger.Infof("Watching config file '%s' for changes.", absConfigFile)
			}
		}
	}

	// Channel to signal application shutdown completion
	appDoneChan := make(chan struct{})

	// Goroutine for handling signals and file events
	go func(quitSig chan os.Signal, hupSig chan os.Signal, watcher *fsnotify.Watcher, appExitChan chan struct{}) {
		defer close(appExitChan) // Signal that this goroutine has finished
		if watcher != nil {      // Only defer watcher.Close if it was successfully initialized
			defer watcher.Close()
		}

		for {
			select {
			case <-hupSig:
				logging.Logger.Info("SIGHUP signal received. Triggering configuration reload.")
				reloadConfiguration()

			case event, ok := <-watcher.Events:
				if !ok { // Channel closed
					logging.Logger.Info("fsnotify event channel closed.")
					return
				}
				absConfFile, _ := filepath.Abs(configFile) // Should be cached or passed in
				if filepath.Clean(event.Name) == absConfFile {
					if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
						logging.Logger.Infof("Config file event for '%s': %s. Triggering reload.", event.Name, event.Op.String())
						reloadConfiguration()
					} else if event.Op&fsnotify.Remove == fsnotify.Remove || event.Op&fsnotify.Rename == fsnotify.Rename {
						logging.Logger.Warnf("Config file '%s' event: %s. Attempting to re-watch.", event.Name, event.Op.String())
						if watcher != nil {
							watcher.Remove(event.Name) // Remove old watch, ignore error
							if err := watcher.Add(absConfFile); err != nil {
								logging.Logger.Errorf("Failed to re-watch config file '%s': %v. Auto-reload may fail.", absConfFile, err)
							} else {
								logging.Logger.Infof("Successfully re-watched config file '%s'. Triggering reload as content might have changed.", absConfFile)
								reloadConfiguration()
							}
						}
					}
				} else {
					logging.Logger.Debugf("fsnotify event for unrelated file/dir '%s': %s. Ignored.", event.Name, event.Op.String())
				}


			case errWatcher, ok := <-watcher.Errors:
				if !ok { // Channel closed
					logging.Logger.Info("fsnotify error channel closed.")
					return
				}
				logging.Logger.Errorf("fsnotify watcher error: %v", errWatcher)

			case <-quitSig:
				logging.Logger.Info("Shutdown signal received. Stopping services...")
				forwardersLock.Lock()
				logging.Logger.Info("Stopping all active forwarders due to shutdown...")
				for _, fwd := range activeForwarders {
					if err := fwd.Stop(); err != nil {
						logging.Logger.Errorf("Error stopping a forwarder during shutdown: %v", err)
					}
				}
				activeForwarders = []forwarder.Forwarder{} // Clear the slice
				currentConfig = nil                         // Clear current config state
				forwardersLock.Unlock()
				logging.Logger.Info("All forwarders stopped. RabbitProxy is shutting down.")
				return // Exit this goroutine, which will lead to appDoneChan being closed
			}
		}
	}(signal.NewNotifier(os.Interrupt, syscall.SIGTERM), sighupChan, fsWatcher, appDoneChan)


	logging.Logger.Info("RabbitProxy started. Monitoring for signals and configuration changes. Press Ctrl+C or send SIGTERM to exit.")
	<-appDoneChan // Block main goroutine until the appDoneChan is closed
	logging.Logger.Info("RabbitProxy has shut down gracefully.")
}
