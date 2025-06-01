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
	"rabbitproxy/logging" // Now using Zap
)

var (
	activeForwarders []forwarder.Forwarder
	forwardersLock   sync.Mutex     // Protects activeForwarders and currentConfig
	currentConfig    *config.Config // Holds the currently active configuration
	configFile       string         = "config.toml" // Default config file path
)

// setupForwarders configures and starts forwarders based on the provided application configuration.
func setupForwarders(cfg *config.Config) ([]forwarder.Forwarder, error) {
	newlyStartedForwarders := []forwarder.Forwarder{}
	logging.S.Info("--- Setting up forwarders based on current configuration ---")

	// TCP Rules
	logging.S.Debugf("Processing %d TCP rules from configuration", len(cfg.TCP))
	for i, rule := range cfg.TCP { // Use direct iteration, rule is a copy
		ruleDesc := rule.Description
		if ruleDesc == "" {
			ruleDesc = fmt.Sprintf("TCP Rule #%d (Listen: %v, Target: %s)", i+1, rule.Listen, rule.Target)
		}
		logging.S.Infof("Processing %s", ruleDesc)

		listenPorts, err := config.ParseListenEntry(rule.Listen, "tcp")
		if err != nil {
			logging.S.Warnf("Skipping %s: Error parsing listen entry: %v", ruleDesc, err)
			continue
		}

		targetPorts, err := config.ParseTarget(rule.Target, len(listenPorts), "tcp")
		if err != nil {
			logging.S.Warnf("Skipping %s: Error parsing target entry: %v", ruleDesc, err)
			continue
		}

		if len(listenPorts) == 1 && len(targetPorts) == 1 { // 1:1 mapping
			lp := listenPorts[0]
			tp := targetPorts[0]
			fwdDesc := fmt.Sprintf("%s (L:%s%d -> T:%s:%d)", ruleDesc, lp.Host, lp.Port, tp.Host, tp.Port)

			tcpFwd, err := forwarder.NewTCPForwarder(lp, tp, fwdDesc, rule.ParsedTimeout, rule.MaxConnections, rule.ParsedBandwidth)
			if err != nil {
				logging.S.Errorf("Failed to create TCP forwarder for %s: %v", fwdDesc, err)
				continue
			}
			if err := tcpFwd.Start(); err != nil {
				logging.S.Errorf("Failed to start TCP forwarder for %s: %v", fwdDesc, err)
				continue
			}
			newlyStartedForwarders = append(newlyStartedForwarders, tcpFwd)
			logging.S.Infof("Successfully started TCP forwarder: %s", fwdDesc)

		} else if len(listenPorts) > 1 && len(targetPorts) == len(listenPorts) { // N:N mapping
			for j, lp := range listenPorts {
				tp := targetPorts[j]
				fwdDesc := fmt.Sprintf("%s [map %d/%d] (L:%s%d -> T:%s:%d)", ruleDesc, j+1, len(listenPorts), lp.Host, lp.Port, tp.Host, tp.Port)
				tcpFwd, err := forwarder.NewTCPForwarder(lp, tp, fwdDesc, rule.ParsedTimeout, rule.MaxConnections, rule.ParsedBandwidth)
				if err != nil {
					logging.S.Errorf("Failed to create TCP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				if err := tcpFwd.Start(); err != nil {
					logging.S.Errorf("Failed to start TCP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				newlyStartedForwarders = append(newlyStartedForwarders, tcpFwd)
				logging.S.Infof("Successfully started TCP forwarder: %s", fwdDesc)
			}
		} else if len(listenPorts) > 1 && len(targetPorts) == 1 { // N:1 mapping
			tp := targetPorts[0]
			for j, lp := range listenPorts {
				fwdDesc := fmt.Sprintf("%s [map %d/%d] (L:%s%d -> T:%s:%d)", ruleDesc, j+1, len(listenPorts), lp.Host, lp.Port, tp.Host, tp.Port)
				tcpFwd, err := forwarder.NewTCPForwarder(lp, tp, fwdDesc, rule.ParsedTimeout, rule.MaxConnections, rule.ParsedBandwidth)
				if err != nil {
					logging.S.Errorf("Failed to create TCP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				if err := tcpFwd.Start(); err != nil {
					logging.S.Errorf("Failed to start TCP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				newlyStartedForwarders = append(newlyStartedForwarders, tcpFwd)
				logging.S.Infof("Successfully started TCP forwarder: %s", fwdDesc)
			}
		} else {
			logging.S.Warnf("Skipping %s: Invalid port mapping combination. Listen ports (%d) vs Target ports (%d). Requires 1:1, N:N, or N:1 mapping.",
				ruleDesc, len(listenPorts), len(targetPorts))
		}
	}

	// UDP Rules
	if len(cfg.UDP) > 0 {
		logging.S.Debugf("Processing %d UDP rules from configuration", len(cfg.UDP))
		for i, rule := range cfg.UDP { // Use direct iteration, rule is a copy
			ruleDesc := rule.Description
			if ruleDesc == "" {
				ruleDesc = fmt.Sprintf("UDP Rule #%d (Listen: %v, Target: %s)", i+1, rule.Listen, rule.Target)
			}
			logging.S.Infof("Processing %s", ruleDesc)

			listenPorts, err := config.ParseListenEntry(rule.Listen, "udp")
			if err != nil {
				logging.S.Warnf("Skipping %s: Error parsing listen entry: %v", ruleDesc, err)
				continue
			}

			targetPorts, err := config.ParseTarget(rule.Target, len(listenPorts), "udp")
			if err != nil {
				logging.S.Warnf("Skipping %s: Error parsing target entry: %v", ruleDesc, err)
				continue
			}

			// For UDP, decide how to apply ParsedRuleBandwidth vs ParsedPortBandwidths
			// Option 1: If ParsedPortBandwidths[port] exists, use it. Else use ParsedRuleBandwidth.
			// Option 2: If ParsedPortBandwidths is populated at all, ParsedRuleBandwidth is ignored. (Current config logic implies this)

			if len(listenPorts) == 1 && len(targetPorts) == 1 { // 1:1 mapping
				lp := listenPorts[0]
				tp := targetPorts[0]
				fwdDesc := fmt.Sprintf("%s (L:%s%d -> T:%s:%d)", ruleDesc, lp.Host, lp.Port, tp.Host, tp.Port)

				// Determine bandwidth for this specific port
				bwSetting := rule.ParsedRuleBandwidth // Default to rule-wide
				if specificBw, ok := rule.ParsedPortBandwidths[lp.Port]; ok {
					bwSetting = specificBw
				}

				udpFwd, err := forwarder.NewUDPForwarder(lp, tp, fwdDesc, rule.MaxConnections, bwSetting)
				if err != nil {
					logging.S.Errorf("Failed to create UDP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				if err := udpFwd.Start(); err != nil {
					logging.S.Errorf("Failed to start UDP forwarder for %s: %v", fwdDesc, err)
					continue
				}
				newlyStartedForwarders = append(newlyStartedForwarders, udpFwd)
				logging.S.Infof("Successfully started UDP forwarder: %s (BW IsSet: %t, Rate: %.0f Bps)", fwdDesc, bwSetting.IsSet, bwSetting.RateBPS)


			} else if len(listenPorts) > 1 && len(targetPorts) == len(listenPorts) { // N:N mapping
				for j, lp := range listenPorts {
					tp := targetPorts[j]
					fwdDesc := fmt.Sprintf("%s [map %d/%d] (L:%s%d -> T:%s:%d)", ruleDesc, j+1, len(listenPorts), lp.Host, lp.Port, tp.Host, tp.Port)

					bwSetting := rule.ParsedRuleBandwidth
					if specificBw, ok := rule.ParsedPortBandwidths[lp.Port]; ok {
						bwSetting = specificBw
					}
					udpFwd, err := forwarder.NewUDPForwarder(lp, tp, fwdDesc, rule.MaxConnections, bwSetting)
					if err != nil {
						logging.S.Errorf("Failed to create UDP forwarder for %s: %v", fwdDesc, err)
						continue
					}
					if err := udpFwd.Start(); err != nil {
						logging.S.Errorf("Failed to start UDP forwarder for %s: %v", fwdDesc, err)
						continue
					}
					newlyStartedForwarders = append(newlyStartedForwarders, udpFwd)
					logging.S.Infof("Successfully started UDP forwarder: %s (BW IsSet: %t, Rate: %.0f Bps)", fwdDesc, bwSetting.IsSet, bwSetting.RateBPS)
				}
			} else if len(listenPorts) > 1 && len(targetPorts) == 1 { // N:1 mapping
				tp := targetPorts[0]
				for j, lp := range listenPorts {
					fwdDesc := fmt.Sprintf("%s [map %d/%d] (L:%s%d -> T:%s:%d)", ruleDesc, j+1, len(listenPorts), lp.Host, lp.Port, tp.Host, tp.Port)

					bwSetting := rule.ParsedRuleBandwidth
					if specificBw, ok := rule.ParsedPortBandwidths[lp.Port]; ok {
						bwSetting = specificBw
					}
					udpFwd, err := forwarder.NewUDPForwarder(lp, tp, fwdDesc, rule.MaxConnections, bwSetting)
					if err != nil {
						logging.S.Errorf("Failed to create UDP forwarder for %s: %v", fwdDesc, err)
						continue
					}
					if err := udpFwd.Start(); err != nil {
						logging.S.Errorf("Failed to start UDP forwarder for %s: %v", fwdDesc, err)
						continue
					}
					newlyStartedForwarders = append(newlyStartedForwarders, udpFwd)
					logging.S.Infof("Successfully started UDP forwarder: %s (BW IsSet: %t, Rate: %.0f Bps)", fwdDesc, bwSetting.IsSet, bwSetting.RateBPS)
				}
			} else {
				logging.S.Warnf("Skipping %s: Invalid port mapping combination for UDP. Listen ports (%d) vs Target ports (%d). Requires 1:1, N:N, or N:1 mapping.",
					ruleDesc, len(listenPorts), len(targetPorts))
			}
		}
	}
	logging.S.Info("--- Finished setting up forwarders ---")
	return newlyStartedForwarders, nil
}

func reloadConfiguration() {
	forwardersLock.Lock()
	defer forwardersLock.Unlock()

	logging.S.Info("Reloading configuration from file: " + configFile)
	newCfg, err := config.LoadConfig(configFile)
	if err != nil {
		logging.S.Errorf("Error reloading configuration from '%s': %v. No changes applied.", configFile, err)
		return
	}

	enableSyslog := true // This should ideally be from initial config or a persistent state
	// Pass the new LogFilePath from newCfg
	if err := logging.Init(newCfg.Global, enableSyslog, newCfg.Global.LogFilePath); err != nil {
		logging.S.Errorf("Error re-initializing Zap logger with new settings: %v", err)
	}

	logging.S.Info("Stopping all current forwarders before applying new configuration...")
	for _, fwd := range activeForwarders {
		if err := fwd.Stop(); err != nil {
			logging.S.Errorf("Error stopping a forwarder: %v", err)
		}
	}
	activeForwarders = []forwarder.Forwarder{}

	logging.S.Info("Setting up forwarders with new configuration...")
	newlyStartedForwarders, setupErr := setupForwarders(newCfg)
	if setupErr != nil {
		logging.S.Errorf("Critical error during setupForwarders with new configuration: %v. Service might be degraded or unresponsive.", setupErr)
	}
	activeForwarders = newlyStartedForwarders
	currentConfig = newCfg

	logging.S.Infof("Configuration reloaded. Total active forwarders: %d", len(activeForwarders))
	logging.S.Debugf("Full new configuration after reload: %+v", newCfg)
}

func main() {
	var err error

	currentConfig, err = config.LoadConfig(configFile)
	if err != nil {
		log.Fatalf("CRITICAL: Failed to load initial configuration from '%s': %v. Exiting.", configFile, err)
	}

	enableSyslog := true
	if err_logging := logging.Init(currentConfig.Global, enableSyslog, currentConfig.Global.LogFilePath); err_logging != nil {
		log.Printf("WARNING: Error during Zap logger initialization: %v. Fallback logger might be in use.", err_logging)
	}
	logging.S.Debugf("Initial full configuration details: %+v", currentConfig)

	forwardersLock.Lock()
	activeForwarders, err = setupForwarders(currentConfig)
	if err != nil {
		logging.S.Fatalf("Failed to complete initial setup of forwarders: %v. Exiting.", err)
	}
	if len(activeForwarders) == 0 {
		logging.S.Warn("Initial setup: No active forwarders were started. Check configuration and logs.")
	} else {
		logging.S.Infof("Initial setup: Successfully started %d forwarder(s).", len(activeForwarders))
	}
	forwardersLock.Unlock()

	sighupChan := make(chan os.Signal, 1)
	signal.Notify(sighupChan, syscall.SIGHUP)

	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		logging.S.Errorf("Failed to create fsnotify watcher: %v. Config file changes will not be automatically reloaded.", err)
	} else {
		absConfigFile, configErr := filepath.Abs(configFile)
		if configErr != nil {
			logging.S.Errorf("Failed to get absolute path for config file '%s': %v. Watcher not started.", configFile, configErr)
			fsWatcher.Close()
			fsWatcher = nil
		} else {
			err = fsWatcher.Add(absConfigFile)
			if err != nil {
				logging.S.Errorf("Failed to add config file '%s' to fsnotify watcher: %v.", absConfigFile, err)
				fsWatcher.Close()
				fsWatcher = nil
			} else {
				logging.S.Infof("Watching config file '%s' for changes.", absConfigFile)
			}
		}
	}

	appDoneChan := make(chan struct{})

	go func(quitSig chan os.Signal, hupSig chan os.Signal, watcher *fsnotify.Watcher, appExitChan chan struct{}) {
		defer close(appExitChan)
		if watcher != nil {
			defer watcher.Close()
		}

		for {
			select {
			case <-hupSig:
				logging.S.Info("SIGHUP signal received. Triggering configuration reload.")
				reloadConfiguration()

			case event, ok := <-watcher.Events:
				if !ok {
					logging.S.Info("fsnotify event channel closed.")
					return
				}
				absConfFile, _ := filepath.Abs(configFile)
				if filepath.Clean(event.Name) == absConfFile {
					if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
						logging.S.Infof("Config file event for '%s': %s. Triggering reload.", event.Name, event.Op.String())
						reloadConfiguration()
					} else if event.Op&fsnotify.Remove == fsnotify.Remove || event.Op&fsnotify.Rename == fsnotify.Rename {
						logging.S.Warnf("Config file '%s' event: %s. Attempting to re-watch.", event.Name, event.Op.String())
						if watcher != nil {
							watcher.Remove(event.Name)
							if err := watcher.Add(absConfFile); err != nil {
								logging.S.Errorf("Failed to re-watch config file '%s': %v. Auto-reload may fail.", absConfFile, err)
							} else {
								logging.S.Infof("Successfully re-watched config file '%s'. Triggering reload as content might have changed.", absConfFile)
								reloadConfiguration()
							}
						}
					}
				} else {
					logging.S.Debugf("fsnotify event for unrelated file/dir '%s': %s. Ignored.", event.Name, event.Op.String())
				}

			case errWatcher, ok := <-watcher.Errors:
				if !ok {
					logging.S.Info("fsnotify error channel closed.")
					return
				}
				logging.S.Errorf("fsnotify watcher error: %v", errWatcher)

			case <-quitSig:
				logging.S.Info("Shutdown signal received. Stopping services...")
				forwardersLock.Lock()
				logging.S.Info("Stopping all active forwarders due to shutdown...")
				for _, fwd := range activeForwarders {
					if err := fwd.Stop(); err != nil {
						logging.S.Errorf("Error stopping a forwarder during shutdown: %v", err)
					}
				}
				activeForwarders = []forwarder.Forwarder{}
				currentConfig = nil
				forwardersLock.Unlock()
				logging.S.Info("All forwarders stopped. RabbitProxy is shutting down.")
				return
			}
		}
	}(signal.NewNotifier(os.Interrupt, syscall.SIGTERM), sighupChan, fsWatcher, appDoneChan)

	logging.S.Info("RabbitProxy started. Monitoring for signals and configuration changes. Press Ctrl+C or send SIGTERM to exit.")
	<-appDoneChan
	logging.S.Info("RabbitProxy has shut down gracefully.")
}
