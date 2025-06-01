# RabbitProxy - TCP/UDP Port Forwarder

## Overview

RabbitProxy is a high-performance, configurable TCP/UDP port forwarding tool written in Go. It allows flexible rule definitions for redirecting network traffic and includes advanced features such as dynamic configuration reloading, structured logging, connection limits, TCP timeouts, and granular bandwidth limiting.

## Features

*   **TCP & UDP Forwarding:** Supports both major transport protocols.
*   **Flexible Port Mappings:**
    *   Single port to single port (e.g., `8080` -> `target:80`).
    *   Port range to port range (e.g., `listen = "1000-1005"` -> `target = "host:2000-2005"`).
    *   Multiple specific listen ports to a single target (e.g., `listen = [5000, 5001]`).
*   **TOML Configuration:** Easy-to-understand configuration via a `config.toml` file.
*   **Dynamic Configuration Reloading:**
    *   Automatically reloads configuration on SIGHUP signal.
    *   Automatically reloads configuration when the `config.toml` file is modified (via fsnotify).
*   **Structured Logging with Zap:**
    *   High-performance, structured logging.
    *   Configurable log levels: `debug`, `info`, `warn`, `error`, `fatal`, `panic`.
    *   Outputs to console.
    *   Optional: Output to a log file with automatic rotation (via `log_file_path` setting).
    *   Optional: Syslog integration.
*   **Resource Management:**
    *   Per-rule connection limits for TCP (`max_connections`).
    *   Per-rule active session limits for UDP (`max_connections`).
    *   Per-rule TCP connection timeouts (`timeout`).
*   **Bandwidth Limiting (Token Bucket):**
    *   Per-rule or per-port (for UDP multi-port) bandwidth throttling.
    *   Separate ingress and egress token buckets for each rule.
    *   Configurable sustained rate (`rate`) and maximum burst (`burst`).
    *   Supports units: `K`/`Kbps` (kilobits/sec), `M`/`Mbps` (megabits/sec), `G`/`Gbps` (gigabits/sec), `bps` or plain numbers (bits/sec). "unlimited" is also accepted.

## Installation / Building

1.  Ensure you have Go installed (version 1.21+ recommended).
2.  Clone the repository (if applicable) or navigate to the project directory.
3.  Build the executable:
    ```bash
    go build -o rabbitproxy main.go
    ```
    (Or simply `go build` if your main package is set up appropriately)

## Configuration (`config.toml`)

The application is configured using a `config.toml` file located in the same directory as the executable by default. (Currently, the path "config.toml" is hardcoded).

### Global Settings

The `[global]` section defines application-wide settings:

```toml
[global]
log_level = "info"      # Logging verbosity: "debug", "info", "warn", "error", "fatal", "panic". Default: "info".
log_file_path = "/var/log/rabbitproxy/rabbitproxy.log"  # Optional. Path to log file. Enables file logging with rotation.
# enable_syslog = true  # Currently enabled by default if syslog is accessible. Future: make this a config flag.
```

### TCP Forwarding Rules (`[[tcp]]`)

Define TCP forwarding rules in sections starting with `[[tcp]]`.

```toml
[[tcp]]
listen = 8080                 # Single listen port
target = "192.168.1.100:80"   # Single target host and port
description = "Web server forwarding" # Optional description for logs
timeout = "60s"               # Optional: TCP connection timeout (e.g., "30s", "1m", "1h"). Default: 30s (as per loader.go).
max_connections = 100         # Optional: Max concurrent connections for this rule. Default: 0 (unlimited).
bandwidth = "10M"             # Optional: Sustained rate (10 Mbps). Units: K, M, G, bps. "unlimited" or omit for no limit.

[[tcp]]
listen = "10000-10005"        # Listen on port range (inclusive)
target = "10.0.0.2:20000-20005" # Target port range (must match count of listen ports for N:N mapping)
description = "TCP range forwarding"
bandwidth = { rate = "50M", burst = "75M" } # Advanced: Rate 50Mbps, Burst 75Mbps

[[tcp]]
listen = [5000, 5001, 5002]   # Listen on multiple specific ports
target = "192.168.2.1:9000"   # All map to the same single target
description = "Multiple ports to single TCP target"
max_connections = 20
```

### UDP Forwarding Rules (`[[udp]]`)

Define UDP forwarding rules in sections starting with `[[udp]]`.

```toml
[[udp]]
listen = 5353
target = "8.8.8.8:53"
description = "DNS forwarding to Google DNS"
max_connections = 200         # Optional: Max concurrent UDP sessions for this rule. Default: 0 (unlimited).
bandwidth = "1M"              # Optional: Rule-wide bandwidth limit (1 Mbps).

[[udp]]
listen = "20000-20005"
target = "10.1.0.2:30000-30005"
description = "UDP range forwarding"

[[udp]]
listen = [7000, 7001, 7002]
target = "10.1.0.3:8000"
description = "Multiple UDP ports to single target"
# Example of per-port bandwidth configuration for a multi-port listen rule:
# If 'bandwidth' is a list, it defines limits ONLY for the specified ports.
# Other ports in the 'listen' list for this rule would need a rule-wide bandwidth or would be unlimited.
# For more clarity, ensure all listened ports are covered or define a separate rule-wide bandwidth.
bandwidth = [
  { port = 7000, rate = "500K" },
  { port = 7001, rate = "1M", burst = "1.2M" },
  { port = 7002, rate = "unlimited" }  # This specific port will have no limit
]

[[udp]]
listen = 7003
target = "10.1.0.4:8001"
description = "UDP with advanced rule-wide bandwidth"
bandwidth = { rate = "5M", burst = "6M" }
```

**Note on Bandwidth Units:**
*   `K` or `Kbps`: Kilobits per second (e.g., "500K" = 500 Kbps)
*   `M` or `Mbps`: Megabits per second (e.g., "10M" = 10 Mbps)
*   `G` or `Gbps`: Gigabits per second (e.g., "1G" = 1 Gbps)
*   `bps` or plain numbers: Bits per second (e.g., "1024bps" or "1024" = 1024 bps)
*   The tool converts these values to Bytes per Second internally for the token bucket.
*   `burst` is optional. If not specified, it defaults to 1.5 times the `rate` (if rate > 0).
*   `"unlimited"` or omitting the `bandwidth` key means no rate limiting for that rule/port.

## Running RabbitProxy

1.  Create your `config.toml` file.
2.  Run the executable:
    ```bash
    ./rabbitproxy
    ```
    Ensure `config.toml` is in the same directory as the executable.

## Configuration Reloading

*   **SIGHUP:** Send a SIGHUP signal to the RabbitProxy process to trigger a configuration reload.
    ```bash
    kill -HUP <pid_of_rabbitproxy>
    ```
*   **File Watching:** Modifying and saving the `config.toml` file will automatically trigger a reload.

The application will log the outcome of the reload attempt. Existing connections are generally not dropped for unchanged rules; rules that are removed will have their connections gracefully terminated.

## Error Handling

*   The application logs errors to console (and optionally to file/syslog).
*   Invalid configuration entries are typically logged at startup or during a reload attempt, and problematic rules may be skipped.
*   Warnings are issued for potential issues like attempting to use privileged ports (<1024) without sufficient permissions.
*   TCP connections will attempt to retry connecting to a target if it's initially unavailable (up to 3 times with a 2-second delay by default).

---

*Generated by AI Port Forwarder Assistant.*
