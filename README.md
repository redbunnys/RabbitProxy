# RabbitProxy

## Overview

RabbitProxy is a high-performance TCP proxy written in Go. It provides robust rate limiting capabilities based on token buckets and detailed traffic statistics collection. The proxy is designed to be dynamically configurable via an HTTP API for adjusting rate limits and viewing operational statistics.

## Features

*   **TCP Proxying**: Forwards TCP connections to a configurable backend server.
*   **Rate Limiting**:
    *   Token bucket algorithm for smooth traffic flow.
    *   Per-client IP rate limiting, with separate limits for upload and download streams (e.g., `1.2.3.4_up`, `1.2.3.4_down`).
    *   Configurable default rate and burst sizes for new client streams.
    *   Default and per-identifier (client stream) rate limits are dynamically configurable via an HTTP API.
*   **Traffic Statistics**:
    *   Comprehensive statistics collection:
        *   Global aggregates (total bytes/packets, peak bandwidth).
        *   Per-source IP address.
        *   Per-protocol/stream label (e.g., "TCP_UP", "TCP_DOWN").
        *   Per-destination port.
    *   Metrics include total bytes, total packets, peak bandwidth, and timestamps.
    *   Real-time bandwidth calculation for recent traffic.
    *   Top-N port analysis based on bytes transferred.
    *   All statistics are accessible via the HTTP API.
    *   Supports a statistical multiplier for scenarios involving sampled traffic.
*   **Dynamic Configuration**:
    *   An HTTP API allows for real-time viewing of statistics.
    *   Rate limiting parameters (defaults and per-identifier) can be updated dynamically without restarting the proxy.

## Build Instructions

To build RabbitProxy, ensure you have Go installed (version 1.22 or newer recommended for HTTP path parameter support in `net/http`).

```bash
go build
```

This will produce an executable named `rabbitproxy` (or `rabbitproxy.exe` on Windows).

## Running the Proxy

The proxy is configured via command-line flags:

*   `-listen <addr:port>`: Address and port for the proxy to listen on.
    *   Default: `localhost:8080`
*   `-target <addr:port>`: Address and port of the target backend server to proxy connections to.
    *   Default: `localhost:8000`
*   `-rate <MBps>`: Default rate limit in Megabytes per second (MB/s) applied to each new client stream (separately for upload and download).
    *   Default: `1.0`
*   `-burst <MB>`: Default burst limit in Megabytes (MB) allowed for each new client stream.
    *   Default: `2.0`
*   `-api <addr:port>`: Address and port for the HTTP API server.
    *   Default: `localhost:8081`

**Example:**

```bash
./rabbitproxy -listen :9090 -target backend.example.com:80 -rate 2 -burst 4 -api :9091
```

This command starts RabbitProxy listening on port `9090`, forwarding traffic to `backend.example.com:80`. New client streams will default to a rate limit of 2 MB/s and a burst of 4 MB. The HTTP API will be available on port `9091`.

For testing the backend, you can use `nc` (netcat):
`nc -lk 8000` (listens on port 8000 and prints received data).

## HTTP API Documentation

The HTTP API provides endpoints for dynamic configuration and statistics viewing. The base URL depends on the `-api` flag. Assuming default `-api localhost:8081`:

### Rate Limit Endpoints

*   **Get Default Rate Limit Settings**
    *   `GET /config/ratelimit/default`
    *   Description: Retrieves the current default rate and burst limits applied to new client streams.
    *   Response (200 OK):
        ```json
        {
            "rate_bps": 1048576,  // Rate in Bytes per second
            "burst_bps": 2097152   // Burst in Bytes
        }
        ```

*   **Set Default Rate Limit Settings**
    *   `PUT /config/ratelimit/default`
    *   Description: Updates the default rate and burst limits for new client streams.
    *   Request Body:
        ```json
        {
            "rate_bps": 2097152,  // New rate in Bytes per second
            "burst_bps": 4194304   // New burst in Bytes
        }
        ```
    *   Response (200 OK):
        ```json
        {
            "message": "Default rate limit updated successfully."
        }
        ```
    *   Error Responses: 400 Bad Request for invalid input.

*   **Get Rate Limit for a Specific Identifier**
    *   `GET /config/ratelimit/identifier/{id}`
    *   Description: Retrieves the rate and burst limits for a specific identifier (e.g., `1.2.3.4_up`, `another_id_down`).
    *   Path Parameter: `{id}` - The unique identifier for the rate limit bucket.
    *   Response (200 OK):
        ```json
        {
            "rate_bps": 524288,
            "burst_bps": 1048576
        }
        ```
    *   Error Responses: 404 Not Found if the identifier does not have specific settings.

*   **Set Rate Limit for a Specific Identifier**
    *   `PUT /config/ratelimit/identifier/{id}`
    *   Description: Sets or updates the rate and burst limits for a specific identifier. If the identifier doesn't exist, a new rate limit bucket is created for it.
    *   Path Parameter: `{id}` - The unique identifier.
    *   Request Body:
        ```json
        {
            "rate_bps": 524288,
            "burst_bps": 1048576
        }
        ```
    *   Response (200 OK):
        ```json
        {
            "message": "Rate limit for identifier '{id}' updated successfully."
        }
        ```
    *   Error Responses: 400 Bad Request for invalid input.

### Statistics Endpoints

*   **Get Global Traffic Statistics**
    *   `GET /stats/global`
    *   Description: Retrieves aggregated global traffic statistics.
    *   Response (200 OK): (Structure matches `trafficstats.GlobalStats`)
        ```json
        {
            "TotalBytesForwarded": 1024000,
            "TotalPacketsForwarded": 1000,
            "PeakBandwidth": 125000.0, // Bytes per second
            "PeakBandwidthTimestamp": "2023-10-27T10:30:00Z"
        }
        ```

*   **Get Statistics for a Specific Source IP**
    *   `GET /stats/ip/{ip}`
    *   Description: Retrieves traffic statistics for a specific source IP address.
    *   Path Parameter: `{ip}` - The source IP address.
    *   Response (200 OK): (Structure matches `trafficstats.GlobalStats`)
    *   Error Responses: 404 Not Found.

*   **Get Statistics for a Specific Protocol/Stream Label**
    *   `GET /stats/protocol/{protocol}`
    *   Description: Retrieves traffic statistics for a specific protocol or stream label (e.g., "TCP_UP", "TCP_DOWN").
    *   Path Parameter: `{protocol}` - The protocol/stream label.
    *   Response (200 OK): (Structure matches `trafficstats.GlobalStats`)
    *   Error Responses: 404 Not Found.

*   **Get Statistics for a Specific Port**
    *   `GET /stats/port/{port}`
    *   Description: Retrieves traffic statistics for a specific port number.
    *   Path Parameter: `{port}` - The port number.
    *   Response (200 OK): (Structure matches `trafficstats.GlobalStats`)
    *   Error Responses: 400 Bad Request for invalid port, 404 Not Found.

*   **Get Top N Ports by Bytes Transferred**
    *   `GET /stats/ports/topn[?n=X]`
    *   Description: Retrieves a list of the top N ports ranked by total bytes transferred.
    *   Query Parameter: `n` (optional) - The number of top ports to return. Defaults to 10.
    *   Response (200 OK):
        ```json
        [
            {
                "Port": 80,
                "Stats": { "TotalBytesForwarded": 512000, ... }
            },
            {
                "Port": 443,
                "Stats": { "TotalBytesForwarded": 256000, ... }
            }
            // ... up to N items
        ]
        ```
