package forwarder

// Forwarder defines the interface for port forwarding operations.
type Forwarder interface {
	Start() error
	Stop() error
}
