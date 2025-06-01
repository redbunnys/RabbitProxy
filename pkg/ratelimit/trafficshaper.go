package ratelimit

import (
	"sync"
	"time"
	// Assuming TokenBucket is in the same package (e.g., from tokenbucket.go)
	// "rabbitproxy/pkg/ratelimit"
)

// PacketData represents a unit of data to be processed by the TrafficShaper.
type PacketData struct {
	Data      []byte
	AddedTime time.Time
}

// TrafficShaper implements a token bucket-based traffic shaping mechanism.
type TrafficShaper struct {
	queue       chan PacketData
	tokenBucket *TokenBucket // Assumes TokenBucket is defined in this package
	outputFunc  func(item PacketData)
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

// NewTrafficShaper creates and starts a new TrafficShaper.
// bucket: The TokenBucket instance to regulate traffic.
// queueSize: The maximum number of items the internal queue can hold.
// outputFunc: The callback function to process items dequeued and permitted by the token bucket.
func NewTrafficShaper(bucket *TokenBucket, queueSize int, outputFunc func(item PacketData)) *TrafficShaper {
	if bucket == nil {
		// A token bucket is essential for the shaper to function.
		// Handle this error, perhaps by returning nil or panicking.
		// For now, let's assume a valid bucket is always provided.
		// Or, create a default one if appropriate for the design.
		panic("TrafficShaper: TokenBucket cannot be nil")
	}
	if outputFunc == nil {
		panic("TrafficShaper: outputFunc cannot be nil")
	}
	if queueSize <= 0 {
		queueSize = 100 // Default queue size
	}

	ts := &TrafficShaper{
		queue:       make(chan PacketData, queueSize),
		tokenBucket: bucket,
		outputFunc:  outputFunc,
		stopChan:    make(chan struct{}),
	}

	ts.wg.Add(1)
	go ts.run()

	return ts
}

// Enqueue attempts to add data to the shaper's queue.
// Returns true if the data was successfully enqueued, false if the queue is full.
func (ts *TrafficShaper) Enqueue(data []byte) bool {
	item := PacketData{
		Data:      data,
		AddedTime: time.Now(),
	}

	select {
	case ts.queue <- item:
		return true
	default:
		// Queue is full, non-blocking send failed
		return false
	}
}

// run is the internal processing loop for the TrafficShaper.
// It dequeues items and processes them according to the token bucket's rate.
func (ts *TrafficShaper) run() {
	defer ts.wg.Done()

	for {
		select {
		case <-ts.stopChan:
			// Drain the queue? Or just stop processing new items?
			// For now, just stop processing. Items in queue will be lost if not handled.
			// A more graceful shutdown might process remaining items or provide a way to retrieve them.
			return
		case item, ok := <-ts.queue:
			if !ok {
				// Queue channel closed, should not happen if Stop() is used correctly.
				return
			}

			// Wait for a token. Assuming 1 token per packet.
			// The cost could be item-dependent in a more advanced shaper.
			err := ts.tokenBucket.Wait(1) // Cost of 1 token per item
			if err != nil {
				// This might happen if the context within Wait is cancelled,
				// or if Wait is modified to return errors for other reasons.
				// Log this or handle as appropriate. For now, we assume Wait blocks until success or stop.
				// If tokenBucket itself is stopped or rate set to 0, Wait might block indefinitely.
				// Consider what happens if stopChan is closed while waiting on Wait().
				// A more robust Wait might take a context.

				// If Wait failed due to a context cancellation that might be tied to stopChan,
				// we should check if we need to exit.
				select {
				case <-ts.stopChan:
					return // Exit if stop was signalled during wait
				default:
					// Continue or log error
				}
				// For now, let's assume if Wait returns (even with error), we try to continue or exit if stopChan is closed.
				// If Wait can return error for other reasons (e.g. bucket disabled), we might log and drop item.
				// fmt.Printf("Error waiting for token: %v. Item might be dropped or retried depending on policy.\n", err)
				// For this basic version, we'll assume Wait blocks until a token is available or stopChan is handled by another select case.
				// The current TokenBucket.Wait() blocks indefinitely if no tokens and rate is >0.
				// It doesn't currently return an error unless count is <=0.
			}

			// Before calling outputFunc, ensure we haven't been stopped.
			// This is a secondary check in case stopChan was signaled while tokenBucket.Wait was blocking.
			select {
			case <-ts.stopChan:
				// If stopped, don't process the item, just return.
				// The item remains "taken" from the queue but not processed by outputFunc.
				return
			default:
				// Not stopped, proceed to call outputFunc
				ts.outputFunc(item)
			}
		}
	}
}

// Stop signals the TrafficShaper to stop processing and waits for it to shut down.
func (ts *TrafficShaper) Stop() {
	close(ts.stopChan)
	ts.wg.Wait()
	// Consider closing the ts.queue channel here if no more items will be enqueued
	// and run() handles channel closure gracefully. However, Enqueue might panic if sending to closed channel.
	// For now, rely on stopChan to terminate run().
}
