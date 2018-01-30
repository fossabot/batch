// Package batch contains types & methods for collecting items in batches of a configured size.
package batch

import (
	"time"
)

type (
	// The Batch type contains items added to it and writes to a channel when the batch is full or
	// has been waiting a configured time.
	Batch struct {
		maxSize int
		maxWait time.Duration

		empty []interface{}
		inner []interface{}

		notify chan []interface{}
		add    chan interface{}
		close  chan bool
	}
)

// New creates a new Batch with a given size & wait time. The batch writes to a channel when
// either the batch is full or the batch has waited the configured time.
func New(maxSize int, maxWait time.Duration) *Batch {
	batch := &Batch{
		maxSize: maxSize,
		maxWait: maxWait,
		inner:   make([]interface{}, 0, maxSize),
		empty:   make([]interface{}, 0, maxSize),
		notify:  make(chan []interface{}),
		add:     make(chan interface{}),
		close:   make(chan bool),
	}

	go batch.wait()

	return batch
}

// Add adds an item to the batch.
func (b *Batch) Add(item interface{}) {
	b.add <- item
}

// Ready indicates that either the batch is full or has waited the configured time.
func (b *Batch) Ready() <-chan []interface{} {
	return b.notify
}

// Close causes the batch to stop checking its size & duration. Should be used when the
// batch is no longer required.
func (b *Batch) Close() {
	b.close <- true
}

func (b *Batch) wait() {
	for {
		select {
		// If we've reached the maximum wait time
		case <-time.Tick(b.maxWait):
			// If we have items in the batch
			if len(b.inner) > 0 {
				// Write batch contents to channel,
				b.notify <- b.inner
			}

			// Clear batched items
			b.inner = b.empty
			break

		// If an item has been added to the batch.
		case item := <-b.add:
			b.inner = append(b.inner, item)

			// If we've reched the maximum batch size, write batch
			// contents to channel, clear batched item and add new
			// item to empty batch.
			if len(b.inner) == b.maxSize {
				b.notify <- b.inner
				b.inner = b.empty
			}

			break

		// If the batch has been closed, exit the loop
		case <-b.close:
			return
		}
	}
}
