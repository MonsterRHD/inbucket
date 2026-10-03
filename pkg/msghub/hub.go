package msghub

import (
	"container/ring"
	"context"
	"errors"
	"sync"

	"github.com/inbucket/inbucket/v3/pkg/extension"
	"github.com/inbucket/inbucket/v3/pkg/extension/event"
	"github.com/rs/zerolog/log"
)

// Length of msghub operation queue
const opChanLen = 100

// ErrClosed is returned by Hub operations submitted after (or during) shutdown once the
// actor has stopped accepting operations.
var ErrClosed = errors.New("msghub: hub is stopped")

// ErrQueueFull is returned by non-blocking Hub operations when the operation queue is
// saturated; the operation was not queued and the caller may retry or report the failure.
var ErrQueueFull = errors.New("msghub: operation queue full")

// Listener receives the contents of the history buffer, followed by new messages
type Listener interface {
	Receive(msg event.MessageMetadata) error
	Delete(mailbox string, id string) error
}

// Hub relays messages on to its listeners
type Hub struct {
	// history buffer, points next Message to write.  Proceeding non-nil entry is oldest Message
	history   *ring.Ring
	listeners map[Listener]struct{} // listeners interested in new messages
	opChan    chan func(h *Hub)     // operations queued for this actor

	// freeSlots is a counting semaphore with one token per free opChan slot: an
	// operation producer must take a token before it sends, and the actor returns the
	// token immediately after taking an operation.  A blocked reservation can therefore
	// always be interrupted by done on shutdown, without ever closing opChan -- a send
	// can never race a channel close.
	freeSlots chan struct{}

	// closeMu guards closed.  Producers hold it while deciding on/committing a send.
	closeMu sync.Mutex
	closed  bool
	done    chan struct{} // closed once the actor has drained its queue and stopped
}

// New constructs a new Hub which will cache historyLen messages in memory for playback to future
// listeners.  A goroutine is created to handle incoming messages; it will run until the provided
// context is canceled.
func New(historyLen int, extHost *extension.Host) *Hub {
	hub := &Hub{
		history:   ring.New(historyLen),
		listeners: make(map[Listener]struct{}),
		opChan:    make(chan func(h *Hub), opChanLen),
		freeSlots: make(chan struct{}, opChanLen),
		done:      make(chan struct{}),
	}
	for range opChanLen {
		hub.freeSlots <- struct{}{}
	}

	// Register an extension event listener for MessageStored.
	extHost.Events.AfterMessageStored.AddListener("msghub",
		func(msg event.MessageMetadata) {
			if err := hub.Dispatch(msg); err != nil {
				hub.reportEnqueueErr("MessageStored broadcast", err)
			}
		})

	extHost.Events.AfterMessageDeleted.AddListener("msghub",
		func(msg event.MessageMetadata) {
			if err := hub.Delete(msg.Mailbox, msg.ID); err != nil {
				hub.reportEnqueueErr("MessageDeleted broadcast", err)
			}
		})

	return hub
}

// Start Hub processing loop.
func (hub *Hub) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Shutdown: process every operation committed before closure, in FIFO order.
			hub.shutdown()
			return
		case op := <-hub.opChan:
			// Return the slot token before running the operation, so producers reserve
			// against physical queue capacity rather than operation duration.
			hub.freeSlots <- struct{}{}
			hub.runOp(op)
		}
	}
}

// shutdown drains all operations still queued, preserving their submission order, and then
// marks the Hub closed.  The drain interleaves with enqueue/enqueueAlways under closeMu: an
// operation observed in the queue is processed, and only an emptiness observed while holding
// closeMu commits the closed decision, so no producer can slip an operation past it.
func (hub *Hub) shutdown() {
	hub.closeMu.Lock()
	defer hub.closeMu.Unlock()

	for {
		select {
		case op := <-hub.opChan:
			hub.freeSlots <- struct{}{}
			// Run without holding the lock: a blocking listener call (full event queue)
			// must not stall enqueue coordination; producers instead observe the queue.
			hub.closeMu.Unlock()
			hub.runOp(op)
			hub.closeMu.Lock()
		default:
			hub.closed = true
			close(hub.done)
			return
		}
	}
}

// Done returns a channel closed once the Hub has drained its queue and stopped.
func (hub *Hub) Done() <-chan struct{} {
	return hub.done
}

// enqueue submits an operation without blocking.  It returns nil once queued, ErrQueueFull
// if no slot is available, or ErrClosed once the Hub has stopped.
func (hub *Hub) enqueue(op func(h *Hub)) error {
	hub.closeMu.Lock()
	defer hub.closeMu.Unlock()

	if hub.closed {
		return ErrClosed
	}
	select {
	case <-hub.freeSlots:
	default:
		return ErrQueueFull
	}
	// A token reserves exactly one opChan slot; closed cannot flip while closeMu is held.
	hub.opChan <- op
	return nil
}

// enqueueAlways submits an operation that must not be lost while the Hub is alive, waiting
// for a queue slot if needed; it returns ErrClosed without queuing once the Hub has stopped.
func (hub *Hub) enqueueAlways(op func(h *Hub)) error {
	hub.closeMu.Lock()
	if hub.closed {
		hub.closeMu.Unlock()
		return ErrClosed
	}
	hub.closeMu.Unlock()

	// Reserve a slot without holding closeMu: the actor keeps returning tokens while
	// draining, and done interrupts the wait when the hub stops first.
	select {
	case <-hub.freeSlots:
	case <-hub.done:
		return ErrClosed
	}

	hub.closeMu.Lock()
	defer hub.closeMu.Unlock()
	if hub.closed {
		// Shutdown committed between the reservation and the send; give the token back.
		hub.freeSlots <- struct{}{}
		return ErrClosed
	}
	hub.opChan <- op
	return nil
}

// Dispatch queues a message for broadcast by the hub.  The message will be placed into the
// history buffer and then relayed to all registered listeners.  A non-nil error means the
// message was not queued (ErrQueueFull or ErrClosed).
func (hub *Hub) Dispatch(msg event.MessageMetadata) error {
	return hub.enqueue(func(h *Hub) {
		if h.history != nil {
			// Add to history buffer
			h.history.Value = msg
			h.history = h.history.Next()

			// Relay event to all listeners, removing listeners if they return an error.
			for l := range h.listeners {
				if err := l.Receive(msg); err != nil {
					delete(h.listeners, l)
				}
			}
		}
	})
}

// Delete removes the message from the history buffer and instructs listeners to do the same.
// A non-nil error means the deletion was not queued (ErrQueueFull or ErrClosed).
func (hub *Hub) Delete(mailbox string, id string) error {
	return hub.enqueue(func(h *Hub) {
		if h.history == nil {
			return
		}

		// Locate and remove history entry.
		p := h.history
		end := p
		for {
			if next, ok := p.Next().Value.(event.MessageMetadata); ok {
				if mailbox == next.Mailbox && id == next.ID {
					p.Next().Value = nil
					break
				}
			}
			if p = p.Next(); p == end {
				break
			}
		}

		// Relay event to all listeners, removing listeners if they return an error.
		for l := range h.listeners {
			if err := l.Delete(mailbox, id); err != nil {
				delete(h.listeners, l)
			}
		}
	})
}

// AddListener registers a listener to receive broadcasted messages.  The history buffer is
// played back before the registration takes effect.  A non-nil error means the listener was
// not registered (ErrQueueFull or ErrClosed).
func (hub *Hub) AddListener(l Listener) error {
	return hub.enqueue(func(h *Hub) {
		// Playback log
		h.history.Do(func(v interface{}) {
			if v != nil {
				_ = l.Receive(v.(event.MessageMetadata))
			}
		})

		// Add to listeners
		h.listeners[l] = struct{}{}
	})
}

// RemoveListener deletes a listener registration, it will cease to receive messages.  The
// removal is guaranteed to be queued for as long as the Hub is alive, even when the operation
// queue is saturated; it returns ErrClosed without queuing after the Hub has stopped.
func (hub *Hub) RemoveListener(l Listener) error {
	return hub.enqueueAlways(func(h *Hub) {
		delete(h.listeners, l)
	})
}

// Sync blocks until the msghub has processed its queue up to this point, useful
// for unit tests.  It returns ErrClosed if the Hub stops before the barrier is queued.
func (hub *Hub) Sync() error {
	done := make(chan struct{})
	if err := hub.enqueueAlways(func(_ *Hub) {
		close(done)
	}); err != nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-hub.done:
		// Every queued operation is drained during shutdown, so this is defensive.
		return ErrClosed
	}
}

// reportEnqueueErr logs an enqueue failure.  A shutdown racing an event is expected and
// logged at debug level; saturation is an observable backpressure condition worth warning on.
func (hub *Hub) reportEnqueueErr(action string, err error) {
	switch {
	case errors.Is(err, ErrClosed):
		log.Debug().Str("module", "msghub").Err(err).Msgf("%s rejected after shutdown", action)
	case errors.Is(err, ErrQueueFull):
		log.Warn().Str("module", "msghub").Err(err).Msgf("%s dropped: operation queue full", action)
	default:
		log.Error().Str("module", "msghub").Err(err).Msgf("%s failed", action)
	}
}

func (hub *Hub) runOp(op func(*Hub)) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				log.Error().Str("module", "msghub").Err(err).Msg("Operation panicked")
			} else {
				log.Error().Str("module", "msghub").Interface("recover", r).Msg("Operation panicked")
			}
		}
	}()

	op(hub)
}
