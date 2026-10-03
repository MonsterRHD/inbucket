package msghub

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/inbucket/inbucket/v3/pkg/extension"
	"github.com/inbucket/inbucket/v3/pkg/extension/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testListener implements the Listener interface, mock for unit tests
type testListener struct {
	messages   []*event.MessageMetadata // received messages
	deletes    []string                 // received deletes
	wantEvents int                      // how many events this listener wants to receive
	errorAfter int                      // when != 0, event count until Receive() begins returning error
	gotEvents  int

	done     chan struct{} // closed once we have received wantMessages
	overflow chan struct{} // closed if we receive wantMessages+1
}

func newTestListener(want int) *testListener {
	l := &testListener{
		messages:   make([]*event.MessageMetadata, 0, want*2),
		deletes:    make([]string, 0, want*2),
		wantEvents: want,
		done:       make(chan struct{}),
		overflow:   make(chan struct{}),
	}
	if want == 0 {
		close(l.done)
	}
	return l
}

// Receive a Message, store it in the messages slice, close applicable channels, and return an error
// if instructed
func (l *testListener) Receive(msg event.MessageMetadata) error {
	l.gotEvents++
	l.messages = append(l.messages, &msg)
	if l.gotEvents == l.wantEvents {
		close(l.done)
	}
	if l.gotEvents == l.wantEvents+1 {
		close(l.overflow)
	}
	if l.errorAfter > 0 && l.gotEvents > l.errorAfter {
		return errors.New("too many messages")
	}
	return nil
}

func (l *testListener) Delete(mailbox string, id string) error {
	l.gotEvents++
	l.deletes = append(l.deletes, mailbox+"/"+id)
	return nil
}

// String formats the got vs wanted message counts
func (l *testListener) String() string {
	return fmt.Sprintf("got %v messages, wanted %v", len(l.messages), l.wantEvents)
}

// nopListener accepts unlimited events without tracking them; used by shutdown and
// backpressure tests where event counts are irrelevant.
type nopListener struct{}

func (nopListener) Receive(msg event.MessageMetadata) error { return nil }
func (nopListener) Delete(mailbox string, id string) error  { return nil }

func TestHubNew(t *testing.T) {
	hub := New(5, extension.NewHost())
	if hub == nil {
		t.Fatal("New() == nil, expected a new Hub")
	}
}

func TestHubZeroLen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := New(0, extension.NewHost())
	go hub.Start(ctx)
	m := event.MessageMetadata{}
	for range 100 {
		require.NoError(t, hub.Dispatch(m))
	}
	// Ensures Hub doesn't panic
}

func TestHubZeroListeners(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := New(5, extension.NewHost())
	go hub.Start(ctx)
	m := event.MessageMetadata{}
	for range 100 {
		require.NoError(t, hub.Dispatch(m))
	}
	// Ensures Hub doesn't panic
}

func TestHubOneListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := New(5, extension.NewHost())
	go hub.Start(ctx)
	m := event.MessageMetadata{}
	l := newTestListener(1)

	require.NoError(t, hub.AddListener(l))
	require.NoError(t, hub.Dispatch(m))
	// Wait for messages
	select {
	case <-l.done:
	case <-time.After(time.Second):
		t.Error("Timeout:", l)
	}
}

func TestHubRemoveListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := New(5, extension.NewHost())
	go hub.Start(ctx)
	m := event.MessageMetadata{}
	l := newTestListener(1)

	require.NoError(t, hub.AddListener(l))
	require.NoError(t, hub.Dispatch(m))
	require.NoError(t, hub.RemoveListener(l))
	require.NoError(t, hub.Dispatch(m))
	require.NoError(t, hub.Sync())
	// Wait for messages
	select {
	case <-l.overflow:
		t.Error(l)
	case <-time.After(50 * time.Millisecond):
		// Expected result, no overflow
	}
}

func TestHubRemoveListenerOnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := New(5, extension.NewHost())
	go hub.Start(ctx)
	m := event.MessageMetadata{}

	// error after 1 means listener should receive 2 messages before being removed
	l := newTestListener(2)
	l.errorAfter = 1

	require.NoError(t, hub.AddListener(l))
	require.NoError(t, hub.Dispatch(m))
	require.NoError(t, hub.Dispatch(m))
	require.NoError(t, hub.Dispatch(m))
	require.NoError(t, hub.Dispatch(m))
	require.NoError(t, hub.Sync())
	// Wait for messages
	select {
	case <-l.overflow:
		t.Error(l)
	case <-time.After(50 * time.Millisecond):
		// Expected result, no overflow
	}
}

func TestHubHistoryReplay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := New(100, extension.NewHost())
	go hub.Start(ctx)
	l1 := newTestListener(3)
	require.NoError(t, hub.AddListener(l1))
	// Broadcast 3 messages with no listeners
	msgs := make([]event.MessageMetadata, 3)
	for i := range msgs {
		msgs[i] = event.MessageMetadata{
			Subject: fmt.Sprintf("subj %v", i),
		}
		require.NoError(t, hub.Dispatch(msgs[i]))
	}

	// Wait for messages (live)
	select {
	case <-l1.done:
	case <-time.After(time.Second):
		t.Fatal("Timeout:", l1)
	}

	// Add a new listener
	l2 := newTestListener(3)
	require.NoError(t, hub.AddListener(l2))
	// Wait for messages (history)
	select {
	case <-l2.done:
	case <-time.After(time.Second):
		t.Fatal("Timeout:", l2)
	}

	for i := range msgs {
		got := l2.messages[i].Subject
		want := msgs[i].Subject
		if got != want {
			t.Errorf("msg[%v].Subject == %q, want %q", i, got, want)
		}
	}
}

func TestHubHistoryDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := New(100, extension.NewHost())
	go hub.Start(ctx)
	l1 := newTestListener(3)
	require.NoError(t, hub.AddListener(l1))
	// Broadcast 3 messages with no listeners
	msgs := make([]event.MessageMetadata, 3)
	for i := range msgs {
		msgs[i] = event.MessageMetadata{
			Mailbox: "hub",
			ID:      strconv.Itoa(i),
			Subject: fmt.Sprintf("subj %v", i),
		}
		require.NoError(t, hub.Dispatch(msgs[i]))
	}

	// Wait for messages (live)
	select {
	case <-l1.done:
	case <-time.After(time.Second):
		t.Fatal("Timeout:", l1)
	}

	require.NoError(t, hub.Delete("hub", "1")) // Delete a message
	require.NoError(t, hub.Delete("zzz", "0")) // Attempt to delete non-existent mailbox message

	// Add a new listener, waits for 2 messages
	l2 := newTestListener(2)
	require.NoError(t, hub.AddListener(l2))
	// Wait for messages (history)
	select {
	case <-l2.done:
	case <-time.After(time.Second):
		t.Fatal("Timeout:", l2)
	}

	want := []string{"subj 0", "subj 2"}
	for i := range want {
		got := l2.messages[i].Subject
		if got != want[i] {
			t.Errorf("msg[%v].Subject == %q, want %q", i, got, want[i])
		}
	}
}

func TestHubHistoryReplayWrap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := New(5, extension.NewHost())
	go hub.Start(ctx)
	l1 := newTestListener(20)
	require.NoError(t, hub.AddListener(l1))
	// Broadcast more messages than the hub can hold
	msgs := make([]event.MessageMetadata, 20)
	for i := range msgs {
		msgs[i] = event.MessageMetadata{
			Subject: fmt.Sprintf("subj %v", i),
		}
		require.NoError(t, hub.Dispatch(msgs[i]))
	}

	// Wait for messages (live)
	select {
	case <-l1.done:
	case <-time.After(time.Second):
		t.Fatal("Timeout:", l1)
	}

	// Add a new listener
	l2 := newTestListener(5)
	require.NoError(t, hub.AddListener(l2))
	// Wait for messages (history)
	select {
	case <-l2.done:
	case <-time.After(time.Second):
		t.Fatal("Timeout:", l2)
	}

	for i := range 5 {
		got := l2.messages[i].Subject
		want := msgs[i+15].Subject
		if got != want {
			t.Errorf("msg[%v].Subject == %q, want %q", i, got, want)
		}
	}
}

func TestHubHistoryReplayWrapAfterDelete(t *testing.T) {
	bufferSize := 5

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := New(bufferSize, extension.NewHost())
	go hub.Start(ctx)

	waitForMessages := func(n int) {
		l := newTestListener(n)
		require.NoError(t, hub.AddListener(l))
		select {
		case <-l.done:
		case <-time.After(time.Second):
			t.Fatal("Timeout:", l)
		}
	}

	// Broadcast more messages than the hub can hold.
	msgs := make([]event.MessageMetadata, 10)
	for i := range msgs {
		msgs[i] = event.MessageMetadata{
			Mailbox: "first",
			ID:      strconv.Itoa(i),
			Subject: fmt.Sprintf("subj %v", i),
		}
		require.NoError(t, hub.Dispatch(msgs[i]))
	}
	waitForMessages(bufferSize)

	// Buffer must be configured size.
	require.Equal(t, bufferSize, hub.history.Len())

	// Delete a message still present in buffer.
	require.NoError(t, hub.Delete("first", "7"))
	// Broadcast another set of messages.
	for i := range msgs {
		msgs[i] = event.MessageMetadata{
			Mailbox: "second",
			ID:      strconv.Itoa(i),
			Subject: fmt.Sprintf("subj %v", i),
		}
		require.NoError(t, hub.Dispatch(msgs[i]))
	}
	waitForMessages(bufferSize)

	// Ensure the buffer did not shrink after delete.
	got := hub.history.Len()
	assert.Equal(t, bufferSize, got, "got buffer size %d, wanted %d", got, bufferSize)
}

func TestHubContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	hub := New(5, extension.NewHost())
	go hub.Start(ctx)
	m := event.MessageMetadata{}
	l := newTestListener(1)

	require.NoError(t, hub.AddListener(l))
	require.NoError(t, hub.Dispatch(m))
	require.NoError(t, hub.Sync())
	cancel()

	// The actor must stop; queued-work draining must not hang shutdown.
	select {
	case <-hub.Done():
	case <-time.After(time.Second):
		t.Fatal("hub did not stop after context cancellation")
	}

	// Wait for messages
	select {
	case <-l.overflow:
		t.Error(l)
	case <-time.After(50 * time.Millisecond):
		// Expected result, no overflow
	}
}

func TestHubQueueFullReturnsError(t *testing.T) {
	// An unstarted hub keeps every enqueue buffered; once the buffer is saturated,
	// non-blocking operations report ErrQueueFull rather than blocking callers.
	hub := New(5, extension.NewHost())
	l := nopListener{}
	require.NoError(t, hub.AddListener(l)) // consumes one buffer slot

	m := event.MessageMetadata{}
	for range opChanLen - 1 {
		require.NoError(t, hub.Dispatch(m))
	}

	// Buffer now holds opChanLen operations; further non-blocking enqueues are rejected.
	require.ErrorIs(t, hub.Dispatch(m), ErrQueueFull)
	require.ErrorIs(t, hub.Delete("mb", "1"), ErrQueueFull)

	// Starting drains the buffer and the hub remains usable.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Start(ctx)
	require.NoError(t, hub.Sync())
	assert.NoError(t, hub.Dispatch(m))
}

func TestHubShutdownDrainsQueuedOps(t *testing.T) {
	// Operations queued before cancellation must be processed in submission order, even
	// when cancellation happens before the actor starts: the AddListener replay op runs
	// first, followed by the dispatches delivered live in their enqueue order.
	hub := New(100, extension.NewHost())

	const count = 50 // AddListener op + 50 dispatches fit the opChanLen buffer
	l := newTestListener(count)
	require.NoError(t, hub.AddListener(l))

	msgs := make([]event.MessageMetadata, count)
	for i := range msgs {
		msgs[i] = event.MessageMetadata{
			Mailbox: "hub",
			ID:      strconv.Itoa(i),
			Subject: fmt.Sprintf("subj %v", i),
		}
		require.NoError(t, hub.Dispatch(msgs[i]))
	}

	// Cancel before starting, so shutdown races a fully buffered queue on the first select.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	go hub.Start(ctx)

	select {
	case <-hub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("hub did not finish draining queued operations")
	}

	require.Len(t, l.messages, count, "every queued event should be delivered during drain")
	for i := range count {
		got := l.messages[i].Subject
		want := msgs[i].Subject
		if got != want {
			t.Errorf("msg[%v].Subject == %q, want %q", i, got, want)
		}
	}
}

func TestHubRejectsOperationsAfterShutdown(t *testing.T) {
	hub := New(5, extension.NewHost())
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Start(ctx)

	cancel()
	select {
	case <-hub.Done():
	case <-time.After(time.Second):
		t.Fatal("hub did not stop after context cancellation")
	}

	m := event.MessageMetadata{}
	l := nopListener{}
	require.ErrorIs(t, hub.Dispatch(m), ErrClosed)
	require.ErrorIs(t, hub.Delete("mb", "1"), ErrClosed)
	require.ErrorIs(t, hub.AddListener(l), ErrClosed)
	require.ErrorIs(t, hub.RemoveListener(l), ErrClosed)
	require.ErrorIs(t, hub.Sync(), ErrClosed)
}

func TestHubShutdownConcurrentDispatchNoPanic(t *testing.T) {
	// Dispatch/Delete racing context cancellation must never panic on a closed channel
	// (run under -race in CI); the only legal outcomes are delivery, ErrQueueFull, or
	// ErrClosed after the drain completes.
	hub := New(10, extension.NewHost())
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Start(ctx)
	l := nopListener{}
	require.NoError(t, hub.AddListener(l))

	const goroutines = 8
	const iterations = 300
	var wg sync.WaitGroup
	start := make(chan struct{})
	errCh := make(chan error, goroutines*iterations)
	legal := func(err error) bool {
		return errors.Is(err, ErrQueueFull) || errors.Is(err, ErrClosed)
	}
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for i := range iterations {
				m := event.MessageMetadata{Mailbox: "mb", ID: strconv.Itoa(g*iterations + i)}
				if err := hub.Dispatch(m); err != nil && !legal(err) {
					errCh <- err
				}
				if i%7 == 0 {
					if err := hub.Delete("mb", m.ID); err != nil && !legal(err) {
						errCh <- err
					}
				}
			}
		}(g)
	}

	close(start)
	// Let dispatchers run briefly, then cancel while traffic is in flight.
	time.Sleep(time.Millisecond)
	cancel()

	wg.Wait()
	select {
	case <-hub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("hub did not stop with concurrent dispatchers in flight")
	}
	close(errCh)
	for err := range errCh {
		t.Fatalf("unexpected hub error during shutdown: %v", err)
	}
}
