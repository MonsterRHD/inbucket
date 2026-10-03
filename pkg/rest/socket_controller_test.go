package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/mail"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/inbucket/inbucket/v3/pkg/extension"
	"github.com/inbucket/inbucket/v3/pkg/extension/event"
	"github.com/inbucket/inbucket/v3/pkg/msghub"
	"github.com/inbucket/inbucket/v3/pkg/server/web"
	"github.com/inbucket/inbucket/v3/pkg/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// monitorTestTimeout bounds WebSocket reads so a broken server fails the test instead of hanging.
const monitorTestTimeout = 5 * time.Second

// monitorDialer dials test servers directly; the default dialer would honor proxy env vars.
var monitorDialer = &websocket.Dialer{}

// startMsgHub returns a running *msghub.Hub with room for every message a test dispatches
// (replayed to listeners that connect later).  The actor goroutine is stopped via t.Cleanup.
func startMsgHub(t *testing.T) *msghub.Hub {
	t.Helper()
	hub := msghub.New(10, extension.NewHost())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go hub.Start(ctx)
	return hub
}

// setupMonitorServer wires a real (running) msg hub into the web server globals and serves the
// global router over HTTP; upgrading WebSocket endpoints needs a real connection to hijack.
// The returned gauge baseline is the value of web.ExpWebSocketConnectsCurrent before any
// connection is made; pass it to dialMonitor so sessions can be awaited on teardown.
func setupMonitorServer(t *testing.T, hub *msghub.Hub) (*httptest.Server, int64) {
	t.Helper()
	mm := test.NewManager()
	setupWebServer(mm, hub)
	server := httptest.NewServer(web.Router)
	// Registered before any connection cleanup so the listener socket is closed after the
	// sessions below have been awaited.
	t.Cleanup(server.Close)
	return server, web.ExpWebSocketConnectsCurrent.Value()
}

// dialMonitor connects a WebSocket client to path on server.  The cleanup closes the connection
// and waits for the server-side handler goroutine to exit, which is required before the msg hub
// may be stopped: httptest.Server.Close does not wait for hijacked (WebSocket) connections, and
// a handler deregistering its hub listener after the hub was stopped races with it.
func dialMonitor(t *testing.T, server *httptest.Server, path string, gaugeBase int64) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + path
	conn, resp, err := monitorDialer.Dial(url, nil)
	require.NoError(t, err)
	// resp.Body is an empty no-op closer once the connection is upgraded.
	_ = resp.Body.Close()
	t.Cleanup(func() {
		_ = conn.Close()
		require.Eventually(t, func() bool {
			return web.ExpWebSocketConnectsCurrent.Value() == gaugeBase
		}, monitorTestTimeout, time.Millisecond, "server handler did not shut down")
	})
	return conn
}

// readMonitorJSON reads the next data message from conn and decodes it into a generic map.
func readMonitorJSON(t *testing.T, conn *websocket.Conn) map[string]interface{} {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(monitorTestTimeout)))
	_, data, err := conn.ReadMessage()
	require.NoError(t, err)
	var decoded interface{}
	require.NoError(t, json.Unmarshal(data, &decoded), "payload: %s", data)
	obj, ok := decoded.(map[string]interface{})
	require.True(t, ok, "expected JSON object, got: %s", data)
	return obj
}

// testMetadata builds a message for dispatch into the hub.
func testMetadata(mailbox, id string) event.MessageMetadata {
	return event.MessageMetadata{
		Mailbox: mailbox,
		ID:      id,
		From:    &mail.Address{Address: "from-" + id + "@host"},
		To:      []*mail.Address{{Address: "to-" + id + "@host"}},
		Subject: "subject " + id,
		Date:    time.Date(2012, 2, 1, 10, 11, 12, 0, time.UTC),
		Size:    1234,
	}
}

// assertMonitorHeader checks the JSONMessageHeaderV1 fields of a decoded monitor payload.
func assertMonitorHeader(t *testing.T, decoded interface{}, want event.MessageMetadata) {
	t.Helper()
	decodedStringEquals(t, decoded, "mailbox", want.Mailbox)
	decodedStringEquals(t, decoded, "id", want.ID)
	decodedStringEquals(t, decoded, "from", "<"+want.From.Address+">")
	decodedStringEquals(t, decoded, "to/[0]", "<"+want.To[0].Address+">")
	decodedStringEquals(t, decoded, "subject", want.Subject)
	decodedStringEquals(t, decoded, "date", "2012-02-01T10:11:12Z")
	decodedNumberEquals(t, decoded, "posix-millis", float64(want.Date.UnixNano()/1000000))
	decodedNumberEquals(t, decoded, "size", float64(want.Size))
}

// monitorEventHeader returns the nested header object of a decoded v2 monitor event.
func monitorEventHeader(t *testing.T, ev map[string]interface{}) map[string]interface{} {
	t.Helper()
	header, ok := ev["header"].(map[string]interface{})
	require.True(t, ok, "event has no header object: %v", ev)
	return header
}

func TestMonitorAllMessagesV1(t *testing.T) {
	hub := startMsgHub(t)
	server, gaugeBase := setupMonitorServer(t, hub)

	// Messages dispatched before connecting are replayed from hub history to new listeners.
	replayed := testMetadata("replay", "0001")
	require.NoError(t, hub.Dispatch(replayed))
	conn := dialMonitor(t, server, "/api/v1/monitor/messages", gaugeBase)
	assertMonitorHeader(t, readMonitorJSON(t, conn), replayed)
	assert.Equal(t, gaugeBase+1, web.ExpWebSocketConnectsCurrent.Value(),
		"open WebSocket connections gauge")

	// Messages dispatched after registration are delivered live.  Receiving the replayed
	// message above proves the listener was registered before this dispatch.
	live := testMetadata("live", "0002")
	require.NoError(t, hub.Dispatch(live))
	assertMonitorHeader(t, readMonitorJSON(t, conn), live)

	// A clean client close tears the session down server-side: the read pump sees the close
	// frame, the listener deregisters from the hub, and the connections gauge drops back.
	require.NoError(t, conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(monitorTestTimeout)))
	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool {
		return web.ExpWebSocketConnectsCurrent.Value() == gaugeBase
	}, monitorTestTimeout, time.Millisecond, "open WebSocket connections gauge")
}

func TestMonitorMailboxMessagesV1(t *testing.T) {
	hub := startMsgHub(t)
	server, gaugeBase := setupMonitorServer(t, hub)

	require.NoError(t, hub.Dispatch(testMetadata("other", "0001")))
	watched := testMetadata("watched", "0002")
	require.NoError(t, hub.Dispatch(watched))
	require.NoError(t, hub.Dispatch(testMetadata("other", "0003")))
	conn := dialMonitor(t, server, "/api/v1/monitor/messages/watched", gaugeBase)
	assertMonitorHeader(t, readMonitorJSON(t, conn), watched)

	// Events are relayed in dispatch order, so receiving 0005 next proves both the replayed
	// and live deliveries for other mailboxes were filtered, not merely delayed.
	require.NoError(t, hub.Dispatch(testMetadata("other", "0004")))
	live := testMetadata("watched", "0005")
	require.NoError(t, hub.Dispatch(live))
	assertMonitorHeader(t, readMonitorJSON(t, conn), live)

	// Documents current behavior: v1 monitors ignore message deletions and emit no event;
	// v2 emits message-deleted.  The subsequent dispatch arriving next proves the delete
	// produced nothing.  Flagged for a follow-up fix.
	require.NoError(t, hub.Delete("watched", "0005"))
	after := testMetadata("watched", "0006")
	require.NoError(t, hub.Dispatch(after))
	assertMonitorHeader(t, readMonitorJSON(t, conn), after)
}

func TestMonitorMailboxAddressV1(t *testing.T) {
	hub := startMsgHub(t)
	server, gaugeBase := setupMonitorServer(t, hub)

	// The {name} path segment goes through Manager.MailboxForAddress; with the test stub's
	// full-address naming policy, user@example.com maps to a mailbox of the same name.
	stored := testMetadata("user@example.com", "0001")
	require.NoError(t, hub.Dispatch(stored))
	conn := dialMonitor(t, server, "/api/v1/monitor/messages/user%40example.com", gaugeBase)
	assertMonitorHeader(t, readMonitorJSON(t, conn), stored)
}

func TestMonitorMailboxInvalidNameV1(t *testing.T) {
	hub := startMsgHub(t)
	setupWebServer(test.NewManager(), hub)

	// Invalid mailbox names are rejected before the connection is upgraded.
	w, err := testRestGet("http://localhost/api/v1/monitor/messages/foo%20bar")
	require.NoError(t, err)
	assert.Equal(t, 500, w.Code)
}

func TestMonitorUpgradeFailedV1(t *testing.T) {
	hub := startMsgHub(t)
	setupWebServer(test.NewManager(), hub)

	// A plain GET without WebSocket upgrade headers fails the handshake; the upgrader writes
	// the 400 itself before the handler error is reported.
	w, err := testRestGet("http://localhost/api/v1/monitor/messages")
	require.NoError(t, err)
	assert.Equal(t, 400, w.Code)
}

func TestMonitorClientCloseV1(t *testing.T) {
	hub := startMsgHub(t)
	server, gaugeBase := setupMonitorServer(t, hub)

	stored := testMetadata("close", "0001")
	require.NoError(t, hub.Dispatch(stored))
	conn := dialMonitor(t, server, "/api/v1/monitor/messages", gaugeBase)

	// Data messages sent by the client are discarded; the monitor keeps relaying events.
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"junk":true}`)))
	assertMonitorHeader(t, readMonitorJSON(t, conn), stored)

	// A clean client close ends the session.  The peer may observe any of: the close-code
	// echo sent by the server read pump's default close handler, the writer's empty close
	// frame, or a dropped TCP connection from the handler teardown; all indicate shutdown.
	require.NoError(t, conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(monitorTestTimeout)))
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(monitorTestTimeout)))
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		assert.Contains(t, []int{
			websocket.CloseNormalClosure,    // echoed by the server read pump
			websocket.CloseNoStatusReceived, // writer's empty close frame
		}, closeErr.Code)
	}
}

func TestMonitorAllMessagesV2(t *testing.T) {
	hub := startMsgHub(t)
	server, gaugeBase := setupMonitorServer(t, hub)

	// Stored messages dispatched before connecting are replayed from hub history.
	stored := testMetadata("replay", "0001")
	require.NoError(t, hub.Dispatch(stored))
	conn := dialMonitor(t, server, "/api/v2/monitor/messages", gaugeBase)
	ev := readMonitorJSON(t, conn)
	decodedStringEquals(t, ev, "variant", "message-stored")
	assertMonitorHeader(t, monitorEventHeader(t, ev), stored)

	// Data messages sent by the client are discarded; the monitor keeps relaying events.
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"junk":true}`)))

	live := testMetadata("live", "0002")
	require.NoError(t, hub.Dispatch(live))
	ev = readMonitorJSON(t, conn)
	decodedStringEquals(t, ev, "variant", "message-stored")
	assertMonitorHeader(t, monitorEventHeader(t, ev), live)

	// Deletions are reported as message-deleted events with the message identifier.
	require.NoError(t, hub.Delete("live", "0002"))
	ev = readMonitorJSON(t, conn)
	decodedStringEquals(t, ev, "variant", "message-deleted")
	decodedStringEquals(t, ev, "identifier/mailbox", "live")
	decodedStringEquals(t, ev, "identifier/id", "0002")
}

func TestMonitorMailboxMessagesV2(t *testing.T) {
	hub := startMsgHub(t)
	server, gaugeBase := setupMonitorServer(t, hub)

	require.NoError(t, hub.Dispatch(testMetadata("other", "0001")))
	watched := testMetadata("watched", "0002")
	require.NoError(t, hub.Dispatch(watched))
	conn := dialMonitor(t, server, "/api/v2/monitor/messages/watched", gaugeBase)
	ev := readMonitorJSON(t, conn)
	decodedStringEquals(t, ev, "variant", "message-stored")
	assertMonitorHeader(t, monitorEventHeader(t, ev), watched)

	// Events are relayed in dispatch order, so receiving message-stored for 0003 next proves
	// the other-mailbox delete was filtered, not merely delayed.
	require.NoError(t, hub.Delete("other", "0001"))
	live := testMetadata("watched", "0003")
	require.NoError(t, hub.Dispatch(live))
	ev = readMonitorJSON(t, conn)
	decodedStringEquals(t, ev, "variant", "message-stored")
	assertMonitorHeader(t, monitorEventHeader(t, ev), live)

	require.NoError(t, hub.Delete("other", "9999"))
	require.NoError(t, hub.Delete("watched", "0003"))
	ev = readMonitorJSON(t, conn)
	decodedStringEquals(t, ev, "variant", "message-deleted")
	decodedStringEquals(t, ev, "identifier/mailbox", "watched")
	decodedStringEquals(t, ev, "identifier/id", "0003")
}

func TestMonitorMailboxAddressV2(t *testing.T) {
	hub := startMsgHub(t)
	server, gaugeBase := setupMonitorServer(t, hub)

	// The {name} path segment goes through Manager.MailboxForAddress; with the test stub's
	// full-address naming policy, user@example.com maps to a mailbox of the same name.
	stored := testMetadata("user@example.com", "0001")
	require.NoError(t, hub.Dispatch(stored))
	conn := dialMonitor(t, server, "/api/v2/monitor/messages/user%40example.com", gaugeBase)
	ev := readMonitorJSON(t, conn)
	decodedStringEquals(t, ev, "variant", "message-stored")
	assertMonitorHeader(t, monitorEventHeader(t, ev), stored)
}

func TestMonitorMailboxInvalidNameV2(t *testing.T) {
	hub := startMsgHub(t)
	setupWebServer(test.NewManager(), hub)

	// Invalid mailbox names are rejected before the connection is upgraded.
	w, err := testRestGet("http://localhost/api/v2/monitor/messages/foo%20bar")
	require.NoError(t, err)
	assert.Equal(t, 500, w.Code)
}

func TestMonitorUpgradeFailedV2(t *testing.T) {
	hub := startMsgHub(t)
	setupWebServer(test.NewManager(), hub)

	// A plain GET without WebSocket upgrade headers fails the handshake; the upgrader writes
	// the 400 itself before the handler error is reported.
	w, err := testRestGet("http://localhost/api/v2/monitor/messages")
	require.NoError(t, err)
	assert.Equal(t, 400, w.Code)
}

func TestMonitorRejectedAfterHubStopV2(t *testing.T) {
	// When the hub has stopped (server shutdown) before the upgrade completes, AddListener
	// returns ErrClosed; the handler must tear the fresh connection down instead of leaving
	// it open with an unregistered listener that waits for events forever.
	hub := msghub.New(10, extension.NewHost())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go hub.Start(ctx)

	server, gaugeBase := setupMonitorServer(t, hub)

	cancel()
	select {
	case <-hub.Done():
	case <-time.After(monitorTestTimeout):
		t.Fatal("hub did not stop after context cancellation")
	}

	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v2/monitor/messages"
	conn, resp, err := monitorDialer.Dial(url, nil)
	require.NoError(t, err)
	// resp.Body is an empty no-op closer once the connection is upgraded.
	_ = resp.Body.Close()
	t.Cleanup(func() { _ = conn.Close() })

	// The server-side handler returns after the refused registration and closes the
	// hijacked connection, so the next read must fail and the gauge must drop back.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(monitorTestTimeout)))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("expected connection to close after hub rejected the listener registration")
	}
	require.Eventually(t, func() bool {
		return web.ExpWebSocketConnectsCurrent.Value() == gaugeBase
	}, monitorTestTimeout, time.Millisecond, "server handler did not shut down")
}

// assertNoMoreEvents fails the test if any further events are queued on the channel.
func assertNoMoreEvents[T any](t *testing.T, c chan T) {
	t.Helper()
	select {
	case ev := <-c:
		t.Fatalf("unexpected event: %v", ev)
	default:
	}
}

func TestMsgListenerWatches(t *testing.T) {
	testCases := []struct {
		name    string
		mailbox string // listener watch; "" watches all mailboxes
		event   string // mailbox of the incoming event
		want    bool
	}{
		{name: "all watcher matches any mailbox", mailbox: "", event: "anything", want: true},
		{name: "watched mailbox matches", mailbox: "watched", event: "watched", want: true},
		{name: "other mailbox filtered", mailbox: "watched", event: "other", want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ml := &msgListener[string]{mailbox: tc.mailbox}
			assert.Equal(t, tc.want, ml.watches(tc.event))
		})
	}
}

func TestMsgListenerReceiveFiltersAndShapes(t *testing.T) {
	hub := startMsgHub(t)
	ml, err := newMsgListener(hub, "watched",
		func(msg event.MessageMetadata) string { return "stored:" + msg.Mailbox + "/" + msg.ID },
		func(mailbox, id string) string { return "deleted:" + mailbox + "/" + id })
	require.NoError(t, err)

	require.NoError(t, ml.Receive(testMetadata("other", "0001")))
	require.NoError(t, ml.Receive(testMetadata("watched", "0002")))
	require.NoError(t, ml.Delete("other", "0003"))
	require.NoError(t, ml.Delete("watched", "0004"))

	// Events for other mailboxes are filtered; payloads are shaped by the transforms.
	assert.Equal(t, "stored:watched/0002", <-ml.c)
	assert.Equal(t, "deleted:watched/0004", <-ml.c)
	assertNoMoreEvents(t, ml.c)
}

func TestMsgListenerNilDeletedDropsDeletes(t *testing.T) {
	hub := startMsgHub(t)
	ml, err := newMsgListener(hub, "watched",
		func(msg event.MessageMetadata) string { return "stored:" + msg.ID },
		nil) // socketv1 API ignores deletions
	require.NoError(t, err)

	require.NoError(t, ml.Receive(testMetadata("watched", "0001")))
	require.NoError(t, ml.Delete("watched", "0001"))

	assert.Equal(t, "stored:0001", <-ml.c)
	assertNoMoreEvents(t, ml.c)
}

func TestMsgListenerCloseIdempotent(t *testing.T) {
	hub := startMsgHub(t)
	ml, err := newMsgListener(hub, "watched",
		func(msg event.MessageMetadata) string { return "stored" },
		func(mailbox, id string) string { return "deleted" })
	require.NoError(t, err)

	ml.Close()
	ml.Close() // second close must not panic or double-close the done channel

	// Close is exactly-once: the done channel ends delivery and the write pump.
	select {
	case <-ml.done:
	default:
		t.Fatal("done channel should be closed after Close")
	}

	// Closed listeners either deliver into the never-again-read queue or report closed;
	// both outcomes are terminal, as the hub-actor send races the done channel.
	err = ml.Receive(testMetadata("watched", "0001"))
	if err != nil {
		require.ErrorIs(t, err, errListenerClosed)
	}
	err = ml.Delete("watched", "0001")
	if err != nil {
		require.ErrorIs(t, err, errListenerClosed)
	}
}

func TestMsgListenerCloseFullQueue(t *testing.T) {
	// A disconnecting client may leave a full event queue behind; Close must still
	// deregister the listener, or the hub actor blocks on its next Receive into the full
	// queue forever, wedging all monitor dispatch.  Regression: the old check-then-act
	// select mistook the buffered events for a closed-channel signal and skipped
	// RemoveListener entirely.
	hub := startMsgHub(t)
	ml, err := newMsgListener(hub, "watched",
		func(msg event.MessageMetadata) string { return "stored:" + msg.ID }, nil)
	require.NoError(t, err)

	// Fill the event queue (capacity 100) with no reader draining it.
	for i := range 100 {
		require.NoError(t, hub.Dispatch(testMetadata("watched", strconv.Itoa(i))))
	}
	require.NoError(t, hub.Sync())

	ml.Close()
	require.NoError(t, hub.Sync())
	// The backlog remains queued, but the listener is deregistered: dispatch must not
	// block the hub, and must not be delivered.  Pre-fix, the no-op Close left the
	// listener registered, so the dispatch was delivered (and the next dispatch would
	// have wedged the actor inside Receive).
	backlog, _ := drainEvents(ml.c)
	assert.Equal(t, 100, backlog, "backlog should remain queued after Close")

	require.NoError(t, hub.Dispatch(testMetadata("watched", "after-close")))
	require.NoError(t, hub.Sync())
	count, _ := drainEvents(ml.c)
	assert.Equal(t, 0, count, "closed listener must not receive further events")
}

// drainEvents reads buffered events from c until it is empty; it reports how many events
// were drained and whether the channel was found closed.  A receive from a closed channel
// succeeds immediately, so a plain select-default loop would never terminate on one.
func drainEvents[T any](c chan T) (count int, closed bool) {
	for {
		select {
		case _, ok := <-c:
			if !ok {
				return count, true
			}
			count++
		default:
			return count, false
		}
	}
}

// blockingListener stalls the hub actor inside Receive until released.  engaged is closed
// by the first Receive, letting tests know the actor has reached the stall point.
type blockingListener struct {
	release chan struct{}
	engaged chan struct{}
	once    sync.Once
}

func newBlockingListener() *blockingListener {
	return &blockingListener{
		release: make(chan struct{}),
		engaged: make(chan struct{}),
	}
}

func (l *blockingListener) Receive(msg event.MessageMetadata) error {
	l.once.Do(func() { close(l.engaged) })
	<-l.release
	return nil
}

func (l *blockingListener) Delete(mailbox string, id string) error {
	return nil
}

// stallAndSaturate blocks the hub actor inside blocker.Receive and then fills the hub op
// queue behind it, proving saturation via the non-blocking Dispatch returning ErrQueueFull.
func stallAndSaturate(t *testing.T, hub *msghub.Hub, blocker *blockingListener) {
	t.Helper()
	require.NoError(t, hub.Dispatch(testMetadata("watched", "stall")))
	select {
	case <-blocker.engaged:
	case <-time.After(monitorTestTimeout):
		t.Fatal("blocking listener never engaged")
	}
	require.Eventually(t, func() bool {
		return errors.Is(hub.Dispatch(testMetadata("watched", "fill")), msghub.ErrQueueFull)
	}, monitorTestTimeout, time.Microsecond, "hub op queue should saturate")
}

func TestMsgListenerCloseConcurrent(t *testing.T) {
	// The read and write pumps both call Close when the connection drops, so Close must be
	// safe to call concurrently.  Regression: the check-then-act select let both callers
	// through the default branch and panicked with "close of closed channel" on the second
	// close.
	//
	// Deterministic reproduction: a blocking listener stalls the hub actor mid-dispatch,
	// and the hub op queue (capacity 100) is saturated behind it.  Every concurrent Close
	// then contends on closeOnce / the guaranteed removal enqueue before the actor can
	// resume.  Releasing the actor lets it drain its queue and process the removal,
	// resuming all the blocked closers.
	hub := startMsgHub(t)
	blocker := newBlockingListener()
	require.NoError(t, hub.AddListener(blocker))
	require.NoError(t, hub.Sync())
	ml, err := newMsgListener(hub, "watched",
		func(msg event.MessageMetadata) string { return "stored:" + msg.ID },
		func(mailbox, id string) string { return "deleted:" + id })
	require.NoError(t, err)
	require.NoError(t, hub.Sync())

	stallAndSaturate(t, hub, blocker)

	// The stalled dispatch may have delivered 0 or 1 events to ml (listener iteration
	// order is random); drain whatever arrived so the queue is empty when the closers
	// check it.
	drainEvents(ml.c)

	const callers = 8
	done := make(chan struct{}, callers) // ensure capacity so we do not block population
	for range callers {
		go func() {
			ml.Close()
			done <- struct{}{}
		}()
	}
	// Give every closer time to block inside closeOnce/RemoveListener before the actor
	// resumes; otherwise their ops interleave with the queued dispatches and the
	// reproduction becomes timing-dependent.
	time.Sleep(50 * time.Millisecond)

	close(blocker.release)
	for range callers {
		<-done
	}

	// Events racing in before the hub processed the removal may sit in the queue, but
	// once it has (Sync), the listener must be deregistered: later dispatches are
	// neither delivered to it nor blocking the hub.
	require.NoError(t, hub.Sync())
	drainEvents(ml.c)
	require.NoError(t, hub.Dispatch(testMetadata("watched", "after-close")))
	require.NoError(t, hub.Sync())
	count, _ := drainEvents(ml.c)
	assert.Equal(t, 0, count, "closed listener must not receive further events")
}

func TestMsgListenerCloseUnblocksHub(t *testing.T) {
	// Close must unblock a hub actor stuck sending into the listener's full event queue:
	// a slow monitor client disconnecting with a backlog of events would otherwise wedge
	// every hub dispatch.  (With the original close(ml.c) approach, this
	// send-on-closed-channel panicked inside the actor instead.)
	hub := startMsgHub(t)
	blocker := newBlockingListener()
	require.NoError(t, hub.AddListener(blocker))
	require.NoError(t, hub.Sync())
	ml, err := newMsgListener(hub, "watched",
		func(msg event.MessageMetadata) string { return "stored:" + msg.ID }, nil)
	require.NoError(t, err)
	require.NoError(t, hub.Sync())

	// One dispatch stalls the actor; 100 fill the hub op queue: 101 watched events in
	// flight, one more than the listener's event-queue capacity of 100.
	stallAndSaturate(t, hub, blocker)
	drainEvents(ml.c) // drop the 0 or 1 events delivered before the actor stalled
	close(blocker.release)

	// As the actor drains the queued dispatches, the event queue fills and the next
	// delivery wedges the actor inside Receive (done is still open).
	require.Eventually(t, func() bool {
		return len(ml.c) == socketChanLen
	}, monitorTestTimeout, time.Millisecond, "event queue should fill and wedge the hub actor")

	ml.Close()

	// The blocked send must abort and the hub must keep processing.
	synced := make(chan error, 1)
	go func() {
		synced <- hub.Sync()
	}()
	select {
	case err := <-synced:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("hub actor still wedged after Close")
	}

	// The listener is deregistered: later dispatches are not delivered to it.
	drainEvents(ml.c)
	require.NoError(t, hub.Dispatch(testMetadata("watched", "after-close")))
	require.NoError(t, hub.Sync())
	count, _ := drainEvents(ml.c)
	assert.Equal(t, 0, count, "closed listener must not receive further events")
}

func TestMsgListenerCloseFullHubOpQueue(t *testing.T) {
	// Regression: with the hub actor blocked inside this listener's Receive (full event
	// queue) and the hub op queue (capacity 100) also full, the removal must still be
	// enqueued.  Close closes done before queueing the removal and RemoveListener waits
	// for a freed slot via the hub's condition variable, so Close returns even while
	// concurrent dispatchers keep the hub queue saturated.
	hub := startMsgHub(t)
	blocker := newBlockingListener()
	require.NoError(t, hub.AddListener(blocker))
	require.NoError(t, hub.Sync())
	ml, err := newMsgListener(hub, "watched",
		func(msg event.MessageMetadata) string { return "stored:" + msg.ID }, nil)
	require.NoError(t, err)
	require.NoError(t, hub.Sync())

	// Stall the actor inside the blocker, then saturate the hub op queue behind it.
	stallAndSaturate(t, hub, blocker)
	drainEvents(ml.c)

	// Once the blocker is released, a dispatcher goroutine keeps the op queue saturated:
	// Dispatch is non-blocking, so it retries on ErrQueueFull until each event is
	// enqueued.  150 events exceed the ~102 slots freed before the actor wedges inside
	// the listener's full event queue.
	dispatched := make(chan struct{})
	go func() {
		defer close(dispatched)
		for i := range 150 {
			for {
				if err := hub.Dispatch(testMetadata("watched", strconv.Itoa(100+i))); err == nil {
					break
				}
				// ErrQueueFull: yield and retry; the hub stays alive throughout the test.
				runtime.Gosched()
			}
		}
	}()
	close(blocker.release)

	// Wait for the wedge: the event queue is full and nothing drains it.  The sleep lets
	// the dispatcher refill freed slots and keep the op queue saturated.
	require.Eventually(t, func() bool {
		return len(ml.c) == socketChanLen
	}, monitorTestTimeout, time.Millisecond, "event queue should fill and wedge the hub actor")
	time.Sleep(50 * time.Millisecond)

	// Close must return despite the wedged actor and the saturated op queue.
	closed := make(chan struct{})
	go func() {
		ml.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked: hub actor blocked in Receive with a full op queue")
	}

	// The released actor must drain the saturated op queue: the dispatcher completes and
	// the hub answers Sync again.
	select {
	case <-dispatched:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatches still blocked after Close")
	}
	synced := make(chan error, 1)
	go func() {
		synced <- hub.Sync()
	}()
	select {
	case err := <-synced:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("hub actor still wedged after Close")
	}

	// The listener is deregistered: later dispatches are not delivered to it.
	drainEvents(ml.c)
	require.NoError(t, hub.Dispatch(testMetadata("watched", "after-close")))
	require.NoError(t, hub.Sync())
	count, _ := drainEvents(ml.c)
	assert.Equal(t, 0, count, "closed listener must not receive further events")
}

func TestMsgListenerCloseAfterHubStop(t *testing.T) {
	// A monitor session outliving hub shutdown (hijacked connections are not closed by
	// http.Server.Shutdown) must still be able to Close: the removal is rejected with
	// ErrClosed rather than panicking on a closed channel or blocking forever.
	hub := msghub.New(10, extension.NewHost())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go hub.Start(ctx)

	ml, err := newMsgListener(hub, "watched",
		func(msg event.MessageMetadata) string { return "stored:" + msg.ID },
		func(mailbox, id string) string { return "deleted:" + id })
	require.NoError(t, err)

	cancel()
	select {
	case <-hub.Done():
	case <-time.After(monitorTestTimeout):
		t.Fatal("hub did not stop after context cancellation")
	}

	closed := make(chan struct{})
	go func() {
		ml.Close() // must not panic or block
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(monitorTestTimeout):
		t.Fatal("Close blocked after hub shutdown")
	}

	// Hub operations are observably rejected after shutdown.
	assert.ErrorIs(t, hub.Dispatch(testMetadata("watched", "late")), msghub.ErrClosed)
}
