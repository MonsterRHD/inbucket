package mem

import (
	"errors"
	"sort"
	"strconv"
	"sync"

	"github.com/inbucket/inbucket/v3/pkg/storage"
)

// errAttemptClosed is returned when an attempt is committed or discarded more than once.
var errAttemptClosed = errors.New("delivery attempt already committed or discarded")

// errAttemptConflict is returned when a replayed delivery target vanished before commit; the
// caller is expected to retry.
var errAttemptConflict = errors.New("delivery attempt target conflict, retry required")

var _ storage.AttemptStore = (*Store)(nil)

// keyLock serializes delivery attempts sharing an idempotency key, so a retry can never race the
// original attempt.
func (s *Store) keyLock(key string) *sync.Mutex {
	s.keyRegMu.Lock()
	mu, ok := s.keyLocks[key]
	if !ok {
		mu = &sync.Mutex{}
		s.keyLocks[key] = mu
	}
	s.keyRegMu.Unlock()
	return mu
}

// BeginDelivery starts an atomic, idempotent multi-mailbox delivery attempt.  It blocks while an
// earlier attempt with the same key is still in progress.  Staged messages remain invisible to
// readers until Commit publishes all of them.
func (s *Store) BeginDelivery(key string) storage.DeliveryAttempt {
	mu := s.keyLock(key)
	mu.Lock()
	return &delivery{
		store:   s,
		key:     key,
		keyMu:   mu,
	}
}

// delivery is the in-memory implementation of storage.DeliveryAttempt.
type delivery struct {
	store   *Store
	key     string
	keyMu   *sync.Mutex
	targets []*stagedTarget
	mu      sync.Mutex
	closed  bool
}

// stagedTarget tracks one target mailbox of a delivery.
type stagedTarget struct {
	name     string
	mb       *mbox
	msg      *Message // nil when this target is replaying an earlier attempt.
	replayed bool
	replayID string
}

// Add reserves the message ID for a target mailbox.  The message content is captured immediately,
// but it stays invisible (outside of mbox.messages) until Commit.  If an earlier attempt with the
// same key already reached this mailbox, its committed ID is reported as a replay.
func (d *delivery) Add(message storage.Message) (id string, err error) {
	source, err := readSource(message)
	if err != nil {
		return "", err
	}

	t := &stagedTarget{name: message.Mailbox()}
	d.store.withMailbox(t.name, true, func(mb *mbox) {
		t.mb = mb
		// Same-key attempts are serialized; an entry here is a committed prior attempt.
		if existing, ok := mb.attempts[d.key]; ok {
			t.replayed = true
			t.replayID = existing
			id = existing
			return
		}
		// Reserve the next ID for this attempt without making the message visible.
		mb.last++
		m := &Message{
			index:   mb.last,
			mailbox: t.name,
			id:      strconv.Itoa(mb.last),
			from:    message.From(),
			to:      message.To(),
			date:    message.Date(),
			subject: message.Subject(),
			source:  source,
			attempt: d.key,
		}
		mb.attempts[d.key] = m.id
		t.msg = m
		id = m.id
	})

	d.mu.Lock()
	d.targets = append(d.targets, t)
	d.mu.Unlock()
	return id, nil
}

// Commit publishes every staged message in a single atomic visibility step.
func (d *delivery) Commit() ([]storage.DeliveryReceipt, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, errAttemptClosed
	}
	d.closed = true
	targets := d.targets
	d.mu.Unlock()
	defer d.keyMu.Unlock()

	// A target mailbox may appear once at most; lock every affected mailbox in name order so
	// concurrent attempts touching overlapping mailboxes cannot deadlock, and readers cannot
	// observe a partial publish.
	seen := make(map[*mbox]struct{}, len(targets))
	ordered := make([]*stagedTarget, len(targets))
	copy(ordered, targets)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].name < ordered[j].name
	})
	lockOrder := make([]*mbox, 0, len(targets))
	for _, t := range ordered {
		if _, ok := seen[t.mb]; ok {
			continue
		}
		t.mb.Lock()
		seen[t.mb] = struct{}{}
		lockOrder = append(lockOrder, t.mb)
	}
	unlock := func() {
		for i := len(lockOrder) - 1; i >= 0; i-- {
			lockOrder[i].Unlock()
		}
	}

	inserted := make([]*stagedTarget, 0, len(targets))
	var commitErr error
	for _, t := range targets {
		if t.replayed {
			if _, ok := t.mb.messages[t.replayID]; !ok {
				// Defensive: key serialization makes this unreachable.
				commitErr = errAttemptConflict
				break
			}
			continue
		}
		if rid, ok := t.mb.attempts[d.key]; !ok || rid != t.msg.id {
			// Defensive: key serialization makes this unreachable.
			commitErr = errAttemptConflict
			break
		}
		t.mb.messages[t.msg.id] = t.msg
		d.store.enforceCapLocked(t.mb)
		inserted = append(inserted, t)
	}
	if commitErr != nil {
		// Roll back exactly the messages and reservations this attempt created, still holding
		// all locks.
		for _, t := range targets {
			if t.replayed || t.msg == nil {
				continue
			}
			delete(t.mb.messages, t.msg.id)
			if t.mb.attempts[d.key] == t.msg.id {
				delete(t.mb.attempts, d.key)
			}
		}
		unlock()
		return nil, commitErr
	}
	unlock()

	// Notify the size enforcer only after the batch is visible and locks are released; its
	// evictions take mailbox locks themselves.
	for _, t := range inserted {
		d.store.enforcerDeliver(t.msg)
	}

	receipts := make([]storage.DeliveryReceipt, len(targets))
	for i, t := range targets {
		r := storage.DeliveryReceipt{Mailbox: t.name}
		if t.replayed {
			r.ID = t.replayID
			r.Replayed = true
		} else {
			r.ID = t.msg.id
		}
		receipts[i] = r
	}
	return receipts, nil
}

// Discard abandons the attempt, releasing only the ID reservations it made.  Staged messages were
// never visible, so no mailbox content is removed.
func (d *delivery) Discard() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	targets := d.targets
	d.mu.Unlock()
	defer d.keyMu.Unlock()

	for _, t := range targets {
		if t.replayed || t.msg == nil {
			continue
		}
		d.store.withMailbox(t.name, true, func(mb *mbox) {
			if mb.attempts[d.key] == t.msg.id {
				delete(mb.attempts, d.key)
			}
		})
	}
}
