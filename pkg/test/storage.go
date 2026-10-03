package test

import (
	"errors"
	"sync"

	"github.com/inbucket/inbucket/v3/pkg/storage"
)

// StoreStub stubs storage.Store for testing.
type StoreStub struct {
	storage.Store
	mu        sync.Mutex
	mailboxes map[string][]storage.Message // Stored messages, by mailbox.
	deleted   map[storage.Message]struct{} // Deleted message references.
	attempts  map[string]map[string]string // Delivery attempt key -> mailbox -> message ID.
	// FailAdd, when set and non-nil, forces attempt and AddMessage delivery to fail for a mailbox.
	FailAdd func(mailbox string) error
}

// NewStore creates a new StoreStub.
func NewStore() *StoreStub {
	return &StoreStub{
		mailboxes: make(map[string][]storage.Message),
		deleted:   make(map[storage.Message]struct{}),
		attempts:  make(map[string]map[string]string),
	}
}

// AddMessage adds a message to the specified mailbox.
func (s *StoreStub) AddMessage(m storage.Message) (id string, err error) {
	if s.FailAdd != nil {
		if err := s.FailAdd(m.Mailbox()); err != nil {
			return "", err
		}
	}
	stub := &MessageStub{Message: m}
	s.mu.Lock()
	mb := m.Mailbox()
	s.mailboxes[mb] = append(s.mailboxes[mb], stub)
	s.mu.Unlock()
	return m.ID(), nil
}

// BeginDelivery starts an atomic multi-mailbox delivery on the stub.
func (s *StoreStub) BeginDelivery(key string) storage.DeliveryAttempt {
	return &stubAttempt{s: s, key: key}
}

var _ storage.AttemptStore = (*StoreStub)(nil)

type stubTarget struct {
	mailbox  string
	msg      *MessageStub // nil when replaying an earlier attempt.
	replayID string
}

type stubAttempt struct {
	s       *StoreStub
	key     string
	targets []*stubTarget
	closed  bool
}

// Add stages a message; staged messages stay out of mailbox slices until Commit.
func (a *stubAttempt) Add(m storage.Message) (string, error) {
	if a.s.FailAdd != nil {
		if err := a.s.FailAdd(m.Mailbox()); err != nil {
			return "", err
		}
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()

	if byBox, ok := a.s.attempts[a.key]; ok {
		if id, replays := byBox[m.Mailbox()]; replays {
			a.targets = append(a.targets, &stubTarget{mailbox: m.Mailbox(), replayID: id})
			return id, nil
		}
	}

	stub := &MessageStub{Message: m, attempt: a.key}
	a.targets = append(a.targets, &stubTarget{mailbox: m.Mailbox(), msg: stub})
	return m.ID(), nil
}

// Commit publishes all staged messages in one locked step.
func (a *stubAttempt) Commit() ([]storage.DeliveryReceipt, error) {
	if a.closed {
		return nil, errors.New("delivery attempt already committed or discarded")
	}
	a.closed = true

	a.s.mu.Lock()
	defer a.s.mu.Unlock()

	byBox := a.s.attempts[a.key]
	if byBox == nil {
		byBox = make(map[string]string)
		a.s.attempts[a.key] = byBox
	}
	receipts := make([]storage.DeliveryReceipt, 0, len(a.targets))
	for _, t := range a.targets {
		r := storage.DeliveryReceipt{Mailbox: t.mailbox}
		if t.msg == nil {
			r.ID = t.replayID
			r.Replayed = true
			receipts = append(receipts, r)
			continue
		}
		a.s.mailboxes[t.mailbox] = append(a.s.mailboxes[t.mailbox], t.msg)
		byBox[t.mailbox] = t.msg.ID()
		r.ID = t.msg.ID()
		receipts = append(receipts, r)
	}
	return receipts, nil
}

// Discard abandons the attempt; staged messages were never visible.
func (a *stubAttempt) Discard() {
	a.closed = true
}

// GetMessage gets a message by ID from the specified mailbox.
func (s *StoreStub) GetMessage(mailbox, id string) (storage.Message, error) {
	if mailbox == "messageerr" {
		return nil, errors.New("internal error")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.mailboxes[mailbox] {
		if m.ID() == id {
			return m, nil
		}
	}
	return nil, storage.ErrNotExist
}

// GetMessages gets all the messages for the specified mailbox.
func (s *StoreStub) GetMessages(mailbox string) ([]storage.Message, error) {
	if mailbox == "messageserr" {
		return nil, errors.New("internal error")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mailboxes[mailbox], nil
}

// MarkSeen marks the message as having been seen.
func (s *StoreStub) MarkSeen(mailbox, id string) error {
	if mailbox == "messageerr" {
		return errors.New("internal error")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.mailboxes[mailbox] {
		if m.ID() == id {
			if stub, ok := m.(*MessageStub); ok {
				stub.seen = true
				return nil
			}
			return errors.New("unexpected type in StoreStub.mailboxes")
		}
	}
	return storage.ErrNotExist
}

// RemoveMessage deletes a message by ID from the specified mailbox.
func (s *StoreStub) RemoveMessage(mailbox, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if mb, ok := s.mailboxes[mailbox]; ok {
		var removed storage.Message
		for i, m := range mb {
			if m.ID() == id {
				removed = m
				s.mailboxes[mailbox] = append(mb[:i], mb[i+1:]...)
				break
			}
		}

		if removed != nil {
			// Clients will be checking for their original storage.Message, not our wrapper.
			if stub, ok := removed.(*MessageStub); ok {
				s.deleted[stub.Message] = struct{}{}
				if stub.attempt != "" {
					if byBox := s.attempts[stub.attempt]; byBox != nil {
						delete(byBox, mailbox)
					}
				}
				return nil
			}
			return errors.New("unexpected type in StoreStub.mailboxes")
		}
	}

	return storage.ErrNotExist
}

// PurgeMessages deletes the contents of a mailbox.
func (s *StoreStub) PurgeMessages(mailbox string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, removed := range s.mailboxes[mailbox] {
		// Clients will be checking for their original storage.Message, not our wrapper.
		if stub, ok := removed.(*MessageStub); ok {
			s.deleted[stub.Message] = struct{}{}
		} else {
			return errors.New("unexpected type in StoreStub.mailboxes")
		}
	}

	s.mailboxes[mailbox] = nil
	return nil
}

// VisitMailboxes accepts a function that will be called with the messages in each mailbox while it
// continues to return true.
func (s *StoreStub) VisitMailboxes(f func([]storage.Message) (cont bool)) error {
	s.mu.Lock()
	names := make([]string, 0, len(s.mailboxes))
	for name := range s.mailboxes {
		names = append(names, name)
	}
	snapshot := make(map[string][]storage.Message, len(names))
	for _, name := range names {
		snapshot[name] = append([]storage.Message(nil), s.mailboxes[name]...)
	}
	s.mu.Unlock()
	for _, name := range names {
		if !f(snapshot[name]) {
			return nil
		}
	}
	return nil
}

// MessageDeleted returns true if the specified message was deleted
func (s *StoreStub) MessageDeleted(m storage.Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.deleted[m]
	return ok
}

// MessageStub wraps a storage.Message with "seen" functionality.
type MessageStub struct {
	storage.Message
	seen    bool
	attempt string
}

// Seen returns true if the message has been marked as seen previously.
func (m *MessageStub) Seen() bool {
	return m.seen
}
