package mem

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"

	"github.com/inbucket/inbucket/v3/pkg/config"
	"github.com/inbucket/inbucket/v3/pkg/extension"
	"github.com/inbucket/inbucket/v3/pkg/message"
	"github.com/inbucket/inbucket/v3/pkg/storage"
)

// Store implements an in-memory message store.
type Store struct {
	sync.Mutex
	boxes     map[string]*mbox
	cap       int           // Per-mailbox message cap.
	incoming  chan *msgDone // New messages for size enforcer.
	remove    chan *msgDone // Remove deleted messages from size enforcer.
	extHost   *extension.Host
	keyRegMu  sync.Mutex
	keyLocks  map[string]*sync.Mutex
}

type mbox struct {
	sync.RWMutex
	name     string
	last     int
	first    int
	messages map[string]*Message
	attempts map[string]string // Delivery attempt key -> message ID.
}

var _ storage.Store = &Store{}

// New returns an empty memory store.
func New(cfg config.Storage, extHost *extension.Host) (storage.Store, error) {
	s := &Store{
		boxes:    make(map[string]*mbox),
		cap:      cfg.MailboxMsgCap,
		extHost:  extHost,
		keyLocks: make(map[string]*sync.Mutex),
	}
	if str, ok := cfg.Params["maxkb"]; ok {
		maxKB, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse maxkb: %v", err)
		}
		if maxKB > 0 {
			// Setup enforcer.
			s.incoming = make(chan *msgDone)
			s.remove = make(chan *msgDone)
			go s.maxSizeEnforcer(maxKB * 1024)
		}
	}
	return s, nil
}

// AddMessage stores the message, message ID and Size will be ignored.
func (s *Store) AddMessage(message storage.Message) (id string, err error) {
	source, err := readSource(message)
	if err != nil {
		return "", err
	}
	m := &Message{
		mailbox: message.Mailbox(),
		from:    message.From(),
		to:      message.To(),
		date:    message.Date(),
		subject: message.Subject(),
		source:  source,
	}
	s.withMailbox(message.Mailbox(), true, func(mb *mbox) {
		id = s.insertLocked(mb, m)
	})
	s.enforcerDeliver(m)
	return id, err
}

// readSource reads and closes the message source.
func readSource(message storage.Message) ([]byte, error) {
	r, err := message.Source()
	if err != nil {
		return nil, err
	}
	source, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	_ = r.Close()
	return source, nil
}

// insertLocked assigns an ID, stores the message, and enforces the per-mailbox cap.  The mailbox
// write lock must be held.
func (s *Store) insertLocked(mb *mbox, m *Message) string {
	mb.last++
	m.index = mb.last
	m.id = strconv.Itoa(mb.last)
	mb.messages[m.id] = m
	if m.attempt != "" {
		mb.attempts[m.attempt] = m.id
	}
	s.enforceCapLocked(mb)
	return m.id
}

// enforceCapLocked drops the oldest messages until the per-mailbox cap is satisfied.  The mailbox
// write lock must be held.
func (s *Store) enforceCapLocked(mb *mbox) {
	if s.cap <= 0 {
		return
	}
	for len(mb.messages) > s.cap {
		old := mb.messages[strconv.Itoa(mb.first)]
		delete(mb.messages, strconv.Itoa(mb.first))
		if old != nil && old.attempt != "" {
			delete(mb.attempts, old.attempt)
		}
		mb.first++
	}
}

// GetMessage gets a mesage.
func (s *Store) GetMessage(mailbox, id string) (m storage.Message, err error) {
	if id == "latest" {
		ms, err := s.GetMessages(mailbox)
		if err != nil {
			return nil, err
		}
		count := len(ms)
		if count == 0 {
			return nil, nil
		}
		return ms[count-1], nil
	}
	s.withMailbox(mailbox, false, func(mb *mbox) {
		var ok bool
		m, ok = mb.messages[id]
		if !ok {
			m = nil
		}
	})
	return m, err
}

// GetMessages gets a list of messages.
func (s *Store) GetMessages(mailbox string) (ms []storage.Message, err error) {
	s.withMailbox(mailbox, false, func(mb *mbox) {
		ms = make([]storage.Message, 0, len(mb.messages))
		for _, v := range mb.messages {
			ms = append(ms, v)
		}
		sort.Slice(ms, func(i, j int) bool {
			return ms[i].(*Message).index < ms[j].(*Message).index
		})
	})
	return ms, err
}

// MarkSeen marks a message as having been read.
func (s *Store) MarkSeen(mailbox, id string) error {
	s.withMailbox(mailbox, true, func(mb *mbox) {
		m := mb.messages[id]
		if m != nil {
			m.seen = true
		}
	})
	return nil
}

// PurgeMessages deletes the contents of the specified mailbox.
func (s *Store) PurgeMessages(mailbox string) error {
	// Grab lock, copy messages, clear, and drop lock.
	var messages map[string]*Message
	s.withMailbox(mailbox, true, func(mb *mbox) {
		messages = mb.messages
		mb.messages = make(map[string]*Message)
		mb.attempts = make(map[string]string)
	})

	// Process size/quota.
	if s.remove != nil {
		for _, m := range messages {
			s.enforcerRemove(m)
		}
	}

	// Emit delete events.
	for _, m := range messages {
		s.extHost.Events.AfterMessageDeleted.Emit(message.MakeMetadata(m))
	}

	return nil
}

// removeMessage deletes a single message without notifying the size enforcer.  Returns the message
// that was removed.
func (s *Store) removeMessage(mailbox, id string) *Message {
	var m *Message
	s.withMailbox(mailbox, true, func(mb *mbox) {
		m = mb.messages[id]
		if m != nil {
			delete(mb.messages, id)
			if m.attempt != "" && mb.attempts[m.attempt] == id {
				delete(mb.attempts, m.attempt)
			}
		}
	})

	if m != nil {
		s.extHost.Events.AfterMessageDeleted.Emit(message.MakeMetadata(m))
	}

	return m
}

// RemoveMessage deletes a single message.
func (s *Store) RemoveMessage(mailbox, id string) error {
	m := s.removeMessage(mailbox, id)
	if m != nil {
		s.enforcerRemove(m)
	}
	return nil
}

// VisitMailboxes visits each mailbox in the store.
func (s *Store) VisitMailboxes(f func([]storage.Message) (cont bool)) error {
	// Lock store, get names of all mailboxes.
	s.Lock()
	boxNames := make([]string, 0, len(s.boxes))
	for k := range s.boxes {
		boxNames = append(boxNames, k)
	}
	s.Unlock()
	// Process mailboxes.
	for _, mailbox := range boxNames {
		ms, _ := s.GetMessages(mailbox)
		if !f(ms) {
			break
		}
	}
	return nil
}

// withMailbox gets or creates a mailbox, locks it, then calls f.
func (s *Store) withMailbox(mailbox string, writeLock bool, f func(mb *mbox)) {
	s.Lock()
	mb, ok := s.boxes[mailbox]
	if !ok {
		// Create mailbox
		mb = &mbox{
			name:     mailbox,
			messages: make(map[string]*Message),
			attempts: make(map[string]string),
		}
		s.boxes[mailbox] = mb
	}
	s.Unlock()
	if writeLock {
		mb.Lock()
	} else {
		mb.RLock()
	}
	defer func() {
		if writeLock {
			mb.Unlock()
		} else {
			mb.RUnlock()
		}
	}()
	f(mb)
}
