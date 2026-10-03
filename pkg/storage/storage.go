// Package storage contains implementation independent datastore logic
package storage

import (
	"errors"
	"fmt"
	"io"
	"net/mail"
	"time"

	"github.com/inbucket/inbucket/v3/pkg/config"
	"github.com/inbucket/inbucket/v3/pkg/extension"
)

var (
	// ErrNotExist indicates the requested message does not exist.
	ErrNotExist = errors.New("message does not exist")

	// ErrNotWritable indicates the message is closed; no longer writable
	ErrNotWritable = errors.New("Message not writable")

	// Constructors tracks registered storage constructors
	Constructors = make(map[string]func(config.Storage, *extension.Host) (Store, error))
)

// Store is the interface Inbucket uses to interact with storage implementations.
type Store interface {
	// AddMessage stores the message, message ID and Size will be ignored.
	AddMessage(message Message) (id string, err error)
	GetMessage(mailbox, id string) (Message, error)
	GetMessages(mailbox string) ([]Message, error)
	MarkSeen(mailbox, id string) error
	PurgeMessages(mailbox string) error
	RemoveMessage(mailbox, id string) error
	VisitMailboxes(f func([]Message) (cont bool)) error
}

// DeliveryReceipt reports the outcome for a single target of a delivery attempt.
type DeliveryReceipt struct {
	// Mailbox the message was committed to.
	Mailbox string
	// ID assigned to the committed message.
	ID string
	// Replayed is true when an earlier attempt with the same key already committed a message
	// to this mailbox; no new message was stored.
	Replayed bool
}

// DeliveryAttempt represents one message delivery to one or more mailboxes that becomes visible
// atomically.  Staged messages stay invisible to GetMessage, GetMessages, POP3 and REST until
// Commit succeeds.  A failed attempt must be abandoned with Discard, which only removes content
// staged by this attempt and never pre-existing mailbox content.
//
// Commit is idempotent with respect to the attempt key: when a previous attempt with the same key
// already committed one or more targets, those targets are reported as replayed instead of
// producing duplicate messages.
type DeliveryAttempt interface {
	// Add stages a message for a single target mailbox; its assigned ID is returned.  The same
	// mailbox must not be added to an attempt more than once.
	Add(message Message) (id string, err error)
	// Commit publishes every staged message as a single delivery.  Receipts are returned in the
	// same order the messages were added.
	Commit() ([]DeliveryReceipt, error)
	// Discard abandons the attempt; staged content that never became visible is removed.
	Discard()
}

// AttemptStore is implemented by Stores capable of atomic, idempotent multi-mailbox delivery
// attempts.  Stores that do not implement it fall back to per-message delivery.
type AttemptStore interface {
	// BeginDelivery starts a delivery attempt identified by key.  The key identifies retries of
	// the same message; identical keys must never produce duplicate mailbox content.
	BeginDelivery(key string) DeliveryAttempt
}

// Message represents a message to be stored, or returned from a storage implementation.
type Message interface {
	Mailbox() string
	ID() string
	From() *mail.Address
	To() []*mail.Address
	Date() time.Time
	Subject() string
	Source() (io.ReadCloser, error)
	Size() int64
	Seen() bool
}

// FromConfig creates an instance of the Store based on the provided configuration.
func FromConfig(c config.Storage, extHost *extension.Host) (store Store, err error) {
	if cf := Constructors[c.Type]; cf != nil {
		return cf(c, extHost)
	}
	return nil, fmt.Errorf("unknown storage type configured: %q", c.Type)
}
