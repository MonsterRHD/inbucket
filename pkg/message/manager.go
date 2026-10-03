package message

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/mail"
	"sort"
	"strings"
	"time"

	"github.com/inbucket/inbucket/v3/pkg/extension"
	"github.com/inbucket/inbucket/v3/pkg/extension/event"
	"github.com/inbucket/inbucket/v3/pkg/policy"
	"github.com/inbucket/inbucket/v3/pkg/storage"
	"github.com/jhillyerd/enmime/v2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// recvdTimeFmt to use in generated Received header.
const recvdTimeFmt = "Mon, 02 Jan 2006 15:04:05 -0700 (MST)"

// Manager is the interface controllers use to interact with messages.
type Manager interface {
	Deliver(
		from *policy.Origin,
		recipients []*policy.Recipient,
		recvdHeader string,
		content []byte,
	) error
	GetMetadata(mailbox string) ([]*event.MessageMetadata, error)
	GetMessage(mailbox, id string) (*Message, error)
	MarkSeen(mailbox, id string) error
	PurgeMessages(mailbox string) error
	RemoveMessage(mailbox, id string) error
	SourceReader(mailbox, id string) (io.ReadCloser, error)
	MailboxForAddress(address string) (string, error)
}

// StoreManager is a message Manager backed by the storage.Store.
type StoreManager struct {
	AddrPolicy *policy.Addressing
	Store      storage.Store
	ExtHost    *extension.Host
}

// Deliver submits a new message to the store.
//
// The target set is resolved once, after address policy and the BeforeMessageStored extension,
// with duplicate mailboxes collapsed to a single target.  All targets belong to one atomic
// delivery attempt identified by a deterministic idempotency key, so an SMTP client retrying the
// same DATA after a temporary failure cannot create duplicate messages in already committed
// mailboxes.  AfterMessageStored events are emitted only after every target commits; targets
// replayed from an earlier attempt do not emit another event.
func (s *StoreManager) Deliver(
	from *policy.Origin,
	recipients []*policy.Recipient,
	recvdHeader string,
	source []byte,
) error {
	logger := log.With().Str("module", "message").Logger()

	// Parse envelope headers.
	header, err := enmime.DecodeHeaders(source)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to parse message headers")
		return err
	}

	fromAddrs, err := enmime.ParseAddressList(header.Get("From"))
	if err != nil || len(fromAddrs) == 0 {
		// Failed to parse From header, use SMTP MAIL FROM instead.
		fromAddrs = make([]*mail.Address, 1)
		fromAddrs[0] = &from.Address
	}

	toAddrs, err := enmime.ParseAddressList(header.Get("To"))
	if err != nil {
		// Failed to parse To header, use SMTP RCPT TO instead.
		toAddrs = make([]*mail.Address, len(recipients))
		for i, torecip := range recipients {
			toAddrs[i] = &torecip.Address
		}
	}

	subject := header.Get("Subject")
	now := time.Now()
	tstamp := now.UTC().Format(recvdTimeFmt)

	// Process inbound message through extensions.
	mailboxes := make([]string, 0, len(recipients))
	for _, recip := range recipients {
		mailboxes = append(mailboxes, recip.Mailbox)
	}

	// Construct InboundMessage event and process through extensions.
	inbound := &event.InboundMessage{
		Mailboxes: mailboxes,
		From:      fromAddrs[0],
		To:        toAddrs,
		Subject:   subject,
		Size:      int64(len(source)),
	}

	extResult := s.ExtHost.Events.BeforeMessageStored.Emit(inbound)
	if extResult == nil {
		// Use address policy to determine deliverable mailboxes.
		mailboxes = mailboxes[:0]
		for _, recip := range recipients {
			if recip.ShouldStore() {
				mailboxes = append(mailboxes, recip.Mailbox)
			}
		}
		inbound.Mailboxes = mailboxes
	} else {
		// Event response overrides destination mailboxes and address policy.
		inbound = extResult
	}

	// One DATA is one delivery attempt: collapse duplicate targets, preserving first-seen order.
	targets := dedupeMailboxes(inbound.Mailboxes)

	// Nothing to store, e.g. policy discarded every recipient.
	if len(targets) == 0 {
		return nil
	}

	// Deterministic idempotency key; an SMTP retry carries the same envelope, targets and DATA.
	key := attemptKey(from.Address.Address, targets, source)

	// Build the staged delivery for each target.  Each delivery needs its own source reader.
	deliveries := make([]*Delivery, len(targets))
	for i, mb := range targets {
		logger.Debug().Str("mailbox", mb).Msg("Delivering message")
		deliveries[i] = s.buildDelivery(inbound, mb, from.Address.Address, recvdHeader, tstamp, now, source)
	}

	if ats, ok := s.Store.(storage.AttemptStore); ok {
		return s.deliverAtomic(logger, ats, key, deliveries)
	}
	return s.deliverSequential(logger, deliveries)
}

// buildDelivery constructs the staged message for one target mailbox.
func (s *StoreManager) buildDelivery(
	inbound *event.InboundMessage,
	mb string,
	fromAddr string,
	recvdHeader string,
	tstamp string,
	now time.Time,
	source []byte,
) *Delivery {
	recvd := fmt.Sprintf("%s  for <%s>; %s\r\n", recvdHeader, mb, tstamp)
	returnPath := fmt.Sprintf("Return-Path: <%s>\r\n", fromAddr)
	return &Delivery{
		Meta: event.MessageMetadata{
			Mailbox: mb,
			From:    inbound.From,
			To:      inbound.To,
			Date:    now,
			Subject: inbound.Subject,
			Size:    inbound.Size,
		},
		Reader: io.MultiReader(
			strings.NewReader(returnPath),
			strings.NewReader(recvd),
			bytes.NewReader(source),
		),
	}
}

// deliverAtomic stages every target, commits them together, then emits one AfterMessageStored
// event per newly committed mailbox.  A failed staging or commit is discarded before returning,
// leaving no target visible.
func (s *StoreManager) deliverAtomic(
	logger zerolog.Logger,
	ats storage.AttemptStore,
	key string,
	deliveries []*Delivery,
) error {
	attempt := ats.BeginDelivery(key)
	for _, delivery := range deliveries {
		if _, err := attempt.Add(delivery); err != nil {
			logger.Error().Str("mailbox", delivery.Mailbox()).Err(err).Msg("Delivery failed")
			attempt.Discard()
			return err
		}
	}
	receipts, err := attempt.Commit()
	if err != nil {
		logger.Error().Err(err).Msg("Delivery commit failed")
		attempt.Discard()
		return err
	}
	// All targets became visible together; announce only the ones this attempt actually stored.
	for i, receipt := range receipts {
		if receipt.Replayed {
			logger.Debug().Str("mailbox", receipt.Mailbox).
				Msg("Delivery replayed earlier committed attempt")
			continue
		}
		ev := deliveries[i].Meta
		ev.ID = receipt.ID
		s.ExtHost.Events.AfterMessageStored.Emit(&ev)
	}
	return nil
}

// deliverSequential is the fallback for stores without AttemptStore support.  On failure it
// removes only the messages this call created, and events fire only after every add succeeds.
func (s *StoreManager) deliverSequential(
	logger zerolog.Logger,
	deliveries []*Delivery,
) error {
	added := make([]storage.DeliveryReceipt, 0, len(deliveries))
	for _, delivery := range deliveries {
		id, err := s.Store.AddMessage(delivery)
		if err != nil {
			logger.Error().Str("mailbox", delivery.Mailbox()).Err(err).Msg("Delivery failed")
			for _, done := range added {
				if rerr := s.Store.RemoveMessage(done.Mailbox, done.ID); rerr != nil {
					logger.Error().Str("mailbox", done.Mailbox).Err(rerr).
						Msg("Failed to undo delivery during compensation")
				}
			}
			return err
		}
		added = append(added, storage.DeliveryReceipt{Mailbox: delivery.Mailbox(), ID: id})
	}
	for i, receipt := range added {
		ev := deliveries[i].Meta
		ev.ID = receipt.ID
		s.ExtHost.Events.AfterMessageStored.Emit(&ev)
	}
	return nil
}

// dedupeMailboxes returns mailboxes in first-seen order with duplicates removed.
func dedupeMailboxes(mailboxes []string) []string {
	seen := make(map[string]struct{}, len(mailboxes))
	out := make([]string, 0, len(mailboxes))
	for _, mb := range mailboxes {
		if _, ok := seen[mb]; ok {
			continue
		}
		seen[mb] = struct{}{}
		out = append(out, mb)
	}
	return out
}

// attemptKey derives the deterministic idempotency key for one delivery attempt: envelope sender,
// the sorted target set, and the exact received DATA content.
func attemptKey(from string, mailboxes []string, source []byte) string {
	sorted := append([]string(nil), mailboxes...)
	sort.Strings(sorted)
	h := sha256.New()
	_, _ = h.Write([]byte(from))
	_, _ = h.Write([]byte{0})
	for i, mb := range sorted {
		if i > 0 {
			_, _ = h.Write([]byte{','})
		}
		_, _ = h.Write([]byte(mb))
	}
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(source)
	return hex.EncodeToString(h.Sum(nil))
}

// GetMetadata returns a slice of metadata for the specified mailbox.
func (s *StoreManager) GetMetadata(mailbox string) ([]*event.MessageMetadata, error) {
	messages, err := s.Store.GetMessages(mailbox)
	if err != nil {
		return nil, err
	}
	metas := make([]*event.MessageMetadata, len(messages))
	for i, sm := range messages {
		metas[i] = MakeMetadata(sm)
	}
	return metas, nil
}

// GetMessage returns the specified message.
func (s *StoreManager) GetMessage(mailbox, id string) (*Message, error) {
	sm, err := s.Store.GetMessage(mailbox, id)
	if err != nil || sm == nil {
		return nil, err
	}
	r, err := sm.Source()
	if err != nil {
		return nil, err
	}
	env, err := enmime.ReadEnvelope(r)
	if err != nil {
		return nil, err
	}
	_ = r.Close()
	header := MakeMetadata(sm)
	return &Message{MessageMetadata: *header, env: env}, nil
}

// MarkSeen marks the message as having been read.
func (s *StoreManager) MarkSeen(mailbox, id string) error {
	log.Debug().Str("module", "manager").Str("mailbox", mailbox).Str("id", id).
		Msg("Marking as seen")
	return s.Store.MarkSeen(mailbox, id)
}

// PurgeMessages removes all messages from the specified mailbox.
func (s *StoreManager) PurgeMessages(mailbox string) error {
	return s.Store.PurgeMessages(mailbox)
}

// RemoveMessage deletes the specified message.
func (s *StoreManager) RemoveMessage(mailbox, id string) error {
	return s.Store.RemoveMessage(mailbox, id)
}

// SourceReader allows the stored message source to be read.
func (s *StoreManager) SourceReader(mailbox, id string) (io.ReadCloser, error) {
	sm, err := s.Store.GetMessage(mailbox, id)
	if err != nil || sm == nil {
		return nil, err
	}
	return sm.Source()
}

// MailboxForAddress parses an email address to return the canonical mailbox name.
func (s *StoreManager) MailboxForAddress(mailbox string) (string, error) {
	return s.AddrPolicy.ExtractMailbox(mailbox)
}

// MakeMetadata populates Metadata from a storage.Message.
func MakeMetadata(m storage.Message) *event.MessageMetadata {
	return &event.MessageMetadata{
		Mailbox: m.Mailbox(),
		ID:      m.ID(),
		From:    m.From(),
		To:      m.To(),
		Date:    m.Date(),
		Subject: m.Subject(),
		Size:    m.Size(),
		Seen:    m.Seen(),
	}
}
