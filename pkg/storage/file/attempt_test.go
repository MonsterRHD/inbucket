package file

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"net/mail"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/inbucket/inbucket/v3/pkg/config"
	"github.com/inbucket/inbucket/v3/pkg/extension"
	"github.com/inbucket/inbucket/v3/pkg/extension/event"
	"github.com/inbucket/inbucket/v3/pkg/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// attemptTestKey is a filesystem-safe 32 char hex key.
const attemptTestKey = "0123456789abcdef0123456789abcdef"

func attemptTestDelivery(mailbox, subject string) *message.Delivery {
	content := fmt.Sprintf("To: %s\r\nFrom: from@example.com\r\nSubject: %s\r\n\r\nbody\r\n",
		mailbox, subject)
	return &message.Delivery{
		Meta: event.MessageMetadata{
			Mailbox: mailbox,
			From:    &mail.Address{Address: "from@example.com"},
			To:      []*mail.Address{{Address: mailbox}},
			Date:    time.Now(),
			Subject: subject,
		},
		Reader: strings.NewReader(content),
	}
}

func reopenFileStore(t *testing.T, ds *Store) *Store {
	t.Helper()
	s, err := New(
		config.Storage{Params: map[string]string{"path": ds.path}},
		extension.NewHost(),
	)
	require.NoError(t, err)
	return s.(*Store)
}

func assertMailboxCount(t *testing.T, ds *Store, mailbox string, count int) {
	t.Helper()
	msgs, err := ds.GetMessages(mailbox)
	require.NoError(t, err)
	assert.Len(t, msgs, count, "mailbox %q message count", mailbox)
}

func assertSpoolEmpty(t *testing.T, ds *Store) {
	t.Helper()
	entries, err := os.ReadDir(ds.spoolPath)
	require.NoError(t, err)
	assert.Empty(t, entries, "spool directory should be empty")
}

// stagedFixture writes raw files and a journal as if an attempt had been prepared, without
// touching any mailbox index.
func stagedFixture(t *testing.T, ds *Store, mailboxes, subjects []string) *spoolRecord {
	t.Helper()
	require.Len(t, subjects, len(mailboxes))
	key := attemptTestKey
	dir := filepath.Join(ds.spoolPath, key)
	require.NoError(t, os.MkdirAll(dir, 0770))
	rec := &spoolRecord{Key: key}
	for i, name := range mailboxes {
		mb := ds.mbox(name)
		date := time.Now()
		fid := generateID(date)
		raw := []byte(fmt.Sprintf("To: %s\r\nSubject: %s\r\n\r\nbody of %s\r\n", name, subjects[i], fid))
		require.NoError(t, os.WriteFile(filepath.Join(dir, fid+".raw"), raw, 0660))
		rec.Targets = append(rec.Targets, &spoolTarget{
			Mailbox: name,
			Hash:    mb.dirName,
			ID:      fid,
			Date:    date,
			From:    &mail.Address{Address: "from@example.com"},
			To:      []*mail.Address{{Address: name}},
			Subject: subjects[i],
			Size:    int64(len(raw)),
		})
	}
	writeTestJournal(t, ds, rec)
	return rec
}

func writeTestJournal(t *testing.T, ds *Store, rec *spoolRecord) {
	t.Helper()
	path := filepath.Join(ds.spoolPath, rec.Key+journalExt)
	f, err := os.Create(path)
	require.NoError(t, err)
	w := bufio.NewWriter(f)
	require.NoError(t, gob.NewEncoder(w).Encode(rec))
	require.NoError(t, w.Flush())
	require.NoError(t, f.Close())
}

// TestAttemptCommitReplayCleanup exercises the happy path: content appears only after commit, the
// spool is cleaned, and a same-key retry replays without duplicating.
func TestAttemptCommitReplayCleanup(t *testing.T) {
	ds, _ := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	boxes := []string{"alpha", "beta"}
	a := ds.BeginDelivery(attemptTestKey)
	for _, name := range boxes {
		_, err := a.Add(attemptTestDelivery(name, "first"))
		require.NoError(t, err)
	}
	// Nothing visible before commit.
	for _, name := range boxes {
		assertMailboxCount(t, ds, name, 0)
	}
	receipts, err := a.Commit()
	require.NoError(t, err)
	require.Len(t, receipts, 2)
	for _, r := range receipts {
		assert.False(t, r.Replayed)
		assert.NotEmpty(t, r.ID)
	}
	for _, name := range boxes {
		assertMailboxCount(t, ds, name, 1)
	}
	assertSpoolEmpty(t, ds)

	// Retrying the same key replays the committed messages and adds nothing.
	expectedIDs := make(map[string]string, len(receipts))
	for _, r := range receipts {
		expectedIDs[r.Mailbox] = r.ID
	}
	retry := ds.BeginDelivery(attemptTestKey)
	for _, name := range boxes {
		id, err := retry.Add(attemptTestDelivery(name, "first"))
		require.NoError(t, err)
		assert.Equal(t, expectedIDs[name], id, "replay returned wrong ID for %s", name)
	}
	receipts2, err := retry.Commit()
	require.NoError(t, err)
	require.Len(t, receipts2, 2)
	for i, name := range boxes {
		assert.True(t, receipts2[i].Replayed, "%s should replay", name)
		assert.Equal(t, receipts[i].ID, receipts2[i].ID)
		assertMailboxCount(t, ds, name, 1)
	}
	assertSpoolEmpty(t, ds)
}

// TestAttemptDiscard leaves no visible content or spool artifacts.
func TestAttemptDiscard(t *testing.T) {
	ds, _ := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	a := ds.BeginDelivery(attemptTestKey)
	_, err := a.Add(attemptTestDelivery("alpha", "x"))
	require.NoError(t, err)
	_, err = a.Add(attemptTestDelivery("beta", "y"))
	require.NoError(t, err)
	a.Discard()

	assertMailboxCount(t, ds, "alpha", 0)
	assertMailboxCount(t, ds, "beta", 0)
	assertSpoolEmpty(t, ds)
}

// TestRecoverRollbackUncommitted: a prepared journal with no committed targets is cleaned up.
func TestRecoverRollbackUncommitted(t *testing.T) {
	ds, _ := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	boxes := []string{"alpha", "beta"}
	stagedFixture(t, ds, boxes, []string{"s1", "s2"})

	ds = reopenFileStore(t, ds)
	for _, name := range boxes {
		assertMailboxCount(t, ds, name, 0)
	}
	assertSpoolEmpty(t, ds)
}

// TestRecoverContinuePartial: one target committed before exit; recovery commits the other.
func TestRecoverContinuePartial(t *testing.T) {
	ds, _ := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	boxes := []string{"alpha", "beta"}
	rec := stagedFixture(t, ds, boxes, []string{"s1", "s2"})

	// Simulate a crash after committing only the first target.
	require.NoError(t, ds.commitTarget(rec.Key, rec.Targets[0]))
	assertMailboxCount(t, ds, "alpha", 1)
	assertMailboxCount(t, ds, "beta", 0)

	ds = reopenFileStore(t, ds)
	for _, name := range boxes {
		assertMailboxCount(t, ds, name, 1)
	}
	assertSpoolEmpty(t, ds)

	// Recovered message content is readable.
	msgs, err := ds.GetMessages("beta")
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	r, err := msgs[0].Source()
	require.NoError(t, err)
	body, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	assert.Contains(t, string(body), "s2")
	assert.Equal(t, rec.Targets[1].ID, msgs[0].ID())

	// Same-key retry after recovery is a replay.
	a := ds.BeginDelivery(attemptTestKey)
	_, err = a.Add(attemptTestDelivery("alpha", "s1"))
	require.NoError(t, err)
	_, err = a.Add(attemptTestDelivery("beta", "s2"))
	require.NoError(t, err)
	receipts, err := a.Commit()
	require.NoError(t, err)
	for _, r := range receipts {
		assert.True(t, r.Replayed)
	}
	for _, name := range boxes {
		assertMailboxCount(t, ds, name, 1)
	}
}

// TestRecoverContinueAfterRename: a raw file moved ahead of its index write is picked up by
// recovery and appended to the index.
func TestRecoverContinueAfterRename(t *testing.T) {
	ds, _ := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	boxes := []string{"alpha", "beta"}
	rec := stagedFixture(t, ds, boxes, []string{"s1", "s2"})

	// Target alpha committed normally.
	require.NoError(t, ds.commitTarget(rec.Key, rec.Targets[0]))

	// Target beta: raw renamed into mailbox dir but index never written.
	mb := ds.mbox("beta")
	require.NoError(t, mb.createDir())
	dest := filepath.Join(mb.path, rec.Targets[1].ID+".raw")
	require.NoError(t, os.Rename(
		filepath.Join(ds.spoolPath, rec.Key, rec.Targets[1].ID+".raw"), dest))

	ds = reopenFileStore(t, ds)
	for _, name := range boxes {
		assertMailboxCount(t, ds, name, 1)
	}
	msgs, err := ds.GetMessages("beta")
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, rec.Targets[1].ID, msgs[0].ID())
	assertSpoolEmpty(t, ds)
}

// TestRecoverCompletedRemovesJournal: a journal left behind after a full commit is cleaned up and
// content stays intact.
func TestRecoverCompletedRemovesJournal(t *testing.T) {
	ds, _ := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	boxes := []string{"alpha", "beta"}
	rec := stagedFixture(t, ds, boxes, []string{"s1", "s2"})
	for _, target := range rec.Targets {
		require.NoError(t, ds.commitTarget(rec.Key, target))
	}
	// Journal deliberately left on disk, stage raws moved; reopen must tolerate both.
	ds = reopenFileStore(t, ds)
	for _, name := range boxes {
		assertMailboxCount(t, ds, name, 1)
	}
	assertSpoolEmpty(t, ds)
}

// TestRecoverStrayStageDir: a stage directory without a journal is removed; nothing was indexed.
func TestRecoverStrayStageDir(t *testing.T) {
	ds, _ := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	dir := filepath.Join(ds.spoolPath, "deadbeefdeadbeefdeadbeefdeadbeef")
	require.NoError(t, os.MkdirAll(dir, 0770))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "x.raw"), []byte("x"), 0660))
	require.NoError(t, os.WriteFile(filepath.Join(ds.spoolPath, "y.gob.tmp"), []byte("x"), 0660))

	ds = reopenFileStore(t, ds)
	assertSpoolEmpty(t, ds)
}
