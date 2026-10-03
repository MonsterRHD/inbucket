package file

import (
	"bytes"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/inbucket/inbucket/v3/pkg/config"
	"github.com/inbucket/inbucket/v3/pkg/extension"
	"github.com/inbucket/inbucket/v3/pkg/storage"
	"github.com/inbucket/inbucket/v3/pkg/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSuite runs storage package test suite on file store.
func TestSuite(t *testing.T) {
	test.StoreSuite(t,
		func(conf config.Storage, extHost *extension.Host) (storage.Store, func(), error) {
			ds, _ := setupDataStore(conf, extHost)
			destroy := func() {
				teardownDataStore(ds)
			}
			return ds, destroy, nil
		})
}

// Test filestore initialization.
func TestFSNew(t *testing.T) {
	// Should fail if no path specified.
	ds, err := New(config.Storage{}, extension.NewHost())
	require.ErrorContains(t, err, "parameter not specified")
	assert.Nil(t, ds)
}

func TestFSGetMailPath(t *testing.T) {
	// Path should have `mail` dir appended.
	got := getMailPath(`one`)
	assert.Regexp(t, "^one.mail$", got, "Expected one/mail or similar")

	// Path should convert `$` to `:`.
	got = getMailPath(`C$\inbucket`)
	assert.Regexp(t, "^C:.inbucket.mail$", got, "Expected C:\\inbucket\\mail or similar")
}

// Test directory structure created by filestore
func TestFSDirStructure(t *testing.T) {
	ds, logbuf := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)
	root := ds.path

	// james hashes to 474ba67bdb289c6263b36dfd8a7bed6c85b04943
	mbName := "james"

	// Check filestore root exists
	assert.True(t, isDir(root), "Expected %q to be a directory", root)

	// Check mail dir exists
	expect := filepath.Join(root, "mail")
	assert.True(t, isDir(expect), "Expected %q to be a directory", expect)

	// Check first hash section does not exist
	expect = filepath.Join(root, "mail", "474")
	assert.False(t, isDir(expect), "Expected %q to not exist", expect)

	// Deliver test message
	id1, _ := test.DeliverToStore(t, ds, mbName, "test", time.Now())

	// Check path to message exists
	assert.True(t, isDir(expect), "Expected %q to be a directory", expect)
	expect = filepath.Join(expect, "474ba6")
	assert.True(t, isDir(expect), "Expected %q to be a directory", expect)
	expect = filepath.Join(expect, "474ba67bdb289c6263b36dfd8a7bed6c85b04943")
	assert.True(t, isDir(expect), "Expected %q to be a directory", expect)

	// Check files
	mbPath := expect
	expect = filepath.Join(mbPath, "index.gob")
	assert.True(t, isFile(expect), "Expected %q to be a file", expect)
	expect = filepath.Join(mbPath, id1+".raw")
	assert.True(t, isFile(expect), "Expected %q to be a file", expect)

	// Deliver second test message
	id2, _ := test.DeliverToStore(t, ds, mbName, "test 2", time.Now())

	// Check files
	expect = filepath.Join(mbPath, "index.gob")
	assert.True(t, isFile(expect), "Expected %q to be a file", expect)
	expect = filepath.Join(mbPath, id2+".raw")
	assert.True(t, isFile(expect), "Expected %q to be a file", expect)

	// Delete message
	err := ds.RemoveMessage(mbName, id1)
	require.NoError(t, err)

	// Message should be removed
	expect = filepath.Join(mbPath, id1+".raw")
	assert.False(t, isPresent(expect), "Did not expect %q to exist", expect)
	expect = filepath.Join(mbPath, "index.gob")
	assert.True(t, isFile(expect), "Expected %q to be a file", expect)

	// Delete message
	err = ds.RemoveMessage(mbName, id2)
	require.NoError(t, err)

	// Message should be removed
	expect = filepath.Join(mbPath, id2+".raw")
	assert.False(t, isPresent(expect), "Did not expect %q to exist", expect)

	// No messages, index & maildir should be removed
	expect = filepath.Join(mbPath, "index.gob")
	assert.False(t, isPresent(expect), "Did not expect %q to exist", expect)
	expect = mbPath
	assert.False(t, isPresent(expect), "Did not expect %q to exist", expect)

	if t.Failed() {
		// Wait for handler to finish logging
		time.Sleep(2 * time.Second)
		// Dump buffered log data if there was a failure
		_, _ = io.Copy(os.Stderr, logbuf)
	}
}

// Test missing files
func TestFSMissing(t *testing.T) {
	ds, logbuf := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	mbName := "fred"
	subjects := []string{"a", "b", "c"}
	sentIds := make([]string, len(subjects))

	for i, subj := range subjects {
		// Add a message
		id, _ := test.DeliverToStore(t, ds, mbName, subj, time.Now())
		sentIds[i] = id
	}

	// Delete a message file without removing it from index
	msg, err := ds.GetMessage(mbName, sentIds[1])
	require.NoError(t, err)
	fmsg := msg.(*Message)
	_ = os.Remove(fmsg.rawPath())
	msg, err = ds.GetMessage(mbName, sentIds[1])
	require.NoError(t, err)

	// Try to read parts of message
	_, err = msg.Source()
	require.Error(t, err)

	if t.Failed() {
		// Wait for handler to finish logging
		time.Sleep(2 * time.Second)
		// Dump buffered log data if there was a failure
		_, _ = io.Copy(os.Stderr, logbuf)
	}
}

// Test Get the latest message
func TestGetLatestMessage(t *testing.T) {
	ds, logbuf := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	// james hashes to 474ba67bdb289c6263b36dfd8a7bed6c85b04943
	mbName := "james"

	// Test empty mailbox
	msg, err := ds.GetMessage(mbName, "latest")
	assert.Nil(t, msg)
	require.Error(t, err)

	// Deliver test message
	test.DeliverToStore(t, ds, mbName, "test", time.Now())

	// Deliver test message 2
	id2, _ := test.DeliverToStore(t, ds, mbName, "test 2", time.Now())

	// Test get the latest message
	msg, err = ds.GetMessage(mbName, "latest")
	require.NoError(t, err)
	assert.Equal(t, id2, msg.ID(), "Expected %q to be equal to %q", msg.ID(), id2)

	// Deliver test message 3
	id3, _ := test.DeliverToStore(t, ds, mbName, "test 3", time.Now())

	msg, err = ds.GetMessage(mbName, "latest")
	require.NoError(t, err)
	assert.Equal(t, id3, msg.ID(), "Expected %q to be equal to %q", msg.ID(), id3)

	// Test wrong id
	_, err = ds.GetMessage(mbName, "wrongid")
	require.Error(t, err)

	if t.Failed() {
		// Wait for handler to finish logging
		time.Sleep(2 * time.Second)
		// Dump buffered log data if there was a failure
		_, _ = io.Copy(os.Stderr, logbuf)
	}
}

// Test that index publishing leaves no temp file behind during normal
// add/mark-seen/remove operations.
func TestFSIndexNoTempLeftovers(t *testing.T) {
	ds, logbuf := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	mbName := "james"
	mb := ds.mbox(mbName)

	id1, _ := test.DeliverToStore(t, ds, mbName, "one", time.Now())
	assert.True(t, isFile(mb.indexPath), "index should be published")
	assert.False(t, isPresent(mb.tempIndexPath), "temp index should not linger after publish")

	id2, _ := test.DeliverToStore(t, ds, mbName, "two", time.Now())
	require.NoError(t, ds.MarkSeen(mbName, id1))
	assert.False(t, isPresent(mb.tempIndexPath), "temp index should not linger after MarkSeen")

	require.NoError(t, ds.RemoveMessage(mbName, id1))
	assert.True(t, isFile(mb.indexPath), "index should remain for non-empty mailbox")
	assert.False(t, isPresent(mb.tempIndexPath), "temp index should not linger after remove")

	require.NoError(t, ds.RemoveMessage(mbName, id2))
	assert.False(t, isPresent(mb.indexPath), "index should be gone for empty mailbox")
	assert.False(t, isPresent(mb.tempIndexPath), "no temp should remain after last message removed")
	assert.False(t, isPresent(mb.path), "mailbox directory should be removed when empty")

	if t.Failed() {
		time.Sleep(2 * time.Second)
		_, _ = io.Copy(os.Stderr, logbuf)
	}
}

// Test that purging and appending agree on empty directory lifecycle, and that
// delivery works again after a purge.
func TestFSAppendAndPurgeEmptyDir(t *testing.T) {
	ds, logbuf := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	mbName := "james"
	mb := ds.mbox(mbName)

	for _, subj := range []string{"a", "b", "c"} {
		test.DeliverToStore(t, ds, mbName, subj, time.Now())
	}
	assert.True(t, isDir(mb.path))

	require.NoError(t, ds.PurgeMessages(mbName))
	assert.False(t, isPresent(mb.path), "purge should remove the mailbox directory")
	assert.False(t, isPresent(mb.tempIndexPath), "purge should not leave a temp index")

	// Purging a mailbox that never existed must behave the same and not error.
	require.NoError(t, ds.PurgeMessages("never-existed"))

	// Appending to a purged mailbox recreates everything from scratch.
	id, _ := test.DeliverToStore(t, ds, mbName, "resurrected", time.Now())
	assert.True(t, isFile(mb.indexPath))
	assert.True(t, isFile(filepath.Join(mb.path, id+".raw")))
	msgs, err := ds.GetMessages(mbName)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "resurrected", msgs[0].Subject())

	if t.Failed() {
		time.Sleep(2 * time.Second)
		_, _ = io.Copy(os.Stderr, logbuf)
	}
}

// TestFSRecoverStaleTemp simulates crashes at every stage of writeIndex
// (os.Create, gob encode, Flush, Sync, Close, pre-rename) by leaving temp files
// in various states, then reopening the store as Inbucket would on startup.
// Recovery is deterministic: the committed index stays authoritative; a temp
// without a committed index is an uncommitted generation and yields an empty
// mailbox.
func TestFSRecoverStaleTemp(t *testing.T) {
	mbName := "james"

	cases := []struct {
		name        string
		removeIndex bool // remove the committed index before reopening
		tempData    []byte
		wantCount   int
	}{
		{"no temp, valid index", false, nil, 3},
		{"empty temp, valid index", false, []byte{}, 3},
		{"garbage temp, valid index", false, []byte("not a gob at all"), 3},
		{"truncated temp, valid index", false, nil, 3}, // filled below from index prefix
		{"complete temp, valid index", false, nil, 3},  // filled below as full index copy
		{"garbage temp, no index", true, []byte("partial write"), 0},
		{"complete temp, no index", true, nil, 0}, // uncommitted generation
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds, logbuf := setupDataStore(config.Storage{}, extension.NewHost())
			defer teardownDataStore(ds)
			mb := ds.mbox(mbName)

			ids := make([]string, 0, 3)
			for _, subj := range []string{"one", "two", "three"} {
				id, _ := test.DeliverToStore(t, ds, mbName, subj, time.Now())
				ids = append(ids, id)
			}
			validIndex, err := os.ReadFile(mb.indexPath)
			require.NoError(t, err)

			switch tc.name {
			case "truncated temp, valid index":
				tc.tempData = validIndex[:len(validIndex)/2]
			case "complete temp, valid index", "complete temp, no index":
				tc.tempData = validIndex
			}

			if tc.removeIndex {
				require.NoError(t, os.Remove(mb.indexPath))
			}
			if tc.tempData != nil {
				require.NoError(t, os.WriteFile(mb.tempIndexPath, tc.tempData, 0660))
			}

			// Restart: a fresh Store instance over the same directory.
			reopened := reopenDataStore(t, ds)
			msgs, err := reopened.GetMessages(mbName)
			require.NoError(t, err, "recovery must not fail")
			assert.Len(t, msgs, tc.wantCount)

			// Temp must be gone deterministically after recovery.
			assert.False(t, isPresent(mb.tempIndexPath), "stale temp must be removed")

			if tc.wantCount > 0 {
				assert.True(t, isFile(mb.indexPath), "committed index must remain readable")
				for i, id := range ids {
					got, err := reopened.GetMessage(mbName, id)
					require.NoError(t, err)
					assert.Equal(t, id, got.ID(), "message %d mismatch", i)
				}
			}

			// Visiting mailboxes after recovery must work.
			require.NoError(t, reopened.VisitMailboxes(func([]storage.Message) bool {
				return true
			}))

			if t.Failed() {
				time.Sleep(2 * time.Second)
				_, _ = io.Copy(os.Stderr, logbuf)
			}
		})
	}
}

// TestFSCorruptIndexReturnsError ensures a corrupt committed index surfaces a
// clear error instead of silently looking like an empty mailbox or wiping raw
// message files.
func TestFSCorruptIndexReturnsError(t *testing.T) {
	mbName := "james"
	cases := []struct {
		name string
		// mutate corrupts the index in place.
		mutate func(indexBytes []byte, mb *mbox) error
	}{
		{"zero bytes", func(_ []byte, mb *mbox) error {
			return os.WriteFile(mb.indexPath, []byte{}, 0660)
		}},
		{"truncated mid stream", func(b []byte, mb *mbox) error {
			return os.WriteFile(mb.indexPath, b[:len(b)/2], 0660)
		}},
		{"garbage overwritten", func(_ []byte, mb *mbox) error {
			return os.WriteFile(mb.indexPath, []byte("corruption!"), 0660)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds, logbuf := setupDataStore(config.Storage{}, extension.NewHost())
			defer teardownDataStore(ds)
			mb := ds.mbox(mbName)

			ids := make([]string, 0, 3)
			for _, subj := range []string{"one", "two", "three"} {
				id, _ := test.DeliverToStore(t, ds, mbName, subj, time.Now())
				ids = append(ids, id)
			}
			indexBytes, err := os.ReadFile(mb.indexPath)
			require.NoError(t, err)
			require.NoError(t, tc.mutate(indexBytes, mb))

			reopened := reopenDataStore(t, ds)

			_, err = reopened.GetMessages(mbName)
			require.Error(t, err, "corrupt index must not look like an empty mailbox")
			assert.Contains(t, err.Error(), "corrupt mailbox index")

			_, err = reopened.GetMessage(mbName, ids[0])
			require.Error(t, err)
			assert.Contains(t, err.Error(), "corrupt mailbox index")

			// Purge must not wipe the mailbox directory while the index is
			// unreadable; raw message files stay in place for intervention.
			require.Error(t, reopened.PurgeMessages(mbName))
			for _, id := range ids {
				assert.True(t, isFile(filepath.Join(mb.path, id+".raw")),
					"raw message %s must not be silently deleted", id)
			}
			assert.True(t, isFile(mb.indexPath), "corrupt index must not be silently deleted")

			if t.Failed() {
				time.Sleep(2 * time.Second)
				_, _ = io.Copy(os.Stderr, logbuf)
			}
		})
	}
}

// TestFSWriteIndexAtomicPublish exercises writeIndex directly: after a
// successful call only the committed index exists and it decodes to exactly
// the published messages.
func TestFSWriteIndexAtomicPublish(t *testing.T) {
	ds, logbuf := setupDataStore(config.Storage{}, extension.NewHost())
	defer teardownDataStore(ds)

	mb := ds.mbox("direct")
	mb.Lock()
	mb.name = "direct"
	mb.messages = []*Message{{Fid: "20060102T150405-0042.raw", Fsubject: "direct"}}
	require.NoError(t, mb.writeIndex())
	mb.Unlock()

	assert.True(t, isFile(mb.indexPath))
	assert.False(t, isPresent(mb.tempIndexPath))

	read := ds.mbox("direct")
	read.RLock()
	defer read.RUnlock()
	msgs, err := read.getMessages()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "20060102T150405-0042.raw", msgs[0].ID())

	if t.Failed() {
		time.Sleep(2 * time.Second)
		_, _ = io.Copy(os.Stderr, logbuf)
	}
}

// reopenDataStore creates a new Store over an existing store's path, simulating
// an Inbucket restart.
func reopenDataStore(t *testing.T, ds *Store) storage.Store {
	t.Helper()
	cfg := config.Storage{Params: map[string]string{"path": ds.path}}
	s, err := New(cfg, extension.NewHost())
	require.NoError(t, err)
	return s
}

// setupDataStore creates a new FileDataStore in a temporary directory
func setupDataStore(cfg config.Storage, extHost *extension.Host) (*Store, *bytes.Buffer) {
	path, err := os.MkdirTemp("", "inbucket")
	if err != nil {
		panic(err)
	}

	// Capture log output.
	buf := new(bytes.Buffer)
	log.SetOutput(buf)

	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	cfg.Params["path"] = path
	s, err := New(cfg, extHost)
	if err != nil {
		panic(err)
	}

	return s.(*Store), buf
}

func teardownDataStore(ds *Store) {
	if err := os.RemoveAll(ds.path); err != nil {
		panic(err)
	}
}

func isPresent(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

func isFile(path string) bool {
	if fi, err := os.Lstat(path); err == nil {
		return !fi.IsDir()
	}
	return false
}

func isDir(path string) bool {
	if fi, err := os.Lstat(path); err == nil {
		return fi.IsDir()
	}
	return false
}
