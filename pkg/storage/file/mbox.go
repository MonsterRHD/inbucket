package file

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/inbucket/inbucket/v3/pkg/message"
	"github.com/inbucket/inbucket/v3/pkg/storage"
	"github.com/rs/zerolog/log"
)

// indexTempSuffix is appended to the index file name while a new index
// generation is being written.  The temp file lives in the same directory as
// the index so it can be published with an atomic os.Rename; a leftover file
// with this suffix is always an uncommitted generation and is discarded by
// readIndex.
const indexTempSuffix = ".tmp"

// mbox manages the mail for a specific user and correlates to a particular directory on disk.
// mbox methods are not thread safe, mbox.RWMutex must be held prior to calling.
type mbox struct {
	*sync.RWMutex
	store         *Store
	name          string
	dirName       string
	path          string
	indexLoaded   bool
	indexPath     string
	tempIndexPath string
	messages      []*Message
}

// getMessages scans the mailbox directory for .gob files and decodes them into
// a slice of Message objects.
func (mb *mbox) getMessages() ([]storage.Message, error) {
	if !mb.indexLoaded {
		if err := mb.readIndex(); err != nil {
			return nil, err
		}
	}
	messages := make([]storage.Message, len(mb.messages))
	for i, m := range mb.messages {
		messages[i] = m
	}
	return messages, nil
}

// getMessage decodes a single message by ID and returns a Message object.
func (mb *mbox) getMessage(id string) (storage.Message, error) {
	if !mb.indexLoaded {
		if err := mb.readIndex(); err != nil {
			return nil, err
		}
	}
	if id == "latest" && len(mb.messages) != 0 {
		return mb.messages[len(mb.messages)-1], nil
	}
	for _, m := range mb.messages {
		if m.Fid == id {
			return m, nil
		}
	}
	return nil, storage.ErrNotExist
}

// removeMessage deletes the message off disk and removes it from the index.
func (mb *mbox) removeMessage(id string) error {
	if !mb.indexLoaded {
		if err := mb.readIndex(); err != nil {
			return err
		}
	}
	var msg *Message
	for i, m := range mb.messages {
		if id == m.ID() {
			msg = m
			// Slice around message we are deleting
			mb.messages = append(mb.messages[:i], mb.messages[i+1:]...)

			// Emit deleted event.
			mb.store.extHost.Events.AfterMessageDeleted.Emit(message.MakeMetadata(msg))

			break
		}
	}
	if msg == nil {
		return storage.ErrNotExist
	}
	if err := mb.writeIndex(); err != nil {
		return err
	}
	if len(mb.messages) == 0 {
		// This was the last message, thus writeIndex() has removed the entire
		// directory; we don't need to delete the raw file.
		return nil
	}
	// There are still messages in the index
	log.Debug().Str("module", "storage").Str("path", msg.rawPath()).Msg("Deleting file")
	return os.Remove(msg.rawPath())
}

// purge deletes all messages in this mailbox.
func (mb *mbox) purge() error {
	mb.messages = mb.messages[:0]
	return mb.writeIndex()
}

// readIndex loads the mailbox index data from disk
func (mb *mbox) readIndex() error {
	// Clear message slice, open index
	mb.messages = mb.messages[:0]

	// Deterministic crash recovery: os.Rename is the commit point in
	// writeIndex, so any leftover temp file belongs to an index generation that
	// was never published.  Discard it; the previously committed index (if
	// any) stays authoritative and remains readable.
	if err := os.Remove(mb.tempIndexPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Warn().Str("module", "storage").Str("path", mb.tempIndexPath).Err(err).
			Msg("Failed to remove stale temp index")
		// Continue on: it never shadows the committed index path.
	}

	// Check if index exists
	if _, err := os.Stat(mb.indexPath); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			// A real I/O error must surface rather than looking like an empty
			// mailbox.
			return err
		}
		// Does not exist, but that's not an error in our world
		log.Debug().Str("module", "storage").Str("path", mb.indexPath).
			Msg("Index does not yet exist")
		mb.indexLoaded = true

		//lint:ignore nilerr missing mailboxes are considered empty.
		return nil
	}
	file, err := os.Open(mb.indexPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Error().Str("module", "storage").Str("path", mb.indexPath).Err(err).
				Msg("Failed to close")
		}
	}()
	// Decode gob data
	br := mb.store.getPooledReader(file)
	defer mb.store.putPooledReader(br)
	dec := gob.NewDecoder(br)
	name := ""
	if err = dec.Decode(&name); err != nil {
		return fmt.Errorf("corrupt mailbox index %q: %w", mb.indexPath, err)
	}
	mb.name = name
	for {
		// Load messages until EOF
		msg := &Message{}
		if err = dec.Decode(msg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("corrupt mailbox index %q: %w", mb.indexPath, err)
		}
		msg.mailbox = mb
		mb.messages = append(mb.messages, msg)
	}
	mb.indexLoaded = true
	return nil
}

// writeIndex atomically overwrites the index on disk with the current mailbox
// data.  The new generation is fully written, flushed and synced to a temp
// file, then published over the index with an atomic os.Rename, so a crash
// during os.Create, gob encoding, Flush, Sync or Close leaves the previously
// committed index readable.  When there are no messages left the mailbox
// directory is removed instead; this is the same empty-directory behavior used
// by both single message removal and purge.
func (mb *mbox) writeIndex() error {
	if len(mb.messages) == 0 {
		// No messages, delete index, stale temp, raw files and maildir.
		log.Debug().Str("module", "storage").Str("path", mb.path).Msg("Removing mailbox")
		return mb.removeDir()
	}
	// Ensure mailbox directory exists before creating the temp file; it must
	// be in the same directory as the index so Rename is atomic.
	if err := mb.createDir(); err != nil {
		return err
	}
	file, err := os.Create(mb.tempIndexPath)
	if err != nil {
		return err
	}
	// On any failure the temp file is unpublished garbage; remove it so it can
	// never be confused with committed data.  readIndex performs the same
	// cleanup after a hard crash.
	published := false
	defer func() {
		if !published {
			if err := os.Remove(mb.tempIndexPath); err != nil &&
				!errors.Is(err, fs.ErrNotExist) {
				log.Warn().Str("module", "storage").Str("path", mb.tempIndexPath).Err(err).
					Msg("Failed to remove temp index")
			}
		}
	}()
	writer := bufio.NewWriter(file)
	// Write each message and then flush
	enc := gob.NewEncoder(writer)
	if err = enc.Encode(mb.name); err != nil {
		_ = file.Close()
		return err
	}
	for _, m := range mb.messages {
		if err = enc.Encode(m); err != nil {
			_ = file.Close()
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		log.Error().Str("module", "storage").Str("path", mb.tempIndexPath).Err(err).
			Msg("Failed to close")
		return err
	}
	// Atomic publish: the old index stays readable up to this point.
	if err := os.Rename(mb.tempIndexPath, mb.indexPath); err != nil {
		return err
	}
	published = true
	// Best-effort flush of the directory entry so the rename survives power
	// loss as well.
	syncDirBestEffort(mb.path)
	return nil
}

// syncDirBestEffort flushes directory metadata to disk so a renamed index
// entry survives a power failure.  It is best-effort, and a no-op where
// directory sync is unsupported.
func syncDirBestEffort(path string) {
	dir, err := os.Open(path)
	if err != nil {
		return
	}
	defer func() {
		_ = dir.Close()
	}()
	if err := dir.Sync(); err != nil {
		log.Debug().Str("module", "storage").Str("path", path).Err(err).
			Msg("Unable to sync directory")
	}
}

// createDir checks for the presence of the path for this mailbox, creates it if needed
func (mb *mbox) createDir() error {
	if _, err := os.Stat(mb.path); err != nil {
		if err := os.MkdirAll(mb.path, 0770); err != nil {
			log.Error().Str("module", "storage").Str("path", mb.path).Err(err).
				Msg("Failed to create directory")
			return err
		}
	}
	return nil
}

// removeDir removes the mailbox, plus empty higher level directories
func (mb *mbox) removeDir() error {
	// remove mailbox dir, including index file
	if err := os.RemoveAll(mb.path); err != nil {
		return err
	}
	// remove parents if empty
	dir := filepath.Dir(mb.path)
	if removeDirIfEmpty(dir) {
		removeDirIfEmpty(filepath.Dir(dir))
	}
	return nil
}

// removeDirIfEmpty will remove the specified directory if it contains no files or directories.
// Returns true if dir was removed.
func removeDirIfEmpty(path string) (removed bool) {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	files, err := f.Readdirnames(0)
	_ = f.Close()
	if err != nil {
		return false
	}
	if len(files) > 0 {
		// Dir not empty
		return false
	}
	log.Debug().Str("module", "storage").Str("path", path).Msg("Removing dir")
	err = os.Remove(path)
	if err != nil {
		log.Error().Str("module", "storage").Str("path", path).Err(err).Msg("Failed to remove")
		return false
	}
	return true
}
