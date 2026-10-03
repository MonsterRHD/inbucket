package file

import (
	"bufio"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net/mail"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/inbucket/inbucket/v3/pkg/storage"
	"github.com/rs/zerolog/log"
)

var (
	// errSpoolMissing is returned when staged content is missing in both the spool directory and
	// the destination mailbox.
	errSpoolMissing = errors.New("staged message content is missing")

	// errAttemptDone is returned when an attempt is committed or discarded more than once.
	errAttemptDone = errors.New("delivery attempt already committed or discarded")
)

var _ storage.AttemptStore = (*Store)(nil)

// spoolTarget is one persisted target of a delivery attempt journal.
type spoolTarget struct {
	Mailbox   string
	Hash      string
	ID        string
	Date      time.Time
	From      *mail.Address
	To        []*mail.Address
	Subject   string
	Size      int64
	Committed bool
}

// spoolRecord is the durable journal of one staged delivery attempt.
type spoolRecord struct {
	Key     string
	Targets []*spoolTarget
}

// fileTarget tracks one target while an attempt is in progress.
type fileTarget struct {
	mailbox  string
	rec      *spoolTarget // nil when replaying an already committed target.
	replayID string
}

// fileDelivery implements storage.DeliveryAttempt on top of a per-attempt spool directory and a
// durable gob journal.
type fileDelivery struct {
	fs      *Store
	key     string // filesystem-safe attempt key
	keyMu   *sync.Mutex
	targets []*fileTarget
	dirMade bool
	mu      sync.Mutex
	closed  bool
}

// spoolKey maps an arbitrary attempt key to a filesystem-safe, stable identifier.
func spoolKey(key string) string {
	safe := len(key) >= 16
	for _, r := range key {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			safe = false
			break
		}
	}
	if !safe {
		return fmt.Sprintf("%x", sha256.Sum256([]byte(key)))
	}
	return key
}

// keyLock serializes delivery attempts sharing an idempotency key.
func (fs *Store) keyLock(key string) *sync.Mutex {
	fs.keyRegMu.Lock()
	mu, ok := fs.keyLocks[key]
	if !ok {
		mu = &sync.Mutex{}
		fs.keyLocks[key] = mu
	}
	fs.keyRegMu.Unlock()
	return mu
}

// BeginDelivery starts an atomic, idempotent multi-mailbox delivery attempt.  It blocks while an
// earlier attempt with the same key is still in progress.
func (fs *Store) BeginDelivery(rawKey string) storage.DeliveryAttempt {
	key := spoolKey(rawKey)
	mu := fs.keyLock(key)
	mu.Lock()
	return &fileDelivery{
		fs:    fs,
		key:   key,
		keyMu: mu,
	}
}

// stageDir returns the directory holding staged raw messages for this attempt.
func (d *fileDelivery) stageDir() string {
	return filepath.Join(d.fs.spoolPath, d.key)
}

// journalPath returns the durable journal path of this attempt.
func (d *fileDelivery) journalPath() string {
	return filepath.Join(d.fs.spoolPath, d.key+journalExt)
}

// Add stages one target message.  Content is written to the spool directory and stays absent from
// mailbox indexes until Commit.  A target already committed under this key is reported as replay.
func (d *fileDelivery) Add(m storage.Message) (id string, err error) {
	mb := d.fs.mbox(m.Mailbox())
	mb.Lock()
	defer mb.Unlock()

	if !mb.indexLoaded {
		if err = mb.readIndex(); err != nil {
			return "", err
		}
	}
	if existing := mb.findAttempt("", d.key); existing != nil {
		// An earlier attempt with this key already committed the target.
		d.mu.Lock()
		d.targets = append(d.targets, &fileTarget{
			mailbox:  m.Mailbox(),
			replayID: existing.Fid,
		})
		d.mu.Unlock()
		return existing.Fid, nil
	}

	date := m.Date()
	fid := generateID(date)

	if !d.dirMade {
		if err := os.MkdirAll(d.stageDir(), 0770); err != nil {
			return "", err
		}
		d.dirMade = true
	}

	r, err := m.Source()
	if err != nil {
		return "", err
	}
	rawPath := filepath.Join(d.stageDir(), fid+".raw")
	file, err := os.Create(rawPath)
	if err != nil {
		_ = r.Close()
		return "", err
	}
	w := bufio.NewWriter(file)
	size, err := io.Copy(w, r)
	_ = r.Close()
	if err != nil {
		_ = file.Close()
		_ = os.Remove(rawPath)
		return "", err
	}
	if err := w.Flush(); err != nil {
		_ = file.Close()
		_ = os.Remove(rawPath)
		return "", err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(rawPath)
		return "", err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(rawPath)
		return "", err
	}

	t := &fileTarget{
		mailbox: m.Mailbox(),
		rec: &spoolTarget{
			Mailbox: m.Mailbox(),
			Hash:    mb.dirName,
			ID:      fid,
			Date:    date,
			From:    m.From(),
			To:      m.To(),
			Subject: m.Subject(),
			Size:    size,
		},
	}
	d.mu.Lock()
	d.targets = append(d.targets, t)
	d.mu.Unlock()
	return fid, nil
}

// Commit publishes all staged targets.  Every target mailbox is locked for the duration, so
// readers cannot observe a partial attempt.  A durable journal is written first, allowing
// recoverSpool to resolve a process exit during the commit.
func (d *fileDelivery) Commit() ([]storage.DeliveryReceipt, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, errAttemptDone
	}
	d.closed = true
	targets := d.targets
	d.mu.Unlock()
	defer d.keyMu.Unlock()

	fresh := make([]*spoolTarget, 0, len(targets))
	for _, t := range targets {
		if t.rec != nil {
			fresh = append(fresh, t.rec)
		}
	}

	if len(fresh) > 0 {
		// Durably record the plan before touching any mailbox index.
		if err := d.writeJournal(&spoolRecord{Key: d.key, Targets: fresh}); err != nil {
			d.cleanup()
			return nil, err
		}

		// Lock every distinct target mailbox in stable hash order for an atomic in-process
		// visibility switch.
		ordered := make([]*mbox, 0, len(fresh))
		lockedSeen := make(map[string]struct{}, len(fresh))
		for _, rec := range fresh {
			if _, ok := lockedSeen[rec.Hash]; ok {
				continue
			}
			lockedSeen[rec.Hash] = struct{}{}
			ordered = append(ordered, d.fs.mbox(rec.Mailbox))
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].dirName < ordered[j].dirName })
		for _, mb := range ordered {
			mb.Lock()
		}
		unlock := func() {
			for i := len(ordered) - 1; i >= 0; i-- {
				ordered[i].Unlock()
			}
		}

		for _, rec := range fresh {
			mb := d.fs.mbox(rec.Mailbox)
			if mb.indexedAttempt(rec.ID, d.key) {
				rec.Committed = true
				continue
			}
			if err := d.fs.commitTargetLocked(mb, d.key, rec); err != nil {
				// Roll back everything this attempt made visible, still holding all locks.
				d.rollbackLocked(fresh)
				unlock()
				d.cleanup()
				return nil, err
			}
			// Persist progress so recovery resumes from here.
			if err := d.writeJournal(&spoolRecord{Key: d.key, Targets: fresh}); err != nil {
				log.Warn().Str("module", "storage").Str("attempt", d.key).Err(err).
					Msg("Failed to update delivery journal")
			}
		}
		unlock()
	}

	// All targets durable in their mailbox indexes; spool content and journal are no longer needed.
	d.cleanup()

	receipts := make([]storage.DeliveryReceipt, len(targets))
	for i, t := range targets {
		r := storage.DeliveryReceipt{Mailbox: t.mailbox}
		if t.rec != nil {
			r.ID = t.rec.ID
		} else {
			r.ID = t.replayID
			r.Replayed = true
		}
		receipts[i] = r
	}
	return receipts, nil
}

// rollbackLocked removes attempt content from every target while the commit locks are held,
// without emitting delete events: the content was never visible.
func (d *fileDelivery) rollbackLocked(fresh []*spoolTarget) {
	for _, rec := range fresh {
		mb := d.fs.mbox(rec.Mailbox)
		if mb.indexedAttempt(rec.ID, d.key) {
			if _, err := mb.deleteIndexed(rec.ID); err != nil {
				log.Error().Str("module", "storage").Str("mailbox", rec.Mailbox).Str("id", rec.ID).
					Err(err).Msg("Failed to roll back committed target")
			}
			continue
		}
		// Rename may have landed the raw file ahead of a failed index write.
		if _, err := os.Stat(mb.path); err == nil {
			if err := os.Remove(filepath.Join(mb.path, rec.ID+".raw")); err != nil &&
				!errors.Is(err, os.ErrNotExist) {
				log.Error().Str("module", "storage").Str("path", mb.path).Err(err).
					Msg("Failed to remove orphan raw during rollback")
			}
		}
	}
}

// Discard abandons the attempt.  It is called after staging failures or a rolled-back Commit, so
// it only needs to drop the spool stage; mailbox indexes never reference a discarded attempt.
func (d *fileDelivery) Discard() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	d.mu.Unlock()
	defer d.keyMu.Unlock()

	d.cleanup()
}

// cleanup removes staged files, the stage directory, and the journal.
func (d *fileDelivery) cleanup() {
	if err := os.RemoveAll(d.stageDir()); err != nil {
		log.Warn().Str("module", "storage").Str("path", d.stageDir()).Err(err).
			Msg("Failed to remove delivery stage")
	}
	if err := os.Remove(d.journalPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Warn().Str("module", "storage").Str("path", d.journalPath()).Err(err).
			Msg("Failed to remove delivery journal")
	}
	_ = os.Remove(d.journalPath() + ".tmp")
}

// writeJournal atomically writes the attempt journal.
func (d *fileDelivery) writeJournal(rec *spoolRecord) error {
	tmp := d.journalPath() + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(file)
	enc := gob.NewEncoder(w)
	if err := enc.Encode(rec); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := w.Flush(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, d.journalPath()); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	syncDir(d.fs.spoolPath)
	return nil
}

// commitTarget publishes a single journal target into its mailbox index, taking the mailbox lock.
// It is idempotent: a target already present in the index is simply marked committed.  Used by
// crash recovery.
func (fs *Store) commitTarget(key string, t *spoolTarget) error {
	mb := fs.mbox(t.Mailbox)
	mb.Lock()
	defer mb.Unlock()
	return fs.commitTargetLocked(mb, key, t)
}

// commitTargetLocked publishes one target assuming its mailbox lock is held and the index is
// loaded.
func (fs *Store) commitTargetLocked(mb *mbox, key string, t *spoolTarget) error {
	if !mb.indexLoaded {
		if err := mb.readIndex(); err != nil {
			return err
		}
	}
	if mb.indexedAttempt(t.ID, key) {
		t.Committed = true
		return nil
	}

	if err := mb.evictOverCap(); err != nil {
		return err
	}

	// Eviction may remove the last message, which removes the mailbox directory.
	if err := mb.createDir(); err != nil {
		return err
	}

	dest := filepath.Join(mb.path, t.ID+".raw")
	if _, err := os.Stat(dest); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		src := filepath.Join(fs.spoolPath, key, t.ID+".raw")
		if rerr := os.Rename(src, dest); rerr != nil {
			// Rename is atomic on the same volume; only accept failure if the destination exists.
			if _, serr := os.Stat(dest); serr != nil {
				if _, serr := os.Stat(src); serr != nil {
					return fmt.Errorf("%w: %s: %v", errSpoolMissing, t.Mailbox, rerr)
				}
				return rerr
			}
		}
	}

	mb.messages = append(mb.messages, &Message{
		mailbox:  mb,
		Fid:      t.ID,
		Fdate:    t.Date,
		Ffrom:    t.From,
		Fto:      t.To,
		Fsubject: t.Subject,
		Fsize:    t.Size,
		Fattempt: key,
	})
	if err := mb.writeIndex(); err != nil {
		return err
	}
	t.Committed = true
	return nil
}

// findAttempt returns an indexed message matching the given ID or attempt key; the mailbox lock
// must be held and the index loaded.
func (mb *mbox) findAttempt(id, key string) *Message {
	for _, m := range mb.messages {
		if (id != "" && m.Fid == id) || (key != "" && m.Fattempt == key) {
			return m
		}
	}
	return nil
}

// indexedAttempt reports whether the target ID (or any message of the attempt) is already in the
// index.  The mailbox lock must be held.
func (mb *mbox) indexedAttempt(id, key string) bool {
	if !mb.indexLoaded {
		if err := mb.readIndex(); err != nil {
			return false
		}
	}
	return mb.findAttempt(id, key) != nil
}

// syncDir fsyncs a directory so recent renames are durable.
func syncDir(path string) {
	dir, err := os.Open(path)
	if err != nil {
		return
	}
	_ = dir.Sync()
	_ = dir.Close()
}

// readJournal decodes an attempt journal.
func readJournal(path string) (*spoolRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	rec := &spoolRecord{}
	if err := gob.NewDecoder(file).Decode(rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// recoverSpool resolves delivery attempts interrupted by a process exit, using only persisted
// state (journal flags + mailbox indexes): fully or partially committed attempts are completed,
// unstarted ones are cleaned up.
func (fs *Store) recoverSpool() error {
	entries, err := os.ReadDir(fs.spoolPath)
	if err != nil {
		return err
	}

	type pending struct {
		key string
		rec *spoolRecord
	}
	var journals []pending
	stagedDirs := make(map[string]bool)
	var leftovers []string

	for _, e := range entries {
		name := e.Name()
		switch {
		case e.IsDir():
			stagedDirs[name] = true
		case strings.HasSuffix(name, journalExt):
			journals = append(journals, pending{key: strings.TrimSuffix(name, journalExt)})
		case strings.HasSuffix(name, ".tmp"):
			leftovers = append(leftovers, filepath.Join(fs.spoolPath, name))
		}
	}

	for i := range journals {
		j := &journals[i]
		jpath := filepath.Join(fs.spoolPath, j.key+journalExt)
		rec, err := readJournal(jpath)
		if err != nil {
			return fmt.Errorf("corrupt delivery journal %q: %w", jpath, err)
		}
		j.rec = rec
		delete(stagedDirs, j.key)

		committed := 0
		status := make([]bool, len(rec.Targets))
		for n, t := range rec.Targets {
			mb := fs.mbox(t.Mailbox)
			mb.Lock()
			present := mb.indexedAttempt(t.ID, rec.Key)
			mb.Unlock()
			status[n] = present
			if present {
				committed++
			}
		}

		switch committed {
		case 0:
			// Nothing ever became visible; remove orphan raw files that were renamed ahead of an
			// index write, then drop stage and journal.
			fs.removeOrphanRaws(rec)
			fs.removeSpool(rec.Key)
			log.Info().Str("module", "storage").Str("attempt", rec.Key).
				Msg("Recovered: rolled back uncommitted delivery attempt")
		case len(rec.Targets):
			// Commit had completed before exit.
			fs.removeSpool(rec.Key)
			log.Info().Str("module", "storage").Str("attempt", rec.Key).
				Msg("Recovered: completed delivery attempt")
		default:
			// Partial commit: continue the remaining targets.
			fs.resumeAttempt(rec, status)
		}
	}

	// Staging directories without a journal predate the durable plan and could never have reached
	// a mailbox index; remove them along with temporary journal files.
	for dir := range stagedDirs {
		leftovers = append(leftovers, filepath.Join(fs.spoolPath, dir))
	}
	for _, p := range leftovers {
		if err := os.RemoveAll(p); err != nil {
			log.Warn().Str("module", "storage").Str("path", p).Err(err).
				Msg("Failed to remove leftover spool entry")
		}
	}
	return nil
}

// resumeAttempt finishes a partially committed attempt, or rolls it back if staged content has
// been lost.
func (fs *Store) resumeAttempt(rec *spoolRecord, status []bool) {
	for n, t := range rec.Targets {
		if status[n] {
			t.Committed = true
			continue
		}
		if err := fs.commitTarget(rec.Key, t); err != nil {
			log.Error().Str("module", "storage").Str("attempt", rec.Key).Str("mailbox", t.Mailbox).
				Err(err).Msg("Cannot resume delivery target, rolling attempt back")
			fs.rollbackAttempt(rec)
			return
		}
	}
	fs.removeSpool(rec.Key)
	log.Info().Str("module", "storage").Str("attempt", rec.Key).
		Msg("Recovered: continued partial delivery attempt")
}

// rollbackAttempt removes exactly the messages this attempt added so every target becomes
// invisible again; pre-existing mailbox content is untouched.
func (fs *Store) rollbackAttempt(rec *spoolRecord) {
	for _, t := range rec.Targets {
		mb := fs.mbox(t.Mailbox)
		mb.Lock()
		if !mb.indexLoaded {
			if err := mb.readIndex(); err != nil {
				mb.Unlock()
				log.Error().Str("module", "storage").Str("mailbox", t.Mailbox).Err(err).
					Msg("Rollback failed to read index")
				continue
			}
		}
		if mb.findAttempt(t.ID, rec.Key) == nil {
			mb.Unlock()
			continue
		}
		if err := mb.removeMessage(t.ID); err != nil {
			log.Error().Str("module", "storage").Str("mailbox", t.Mailbox).Str("id", t.ID).Err(err).
				Msg("Rollback failed to remove message")
		}
		mb.Unlock()
	}
	fs.removeOrphanRaws(rec)
	fs.removeSpool(rec.Key)
}

// removeOrphanRaws removes raw files belonging to the attempt only when they are not referenced
// by a mailbox index; indexed content is never touched.
func (fs *Store) removeOrphanRaws(rec *spoolRecord) {
	_ = os.RemoveAll(filepath.Join(fs.spoolPath, rec.Key))
	for _, t := range rec.Targets {
		mb := fs.mbox(t.Mailbox)
		p := filepath.Join(mb.path, t.ID+".raw")
		if _, err := os.Stat(p); err != nil {
			continue
		}
		mb.RLock()
		indexed := mb.indexedAttempt(t.ID, rec.Key)
		mb.RUnlock()
		if !indexed {
			_ = os.Remove(p)
		}
	}
}

// removeSpool deletes the attempt stage directory and journal.
func (fs *Store) removeSpool(key string) {
	if err := os.RemoveAll(filepath.Join(fs.spoolPath, key)); err != nil {
		log.Warn().Str("module", "storage").Str("attempt", key).Err(err).
			Msg("Failed to remove spool directory")
	}
	if err := os.Remove(filepath.Join(fs.spoolPath, key+journalExt)); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		log.Warn().Str("module", "storage").Str("attempt", key).Err(err).
			Msg("Failed to remove spool journal")
	}
}
