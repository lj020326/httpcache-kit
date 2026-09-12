package httpcache

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"net/http"
	"net/textproto"
	"os"
	pathutil "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/soulteary/vfs-kit"
)

// hash64Pool reuses FNV-1a 64-bit hashers to reduce allocations in hashKey.
var hash64Pool = sync.Pool{
	New: func() interface{} { return fnv.New64a() },
}

const (
	headerPrefix = "header/"
	bodyPrefix   = "body/"
	formatPrefix = "v1/"

	// storedAtPreamble persists the cache's full-precision local write or
	// validation time before the serialized HTTP response. Keeping metadata
	// outside the response header namespace means no origin field can collide
	// with it.
	storedAtPreamble = "HTTPCACHE/1 "
)

// Returned when a resource doesn't exist
var ErrNotFoundInCache = errors.New("not found in cache")

type Cache interface {
	Header(key string) (Header, error)
	Store(res *Resource, keys ...string) error
	Retrieve(key string) (*Resource, error)
	Invalidate(keys ...string)
	Freshen(res *Resource, keys ...string) error
}

// ExtendedCache extends Cache with management capabilities
type ExtendedCache interface {
	Cache
	// Stats returns current cache statistics
	Stats() CacheStats
	// Cleanup runs a manual cleanup cycle
	Cleanup() CleanupResult
	// Purge removes all cached items
	Purge() error
	// Close stops the cache and cleanup goroutines
	Close() error
}

// CacheStats holds cache statistics
type CacheStats struct {
	// TotalSize is the total size of cached items in bytes
	TotalSize int64
	// ItemCount is the number of cached items
	ItemCount int
	// StaleCount is the number of stale map entries
	StaleCount int
	// HitCount is the number of cache hits
	HitCount int64
	// MissCount is the number of cache misses
	MissCount int64
}

// CleanupResult holds the result of a cleanup operation
type CleanupResult struct {
	// RemovedItems is the number of items removed
	RemovedItems int
	// RemovedBytes is the number of bytes freed
	RemovedBytes int64
	// RemovedStaleEntries is the number of stale map entries removed
	RemovedStaleEntries int
	// Duration is how long the cleanup took
	Duration time.Duration
}

// cacheEntry tracks metadata for a cached item
type cacheEntry struct {
	key        string
	hashedKey  string
	size       int64
	storedAt   time.Time
	accessedAt time.Time
	element    *list.Element // for LRU tracking
}

// cache provides a storage mechanism for cached Resources
type cache struct {
	fs     vfs.VFS
	config *CacheConfig
	// diskRoot is set only by NewDiskCacheWithConfig. It lets the marker
	// snapshot use os.Rename, which the deliberately small vfs.VFS interface
	// does not expose; custom VFS backends use the two-slot journal instead.
	diskRoot string

	// generationMu orders complete stores/freshens against the FINAL timestamp
	// of an invalidation. Invalidate first installs a visible barrier marker,
	// then waits here for older writers before replacing it with the timestamp
	// that permanently judges their stored generations.
	generationMu sync.RWMutex

	// stale map with mutex protection
	stale           map[string]time.Time
	staleGeneration uint64
	staleMutex      sync.RWMutex

	// LRU tracking
	lruList   *list.List             // front = most recently used
	lruIndex  map[string]*cacheEntry // hashedKey -> entry
	lruMutex  sync.RWMutex
	totalSize int64

	// Statistics
	hitCount  int64
	missCount int64
	statMutex sync.RWMutex

	// Cleanup control
	stopChan  chan struct{}
	stopped   bool
	closeOnce sync.Once
}

var _ Cache = (*cache)(nil)
var _ ExtendedCache = (*cache)(nil)

type Header struct {
	http.Header
	StatusCode int
}

// NewVFSCache returns a cache backend off the provided VFS
func NewVFSCache(fs vfs.VFS) Cache {
	return NewVFSCacheWithConfig(fs, nil)
}

// NewVFSCacheWithConfig returns a cache backend with custom configuration
func NewVFSCacheWithConfig(fs vfs.VFS, config *CacheConfig) ExtendedCache {
	return newVFSCacheWithConfig(fs, config, "")
}

func newVFSCacheWithConfig(fs vfs.VFS, config *CacheConfig, diskRoot string) ExtendedCache {
	if config == nil {
		config = DefaultCacheConfig()
	}
	config.Validate()

	c := &cache{
		fs:       fs,
		config:   config,
		diskRoot: diskRoot,
		stale:    make(map[string]time.Time),
		lruList:  list.New(),
		lruIndex: make(map[string]*cacheEntry),
		stopChan: make(chan struct{}),
	}

	// A caller may supply a persistent or deliberately reused VFS. Restore it
	// here rather than only in NewDiskCacheWithConfig; otherwise its body and
	// header files survive reconstruction while the invalidation markers do
	// not, republishing pre-mutation entries as fresh.
	if err := c.scanExistingCache(); err != nil {
		debugf("warning: failed to scan existing cache: %v", err)
	}
	c.loadStale()

	// Start cleanup goroutine if interval is configured
	if config.CleanupInterval > 0 {
		go c.cleanupLoop()
	}

	return c
}

// NewMemoryCache returns an ephemeral cache in memory
func NewMemoryCache() Cache {
	return NewVFSCache(vfs.Memory())
}

// NewMemoryCacheWithConfig returns an ephemeral cache with custom configuration
func NewMemoryCacheWithConfig(config *CacheConfig) ExtendedCache {
	return NewVFSCacheWithConfig(vfs.Memory(), config)
}

// NewDiskCache returns a disk-backed cache
func NewDiskCache(dir string) (Cache, error) {
	return NewDiskCacheWithConfig(dir, nil)
}

// NewDiskCacheWithConfig returns a disk-backed cache with custom configuration
func NewDiskCacheWithConfig(dir string, config *CacheConfig) (ExtendedCache, error) {
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0750); err != nil {
		return nil, err
	}
	fs, err := vfs.FS(root)
	if err != nil {
		return nil, err
	}
	chfs, err := vfs.Chroot("/", fs)
	if err != nil {
		return nil, err
	}
	// Supply the OS root before restoration and the cleanup goroutine start,
	// so every disk marker write can use atomic replacement without a race.
	return newVFSCacheWithConfig(chfs, config, root), nil
}

// scanExistingCache scans the cache directory for existing cached files
// and rebuilds the LRU index. Constructors call it for any potentially
// persistent VFS.
func (c *cache) scanExistingCache() error {
	start := Clock()
	scannedFiles := 0
	var totalSize int64

	// Scan body files (they contain the actual cached data)
	bodyDir := bodyPrefix + formatPrefix
	if err := c.scanDirectory(bodyDir, func(hashedKey string, info os.FileInfo) {
		// Create entry for this cached item
		storedAt := info.ModTime()
		entry := &cacheEntry{
			key:        hashedKey, // We don't have the original key, use hashed key
			hashedKey:  hashedKey,
			size:       info.Size(),
			storedAt:   storedAt,
			accessedAt: info.ModTime(),
		}

		// Check if corresponding header file exists and add its size
		headerPath := headerPrefix + formatPrefix + hashedKey
		if headerInfo, err := c.fs.Stat(headerPath); err == nil {
			entry.size += headerInfo.Size()
			// Freshen does not rewrite the body, so its mtime cannot carry the
			// new validation generation across a restart. The header is written
			// on both Store and Freshen, making its mtime the safe old-format
			// fallback; a new-format preamble supplies the exact timestamp.
			entry.storedAt = headerInfo.ModTime()
			entry.accessedAt = headerInfo.ModTime()
			if _, persistedAt, readErr := c.readHeaderFile(headerPath, hashedKey); readErr == nil && !persistedAt.IsZero() {
				entry.storedAt = persistedAt
				entry.accessedAt = persistedAt
			}
		}

		// Add to LRU tracking
		c.lruMutex.Lock()
		entry.element = c.lruList.PushBack(entry) // Push to back (oldest first for startup)
		c.lruIndex[hashedKey] = entry
		c.totalSize += entry.size
		c.lruMutex.Unlock()

		scannedFiles++
		totalSize += entry.size
	}); err != nil {
		return err
	}

	duration := Clock().Sub(start)
	if scannedFiles > 0 {
		debugf("scanned %d existing cache files (%d bytes) in %s",
			scannedFiles, totalSize, duration)
	}

	return nil
}

// scanDirectory scans a directory for cached files and calls the callback for each
func (c *cache) scanDirectory(dir string, callback func(hashedKey string, info os.FileInfo)) error {
	files, err := c.fs.ReadDir(dir)
	if err != nil {
		if vfs.IsNotExist(err) {
			return nil // Directory doesn't exist yet, that's fine
		}
		return err
	}

	for _, info := range files {
		if info.IsDir() {
			continue
		}
		// The filename is the hashed key
		hashedKey := info.Name()
		callback(hashedKey, info)
	}

	return nil
}

func (c *cache) vfsWrite(path string, r io.Reader) (int64, error) {
	if err := vfs.MkdirAll(c.fs, pathutil.Dir(path), 0700); err != nil {
		return 0, fmt.Errorf("failed to create cache directory for %q: %w", path, err)
	}
	f, err := c.fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return 0, fmt.Errorf("failed to open cache file %q: %w", path, err)
	}
	n, err := io.Copy(f, r)
	if err != nil {
		_ = f.Close()
		return 0, fmt.Errorf("failed to write cache file %q: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return 0, fmt.Errorf("failed to close cache file %q: %w", path, err)
	}
	return n, nil
}

// atomicWriteFile writes a complete sibling temporary file before replacing
// path. A failed copy, sync, or close therefore leaves the last valid target
// untouched; os.Rename makes the final same-filesystem replacement atomic.
func atomicWriteFile(path string, r io.Reader) (int64, error) {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return 0, fmt.Errorf("failed to create temporary cache file for %q: %w", path, err)
	}
	tmpPath := f.Name()
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
		_ = os.Remove(tmpPath)
	}()

	n, err := io.Copy(f, r)
	if err != nil {
		return 0, fmt.Errorf("failed to write temporary cache file for %q: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync temporary cache file for %q: %w", path, err)
	}
	closeErr := f.Close()
	closed = true
	if closeErr != nil {
		return 0, fmt.Errorf("failed to close temporary cache file for %q: %w", path, closeErr)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return 0, fmt.Errorf("failed to replace cache file %q: %w", path, err)
	}
	return n, nil
}

// Retrieve the Status and Headers for a given key path
func (c *cache) Header(key string) (Header, error) {
	path := headerPrefix + formatPrefix + hashKey(key)
	h, _, err := c.readHeaderFile(path, key)
	return h, err
}

// readHeaderFile reads a stored response header and its separate cache
// metadata preamble.
func (c *cache) readHeaderFile(path, key string) (Header, time.Time, error) {
	f, err := c.fs.Open(path)
	if err != nil {
		if vfs.IsNotExist(err) {
			return Header{}, time.Time{}, ErrNotFoundInCache
		}
		return Header{}, time.Time{}, fmt.Errorf("failed to open header file %q for key %q: %w", path, key, err)
	}
	defer func() { _ = f.Close() }()

	h, storedAt, err := readStoredHeaders(bufio.NewReader(f))
	if err != nil {
		return Header{}, time.Time{}, fmt.Errorf("failed to read headers from %q for key %q: %w", path, key, err)
	}

	return h, storedAt, nil
}

// Store a resource against a number of keys. Resource bodies are streamed
// directly into the backing VFS. For multiple keys (for example a primary and
// Vary key), the seekable resource is rewound instead of being copied into an
// unbounded in-memory buffer.
func (c *cache) Store(res *Resource, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	// Make the stale check and every write one ordered operation relative to
	// Invalidate. Checking a marker and then releasing the lock before body or
	// header I/O left a window in which a mutation could land, after which the
	// older response was still written and tracked as newer.
	c.generationMu.RLock()
	defer c.generationMu.RUnlock()

	// A multi-key store is all-or-nothing with respect to invalidation. Vary
	// responses contain the governing base key followed by a variant key. If
	// the base marker supersedes the response, continuing to the unmarked
	// variant publishes the same pre-mutation body under that second key.
	for _, key := range keys {
		if staleTime, marked := c.StaleAt(key); marked && !res.ReceivedAfter(staleTime) {
			debugf("not storing %q: response superseded by the invalidation at %s", keys, staleTime)
			return nil
		}
	}

	expectedSize, hasExpectedSize := int64(0), false
	if rawLength := res.Header().Get("Content-Length"); rawLength != "" {
		length, err := strconv.ParseInt(rawLength, 10, 64)
		if err == nil && length >= 0 {
			expectedSize, hasExpectedSize = length, true
		}
	}

	for _, key := range keys {
		if _, err := res.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to rewind resource for key %q: %w", key, err)
		}

		// The stale marker is deliberately LEFT IN PLACE.
		//
		// Retrieve already compares a stored response's Date against the
		// marker, so a response written after the invalidation supersedes it
		// without the marker having to be deleted. Deleting it here was
		// actively wrong twice over: a store that was already in flight when a
		// mutation completed erased the newer invalidation and republished the
		// pre-mutation response as fresh, and refetching ONE Vary variant
		// cleared the base marker that every OTHER variant is still judged
		// against. The cleanup goroutine expires markers on its own.
		hashedKey := hashKey(key)

		// If Content-Length is known, make room before the write. Unknown-length
		// streams are accounted for and evicted immediately after the write.
		if hasExpectedSize {
			c.evictIfNeeded(expectedSize)
		}

		var body io.Reader = res
		if hasExpectedSize {
			body = io.LimitReader(res, expectedSize)
		}
		written, err := c.storeBody(body, key)
		if err != nil {
			return err
		}
		if hasExpectedSize && written != expectedSize {
			bodyPath := bodyPrefix + formatPrefix + hashedKey
			_ = c.fs.Remove(bodyPath)
			return fmt.Errorf("resource body for key %q was %d bytes, expected %d", key, written, expectedSize)
		}
		if !hasExpectedSize {
			c.evictIfNeeded(written)
		}

		storedAt := Clock()
		headerBytes, err := c.storeHeader(res.Status(), res.Header(), key, storedAt)
		if err != nil {
			return err
		}

		// Update LRU tracking
		c.trackEntry(key, hashedKey, written+headerBytes, storedAt)
	}

	return nil
}

func (c *cache) storeBody(r io.Reader, key string) (int64, error) {
	n, err := c.vfsWrite(bodyPrefix+formatPrefix+hashKey(key), r)
	if err != nil {
		return 0, fmt.Errorf("failed to store body for key %q: %w", key, err)
	}
	return n, nil
}

func (c *cache) storeHeader(code int, h http.Header, key string, storedAt time.Time) (int64, error) {
	hb := &bytes.Buffer{}
	fmt.Fprintf(hb, "%s%s\r\n", storedAtPreamble, storedAt.UTC().Format(time.RFC3339Nano))
	fmt.Fprintf(hb, "HTTP/1.1 %d %s\r\n", code, http.StatusText(code))
	if err := headersToWriter(h, hb); err != nil {
		return 0, fmt.Errorf("failed to serialize headers for key %q: %w", key, err)
	}
	n, err := c.vfsWrite(headerPrefix+formatPrefix+hashKey(key), bytes.NewReader(hb.Bytes()))
	if err != nil {
		return 0, fmt.Errorf("failed to store header for key %q: %w", key, err)
	}
	return n, nil
}

// Retrieve returns a cached Resource for the given key
func (c *cache) Retrieve(key string) (*Resource, error) {
	hashedKey := hashKey(key)
	bodyPath := bodyPrefix + formatPrefix + hashedKey
	f, err := c.fs.Open(bodyPath)
	if err != nil {
		if vfs.IsNotExist(err) {
			c.recordMiss()
			return nil, ErrNotFoundInCache
		}
		return nil, fmt.Errorf("failed to open body file %q for key %q: %w", bodyPath, key, err)
	}
	h, persistedAt, err := c.readHeaderFile(headerPrefix+formatPrefix+hashedKey, key)
	if err != nil {
		_ = f.Close()
		if err == ErrNotFoundInCache {
			c.recordMiss()
			return nil, ErrNotFoundInCache
		}
		return nil, fmt.Errorf("failed to retrieve header for key %q: %w", key, err)
	}
	res := NewResource(h.StatusCode, f, h.Header)
	if persistedAt.IsZero() {
		persistedAt = c.entryStoredAt(hashedKey)
	}
	res.SetStoredAt(persistedAt)

	// Check stale map with proper locking
	c.staleMutex.RLock()
	staleTime, exists := c.stale[key]
	c.staleMutex.RUnlock()

	if exists {
		if !res.StoredAfter(staleTime) {
			debugf("stale marker of %s found", staleTime)
			res.MarkStale()
		}
	}

	// Update LRU access time
	c.touchEntry(hashedKey)
	c.recordHit()

	return res, nil
}

func (c *cache) Invalidate(keys ...string) {
	debugf("invalidating %q", keys)
	if len(keys) == 0 {
		return
	}

	// Publish and persist an immediately visible barrier BEFORE waiting for
	// active stores. Retrieve does not take generationMu, so acquiring its write
	// side first left a successful mutation invisible for the entire duration of
	// an unrelated slow cache write. The far-future value conservatively marks
	// every currently writable generation stale; if the process dies while
	// waiting, loadStale recovers it to the restart time, after all writes that
	// could have survived that process.
	c.staleMutex.Lock()
	for _, key := range keys {
		c.stale[key] = invalidationBarrierTime
	}
	c.persistStale(c.snapshotStaleLocked())
	c.staleMutex.Unlock()

	// Existing stores may already have passed their stale check. Wait until
	// their stored timestamps are fixed, while the barrier keeps lookups from
	// treating those later file writes as post-mutation responses.
	c.generationMu.Lock()
	defer c.generationMu.Unlock()

	c.staleMutex.Lock()
	defer c.staleMutex.Unlock()
	now := Clock()
	for _, key := range keys {
		c.stale[key] = now
	}
	snapshot := c.snapshotStaleLocked()

	// Written through to the backing store, so a restart does not lose it.
	//
	// The markers are the ONLY record that the entries still on disk predate
	// a mutation, and scanExistingCache restores those entries. Keeping the
	// map in memory alone meant every deploy or crash republished the
	// pre-mutation representation -- Vary variants included -- as a fresh HIT.
	// Keep staleMutex held through persistence. Otherwise two invalidations
	// can snapshot in the right order but finish their writes in the opposite
	// order, allowing the older snapshot to erase the newer marker (or two
	// O_TRUNC writes to corrupt the file).
	c.persistStale(snapshot)
}

// staleMapPath is the atomic OS-backed snapshot and the legacy generic-VFS
// snapshot path. The other entries live under hashed-key prefixes, so it
// cannot collide with one.
const staleMapPath = "stale-markers.json"

// invalidationBarrierTime is persisted while Invalidate waits for writers
// that were already active. It must be JSON/RFC3339 representable.
var invalidationBarrierTime = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)

// staleSnapshot is the bounded two-slot journal used when all that is known
// about a caller-provided VFS is the small vfs.VFS interface. That interface
// has no Rename or Sync operation, so rewriting one snapshot in place cannot
// be made failure-safe. Each generation overwrites the older slot while the
// other slot remains intact; startup selects the highest complete generation.
type staleSnapshot struct {
	Generation uint64               `json:"generation"`
	Markers    map[string]time.Time `json:"markers"`
}

func staleMapSlotPath(generation uint64) string {
	return "stale-markers." + strconv.FormatUint(generation%2, 10) + ".json"
}

// snapshotStaleLocked copies the marker map. Callers hold staleMutex.
func (c *cache) snapshotStaleLocked() map[string]time.Time {
	out := make(map[string]time.Time, len(c.stale))
	for k, v := range c.stale {
		out[k] = v
	}
	return out
}

// persistStale writes the markers to the backing store. Callers hold
// staleMutex across this call, which serialises complete snapshots through
// their file write instead of merely serialising map access.
//
// Failures are logged, not returned: the in-memory markers are already
// correct, so the mutation this accompanies has still been honoured for the
// life of the process. What is lost is only the restart guarantee.
func (c *cache) persistStale(snapshot map[string]time.Time) {
	persisted := interface{}(snapshot)
	nextGeneration := c.staleGeneration
	if c.diskRoot == "" {
		nextGeneration++
		persisted = staleSnapshot{Generation: nextGeneration, Markers: snapshot}
	}

	encoded, err := json.Marshal(persisted)
	if err != nil {
		debugf("failed to encode invalidation markers: %v", err)
		return
	}
	var writeErr error
	if c.diskRoot != "" {
		_, writeErr = atomicWriteFile(filepath.Join(c.diskRoot, filepath.FromSlash(staleMapPath)), bytes.NewReader(encoded))
	} else {
		_, writeErr = c.vfsWrite(staleMapSlotPath(nextGeneration), bytes.NewReader(encoded))
	}
	if writeErr != nil {
		debugf("failed to persist invalidation markers: %v", writeErr)
		return
	}
	if c.diskRoot == "" {
		c.staleGeneration = nextGeneration
	}
}

// loadStale restores markers written by a previous process.
func (c *cache) loadStale() {
	// Caller-provided persistent VFS backends use alternating snapshots. A
	// malformed newest slot (for example after a short write) is ignored and
	// the other complete generation remains authoritative.
	if c.diskRoot == "" {
		var best staleSnapshot
		found := false
		for slot := uint64(0); slot < 2; slot++ {
			var candidate staleSnapshot
			if err := c.readStaleJSON(staleMapSlotPath(slot), &candidate); err != nil {
				if !vfs.IsNotExist(err) {
					debugf("failed to read invalidation marker slot %d: %v", slot, err)
				}
				continue
			}
			if candidate.Generation == 0 || candidate.Markers == nil {
				debugf("ignoring invalid invalidation marker slot %d", slot)
				continue
			}
			if !found || candidate.Generation > best.Generation {
				best = candidate
				found = true
			}
		}
		if found {
			c.staleGeneration = best.Generation
			if c.restoreStale(best.Markers) {
				c.persistRestoredStale()
			}
			return
		}
	}

	// Legacy generic-VFS snapshots and current OS-backed snapshots use the raw
	// marker map. Once a generic VFS writes a journal slot, the legacy file is
	// deliberately ignored so an older snapshot cannot resurrect swept keys.
	var restored map[string]time.Time
	if err := c.readStaleJSON(staleMapPath, &restored); err != nil {
		if !vfs.IsNotExist(err) {
			debugf("failed to read invalidation markers: %v", err)
		}
		return
	}
	if c.restoreStale(restored) {
		c.persistRestoredStale()
	}
}

func (c *cache) readStaleJSON(path string, dst interface{}) error {
	f, err := c.fs.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return json.NewDecoder(f).Decode(dst)
}

// restoreStale merges a snapshot and returns whether it recovered an
// invalidation that was interrupted while waiting for active writers.
func (c *cache) restoreStale(restored map[string]time.Time) bool {
	recovered := false
	recoveredAt := Clock()
	c.staleMutex.Lock()
	defer c.staleMutex.Unlock()
	for key, at := range restored {
		if at.Equal(invalidationBarrierTime) {
			at = recoveredAt
			recovered = true
		}
		// Never overwrite a marker this process has already written: it is
		// newer than anything on disk.
		if _, ok := c.stale[key]; !ok {
			c.stale[key] = at
		}
	}
	debugf("restored %d invalidation marker(s)", len(restored))
	return recovered
}

// persistRestoredStale replaces a recovered barrier with the restart time.
// No writer from the previous process can still be active, so that time is a
// final upper bound for every cache file that survived it.
func (c *cache) persistRestoredStale() {
	c.staleMutex.Lock()
	defer c.staleMutex.Unlock()
	c.persistStale(c.snapshotStaleLocked())
}

// StaleAt returns when key was invalidated, if it was.
//
// It lets the handler judge a Vary VARIANT against the base key's marker:
// variants live under their own keys, which invalidation cannot enumerate, so
// without this each one would have to be reached through a base entry that a
// single refetch makes fresh again.
func (c *cache) StaleAt(key string) (time.Time, bool) {
	c.staleMutex.RLock()
	defer c.staleMutex.RUnlock()
	t, ok := c.stale[key]
	return t, ok
}

func (c *cache) Freshen(res *Resource, keys ...string) error {
	var invalidate []string
	err := func() error {
		c.generationMu.RLock()
		defer c.generationMu.RUnlock()

		// Validation that started before a mutation must not freshen the old
		// body after that mutation. Validator records the precise local request
		// time on the Resource; older resources fall back to HTTP timestamps.
		for _, key := range keys {
			if staleTime, marked := c.StaleAt(key); marked && !res.ReceivedAfter(staleTime) {
				return fmt.Errorf("validation for key %q was superseded by invalidation at %s", key, staleTime)
			}
		}

		for _, key := range keys {
			if h, headerErr := c.Header(key); headerErr == nil {
				if h.StatusCode == res.Status() && headersEqual(h.Header, res.Header()) {
					debugf("freshening key %s", key)
					freshenedAt := Clock()
					if _, writeErr := c.storeHeader(h.StatusCode, res.Header(), key, freshenedAt); writeErr != nil {
						return fmt.Errorf("failed to freshen header for key %q: %w", key, writeErr)
					}
					// The entry has just been validated against the origin, so
					// its full-precision generation is updated in memory and in
					// the stored header used after a disk-cache restart.
					c.markFreshened(hashKey(key), freshenedAt)
				} else {
					debugf("freshen failed, invalidating %s", key)
					invalidate = append(invalidate, key)
				}
			}
		}
		return nil
	}()
	if err != nil {
		return err
	}

	// Do not try to upgrade generationMu from a read lock inside the loop.
	// Invalidate takes its write side, so mismatched entries are handled only
	// after the ordered freshen operation has completed.
	if len(invalidate) > 0 {
		c.Invalidate(invalidate...)
	}
	return nil
}

func hashKey(key string) string {
	h := hash64Pool.Get().(hash.Hash64)
	defer func() {
		h.Reset()
		hash64Pool.Put(h)
	}()
	if _, err := h.Write([]byte(key)); err != nil {
		return "unable-to-calculate"
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func readStoredHeaders(r *bufio.Reader) (Header, time.Time, error) {
	tp := textproto.NewReader(r)
	line, err := tp.ReadLine()
	if err != nil {
		return Header{}, time.Time{}, err
	}

	var storedAt time.Time
	if strings.HasPrefix(line, storedAtPreamble) {
		storedAt, err = time.Parse(time.RFC3339Nano, strings.TrimPrefix(line, storedAtPreamble))
		if err != nil {
			return Header{}, time.Time{}, fmt.Errorf("malformed cache timestamp: %w", err)
		}
		line, err = tp.ReadLine()
		if err != nil {
			return Header{}, time.Time{}, err
		}
	}

	f := strings.SplitN(line, " ", 3)
	if len(f) < 2 {
		return Header{}, time.Time{}, fmt.Errorf("malformed HTTP response: %s", line)
	}
	statusCode, err := strconv.Atoi(f[1])
	if err != nil {
		return Header{}, time.Time{}, fmt.Errorf("malformed HTTP status code: %s", f[1])
	}

	mimeHeader, err := tp.ReadMIMEHeader()
	if err != nil {
		return Header{}, time.Time{}, err
	}
	return Header{StatusCode: statusCode, Header: http.Header(mimeHeader)}, storedAt, nil
}

func readHeaders(r *bufio.Reader) (Header, error) {
	h, _, err := readStoredHeaders(r)
	return h, err
}

func headersToWriter(h http.Header, w io.Writer) error {
	if err := h.Write(w); err != nil {
		return err
	}
	// ReadMIMEHeader expects a trailing newline
	_, err := w.Write([]byte("\r\n"))
	return err
}

// LRU tracking methods

// trackEntry adds or updates an entry in the LRU index
func (c *cache) trackEntry(key, hashedKey string, size int64, storedAt time.Time) {
	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	if storedAt.IsZero() {
		storedAt = Clock()
	}

	// Check if entry already exists
	if entry, exists := c.lruIndex[hashedKey]; exists {
		// Update existing entry
		c.totalSize -= entry.size
		entry.size = size
		entry.storedAt = storedAt
		entry.accessedAt = storedAt
		c.totalSize += size
		// Move to front of LRU list
		c.lruList.MoveToFront(entry.element)
	} else {
		// Create new entry
		entry := &cacheEntry{
			key:        key,
			hashedKey:  hashedKey,
			size:       size,
			storedAt:   storedAt,
			accessedAt: storedAt,
		}
		entry.element = c.lruList.PushFront(entry)
		c.lruIndex[hashedKey] = entry
		c.totalSize += size
	}
}

// entryStoredAt reports when this cache last WROTE the entry, at full
// precision. Zero when the entry is not tracked.
func (c *cache) entryStoredAt(hashedKey string) time.Time {
	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	if entry, exists := c.lruIndex[hashedKey]; exists {
		return entry.storedAt
	}
	return time.Time{}
}

// markFreshened records that the entry was just validated against the origin.
//
// Only the timestamps: the size is unchanged by a freshen, and writing the
// header's length over it would corrupt the total the size cap is enforced
// against.
func (c *cache) markFreshened(hashedKey string, freshenedAt time.Time) {
	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	if entry, exists := c.lruIndex[hashedKey]; exists {
		entry.storedAt = freshenedAt
		entry.accessedAt = freshenedAt
		c.lruList.MoveToFront(entry.element)
	}
}

// touchEntry updates the access time and moves entry to front of LRU list
func (c *cache) touchEntry(hashedKey string) {
	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	if entry, exists := c.lruIndex[hashedKey]; exists {
		entry.accessedAt = Clock()
		c.lruList.MoveToFront(entry.element)
	}
}

// removeEntry removes an entry from the cache and LRU tracking
func (c *cache) removeEntry(entry *cacheEntry) error {
	// Remove from filesystem
	bodyPath := bodyPrefix + formatPrefix + entry.hashedKey
	headerPath := headerPrefix + formatPrefix + entry.hashedKey

	if err := c.fs.Remove(bodyPath); err != nil && !vfs.IsNotExist(err) {
		debugf("failed to remove body file %s: %v", bodyPath, err)
	}
	if err := c.fs.Remove(headerPath); err != nil && !vfs.IsNotExist(err) {
		debugf("failed to remove header file %s: %v", headerPath, err)
	}

	// Remove from LRU tracking (assumes lruMutex is already held)
	c.lruList.Remove(entry.element)
	delete(c.lruIndex, entry.hashedKey)
	c.totalSize -= entry.size

	// The invalidation marker is deliberately NOT removed with the entry.
	//
	// It governs more than this entry: a base key's marker is what every Vary
	// VARIANT of that resource is judged against, and variants are tracked and
	// evicted independently. Dropping it when LRU eviction happened to take
	// the base entry left the surviving pre-mutation variants with nothing
	// marking them stale, so a refetch could publish a new base and a
	// concurrent lookup reach an old variant through it as a fresh HIT.
	//
	// cleanupStaleMap frees markers by AGE instead -- see staleRetention --
	// which is the only bound that knows how long an entry it governs can
	// still be around.

	return nil
}

// evictIfNeeded evicts the least recently used items if cache size exceeds limit
func (c *cache) evictIfNeeded(additionalSize int64) {
	if c.config.MaxSize <= 0 {
		return
	}

	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	targetSize := c.config.MaxSize - additionalSize
	if targetSize < 0 {
		targetSize = 0
	}

	// Evict from the back (least recently used)
	for c.totalSize > targetSize && c.lruList.Len() > 0 {
		elem := c.lruList.Back()
		if elem == nil {
			break
		}
		entry := elem.Value.(*cacheEntry)
		debugf("evicting LRU entry: %s (size: %d, accessed: %s)",
			entry.key, entry.size, entry.accessedAt.Format(time.RFC3339))
		_ = c.removeEntry(entry)

		// Record eviction metric
		if getDefaultMetrics() != nil {
			getDefaultMetrics().RecordCacheEviction("lru")
		}
	}
}

// Cleanup methods

// cleanupLoop runs periodic cleanup
func (c *cache) cleanupLoop() {
	ticker := time.NewTicker(c.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			result := c.Cleanup()
			if result.RemovedItems > 0 || result.RemovedStaleEntries > 0 {
				debugf("cleanup completed: removed %d items (%d bytes), %d stale entries in %s",
					result.RemovedItems, result.RemovedBytes, result.RemovedStaleEntries, result.Duration)
			}
		case <-c.stopChan:
			return
		}
	}
}

// Cleanup runs a manual cleanup cycle
func (c *cache) Cleanup() CleanupResult {
	start := Clock()
	result := CleanupResult{}

	// Clean up stale map entries
	result.RemovedStaleEntries = c.cleanupStaleMap()

	// Clean up TTL-expired entries
	if c.config.TTL > 0 {
		removed, bytes := c.cleanupTTLExpired()
		result.RemovedItems += removed
		result.RemovedBytes += bytes
	}

	// Enforce size limit
	if c.config.MaxSize > 0 {
		removed, bytes := c.enforceMaxSize()
		result.RemovedItems += removed
		result.RemovedBytes += bytes
	}

	result.Duration = Clock().Sub(start)

	// Record cleanup duration metric
	if getDefaultMetrics() != nil {
		getDefaultMetrics().RecordCleanupDuration(result.Duration.Seconds())
		// Update cache stats metrics
		stats := c.Stats()
		getDefaultMetrics().UpdateCacheStats(stats)
	}

	return result
}

// staleRetention is how long an invalidation marker has to be kept.
//
// The marker is the ONLY record that entries older than it are pre-mutation,
// so dropping it while such an entry survives republishes that entry as a HIT.
// removeEntry already drops a marker when its entry is evicted; this covers
// the age-based sweep, which knows nothing about what is still stored.
//
// StaleMapTTL alone did not: its 24 hour default against the 7 day cache TTL
// left six days in which an infrequently requested representation -- a Vary
// variant especially, since those are judged against the BASE key's marker and
// may never be fetched in the meantime -- came back as fresh pre-mutation
// content.
//
// With TTL disabled an entry can survive indefinitely, so markers are then
// only removed with their entry, never by age. That trades unbounded growth of
// one time.Time per invalidated key against serving content a mutation
// already replaced.
func (c *cache) staleRetention() time.Duration {
	if c.config.TTL <= 0 {
		return 0
	}
	return max(c.config.StaleMapTTL, c.config.TTL)
}

// cleanupStaleMap removes old stale map entries
func (c *cache) cleanupStaleMap() int {
	retention := c.staleRetention()
	if retention <= 0 {
		return 0
	}

	c.staleMutex.Lock()
	defer c.staleMutex.Unlock()

	cutoff := Clock().Add(-retention)
	removed := 0

	for key, staleTime := range c.stale {
		if staleTime.Before(cutoff) {
			delete(c.stale, key)
			removed++
		}
	}

	if removed > 0 {
		// Written through, like Invalidate: otherwise a restart would restore
		// markers this sweep has just decided are no longer needed. Persistence
		// happens while staleMutex is held, so a newer snapshot cannot be
		// overtaken by this older one.
		snapshot := c.snapshotStaleLocked()
		c.persistStale(snapshot)
	}

	return removed
}

// cleanupTTLExpired removes items that have exceeded their TTL
func (c *cache) cleanupTTLExpired() (int, int64) {
	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	cutoff := Clock().Add(-c.config.TTL)
	removed := 0
	var bytesRemoved int64

	// Iterate from back (oldest) to front
	for elem := c.lruList.Back(); elem != nil; {
		entry := elem.Value.(*cacheEntry)
		prev := elem.Prev()

		if entry.storedAt.Before(cutoff) {
			debugf("removing TTL-expired entry: %s (stored: %s)",
				entry.key, entry.storedAt.Format(time.RFC3339))
			bytesRemoved += entry.size
			_ = c.removeEntry(entry)
			removed++

			// Record eviction metric
			if getDefaultMetrics() != nil {
				getDefaultMetrics().RecordCacheEviction("ttl")
			}
		}

		elem = prev
	}

	return removed, bytesRemoved
}

// enforceMaxSize ensures cache doesn't exceed max size
func (c *cache) enforceMaxSize() (int, int64) {
	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	removed := 0
	var bytesRemoved int64

	for c.totalSize > c.config.MaxSize && c.lruList.Len() > 0 {
		elem := c.lruList.Back()
		if elem == nil {
			break
		}
		entry := elem.Value.(*cacheEntry)
		debugf("enforcing max size, removing: %s (size: %d)",
			entry.key, entry.size)
		bytesRemoved += entry.size
		_ = c.removeEntry(entry)
		removed++

		// Record eviction metric
		if getDefaultMetrics() != nil {
			getDefaultMetrics().RecordCacheEviction("size_limit")
		}
	}

	return removed, bytesRemoved
}

// Statistics methods

func (c *cache) recordHit() {
	c.statMutex.Lock()
	c.hitCount++
	c.statMutex.Unlock()
}

func (c *cache) recordMiss() {
	c.statMutex.Lock()
	c.missCount++
	c.statMutex.Unlock()
}

// Stats returns current cache statistics
func (c *cache) Stats() CacheStats {
	c.lruMutex.RLock()
	totalSize := c.totalSize
	itemCount := c.lruList.Len()
	c.lruMutex.RUnlock()

	c.staleMutex.RLock()
	staleCount := len(c.stale)
	c.staleMutex.RUnlock()

	c.statMutex.RLock()
	hitCount := c.hitCount
	missCount := c.missCount
	c.statMutex.RUnlock()

	return CacheStats{
		TotalSize:  totalSize,
		ItemCount:  itemCount,
		StaleCount: staleCount,
		HitCount:   hitCount,
		MissCount:  missCount,
	}
}

// Purge removes all cached items
func (c *cache) Purge() error {
	c.generationMu.Lock()
	defer c.generationMu.Unlock()

	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	// Remove all entries
	for elem := c.lruList.Front(); elem != nil; {
		entry := elem.Value.(*cacheEntry)
		next := elem.Next()
		_ = c.removeEntry(entry)
		elem = next
	}

	// Clear the stale map in the backing store too. Persisting an empty newest
	// generation is safer than deleting journal slots one by one: a restart can
	// never select an older non-empty generation between those removals.
	c.staleMutex.Lock()
	defer c.staleMutex.Unlock()
	c.stale = make(map[string]time.Time)
	c.persistStale(c.snapshotStaleLocked())

	return nil
}

// Close stops the cache and cleanup goroutines.
// Safe to call multiple times; only the first call performs the shutdown.
func (c *cache) Close() error {
	c.closeOnce.Do(func() {
		c.stopped = true
		close(c.stopChan)
	})
	return nil
}
