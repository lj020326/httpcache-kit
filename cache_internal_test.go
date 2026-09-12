package httpcache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	metrics "github.com/soulteary/metrics-kit/v2"
	"github.com/soulteary/vfs-kit"
)

func TestCache_Header_OpenError(t *testing.T) {
	// VFS that returns error on Open (not IsNotExist) to cover non-IsNotExist path
	c := NewVFSCacheWithConfig(&openFailVFS{vfs.Memory()}, DefaultCacheConfig())
	_, err := c.Header("any-key")
	if err == nil {
		t.Error("expected error when Open fails")
	}
	if err == ErrNotFoundInCache {
		t.Error("expected non-ErrNotFoundInCache when Open returns permission error")
	}
}

type openFailVFS struct{ vfs.VFS }

func (o *openFailVFS) Open(name string) (vfs.RFile, error) {
	return nil, os.ErrPermission
}

// bodyOpenFailVFS fails Open only for paths under body/ so Store (OpenFile) works but Retrieve (Open body) fails.
type bodyOpenFailVFS struct{ vfs.VFS }

func (b *bodyOpenFailVFS) Open(name string) (vfs.RFile, error) {
	if strings.Contains(name, "body/") {
		return nil, os.ErrPermission
	}
	return b.VFS.Open(name)
}

// readDirFailVFS fails ReadDir for body/v1/ so scanExistingCache gets non-IsNotExist error from scanDirectory.
type readDirFailVFS struct{ vfs.VFS }

func (r *readDirFailVFS) ReadDir(path string) ([]os.FileInfo, error) {
	if strings.HasPrefix(path, "body/") {
		return nil, os.ErrPermission
	}
	return r.VFS.ReadDir(path)
}

// mkdirFailVFS fails Mkdir for path containing "body" so vfsWrite gets error from MkdirAll.
type mkdirFailVFS struct{ vfs.VFS }

func (m *mkdirFailVFS) Mkdir(path string, perm os.FileMode) error {
	if strings.Contains(path, "body") {
		return os.ErrPermission
	}
	return m.VFS.Mkdir(path, perm)
}

// openFileFailVFS fails OpenFile for paths under body/ so vfsWrite gets error after MkdirAll.
type openFileFailVFS struct{ vfs.VFS }

func (o *openFileFailVFS) OpenFile(path string, flag int, perm os.FileMode) (vfs.WFile, error) {
	if strings.HasPrefix(path, "body/") {
		return nil, os.ErrPermission
	}
	return o.VFS.OpenFile(path, flag, perm)
}

// removeFailVFS fails Remove for body/ and header/ so removeEntry hits debugf path.
type removeFailVFS struct{ vfs.VFS }

func (r *removeFailVFS) Remove(path string) error {
	if strings.HasPrefix(path, "body/") || strings.HasPrefix(path, "header/") {
		return os.ErrPermission
	}
	return r.VFS.Remove(path)
}

type selectiveRemoveFailVFS struct {
	vfs.VFS
	failHash string
}

func (r *selectiveRemoveFailVFS) Remove(path string) error {
	if r.failHash != "" && strings.HasSuffix(path, "/"+r.failHash) {
		return os.ErrPermission
	}
	return r.VFS.Remove(path)
}

func TestReadHeaders_Malformed(t *testing.T) {
	// Malformed status line: "HTTP/1.1" only one part
	r := bufio.NewReader(bytes.NewReader([]byte("HTTP/1.1\r\n\r\n")))
	_, err := readHeaders(r)
	if err == nil {
		t.Error("expected error for malformed status line")
	}
	// Malformed status code: non-numeric
	r2 := bufio.NewReader(bytes.NewReader([]byte("HTTP/1.1 abc OK\r\n\r\n")))
	_, err = readHeaders(r2)
	if err == nil {
		t.Error("expected error for non-numeric status code")
	}
	// Valid status line but ReadMIMEHeader can fail on malformed header body
	r3 := bufio.NewReader(bytes.NewReader([]byte("HTTP/1.1 200 OK\r\nX: \x00\r\n\r\n")))
	_, err = readHeaders(r3)
	if err != nil {
		t.Logf("ReadMIMEHeader error (expected for invalid header): %v", err)
	}
}

func TestHashKey_EmptyKey(t *testing.T) {
	s := hashKey("")
	if s == "" {
		t.Error("hashKey empty should not return empty string")
	}
	if s == "unable-to-calculate" {
		// FNV succeeds for empty input
		t.Logf("hashKey(\"\") = %s", s)
	}
}

func TestNewDiskCache(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	if cache == nil {
		t.Fatal("cache is nil")
	}
	if ext, ok := cache.(ExtendedCache); ok {
		_ = ext.Close()
	}
}

func TestNewDiskCacheWithConfig_InvalidPath(t *testing.T) {
	// Path that is an existing file (not directory) -> MkdirAll fails
	f, err := os.CreateTemp("", "httpcache-dir-test-*")
	if err != nil {
		t.Skip("CreateTemp failed:", err)
	}
	path := f.Name()
	_ = f.Close()
	defer func() { _ = os.Remove(path) }()
	_, err = NewDiskCacheWithConfig(path, DefaultCacheConfig())
	if err == nil {
		t.Error("expected error when path is existing file")
	}
}

func TestNewDiskCacheWithConfig_MkdirAllFails(t *testing.T) {
	// Path with invalid character -> MkdirAll may fail (platform-dependent)
	_, err := NewDiskCacheWithConfig(string([]byte{0}), DefaultCacheConfig())
	if err == nil {
		t.Log("MkdirAll with null byte did not fail on this platform")
		return
	}
}

func TestNewDiskCacheWithConfig_ScanDirFails(t *testing.T) {
	// When scanDirectory returns non-IsNotExist error, scanExistingCache returns it (debugf path in NewDiskCacheWithConfig)
	dir := t.TempDir()
	bodyDir := filepath.Join(dir, "body", "v1")
	if err := os.MkdirAll(bodyDir, 0750); err != nil {
		t.Fatal(err)
	}
	bodyParent := filepath.Join(dir, "body")
	if err := os.Chmod(bodyParent, 0000); err != nil {
		t.Skip("chmod 000 not supported or not runnable")
	}
	defer func() { _ = os.Chmod(bodyParent, 0750) }()
	cache, err := NewDiskCacheWithConfig(dir, DefaultCacheConfig())
	if err != nil {
		t.Fatalf("NewDiskCacheWithConfig: %v", err)
	}
	if cache == nil {
		t.Fatal("cache is nil")
	}
	_ = cache.Close()
}

func TestScanDirectory_NotExist(t *testing.T) {
	fs := vfs.Memory()
	c := NewVFSCacheWithConfig(fs, DefaultCacheConfig()).(*cache)
	defer func() { _ = c.Close() }()
	// Scan a directory that doesn't exist -> ReadDir returns error; if IsNotExist we return nil
	err := c.scanDirectory("body/v1/", func(k string, info os.FileInfo) {})
	if err != nil {
		t.Logf("scanDirectory (dir not exist): %v", err)
	}
}

func TestEnforceMaxSize(t *testing.T) {
	config := DefaultCacheConfig().
		WithMaxSize(800).
		WithCleanupInterval(0)
	cache := NewMemoryCacheWithConfig(config).(*cache)
	defer func() { _ = cache.Close() }()
	// Store several small items so total > MaxSize
	for i := 0; i < 5; i++ {
		body := strings.Repeat("x", 200)
		res := NewResourceBytes(200, []byte(body), http.Header{"Content-Length": []string{"200"}})
		if err := cache.Store(res, "key"+string(rune('0'+i))); err != nil {
			t.Fatal(err)
		}
	}
	// Run Cleanup to trigger enforceMaxSize
	result := cache.Cleanup()
	if result.RemovedItems == 0 && cache.Stats().TotalSize > config.MaxSize {
		t.Logf("enforceMaxSize may have run: removed=%d size=%d", result.RemovedItems, cache.Stats().TotalSize)
	}
}

// TestEnforceMaxSizeAfterScan opens a disk cache that has more data than new MaxSize so Cleanup runs enforceMaxSize.
func TestEnforceMaxSizeAfterScan(t *testing.T) {
	dir := t.TempDir()
	largeConfig := DefaultCacheConfig().WithMaxSize(10 * 1024 * 1024).WithCleanupInterval(0)
	c1, err := NewDiskCacheWithConfig(dir, largeConfig)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		body := strings.Repeat("x", 500)
		res := NewResourceBytes(200, []byte(body), http.Header{"Content-Length": []string{"500"}})
		if err := c1.Store(res, "key"+string(rune('0'+i))); err != nil {
			t.Fatal(err)
		}
	}
	_ = c1.Close()

	smallConfig := DefaultCacheConfig().WithMaxSize(1000).WithCleanupInterval(0)
	c2, err := NewDiskCacheWithConfig(dir, smallConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	cc := c2.(*cache)
	// After scan, totalSize exceeds MaxSize; Cleanup should run enforceMaxSize
	result := cc.Cleanup()
	if result.RemovedItems == 0 {
		t.Logf("enforceMaxSize: removed=%d (may be 0 if eviction already happened)", result.RemovedItems)
	}
	stats := cc.Stats()
	if stats.TotalSize > smallConfig.MaxSize && result.RemovedItems == 0 {
		t.Logf("size %d > max %d", stats.TotalSize, smallConfig.MaxSize)
	}
}

func TestScanExistingCache_ReadDirFails(t *testing.T) {
	// When ReadDir returns non-IsNotExist error, scanExistingCache returns that error
	fs := vfs.Memory()
	c := NewVFSCacheWithConfig(&readDirFailVFS{fs}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	err := c.scanExistingCache()
	if err == nil {
		t.Error("expected error when ReadDir fails with permission error")
	}
}

func TestStore_VfsWriteMkdirFails(t *testing.T) {
	c := NewVFSCacheWithConfig(&mkdirFailVFS{vfs.Memory()}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{})
	err := c.Store(res, "k")
	if err == nil {
		t.Error("expected error when MkdirAll fails")
	}
}

func TestStore_VfsWriteOpenFileFails(t *testing.T) {
	c := NewVFSCacheWithConfig(&openFileFailVFS{vfs.Memory()}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{})
	err := c.Store(res, "k")
	if err == nil {
		t.Error("expected error when OpenFile fails for body path")
	}
}

func TestRemoveEntry_RemoveFails(t *testing.T) {
	// A real removal error is retained and prevents an over-limit admission.
	config := DefaultCacheConfig().WithMaxSize(500).WithCleanupInterval(0)
	c := NewVFSCacheWithConfig(&removeFailVFS{vfs.Memory()}, config).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte(strings.Repeat("x", 300)), http.Header{"Content-Length": []string{"300"}})
	if err := c.Store(res, "k1"); err != nil {
		t.Fatal(err)
	}
	res2 := NewResourceBytes(200, []byte(strings.Repeat("y", 300)), http.Header{"Content-Length": []string{"300"}})
	if err := c.Store(res2, "k2"); err == nil {
		t.Fatal("Store(k2) succeeded despite the failed eviction")
	}
	if stats := c.Stats(); stats.ItemCount != 1 || stats.TotalSize > config.MaxSize {
		t.Fatalf("stats after rejected admission = %+v", stats)
	}
}

func TestEvictionSkipsFailedVictimAndReclaimsAnotherEntry(t *testing.T) {
	fs := &selectiveRemoveFailVFS{VFS: vfs.Memory()}
	c := NewVFSCacheWithConfig(fs, DefaultCacheConfig().WithMaxSize(1<<20).WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()

	store := func(key, body string) {
		t.Helper()
		res := NewResourceBytes(http.StatusOK, []byte(body), http.Header{
			"Content-Length": {fmt.Sprint(len(body))},
		})
		if err := c.Store(res, key); err != nil {
			t.Fatalf("Store(%q): %v", key, err)
		}
	}
	body := strings.Repeat("x", 64)
	store("stuck", body)
	store("removable", body)

	before := c.Stats()
	// Leave enough slack for sub-second timestamp serialization to vary by a
	// few bytes while still requiring one complete entry to be reclaimed.
	c.config.MaxSize = before.TotalSize + 16
	fs.failHash = hashKey("stuck")
	store("new", body)

	if got := c.Stats().TotalSize; got > c.config.MaxSize {
		t.Fatalf("cache size = %d, exceeds limit %d", got, c.config.MaxSize)
	}
	if _, err := c.Retrieve("removable"); err != ErrNotFoundInCache {
		t.Fatalf("removable entry error = %v, want ErrNotFoundInCache", err)
	}
	for _, key := range []string{"stuck", "new"} {
		res, err := c.Retrieve(key)
		if err != nil {
			t.Fatalf("Retrieve(%q): %v", key, err)
		}
		_ = res.Close()
	}
}

func TestStoreRejectsAdmissionWhenNoVictimCanBeRemoved(t *testing.T) {
	fs := &selectiveRemoveFailVFS{VFS: vfs.Memory()}
	c := NewVFSCacheWithConfig(fs, DefaultCacheConfig().WithMaxSize(1<<20).WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	body := strings.Repeat("x", 64)
	first := NewResourceBytes(http.StatusOK, []byte(body), http.Header{
		"Content-Length": {fmt.Sprint(len(body))},
	})
	if err := c.Store(first, "stuck"); err != nil {
		t.Fatalf("Store(stuck): %v", err)
	}

	before := c.Stats()
	c.config.MaxSize = before.TotalSize
	fs.failHash = hashKey("stuck")
	second := NewResourceBytes(http.StatusOK, []byte(body), http.Header{
		"Content-Length": {fmt.Sprint(len(body))},
	})
	if err := c.Store(second, "rejected"); err == nil {
		t.Fatal("Store(rejected) succeeded despite having no removable victim")
	}
	after := c.Stats()
	if after.ItemCount != before.ItemCount || after.TotalSize != before.TotalSize {
		t.Fatalf("failed admission changed stats: before=%+v after=%+v", before, after)
	}
	if _, err := c.Retrieve("rejected"); err != ErrNotFoundInCache {
		t.Fatalf("rejected entry error = %v, want ErrNotFoundInCache", err)
	}
}

func TestRejectedUnknownLengthReplacementDiscardsOldMetadata(t *testing.T) {
	config := DefaultCacheConfig().WithMaxSize(1 << 20).WithCleanupInterval(0)
	c := NewMemoryCacheWithConfig(config).(*cache)
	defer func() { _ = c.Close() }()

	old := NewResourceBytes(http.StatusOK, []byte("old"), http.Header{
		"Content-Length": {"3"},
	})
	if err := c.Store(old, "same-key"); err != nil {
		t.Fatalf("Store(old): %v", err)
	}
	config.MaxSize = c.Stats().TotalSize

	// With no Content-Length, Store learns the replacement size only after it
	// has overwritten the body. Rejection must discard the old header and LRU
	// record as well, leaving a consistent miss rather than ghost metadata.
	replacement := NewResourceBytes(http.StatusOK, []byte(strings.Repeat("x", 512)), http.Header{})
	if err := c.Store(replacement, "same-key"); err == nil {
		t.Fatal("oversized unknown-length replacement was admitted")
	}
	if stats := c.Stats(); stats.ItemCount != 0 || stats.TotalSize != 0 {
		t.Fatalf("rejected replacement left ghost metadata: %+v", stats)
	}
	if _, err := c.Retrieve("same-key"); err != ErrNotFoundInCache {
		t.Fatalf("replacement key error = %v, want ErrNotFoundInCache", err)
	}
	if _, err := c.Header("same-key"); err != ErrNotFoundInCache {
		t.Fatalf("replacement header error = %v, want ErrNotFoundInCache", err)
	}
}

func TestStore_NoContentLength(t *testing.T) {
	// Store with no Content-Length uses io.Copy path
	cache := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("body-without-cl"), http.Header{})
	if err := cache.Store(res, "nocl"); err != nil {
		t.Fatal(err)
	}
	out, err := cache.Retrieve("nocl")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()
	b, _ := io.ReadAll(out)
	if string(b) != "body-without-cl" {
		t.Errorf("body: got %q", b)
	}
}

func TestCache_Retrieve_HeaderMissing(t *testing.T) {
	// Body exists but header file missing -> Header returns ErrNotFoundInCache, we recordMiss
	config := DefaultCacheConfig().WithCleanupInterval(0)
	cc := NewVFSCacheWithConfig(vfs.Memory(), config).(*cache)
	defer func() { _ = cc.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{"Content-Length": []string{"1"}})
	if err := cc.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	hashed := hashKey("k")
	headerPath := headerPrefix + formatPrefix + hashed
	if err := cc.fs.Remove(headerPath); err != nil {
		t.Fatal(err)
	}
	_, err := cc.Retrieve("k")
	if err != ErrNotFoundInCache {
		t.Errorf("want ErrNotFoundInCache when header missing, got %v", err)
	}
	stats := cc.Stats()
	if stats.MissCount != 1 {
		t.Logf("miss count: %d", stats.MissCount)
	}
}

func TestCache_Retrieve_BodyOpenFails(t *testing.T) {
	config := DefaultCacheConfig().WithCleanupInterval(0)
	mem := vfs.Memory()
	c := NewVFSCacheWithConfig(&bodyOpenFailVFS{mem}, config).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("body"), http.Header{})
	if err := c.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	_, err := c.Retrieve("k")
	if err == nil {
		t.Error("expected error when body Open fails")
	}
	if err == ErrNotFoundInCache {
		t.Error("expected non-ErrNotFoundInCache when Open returns permission error")
	}
}

func TestCleanupLoop_Runs(t *testing.T) {
	config := DefaultCacheConfig().
		WithCleanupInterval(2 * time.Millisecond).
		WithTTL(1 * time.Hour).
		WithStaleMapTTL(1 * time.Hour)
	cache := NewMemoryCacheWithConfig(config)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{})
	if err := cache.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	result := cache.Cleanup()
	_ = result
}

type blockingRemoveVFS struct {
	vfs.VFS
	enabled bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingRemoveVFS) Remove(path string) error {
	if b.enabled {
		b.once.Do(func() {
			close(b.entered)
			<-b.release
		})
	}
	return b.VFS.Remove(path)
}

// TestCleanupRemovesExpiredEntryBeforeMarker verifies that cleanup never
// publishes marker removal while the pre-invalidation files are still being
// evicted. Otherwise a concurrent read, or a restart at that point, can treat
// the old representation as fresh.
func TestCleanupRemovesExpiredEntryBeforeMarker(t *testing.T) {
	originalClock := Clock
	defer func() { Clock = originalClock }()
	now := time.Now().UTC()
	Clock = func() time.Time { return now }

	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	fs := &blockingRemoveVFS{
		VFS:     vfs.Memory(),
		entered: make(chan struct{}),
		release: release,
	}
	config := DefaultCacheConfig().
		WithTTL(time.Hour).
		WithStaleMapTTL(time.Hour).
		WithCleanupInterval(0)
	c := NewVFSCacheWithConfig(fs, config).(*cache)
	defer func() { _ = c.Close() }()

	const key = "cleanup-order"
	if err := c.Store(NewResourceBytes(http.StatusOK, []byte("old"), http.Header{}), key); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	c.Invalidate(key)
	now = now.Add(2 * time.Hour)
	fs.enabled = true

	done := make(chan CleanupResult, 1)
	go func() { done <- c.Cleanup() }()
	select {
	case <-fs.entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not begin evicting the expired entry")
	}
	if _, ok := c.StaleAt(key); !ok {
		t.Error("invalidation marker was removed before its expired entry")
	}

	unblock()
	select {
	case result := <-done:
		if result.RemovedItems != 1 || result.RemovedStaleEntries != 1 {
			t.Errorf("cleanup result = %+v, want one item and one marker removed", result)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after filesystem removal resumed")
	}
	if _, ok := c.StaleAt(key); ok {
		t.Error("expired marker remained after its governed entry was removed")
	}
}

// TestCleanupRetainsMarkerWhenEntryRemovalFails covers the crash-safe failure
// path: an undeleted file must keep both its LRU record (for retry) and its
// stale marker (so restart cannot republish it as fresh).
func TestCleanupRetainsMarkerWhenEntryRemovalFails(t *testing.T) {
	originalClock := Clock
	defer func() { Clock = originalClock }()
	now := time.Now().UTC()
	Clock = func() time.Time { return now }

	config := DefaultCacheConfig().
		WithTTL(time.Hour).
		WithStaleMapTTL(time.Hour).
		WithCleanupInterval(0)
	c := NewVFSCacheWithConfig(&removeFailVFS{vfs.Memory()}, config).(*cache)
	defer func() { _ = c.Close() }()
	const key = "failed-cleanup"
	if err := c.Store(NewResourceBytes(http.StatusOK, []byte("old"), http.Header{}), key); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	c.Invalidate(key)
	now = now.Add(2 * time.Hour)

	result := c.Cleanup()
	if result.RemovedItems != 0 || result.RemovedStaleEntries != 0 {
		t.Errorf("cleanup result = %+v, want failed item and marker retained", result)
	}
	if stats := c.Stats(); stats.ItemCount != 1 || stats.StaleCount != 1 {
		t.Errorf("stats after failed removal = %+v, want one retryable item and marker", stats)
	}
	if _, ok := c.StaleAt(key); !ok {
		t.Error("marker was removed even though its backing entry could not be deleted")
	}
}

// TestCloseWaitsForCleanupLoop makes Close's join guarantee observable: the
// periodic loop is held inside a filesystem removal and Close must not return
// until that cleanup exits.
func TestCloseWaitsForCleanupLoop(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	fs := &blockingRemoveVFS{
		VFS:     vfs.Memory(),
		enabled: true,
		entered: make(chan struct{}),
		release: release,
	}
	config := DefaultCacheConfig().
		WithTTL(time.Nanosecond).
		WithCleanupInterval(time.Millisecond)
	c := NewVFSCacheWithConfig(fs, config).(*cache)
	if err := c.Store(NewResourceBytes(http.StatusOK, []byte("old"), http.Header{}), "close-join"); err != nil {
		unblock()
		_ = c.Close()
		t.Fatal(err)
	}

	select {
	case <-fs.entered:
	case <-time.After(time.Second):
		unblock()
		_ = c.Close()
		t.Fatal("cleanup loop did not enter filesystem removal")
	}

	closed := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closed)
	}()
	// Observe that Close has issued the stop request before asserting that it
	// still waits for the active cleanup call.
	<-c.stopChan
	select {
	case <-closed:
		unblock()
		t.Fatal("Close returned while the cleanup goroutine was still running")
	case <-time.After(10 * time.Millisecond):
	}

	unblock()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the cleanup goroutine exited")
	}
}

func TestCache_Freshen_KeyNotInCache(t *testing.T) {
	cache := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{"Cache-Control": []string{"max-age=60"}})
	// Freshen with key that was never stored -> Header returns ErrNotFoundInCache, we skip
	err := cache.Freshen(res, "nonexistent-key")
	if err != nil {
		t.Fatal(err)
	}
}

func TestCache_Freshen_InvalidatePath(t *testing.T) {
	cache := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{
		"Cache-Control":  []string{"max-age=60"},
		"Content-Length": []string{"1"},
	})
	if err := cache.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	// Freshen with different status -> invalidate path
	res2 := NewResourceBytes(304, []byte(""), http.Header{"Cache-Control": []string{"max-age=60"}})
	err := cache.Freshen(res2, "k")
	if err != nil {
		t.Fatal(err)
	}
}

func TestEvictIfNeeded_NoLimit(t *testing.T) {
	config := DefaultCacheConfig().WithMaxSize(0).WithCleanupInterval(0)
	cache := NewMemoryCacheWithConfig(config).(*cache)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{})
	if err := cache.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	// evictIfNeeded with MaxSize 0 does nothing (early return when MaxSize <= 0)
	stats := cache.Stats()
	if stats.ItemCount != 1 {
		t.Errorf("expected 1 item, got %d", stats.ItemCount)
	}
}

func TestEvictIfNeeded_RejectsOversizedItem(t *testing.T) {
	config := DefaultCacheConfig().WithMaxSize(30).WithCleanupInterval(0)
	cache := NewMemoryCacheWithConfig(config).(*cache)
	defer func() { _ = cache.Close() }()
	res1 := NewResourceBytes(200, []byte("12"), http.Header{"Content-Length": []string{"2"}})
	if err := cache.Store(res1, "k1"); err == nil {
		t.Fatal("oversized item was admitted")
	}
	if stats := cache.Stats(); stats.ItemCount != 0 || stats.TotalSize != 0 {
		t.Fatalf("oversized admission changed stats: %+v", stats)
	}
}

func TestEvictIfNeeded_WithMetrics(t *testing.T) {
	prev := GetDefaultMetrics()
	defer func() { SetDefaultMetrics(prev) }()
	reg := metrics.NewRegistry("test_evict_metrics")
	SetDefaultMetrics(NewCacheMetrics(reg))
	config := DefaultCacheConfig().WithMaxSize(1 << 20).WithCleanupInterval(0)
	cache := NewMemoryCacheWithConfig(config).(*cache)
	defer func() { _ = cache.Close() }()
	res1 := NewResourceBytes(200, []byte(strings.Repeat("x", 80)), http.Header{"Content-Length": []string{"80"}})
	if err := cache.Store(res1, "k1"); err != nil {
		t.Fatal(err)
	}
	config.MaxSize = cache.Stats().TotalSize + 16
	res2 := NewResourceBytes(200, []byte(strings.Repeat("y", 80)), http.Header{"Content-Length": []string{"80"}})
	if err := cache.Store(res2, "k2"); err != nil {
		t.Fatal(err)
	}
	// Second Store should trigger eviction of k1 (LRU); RecordCacheEviction is called
	stats := cache.Stats()
	if stats.ItemCount != 1 {
		t.Logf("eviction may have run: item count %d", stats.ItemCount)
	}
}

func TestCleanupLoop_TickerFires(t *testing.T) {
	config := DefaultCacheConfig().
		WithCleanupInterval(5 * time.Millisecond).
		WithTTL(1 * time.Millisecond).
		WithStaleMapTTL(time.Hour)
	cache := NewMemoryCacheWithConfig(config)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{})
	if err := cache.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond) // wait for at least one cleanup tick
	result := cache.Cleanup()
	_ = result
}

func TestCleanup_WithMetrics(t *testing.T) {
	prev := GetDefaultMetrics()
	defer func() { SetDefaultMetrics(prev) }()
	reg := metrics.NewRegistry("test_cleanup_metrics")
	SetDefaultMetrics(NewCacheMetrics(reg))
	config := DefaultCacheConfig().WithMaxSize(200).WithCleanupInterval(0)
	cache := NewMemoryCacheWithConfig(config)
	defer func() { _ = cache.Close() }()
	for i := 0; i < 5; i++ {
		res := NewResourceBytes(200, []byte(strings.Repeat("x", 100)), http.Header{"Content-Length": []string{"100"}})
		if err := cache.Store(res, "key"+string(rune('0'+i))); err != nil {
			t.Fatal(err)
		}
	}
	result := cache.Cleanup()
	if result.RemovedItems == 0 {
		t.Logf("enforceMaxSize may have run: removed=%d", result.RemovedItems)
	}
}

func TestCache_Header_ReadError(t *testing.T) {
	config := DefaultCacheConfig().WithCleanupInterval(0)
	cc := NewVFSCacheWithConfig(vfs.Memory(), config).(*cache)
	defer func() { _ = cc.Close() }()
	res := NewResourceBytes(200, []byte("body"), http.Header{})
	if err := cc.Store(res, "testkey"); err != nil {
		t.Fatal(err)
	}
	hashed := hashKey("testkey")
	headerPath := headerPrefix + formatPrefix + hashed
	f, err := cc.fs.OpenFile(headerPath, os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("not valid status line\r\n\r\n"))
	_ = f.Close()
	_, err = cc.Header("testkey")
	if err == nil {
		t.Error("expected error for malformed header file")
	}
}

// errWriter always returns error on Write (covers headersToWriter error path).
type errWriter struct{}

func (errWriter) Write(p []byte) (n int, err error) {
	return 0, os.ErrPermission
}

func TestHeadersToWriter_Fail(t *testing.T) {
	h := http.Header{"X-Foo": []string{"bar"}}
	err := headersToWriter(h, errWriter{})
	if err == nil {
		t.Error("expected error when writer fails")
	}
}

// headerOpenFileFailVFS fails OpenFile for header/ path so storeHeader vfsWrite fails.
type headerOpenFileFailVFS struct{ vfs.VFS }

func (h *headerOpenFileFailVFS) OpenFile(path string, flag int, perm os.FileMode) (vfs.WFile, error) {
	if strings.HasPrefix(path, "header/") {
		return nil, os.ErrPermission
	}
	return h.VFS.OpenFile(path, flag, perm)
}

func TestStore_HeaderOpenFileFails(t *testing.T) {
	c := NewVFSCacheWithConfig(&headerOpenFileFailVFS{vfs.Memory()}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{"Content-Length": []string{"1"}})
	err := c.Store(res, "k")
	if err == nil {
		t.Error("expected error when OpenFile fails for header path")
	}
	if err != nil && !strings.Contains(err.Error(), "header") {
		t.Logf("error message should mention header: %v", err)
	}
}

// writeFailWFile wraps a WFile and makes Write return error (covers vfsWrite io.Copy failure).
type writeFailWFile struct {
	vfs.WFile
}

func (w *writeFailWFile) Write(p []byte) (n int, err error) {
	return 0, os.ErrPermission
}

type partialMarkerWFile struct {
	vfs.WFile
	wrote bool
}

func (w *partialMarkerWFile) Write(p []byte) (int, error) {
	if w.wrote {
		return 0, errors.New("injected marker snapshot failure")
	}
	w.wrote = true
	n := len(p) / 2
	if n == 0 {
		n = 1
	}
	written, err := w.WFile.Write(p[:n])
	if err != nil {
		return written, err
	}
	return written, errors.New("injected marker snapshot failure")
}

type failSecondMarkerSnapshotVFS struct {
	vfs.VFS
	markerWrites int
}

func (v *failSecondMarkerSnapshotVFS) OpenFile(path string, flag int, perm os.FileMode) (vfs.WFile, error) {
	f, err := v.VFS.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(path, "stale-markers.") {
		v.markerWrites++
		if v.markerWrites == 2 {
			return &partialMarkerWFile{WFile: f}, nil
		}
	}
	return f, nil
}

// writeFailVFS returns a WFile that fails Write so io.Copy in vfsWrite fails.
type writeFailVFS struct {
	vfs.VFS
}

func (w *writeFailVFS) OpenFile(path string, flag int, perm os.FileMode) (vfs.WFile, error) {
	f, err := w.VFS.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &writeFailWFile{WFile: f}, nil
}

func TestStore_VfsWriteCopyFails(t *testing.T) {
	c := NewVFSCacheWithConfig(&writeFailVFS{vfs.Memory()}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("ab"), http.Header{"Content-Length": []string{"2"}})
	err := c.Store(res, "k")
	if err == nil {
		t.Error("expected error when io.Copy fails in vfsWrite")
	}
}

type failAfterDataReader struct {
	delivered bool
}

func (r *failAfterDataReader) Read(p []byte) (int, error) {
	if r.delivered {
		return 0, errors.New("injected snapshot read failure")
	}
	r.delivered = true
	return copy(p, "partial-new-snapshot"), nil
}

// TestAtomicWriteFilePreservesPreviousSnapshot is the regression test for
// opening stale-markers.json with O_TRUNC. A crash, full disk, or copy error
// during the next snapshot destroyed the previous valid markers and made
// every older invalidation disappear after restart.
func TestAtomicWriteFilePreservesPreviousSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, staleMapPath)
	old := []byte(`{"old-key":"2026-09-12T00:00:00Z"}`)
	if err := os.WriteFile(path, old, 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := atomicWriteFile(path, &failAfterDataReader{}); err == nil {
		t.Fatal("atomicWriteFile succeeded despite the injected copy failure")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, old) {
		t.Errorf("snapshot after failed replacement = %q, want previous snapshot %q", got, old)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "."+filepath.Base(path)+".tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Errorf("temporary snapshot files were not cleaned up: %v", matches)
	}
}

// TestVFSSnapshotPreservesPreviousGeneration covers persistent VFS backends
// that do not expose an atomic Rename operation. A partial write of the next
// slot must leave the other complete generation available after reconstruction.
func TestVFSSnapshotPreservesPreviousGeneration(t *testing.T) {
	fs := &failSecondMarkerSnapshotVFS{VFS: vfs.Memory()}
	config := DefaultCacheConfig().WithCleanupInterval(0)
	first := NewVFSCacheWithConfig(fs, config)
	innerFirst := first.(*cache)
	innerFirst.staleMutex.Lock()
	innerFirst.stale["old-key"] = Clock()
	innerFirst.persistStale(innerFirst.snapshotStaleLocked())
	innerFirst.stale["new-key"] = Clock()
	// The second journal-slot write fails halfway, as a crash during the next
	// complete snapshot would. The first slot must remain authoritative.
	innerFirst.persistStale(innerFirst.snapshotStaleLocked())
	innerFirst.staleMutex.Unlock()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := NewVFSCacheWithConfig(fs, config)
	defer func() { _ = second.Close() }()
	inner := second.(*cache)
	if _, ok := inner.StaleAt("old-key"); !ok {
		t.Fatal("partial VFS snapshot destroyed the previous invalidation generation")
	}
	if _, ok := inner.StaleAt("new-key"); ok {
		t.Error("partially written invalidation generation was accepted as complete")
	}
}

// TestInvalidateIsVisibleWhileWritersDrain covers taking generationMu before
// publishing the stale marker. One unrelated slow Store could hold its read
// side and leave the successfully mutated representation visible as a fresh
// HIT until that write completed.
func TestInvalidateIsVisibleWhileWritersDrain(t *testing.T) {
	c := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	const key = "GET:http://example.org/mutated"
	if err := c.Store(NewResourceBytes(http.StatusOK, []byte("old"), http.Header{}), key); err != nil {
		t.Fatal(err)
	}

	// Model an unrelated Store that is already active.
	c.generationMu.RLock()
	locked := true
	done := make(chan struct{})
	go func() {
		c.Invalidate(key)
		close(done)
	}()
	defer func() {
		if locked {
			c.generationMu.RUnlock()
		}
		<-done
	}()

	deadline := time.After(time.Second)
	for {
		if at, ok := c.StaleAt(key); ok {
			if !at.Equal(invalidationBarrierTime) {
				t.Fatalf("marker while writer is active = %s, want barrier", at)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("invalidation did not become visible while an active writer held generationMu")
		case <-time.After(time.Millisecond):
		}
	}

	select {
	case <-done:
		t.Fatal("Invalidate completed before the active writer was released")
	default:
	}
	res, err := c.Retrieve(key)
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsStale() {
		t.Error("pre-mutation representation remained fresh while Invalidate waited for a writer")
	}
	_ = res.Close()

	c.generationMu.RUnlock()
	locked = false
	<-done
}

// TestInterruptedInvalidationBarrierRecoversOnRestart covers a process dying
// after the visible barrier was persisted but before active writers drained.
// No writer survives a process restart, so startup turns the barrier into its
// own timestamp and persists that final marker.
func TestInterruptedInvalidationBarrierRecoversOnRestart(t *testing.T) {
	originalClock := Clock
	defer func() { Clock = originalClock }()
	now := time.Now().UTC()
	Clock = func() time.Time { return now }

	fs := vfs.Memory()
	config := DefaultCacheConfig().WithCleanupInterval(0)
	const key = "GET:http://example.org/interrupted"
	first := NewVFSCacheWithConfig(fs, config).(*cache)
	if err := first.Store(NewResourceBytes(http.StatusOK, []byte("old"), http.Header{}), key); err != nil {
		t.Fatal(err)
	}
	first.staleMutex.Lock()
	first.stale[key] = invalidationBarrierTime
	first.persistStale(first.snapshotStaleLocked())
	first.staleMutex.Unlock()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	now = now.Add(time.Second)
	second := NewVFSCacheWithConfig(fs, config).(*cache)
	defer func() { _ = second.Close() }()
	if at, ok := second.StaleAt(key); !ok || !at.Equal(now) {
		t.Fatalf("recovered marker = (%s, %v), want restart time %s", at, ok, now)
	}
	res, err := second.Retrieve(key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Close() }()
	if !res.IsStale() {
		t.Error("pre-mutation entry was fresh after recovering an interrupted invalidation")
	}
}

// TestStoredAtMetadataPreservesOriginHeader is the regression test for using
// a private-looking HTTP field as cache metadata. An origin is allowed to use
// that name; overwriting it on disk and deleting it on read made HIT responses
// differ from the original MISS.
func TestStoredAtMetadataPreservesOriginHeader(t *testing.T) {
	c := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()

	const key = "GET:http://example.org/origin-header"
	const headerName = "X-Httpcache-Internal-Stored-At"
	// Deliberately parseable as the interim metadata format. A non-time value
	// would miss the ambiguous upgrade case that caused the field to be lost.
	const value = "2026-09-12T00:00:00.123456789Z"
	res := NewResourceBytes(http.StatusOK, []byte("body"), http.Header{
		headerName: {value},
	})
	if err := c.Store(res, key); err != nil {
		t.Fatal(err)
	}

	// Rewrite just the header record without the new preamble, simulating an
	// entry created before cache metadata had an unambiguous framing line.
	hb := &bytes.Buffer{}
	fmt.Fprintf(hb, "HTTP/1.1 %d %s\r\n", http.StatusOK, http.StatusText(http.StatusOK))
	if err := headersToWriter(res.Header(), hb); err != nil {
		t.Fatal(err)
	}
	path := headerPrefix + formatPrefix + hashKey(key)
	f, err := c.fs.OpenFile(path, os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(hb.Bytes()); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	h, err := c.Header(key)
	if err != nil {
		t.Fatal(err)
	}
	if got := h.Get(headerName); got != value {
		t.Errorf("cached origin header = %q, want %q", got, value)
	}
	got, err := c.Retrieve(key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = got.Close() }()
	if valueAfterHit := got.Header().Get(headerName); valueAfterHit != value {
		t.Errorf("origin header after cache HIT = %q, want %q", valueAfterHit, value)
	}
}

// headerFailAfterFirstVFS: OpenFile for header/ path succeeds first time, fails second (for Freshen storeHeader).
type headerFailAfterFirstVFS struct {
	vfs.VFS
	headerOpenCount int
}

func (h *headerFailAfterFirstVFS) OpenFile(path string, flag int, perm os.FileMode) (vfs.WFile, error) {
	if strings.HasPrefix(path, "header/") {
		h.headerOpenCount++
		if h.headerOpenCount > 1 {
			return nil, os.ErrPermission
		}
	}
	return h.VFS.OpenFile(path, flag, perm)
}

func TestFreshen_StoreHeaderFails(t *testing.T) {
	base := vfs.Memory()
	c := NewVFSCacheWithConfig(&headerFailAfterFirstVFS{VFS: base}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{
		"Content-Length": []string{"1"},
		"Cache-Control":  []string{"max-age=60"},
	})
	if err := c.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	// Freshen: Header("k") opens header for read (Open, not OpenFile), then storeHeader does OpenFile(header) -> fails (second time).
	res2 := NewResourceBytes(200, []byte("x"), http.Header{
		"Cache-Control":  []string{"max-age=60"},
		"Content-Length": []string{"1"},
	})
	err := c.Freshen(res2, "k")
	if err == nil {
		t.Error("expected error when storeHeader fails during Freshen")
	}
}

func TestCache_Retrieve_HeaderReadError(t *testing.T) {
	config := DefaultCacheConfig().WithCleanupInterval(0)
	cc := NewVFSCacheWithConfig(vfs.Memory(), config).(*cache)
	defer func() { _ = cc.Close() }()
	res := NewResourceBytes(200, []byte("body"), http.Header{})
	if err := cc.Store(res, "testkey"); err != nil {
		t.Fatal(err)
	}
	hashed := hashKey("testkey")
	headerPath := headerPrefix + formatPrefix + hashed
	f, err := cc.fs.OpenFile(headerPath, os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("HTTP/1.1\r\n\r\n")) // malformed: len(f)<2
	_ = f.Close()
	_, err = cc.Retrieve("testkey")
	if err == nil {
		t.Error("expected error when header read fails on Retrieve")
	}
	if err == ErrNotFoundInCache {
		t.Error("expected non-ErrNotFoundInCache when readHeaders fails")
	}
}

func TestStore_ReaderFails(t *testing.T) {
	c := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResource(200, &errReadSeekCloser{err: errors.New("read fails")}, http.Header{}) // no Content-Length, io.Copy(buf, res) will fail
	err := c.Store(res, "k")
	if err == nil {
		t.Error("expected error when body reader fails")
	}
}

func TestReadHeaders_MalformedStatusLineOnlyOnePart(t *testing.T) {
	// Status line with only one token (e.g. "HTTP/1.1" only) -> len(f) < 2
	r := bufio.NewReader(bytes.NewReader([]byte("HTTP/1.1\r\n\r\n")))
	_, err := readHeaders(r)
	if err == nil {
		t.Error("expected error for status line with only one part")
	}
}
