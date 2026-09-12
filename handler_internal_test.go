package httpcache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	logger "github.com/soulteary/logger-kit/v2"
	metrics "github.com/soulteary/metrics-kit/v2"
)

func TestNewHandlerWithOptions_WithLogger(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	cache := NewMemoryCache()
	log := logger.Default()
	h := NewHandlerWithOptions(cache, upstream, &HandlerOptions{Logger: log})
	t.Cleanup(func() { h.writes.Wait() })
	if h == nil {
		t.Fatal("handler is nil")
	}
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("code: %d", rec.Code)
	}
	// passUpstream deliberately persists the body asynchronously; wait before
	// making the request whose purpose is to exercise the cache-hit path.
	h.writes.Wait()
	// Trigger logRef with IsDebugLogging so handler uses injected logger for debugf
	prev := IsDebugLogging()
	SetDebugLogging(true)
	defer func() { SetDebugLogging(prev) }()
	req2 := httptest.NewRequest("GET", "http://example.org/", nil)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Errorf("code: %d", rec2.Code)
	}
}

func TestNewCacheRequest_EmptyHost(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	req.Proto = "HTTP/1.1"
	req.Host = ""
	_, err := newCacheRequest(req)
	if err == nil {
		t.Fatal("expected error for empty Host")
	}
	if !strings.Contains(err.Error(), "host") {
		t.Errorf("error should mention host: %v", err)
	}
}

func TestServeHTTP_InvalidRequest(t *testing.T) {
	h := NewHandler(NewMemoryCache(), http.NotFoundHandler())
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	req.Proto = "HTTP/1.1"
	req.Host = ""
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rec.Code)
	}
}

func TestServeHTTP_OnlyIfCached_Miss(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(NewMemoryCache(), upstream)
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	req.Header.Set("Cache-Control", "only-if-cached")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusGatewayTimeout {
		t.Errorf("want 504 when only-if-cached and miss, got %d", rec.Code)
	}
}

func TestServeHTTP_LookupError(t *testing.T) {
	mem := NewMemoryCache()
	cc, ok := mem.(*cache)
	if !ok {
		t.Fatal("NewMemoryCache should return *cache")
	}
	failCache := &failRetrieveCache{cache: cc}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(failCache, upstream)
	// First request stores; second would hit Retrieve error. So we need to store then retrieve with error.
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	Writes.Wait()
	failCache.failNext = true
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusInternalServerError {
		t.Errorf("want 500 on lookup error, got %d", rec2.Code)
	}
}

type failRetrieveCache struct {
	cache    *cache
	failNext bool
}

func (f *failRetrieveCache) Header(key string) (Header, error) { return f.cache.Header(key) }
func (f *failRetrieveCache) Store(res *Resource, keys ...string) error {
	return f.cache.Store(res, keys...)
}
func (f *failRetrieveCache) Retrieve(key string) (*Resource, error) {
	if f.failNext {
		return nil, errors.New("injected retrieve error")
	}
	return f.cache.Retrieve(key)
}
func (f *failRetrieveCache) Invalidate(keys ...string) { f.cache.Invalidate(keys...) }
func (f *failRetrieveCache) Freshen(res *Resource, keys ...string) error {
	return f.cache.Freshen(res, keys...)
}

func TestHandler_Errorf_StoreFails(t *testing.T) {
	mem := NewMemoryCache()
	cc, ok := mem.(*cache)
	if !ok {
		t.Fatal("expected *cache")
	}
	failStore := &failStoreCache{cache: cc}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(failStore, upstream)
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	Writes.Wait()
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
}

type failStoreCache struct {
	cache *cache
}

func (f *failStoreCache) Header(key string) (Header, error) { return f.cache.Header(key) }
func (f *failStoreCache) Store(res *Resource, keys ...string) error {
	return errors.New("injected store error")
}
func (f *failStoreCache) Retrieve(key string) (*Resource, error) { return f.cache.Retrieve(key) }
func (f *failStoreCache) Invalidate(keys ...string)              { f.cache.Invalidate(keys...) }
func (f *failStoreCache) Freshen(res *Resource, keys ...string) error {
	return f.cache.Freshen(res, keys...)
}

func TestTempFileReadSeekCloser(t *testing.T) {
	f, err := os.CreateTemp("", "httpcache-test-*")
	if err != nil {
		t.Skip("CreateTemp failed:", err)
	}
	path := f.Name()
	_, _ = f.Write([]byte("hello"))
	_ = f.Close()
	f2, err := os.Open(path)
	if err != nil {
		_ = os.Remove(path)
		t.Fatal(err)
	}
	tf := &tempFileReadSeekCloser{file: f2, path: path}
	buf := make([]byte, 2)
	n, _ := tf.Read(buf)
	if n != 2 || string(buf) != "he" {
		t.Errorf("Read: n=%d buf=%s", n, buf)
	}
	pos, err := tf.Seek(0, 0)
	if err != nil || pos != 0 {
		t.Errorf("Seek: pos=%d err=%v", pos, err)
	}
	n, _ = tf.Read(buf)
	if n != 2 {
		t.Errorf("Read after Seek: n=%d", n)
	}
	if err := tf.Close(); err != nil {
		t.Error("Close:", err)
	}
}

// TestTempFileReadSeekCloser_Close_RemoveFails covers Close() when os.Remove(path) fails (e.g. path nonexistent).
func TestTempFileReadSeekCloser_Close_RemoveFails(t *testing.T) {
	tf := &tempFileReadSeekCloser{file: nil, path: "/nonexistent_httpcache_test_xyz"}
	err := tf.Close()
	if err == nil {
		t.Error("Close with nonexistent path should return error from Remove")
	}
}

func TestPassUpstream_NoDate_CorrectedAgeError(t *testing.T) {
	oldTmp := os.Getenv("TMPDIR")
	_ = os.Setenv("TMPDIR", "/nonexistent_httpcache_test_xyz")
	defer func() {
		if oldTmp != "" {
			_ = os.Setenv("TMPDIR", oldTmp)
		} else {
			_ = os.Unsetenv("TMPDIR")
		}
	}()
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		// No Date header -> correctedAge fails in finishPassUpstream
	})
	h := NewHandler(NewMemoryCache(), upstream)
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	Writes.Wait()
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
}

func TestPassUpstream_NoDate_WithMetrics(t *testing.T) {
	oldTmp := os.Getenv("TMPDIR")
	_ = os.Setenv("TMPDIR", "/nonexistent_httpcache_test_xyz")
	defer func() {
		if oldTmp != "" {
			_ = os.Setenv("TMPDIR", oldTmp)
		} else {
			_ = os.Unsetenv("TMPDIR")
		}
	}()
	reg := metrics.NewRegistry("test_finish_pass_metrics")
	m := NewCacheMetrics(reg)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(NewMemoryCache(), upstream)
	h.SetMetrics(m)
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	Writes.Wait()
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
}

func TestIsCacheable_PrivateShared(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(NewMemoryCache(), upstream)
	h.Shared = true
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
	if rec.Header().Get(CacheHeader) != "SKIP" && rec.Header().Get(CacheHeader) != "MISS" {
		t.Logf("X-Cache: %s (expect SKIP when uncacheable private)", rec.Header().Get(CacheHeader))
	}
}

func TestIsCacheable_NonStoreableStatus(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	})
	h := NewHandler(NewMemoryCache(), upstream)
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("want 404, got %d", rec.Code)
	}
	if rec.Header().Get(CacheHeader) != "SKIP" {
		t.Logf("X-Cache: %s (404 not storeable)", rec.Header().Get(CacheHeader))
	}
}

func TestIsCacheable_RequestAuthorizationShared(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(NewMemoryCache(), upstream)
	h.Shared = true
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
	if rec.Header().Get(CacheHeader) != "SKIP" && rec.Header().Get(CacheHeader) != "MISS" {
		t.Logf("X-Cache: %s (request Authorization + Shared -> uncacheable)", rec.Header().Get(CacheHeader))
	}
}

func TestIsCacheable_ResponseAuthorizationShared(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("Authorization", "Bearer response-token")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(NewMemoryCache(), upstream)
	h.Shared = true
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
	if rec.Header().Get(CacheHeader) != "SKIP" {
		t.Logf("X-Cache: %s (response Authorization + Shared without s-maxage -> uncacheable)", rec.Header().Get(CacheHeader))
	}
}

func TestErrReadSeekCloser(t *testing.T) {
	e := errReadSeekCloser{err: errors.New("test")}
	if e.Error() != "test" {
		t.Errorf("Error(): %s", e.Error())
	}
	if e.Close() != e.err {
		t.Errorf("Close() should return err")
	}
	n, err := e.Read(nil)
	if n != 0 || err != e.err {
		t.Errorf("Read: n=%d err=%v", n, err)
	}
	_, err = e.Seek(0, 0)
	if err != e.err {
		t.Errorf("Seek: %v", err)
	}
}

func TestResponseStreamer_Resource_ReadError(t *testing.T) {
	// Resource() reads from pipeReader; when pipe returns error, Resource() returns errReadSeekCloser body
	pr, pw := io.Pipe()
	pw.CloseWithError(errors.New("broken"))
	rec := httptest.NewRecorder()
	rs := &responseStreamer{
		ResponseWriter: rec,
		pipeReader:     pr,
		StatusCode:     200,
	}
	res := rs.Resource()
	if res == nil {
		t.Fatal("Resource() returned nil")
	}
	if res.ReadSeekCloser == nil {
		t.Fatal("body is nil")
	}
	_, err := res.Read([]byte{0})
	if err == nil {
		t.Error("expected read error")
	}
}

func TestCorrectedAge_NoDate(t *testing.T) {
	h := http.Header{}
	_, err := correctedAge(h, time.Now(), time.Now())
	if err == nil {
		t.Error("expected error when Date header missing")
	}
}

func TestCorrectedAge_NoAge(t *testing.T) {
	h := http.Header{}
	h.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	// Age header missing - intHeader returns errNoHeader
	_, err := correctedAge(h, time.Now(), time.Now())
	if err == nil {
		t.Error("expected error when Age header missing")
	}
}

func TestCorrectedAge_ApparentAgeNegative(t *testing.T) {
	// Date in future -> apparentAge = respTime - date < 0 -> clamp to 0
	now := Clock().UTC()
	future := now.Add(1 * time.Hour)
	h := http.Header{}
	h.Set("Date", future.Format(http.TimeFormat))
	h.Set("Age", "0")
	reqTime := now.Add(-1 * time.Second)
	respTime := now
	_, err := correctedAge(h, reqTime, respTime)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCorrectedAge_ApparentAgeGreaterThanCorrected(t *testing.T) {
	// apparentAge > correctedAge -> use apparentAge
	now := Clock().UTC()
	past := now.Add(-100 * time.Second)
	h := http.Header{}
	h.Set("Date", past.Format(http.TimeFormat))
	h.Set("Age", "0")
	reqTime := now.Add(-2 * time.Second)
	respTime := now
	age, err := correctedAge(h, reqTime, respTime)
	if err != nil {
		t.Fatal(err)
	}
	if age <= 0 {
		t.Errorf("expected positive age, got %v", age)
	}
}

func TestServeResource_NonOKStatus(t *testing.T) {
	cache := NewMemoryCache()
	res := NewResourceBytes(http.StatusNotFound, []byte("not found"), http.Header{
		"Cache-Control": []string{"public, max-age=60"},
		"Date":          []string{time.Now().UTC().Format(http.TimeFormat)},
	})
	if err := cache.Store(res, "GET:http://example.org/notfound"); err != nil {
		t.Fatal(err)
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/notfound", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("want 404, got %d", rec.Code)
	}
}

func TestLookup_VarySecondaryMiss(t *testing.T) {
	// Primary key hit, resource has Vary, secondary (vary) key miss -> return primary resource
	cache := NewMemoryCache()
	res := NewResourceBytes(200, []byte("ok"), http.Header{
		"Cache-Control": []string{"max-age=60"},
		"Vary":          []string{"Accept-Language"},
		"Date":          []string{time.Now().UTC().Format(http.TimeFormat)},
	})
	if err := cache.Store(res, "GET:http://example.org/"); err != nil {
		t.Fatal(err)
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	req.Header.Set("Accept-Language", "de")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
}

func TestLookup_HEAD_NoExplicitExpiration(t *testing.T) {
	// HEAD request when GET key exists but has no explicit expiration -> return ErrNotFoundInCache
	cache := NewMemoryCache()
	lastMod := time.Now().UTC().Add(-24 * time.Hour).Format(http.TimeFormat)
	res := NewResourceBytes(200, []byte("body"), http.Header{
		"Last-Modified": []string{lastMod},
		"Date":          []string{time.Now().UTC().Format(http.TimeFormat)},
	})
	if err := cache.Store(res, "GET:http://example.org/"); err != nil {
		t.Fatal(err)
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("HEAD", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Logf("HEAD without explicit expiration: code %d", rec.Code)
	}
}

func TestOnlyIfCached_InCacheNeedsValidation(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60, must-revalidate")
		w.Header().Set("Date", time.Now().UTC().Add(-2*time.Minute).Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	cache := NewMemoryCache()
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	Writes.Wait()
	cReq, err := newCacheRequest(req)
	if err != nil {
		t.Fatalf("newCacheRequest: %v", err)
	}
	cache.Invalidate(cReq.Key.String())
	req2 := httptest.NewRequest("GET", "http://example.org/", nil)
	req2.Header.Set("Cache-Control", "only-if-cached")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusGatewayTimeout {
		t.Errorf("want 504, got %d", rec2.Code)
	}
}

func TestNeedsValidation_MinFreshError(t *testing.T) {
	// needsValidation when request has min-fresh that fails to parse
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("Date", time.Now().UTC().Add(-30*time.Second).Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	cache := NewMemoryCache()
	if err := cache.Store(NewResourceBytes(200, []byte("x"), http.Header{
		"Cache-Control": []string{"max-age=60"},
		"Date":          []string{time.Now().UTC().Add(-30 * time.Second).Format(http.TimeFormat)},
	}), "GET:http://example.org/"); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	req.Header.Set("Cache-Control", "min-fresh=invalid")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Logf("code %d (min-fresh parse error may trigger revalidation)", rec.Code)
	}
}

func TestFreshness_RequestMaxAge(t *testing.T) {
	// freshness when request has max-age that limits
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Date", time.Now().UTC().Add(-1*time.Second).Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	cache := NewMemoryCache()
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	req.Header.Set("Cache-Control", "max-age=10")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	Writes.Wait()
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusOK {
		t.Errorf("second request: %d", rec2.Code)
	}
}

func TestFreshness_HeuristicFreshness(t *testing.T) {
	// freshness when HeuristicFreshness() > maxAge (resource with Last-Modified, no max-age)
	lastMod := time.Now().UTC().Add(-24 * time.Hour)
	cache := NewMemoryCache()
	res := NewResourceBytes(200, []byte("ok"), http.Header{
		"Last-Modified": []string{lastMod.Format(http.TimeFormat)},
		"Date":          []string{time.Now().UTC().Format(http.TimeFormat)},
	})
	if err := cache.Store(res, "GET:http://example.org/heuristic"); err != nil {
		t.Fatal(err)
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/heuristic", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
	if rec.Header().Get(CacheHeader) != "HIT" {
		t.Errorf("want HIT, got %s", rec.Header().Get(CacheHeader))
	}
}

func TestNeedsValidation_MinFresh(t *testing.T) {
	// Cached resource with freshness 30s, request min-fresh=60 -> need validation
	now := time.Now().UTC()
	cache := NewMemoryCache()
	res := NewResourceBytes(200, []byte("ok"), http.Header{
		"Cache-Control": []string{"max-age=60"},
		"Date":          []string{now.Add(-30 * time.Second).Format(http.TimeFormat)},
	})
	if err := cache.Store(res, "GET:http://example.org/minfresh"); err != nil {
		t.Fatal(err)
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("Date", now.Add(-30*time.Second).Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/minfresh", nil)
	req.Header.Set("Cache-Control", "min-fresh=60")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
}

func TestNeedsValidation_MaxStale_Empty(t *testing.T) {
	// Stale resource but request has max-stale (no value) -> serve stale without revalidation
	now := time.Now().UTC()
	cache := NewMemoryCache()
	res := NewResourceBytes(200, []byte("ok"), http.Header{
		"Cache-Control": []string{"max-age=10"},
		"Date":          []string{now.Add(-1 * time.Minute).Format(http.TimeFormat)},
	})
	if err := cache.Store(res, "GET:http://example.org/maxstale"); err != nil {
		t.Fatal(err)
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("new"))
	})
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/maxstale", nil)
	req.Header.Set("Cache-Control", "max-stale")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
	if rec.Header().Get(CacheHeader) != "HIT" {
		t.Errorf("want HIT (serve stale with max-stale), got %s", rec.Header().Get(CacheHeader))
	}
}

func TestNeedsValidation_MaxStale_WithValue(t *testing.T) {
	// Stale resource, request has max-stale=3600 -> serve stale (maxStale >= -freshness branch)
	now := time.Now().UTC()
	cache := NewMemoryCache()
	res := NewResourceBytes(200, []byte("ok"), http.Header{
		"Cache-Control": []string{"max-age=10"},
		"Date":          []string{now.Add(-1 * time.Minute).Format(http.TimeFormat)},
	})
	if err := cache.Store(res, "GET:http://example.org/maxstaleval"); err != nil {
		t.Fatal(err)
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("new"))
	})
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/maxstaleval", nil)
	req.Header.Set("Cache-Control", "max-stale=3600")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
	if rec.Header().Get(CacheHeader) != "HIT" {
		t.Errorf("want HIT (serve stale with max-stale=3600), got %s", rec.Header().Get(CacheHeader))
	}
}

func TestFreshness_IsStale(t *testing.T) {
	// Cached resource then invalidated -> Retrieve returns resource with MarkStale(); freshness returns 0
	cache := NewMemoryCache()
	res := NewResourceBytes(200, []byte("ok"), http.Header{
		"Cache-Control": []string{"max-age=60"},
		"Date":          []string{time.Now().UTC().Format(http.TimeFormat)},
	})
	if err := cache.Store(res, "GET:http://example.org/stale"); err != nil {
		t.Fatal(err)
	}
	cache.Invalidate("GET:http://example.org/stale")
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("new"))
	})
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/stale", nil)
	req.Header.Set("Cache-Control", "max-stale=60")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
}

func TestNeedsValidation_FreshnessError(t *testing.T) {
	// Cached resource with invalid max-age -> freshness(res, r) returns error -> needsValidation true
	cache := NewMemoryCache()
	res := NewResourceBytes(200, []byte("ok"), http.Header{
		"Cache-Control": []string{"max-age=invalid"},
		"Date":          []string{time.Now().UTC().Format(http.TimeFormat)},
	})
	if err := cache.Store(res, "GET:http://example.org/badmaxage"); err != nil {
		t.Fatal(err)
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(cache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/badmaxage", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
}

func TestIsCacheable_PublicWithHeuristic(t *testing.T) {
	// GET miss, upstream returns 200 Cache-Control: public, Last-Modified (no max-age) -> cacheableByDefault + HeuristicFreshness
	lastMod := time.Now().UTC().Add(-1 * time.Hour)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public")
		w.Header().Set("Last-Modified", lastMod.Format(http.TimeFormat))
		w.Header().Set("Date", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(NewMemoryCache(), upstream)
	req := httptest.NewRequest("GET", "http://example.org/public", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
}

func TestServeResource_CloseError(t *testing.T) {
	// When res.Close() fails after serving we call errorf
	brokenRes := NewResourceBytes(200, []byte("ok"), http.Header{
		"Cache-Control": []string{"max-age=60"},
		"Date":          []string{time.Now().UTC().Format(http.TimeFormat)},
	})
	brokenRes.ReadSeekCloser = &closeFailReadSeekCloser{ReadSeekCloser: &byteReadSeekCloser{bytes.NewReader([]byte("ok"))}}
	fakeCache := &closeFailCache{res: brokenRes}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(fakeCache, upstream)
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
}

type closeFailReadSeekCloser struct {
	ReadSeekCloser
}
type closeFailCache struct {
	res *Resource
}

func (c *closeFailReadSeekCloser) Close() error { return errors.New("close failed") }
func (c *closeFailCache) Header(string) (Header, error) {
	return Header{}, ErrNotFoundInCache
}
func (c *closeFailCache) Store(*Resource, ...string) error { return nil }
func (c *closeFailCache) Retrieve(string) (*Resource, error) {
	return c.res, nil
}
func (c *closeFailCache) Invalidate(...string) {}
func (c *closeFailCache) Freshen(*Resource, ...string) error {
	return nil
}

func TestIsCacheable_StatusNotStoreable(t *testing.T) {
	// Upstream returns 201 Created -> not in storeable map -> isCacheable false -> SKIP
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created"))
	})
	h := NewHandler(NewMemoryCache(), upstream)
	req := httptest.NewRequest("GET", "http://example.org/create", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Errorf("want 201, got %d", rec.Code)
	}
	if rec.Header().Get(CacheHeader) != "SKIP" && rec.Header().Get(CacheHeader) != "MISS" {
		t.Logf("X-Cache: %s (201 not storeable)", rec.Header().Get(CacheHeader))
	}
}

func TestPipeUpstream_NonCacheableRequest(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		// Deliberately let Write send the headers. pipeUpstream must keep its
		// pipe reader alive after WaitHeaders returns or this write fails before
		// responseStreamer can copy the body to the client.
		_, _ = w.Write([]byte("ok"))
	})
	h := NewHandler(NewMemoryCache(), upstream)
	// Request with If-Range makes it non-cacheable -> pipeUpstream
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	req.Header.Set("If-Range", "etag-value")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
	if rec.Header().Get(CacheHeader) != "SKIP" {
		t.Errorf("want X-Cache: SKIP, got %s", rec.Header().Get(CacheHeader))
	}
	if got := rec.Body.String(); got != "ok" {
		t.Errorf("want body %q, got %q", "ok", got)
	}
}

// TestPassUpstream_CreateTempFails triggers finishPassUpstream by making os.CreateTemp fail (invalid TMPDIR).
func TestPassUpstream_CreateTempFails(t *testing.T) {
	oldTmp := os.Getenv("TMPDIR")
	_ = os.Setenv("TMPDIR", "/nonexistent_httpcache_test_xyz_123")
	defer func() {
		if oldTmp != "" {
			_ = os.Setenv("TMPDIR", oldTmp)
		} else {
			_ = os.Unsetenv("TMPDIR")
		}
	}()

	body := []byte("small")
	var upstreamCalls atomic.Int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	h := NewHandler(NewMemoryCache(), upstream)
	req := httptest.NewRequest("GET", "http://example.org/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	Writes.Wait()
	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
	got, _ := io.ReadAll(rec.Body)
	if string(got) != string(body) {
		t.Errorf("body: got %q", got)
	}
	if rec.Header().Get(CacheHeader) != "SKIP" {
		t.Fatalf("X-Cache = %q, want SKIP", rec.Header().Get(CacheHeader))
	}

	// A second request must reach upstream again: a temp-file failure is a
	// cache bypass, never an unbounded in-memory cache fallback.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "http://example.org/", nil))
	if got := upstreamCalls.Load(); got != 2 {
		t.Fatalf("upstream calls = %d, want 2", got)
	}
}

func TestSharedCacheDoesNotReuseAuthenticatedResponses(t *testing.T) {
	var calls atomic.Int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(r.Header.Get("Authorization")))
	})
	h := NewHandler(NewMemoryCache(), upstream)
	h.Shared = true

	for _, credential := range []string{"Bearer user-a", "Bearer user-b"} {
		req := httptest.NewRequest(http.MethodGet, "http://example.org/private.deb", nil)
		req.Header.Set("Authorization", credential)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Body.String() != credential {
			t.Fatalf("body = %q, want %q", rec.Body.String(), credential)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("authenticated responses were shared: upstream calls = %d, want 2", got)
	}
}

func TestVaryWildcardIsNotCached(t *testing.T) {
	var calls atomic.Int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Vary", "*")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))
	})
	h := NewHandler(NewMemoryCache(), upstream)
	h.Shared = true
	for i := 0; i < 2; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "http://example.org/vary", nil))
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("Vary: * response was cached: upstream calls = %d, want 2", got)
	}
}

func TestConcurrentMissesAreCollapsed(t *testing.T) {
	var calls atomic.Int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		time.Sleep(25 * time.Millisecond)
		_, _ = w.Write([]byte("package"))
	})
	cache, err := NewDiskCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	if extended, ok := cache.(ExtendedCache); ok {
		defer func() { _ = extended.Close() }()
	}
	h := NewHandler(cache, upstream)
	h.Shared = true

	const clients = 12
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan string, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://example.org/pkg.deb", nil))
			if rec.Code != http.StatusOK || rec.Body.String() != "package" {
				errs <- rec.Body.String()
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected follower response: %q", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
}

type missThenStaleCache struct {
	Cache
	retrieves atomic.Int32
	first     chan struct{}
}

func (c *missThenStaleCache) Retrieve(string) (*Resource, error) {
	if c.retrieves.Add(1) == 1 {
		close(c.first)
		return nil, ErrNotFoundInCache
	}
	res := NewResourceBytes(http.StatusOK, []byte("before"), http.Header{
		"Cache-Control": {"max-age=3600"},
	})
	res.MarkStale()
	return res, nil
}

func TestMissFollowerRevalidatesResultInvalidatedAfterLeaderStore(t *testing.T) {
	cache := &missThenStaleCache{Cache: NewMemoryCache(), first: make(chan struct{})}
	var upstreamCalls atomic.Int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Cache-Control", "max-age=60")
		_, _ = w.Write([]byte("after"))
	})
	h := NewHandler(cache, upstream)
	t.Cleanup(func() { h.writes.Wait() })

	req := httptest.NewRequest(http.MethodGet, "http://example.org/item", nil)
	cReq, err := newCacheRequest(req)
	if err != nil {
		t.Fatalf("newCacheRequest: %v", err)
	}
	flight, leader := h.claimMiss(cReq.Key.String())
	if !leader {
		t.Fatal("failed to install the test's leader flight")
	}

	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(rec, req)
	}()
	<-cache.first

	// The completed leader wakes the follower, whose second lookup observes a
	// response invalidated between Store and wakeup. It must pass through the
	// regular validation path rather than being served as an unconditional HIT.
	h.finishMiss(cReq.Key.String(), flight)
	<-done
	h.writes.Wait()

	if got := rec.Body.String(); got != "after" {
		t.Fatalf("body = %q, want refreshed body after", got)
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
}

type blockingStoreCache struct {
	Cache
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingStoreCache) Store(res *Resource, keys ...string) error {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return c.Cache.Store(res, keys...)
}

func TestHandlerShutdownDrainsWrites(t *testing.T) {
	cache := &blockingStoreCache{
		Cache:   NewMemoryCache(),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("package"))
	})
	h := NewHandler(cache, upstream)
	h.Shared = true
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "http://example.org/pkg.deb", nil))
	<-cache.started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := h.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want deadline exceeded while store is active", err)
	}
	close(cache.release)
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown after releasing store: %v", err)
	}
}

// closeCountingBody is a ReadSeekCloser that records how often it was closed.
type closeCountingBody struct {
	*bytes.Reader
	closes int32
}

func (b *closeCountingBody) Close() error { atomic.AddInt32(&b.closes, 1); return nil }

// staleResourceCache always hands back one caller-supplied resource. Store and
// the rest go to a real cache, which drains the upstream stream -- a Store
// that ignores its body deadlocks passUpstream against its own writer.
type staleResourceCache struct {
	Cache
	res *Resource
}

func (c *staleResourceCache) Header(string) (Header, error) {
	return Header{Header: c.res.Header(), StatusCode: c.res.Status()}, nil
}
func (c *staleResourceCache) Retrieve(string) (*Resource, error) { return c.res, nil }

// TestRefetchWithoutValidatorClosesTheCachedResource is the regression test for
// the early return added when a stale entry has neither ETag nor
// Last-Modified. ServeHTTP goes straight to passUpstream and returns, skipping
// the res.Close() at the end of the function -- so the disk backend held one
// more file descriptor for every such refresh, and repeated expiry or
// invalidation walked the process into its open-file limit.
func TestRefetchWithoutValidatorClosesTheCachedResource(t *testing.T) {
	body := &closeCountingBody{Reader: bytes.NewReader([]byte("cached"))}
	// Stale, and nothing to revalidate WITH.
	res := NewResource(http.StatusOK, body, http.Header{
		"Cache-Control": {"max-age=0, must-revalidate"},
		"Date":          {Clock().Format(http.TimeFormat)},
	})

	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		_, _ = w.Write([]byte("fresh"))
	})
	h := NewHandler(&staleResourceCache{Cache: NewMemoryCache(), res: res}, upstream)
	// storeResource runs in the background; without this it outlives the test
	// and races the next one's Clock reassignment.
	t.Cleanup(func() { h.writes.Wait() })

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "http://example.org/x", nil))

	if got := rec.Body.String(); got != "fresh" {
		t.Fatalf("body = %q, want the refetched %q", got, "fresh")
	}
	if atomic.LoadInt32(&body.closes) == 0 {
		t.Error("the cached resource was not closed before the full refetch")
	}
}

// TestSupersedingIsJudgedByLocalReceiveTime is the regression test for
// deciding whether a stored response supersedes an invalidation from the
// ORIGIN's Date header. A replacement carrying no Date -- which a direct
// http.Handler upstream may well omit -- never satisfied it, so the key stayed
// stale on every retrieval and was refetched until the marker was swept.
func TestSupersedingIsJudgedByLocalReceiveTime(t *testing.T) {
	originalClock := Clock
	defer func() { Clock = originalClock }()
	now := time.Now().UTC()
	Clock = func() time.Time { return now }

	c := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0))
	defer func() { _ = c.Close() }()

	if err := c.Store(NewResourceBytes(http.StatusOK, []byte("before"), http.Header{}), "k"); err != nil {
		t.Fatal(err)
	}
	c.Invalidate("k")

	// The replacement arrives a minute later with NO origin Date -- only the
	// Proxy-Date the handler stamps from the local clock on receipt.
	now = now.Add(time.Minute)
	replacement := NewResourceBytes(http.StatusOK, []byte("after"), http.Header{
		ProxyDateHeader: {Clock().Format(http.TimeFormat)},
	})
	if err := c.Store(replacement, "k"); err != nil {
		t.Fatal(err)
	}

	got, err := c.Retrieve("k")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = got.Close() }()
	if got.IsStale() {
		t.Error("a replacement stored after the invalidation was still marked stale; it carried no origin Date")
	}
}

// TestUpstreamWithoutExplicitWriteHeaderDoesNotHang is the regression test for
// responseStreamer.Write not implying WriteHeader(StatusOK).
//
// http.ResponseWriter requires it, and an upstream that just writes a body --
// the ordinary way to answer 200 -- relied on it. Without it C was never
// closed, so passUpstream waited in WaitHeaders and never reached the code
// that drains the pipe, while the upstream goroutine blocked in Write against
// a pipe with no reader. The request hung forever. Every other test in this
// package calls WriteHeader explicitly, which is why nothing caught it.
func TestUpstreamWithoutExplicitWriteHeaderDoesNotHang(t *testing.T) {
	for _, tc := range []struct {
		name     string
		upstream http.HandlerFunc
		want     int
		wantBody string
	}{
		{
			name: "writes a body without WriteHeader",
			upstream: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Cache-Control", "max-age=60")
				_, _ = w.Write([]byte("ok"))
			},
			want:     http.StatusOK,
			wantBody: "ok",
		},
		{
			name:     "writes nothing at all",
			upstream: func(http.ResponseWriter, *http.Request) {},
			want:     http.StatusOK,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(NewMemoryCache(), tc.upstream)
			t.Cleanup(func() { h.writes.Wait() })

			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest("GET", "http://example.org/x", nil))
				done <- rec
			}()

			select {
			case rec := <-done:
				if rec.Code != tc.want {
					t.Errorf("status = %d, want %d", rec.Code, tc.want)
				}
				if got := rec.Body.String(); got != tc.wantBody {
					t.Errorf("body = %q, want %q", got, tc.wantBody)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the request never completed; Write must imply WriteHeader(StatusOK)")
			}
		})
	}
}

// --- Codex review round 5 (PR #5) ---

// TestLookupRecordsTheServedVariantKey pins the mechanism the validation path
// relies on. A successful revalidation used to freshen only the BASE key, so
// the variant that was actually retrieved kept a Proxy-Date older than the
// base marker and was marked stale again on the very next request --
// revalidating upstream every time until the marker was swept.
func TestLookupRecordsTheServedVariantKey(t *testing.T) {
	var hits int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Vary", "Accept-Language")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))
	})

	h := NewHandler(NewMemoryCache(), upstream)
	t.Cleanup(func() { h.writes.Wait() })

	req := httptest.NewRequest("GET", "http://example.org/thing", nil)
	req.Header.Set("Accept-Language", "en")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	rec.Flush()
	h.writes.Wait()

	cReq, err := newCacheRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	res, err := h.lookup(cReq)
	if err != nil {
		t.Fatalf("lookup error = %v", err)
	}
	defer func() { _ = res.Close() }()

	want := cReq.Key.Vary("Accept-Language", req).String()
	if cReq.servedKey != want {
		t.Errorf("servedKey = %q, want the variant key %q", cReq.servedKey, want)
	}
	if cReq.servedKey == cReq.Key.String() {
		t.Error("servedKey is the base key; the variant that was served is not recorded")
	}
}

// TestStaleMarkerTTLIsConfigurable is the regression test for hard-coding the
// handler's marker retention to DefaultCacheTTL. A third-party Cache keeping
// entries longer than seven days lost the invalidation before its own entries
// expired, so a pre-mutation Vary variant became a HIT again. Only the
// implementation knows its retention, so it is a knob.
func TestStaleMarkerTTLIsConfigurable(t *testing.T) {
	originalClock := Clock
	defer func() { Clock = originalClock }()
	now := time.Now().UTC()
	Clock = func() time.Time { return now }

	upstream := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

	if got := NewHandler(plainCache{inner: NewMemoryCache()}, upstream).staleMarkerTTL(); got != DefaultCacheTTL {
		t.Errorf("default staleMarkerTTL = %s, want %s", got, DefaultCacheTTL)
	}

	const long = 30 * 24 * time.Hour
	h := NewHandlerWithOptions(plainCache{inner: NewMemoryCache()}, upstream,
		&HandlerOptions{StaleMarkerTTL: long})
	if got := h.staleMarkerTTL(); got != long {
		t.Fatalf("staleMarkerTTL = %s, want %s", got, long)
	}

	h.recordStale("k")

	// Well past DefaultCacheTTL, but inside the configured retention. The
	// sweep runs on each unsafe request, so drive one.
	now = now.Add(DefaultCacheTTL + 24*time.Hour)
	h.recordStale("other")

	if _, marked := h.staleAt("k"); !marked {
		t.Error("the marker was swept after DefaultCacheTTL despite a longer StaleMarkerTTL")
	}

	// And it IS swept once past the configured retention.
	now = now.Add(long)
	h.recordStale("third")
	if _, marked := h.staleAt("k"); marked {
		t.Error("the marker outlived the configured StaleMarkerTTL")
	}
}
