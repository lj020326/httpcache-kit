package httpcache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soulteary/vfs-kit"
)

// TestInvalidationKeys covers the key set RFC 7234 section 4.4 requires a
// successful unsafe request to invalidate.
func TestInvalidationKeys(t *testing.T) {
	newReq := func(method, target string) *cacheRequest {
		r := httptest.NewRequest(method, target, nil)
		cr, err := newCacheRequest(r)
		if err != nil {
			t.Fatal(err)
		}
		return cr
	}
	resWith := func(header http.Header) *Resource {
		return NewResourceBytes(http.StatusOK, nil, header)
	}
	has := func(keys []string, want string) bool {
		for _, k := range keys {
			if k == want {
				return true
			}
		}
		return false
	}

	t.Run("effective request URI, GET and HEAD", func(t *testing.T) {
		r := newReq("POST", "http://example.org/orders")
		keys := invalidationKeys(resWith(http.Header{}), r)

		base := NewKey("GET", r.URL, r.Header)
		for _, want := range []string{base.String(), base.ForMethod("HEAD").String()} {
			if !has(keys, want) {
				t.Errorf("missing key %q in %q", want, keys)
			}
		}
	})

	t.Run("same-origin Location and Content-Location", func(t *testing.T) {
		r := newReq("POST", "http://example.org/orders")
		h := http.Header{}
		h.Set("Location", "/orders/1")
		h.Set("Content-Location", "http://example.org/orders/summary")
		keys := invalidationKeys(resWith(h), r)

		for _, path := range []string{"/orders/1", "/orders/summary"} {
			want := NewKey("GET", newReq("GET", "http://example.org"+path).URL, r.Header).String()
			if !has(keys, want) {
				t.Errorf("missing key %q for %s in %q", want, path, keys)
			}
		}
	})

	t.Run("cross-origin targets are ignored", func(t *testing.T) {
		r := newReq("POST", "http://example.org/orders")
		h := http.Header{}
		h.Set("Location", "http://evil.example/victim")
		h.Set("Content-Location", "http://other.example/victim")
		keys := invalidationKeys(resWith(h), r)

		for _, k := range keys {
			if strings.Contains(k, "evil.example") || strings.Contains(k, "other.example") {
				t.Errorf("cross-origin key %q must not be invalidated (keys: %q)", k, keys)
			}
		}
		if len(keys) != 2 {
			t.Errorf("got %d keys %q, want only the request URI's GET and HEAD keys", len(keys), keys)
		}
	})
}

// TestUnsafeRequestInvalidatesCachedGET is the end-to-end regression test:
// invalidateResource used to only log, and later recognized just four common
// mutation methods, so a resource changed by an unsafe extension method kept
// being served from cache without revalidation.
func TestUnsafeRequestInvalidatesCachedGET(t *testing.T) {
	var upstreamHits int
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		w.Header().Set("Cache-Control", "max-age=3600")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))
	})

	h := NewHandler(NewMemoryCache(), upstream)
	// Storing runs on a tracked background goroutine. Draining it before the
	// test returns keeps it from reading the package-level Clock while a later
	// test writes it.
	t.Cleanup(func() { h.writes.Wait() })

	do := func(method, target string) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
		rec.Flush()
	}

	do("GET", "http://example.org/thing") // MISS, stores
	before := upstreamHits
	do("GET", "http://example.org/thing") // HIT, must not reach upstream
	if upstreamHits != before {
		t.Fatalf("second GET reached upstream %d time(s); entry was not cached", upstreamHits-before)
	}

	// No h.writes.Wait() here on purpose: invalidation must be installed
	// before the unsafe request's ServeHTTP returns. Waiting on the write
	// group masked the window in which the mutation had been answered while
	// the previous representation was still considered fresh.
	do("PROPPATCH", "http://example.org/thing")

	hitsBeforeFinalGet := upstreamHits
	do("GET", "http://example.org/thing")
	if upstreamHits == hitsBeforeFinalGet {
		t.Error("GET after PROPPATCH was served from cache without revalidation; the entry was never invalidated")
	}
}

// TestUnsafeRequestInvalidatesBeforeBodyCompletes covers waiting for
// responseStreamer.Resource to drain an unsafe response before invalidating.
// A successful status is already visible to the client at that point, and a
// slow or streaming mutation body must not leave the old representation fresh.
func TestUnsafeRequestInvalidatesBeforeBodyCompletes(t *testing.T) {
	statusWritten := make(chan struct{})
	releaseBody := make(chan struct{})
	defer func() {
		select {
		case <-releaseBody:
		default:
			close(releaseBody)
		}
	}()

	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		w.WriteHeader(http.StatusOK)
		if r.Method != "GET" {
			close(statusWritten)
			<-releaseBody
		}
		_, _ = w.Write([]byte("body"))
	})

	c := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	h := NewHandler(c, upstream)
	t.Cleanup(func() { h.writes.Wait() })

	target := "http://example.org/slow-mutation"
	seed := httptest.NewRecorder()
	h.ServeHTTP(seed, httptest.NewRequest("GET", target, nil))
	h.writes.Wait()

	mutationDone := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", target, nil))
		close(mutationDone)
	}()

	select {
	case <-statusWritten:
	case <-time.After(time.Second):
		t.Fatal("mutation did not publish its response status")
	}

	key := NewRequestKey(httptest.NewRequest("GET", target, nil)).String()
	deadline := time.After(time.Second)
	for {
		if at, ok := c.StaleAt(key); ok && !at.IsZero() {
			break
		}
		select {
		case <-deadline:
			t.Fatal("mutation remained unmarked while its response body was blocked")
		case <-time.After(time.Millisecond):
		}
	}
	res, err := c.Retrieve(key)
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsStale() {
		t.Error("cached representation remained fresh after the mutation status was published")
	}
	_ = res.Close()

	close(releaseBody)
	select {
	case <-mutationDone:
	case <-time.After(time.Second):
		t.Fatal("mutation did not finish after its body was released")
	}
}

// --- Codex review follow-ups (PR #5) ---

// TestInvalidationCoversVaryVariants is the regression test for invalidation
// reaching only the base GET/HEAD keys. storeResource writes both the base key
// and a Key.Vary(...) key, and lookup serves the VARIED entry, so a request
// whose Vary headers matched an existing variant went on being served the
// pre-mutation representation without revalidating after a successful POST.
func TestInvalidationCoversVaryVariants(t *testing.T) {
	var upstreamHits int
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Vary", "Accept-Language")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))
	})

	h := NewHandler(NewMemoryCache(), upstream)
	t.Cleanup(func() { h.writes.Wait() })

	get := func(lang string) {
		req := httptest.NewRequest("GET", "http://example.org/thing", nil)
		req.Header.Set("Accept-Language", lang)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		rec.Flush()
	}

	get("en") // MISS, stores base + the "en" variant
	h.writes.Wait()

	before := upstreamHits
	get("en") // HIT on the variant
	if upstreamHits != before {
		t.Fatalf("second GET reached upstream; the variant was not cached")
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "http://example.org/thing", nil))
	rec.Flush()

	before = upstreamHits
	get("en")
	if upstreamHits == before {
		t.Error("GET after POST was served from the Vary variant without revalidation; only the base key was invalidated")
	}
}

// TestValidateRefusesWithoutAValidator is the regression test for treating a
// header comparison as revalidation. With neither ETag nor Last-Modified on
// the stored response, Validate issued an unconditional request, ignored the
// body that came back, and headersEqual returned true whenever the new
// response carried none of its four comparison headers -- so the OLD body was
// served and freshened even though the resource had changed.
func TestValidateRefusesWithoutAValidator(t *testing.T) {
	var served []byte
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(served)
	})

	v := &Validator{Handler: upstream}

	// No validator on the stored response.
	stored := NewResource(http.StatusOK, nopSeekCloser{strings.NewReader("old")}, http.Header{
		"Cache-Control": []string{"max-age=3600"},
	})
	served = []byte("new")

	req := httptest.NewRequest("GET", "http://example.org/thing", nil)
	if v.Validate(req, stored) {
		t.Error("Validate reported success with no ETag or Last-Modified to validate with; the changed body would be served from cache")
	}

	// With an ETag it validates normally again.
	withETag := NewResource(http.StatusOK, nopSeekCloser{strings.NewReader("old")}, http.Header{
		"Cache-Control": []string{"max-age=3600"},
		"Etag":          []string{`"v1"`},
	})
	etagUpstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == "" {
			t.Error("Validate did not send If-None-Match for a response carrying an ETag")
		}
		w.Header().Set("Etag", `"v1"`)
		w.WriteHeader(http.StatusNotModified)
	})
	if !(&Validator{Handler: etagUpstream}).Validate(req, withETag) {
		t.Error("Validate rejected an unchanged response carrying a matching ETag")
	}
}

// nopSeekCloser adapts a strings.Reader to the ReadSeekCloser a Resource needs.
type nopSeekCloser struct{ *strings.Reader }

func (nopSeekCloser) Close() error { return nil }

// --- Codex review round 2 (PR #5) ---

// TestInvalidationSurvivesRefetchingOneVariant is the regression test for
// propagating the base entry's boolean staleness. Refetching one variant
// rewrites the BASE entry too, and Store used to clear the marker with it, so
// every other pre-mutation variant became a HIT again.
func TestInvalidationSurvivesRefetchingOneVariant(t *testing.T) {
	var upstreamHits int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Vary", "Accept-Language")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))
	})

	h := NewHandler(NewMemoryCache(), upstream)
	t.Cleanup(func() { h.writes.Wait() })

	get := func(lang string) {
		req := httptest.NewRequest("GET", "http://example.org/thing", nil)
		req.Header.Set("Accept-Language", lang)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		rec.Flush()
	}

	// Two variants stored.
	get("en")
	h.writes.Wait()
	get("fr")
	h.writes.Wait()

	before := atomic.LoadInt32(&upstreamHits)
	get("en")
	get("fr")
	if atomic.LoadInt32(&upstreamHits) != before {
		t.Fatal("a variant was not cached")
	}

	// Mutate, then refetch only the "en" variant.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "http://example.org/thing", nil))
	rec.Flush()

	before = atomic.LoadInt32(&upstreamHits)
	get("en")
	if atomic.LoadInt32(&upstreamHits) == before {
		t.Fatal("the en variant was served from cache after the POST")
	}
	h.writes.Wait()

	// "fr" must STILL revalidate: replacing one variant does not un-invalidate
	// the others.
	before = atomic.LoadInt32(&upstreamHits)
	get("fr")
	if atomic.LoadInt32(&upstreamHits) == before {
		t.Error("the fr variant was served from cache after the POST; refetching the en variant cleared the invalidation")
	}
}

// TestOlderStoreCannotClearANewerInvalidation is the regression test for Store
// deleting the stale marker. A cacheable GET whose background store was still
// in flight when a mutation completed erased the newer invalidation and
// republished the pre-mutation response as fresh.
func TestOlderStoreCannotClearANewerInvalidation(t *testing.T) {
	c := NewMemoryCache()

	const key = "GET:http://example.org/thing"
	hdr := http.Header{"Cache-Control": []string{"max-age=3600"}}

	// A response that predates the invalidation, as an in-flight store would.
	old := NewResource(http.StatusOK, nopSeekCloser{strings.NewReader("old")}, hdr.Clone())
	old.Header().Set("Date", Clock().Add(-time.Minute).Format(http.TimeFormat))

	c.Invalidate(key)
	if err := c.Store(old, key); err != nil {
		t.Fatalf("Store error = %v", err)
	}

	// Not served as fresh, by either route: the store is refused outright
	// (which is what happens now -- an entry the marker already supersedes is
	// not worth caching), or it lands and is marked stale.
	got, err := c.Retrieve(key)
	if err == ErrNotFoundInCache {
		return
	}
	if err != nil {
		t.Fatalf("Retrieve error = %v", err)
	}
	defer func() { _ = got.Close() }()

	if !got.IsStale() {
		t.Error("a store that predates the invalidation republished the entry as fresh")
	}
}

// TestInvalidationKeysPreserveEscapedPaths: url.Parse records "/objects/a%2Fb"
// in RawPath and the decoded "/objects/a/b" in Path, so dropping RawPath while
// resolving Location produced an invalidation key for a different resource
// than the one a direct request is cached under.
func TestInvalidationKeysPreserveEscapedPaths(t *testing.T) {
	r := httptest.NewRequest("POST", "http://example.org/objects", nil)
	cr, err := newCacheRequest(r)
	if err != nil {
		t.Fatal(err)
	}

	u := cr.sameOriginURL("/objects/a%2Fb")
	if u == nil {
		t.Fatal("sameOriginURL returned nil for a same-origin Location")
	}
	if got := u.EscapedPath(); got != "/objects/a%2Fb" {
		t.Errorf("EscapedPath = %q, want /objects/a%%2Fb -- the escaped spelling is the cache-key identity", got)
	}
}

// TestInvalidationTargetsRequireTheSameScheme keeps URI invalidation within
// the effective request origin. Matching host names are insufficient when the
// schemes (and therefore their default ports) differ.
func TestInvalidationTargetsRequireTheSameScheme(t *testing.T) {
	r := httptest.NewRequest("POST", "http://example.org/objects", nil)
	cr, err := newCacheRequest(r)
	if err != nil {
		t.Fatal(err)
	}

	if u := cr.sameOriginURL("https://example.org/item"); u != nil {
		t.Fatalf("cross-scheme target = %#v, want nil", u)
	}
	u := cr.sameOriginURL("http://example.org/item")
	if u == nil || u.Scheme != "http" {
		t.Fatalf("same-origin target = %#v, want an HTTP URL", u)
	}

	// A relative target still inherits the request's scheme.
	if rel := cr.sameOriginURL("/item"); rel == nil || rel.Scheme != "http" {
		t.Errorf("relative target scheme = %v, want http", rel)
	}
}

// TestInvalidationOriginNormalizesDefaultPorts is the regression test for
// comparing raw authorities. example.org and example.org:80 identify the same
// HTTP origin, while an explicitly different effective port does not.
func TestInvalidationOriginNormalizesDefaultPorts(t *testing.T) {
	r := httptest.NewRequest("POST", "/objects", nil)
	r.Host = "example.org"
	cr, err := newCacheRequest(r)
	if err != nil {
		t.Fatal(err)
	}

	if u := cr.sameOriginURL("http://example.org:80/item"); u == nil {
		t.Fatal("explicit HTTP default port was treated as cross-origin")
	}
	if u := cr.sameOriginURL("http://example.org:8080/item"); u != nil {
		t.Fatalf("different effective port target = %#v, want nil", u)
	}

	absolute := httptest.NewRequest("POST", "http://example.org/objects", nil)
	absoluteRequest, err := newCacheRequest(absolute)
	if err != nil {
		t.Fatal(err)
	}
	target := absoluteRequest.sameOriginURL("http://example.org:80/item")
	if target == nil {
		t.Fatal("absolute-form default-port target was treated as cross-origin")
	}
	if target.Host != "example.org:80" {
		t.Fatalf("target authority = %q, want example.org:80", target.Host)
	}
	direct := httptest.NewRequest("GET", "http://example.org:80/item", nil)
	if got, want := NewKey("GET", target, absolute.Header).String(), NewRequestKey(direct).String(); got != want {
		t.Errorf("invalidation key = %q, direct target key = %q", got, want)
	}
}

// TestAbsoluteInvalidationTargetMatchesOriginFormRequest is the regression
// test for copying an absolute Location's scheme into an origin-form request
// URL. That created "http:/item", while a direct request is keyed as "/item",
// so the representation named by Location remained fresh.
func TestAbsoluteInvalidationTargetMatchesOriginFormRequest(t *testing.T) {
	r := httptest.NewRequest("POST", "/objects", nil)
	r.Host = "example.org"
	cr, err := newCacheRequest(r)
	if err != nil {
		t.Fatal(err)
	}

	u := cr.sameOriginURL("http://example.org/item")
	if u == nil {
		t.Fatal("sameOriginURL returned nil for a same-origin absolute Location")
	}
	if u.Scheme != "" || u.Host != "" || u.Path != "/item" {
		t.Fatalf("target URL = %#v, want origin-form /item", u)
	}

	direct := httptest.NewRequest("GET", "/item", nil)
	direct.Host = r.Host
	if got, want := NewKey("GET", u, r.Header).String(), NewRequestKey(direct).String(); got != want {
		t.Errorf("invalidation key = %q, direct request key = %q", got, want)
	}

	root := cr.sameOriginURL("http://example.org?q")
	if root == nil {
		t.Fatal("sameOriginURL returned nil for an absolute root target")
	}
	if root.Path != "/" || root.RawQuery != "q" {
		t.Fatalf("empty-path absolute target = %#v, want origin-form /?q", root)
	}
	directRoot := httptest.NewRequest("GET", "/?q", nil)
	directRoot.Host = r.Host
	if got, want := NewKey("GET", root, r.Header).String(), NewRequestKey(directRoot).String(); got != want {
		t.Errorf("root invalidation key = %q, direct request key = %q", got, want)
	}
}

// TestStaleMarkerOutlivesTheEntriesItJudges is the regression test for sweeping
// invalidation markers on StaleMapTTL alone. The marker is the only record
// that entries older than it are pre-mutation, so with the 24 hour default
// against a 7 day cache TTL an infrequently requested representation came back
// as a fresh HIT for the six days after its marker was swept.
func TestStaleMarkerOutlivesTheEntriesItJudges(t *testing.T) {
	originalClock := Clock
	defer func() { Clock = originalClock }()
	now := time.Now().UTC()
	Clock = func() time.Time { return now }

	config := DefaultCacheConfig(). // TTL 7 days, StaleMapTTL 24 hours
					WithCleanupInterval(0)
	c := NewMemoryCacheWithConfig(config)
	defer func() { _ = c.Close() }()

	res := NewResourceBytes(http.StatusOK, []byte("before"), http.Header{})
	if err := c.Store(res, "testkey"); err != nil {
		t.Fatal(err)
	}
	c.Invalidate("testkey")

	// Two days on: past StaleMapTTL, nowhere near the entry's TTL.
	now = now.Add(48 * time.Hour)
	c.Cleanup()

	if got := c.Stats().StaleCount; got != 1 {
		t.Fatalf("stale markers after cleanup = %d, want 1; the entry it judges lives for 7 days", got)
	}

	got, err := c.Retrieve("testkey")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = got.Close() }()
	if !got.IsStale() {
		t.Error("the pre-mutation entry came back fresh; its invalidation marker was swept out from under it")
	}
}

// --- Codex review round 4 (PR #5) ---

// TestBaseMarkerSurvivesBaseEntryEviction is the regression test for deleting
// the invalidation marker in removeEntry. The base key's marker is what every
// Vary VARIANT is judged against, and variants are tracked and evicted
// independently -- so dropping it because LRU eviction happened to take the
// base entry left the surviving pre-mutation variants with nothing marking
// them stale.
func TestBaseMarkerSurvivesBaseEntryEviction(t *testing.T) {
	originalClock := Clock
	defer func() { Clock = originalClock }()
	now := time.Now().UTC()
	Clock = func() time.Time { return now }

	c := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0))
	defer func() { _ = c.Close() }()

	if err := c.Store(NewResourceBytes(http.StatusOK, []byte("base"), http.Header{}), "base"); err != nil {
		t.Fatal(err)
	}
	c.Invalidate("base")

	// The base entry is evicted; its marker must remain.
	inner, ok := c.(*cache)
	if !ok {
		t.Fatalf("cache is %T, want *cache", c)
	}
	inner.lruMutex.Lock()
	for _, entry := range inner.lruIndex {
		_ = inner.removeEntry(entry)
	}
	inner.lruMutex.Unlock()

	if _, marked := inner.StaleAt("base"); !marked {
		t.Error("evicting the base entry removed the marker its Vary variants are judged against")
	}
}

// TestInvalidationKeysPreserveForceQuery is the regression test for dropping
// url.URL.ForceQuery. "/item?" and "/item" are different cache keys to
// NewRequestKey, and target starts as a clone of the mutation's URL -- so the
// flag was both lost where it was wanted and inherited where it was not.
func TestInvalidationKeysPreserveForceQuery(t *testing.T) {
	plain := httptest.NewRequest("POST", "http://example.org/objects", nil)
	cr, err := newCacheRequest(plain)
	if err != nil {
		t.Fatal(err)
	}
	if u := cr.sameOriginURL("/item?"); u == nil || !u.ForceQuery {
		t.Errorf("sameOriginURL(%q).ForceQuery = %v, want true", "/item?", u)
	}

	// And not inherited from the mutation's own URL.
	forced := httptest.NewRequest("POST", "http://example.org/objects?", nil)
	cr2, err := newCacheRequest(forced)
	if err != nil {
		t.Fatal(err)
	}
	if u := cr2.sameOriginURL("/item"); u == nil || u.ForceQuery {
		t.Errorf("sameOriginURL(%q).ForceQuery = %v, want false", "/item", u)
	}
}

// plainCache is a Cache that does NOT implement staleAtChecker, standing in
// for a third-party implementation. It wraps the built-in one so the only
// thing missing is the StaleAt capability.
type plainCache struct {
	inner Cache
}

func (c plainCache) Header(key string) (Header, error)         { return c.inner.Header(key) }
func (c plainCache) Store(res *Resource, keys ...string) error { return c.inner.Store(res, keys...) }
func (c plainCache) Retrieve(key string) (*Resource, error)    { return c.inner.Retrieve(key) }
func (c plainCache) Invalidate(keys ...string)                 { c.inner.Invalidate(keys...) }
func (c plainCache) Freshen(res *Resource, keys ...string) error {
	return c.inner.Freshen(res, keys...)
}

// delayedStoreCache models an opaque third-party backend whose invalidation
// cannot order itself against a Store already in flight. The first Store is
// released only after the mutation has returned, so it overwrites the cache
// with pre-mutation content at a later file-write time.
type delayedStoreCache struct {
	inner   Cache
	delay   atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (c *delayedStoreCache) Header(key string) (Header, error) {
	return c.inner.Header(key)
}

func (c *delayedStoreCache) Store(res *Resource, keys ...string) error {
	if c.delay.CompareAndSwap(true, false) {
		close(c.started)
		<-c.release
	}
	return c.inner.Store(res, keys...)
}

func (c *delayedStoreCache) Retrieve(key string) (*Resource, error) {
	return c.inner.Retrieve(key)
}

func (c *delayedStoreCache) Invalidate(_ ...string) {}

func (c *delayedStoreCache) Freshen(res *Resource, keys ...string) error {
	return c.inner.Freshen(res, keys...)
}

// TestFallbackInvalidatesLateNonVaryStore covers the handler-side generation
// check being applied only after entering the Vary branch. An old ordinary
// response could otherwise finish storing after a mutation and be served as a
// fresh HIT by a third-party Cache that cannot expose invalidation timestamps.
func TestFallbackInvalidatesLateNonVaryStore(t *testing.T) {
	originalClock := Clock
	defer func() { Clock = originalClock }()
	now := time.Now().UTC()
	Clock = func() time.Time { return now }

	cache := &delayedStoreCache{
		inner:   NewMemoryCache(),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	cache.delay.Store(true)
	current := "before"
	var getCalls int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			current = "after"
			w.WriteHeader(http.StatusNoContent)
			return
		}
		atomic.AddInt32(&getCalls, 1)
		w.Header().Set("Cache-Control", "max-age=3600")
		_, _ = w.Write([]byte(current))
	})
	h := NewHandler(cache, upstream)
	t.Cleanup(func() { h.writes.Wait() })

	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest("GET", "http://example.org/thing", nil))
	<-cache.started

	now = now.Add(100 * time.Millisecond)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "http://example.org/thing", nil))
	// Let the old Store acquire a completion timestamp newer than the marker.
	// For an opaque cache that does not prove its ordering, this is not evidence
	// that the response fetch itself began after the mutation.
	now = now.Add(100 * time.Millisecond)
	close(cache.release)
	h.writes.Wait()

	second := httptest.NewRecorder()
	h.ServeHTTP(second, httptest.NewRequest("GET", "http://example.org/thing", nil))
	h.writes.Wait()
	if got := atomic.LoadInt32(&getCalls); got != 2 {
		t.Fatalf("upstream GET calls = %d, want 2; late old Store was served as a HIT", got)
	}
	if got := second.Body.String(); got != "after" {
		t.Errorf("response body = %q, want after", got)
	}
}

// impreciseCache models an existing third-party Cache implementation. It does
// not implement StaleAt and its Resources do not carry the optional
// full-precision storedAt value introduced by the built-in cache.
type impreciseCache struct{ plainCache }

func (c impreciseCache) Retrieve(key string) (*Resource, error) {
	res, err := c.inner.Retrieve(key)
	if res != nil {
		res.SetStoredAt(time.Time{})
	}
	return res, err
}

// hookedImpreciseCache advances a controlled clock after lookup but before
// conditional validation starts. It models time passing between those two
// operations without implementing staleAtChecker.
type hookedImpreciseCache struct {
	impreciseCache
	afterRetrieve func()
}

func (c *hookedImpreciseCache) Retrieve(key string) (*Resource, error) {
	res, err := c.impreciseCache.Retrieve(key)
	if c.afterRetrieve != nil {
		after := c.afterRetrieve
		c.afterRetrieve = nil
		after()
	}
	return res, err
}

// TestInvalidationSurvivesForACacheWithoutStaleAt is the regression test for
// falling back to the base ENTRY's state when a Cache cannot report
// invalidation times. Refetching one variant rewrites the base entry, so every
// other pre-mutation variant became a fresh HIT again -- the exact bug StaleAt
// closes, left open for third-party caches. The handler keeps its own markers
// for them now.
func TestInvalidationSurvivesForACacheWithoutStaleAt(t *testing.T) {
	var upstreamHits int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Vary", "Accept-Language")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))
	})

	// A controlled clock, because Proxy-Date is second-granular: without
	// stepping it, the POST and the refetch land in the same second and the
	// rewritten base entry still looks older than the marker -- which makes
	// even the broken base-entry fallback appear to work.
	originalClock := Clock
	t.Cleanup(func() { Clock = originalClock })
	now := time.Now().UTC()
	Clock = func() time.Time { return now }

	h := NewHandler(plainCache{inner: NewMemoryCache()}, upstream)
	t.Cleanup(func() { h.writes.Wait() })

	if _, ok := h.cache.(staleAtChecker); ok {
		t.Fatal("the fixture cache implements staleAtChecker; it must not")
	}

	get := func(lang string) {
		req := httptest.NewRequest("GET", "http://example.org/thing", nil)
		req.Header.Set("Accept-Language", lang)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		rec.Flush()
	}

	get("en")
	h.writes.Wait()
	get("fr")
	h.writes.Wait()

	before := atomic.LoadInt32(&upstreamHits)
	get("en")
	get("fr")
	if atomic.LoadInt32(&upstreamHits) != before {
		t.Fatal("a variant was not cached")
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "http://example.org/thing", nil))
	rec.Flush()
	h.writes.Wait()

	// A clear second later, so the refetched base entry is unambiguously
	// NEWER than the invalidation.
	now = now.Add(2 * time.Second)

	before = atomic.LoadInt32(&upstreamHits)
	get("en")
	if atomic.LoadInt32(&upstreamHits) == before {
		t.Fatal("the en variant was served from cache after the POST")
	}
	h.writes.Wait()

	before = atomic.LoadInt32(&upstreamHits)
	get("fr")
	if atomic.LoadInt32(&upstreamHits) == before {
		t.Error("the fr variant was served from cache after the POST; refetching en cleared the invalidation for a Cache without StaleAt")
	}
}

// TestThirdPartyVariantValidationUsesPreciseHandlerTime covers a Cache that
// cannot attach a full-precision storedAt value to returned Resources. HTTP
// Date and Proxy-Date carry only seconds, so a mutation and successful Vary
// validation within one second otherwise make the variant revalidate forever.
func TestThirdPartyVariantValidationUsesPreciseHandlerTime(t *testing.T) {
	originalClock := Clock
	defer func() { Clock = originalClock }()
	now := time.Now().UTC().Truncate(time.Second).Add(100 * time.Millisecond)
	Clock = func() time.Time { return now }

	var upstreamHits int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Vary", "Accept-Language")
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))
	})

	cache := &hookedImpreciseCache{impreciseCache: impreciseCache{plainCache{inner: NewMemoryCache()}}}
	h := NewHandler(cache, upstream)
	t.Cleanup(func() { h.writes.Wait() })
	get := func() {
		req := httptest.NewRequest("GET", "http://example.org/thing", nil)
		req.Header.Set("Accept-Language", "en")
		h.ServeHTTP(httptest.NewRecorder(), req)
	}

	get()
	h.writes.Wait()
	now = now.Add(100 * time.Millisecond)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "http://example.org/thing", nil))
	// The next request itself is received at the marker time. Cache retrieval
	// then takes 100ms, so validation starts later in the same HTTP-date second.
	// Recording the request's earlier cReq.Time would fail to clear the marker.
	cache.afterRetrieve = func() { now = now.Add(100 * time.Millisecond) }

	before := atomic.LoadInt32(&upstreamHits)
	get() // one conditional validation
	if atomic.LoadInt32(&upstreamHits) == before {
		t.Fatal("the invalidated third-party-cache variant was served without validation")
	}

	before = atomic.LoadInt32(&upstreamHits)
	get()
	get()
	if got := atomic.LoadInt32(&upstreamHits) - before; got != 0 {
		t.Errorf("%d further upstream request(s) after validation, want 0", got)
	}
}

// --- Codex review round 5 (PR #5) ---

// TestSupersededStoreIsNotCached is the regression test for retaining a
// response the marker already supersedes.
//
// A GET received BEFORE a mutation whose background Store lands after it has a
// Proxy-Date older than the marker but a storedAt that is newer, so it
// outlived markerTime + TTL and became a fresh HIT the moment cleanup removed
// the marker. Not storing it at all makes marker retention tractable: every
// stored entry either predates the marker and is evicted before it, or
// supersedes it.
func TestSupersededStoreIsNotCached(t *testing.T) {
	originalClock := Clock
	defer func() { Clock = originalClock }()
	now := time.Now().UTC()
	Clock = func() time.Time { return now }

	c := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0))
	defer func() { _ = c.Close() }()

	const key = "GET:http://example.org/thing"
	const variantKey = "GET:http://example.org/thing\x00vary\x00Accept-Language=\"en\""

	// Received a minute ago; the mutation happens now.
	stale := NewResourceBytes(http.StatusOK, []byte("before"), http.Header{
		ProxyDateHeader: {now.Add(-time.Minute).Format(http.TimeFormat)},
	})
	c.Invalidate(key)

	if err := c.Store(stale, key, variantKey); err != nil {
		t.Fatalf("Store error = %v", err)
	}
	for _, storedKey := range []string{key, variantKey} {
		if _, err := c.Retrieve(storedKey); err != ErrNotFoundInCache {
			t.Errorf("Retrieve(%q) = %v, want ErrNotFoundInCache; part of a superseded multi-key response was cached", storedKey, err)
		}
	}

	// A replacement received AFTER the mutation is stored normally.
	now = now.Add(2 * time.Second)
	fresh := NewResourceBytes(http.StatusOK, []byte("after"), http.Header{
		ProxyDateHeader: {now.Format(http.TimeFormat)},
	})
	if err := c.Store(fresh, key); err != nil {
		t.Fatalf("Store error = %v", err)
	}
	got, err := c.Retrieve(key)
	if err != nil {
		t.Fatalf("Retrieve error = %v", err)
	}
	defer func() { _ = got.Close() }()
	if got.IsStale() {
		t.Error("the replacement was marked stale")
	}
}

// TestFreshenTimestampSurvivesDiskRestart covers the full-precision local
// generation being updated only in the in-memory LRU. A successful validation
// does not rewrite the body, so a restart reconstructed storedAt from the old
// body mtime and made the already-validated entry stale again.
func TestFreshenTimestampSurvivesDiskRestart(t *testing.T) {
	originalClock := Clock
	defer func() { Clock = originalClock }()
	now := time.Now().UTC().Truncate(time.Second).Add(100 * time.Millisecond)
	Clock = func() time.Time { return now }

	dir := t.TempDir()
	const key = "GET:http://example.org/revalidated"
	hdr := http.Header{
		"Cache-Control": {"max-age=3600"},
		"ETag":          {`"v1"`},
	}

	first, err := NewDiskCacheWithConfig(dir, DefaultCacheConfig().WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Store(NewResourceBytes(http.StatusOK, []byte("body"), hdr.Clone()), key); err != nil {
		t.Fatal(err)
	}

	now = now.Add(100 * time.Millisecond)
	first.Invalidate(key)
	now = now.Add(100 * time.Millisecond) // deliberately still the same HTTP-date second
	validated := NewResourceBytes(http.StatusOK, nil, hdr.Clone())
	validated.RequestTime = now
	if err := first.Freshen(validated, key); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := NewDiskCacheWithConfig(dir, DefaultCacheConfig().WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	got, err := second.Retrieve(key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = got.Close() }()
	if got.IsStale() {
		t.Error("a successfully revalidated entry became stale again after disk-cache restart")
	}
}

// TestAbsoluteInvalidationTargetKeepsItsEmptyPath is the regression test for
// resolving an absolute reference as if it were relative. "Location:
// http://example.org?q" has an EMPTY path, but rebuilding it as a relative
// reference made it inherit the mutation's path -- so a mutation at /orders/1
// invalidated /orders/1?q and left the named representation fresh.
func TestAbsoluteInvalidationTargetKeepsItsEmptyPath(t *testing.T) {
	r := httptest.NewRequest("POST", "http://example.org/orders/1", nil)
	cr, err := newCacheRequest(r)
	if err != nil {
		t.Fatal(err)
	}

	u := cr.sameOriginURL("http://example.org?q")
	if u == nil {
		t.Fatal("sameOriginURL returned nil for a same-host absolute target")
	}
	if u.Path != "" {
		t.Errorf("Path = %q, want empty -- the absolute target named no path", u.Path)
	}
	if u.RawQuery != "q" {
		t.Errorf("RawQuery = %q, want q", u.RawQuery)
	}

	// A genuinely relative query-only reference still inherits the base path.
	if rel := cr.sameOriginURL("?q"); rel == nil || rel.Path != "/orders/1" {
		t.Errorf("relative ?q resolved to %v, want the base path /orders/1", rel)
	}
}

// TestInvalidationSurvivesARestart is the regression test for keeping the
// markers in memory only.
//
// scanExistingCache restores the cached bodies and headers on startup, but the
// markers -- the ONLY record that those entries predate a mutation -- were
// rebuilt empty. Every deploy or crash therefore republished the pre-mutation
// representation, Vary variants included, as a fresh HIT.
func TestInvalidationSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	const key = "GET:http://example.org/thing"

	first, err := NewDiskCacheWithConfig(dir, DefaultCacheConfig().WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Store(NewResourceBytes(http.StatusOK, []byte("before"), http.Header{}), key); err != nil {
		t.Fatal(err)
	}
	first.Invalidate(key)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	// A new process opens the same directory.
	second, err := NewDiskCacheWithConfig(dir, DefaultCacheConfig().WithCleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()

	inner, ok := second.(*cache)
	if !ok {
		t.Fatalf("cache is %T, want *cache", second)
	}
	if _, marked := inner.StaleAt(key); !marked {
		t.Fatal("the invalidation marker was lost across the restart")
	}

	got, err := second.Retrieve(key)
	if err != nil {
		t.Fatalf("Retrieve error = %v; the entry itself should have been restored", err)
	}
	defer func() { _ = got.Close() }()
	if !got.IsStale() {
		t.Error("the pre-mutation entry came back fresh after a restart")
	}
}

// TestInvalidationSurvivesPersistentVFSReopen covers callers that reuse a VFS
// directly rather than constructing the built-in disk cache. The files and
// marker snapshot both persist in that VFS, so reconstruction must restore
// both halves before serving an entry.
func TestInvalidationSurvivesPersistentVFSReopen(t *testing.T) {
	fs := vfs.Memory()
	config := DefaultCacheConfig().WithCleanupInterval(0)
	const key = "GET:http://example.org/persistent-vfs"

	first := NewVFSCacheWithConfig(fs, config)
	if err := first.Store(NewResourceBytes(http.StatusOK, []byte("before"), http.Header{}), key); err != nil {
		t.Fatal(err)
	}
	first.Invalidate(key)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := NewVFSCacheWithConfig(fs, config)
	defer func() { _ = second.Close() }()
	if _, marked := second.(*cache).StaleAt(key); !marked {
		t.Fatal("persistent VFS invalidation marker was not restored")
	}
	got, err := second.Retrieve(key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = got.Close() }()
	if !got.IsStale() {
		t.Error("pre-mutation entry from a persistent VFS was served fresh after reconstruction")
	}
}

// --- Codex review round 6 (PR #5) ---

// TestRevalidatedVariantStopsRevalidating covers freshening the VARIANT that
// lookup actually retrieved, not just the base key.
//
// An invalidated Vary variant carrying an ETag is revalidated upstream, and
// the conditional request comes back unchanged. Freshening only the base key
// left the variant's stored Proxy-Date older than the base marker -- and each
// variant is judged against that marker by its own receive time -- so it was
// marked stale again on the very next request and revalidated upstream every
// time until the marker was swept.
func TestRevalidatedVariantStopsRevalidating(t *testing.T) {
	var upstreamHits int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Vary", "Accept-Language")
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))
	})

	h := NewHandler(NewMemoryCache(), upstream)
	t.Cleanup(func() { h.writes.Wait() })

	get := func(lang string) {
		req := httptest.NewRequest("GET", "http://example.org/thing", nil)
		req.Header.Set("Accept-Language", lang)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		rec.Flush()
	}

	get("en")
	h.writes.Wait()
	get("fr")
	h.writes.Wait()

	// A mutation invalidates the base key, and with it every variant.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "http://example.org/thing", nil))
	rec.Flush()

	// The first request after it revalidates: the ETag is unchanged, so the
	// stored entry is kept and freshened.
	before := atomic.LoadInt32(&upstreamHits)
	get("en")
	h.writes.Wait()
	if atomic.LoadInt32(&upstreamHits) == before {
		t.Fatal("the invalidated variant was served without revalidating")
	}

	// Every request after THAT must be a plain hit. The variant has been
	// validated against the origin once; asking again on each request makes
	// the invalidation permanent for the marker's whole lifetime.
	before = atomic.LoadInt32(&upstreamHits)
	get("en")
	get("en")
	h.writes.Wait()
	if got := atomic.LoadInt32(&upstreamHits) - before; got != 0 {
		t.Errorf("%d further upstream request(s) after a successful revalidation, want 0", got)
	}
}

func TestSuccessfulValidationClearsStaleWarning(t *testing.T) {
	c := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0))
	defer func() { _ = c.Close() }()
	req := httptest.NewRequest(http.MethodGet, "http://example.org/validated-warning", nil)
	cReq, err := newCacheRequest(req)
	if err != nil {
		t.Fatalf("newCacheRequest: %v", err)
	}
	headers := http.Header{
		"Cache-Control": {"max-age=3600"},
		"Date":          {time.Now().UTC().Format(http.TimeFormat)},
		"ETag":          {`"v1"`},
	}
	if err := c.Store(NewResourceBytes(http.StatusOK, []byte("cached"), headers.Clone()), cReq.Key.String()); err != nil {
		t.Fatalf("Store: %v", err)
	}
	c.Invalidate(cReq.Key.String())

	var validations atomic.Int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		validations.Add(1)
		for name, values := range headers {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		w.WriteHeader(http.StatusNotModified)
	})
	h := NewHandler(c, upstream)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := validations.Load(); got != 1 {
		t.Fatalf("validation requests = %d, want 1", got)
	}
	if got := rec.Body.String(); got != "cached" {
		t.Fatalf("body = %q, want cached", got)
	}
	if got := rec.Header().Get(CacheHeader); got != "HIT" {
		t.Fatalf("cache status = %q, want HIT", got)
	}
	if warning := rec.Header().Values("Warning"); len(warning) != 0 {
		t.Fatalf("successfully validated response carried stale warning: %q", warning)
	}
}
