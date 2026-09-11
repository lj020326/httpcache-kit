package httpcache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
// invalidateResource used to only log, so a resource that had been POSTed to
// kept being served from cache without revalidation.
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
	do("POST", "http://example.org/thing")

	hitsBeforeFinalGet := upstreamHits
	do("GET", "http://example.org/thing")
	if upstreamHits == hitsBeforeFinalGet {
		t.Error("GET after POST was served from cache without revalidation; the entry was never invalidated")
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
