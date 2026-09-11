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

	do("POST", "http://example.org/thing")
	h.writes.Wait() // invalidation is performed on a tracked background write

	hitsBeforeFinalGet := upstreamHits
	do("GET", "http://example.org/thing")
	if upstreamHits == hitsBeforeFinalGet {
		t.Error("GET after POST was served from cache without revalidation; the entry was never invalidated")
	}
}
