package httpcache_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/soulteary/httpcache-kit/v2"
)

func mustParseUrl(u string) *url.URL {
	ru, err := url.Parse(u)
	if err != nil {
		panic(err)
	}
	return ru
}

func TestKeyCanonicalizationPreservesCaseSensitiveComponents(t *testing.T) {
	upper := httpcache.NewKey("GET", mustParseUrl("HTTP://EXAMPLE.COM/Packages/Foo.deb?token=AbC"), nil)
	lower := httpcache.NewKey("GET", mustParseUrl("http://example.com/packages/foo.deb?token=abc"), nil)

	if upper.String() == lower.String() {
		t.Fatal("path and query case must remain part of the cache key")
	}
	if !strings.HasPrefix(upper.String(), "GET:http://example.com/Packages/Foo.deb?token=AbC") {
		t.Fatalf("unexpected canonical key: %q", upper.String())
	}

	hostOnly := httpcache.NewKey("GET", mustParseUrl("http://example.com/Packages/Foo.deb?token=AbC"), nil)
	if upper.String() != hostOnly.String() {
		t.Fatal("scheme and host should be canonicalized case-insensitively")
	}
}

func TestVaryKeyAcceptsOptionalWhitespace(t *testing.T) {
	req := &http.Request{Header: http.Header{
		"Accept-Encoding": []string{"gzip"},
		"Accept-Language": []string{"zh-CN"},
	}}
	base := httpcache.NewKey("GET", mustParseUrl("https://example.com/file"), req.Header)
	withSpaces := base.Vary("Accept-Encoding, Accept-Language", req).String()
	withoutSpaces := base.Vary("Accept-Encoding,Accept-Language", req).String()
	withTabs := base.Vary(" Accept-Encoding ,\tAccept-Language ", req).String()

	if withSpaces != withoutSpaces || withSpaces != withTabs {
		t.Fatalf("equivalent Vary fields produced different keys:\n%s\n%s\n%s", withSpaces, withoutSpaces, withTabs)
	}
}

func TestKeysDiffer(t *testing.T) {
	k1 := httpcache.NewKey("GET", mustParseUrl("http://x.org/test"), nil)
	k2 := httpcache.NewKey("GET", mustParseUrl("http://y.org/test"), nil)

	if k1.String() == k2.String() {
		t.Fatal("key should be same")
	}
}

func TestRequestKey(t *testing.T) {
	r := newRequest("GET", "http://x.org/test")

	k1 := httpcache.NewKey("GET", mustParseUrl("http://x.org/test"), nil)
	k2 := httpcache.NewRequestKey(r)

	if k1.String() != k2.String() {
		t.Fatal("request key should be same")
	}
}

func TestVaryKey(t *testing.T) {
	r := newRequest("GET", "http://x.org/test", "Llamas-1: true", "Llamas-2: false")

	k1 := httpcache.NewRequestKey(r)
	k2 := httpcache.NewRequestKey(r).Vary("Llamas-1, Llamas-2", r)

	if k1.String() == k2.String() {
		t.Fatal("vary key should be same")
	}
}

// TestRequestKeyIgnoresContentLocation pins the fix for a shared-cache
// poisoning vector: a request header must never choose which entry a response
// is stored under or looked up from. Before this, a client could request one
// URL while naming another in Content-Location and have its own response
// parked under the other URL's key.
func TestRequestKeyIgnoresContentLocation(t *testing.T) {
	r := newRequest("GET", "http://x.org/test1", "Content-Location: http://x.org/test2")

	other := httpcache.NewKey("GET", mustParseUrl("http://x.org/test2"), nil)
	actual := httpcache.NewRequestKey(r)

	if actual.String() == other.String() {
		t.Fatal("request Content-Location must not redirect the cache key")
	}
	expected := httpcache.NewKey("GET", mustParseUrl("http://x.org/test1"), nil)
	if actual.String() != expected.String() {
		t.Fatalf("key = %q, want the effective request URI key %q", actual.String(), expected.String())
	}
}

// TestKeyEncodingIsInjective: the Vary section must not be forgeable from the
// URL. The old encoding joined a raw URL and raw header values with ":" and
// "::", so a URL containing the delimiters could collide with a different
// URL's varied key.
func TestKeyEncodingIsInjective(t *testing.T) {
	varied := httpcache.NewKey("GET", mustParseUrl("http://x.org/a"), nil).
		Vary("Accept-Encoding", newRequest("GET", "http://x.org/a", "Accept-Encoding: gzip"))

	forged := httpcache.NewKey("GET", mustParseUrl("http://x.org/a::Accept-Encoding=gzip:"), nil)

	if varied.String() == forged.String() {
		t.Fatalf("crafted URL collides with a varied key: %q", forged.String())
	}
}

func TestRequestKeyWithIllegalContentLocation(t *testing.T) {
	r := newRequest("GET", "http://x.org/test1", "Content-Location: http://y.org/test2")

	k1 := httpcache.NewKey("GET", mustParseUrl("http://x.org/test1"), nil)
	k2 := httpcache.NewRequestKey(r)

	if k1.String() != k2.String() {
		t.Fatal("request key should with illegal content location")
	}
}

func TestRequestKeyWithInvalidContentLocation(t *testing.T) {
	// Content-Location that fails to parse -> URL stays as request URL
	r := newRequest("GET", "http://x.org/test1", "Content-Location: :://invalid")
	k := httpcache.NewRequestKey(r)
	// Should not panic; key should be based on original URL
	if k.String() == "" {
		t.Fatal("key should not be empty")
	}
}
