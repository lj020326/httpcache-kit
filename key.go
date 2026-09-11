package httpcache

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Key represents a unique identifier for a resource in the cache
type Key struct {
	method string
	header http.Header
	u      url.URL
	vary   []string
}

// NewKey returns a new Key instance
func NewKey(method string, u *url.URL, h http.Header) Key {
	return Key{method: method, header: h, u: *u, vary: []string{}}
}

// NewRequestKey generates a Key for a request.
//
// The key is derived from the effective request URI only. A request header
// must never be able to choose which cache entry a response is stored under:
// letting the *request's* Content-Location pick the key allowed a client to
// park its own response under another URL's key ("GET /attacker-page" with
// "Content-Location: /admin"), poisoning a shared cache for every other user.
//
// RFC 7234 does use Content-Location, but the *response's* — and only to
// invalidate entries, never to select a storage key. See invalidationKeys.
func NewRequestKey(r *http.Request) Key {
	return NewKey(r.Method, r.URL, r.Header)
}

// ForMethod returns a new Key with a given method
func (k Key) ForMethod(method string) Key {
	k2 := k
	k2.method = method
	return k2
}

// Vary returns a Key that is varied on particular headers in a http.Request
func (k Key) Vary(varyHeader string, r *http.Request) Key {
	k2 := k

	for _, header := range parseVary(varyHeader) {
		k2.vary = append(k2.vary, header+"="+r.Header.Get(header))
	}

	return k2
}

// varySeparator delimits the Vary section of a key and the entries inside it.
//
// It is a US unit separator, chosen because it cannot appear unescaped on
// either side of the boundary: url.URL.String percent-encodes control bytes,
// and each Vary entry is run through strconv.Quote. That makes the encoding
// injective — distinct (URL, Vary) pairs always produce distinct keys.
//
// The previous encoding used ":" and "::" as delimiters against a raw URL and
// raw header values, so a URL containing "::" could be crafted to produce the
// same key string as a different URL carrying Vary values.
const varySeparator = "\x1f"

func (k Key) String() string {
	URL := canonicalURL(&k.u).String()
	var b strings.Builder
	b.Grow(len(k.method) + 1 + len(URL) + 3 + 10*len(k.vary)) // heuristic to reduce allocs
	b.WriteString(k.method)
	b.WriteString(":")
	b.WriteString(URL)
	for _, v := range k.vary {
		b.WriteString(varySeparator)
		b.WriteString(strconv.Quote(v))
	}
	return b.String()
}

func canonicalURL(u *url.URL) *url.URL {
	// URI schemes and host names are case-insensitive, but paths and query
	// strings are not. Lower-casing the entire URL aliases distinct repository
	// objects and can make one response overwrite another in a shared cache.
	canonical := *u
	canonical.Scheme = strings.ToLower(canonical.Scheme)
	canonical.Host = strings.ToLower(canonical.Host)
	canonical.Fragment = ""
	return &canonical
}

func parseVary(varyHeader string) []string {
	parts := strings.Split(varyHeader, ",")
	headers := make([]string, 0, len(parts))
	for _, part := range parts {
		header := http.CanonicalHeaderKey(strings.TrimSpace(part))
		if header != "" {
			headers = append(headers, header)
		}
	}
	return headers
}

func varyWildcard(varyHeader string) bool {
	for _, header := range parseVary(varyHeader) {
		if header == "*" {
			return true
		}
	}
	return false
}
