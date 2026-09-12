package httpcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	lastModDivisor = 10
	viaPseudonym   = "httpcache"
)

var Clock = func() time.Time {
	return time.Now().UTC()
}

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

type byteReadSeekCloser struct {
	*bytes.Reader
}

func (brsc *byteReadSeekCloser) Close() error { return nil }

type Resource struct {
	ReadSeekCloser
	RequestTime, ResponseTime time.Time
	header                    http.Header
	statusCode                int
	cc                        CacheControl
	stale                     bool
	// storedAt is when THIS cache wrote the entry, at full precision. Zero
	// for a resource that did not come out of a cache.
	storedAt time.Time
}

func NewResource(statusCode int, body ReadSeekCloser, hdrs http.Header) *Resource {
	return &Resource{
		header:         hdrs,
		ReadSeekCloser: body,
		statusCode:     statusCode,
	}
}

func NewResourceBytes(statusCode int, b []byte, hdrs http.Header) *Resource {
	return &Resource{
		header:         hdrs,
		statusCode:     statusCode,
		ReadSeekCloser: &byteReadSeekCloser{bytes.NewReader(b)},
	}
}

func (r *Resource) IsNonErrorStatus() bool {
	return r.statusCode >= 200 && r.statusCode < 400
}

func (r *Resource) Status() int {
	return r.statusCode
}

func (r *Resource) Header() http.Header {
	return r.header
}

func (r *Resource) IsStale() bool {
	return r.stale
}

func (r *Resource) MarkStale() {
	r.stale = true
}

func (r *Resource) markFresh() {
	r.stale = false
}

func (r *Resource) cacheControl() (CacheControl, error) {
	if r.cc != nil {
		return r.cc, nil
	}

	cc, err := ParseCacheControlHeaders(r.header)
	if err != nil {
		return cc, err
	}

	r.cc = cc
	return cc, nil
}

func (r *Resource) LastModified() time.Time {
	var modTime time.Time

	if lastModHeader := r.header.Get("Last-Modified"); lastModHeader != "" {
		if t, err := http.ParseTime(lastModHeader); err == nil {
			modTime = t
		}
	}

	return modTime
}

func (r *Resource) Expires() (time.Time, error) {
	if expires := r.header.Get("Expires"); expires != "" {
		return http.ParseTime(expires)
	}

	return time.Time{}, nil
}

func (r *Resource) MustValidate(shared bool) bool {
	cc, err := r.cacheControl()
	if err != nil {
		debugf("Error parsing Cache-Control: %v", err.Error())
		return true
	}

	// The s-maxage directive also implies the semantics of proxy-revalidate
	if cc.Has("s-maxage") && shared {
		return true
	}

	if cc.Has("must-revalidate") || (cc.Has("proxy-revalidate") && shared) {
		return true
	}

	return false
}

func (r *Resource) DateAfter(d time.Time) bool {
	if dateHeader := r.header.Get("Date"); dateHeader != "" {
		if t, err := http.ParseTime(dateHeader); err != nil {
			return false
		} else {
			return t.After(d)
		}
	}
	return false
}

// ReceivedAfter reports whether this response reached the cache after d.
//
// It reads Proxy-Date, which the handler stamps from the LOCAL clock the
// moment the upstream response arrives, and only falls back to the origin's
// Date for a response stored without one.
//
// Deciding this from Date alone was wrong in both directions. A response that
// carries no Date at all -- which a direct http.Handler upstream may well
// omit -- never counted as superseding anything, so an invalidated key stayed
// stale on every retrieval and was refetched until the marker was swept. An
// origin whose clock trails the cache's did the same. Proxy-Date is always
// present and always this machine's clock.
//
// Receive time, not store time: an older store still in flight when a
// mutation lands carries a Proxy-Date from BEFORE the invalidation, so the
// marker still wins and the pre-mutation body is not republished as fresh.
//
// Both stamps are HTTP dates, so this is second-granular: a replacement that
// arrives in the same second as the invalidation does not count as
// superseding it and is refetched once more. That is the safe direction to
// round in.
// SetStoredAt records when this cache wrote the entry.
func (r *Resource) SetStoredAt(t time.Time) { r.storedAt = t }

// StoredAfter reports whether this cache's copy is newer than d.
//
// It prefers the cache's OWN store time to the response's dates, because
// Date and Proxy-Date are HTTP dates: one-second granularity, formatted with
// http.TimeFormat. An invalidation marker is a full-precision time.Time, so a
// response stored or revalidated in the same second as the invalidation is
// never "after" it -- and being judged against a marker it can never clear,
// the entry was re-marked stale and revalidated upstream on EVERY request
// until the marker was swept. The store time is also ours rather than the
// origin's, so a trailing origin clock cannot produce the same deadlock.
//
// Falls back to the header dates for a resource that did not come from this
// cache, which is the only thing available for one.
func (r *Resource) StoredAfter(d time.Time) bool {
	if !r.storedAt.IsZero() {
		return r.storedAt.After(d)
	}
	return r.ReceivedAfter(d)
}

func (r *Resource) ReceivedAfter(d time.Time) bool {
	// For a live upstream response, RequestTime is the precise local instant
	// at which the fetch or validation began. It is deliberately preferred to
	// the later write time: a request already in flight when a mutation lands
	// must not publish its possibly pre-mutation body afterwards.
	if !r.RequestTime.IsZero() {
		return r.RequestTime.After(d)
	}
	if t, err := timeHeader(ProxyDateHeader, r.header); err == nil {
		return t.After(d)
	}
	return r.DateAfter(d)
}

// Calculate the age of the resource
func (r *Resource) Age() (time.Duration, error) {
	var age time.Duration

	if ageInt, err := intHeader("Age", r.header); err == nil {
		age = time.Second * time.Duration(ageInt)
	}

	if proxyDate, err := timeHeader(ProxyDateHeader, r.header); err == nil {
		return Clock().Sub(proxyDate) + age, nil
	}

	if date, err := timeHeader("Date", r.header); err == nil {
		return Clock().Sub(date) + age, nil
	}

	return time.Duration(0), errors.New("unable to calculate age")
}

func (r *Resource) MaxAge(shared bool) (time.Duration, error) {
	cc, err := r.cacheControl()
	if err != nil {
		return time.Duration(0), err
	}

	if cc.Has("s-maxage") && shared {
		if maxAge, err := cc.Duration("s-maxage"); err != nil {
			return time.Duration(0), err
		} else if maxAge > 0 {
			return maxAge, nil
		}
	}

	if cc.Has("max-age") {
		if maxAge, err := cc.Duration("max-age"); err != nil {
			return time.Duration(0), err
		} else if maxAge > 0 {
			return maxAge, nil
		}
	}

	if expiresVal := r.header.Get("Expires"); expiresVal != "" {
		expires, err := http.ParseTime(expiresVal)
		if err != nil {
			return time.Duration(0), err
		}
		return expires.Sub(Clock()), nil
	}

	return time.Duration(0), nil
}

func (r *Resource) RemovePrivateHeaders() {
	cc, err := r.cacheControl()
	if err != nil {
		debugf("Error parsing Cache-Control: %s", err.Error())
	}

	for _, p := range cc["private"] {
		debugf("removing private header %q", p)
		r.header.Del(p)
	}
}

func (r *Resource) HasValidators() bool {
	if r.header.Get("Last-Modified") != "" || r.header.Get("Etag") != "" {
		return true
	}

	return false
}

func (r *Resource) HasExplicitExpiration() bool {
	cc, err := r.cacheControl()
	if err != nil {
		debugf("Error parsing Cache-Control: %s", err.Error())
		return false
	}

	if d, _ := cc.Duration("max-age"); d > time.Duration(0) {
		return true
	}

	if d, _ := cc.Duration("s-maxage"); d > time.Duration(0) {
		return true
	}

	if exp, _ := r.Expires(); !exp.IsZero() {
		return true
	}

	return false
}

func (r *Resource) HeuristicFreshness() time.Duration {
	if !r.HasExplicitExpiration() && r.header.Get("Last-Modified") != "" {
		return Clock().Sub(r.LastModified()) / time.Duration(lastModDivisor)
	}

	return time.Duration(0)
}

func (r *Resource) Via() string {
	via := []string{}
	via = append(via, fmt.Sprintf("1.1 %s", viaPseudonym))
	return strings.Join(via, ",")
}
