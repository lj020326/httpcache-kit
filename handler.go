package httpcache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	logger "github.com/soulteary/logger-kit/v2"
)

const (
	CacheHeader     = "X-Cache"
	ProxyDateHeader = "Proxy-Date"
)

var Writes sync.WaitGroup

var storeable = map[int]bool{
	http.StatusOK:                   true,
	http.StatusFound:                true,
	http.StatusNonAuthoritativeInfo: true,
	http.StatusMultipleChoices:      true,
	http.StatusMovedPermanently:     true,
	http.StatusGone:                 true,
	http.StatusNotFound:             true,
}

var cacheableByDefault = map[int]bool{
	http.StatusOK:                   true,
	http.StatusFound:                true,
	http.StatusNotModified:          true,
	http.StatusNonAuthoritativeInfo: true,
	http.StatusMultipleChoices:      true,
	http.StatusMovedPermanently:     true,
	http.StatusGone:                 true,
	http.StatusPartialContent:       true,
}

// HandlerOptions holds optional configuration for NewHandlerWithOptions.
// Logger injected here is used by the handler for all debug/error logging;
// if nil, the package-level logger (see SetLogger) is used.
type HandlerOptions struct {
	Logger *logger.Logger

	// StaleMarkerTTL is how long the handler remembers an invalidation for a
	// Cache that does not implement StaleAt itself.
	//
	// It must be at least as long as that Cache retains entries: the marker is
	// the only record that entries older than it are pre-mutation, so expiring
	// it first republishes them as fresh. Only the implementation knows its
	// own retention, which is why this is a knob rather than a constant.
	//
	// Zero means DefaultCacheTTL. A Cache that implements StaleAt keeps its
	// own record and ignores this.
	StaleMarkerTTL time.Duration
}

type Handler struct {
	// Shared reports whether this cache is shared between users (a reverse
	// proxy) rather than private to one (a browser-style cache).
	//
	// It defaults to false, which is the *private* cache profile: responses
	// marked "Cache-Control: private" and responses to requests carrying an
	// Authorization header are storable. Deploying a shared cache without
	// setting this serves one user's response to another. Prefer
	// NewSharedHandler, which cannot be forgotten.
	Shared    bool
	upstream  http.Handler
	validator *Validator
	cache     Cache
	metrics   *CacheMetrics
	log       *logger.Logger

	lifecycleMu sync.Mutex
	writes      sync.WaitGroup
	closing     bool
	flightsMu   sync.Mutex
	flights     map[string]*missFlight

	// localStaleMu guards localStale.
	localStaleMu sync.Mutex
	// localStale records invalidation times for a Cache that cannot report
	// them itself. Unused when the cache implements staleAtChecker.
	localStale map[string]time.Time
	// localStaleTTL is how long those markers are kept; see
	// HandlerOptions.StaleMarkerTTL.
	localStaleTTL time.Duration
}

type missFlight struct {
	done chan struct{}
}

// NewHandler returns a private cache handler with default options
// (package-level logger). Use NewSharedHandler for a cache that is shared
// between users, such as a reverse proxy.
func NewHandler(cache Cache, upstream http.Handler) *Handler {
	return NewHandlerWithOptions(cache, upstream, nil)
}

// NewSharedHandler returns a cache handler configured as a shared cache: it
// refuses to store "Cache-Control: private" responses and responses to
// authorized requests unless they are explicitly marked public or carry
// s-maxage, and it strips headers listed in "private" before storing.
//
// This is the constructor to use for a reverse proxy. NewHandler leaves
// Shared false, which is correct for a per-user cache and unsafe for a
// shared one.
func NewSharedHandler(cache Cache, upstream http.Handler) *Handler {
	h := NewHandlerWithOptions(cache, upstream, nil)
	h.Shared = true
	return h
}

// NewHandlerWithOptions returns a cache handler with the given options.
// If opts.Logger is set, it is used for handler logging; otherwise the package-level logger is used.
func NewHandlerWithOptions(cache Cache, upstream http.Handler, opts *HandlerOptions) *Handler {
	h := &Handler{
		upstream:  upstream,
		cache:     cache,
		validator: &Validator{upstream},
		Shared:    false,
		metrics:   getDefaultMetrics(),
		flights:   make(map[string]*missFlight),
	}
	if opts != nil && opts.Logger != nil {
		h.log = opts.Logger
	}
	if opts != nil && opts.StaleMarkerTTL > 0 {
		h.localStaleTTL = opts.StaleMarkerTTL
	}
	return h
}

func (h *Handler) logRef() *logger.Logger {
	if h.log != nil {
		return h.log
	}
	return cacheLogger
}

func (h *Handler) debugf(format string, args ...interface{}) {
	if IsDebugLogging() {
		h.logRef().Debug().Msgf(format, args...)
	}
}

func (h *Handler) errorf(format string, args ...interface{}) {
	h.logRef().Error().Msgf(format, args...)
}

// SetMetrics sets the metrics instance for the handler
func (h *Handler) SetMetrics(m *CacheMetrics) {
	h.metrics = m
}

func (h *Handler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	cReq, err := newCacheRequest(r)
	if err != nil {
		http.Error(rw, "invalid request: "+err.Error(),
			http.StatusBadRequest)
		return
	}

	if !cReq.isCacheable() {
		h.debugf("request not cacheable")
		rw.Header().Set(CacheHeader, "SKIP")
		if h.metrics != nil {
			h.metrics.RecordCacheSkip()
		}
		h.pipeUpstream(rw, cReq)
		return
	}

	res, err := h.lookup(cReq)
	if err != nil && err != ErrNotFoundInCache {
		http.Error(rw, "lookup error: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	cacheType := "private"
	if h.Shared {
		cacheType = "shared"
	}

	if err == ErrNotFoundInCache {
		if cReq.CacheControl.Has("only-if-cached") {
			http.Error(rw, "key not in cache",
				http.StatusGatewayTimeout)
			return
		}
		h.debugf("%s %s not in %s cache", r.Method, r.URL.String(), cacheType)
		if h.metrics != nil {
			h.metrics.RecordCacheMiss(r.Method)
		}
		flight, leader := h.claimMiss(cReq.Key.String())
		if !leader {
			select {
			case <-flight.done:
				res, err = h.lookup(cReq)
				if err == nil {
					res.Header().Set(CacheHeader, "HIT")
					if h.metrics != nil {
						h.metrics.RecordCacheHit(r.Method)
					}
					h.serveResource(res, rw, cReq)
					_ = res.Close()
					return
				}
				// The leader could not cache the response. Fall through to an
				// uncoupled upstream request so followers are never stranded.
				h.passUpstream(rw, cReq, nil)
				return
			case <-r.Context().Done():
				return
			}
		}
		h.passUpstream(rw, cReq, func() { h.finishMiss(cReq.Key.String(), flight) })
		return
	} else {
		h.debugf("%s %s found in %s cache", r.Method, r.URL.String(), cacheType)
	}

	if h.needsValidation(res, cReq) {
		if cReq.CacheControl.Has("only-if-cached") {
			// res came from the cache and is not served; both early returns
			// in this block leave it open otherwise, and the disk backend
			// holds a file descriptor per retrieved resource. Every stale
			// entry lacking a validator took one on each refresh.
			_ = res.Close()
			http.Error(rw, "key was in cache, but required validation",
				http.StatusGatewayTimeout)
			return
		}

		h.debugf("validating cached response")
		if h.validator.Validate(r, res) {
			h.debugf("response is valid")
			// Both the base key and the VARIANT that was actually retrieved.
			// Freshening only the base left the variant's stored Proxy-Date
			// older than the base marker, so it was marked stale again on the
			// very next request and revalidated upstream every time until the
			// marker was swept.
			keys := []string{cReq.Key.String()}
			if cReq.servedKey != "" && cReq.servedKey != keys[0] {
				keys = append(keys, cReq.servedKey)
			}
			_ = h.cache.Freshen(res, keys...)
		} else {
			h.debugf("response is changed")
			_ = res.Close()
			h.passUpstream(rw, cReq, nil)
			return
		}
	}

	h.debugf("serving from cache")
	res.Header().Set(CacheHeader, "HIT")
	if h.metrics != nil {
		h.metrics.RecordCacheHit(r.Method)
	}
	h.serveResource(res, rw, cReq)

	if err := res.Close(); err != nil {
		h.errorf("Error closing resource: %s", err.Error())
	}
}

// freshness returns the duration that a requested resource will be fresh for
func (h *Handler) freshness(res *Resource, r *cacheRequest) (time.Duration, error) {
	maxAge, err := res.MaxAge(h.Shared)
	if err != nil {
		return time.Duration(0), err
	}

	if r.CacheControl.Has("max-age") {
		reqMaxAge, err := r.CacheControl.Duration("max-age")
		if err != nil {
			return time.Duration(0), err
		}

		if reqMaxAge < maxAge {
			h.debugf("using request max-age of %s", reqMaxAge.String())
			maxAge = reqMaxAge
		}
	}

	age, err := res.Age()
	if err != nil {
		return time.Duration(0), err
	}

	if res.IsStale() {
		return time.Duration(0), nil
	}

	if hFresh := res.HeuristicFreshness(); hFresh > maxAge {
		h.debugf("using heuristic freshness of %q", hFresh)
		maxAge = hFresh
	}

	return maxAge - age, nil
}

func (h *Handler) needsValidation(res *Resource, r *cacheRequest) bool {
	if res.MustValidate(h.Shared) {
		return true
	}

	freshness, err := h.freshness(res, r)
	if err != nil {
		h.debugf("error calculating freshness: %s", err.Error())
		return true
	}

	if r.CacheControl.Has("min-fresh") {
		reqMinFresh, err := r.CacheControl.Duration("min-fresh")
		if err != nil {
			h.debugf("error parsing request min-fresh: %s", err.Error())
			return true
		}

		if freshness < reqMinFresh {
			h.debugf("resource is fresh, but won't satisfy min-fresh of %s", reqMinFresh)
			return true
		}
	}

	h.debugf("resource has a freshness of %s", freshness)

	if freshness <= 0 && r.CacheControl.Has("max-stale") {
		if len(r.CacheControl["max-stale"]) == 0 {
			h.debugf("resource is stale, but client sent max-stale")
			return false
		} else if maxStale, _ := r.CacheControl.Duration("max-stale"); maxStale >= (freshness * -1) {
			h.debugf("resource is stale, but within allowed max-stale period of %s", maxStale)
			return false
		}
	}

	return freshness <= 0
}

// pipeUpstream makes the request via the upstream handler, the response is not stored or modified
func (h *Handler) pipeUpstream(w http.ResponseWriter, r *cacheRequest) {
	rw, err := newResponseStreamer(w)
	if err != nil {
		h.debugf("error creating response streamer: %v", err)
		w.Header().Set(CacheHeader, "SKIP")
		h.upstream.ServeHTTP(w, r.Request)
		return
	}
	rdr, err := rw.NextReader()
	if err != nil {
		h.debugf("error creating next stream reader: %v", err)
		w.Header().Set(CacheHeader, "SKIP")
		h.upstream.ServeHTTP(w, r.Request)
		return
	}
	defer func() { _ = rdr.Close() }()

	h.debugf("piping request upstream")
	go func() {
		h.upstream.ServeHTTP(rw, r.Request)
		_ = rw.Close()
	}()
	rw.WaitHeaders()

	if r.Method != "HEAD" && !r.isStateChanging() {
		return
	}

	res := rw.Resource()
	defer func() { _ = res.Close() }()

	if r.Method == "HEAD" {
		_ = h.cache.Freshen(res, r.Key.ForMethod("GET").String())
	} else if res.IsNonErrorStatus() {
		h.invalidateResource(res, r)
	}
}

// passUpstream makes the request via the upstream handler and stores the result
// It uses streaming to avoid loading the entire response into memory for large files.
func (h *Handler) passUpstream(w http.ResponseWriter, r *cacheRequest, complete func()) {
	rw, err := newResponseStreamer(w)
	if err != nil {
		defer callComplete(complete)
		h.debugf("error creating response streamer: %v", err)
		w.Header().Set(CacheHeader, "SKIP")
		h.upstream.ServeHTTP(w, r.Request)
		return
	}
	rdr, err := rw.NextReader()
	if err != nil {
		defer callComplete(complete)
		h.debugf("error creating next stream reader: %v", err)
		w.Header().Set(CacheHeader, "SKIP")
		h.upstream.ServeHTTP(w, r.Request)
		return
	}

	t := Clock()
	h.debugf("passing request upstream")
	rw.Header().Set(CacheHeader, "MISS")

	go func() {
		h.upstream.ServeHTTP(rw, r.Request)
		_ = rw.Close()
	}()
	rw.WaitHeaders()
	h.debugf("upstream responded headers in %s", Clock().Sub(t).String())

	// just the headers! Use a clone so storeResource can mutate (e.g. RemovePrivateHeaders) without racing with the client reading rw.Header().
	res := NewResourceBytes(rw.StatusCode, nil, rw.Header().Clone())
	if !h.isCacheable(res, r) {
		h.debugf("resource is uncacheable")
		rw.Header().Set(CacheHeader, "SKIP")
		// Drain body so upstream goroutine can finish and client receives the response
		_, _ = io.Copy(io.Discard, rdr)
		_ = rdr.Close()
		callComplete(complete)
		return
	}

	// Create temporary file to store body for caching (supports multiple reads for multiple keys)
	tmpFile, err := os.CreateTemp("", "httpcache-*.tmp")
	if err != nil {
		h.debugf("error creating temp file: %v, skipping cache", err)
		// Keep draining the stream so the client receives the complete response,
		// but never fall back to an unbounded in-memory copy.
		_, err := io.Copy(io.Discard, rdr)
		_ = rdr.Close()
		if err != nil {
			h.debugf("error reading stream: %v", err)
		}
		rw.Header().Set(CacheHeader, "SKIP")
		callComplete(complete)
		return
	}
	tmpPath := tmpFile.Name()

	// Since responseStreamer.Write already writes to both pipeWriter (for pipeReader) and ResponseWriter (client),
	// the client is already receiving data. We read from pipeReader and write to temp file for caching.
	// This allows us to cache without blocking the client response.
	_, err = io.Copy(tmpFile, rdr)
	if err != nil {
		h.debugf("error copying to temp file: %v", err)
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		_ = rdr.Close()
		rw.Header().Set(CacheHeader, "SKIP")
		callComplete(complete)
		return
	}

	// Close temp file for writing
	if err := tmpFile.Close(); err != nil {
		h.debugf("error closing temp file: %v", err)
		_ = os.Remove(tmpPath)
		_ = rdr.Close()
		rw.Header().Set(CacheHeader, "SKIP")
		callComplete(complete)
		return
	}
	_ = rdr.Close()

	upstreamDuration := Clock().Sub(t)
	h.debugf("full upstream response took %s", upstreamDuration.String())

	// Create Resource from temp file for caching
	tmpFileReader, err := os.Open(tmpPath)
	if err != nil {
		h.debugf("error reopening temp file for caching: %v", err)
		_ = os.Remove(tmpPath)
		rw.Header().Set(CacheHeader, "SKIP")
		callComplete(complete)
		return
	}

	// Create a ReadSeekCloser from temp file
	// The temp file will be cleaned up after caching is complete
	res.ReadSeekCloser = &tempFileReadSeekCloser{file: tmpFileReader, path: tmpPath}

	// Record upstream duration metric
	if h.metrics != nil {
		h.metrics.RecordUpstreamDuration(r.Method, rw.StatusCode, upstreamDuration.Seconds())
	}

	proxyDate := Clock().Format(http.TimeFormat)
	if age, err := correctedAge(res.Header(), t, Clock()); err == nil {
		ageStr := strconv.Itoa(int(math.Ceil(age.Seconds())))
		res.Header().Set("Age", ageStr)
		rw.Header().Set("Age", ageStr)
	} else {
		h.debugf("error calculating corrected age: %s", err.Error())
	}

	rw.Header().Set(ProxyDateHeader, proxyDate)
	res.Header().Set(ProxyDateHeader, proxyDate) // so cached Resource.Age() uses receive time, not upstream Date

	// Store resource in background - errors won't affect client response
	h.storeResource(res, r, complete)
}

func callComplete(complete func()) {
	if complete != nil {
		complete()
	}
}

// tempFileReadSeekCloser implements ReadSeekCloser for temporary files
type tempFileReadSeekCloser struct {
	file *os.File
	path string
}

func (t *tempFileReadSeekCloser) Read(p []byte) (n int, err error) {
	return t.file.Read(p)
}

func (t *tempFileReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return t.file.Seek(offset, whence)
}

func (t *tempFileReadSeekCloser) Close() error {
	var err error
	if t.file != nil {
		err = t.file.Close()
	}
	// Clean up temp file after use
	if t.path != "" {
		if removeErr := os.Remove(t.path); removeErr != nil && err == nil {
			err = removeErr
		}
	}
	return err
}

// correctedAge adjusts the age of a resource for clock skew and travel time
// https://httpwg.github.io/specs/rfc7234.html#rfc.section.4.2.3
func correctedAge(h http.Header, reqTime, respTime time.Time) (time.Duration, error) {
	date, err := timeHeader("Date", h)
	if err != nil {
		return time.Duration(0), err
	}

	apparentAge := respTime.Sub(date)
	if apparentAge < 0 {
		apparentAge = 0
	}

	respDelay := respTime.Sub(reqTime)
	ageSeconds, err := intHeader("Age", h)
	if err != nil {
		return time.Duration(0), err
	}
	age := time.Second * time.Duration(ageSeconds)
	correctedAge := age + respDelay

	if apparentAge > correctedAge {
		correctedAge = apparentAge
	}

	residentTime := Clock().Sub(respTime)
	currentAge := correctedAge + residentTime

	return currentAge, nil
}

func (h *Handler) isCacheable(res *Resource, r *cacheRequest) bool {
	cc, err := res.cacheControl()
	if err != nil {
		h.errorf("Error parsing cache-control: %s", err.Error())
		return false
	}

	if cc.Has("no-cache") || cc.Has("no-store") {
		return false
	}
	if varyWildcard(res.Header().Get("Vary")) {
		return false
	}

	if cc.Has("private") && len(cc["private"]) == 0 && h.Shared {
		return false
	}

	if _, ok := storeable[res.Status()]; !ok {
		return false
	}

	if r.Header.Get("Authorization") != "" && h.Shared &&
		!cc.Has("public") && !cc.Has("s-maxage") {
		return false
	}

	if res.Header().Get("Authorization") != "" && h.Shared &&
		!cc.Has("must-revalidate") && !cc.Has("s-maxage") {
		return false
	}

	if res.HasExplicitExpiration() {
		return true
	}

	if _, ok := cacheableByDefault[res.Status()]; !ok && !cc.Has("public") {
		return false
	}

	if res.HasValidators() {
		return true
	} else if res.HeuristicFreshness() > 0 {
		return true
	}

	return false
}

func (h *Handler) serveResource(res *Resource, w http.ResponseWriter, req *cacheRequest) {
	for key, headers := range res.Header() {
		for _, header := range headers {
			w.Header().Add(key, header)
		}
	}

	age, err := res.Age()
	if err != nil {
		http.Error(w, "Error calculating age: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	// http://httpwg.github.io/specs/rfc7234.html#warn.113
	if age > (time.Hour*24) && res.HeuristicFreshness() > (time.Hour*24) {
		w.Header().Add("Warning", `113 - "Heuristic Expiration"`)
	}

	// http://httpwg.github.io/specs/rfc7234.html#warn.110
	freshness, err := h.freshness(res, req)
	if err != nil || freshness <= 0 {
		w.Header().Add("Warning", `110 - "Response is Stale"`)
	}

	h.debugf("resource is %s old, updating age from %s",
		age.String(), w.Header().Get("Age"))

	w.Header().Set("Age", fmt.Sprintf("%.f", math.Floor(age.Seconds())))
	w.Header().Set("Via", res.Via())

	// hacky handler for non-ok statuses
	if res.Status() != http.StatusOK {
		w.WriteHeader(res.Status())
		_, _ = io.Copy(w, res)
	} else {
		http.ServeContent(w, req.Request, "", res.LastModified(), res)
	}
}

// invalidateResource invalidates the cache entries that a successful unsafe
// request has made stale, per RFC 7234 section 4.4.
//
// This used to only log: nothing was ever invalidated, so a resource that had
// been POSTed to, PUT or DELETEd kept being served from cache until its own
// freshness lifetime ran out.
func (h *Handler) invalidateResource(res *Resource, r *cacheRequest) {
	keys := invalidationKeys(res, r)
	if len(keys) == 0 {
		return
	}
	// Synchronously, before ServeHTTP returns.
	//
	// Invalidate only writes stale markers under a mutex, so there is nothing
	// to gain by deferring it -- and deferring it left a window in which the
	// unsafe request had already been answered while the previous
	// representation was still considered fresh, so an immediate or concurrent
	// GET could be served stale content after a successful mutation.
	h.cache.Invalidate(keys...)
	h.recordStale(keys...)
	h.debugf("invalidated %d key(s) after %s %s: %q", len(keys), r.Method, r.URL, keys)
}

// recordStale notes when keys were invalidated, for a Cache that cannot.
//
// A cache implementing staleAtChecker keeps this itself, so nothing is
// recorded for one; only a third-party Cache reaches this map.
func (h *Handler) recordStale(keys ...string) {
	if _, ok := h.cache.(staleAtChecker); ok {
		return
	}

	now := Clock()
	cutoff := now.Add(-h.staleMarkerTTL())

	h.localStaleMu.Lock()
	defer h.localStaleMu.Unlock()

	if h.localStale == nil {
		h.localStale = make(map[string]time.Time)
	}
	// Swept opportunistically here rather than on a timer: the map only grows
	// when unsafe requests arrive, so that is also when it is worth pruning.
	for key, at := range h.localStale {
		if at.Before(cutoff) {
			delete(h.localStale, key)
		}
	}
	for _, key := range keys {
		h.localStale[key] = now
	}
}

// staleAt reports when key was invalidated, from whichever record exists.
func (h *Handler) staleAt(key string) (time.Time, bool) {
	if checker, ok := h.cache.(staleAtChecker); ok {
		return checker.StaleAt(key)
	}

	h.localStaleMu.Lock()
	defer h.localStaleMu.Unlock()
	at, ok := h.localStale[key]
	return at, ok
}

// invalidationKeys returns the cache keys made stale by a successful unsafe
// request: the effective request URI, plus the URIs named by the response's
// Location and Content-Location headers.
//
// Both GET and HEAD keys are invalidated, because a HEAD response may have
// been stored separately. Cross-origin targets are ignored — a response must
// not be able to evict another origin's entries.
func invalidationKeys(res *Resource, r *cacheRequest) []string {
	base := NewKey("GET", r.URL, r.Header)
	keys := []string{base.String(), base.ForMethod("HEAD").String()}

	for _, header := range []string{"Location", "Content-Location"} {
		raw := res.Header().Get(header)
		if raw == "" {
			continue
		}
		u := r.sameOriginURL(raw)
		if u == nil {
			debugf("ignoring cross-origin or unparseable %s %q", header, raw)
			continue
		}
		k := NewKey("GET", u, r.Header)
		keys = append(keys, k.String(), k.ForMethod("HEAD").String())
	}
	return keys
}

func (h *Handler) storeResource(res *Resource, r *cacheRequest, complete func()) {
	if !h.beginWrite() {
		_ = res.Close()
		callComplete(complete)
		return
	}

	go func() {
		defer h.endWrite()
		defer callComplete(complete)
		defer func() { _ = res.Close() }()
		t := Clock()
		keys := []string{r.Key.String()}
		headers := res.Header()

		if h.Shared {
			res.RemovePrivateHeaders()
		}

		// store a secondary vary version
		if vary := headers.Get("Vary"); vary != "" {
			keys = append(keys, r.Key.Vary(vary, r.Request).String())
		}

		if err := h.cache.Store(res, keys...); err != nil {
			h.errorf("storing resources %#v failed with error: %s", keys, err.Error())
			if h.metrics != nil {
				h.metrics.RecordStoreOperation(false)
			}
		} else {
			if h.metrics != nil {
				h.metrics.RecordStoreOperation(true)
			}
		}

		h.debugf("stored resources %+v in %s", keys, Clock().Sub(t))
	}()
}

// defaultStaleMarkerTTL is the handler's marker retention when
// HandlerOptions.StaleMarkerTTL is not set.
//
// The built-in cache's own default item TTL, which is the only retention the
// handler can guess at. A Cache keeping entries for longer must say so through
// StaleMarkerTTL, or implement staleAtChecker and keep the record itself --
// expiring a marker before the entries it judges republishes them as fresh.
const defaultStaleMarkerTTL = DefaultCacheTTL

// staleMarkerTTL is the configured handler-side marker retention.
func (h *Handler) staleMarkerTTL() time.Duration {
	if h.localStaleTTL > 0 {
		return h.localStaleTTL
	}
	return defaultStaleMarkerTTL
}

// staleAtChecker is an optional Cache capability: when a key was invalidated.
//
// The built-in cache implements it. A third-party Cache that does not simply
// gets the coarser base-entry fallback in lookup.
type staleAtChecker interface {
	StaleAt(key string) (time.Time, bool)
}

// lookupResource finds the best matching Resource for the
// request, or nil and ErrNotFoundInCache if none is found
func (h *Handler) lookup(req *cacheRequest) (*Resource, error) {
	res, err := h.cache.Retrieve(req.Key.String())

	// HEAD requests can possibly be served from GET
	if err == ErrNotFoundInCache && req.Method == "HEAD" {
		res, err = h.cache.Retrieve(req.Key.ForMethod("GET").String())
		if err != nil {
			return nil, err
		}

		if res.HasExplicitExpiration() && req.isCacheable() {
			h.debugf("using cached GET request for serving HEAD")
			return res, nil
		} else {
			return nil, ErrNotFoundInCache
		}
	} else if err != nil {
		return res, err
	}

	// Secondary lookup for Vary
	if vary := res.Header().Get("Vary"); vary != "" {
		if varyWildcard(vary) {
			_ = res.Close()
			return nil, ErrNotFoundInCache
		}
		// Whether the BASE entry was invalidated, read before it is closed.
		baseStale := res.IsStale()
		baseKey := req.Key.String()

		variantKey := req.Key.Vary(vary, req.Request).String()
		varied, varyErr := h.cache.Retrieve(variantKey)
		// The primary entry is not the one we serve, and nothing else will
		// close it. Releasing it here is what keeps the disk backend from
		// leaking a file handle on every Vary lookup.
		_ = res.Close()
		if varyErr != nil {
			return varied, varyErr
		}

		// An invalidated base entry invalidates its variants.
		//
		// Invalidation marks the base GET/HEAD keys stale, but each variant is
		// stored under its own Key.Vary(...) key, and the Cache interface
		// cannot enumerate them. Without this, a request whose Vary headers
		// matched an existing variant went on being served the pre-mutation
		// representation, with no revalidation, after a successful
		// POST/PUT/PATCH/DELETE.
		//
		// The variant is judged against the base key's invalidation TIME, not
		// against whether the base entry currently reads as stale. Refetching
		// one variant rewrites the base entry too, so a boolean would make
		// every OTHER variant fresh again as soon as the first was replaced;
		// comparing each variant's own Date against the marker keeps it stale
		// until it is itself replaced.
		if varied != nil {
			// Each variant is judged against the BASE key's marker by its own
			// receive time. A boolean would not do: refetching one variant
			// rewrites the base entry, so every OTHER variant would go back to
			// being a fresh HIT as soon as the first was replaced.
			//
			// h.staleAt reads the cache's own record when it keeps one, and
			// the handler's otherwise -- the previous fallback to the base
			// ENTRY's state reopened exactly that bug for third-party caches.
			if staleAt, marked := h.staleAt(baseKey); marked {
				// StoredAfter, not ReceivedAfter: HTTP dates are
				// second-granular and the marker is not, so a variant
				// revalidated in the same second as the mutation could never
				// clear it and was revalidated upstream on every request.
				if !varied.StoredAfter(staleAt) {
					varied.MarkStale()
				}
			} else if baseStale {
				varied.MarkStale()
			}
		}
		req.servedKey = variantKey
		res = varied
	}

	return res, nil
}

type cacheRequest struct {
	*http.Request
	Key          Key
	Time         time.Time
	CacheControl CacheControl

	// servedKey is the key lookup actually retrieved, which differs from Key
	// when a Vary variant was selected.
	servedKey string
}

func newCacheRequest(r *http.Request) (*cacheRequest, error) {
	cc, err := ParseCacheControl(r.Header.Get("Cache-Control"))
	if err != nil {
		return nil, err
	}

	if r.Proto == "HTTP/1.1" && r.Host == "" {
		return nil, errors.New("host header can't be empty")
	}

	return &cacheRequest{
		Request:      r,
		Key:          NewRequestKey(r),
		Time:         Clock(),
		CacheControl: cc,
	}, nil
}

// sameOriginURL resolves raw against the request URI and returns it only when
// it targets the same origin, shaped so that the resulting key matches what
// NewRequestKey would build for a direct request to that path. Returns nil for
// an unparseable or cross-origin reference.
func (r *cacheRequest) sameOriginURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	if u.Host != "" && !strings.EqualFold(u.Host, r.Host) {
		return nil
	}
	// RawPath is carried through: url.Parse records "/objects/a%2Fb" in
	// RawPath and the decoded "/objects/a/b" in Path, so dropping it produced
	// an invalidation key for a different resource than the one a direct
	// request is cached under.
	// An ABSOLUTE reference keeps its own path, empty included. Rebuilding it
	// as a relative one and resolving against the request made
	// "Location: http://example.org?q" inherit the mutation's path, so a
	// mutation at /orders/1 invalidated "/orders/1?q" and left the named
	// representation fresh. A genuinely relative query-only reference, "?q",
	// still inherits it, which is what RFC 3986 says.
	ref := &url.URL{Path: u.Path, RawPath: u.RawPath, RawQuery: u.RawQuery, ForceQuery: u.ForceQuery}
	if u.Scheme == "" && u.Host == "" {
		ref = r.URL.ResolveReference(ref)
	}
	target := *r.URL
	// The scheme is part of the cache key, so an absolute target naming one
	// must keep it. Cloning r.URL wholesale turned "Location:
	// https://example.org/item" into a key for http://example.org/item --
	// invalidating an unrelated entry while the representation the origin
	// actually named stayed fresh.
	if u.Scheme != "" {
		target.Scheme = u.Scheme
	}
	target.Path = ref.Path
	target.RawPath = ref.RawPath
	target.RawQuery = ref.RawQuery
	// ForceQuery too, or "/item?" and "/item" produce the same key while
	// NewRequestKey keeps them apart -- and, because target starts as a clone
	// of the mutation's URL, an inherited ForceQuery would otherwise add a "?"
	// to a target that never had one.
	target.ForceQuery = ref.ForceQuery
	target.Fragment = ""
	return &target
}

func (r *cacheRequest) isStateChanging() bool {
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func (h *Handler) beginWrite() bool {
	h.lifecycleMu.Lock()
	defer h.lifecycleMu.Unlock()
	if h.closing {
		return false
	}
	h.writes.Add(1)
	Writes.Add(1) // retained for backward compatibility with existing callers
	return true
}

func (h *Handler) endWrite() {
	h.writes.Done()
	Writes.Done()
}

// Shutdown prevents new background cache writes and waits for writes already
// in progress. The caller should invoke it before closing the cache backend.
func (h *Handler) Shutdown(ctx context.Context) error {
	h.lifecycleMu.Lock()
	h.closing = true
	h.lifecycleMu.Unlock()

	done := make(chan struct{})
	go func() {
		h.writes.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Handler) claimMiss(key string) (*missFlight, bool) {
	h.flightsMu.Lock()
	defer h.flightsMu.Unlock()
	if flight, ok := h.flights[key]; ok {
		return flight, false
	}
	flight := &missFlight{done: make(chan struct{})}
	h.flights[key] = flight
	return flight, true
}

func (h *Handler) finishMiss(key string, flight *missFlight) {
	h.flightsMu.Lock()
	if current, ok := h.flights[key]; ok && current == flight {
		delete(h.flights, key)
		close(flight.done)
	}
	h.flightsMu.Unlock()
}

func (r *cacheRequest) isCacheable() bool {
	if r.Method != "GET" && r.Method != "HEAD" {
		return false
	}

	if r.Header.Get("If-Match") != "" ||
		r.Header.Get("If-Unmodified-Since") != "" ||
		r.Header.Get("If-Range") != "" {
		return false
	}

	if maxAge, ok := r.CacheControl.Get("max-age"); ok && maxAge == "0" {
		return false
	}

	if r.CacheControl.Has("no-store") || r.CacheControl.Has("no-cache") {
		return false
	}

	return true
}

func newResponseStreamer(w http.ResponseWriter) (*responseStreamer, error) {
	pr, pw := io.Pipe()
	return &responseStreamer{
		ResponseWriter: w,
		pipeReader:     pr,
		pipeWriter:     pw,
		C:              make(chan struct{}),
	}, nil
}

type responseStreamer struct {
	StatusCode int
	http.ResponseWriter
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter
	// C is closed by WriteHeader to signal the headers' writing. headerOnce ensures it is closed at most once.
	C          chan struct{}
	headerOnce sync.Once
}

// WaitHeaders returns when WriteHeader has been called (i.e. rw.C is closed).
func (rw *responseStreamer) WaitHeaders() {
	for range rw.C {
	}
}

// WriteHeader implements http.ResponseWriter. Safe if called more than once; only the first call closes C and writes the status.
// C is closed only after status and headers are written so that WaitHeaders() callers observe consistent state (no data race).
func (rw *responseStreamer) WriteHeader(status int) {
	rw.headerOnce.Do(func() {
		rw.StatusCode = status
		rw.ResponseWriter.WriteHeader(status)
		close(rw.C)
	})
}

// Write implements http.ResponseWriter.
//
// It calls WriteHeader(StatusOK) first, as the interface requires: "If
// WriteHeader has not yet been called, Write calls WriteHeader(http.StatusOK)
// before writing the data."
//
// Without that, an upstream handler that only calls Write -- the ordinary way
// to answer with 200 -- hung the request FOREVER. C was never closed, so
// passUpstream sat in WaitHeaders and never reached the code that drains the
// pipe, while the upstream goroutine blocked in this very call writing into a
// pipe with no reader. Every test in this package happened to call WriteHeader
// explicitly, so nothing caught it.
func (rw *responseStreamer) Write(b []byte) (int, error) {
	rw.WriteHeader(http.StatusOK)
	return io.MultiWriter(rw.pipeWriter, rw.ResponseWriter).Write(b)
}

// Close finishes the upstream response.
//
// It signals the headers too, for the same reason Write does: a handler that
// returns without writing anything at all still produced a 200, exactly as
// net/http would send one, and WaitHeaders would otherwise block on a response
// that is never coming.
func (rw *responseStreamer) Close() error {
	rw.WriteHeader(http.StatusOK)
	return rw.pipeWriter.Close()
}

// NextReader returns a reader for the response body (reads from the same pipe that upstream writes to).
func (rw *responseStreamer) NextReader() (io.ReadCloser, error) {
	return rw.pipeReader, nil
}

// Resource returns a copy of the responseStreamer as a Resource object
func (rw *responseStreamer) Resource() *Resource {
	b, err := io.ReadAll(rw.pipeReader)
	if err != nil {
		return &Resource{
			header:         rw.Header(),
			statusCode:     rw.StatusCode,
			ReadSeekCloser: errReadSeekCloser{err},
		}
	}
	return NewResourceBytes(rw.StatusCode, b, rw.Header())
}

type errReadSeekCloser struct {
	err error
}

func (e errReadSeekCloser) Error() string {
	return e.err.Error()
}
func (e errReadSeekCloser) Close() error                       { return e.err }
func (e errReadSeekCloser) Read(_ []byte) (int, error)         { return 0, e.err }
func (e errReadSeekCloser) Seek(_ int64, _ int) (int64, error) { return 0, e.err }
