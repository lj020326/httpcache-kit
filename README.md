# httpcache-kit

[![Go Reference](https://pkg.go.dev/badge/github.com/soulteary/httpcache-kit/v2.svg)](https://pkg.go.dev/github.com/soulteary/httpcache-kit/v2)
[![Go Report Card](.github/goreportcard.svg)](.github/goreportcard-report.md)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

[中文文档](README_CN.md)

An RFC 7234-compliant HTTP cache handler for Go. Wraps any `http.Handler` —
typically a `httputil.ReverseProxy` — with memory or disk storage, Cache-Control
parsing, conditional revalidation, RFC 7234 invalidation and optional Prometheus
metrics.

Evolved from [lox/httpcache](https://github.com/lox/httpcache) (MIT).

## Features

- **RFC 7234 caching**: freshness, heuristic expiration, revalidation, `Vary`
- **Invalidation**: an unsafe method invalidates the request URI and the URIs named by the response's `Location` / `Content-Location`
- **Private and shared profiles**: a shared cache refuses `private` responses and strips `private` headers
- **Memory, disk and VFS backends**: disk storage goes through [vfs-kit](https://github.com/soulteary/vfs-kit)
- **Bounded**: configurable TTL, max size, cleanup interval, LRU eviction
- **Observable**: optional Prometheus metrics via [metrics-kit](https://github.com/soulteary/metrics-kit), debug logging via [logger-kit](https://github.com/soulteary/logger-kit)
- **Graceful shutdown**: background cache writes are tracked and can be awaited

## Requirements

- **Go 1.27+** (`go.mod` declares `go 1.27.0`)
- `github.com/prometheus/client_golang` for metrics

The v2 module line uses the Fiber v3-compatible `logger-kit/v2` and
`metrics-kit/v2` types exposed by the cache API. Applications still on the v1
kit ecosystem should remain on `github.com/soulteary/httpcache-kit` v1.

## Installation

```bash
go get github.com/soulteary/httpcache-kit/v2
```

## Quick Start

### Shared cache (reverse proxy)

```go
package main

import (
    "log"
    "net/http"
    "net/http/httputil"

    httpcache "github.com/soulteary/httpcache-kit/v2"
)

func main() {
    proxy := &httputil.ReverseProxy{
        Director: func(r *http.Request) {},
    }

    // NewSharedHandler, not NewHandler: this cache serves more than one user.
    handler := httpcache.NewSharedHandler(httpcache.NewMemoryCache(), proxy)

    log.Print("proxy listening on http://localhost:8080")
    log.Fatal(http.ListenAndServe(":8080", handler))
}
```

### Private cache (single user)

```go
handler := httpcache.NewHandler(httpcache.NewMemoryCache(), upstream)
```

### Disk-backed, with options

```go
cache, err := httpcache.NewDiskCacheWithConfig("/var/cache/myproxy",
    httpcache.DefaultCacheConfig().
        WithMaxSize(2 * 1024 * 1024 * 1024).
        WithTTL(24 * time.Hour).
        WithCleanupInterval(30 * time.Minute))
if err != nil {
    log.Fatal(err)
}
defer cache.Close()

handler := httpcache.NewHandlerWithOptions(cache, proxy, &httpcache.HandlerOptions{
    Logger: myLogger,
})
handler.Shared = true
```

## Shared vs Private

This is the one decision to get right, because the unsafe default is the one a
field can be forgotten in.

| | `NewHandler` | `NewSharedHandler` |
|---|---|---|
| `Handler.Shared` | `false` | `true` |
| Correct for | a per-user cache | a reverse proxy, any cache serving more than one user |
| `Cache-Control: private` | stored | **not stored** |
| Responses to authorized requests | stored | **not stored** unless marked `public` or carrying `s-maxage` |
| Headers listed in `private` | kept | **stripped before storing** |
| `s-maxage` | ignored | honoured over `max-age` |

A shared cache running with `Shared: false` will serve one user's private
response to another. Prefer the constructor over setting the field — a
constructor cannot be forgotten.

## Backends

```go
httpcache.NewMemoryCache()                              // Cache
httpcache.NewMemoryCacheWithConfig(cfg)                 // ExtendedCache
httpcache.NewDiskCache("/var/cache/x")                  // Cache
httpcache.NewDiskCacheWithConfig("/var/cache/x", cfg)   // ExtendedCache
httpcache.NewVFSCache(fs)                               // Cache
httpcache.NewVFSCacheWithConfig(fs, cfg)                // ExtendedCache
```

`Cache` is the minimum the handler needs:

```go
type Cache interface {
    Header(key string) (Header, error)
    Retrieve(key string) (*Resource, error)
    Store(res *Resource, keys ...string) error
    Freshen(res *Resource, keys ...string) error
    Invalidate(keys ...string)
}
```

`ExtendedCache` adds management, and is what the `*WithConfig` constructors
return:

```go
type ExtendedCache interface {
    Cache
    Stats() CacheStats
    Cleanup() CleanupResult
    Purge() error
    Close() error
}
```

```go
stats := cache.Stats()
log.Printf("items=%d bytes=%d hits=%d misses=%d stale=%d",
    stats.ItemCount, stats.TotalSize, stats.HitCount, stats.MissCount, stats.StaleCount)

result := cache.Cleanup()
log.Printf("removed %d items (%d bytes, %d stale markers) in %s",
    result.RemovedItems, result.RemovedBytes, result.RemovedStaleEntries, result.Duration)
```

`Retrieve` returns `ErrNotFoundInCache` for a miss — match it with `errors.Is`.
A `*Resource` it returns owns a file handle on the disk backend, so close it.

## Configuration

```go
cfg := httpcache.DefaultCacheConfig().
    WithMaxSize(10 * 1024 * 1024 * 1024).
    WithTTL(7 * 24 * time.Hour).
    WithCleanupInterval(1 * time.Hour).
    WithStaleMapTTL(24 * time.Hour).
    Validate()
```

| Option | Default | Notes |
|--------|---------|-------|
| `MaxSize` | `DefaultMaxCacheSize` (10 GiB) | `0` means unbounded; LRU eviction above it |
| `TTL` | `DefaultCacheTTL` (7 days) | `0` means no TTL |
| `CleanupInterval` | `DefaultCleanupInterval` (1 hour) | `0` disables the background cycle |
| `StaleMapTTL` | `DefaultStaleMapTTL` (24 hours) | how long the backend remembers a stale marker |

`Validate()` clamps negatives to zero and restores `StaleMapTTL` to its default
if it is non-positive. It mutates and returns the same config, so it chains.

### Handler options

```go
handler := httpcache.NewHandlerWithOptions(cache, upstream, &httpcache.HandlerOptions{
    Logger:         myLogger,          // *logger.Logger; nil uses the package logger
    StaleMarkerTTL: 7 * 24 * time.Hour,
})
```

`StaleMarkerTTL` is how long the **handler** remembers an invalidation for a
`Cache` that does not track staleness itself. It must be at least as long as
that cache retains entries: the marker is the only record that older entries are
pre-mutation, so expiring it first republishes them as fresh. Zero means
`DefaultCacheTTL`. A cache that keeps its own record ignores this.

## Cache Keys and Vary

A key is derived from the **effective request URI** and the method:

```go
key := httpcache.NewRequestKey(r)           // from a request
key = httpcache.NewKey("GET", u, r.Header)  // explicitly
key = key.ForMethod("HEAD")                 // the sibling key for another method
key = key.Vary(resp.Header.Get("Vary"), r)  // the variant key
keyString := key.String()
```

The request's own `Content-Location` does **not** affect the key. RFC 7234 uses
`Content-Location`, but the *response's*, and only for invalidation — letting a
request choose its key allows a client to park its response under another URL's
key, or read another URL's entry.

`Key.String()` is injective: the `Vary` section uses a control-byte separator
with each value quoted, and since `url.URL.String` percent-encodes control bytes
while `strconv.Quote` escapes them, neither side of the boundary can contain the
delimiter. Keys without `Vary` are plain and unchanged.

## Invalidation

An unsafe method (`POST`, `PUT`, `DELETE`, `PATCH`) invalidates, per RFC 7234
section 4.4:

- the effective request URI,
- the URI in the response's `Location` header,
- the URI in the response's `Content-Location` header,

for both the `GET` and `HEAD` keys. Cross-origin targets are ignored, so a
response cannot evict another origin's entries.

## Metrics

```go
import metrics "github.com/soulteary/metrics-kit/v2"

registry := metrics.NewRegistry("myproxy")
m := httpcache.NewCacheMetrics(registry)
handler.SetMetrics(m)

// Or register a process-wide default
httpcache.SetDefaultMetrics(m)
m = httpcache.GetDefaultMetrics()

// Feed gauges from a cache's own view
m.UpdateCacheStats(cache.Stats())
```

`CacheMetrics` exposes hits, misses, skips, evictions, store and retrieve
operations, item count, size in bytes, stale count, cleanup duration, and
upstream duration and errors.

## Logging

```go
import logger "github.com/soulteary/logger-kit/v2"

httpcache.SetLogger(myLogger)     // package-level logger
httpcache.SetDebugLogging(true)   // verbose cache decisions
on := httpcache.IsDebugLogging()
```

## Response Headers

| Header | Values | Meaning |
|--------|--------|---------|
| `X-Cache` | `HIT` | served from cache |
| | `MISS` | fetched from upstream and stored |
| | `SKIP` | not cacheable, or cache bypassed |
| `Proxy-Date` | HTTP-date | when this cache received the response |

## Cache-Control

```go
cc, err := httpcache.ParseCacheControl("max-age=3600, s-maxage=60, private")
cc, err = httpcache.ParseCacheControlHeaders(resp.Header)

cc.Has("no-store")
value, ok := cc.Get("max-age")
d, err := cc.Duration("max-age")
cc.Add("stale-while-revalidate", "30")
header := cc.String()
```

## Resources

```go
res := httpcache.NewResourceBytes(200, body, header)
res = httpcache.NewResource(200, readSeekCloser, header)

res.Status()
res.Header()
res.Age()
res.Expires()
res.MaxAge(shared)
res.HasExplicitExpiration()
res.HeuristicFreshness()
res.HasValidators()
res.MustValidate(shared)
res.IsStale()
res.MarkStale()
res.LastModified()
res.RemovePrivateHeaders()
res.Via()
```

## Graceful Shutdown

The handler stores responses in the background. Await them before closing the
backend, or an in-flight write hits a closed cache:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := handler.Shutdown(ctx); err != nil {
    log.Printf("cache shutdown: %v", err)
}
cache.Close()
```

`httpcache.Writes` is a package-level `sync.WaitGroup` covering every handler's
background writes, for tests that need to wait on all of them.

## Caveats

- Conditional requests carrying `Range` are not cached.
- `Clock` is a package-level variable, swappable in tests.

## Upgrade Notes (v2.2.0)

This release changes which entries are served and how they are keyed. One
constructor and one option were added; nothing was removed.

- **Invalidation now happens.** `invalidateResource()`'s entire body was a debug
  log call, so a resource that had been `POST`ed to, `PUT` or `DELETE`d kept
  being served from cache until its own freshness lifetime expired. It now
  invalidates the request URI and the same-origin URIs named by the response's
  `Location` and `Content-Location`, for both the `GET` and `HEAD` keys. **Expect
  more upstream traffic after unsafe methods** — that is the bug being fixed.
- **A request's `Content-Location` no longer selects the cache key.** It did,
  while the upstream request still used the original URL, so a client could park
  its own response under a different URL's key (shared cache poisoning) or read
  another URL's entry. If you deliberately relied on that to alias entries, there
  is no replacement — it was not a feature.
- **`Key.String()` changed for keys with `Vary`.** It joined a raw URL and raw
  header values with `":"` and `"::"`, so a crafted URL could collide with a
  different URL carrying `Vary` values. The `Vary` section now uses a quoted,
  control-byte-separated encoding. **Existing cached entries with `Vary` will
  miss once** and be re-fetched; keys without `Vary` are unchanged.
- **`NewSharedHandler` is the constructor for a reverse proxy.** `Shared`
  defaults to `false`, which is the correct private-cache profile and the wrong
  one for a shared cache — and a field can be forgotten in a way a constructor
  cannot. If you set `handler.Shared = true` by hand, nothing breaks; new code
  should use the constructor.
- **The disk backend no longer leaks a file handle per `Vary` lookup.** The
  primary `Resource` was overwritten without being closed.
- **`CacheControl.String()` no longer emits empty entries.** It allocated its key
  slice with `make([]string, len(cc))` and then appended, so the output began with
  `len(cc)` empty fields.
- **`HandlerOptions.StaleMarkerTTL` is new.** Set it to at least your cache's
  retention when the cache does not track staleness itself; otherwise an expired
  marker republishes pre-mutation entries as fresh.

## Testing

```bash
go test ./...

# With coverage
go test ./... -coverprofile=coverage.out -covermode=atomic
go tool cover -func=coverage.out
```

## References

- [RFC 7234](http://httpwg.github.io/specs/rfc7234.html) — HTTP/1.1 Caching
- [lox/httpcache](https://github.com/lox/httpcache) — the original library (MIT)

## License

Apache License 2.0 — see [LICENSE](LICENSE). Portions derive from
[lox/httpcache](https://github.com/lox/httpcache), MIT licensed.
