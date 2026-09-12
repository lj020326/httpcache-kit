# httpcache-kit

[![Go Reference](https://pkg.go.dev/badge/github.com/soulteary/httpcache-kit/v2.svg)](https://pkg.go.dev/github.com/soulteary/httpcache-kit/v2)
[![Go Report Card](.github/goreportcard.svg)](.github/goreportcard-report.md)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

[English](README.md)

符合 RFC 7234 的 Go HTTP 缓存处理器。把任意 `http.Handler`——通常是
`httputil.ReverseProxy`——包一层，提供内存或磁盘存储、Cache-Control 解析、条件重校验、
RFC 7234 失效，以及可选的 Prometheus 指标。

源自 [lox/httpcache](https://github.com/lox/httpcache)（MIT）。

## 特性

- **RFC 7234 缓存**：新鲜度、启发式过期、重校验、`Vary`
- **失效**：非安全方法会使请求 URI，以及响应 `Location` / `Content-Location` 指向的 URI 失效
- **私有与共享两种模式**：共享缓存拒绝存储 `private` 响应，并剥掉 `private` 列出的 header
- **内存、磁盘与 VFS 后端**：磁盘存储经由 [vfs-kit](https://github.com/soulteary/vfs-kit)
- **有界**：可配置 TTL、最大容量、清理周期、LRU 淘汰
- **可观测**：通过 [metrics-kit](https://github.com/soulteary/metrics-kit) 暴露 Prometheus 指标，通过 [logger-kit](https://github.com/soulteary/logger-kit) 输出调试日志
- **优雅关闭**：后台缓存写入被跟踪，可以等待其完成

## 要求

- **Go 1.27+**（`go.mod` 声明 `go 1.27.0`）
- 指标功能需要 `github.com/prometheus/client_golang`

v2 模块线使用缓存 API 暴露出的、兼容 Fiber v3 的 `logger-kit/v2` 与 `metrics-kit/v2`
类型。仍在 v1 kit 生态上的应用请继续使用 `github.com/soulteary/httpcache-kit` v1。

## 安装

```bash
go get github.com/soulteary/httpcache-kit/v2
```

## 快速开始

### 共享缓存（反向代理）

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

    // 用 NewSharedHandler，不是 NewHandler：这个缓存服务于多个用户。
    handler := httpcache.NewSharedHandler(httpcache.NewMemoryCache(), proxy)

    log.Print("proxy listening on http://localhost:8080")
    log.Fatal(http.ListenAndServe(":8080", handler))
}
```

### 私有缓存（单用户）

```go
handler := httpcache.NewHandler(httpcache.NewMemoryCache(), upstream)
```

### 磁盘后端 + 配置

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

## 共享与私有

这是唯一必须选对的决定，因为不安全的那个恰好是默认值，而字段是会被忘记设置的。

| | `NewHandler` | `NewSharedHandler` |
|---|---|---|
| `Handler.Shared` | `false` | `true` |
| 适用于 | 单用户缓存 | 反向代理，以及任何服务多个用户的缓存 |
| `Cache-Control: private` | 会存 | **不存** |
| 对已授权请求的响应 | 会存 | 除非标了 `public` 或带 `s-maxage`，否则**不存** |
| `private` 列出的 header | 保留 | 存储前**剥掉** |
| `s-maxage` | 忽略 | 优先于 `max-age` |

以 `Shared: false` 运行的共享缓存，会把一个用户的私有响应发给另一个用户。请优先使用
构造函数而不是手动设置字段——构造函数不会被忘记。

## 后端

```go
httpcache.NewMemoryCache()                              // Cache
httpcache.NewMemoryCacheWithConfig(cfg)                 // ExtendedCache
httpcache.NewDiskCache("/var/cache/x")                  // Cache
httpcache.NewDiskCacheWithConfig("/var/cache/x", cfg)   // ExtendedCache
httpcache.NewVFSCache(fs)                               // Cache
httpcache.NewVFSCacheWithConfig(fs, cfg)                // ExtendedCache
```

`Cache` 是处理器所需的最小接口：

```go
type Cache interface {
    Header(key string) (Header, error)
    Retrieve(key string) (*Resource, error)
    Store(res *Resource, keys ...string) error
    Freshen(res *Resource, keys ...string) error
    Invalidate(keys ...string)
}
```

`ExtendedCache` 增加了管理能力，也是 `*WithConfig` 系列构造函数的返回类型：

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

未命中时 `Retrieve` 返回 `ErrNotFoundInCache`——请用 `errors.Is` 判断。它返回的
`*Resource` 在磁盘后端上持有一个文件句柄，用完请关闭。

## 配置

```go
cfg := httpcache.DefaultCacheConfig().
    WithMaxSize(10 * 1024 * 1024 * 1024).
    WithTTL(7 * 24 * time.Hour).
    WithCleanupInterval(1 * time.Hour).
    WithStaleMapTTL(24 * time.Hour).
    Validate()
```

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `MaxSize` | `DefaultMaxCacheSize`（10 GiB） | `0` 表示不限制；超过则按 LRU 淘汰 |
| `TTL` | `DefaultCacheTTL`（7 天） | `0` 表示没有 TTL |
| `CleanupInterval` | `DefaultCleanupInterval`（1 小时） | `0` 关闭后台清理周期 |
| `StaleMapTTL` | `DefaultStaleMapTTL`（24 小时） | 后端记住失效标记的时长 |

`Validate()` 会把负值归零，并在 `StaleMapTTL` 非正时恢复默认值。它原地修改并返回同一个
配置对象，因此可以链式调用。

### 处理器选项

```go
handler := httpcache.NewHandlerWithOptions(cache, upstream, &httpcache.HandlerOptions{
    Logger:         myLogger,          // *logger.Logger；nil 表示用包级 logger
    StaleMarkerTTL: 7 * 24 * time.Hour,
})
```

`StaleMarkerTTL` 是**处理器**为那些自身不记录失效状态的 `Cache` 保留失效标记的时长。
它必须不短于该缓存保留条目的时间：这个标记是"比它更老的条目都是变更前的数据"的唯一
记录，标记先过期就会把那些条目重新当作新鲜数据发出去。零表示 `DefaultCacheTTL`。
自己记录失效状态的缓存会忽略这个值。

## 缓存键与 Vary

键由**有效请求 URI** 和方法导出：

```go
key := httpcache.NewRequestKey(r)           // 来自请求
key = httpcache.NewKey("GET", u, r.Header)  // 显式构造
key = key.ForMethod("HEAD")                 // 另一方法的同级键
key = key.Vary(resp.Header.Get("Vary"), r)  // 变体键
keyString := key.String()
```

请求自身的 `Content-Location` **不会**影响键。RFC 7234 确实会用到
`Content-Location`，但用的是*响应*的，而且只用于失效——让请求自己挑键，意味着客户端
可以把自己的响应塞到别的 URL 的键下，或者读到别的 URL 的条目。

`Key.String()` 是单射的：`Vary` 部分采用控制字节作分隔符、每个值加引号，而
`url.URL.String` 会对控制字节做百分号编码、`strconv.Quote` 会转义它们，因此分界线
两侧都不可能含有该分隔符。不带 `Vary` 的键保持原样。

## 失效

非安全方法（`POST`、`PUT`、`DELETE`、`PATCH`）会按 RFC 7234 第 4.4 节使以下目标失效：

- 有效请求 URI，
- 响应 `Location` header 中的 URI，
- 响应 `Content-Location` header 中的 URI，

`GET` 和 `HEAD` 两个键都会处理。跨源目标会被忽略，因此一个响应无法清掉其他源的条目。

## 指标

```go
import metrics "github.com/soulteary/metrics-kit/v2"

registry := metrics.NewRegistry("myproxy")
m := httpcache.NewCacheMetrics(registry)
handler.SetMetrics(m)

// 或者注册一个进程级默认值
httpcache.SetDefaultMetrics(m)
m = httpcache.GetDefaultMetrics()

// 用缓存自己的视图刷新 gauge
m.UpdateCacheStats(cache.Stats())
```

`CacheMetrics` 暴露命中、未命中、跳过、淘汰、存取操作数、条目数、字节数、陈旧条目数、
清理耗时，以及上游耗时与错误。

## 日志

```go
import logger "github.com/soulteary/logger-kit/v2"

httpcache.SetLogger(myLogger)     // 包级 logger
httpcache.SetDebugLogging(true)   // 详细打印缓存决策
on := httpcache.IsDebugLogging()
```

## 响应 Header

| Header | 取值 | 含义 |
|--------|------|------|
| `X-Cache` | `HIT` | 由缓存响应 |
| | `MISS` | 回源取得并已存储 |
| | `SKIP` | 不可缓存，或绕过了缓存 |
| `Proxy-Date` | HTTP-date | 本缓存收到该响应的时间 |

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

## Resource

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

## 优雅关闭

处理器在后台存储响应。关闭后端之前请先等待这些写入完成，否则在途写入会撞上一个已关闭
的缓存：

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := handler.Shutdown(ctx); err != nil {
    log.Printf("cache shutdown: %v", err)
}
cache.Close()
```

`httpcache.Writes` 是一个包级 `sync.WaitGroup`，覆盖所有处理器的后台写入，便于测试
等待全部完成。

## 注意事项

- 带 `Range` 的条件请求不会被缓存。
- `Clock` 是包级变量，测试中可以替换。

## 升级说明（v2.2.0）

本次发布改变了"哪些条目会被发出"以及"键怎么算"。新增一个构造函数和一个选项，没有删除
任何东西。

- **失效现在真的会发生。** `invalidateResource()` 的整个函数体只是一行调试日志，于是
  一个被 `POST`、`PUT` 或 `DELETE` 过的资源，仍然会从缓存里被发出，直到它自己的新鲜期
  到期。现在它会使请求 URI，以及响应 `Location` 和 `Content-Location` 指向的同源 URI
  失效，`GET` 和 `HEAD` 两个键都处理。**非安全方法之后请预期更多回源流量**——这正是
  被修掉的那个缺陷。
- **请求的 `Content-Location` 不再选择缓存键。** 它此前会，而回源请求用的仍是原始
  URL，于是客户端可以把自己的响应塞到别的 URL 的键下（共享缓存投毒），或读到别的 URL
  的条目。如果你此前是刻意利用它来给条目做别名，没有替代方案——那不是一个特性。
- **带 `Vary` 的键，`Key.String()` 变了。** 它此前用 `":"` 和 `"::"` 拼接原始 URL 和
  原始 header 值，于是精心构造的 URL 可以和另一个带 `Vary` 值的 URL 撞键。现在
  `Vary` 部分采用加引号、以控制字节分隔的编码。**已有的带 `Vary` 的缓存条目会未命中
  一次**并重新取回；不带 `Vary` 的键不变。
- **反向代理请用 `NewSharedHandler`。** `Shared` 默认为 `false`，这对私有缓存是对的、
  对共享缓存是错的——而字段会被忘记，构造函数不会。如果你是手动设置
  `handler.Shared = true`，不会有任何问题；新代码请用构造函数。
- **磁盘后端不再每次 `Vary` 查找泄漏一个文件句柄。** 主 `Resource` 此前被直接覆盖而
  没有关闭。
- **`CacheControl.String()` 不再输出空项。** 它此前用 `make([]string, len(cc))` 分配
  键切片然后再 append，于是输出开头有 `len(cc)` 个空字段。
- **新增 `HandlerOptions.StaleMarkerTTL`。** 当缓存自身不跟踪失效状态时，请把它设成
  不短于缓存的保留时长；否则标记过期会把变更前的条目重新当作新鲜数据发出去。

## 测试

```bash
go test ./...

# 带覆盖率
go test ./... -coverprofile=coverage.out -covermode=atomic
go tool cover -func=coverage.out
```

## 参考

- [RFC 7234](http://httpwg.github.io/specs/rfc7234.html) —— HTTP/1.1 缓存
- [lox/httpcache](https://github.com/lox/httpcache) —— 原始库（MIT）

## 许可证

Apache License 2.0 —— 详见 [LICENSE](LICENSE)。部分代码源自
[lox/httpcache](https://github.com/lox/httpcache)，采用 MIT 许可。
