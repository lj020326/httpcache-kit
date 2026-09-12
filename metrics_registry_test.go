package httpcache

import (
	"testing"

	metrics "github.com/soulteary/metrics-kit/v2"
)

// Two handlers in one process can share a metrics registry, and each calls
// NewCacheMetrics. That builds collectors by name, so the second call used to
// hit metrics-kit's duplicate-registration path and panic:
//
//	panic: duplicate metrics collector registration attempted
//
// metrics-kit v2.2.0 reuses the already-registered collector instead, which is
// what makes a shared registry usable. Pinned here because the failure mode is a
// process-killing panic at startup, and because a downgrade would reintroduce it.
func TestNewCacheMetricsTwiceOnOneRegistry(t *testing.T) {
	reg := metrics.NewRegistry("httpcache_shared_registry_test")

	first := NewCacheMetrics(reg)
	if first == nil {
		t.Fatal("NewCacheMetrics returned nil")
	}

	second := NewCacheMetrics(reg)
	if second == nil {
		t.Fatal("second NewCacheMetrics returned nil")
	}

	// Both are usable: recording through either must not panic either.
	first.RecordCacheHit("GET")
	second.RecordCacheMiss("GET")
}
