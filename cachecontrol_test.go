package httpcache_test

import (
	"net/http"
	"reflect"
	"testing"

	. "github.com/lj020326/httpcache-kit/v2"
)

func TestParsingCacheControl(t *testing.T) {
	table := []struct {
		ccString string
		ccStruct CacheControl
	}{
		{`public, private="set-cookie", max-age=100`, CacheControl{
			"public":  []string{},
			"private": []string{"set-cookie"},
			"max-age": []string{"100"},
		}},
		{` foo="max-age=8, space",  public`, CacheControl{
			"public": []string{},
			"foo":    []string{"max-age=8, space"},
		}},
		{`s-maxage=86400`, CacheControl{
			"s-maxage": []string{"86400"},
		}},
		{`max-stale`, CacheControl{
			"max-stale": []string{},
		}},
		{`max-stale=60`, CacheControl{
			"max-stale": []string{"60"},
		}},
		{`" max-age=8,max-age=8 "=blah`, CacheControl{
			" max-age=8,max-age=8 ": []string{"blah"},
		}},
	}

	for _, expect := range table {
		cc, err := ParseCacheControl(expect.ccString)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(cc, expect.ccStruct) {
			t.Fatalf("cc should be equal")
		}
		if cc.String() == "" {
			t.Fatalf("cc string should not be empty")
		}
	}
}

func TestCacheControl_Get_EmptyValue(t *testing.T) {
	cc, _ := ParseCacheControl("max-age")
	val, exists := cc.Get("max-age")
	if !exists {
		t.Error("key should exist")
	}
	if val != "" {
		t.Errorf("empty value: got %q", val)
	}
}

func TestCacheControl_Get_NoKey(t *testing.T) {
	cc := CacheControl{}
	_, exists := cc.Get("missing")
	if exists {
		t.Error("key should not exist")
	}
}

func TestCacheControl_Add_Has(t *testing.T) {
	cc := make(CacheControl)
	cc.Add("key", "val")
	if !cc.Has("key") {
		t.Error("Has should be true")
	}
	cc.Add("noval", "")
	if !cc.Has("noval") {
		t.Error("Has noval")
	}
}

func TestCacheControl_Duration_Invalid(t *testing.T) {
	cc, _ := ParseCacheControl("max-age=invalid")
	_, err := cc.Duration("max-age")
	if err == nil {
		t.Error("expected error for invalid duration")
	}
}

func TestParseCacheControlHeaders_Multiple(t *testing.T) {
	h := http.Header{}
	h.Add("Cache-Control", "max-age=60")
	h.Add("Cache-Control", "no-cache")
	cc, err := ParseCacheControlHeaders(h)
	if err != nil {
		t.Fatal(err)
	}
	if !cc.Has("max-age") || !cc.Has("no-cache") {
		t.Errorf("expected both directives: %v", cc)
	}
}

func BenchmarkCacheControlParsing(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseCacheControl(`public, private="set-cookie", max-age=100`)
		if err != nil {
			b.Fatal(err)
		}
	}
}
