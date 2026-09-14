package httpcache_test

import (
	"io"
	"log"
	"testing"

	"github.com/lj020326/httpcache-kit/v2"
	logger "github.com/lj020326/logger-kit/v2"
)

func TestSetLogger(t *testing.T) {
	l := logger.Default()
	httpcache.SetLogger(l)
	// Call with nil - should not panic and should not replace
	httpcache.SetLogger(nil)
	httpcache.SetLogger(l)
}

// TestDebugLogging triggers package-level debugf by enabling debug logging and making a request.
func TestDebugLogging(t *testing.T) {
	prev := httpcache.IsDebugLogging()
	httpcache.SetDebugLogging(true)
	defer func() { httpcache.SetDebugLogging(prev) }()
	log.SetOutput(io.Discard)
	client, _ := testSetup()
	_ = client.get("/")
}
