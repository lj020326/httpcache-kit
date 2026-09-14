package httpcache

import (
	"sync/atomic"

	logger "github.com/lj020326/logger-kit/v2"
)

// debugLogging is the atomic flag for whether debug messages are logged.
var debugLogging atomic.Bool

// SetDebugLogging sets whether debug messages are logged. Safe for concurrent use.
func SetDebugLogging(b bool) { debugLogging.Store(b) }

// IsDebugLogging returns whether debug messages are logged. Safe for concurrent use.
func IsDebugLogging() bool { return debugLogging.Load() }

// cacheLogger is the logger instance used by httpcache package
var cacheLogger = logger.Default()

// SetLogger sets the package-level logger used when Handler has no Logger injected.
// Prefer passing Logger via NewHandlerWithOptions; SetLogger is retained for backward compatibility.
func SetLogger(log *logger.Logger) {
	if log != nil {
		cacheLogger = log
	}
}

func debugf(format string, args ...interface{}) {
	if IsDebugLogging() {
		cacheLogger.Debug().Msgf(format, args...)
	}
}

func errorf(format string, args ...interface{}) {
	cacheLogger.Error().Msgf(format, args...)
}
