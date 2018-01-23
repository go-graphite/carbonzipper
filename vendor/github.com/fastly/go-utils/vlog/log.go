// Package vlog contains functions for optionally printing if a verbose flag
// is set and for coalescing multiple duplicate print calls.
package vlog

import (
	"log"
	"time"

	"github.com/fastly/go-utils/suppress"
)

// Verbose determines whether calls to V* functions actually log or not.
var Verbose bool
var suppressDur = time.Second

// SetSuppressDuration sets the time for repeated log calls to be
// suppressed and coalesced.
func SetSuppressDuration(duration time.Duration) {
	suppressDur = duration
}

// Vlogf calls log.Printf if Verbose is true.
func VLogf(format string, v ...interface{}) {
	if Verbose {
		log.Printf(format, v...)
	}
}

// LogfQuiet coalesces multiple calls from the same location into one log.Printf
// call at the end of the suppress duration. If called multiple times, the output
// will be prepended with "[#x] ", where # is the number of duplicate suppressed calls.
func LogfQuiet(id, format string, v ...interface{}) {
	// this is level 3, logfQuietN is level 2, WrapFor level 1, runtime.Caller 0.
	logfQuietN(3, id, format, v...)
}

// VlogFQuiet calls LogfQuiet if Verbose is true.
func VLogfQuiet(id, format string, v ...interface{}) {
	if Verbose {
		logfQuietN(3, id, format, v...)
	}
}

func logfQuietN(depth int, id, format string, v ...interface{}) {
	suppress.WrapFor(depth, suppressDur, id, func(n int, id string) {
		if n <= 1 {
			log.Printf(format, v...)
		} else {
			log.Printf("[%dx] "+format, append([]interface{}{n}, v...)...) // ...
		}
	})
}
