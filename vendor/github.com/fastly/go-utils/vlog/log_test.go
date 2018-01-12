package vlog_test

import (
	"testing"

	"bytes"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/fastly/go-utils/vlog"
)

func TestSilencerLogfQuiet(t *testing.T) {
	res := testLogfQuiet(t, false)
	patterns := []string{"(?s:\\d Line 3 message 0.*\\d Line 4 message 0)", "\\d \\[4x\\] Line 3 message 4", "\\d \\[4x\\] Line 4 message 4"}
	for _, pattern := range patterns {
		if matched, err := regexp.Match(pattern, res); err != nil {
			t.Errorf("error in regexp: %s", err)
		} else if !matched {
			t.Errorf("couldn't match /%s/ against %q", pattern, res)
		}
	}
}

func TestSilencerVLogfQuietVerbose(t *testing.T) {
	vlog.Verbose = true

	res := testLogfQuiet(t, true)
	patterns := []string{"Line 1 message 0", "Line 2 message 0", "\\[4x\\] Line 1 message 4", "\\[4x\\] Line 2 message 4"}
	for _, pattern := range patterns {
		if matched, err := regexp.Match(pattern, res); err != nil {
			t.Errorf("error in regexp: %s", err)
		} else if !matched {
			t.Errorf("couldn't match /%s/ against %q", pattern, res)
		}
	}
}

func TestSilencerVLogfQuietNoVerbose(t *testing.T) {
	vlog.Verbose = false

	if len(testLogfQuiet(t, true)) != 0 {
		t.Errorf("Expected no output from VLogfQuiet with Verbose=false")
	}
}

func testLogfQuiet(t *testing.T, vlogf bool) []byte {
	var buf bytes.Buffer
	log.SetOutput(&buf)

	start := time.Now()
	squelchTime := 100 * time.Millisecond
	vlog.SetSuppressDuration(squelchTime)
	for i := 0; i < 5; i++ {
		if vlogf {
			vlog.VLogfQuiet("", "Line 1 message %d", i)
			vlog.VLogfQuiet("", "Line 2 message %d", i)
		} else {
			vlog.LogfQuiet("", "Line 3 message %d", i)
			vlog.LogfQuiet("", "Line 4 message %d", i)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(start.Add(110 * time.Millisecond).Sub(time.Now()))

	log.SetOutput(os.Stdout)

	return buf.Bytes()
}
