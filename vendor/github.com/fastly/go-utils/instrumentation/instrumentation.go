package instrumentation

import (
	"runtime"
	"strings"
	"syscall"
	"time"
)

type SystemStats struct {
	// Number of goroutines currently running.
	NumGoRoutines int
	// Seconds in userland.
	UserTime float64
	// Seconds in system time.
	SystemTime float64
	// Number of bytes currently allocated.
	BytesAlloc uint64
	// Number of bytes obtained from system.
	BytesFromSystem uint64
	// How long the last GC pause time took in milliseconds.
	GCPauseTimeLast float64
	// Maximum recent GC pause time in milliseconds.
	GCPauseTimeMax float64
	// Total GC pause time in milliseconds.
	GCPauseTimeTotal float64
	// Seconds since last GC pause.
	GCPauseSince float64
}

func GetSystemStats() SystemStats {
	stats := SystemStats{}

	stats.NumGoRoutines = runtime.NumGoroutine()
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) == nil {
		stats.UserTime = float64(r.Utime.Nano()) / 1e9
		stats.SystemTime = float64(r.Stime.Nano()) / 1e9
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	stats.BytesAlloc = mem.Alloc
	stats.BytesFromSystem = mem.Sys
	stats.GCPauseTimeLast = float64(mem.PauseNs[(mem.NumGC+255)%256]) / 1e6
	var gcPauseMax uint64
	for _, v := range mem.PauseNs {
		if v > gcPauseMax {
			gcPauseMax = v
		}
	}
	stats.GCPauseTimeMax = float64(gcPauseMax) / 1e6
	stats.GCPauseTimeTotal = float64(mem.PauseTotalNs) / 1e6
	stats.GCPauseSince = time.Now().Sub(time.Unix(0, int64(mem.LastGC))).Seconds()

	return stats
}

// GetStackTrace returns a string containing the unabbreviated value of
// runtime.Stack(all). Be aware that this function may stop the world multiple
// times in order to obtain the full trace.
func GetStackTrace(all bool) string {
	b := make([]byte, 1<<10)
	for {
		if n := runtime.Stack(b, all); n < len(b) {
			return string(b[:n])
		}
		b = make([]byte, len(b)*2)
	}
}

// GetStackTraces returns a map of goroutines to string
// Slice of their respective stack trace.
func GetStackTraces() map[string][]string {
	stack := GetStackTrace(true)

	info := make(map[string][]string, 0)
	goroutine := ""

	for _, line := range strings.Split(stack, "\n") {
		line = strings.TrimSpace(line)
		line = strings.Trim(line, "\u0000")
		if strings.HasPrefix(line, "goroutine ") {
			goroutine = line
		} else if line != "" {
			info[goroutine] = append(info[goroutine], line)
		}
	}

	return info
}
