// Package suppress has functions to suppress repeated function calls
// into one aggregated function call.
package suppress

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type suppressorState struct {
	count int64
	f     *func(int, string)
}

type locKey struct {
	pc uintptr
	id string
}

var (
	lock        sync.RWMutex
	suppressors = make(map[locKey]*suppressorState, 100)
)

// For aggregates repeated calls to itself and calls f once every duration
// with the number of aggregated calls and the tag coalesced with the calling
// file and line number
func For(duration time.Duration, id string, f func(int, string)) {
	// WrapFor is depth 1,
	WrapFor(2, duration, id, f)
}

// WrapFor is the same as For, except the depth of the call stack can be
// chosen for what ID to tag and coalesce. runtime.Caller will have depth 0, and the
// call to this function will have depth 1, so any additional layers before calling
// this function should have depth >= 1.
func WrapFor(depth int, duration time.Duration, id string, f func(int, string)) {
	pc, file, line, _ := runtime.Caller(depth)
	key := locKey{pc, id}

	lock.RLock()
	state, exists := suppressors[key]
	lock.RUnlock()
	if !exists {
		lock.Lock()
		state, exists = suppressors[key]
		if !exists {
			state = &suppressorState{count: -1, f: &f}
			suppressors[key] = state
		}
		lock.Unlock()
	}

	if atomic.AddInt64(&state.count, 1) > 0 {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&state.f)), unsafe.Pointer(&f))
		return
	}

	tag := fmt.Sprintf("%s:%d %s", filepath.Base(file), line, id)

	// Subsequent calls will be suppressed while this goroutine is running.
	// Every `duration`, it checks to see how many calls have been suppressed.
	// If there have been none, it exits, and sets the suppressorState count to
	// -1, indicating that there is no active suppression.
	go func() {
		ticker := time.NewTicker(duration)
		defer ticker.Stop()

		for range ticker.C {
			if atomic.CompareAndSwapInt64(&state.count, 0, -1) {
				return
			}
			count := atomic.LoadInt64(&state.count)
			f := *(*func(int, string))(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&state.f))))
			f(int(count), tag)
			atomic.AddInt64(&state.count, -count)
		}
	}()

	f(1, tag)
}
