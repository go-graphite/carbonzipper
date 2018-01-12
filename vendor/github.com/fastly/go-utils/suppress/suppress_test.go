package suppress_test

import (
	"testing"

	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fastly/go-utils/suppress"
)

/* racy tests that should be ran and validated manually
func TestSilencer1(t *testing.T) {
	test(t, []string{""}, 1, 1000*time.Millisecond, 100*time.Millisecond, 11)
}

func TestSilencer2(t *testing.T) {
	test(t, []string{"spot1", "spot2"}, 1, 1000*time.Millisecond, 100*time.Millisecond, 11)
}

func TestSilencer3(t *testing.T) {
	test(t, []string{""}, 3, 1000*time.Millisecond, 100*time.Millisecond, 11)
}

func TestSilencer4(t *testing.T) {
	test(t, []string{"#1", "#2"}, 3, 1000*time.Millisecond, 100*time.Millisecond, 11)
}

func TestSilencer5(t *testing.T) {
	test(t, []string{"#1", "#2"}, 3, 10*time.Millisecond, 100*time.Millisecond, 2)
}
*/

func test(t *testing.T, ids []string, invocations int, testTime time.Duration, suppressTime time.Duration, expectedPerInvocation int) {
	var attempts, firings, errors int64
	var lasts struct {
		sync.RWMutex
		m map[string]time.Time
	}
	lasts.m = make(map[string]time.Time)

	f := func(count int, tag string) {
		lasts.RLock()
		last := lasts.m[tag]
		lasts.RUnlock()

		now := time.Now()
		if last.IsZero() || math.Abs(float64(now.Sub(last)-suppressTime)) < float64(5*time.Millisecond) {
			atomic.AddInt64(&firings, 1)
		} else {
			t.Logf("Error %q at %v; delta=%v attempts=%d last=%v count=%d",
				tag, time.Now(), now.Sub(last), atomic.LoadInt64(&attempts), last, count)
			atomic.AddInt64(&errors, 1)
		}

		lasts.Lock()
		lasts.m[tag] = now
		lasts.Unlock()
	}

	expected := invocations * len(ids) * expectedPerInvocation

	start := time.Now()
	end := start.Add(testTime)
	for time.Now().Before(end) {
		att := atomic.AddInt64(&attempts, 1)
		if atomic.LoadInt64(&firings) >= int64(expected) {
			break
		}
		tag := ids[att%int64(len(ids))]
		// use separate calls so program counter is different for each
		if invocations > 0 {
			suppress.For(suppressTime, tag, f)
		}
		if invocations > 1 {
			suppress.For(suppressTime, tag, f)
		}
		if invocations > 2 {
			suppress.For(suppressTime, tag, f)
		}
		runtime.Gosched() // yield to other goroutines
	}

	// wait for flusher goroutines to finish
	finished := time.Now()
	time.Sleep(2 * suppressTime)
	finishedAndWaited := time.Now()

	elapsed := finished.Sub(start)
	longElapsed := finishedAndWaited.Sub(start)

	frng := atomic.LoadInt64(&firings)
	e := atomic.LoadInt64(&errors)
	t.Logf("Ran %d iterations in %v (%v with wait), fired correctly %d times (wanted %d) and %d incorrectly",
		attempts, elapsed, longElapsed, frng, expected, e)
	if frng != int64(expected) {
		t.Errorf("Expected %d firings, got %d", expected, frng)
	}
	if e > 0 {
		t.Errorf("Silencer failed to suppress %d times", e)
	}
}

func TestSilencerStalled(t *testing.T) {
	type Event struct {
		time time.Time
		n    int
	}
	events := make([]Event, 0)

	var mu sync.Mutex

	// fire 5 events in rapid succession, all within the suppress window. the
	// first call should happen immediately but the next four should be
	// coalesced at the end of the suppress period.
	start := time.Now()
	for i := 0; i < 5; i++ {
		suppress.For(100*time.Millisecond, "anon", func(n int, tag string) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, Event{time.Now(), n})
			t.Logf("%v", tag)
		})
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(start.Add(110 * time.Millisecond).Sub(time.Now()))

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 || events[0].n != 1 || events[1].n != 4 {
		t.Errorf("unexpected event stream: %+v", events)
	}
}
