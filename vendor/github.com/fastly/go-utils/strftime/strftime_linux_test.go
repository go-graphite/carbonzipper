// +build linux

package strftime_test

import (
	"testing"

	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/fastly/go-utils/strftime"
)

// these test the pure Go implementation (linux otherwise won't use/test it)
func TestStrftimePureAgainstReference(t *testing.T) {
	testStrftimeAgainstReference(t, strftime.StrftimePure, true)
}

func TestStrftimePureAgainstPerl(t *testing.T) {
	testStrftimeAgainstPerl(t, strftime.StrftimePure, true)
}

// Compares Strftime's output to strftime as exposed by Perl.
func TestStrftimeAgainstPerl(t *testing.T) {
	testStrftimeAgainstPerl(t, strftime.Strftime, false)
}

// number of consecutive seconds to test, starting from refTime
const (
	duration int64 = 2 * 365 * 24 * 60 * 60
	stride   int64 = 7 * 24 * 60 * 60
)

// testStrftimePerl checks a range of times and uses the system's strftime(3)
// as a reference for the correct answer. To access the system strftime, it
// uses the core POSIX module of the first perl(1) found in the path. The test
// is only run if the CHECK_AGAINST_PERL environment variable is non-empty.
func testStrftimeAgainstPerl(t *testing.T, impl strftimeImpl, strict bool) {
	if os.Getenv("CHECK_AGAINST_PERL") == "" {
		t.Skip()
		return
	}

	perlPath, err := exec.LookPath("perl")
	if err != nil {
		t.Logf("Couldn't locate perl, skipping: %s", err)
		t.Skip()
	}
	perlOpts := []string{"-MPOSIX=strftime", "-E"}

	origTimezone := os.Getenv("TZ")
	var perlTests []test
	for _, test := range tests {
		if test.perlShouldWork {
			perlTests = append(perlTests, test)
		}
	}
	stmts := make([]string, len(perlTests))

	for n := int64(0); n < duration; n += stride {
		when := refTime + n
		var tm = time.Unix(when, 0).In(tz)
		/*
		   if n%100 == 0 {
		       log.Printf("Testing %d/%d (%.3f%%): %s", n+1, duration, 100*float64(n)/float64(duration), tm)
		   }
		*/

		// batch up all the tests for a single timestamp into one call out to perl
		for i, test := range perlTests {
			stmts[i] = fmt.Sprintf(`say strftime "%s", @t`, test.format)
		}

		prog := fmt.Sprintf("@t=localtime %d; %s", when, strings.Join(stmts, "; "))
		args := append(perlOpts, prog)
		//t.Logf("Executing perl: %s %v", perlPath, args)
		os.Setenv("TZ", timezone)
		output, err := exec.Command(perlPath, args...).CombinedOutput()
		os.Setenv("TZ", origTimezone)
		if err != nil {
			t.Logf("perl failed: %s [%q]", err, output)
			continue
		}

		results := strings.Split(string(output), "\n")
		results = results[:len(results)-1] // strip trailing \n
		if len(results) != len(perlTests) {
			t.Fatalf("t=%d: got %d results from perl, expected %d: %q", when, len(results), len(perlTests), output)
		}

		for i, test := range perlTests {
			got := strftime.Strftime(test.format, tm)
			expected := results[i]
			if got == string(expected) {
				//t.Logf("ok %d/%d: `%s` => `%s`", i, when, test.format, got)
			} else if test.localeDependent && !strict {
				//t.Logf("meh %d/%d: `%s` => got `%s`, perl produced `%s`", i, when, test.format, got, expected)
			} else {
				t.Errorf("FAIL %d/%d: `%s` => got `%s`, perl produced `%s`", i, when, test.format, got, expected)
			}
		}

		if testing.Short() {
			break
		}
	}
}

func BenchmarkStrftime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		strftime.Strftime(benchFmt, benchTime)
	}
}
