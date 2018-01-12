package strftime_test

import (
	"testing"

	"fmt"
	"os"
	"time"

	"github.com/fastly/go-utils/strftime"
)

const (
	refTime  int64 = 1136239445 // see package time
	timezone       = "MST"
)

var tz *time.Location

func init() {
	var err error
	tz, err = time.LoadLocation(timezone)
	if err != nil {
		panic(fmt.Sprintf("couldn't load timezone %q: %s", timezone, err))
	}
	// try to get consistent answers by using a relatively well-defined locale
	os.Setenv("LC_TIME", "C")
}

type test struct {
	format, expected                string
	localeDependent, perlShouldWork bool
}

var tests = []test{
	{"", ``, false, true},

	// invalid formats
	{"%", `%`, false, true},
	{"%^", `%^`, false, true},
	{"%^ ", `%^ `, false, true},
	{"%^ x", `%^ x`, false, true},
	{"x%^ x", `x%^ x`, false, true},

	// supported locale-invariant formats
	{"%a", `Mon`, false, true},
	{"%A", `Monday`, false, true},
	{"%b", `Jan`, false, true},
	{"%B", `January`, false, true},
	{"%C", `20`, false, true},
	{"%d", `02`, false, true},
	{"%D", `01/02/06`, false, true},
	{"%e", ` 2`, false, true},
	{"%F", `2006-01-02`, false, true},
	{"%G", `2006`, false, true},
	{"%g", `06`, false, true},
	{"%h", `Jan`, false, true},
	{"%H", `15`, false, true},
	{"%I", `03`, false, true},
	{"%j", `002`, false, true},
	{"%k", `15`, false, true},
	{"%l", ` 3`, false, true},
	{"%m", `01`, false, true},
	{"%M", `04`, false, true},
	{"%n", "\n", false, false},
	{"%p", `PM`, false, true},
	{"%r", `03:04:05 PM`, false, true},
	{"%R", `15:04`, false, true},
	{"%s", `1136239445`, false, true},
	{"%S", `05`, false, true},
	{"%t", "\t", false, true},
	{"%T", `15:04:05`, false, true},
	{"%u", `1`, false, true},
	{"%U", `01`, false, true},
	{"%V", `01`, false, true},
	{"%w", `1`, false, true},
	{"%W", `01`, false, true},
	{"%x", `01/02/06`, true, true},
	{"%X", `15:04:05`, true, true},
	{"%y", `06`, false, true},
	{"%Y", `2006`, false, true},
	{"%z", `-0700`, false, true},
	{"%Z", `MST`, false, true},
	{"%%", `%`, false, true},

	// supported locale-varying formats
	{"%c", `Mon Jan  2 15:04:05 2006`, true, true},
	{"%E", `%E`, true, true},
	{"%EF", `%EF`, true, true},
	{"%O", `%O`, true, true},
	{"%OF", `%OF`, true, true},
	{"%P", `pm`, true, true},
	{"%+", `Mon Jan  2 15:04:05 MST 2006`, true, false},
	{
		"%a|%A|%b|%B|%c|%C|%d|%D|%e|%E|%EF|%F|%G|%g|%h|%H|%I|%j|%k|%l|%m|%M|%O|%OF|%p|%P|%r|%R|%s|%S|%t|%T|%u|%U|%V|%w|%W|%x|%X|%y|%Y|%z|%Z|%%",
		`Mon|Monday|Jan|January|Mon Jan  2 15:04:05 2006|20|02|01/02/06| 2|%E|%EF|2006-01-02|2006|06|Jan|15|03|002|15| 3|01|04|%O|%OF|PM|pm|03:04:05 PM|15:04|1136239445|05|	|15:04:05|1|01|01|1|01|01/02/06|15:04:05|06|2006|-0700|MST|%`,
		true, true,
	},
}

type strftimeImpl func(string, time.Time) string

func TestStrftimeAgainstReference(t *testing.T) {
	testStrftimeAgainstReference(t, strftime.Strftime, false)
}

func testStrftimeAgainstReference(t *testing.T, impl strftimeImpl, strict bool) {
	var tm = time.Unix(refTime, 0).In(tz)
	for i, test := range tests {
		got := impl(test.format, tm)
		if got == test.expected {
			t.Logf("ok %d: `%s` => `%s`", i, test.format, got)
		} else if test.localeDependent && !strict {
			// locale-reliant conversions are generally unpredictable, so
			// failures are expected
			t.Logf("meh %d: `%s` => got `%s`, expected `%s`", i, test.format, got, test.expected)
		} else {
			t.Errorf("FAIL %d: `%s` => got `%s`, expected `%s`", i, test.format, got, test.expected)
		}
	}
}
