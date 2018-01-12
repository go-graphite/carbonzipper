package strftime_test

import (
	"testing"

	"time"

	"github.com/fastly/go-utils/strftime"
)

const benchFmt = "%a|%A|%b|%B|%c|%C|%d|%D|%e|%E|%EF|%F|%G|%g|%h|%H|%I|%j|%k|%l|%m|%M|%O|%OF|%p|%P|%r|%R|%s|%S|%t|%T|%u|%U|%V|%w|%W|%x|%X|%y|%Y|%z|%Z|%%|%^|%"

var benchTime = time.Date(2014, time.July, 2, 11, 57, 42, 234098432, time.UTC)

func BenchmarkStrftimePure(b *testing.B) {
	for i := 0; i < b.N; i++ {
		strftime.StrftimePure(benchFmt, benchTime)
	}
}
