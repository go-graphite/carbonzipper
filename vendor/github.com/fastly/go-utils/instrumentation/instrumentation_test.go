package instrumentation_test

import (
	"testing"

	"github.com/fastly/go-utils/instrumentation"
)

func TestGetSystemStats(t *testing.T) {
	instrumentation.GetSystemStats()
}

func TestGetStackTrace(t *testing.T) {
	instrumentation.GetStackTrace(false)
	instrumentation.GetStackTrace(true)
}

func TestGetStackTraces(t *testing.T) {
	instrumentation.GetStackTraces()
}
