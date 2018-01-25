package zipper

import (
	"errors"
	"math"

	pbgrpc "github.com/go-graphite/carbonzipper/carbonzippergrpcpb"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

var ErrResponseLengthMismatch = errors.New("response length mismatch")
var ErrResponseStartTimeMismatch = errors.New("response start time mismatch")
var ErrNotImplementedYet = errors.New("this feature is not implemented yet")
var ErrTimeoutExceeded = errors.New("timeout while fetching response")
var ErrNonFatalErrors = errors.New("response contains non-fatal errors")
var ErrNotFound = errors.New("metric not found")
var ErrNoResponseFetched = errors.New("No responses fetched from upstream")
var ErrNoMetricsFetched = errors.New("No metrics in the response")

var ErrFailedToFetchFmt = "failed to fetch data from server group %v, code %v"

var emptyMsg = &empty.Empty{}

type ServerFetchResponse struct {
	responsesMap map[string][]pbgrpc.FetchResponse
	response     *pbgrpc.MultiFetchResponse
	stats        *Stats
	err          error
}

func swapFetchResponses(m1, m2 *pbgrpc.FetchResponse) {
	m1.Values, m2.Values = m2.Values, m1.Values
	m1.Metadata, m2.Metadata = m2.Metadata, m1.Metadata
	m1.Name, m2.Name = m2.Name, m1.Name
	m1.StartTime, m2.StartTime = m2.StartTime, m1.StartTime
	m1.StopTime, m2.StopTime = m2.StopTime, m1.StopTime
}

func mergeFetchResponses(m1, m2 *pbgrpc.FetchResponse) error {
	logger := zapwriter.Logger("zipper_render")

	if len(m1.Values) != len(m2.Values) {
		interpolate := false
		if len(m1.Values) < len(m2.Values) {
			swapFetchResponses(m1, m2)
		}
		if m1.Metadata.StepTime < m2.Metadata.StepTime {
			interpolate = true

		} else {
			if m1.StartTime == m2.StartTime {
				for i := 0; i < len(m1.Values)-len(m2.Values); i++ {
					m2.Values = append(m2.Values, math.NaN())
				}

				m2.StopTime = m1.StopTime
				goto out
			}
		}

		// TODO(Civil): we must fix the case of m1.StopTime != m2.StopTime
		// We should check if m1.StopTime and m2.StopTime actually the same
		// Also we need to append nans in case StopTimes dramatically differs

		if !interpolate || m1.StopTime-m1.StopTime%m2.Metadata.StepTime != m2.StopTime {
			// m1.Step < m2.Step and len(m1) < len(m2) - most probably garbage data
			logger.Error("unable to merge ovalues",
				zap.Int("metric_values", len(m2.Values)),
				zap.Int("response_values", len(m1.Values)),
			)

			return ErrResponseLengthMismatch
		}

		// len(m1) > len(m2)
		values := make([]float64, 0, len(m1.Values))
		for ts := m1.StartTime; ts < m1.StopTime; ts += m1.Metadata.StepTime {
			idx := (ts - m1.StartTime) / m2.Metadata.StepTime
			values = append(values, m2.Values[idx])
		}
		m2.Values = values
		m2.Metadata.StepTime = m1.Metadata.StepTime
		m2.StartTime = m1.StartTime
		m2.StopTime = m1.StopTime
	}
out:

	if m1.StartTime != m2.StartTime {
		return ErrResponseStartTimeMismatch
	}

	for i := range m1.Values {
		if !math.IsNaN(m1.Values[i]) {
			continue
		}

		// found one
		if !math.IsNaN(m2.Values[i]) {
			m1.Values[i] = m2.Values[i]
		}
	}
	return nil
}

func (r *ServerFetchResponse) Merge(response *ServerFetchResponse) {
	r.stats.Merge(response.stats)

	if response.err != nil {
		return
	}

	metrics := make(map[string]*pbgrpc.FetchResponse)
	for i := range response.response.Metrics {
		metrics[response.response.Metrics[i].Name] = &response.response.Metrics[i]
	}

	for i := range r.response.Metrics {
		if m, ok := metrics[r.response.Metrics[i].Name]; ok {
			err := mergeFetchResponses(&r.response.Metrics[i], m)
			if err != nil {
				// TODO: Normal error handling
				continue
			}
		}
	}

	if r.err != nil && response.err == nil {
		r.err = nil
	}

	return
}

type ServerFindResponse struct {
	response *pbgrpc.MultiGlobResponse
	stats    *Stats
	err      error
}

/*
func mergeFindRequests(f1, f2 []pbgrpc.GlobMatch) []pbgrpc.GlobMatch {
	uniqList := make(map[string]pbgrpc.GlobMatch)

	for _, v := range f1 {
		uniqList[v.Path] = v
	}
	for _, v := range f2 {
		uniqList[v.Path] = v
	}

	res := make([]pbgrpc.GlobMatch, 0, len(uniqList))
	for _, v := range uniqList {
		res = append(res, v)
	}

	return res
}
*/

func (r *ServerFindResponse) Merge(response *ServerFindResponse) {
	r.stats.Merge(response.stats)
	if response.err != nil {
		return
	}

	for k := range response.response.Metrics {
		if _, ok := r.response.Metrics[k]; !ok {
			r.response.Metrics[k] = response.response.Metrics[k]
		}
	}

	if r.err != nil && response.err == nil {
		r.err = nil
	}

	return
}
