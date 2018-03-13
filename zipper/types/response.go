package types

import (
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"math"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type ServerResponse struct {
	Server   string
	Response []byte
}

type ServerFindResponse struct {
	Response *protov3.MultiGlobResponse
	Stats    *Stats
	Err      error
}

/*
func mergeFindRequests(f1, f2 []protov3.GlobMatch) []protov3.GlobMatch {
	uniqList := make(map[string]protov3.GlobMatch)

	for _, v := range f1 {
		uniqList[v.Path] = v
	}
	for _, v := range f2 {
		uniqList[v.Path] = v
	}

	res := make([]protov3.GlobMatch, 0, len(uniqList))
	for _, v := range uniqList {
		res = append(res, v)
	}

	return res
}
*/

func (r *ServerFindResponse) Merge(response *ServerFindResponse) {
	r.Stats.Merge(response.Stats)
	if response.Err != nil {
		return
	}

	seen := make(map[string]struct{}, len(r.Response.Metrics))
	for i := range r.Response.Metrics {
		seen[r.Response.Metrics[i].Name] = struct{}{}
	}

	for i := range response.Response.Metrics {
		if _, ok := seen[r.Response.Metrics[i].Name]; !ok {
			r.Response.Metrics = append(r.Response.Metrics, response.Response.Metrics[i])
			seen[r.Response.Metrics[i].Name] = struct{}{}
		}
	}

	if r.Err != nil && response.Err == nil {
		r.Err = nil
	}

	return
}

type ServerFetchResponse struct {
	ResponsesMap map[string][]protov3.FetchResponse
	Response     *protov3.MultiFetchResponse
	Stats        *Stats
	Err          error
}

func swapFetchResponses(m1, m2 *protov3.FetchResponse) {
	m1.Name, m2.Name = m2.Name, m1.Name
	m1.StartTime, m2.StartTime = m2.StartTime, m1.StartTime
	m1.StepTime, m2.StepTime = m2.StepTime, m1.StepTime
	m1.ConsolidationFunc, m2.ConsolidationFunc = m2.ConsolidationFunc, m1.ConsolidationFunc
	m1.XFilesFactor, m2.XFilesFactor = m2.XFilesFactor, m1.XFilesFactor
	m1.Values, m2.Values = m2.Values, m1.Values
	m1.AppliedFunctions, m2.AppliedFunctions = m2.AppliedFunctions, m1.AppliedFunctions
	m1.StopTime, m2.StopTime = m2.StopTime, m1.StopTime
}

func mergeFetchResponses(m1, m2 *protov3.FetchResponse) error {
	logger := zapwriter.Logger("zipper_render")

	if len(m1.Values) != len(m2.Values) {
		interpolate := false
		if len(m1.Values) < len(m2.Values) {
			swapFetchResponses(m1, m2)
		}
		if m1.StepTime < m2.StepTime {
			interpolate = true

		} else {
			if m1.StartTime == m2.StartTime {
				for i := 0; i < len(m1.Values)-len(m2.Values); i++ {
					m2.Values = append(m2.Values, math.NaN())
				}

				goto out
			}
		}

		// TODO(Civil): we must fix the case of m1.StopTime != m2.StopTime
		// We should check if m1.StopTime and m2.StopTime actually the same
		// Also we need to append nans in case StopTimes dramatically differs

		if !interpolate || m1.StopTime-m1.StopTime%m2.StepTime != m2.StopTime {
			// m1.Step < m2.Step and len(m1) < len(m2) - most probably garbage data
			logger.Error("unable to merge ovalues",
				zap.Int("metric_values", len(m2.Values)),
				zap.Int("response_values", len(m1.Values)),
			)

			return ErrResponseLengthMismatch
		}

		// len(m1) > len(m2)
		values := make([]float64, 0, len(m1.Values))
		for ts := m1.StartTime; ts < m1.StopTime; ts += m1.StepTime {
			idx := (ts - m1.StartTime) / m2.StepTime
			values = append(values, m2.Values[idx])
		}
		m2.Values = values
		m2.StepTime = m1.StepTime
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
	r.Stats.Merge(response.Stats)

	if response.Err != nil {
		return
	}

	metrics := make(map[string]*protov3.FetchResponse)
	for i := range response.Response.Metrics {
		metrics[response.Response.Metrics[i].Name] = &response.Response.Metrics[i]
	}

	for i := range r.Response.Metrics {
		if m, ok := metrics[r.Response.Metrics[i].Name]; ok {
			err := mergeFetchResponses(&r.Response.Metrics[i], m)
			if err != nil {
				// TODO: Normal error handling
				continue
			}
		}
	}

	if r.Err != nil && response.Err == nil {
		r.Err = nil
	}

	return
}
