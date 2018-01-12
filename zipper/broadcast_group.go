package zipper

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	pbgrpc "github.com/go-graphite/carbonzipper/carbonzippergrpcpb"
	"github.com/go-graphite/carbonzipper/limiter"
	"github.com/go-graphite/carbonzipper/pathcache"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type BroadcastGroup struct {
	limiter   limiter.ServerLimiter
	groupName string
	timeout   time.Duration
	clients   []ServerClient

	pathCache pathcache.PathCache
}

func (bg BroadcastGroup) Name() string {
	return bg.groupName
}

type ServerFetchResponse struct {
	responsesMap map[string][]pbgrpc.FetchResponse
	response     *pbgrpc.MultiFetchResponse
	stats        *Stats
	err          error
}

var ErrResponseLengthMismatch = errors.New("response length mismatch")
var ErrResponseStartTimeMismatch = errors.New("response start time mismatch")

func swapFetchResponses(m1, m2 *pbgrpc.FetchResponse) {
	m1.Values, m2.Values = m2.Values, m1.Values
	m1.Metadata, m2.Metadata = m2.Metadata, m1.Metadata
	m1.Name, m2.Name = m2.Name, m1.Name
	m1.StartTime, m2.StartTime = m2.StartTime, m1.StartTime
	m1.StopTime, m2.StopTime = m2.StopTime, m1.StopTime
}

func mergeValues(m1, m2 *pbgrpc.FetchResponse) error {
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

	metrics := make(map[string]*pbgrpc.FetchResponse)
	for i := range response.response.Metrics {
		metrics[response.response.Metrics[i].Name] = &response.response.Metrics[i]
	}

	for i := range r.response.Metrics {
		if m, ok := metrics[r.response.Metrics[i].Name]; ok {
			err := mergeValues(&r.response.Metrics[i], m)
			if err != nil {
				// TODO: Normal error handling
				continue
			}
		}
	}

	return
}

func (bg BroadcastGroup) Fetch(ctx context.Context, request *pbgrpc.MultiFetchRequest) (*pbgrpc.MultiFetchResponse, *Stats, error) {
	var wg sync.WaitGroup
	resCh := make(chan *ServerFetchResponse, len(bg.clients))
	ctx, cancel := context.WithTimeout(ctx, bg.timeout)
	defer cancel()
	for _, client := range bg.clients {
		wg.Add(1)
		bg.limiter.Enter(bg.groupName)
		go func() {
			var r ServerFetchResponse
			r.response, r.stats, r.err = client.Fetch(ctx, request)
			resCh <- &r
			bg.limiter.Leave(bg.groupName)
			wg.Done()
		}()
	}
	wg.Wait()

	var result ServerFetchResponse
	var err error
	for {
		select {
		case r := <-resCh:
			if r.err != nil {
				err = ErrNonFatalErrors
				continue
			}

			if r.response != nil {
				if result.response == nil {
					result = *r
				} else {
					result.Merge(r)
				}
			}
		case <-ctx.Done():
			err = ErrTimeoutExceeded
			break
		}
	}

	return result.response, result.stats, err
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

	for k := range response.response.Metrics {
		if _, ok := r.response.Metrics[k]; !ok {
			r.response.Metrics[k] = response.response.Metrics[k]
		}
	}

	return
}

func (bg BroadcastGroup) Find(ctx context.Context, request *pbgrpc.MultiGlobRequest) (*pbgrpc.MultiGlobResponse, *Stats, error) {
	var wg sync.WaitGroup
	resCh := make(chan *ServerFindResponse, len(bg.clients))
	ctx, cancel := context.WithTimeout(ctx, bg.timeout)
	defer cancel()
	for _, client := range bg.clients {
		wg.Add(1)
		bg.limiter.Enter(bg.groupName)
		go func() {
			var r ServerFindResponse
			r.response, r.stats, r.err = client.Find(ctx, request)
			resCh <- &r
			bg.limiter.Leave(bg.groupName)
			wg.Done()
		}()
	}
	wg.Wait()

	var result ServerFindResponse
	var err error
	for {
		select {
		case r := <-resCh:
			if r.err != nil {
				err = ErrNonFatalErrors
				continue
			}

			if r.response != nil {
				if result.response == nil {
					result = *r
				} else {
					result.Merge(r)
				}
			}
		case <-ctx.Done():
			err = ErrTimeoutExceeded
			break
		}
	}

	return result.response, result.stats, err
}
func (bg BroadcastGroup) Info(ctx context.Context, request *pbgrpc.MultiMetricsInfoRequest) (*pbgrpc.MultiMetricsInfoResponse, *Stats, error) {
	return nil, nil, nil
}

func (bg BroadcastGroup) List(ctx context.Context, servers []string) (*pbgrpc.ListMetricsResponse, *Stats, error) {
	return nil, nil, nil
}
func (bg BroadcastGroup) Stats(ctx context.Context, servers []string) (*pbgrpc.MetricDetailsResponse, *Stats, error) {
	return nil, nil, nil
}

func (c BroadcastGroup) ProbeTLDs(ctx context.Context) ([]string, error) {
	logger := zapwriter.Logger("probe")
	var tlds []string
	var cache map[string][]string
	for _, client := range c.clients {
		res, err := client.ProbeTLDs(ctx)
		if err != nil {
			logger.Error("failed to probe tld",
				zap.Error(err),
			)
			continue
		}
		tlds = append(tlds, res...)
		for _, tld := range res {
			cache[tld] = append(cache[tld], client.Name())
		}
	}

	for k, v := range cache {
		c.pathCache.Set(k, v)
		logger.Debug("TLD Probe",
			zap.String("path", k),
			zap.Strings("servers", v),
		)
	}

	return tlds, nil
}
