package zipper

import (
	"context"
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

func (bg BroadcastGroup) Fetch(ctx context.Context, request *pbgrpc.MultiFetchRequest) (*pbgrpc.MultiFetchResponse, *Stats, error) {
	resCh := make(chan *ServerFetchResponse, len(bg.clients))
	ctx, cancel := context.WithTimeout(ctx, bg.timeout)
	defer cancel()
	for _, client := range bg.clients {
		go func() {
			var r ServerFetchResponse
			bg.limiter.Enter(bg.groupName)
			r.response, r.stats, r.err = client.Fetch(ctx, request)
			resCh <- &r
			bg.limiter.Leave(bg.groupName)
		}()
	}

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
	return nil, nil, ErrNotImplementedYet
}

func (bg BroadcastGroup) List(ctx context.Context) (*pbgrpc.ListMetricsResponse, *Stats, error) {
	return nil, nil, ErrNotImplementedYet
}
func (bg BroadcastGroup) Stats(ctx context.Context) (*pbgrpc.MetricDetailsResponse, *Stats, error) {
	return nil, nil, ErrNotImplementedYet
}

func (c BroadcastGroup) ProbeTLDs(ctx context.Context) ([]string, error) {
	logger := zapwriter.Logger("probe")
	var tlds []string
	var cache map[string][]string
	for _, client := range c.clients {
		logger.Debug("probeTLD",
			zap.Any("client", client),
			zap.Any("clients", c.clients),
		)
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
