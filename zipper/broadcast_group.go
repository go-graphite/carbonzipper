package zipper

import (
	"context"

	pbgrpc "github.com/go-graphite/carbonzipper/carbonzippergrpcpb"
	"github.com/go-graphite/carbonzipper/limiter"
	"github.com/go-graphite/carbonzipper/pathcache"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type BroadcastGroup struct {
	limiter   limiter.ServerLimiter
	groupName string
	timeout   Timeouts
	clients   []ServerClient

	pathCache pathcache.PathCache
}

func NewBroadcastGroup(groupName string, servers []ServerClient, pathCache pathcache.PathCache, concurencyLimit int, timeout Timeouts) (*BroadcastGroup, error) {
	srvs := make([]string, 0, len(servers))
	for _, s := range servers {
		srvs = append(srvs, s.Name())
	}
	logger := zapwriter.Logger("broadCastNewGroup")
	logger.Info("limiter will be created",
		zap.String("name", groupName),
		zap.Strings("servrers", srvs),
		zap.Int("concurency_limit", concurencyLimit),
	)
	limiter := limiter.NewServerLimiter(srvs, concurencyLimit)

	return NewBroadcastGroupWithLimiter(groupName, servers, pathCache, limiter, timeout)
}

func NewBroadcastGroupWithLimiter(groupName string, servers []ServerClient, pathCache pathcache.PathCache, limiter limiter.ServerLimiter, timeout Timeouts) (*BroadcastGroup, error) {
	return &BroadcastGroup{
		timeout:   timeout,
		groupName: groupName,
		clients:   servers,
		limiter:   limiter,

		pathCache: pathCache,
	}, nil
}

func (bg BroadcastGroup) Name() string {
	return bg.groupName
}

func (bg *BroadcastGroup) Fetch(ctx context.Context, request *pbgrpc.MultiFetchRequest) (*pbgrpc.MultiFetchResponse, *Stats, error) {
	resCh := make(chan *ServerFetchResponse, len(bg.clients))
	ctx, cancel := context.WithTimeout(ctx, bg.timeout.Render)
	defer cancel()
	for _, client := range bg.clients {
		go func(client ServerClient) {
			var r ServerFetchResponse
			err := bg.limiter.Enter(ctx, bg.groupName)
			if err != nil {
				return
			}
			r.response, r.stats, r.err = client.Fetch(ctx, request)
			resCh <- &r
			bg.limiter.Leave(ctx, bg.groupName)
		}(client)
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

func (bg *BroadcastGroup) Find(ctx context.Context, request *pbgrpc.MultiGlobRequest) (*pbgrpc.MultiGlobResponse, *Stats, error) {
	logger := zapwriter.Logger("broadcastGroup").With(zap.String("groupName", bg.groupName))
	resCh := make(chan *ServerFindResponse, len(bg.clients))
	ctx, cancel := context.WithTimeout(ctx, bg.timeout.Find)
	defer cancel()
	for _, client := range bg.clients {
		go func(client ServerClient) {
			logger.Debug("waiting for a slot",
				zap.String("group_name", bg.groupName),
				zap.String("client_name", client.Name()),
			)
			err := bg.limiter.Enter(ctx, bg.groupName)
			if err != nil {
				logger.Debug("timeout waiting for a slot")
				return
			}

			logger.Debug("got a slot")
			var r ServerFindResponse
			r.response, r.stats, r.err = client.Find(ctx, request)
			resCh <- &r
			bg.limiter.Leave(ctx, bg.groupName)
			logger.Debug("got result")
		}(client)
	}

	var result ServerFindResponse
	var err error
	responseCounts := 0
GATHER:
	for {
		select {
		case r := <-resCh:
			responseCounts++
			if r.err == nil {
				if r.response != nil {
					if result.response == nil {
						result = *r
					} else {
						result.Merge(r)
					}
				}
			} else {
				err = ErrNonFatalErrors
			}

			if responseCounts == len(bg.clients) {
				break GATHER
			}
		case <-ctx.Done():
			err = ErrTimeoutExceeded
			break
		}
	}

	return result.response, result.stats, err
}

func (bg *BroadcastGroup) Info(ctx context.Context, request *pbgrpc.MultiMetricsInfoRequest) (*pbgrpc.MultiMetricsInfoResponse, *Stats, error) {
	return nil, nil, ErrNotImplementedYet
}

func (bg *BroadcastGroup) List(ctx context.Context) (*pbgrpc.ListMetricsResponse, *Stats, error) {
	return nil, nil, ErrNotImplementedYet
}
func (bg *BroadcastGroup) Stats(ctx context.Context) (*pbgrpc.MetricDetailsResponse, *Stats, error) {
	return nil, nil, ErrNotImplementedYet
}

type tldResponse struct {
	server string
	tlds   []string
}

func (bg *BroadcastGroup) ProbeTLDs(ctx context.Context) ([]string, error) {
	logger := zapwriter.Logger("probe").With(zap.String("groupName", bg.groupName))
	var tlds []string
	cache := make(map[string][]string)
	resCh := make(chan *tldResponse, len(bg.clients))
	ctx, cancel := context.WithTimeout(ctx, bg.timeout.Find)

	for _, client := range bg.clients {
		go func(client ServerClient) {
			logger.Debug("probeTLD",
				zap.String("name", client.Name()),
			)

			res, err := client.ProbeTLDs(ctx)
			if err != nil {
				logger.Error("failed to probe tld",
					zap.String("name", client.Name()),
					zap.Error(err),
				)
				return
			}

			resCh <- &tldResponse{
				server: client.Name(),
				tlds:   res,
			}
		}(client)
	}

	counter := 0
	select {
	case r := <-resCh:
		counter++
		tlds = append(tlds, r.tlds...)
		for _, tld := range r.tlds {
			cache[tld] = append(cache[tld], r.server)
		}

		if counter == len(bg.clients) {
			cancel()
			break
		}
	case <-ctx.Done():
		cancel()
		break
	}

	for k, v := range cache {
		bg.pathCache.Set(k, v)
		logger.Debug("TLD Probe",
			zap.String("path", k),
			zap.Strings("servers", v),
		)
	}

	return tlds, nil
}
