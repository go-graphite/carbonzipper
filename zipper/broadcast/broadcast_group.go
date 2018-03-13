package broadcast

import (
	"context"

	"github.com/go-graphite/carbonzipper/limiter"
	"github.com/go-graphite/carbonzipper/pathcache"
	"github.com/go-graphite/carbonzipper/zipper/types"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type BroadcastGroup struct {
	limiter   limiter.ServerLimiter
	groupName string
	timeout   types.Timeouts
	clients   []types.ServerClient
	servers   []string

	pathCache pathcache.PathCache
}

func NewBroadcastGroup(groupName string, servers []types.ServerClient, pathCache pathcache.PathCache, concurencyLimit int, timeout types.Timeouts) (*BroadcastGroup, error) {
	serverNames := make([]string, 0, len(servers))
	for _, s := range servers {
		serverNames = append(serverNames, s.Name())
	}
	logger := zapwriter.Logger("broadCastNewGroup")
	logger.Info("limiter will be created",
		zap.String("name", groupName),
		zap.Strings("servrers", serverNames),
		zap.Int("concurency_limit", concurencyLimit),
	)
	limiter := limiter.NewServerLimiter(serverNames, concurencyLimit)

	return NewBroadcastGroupWithLimiter(groupName, servers, serverNames, pathCache, limiter, timeout)
}

func NewBroadcastGroupWithLimiter(groupName string, servers []types.ServerClient, serverNames []string, pathCache pathcache.PathCache, limiter limiter.ServerLimiter, timeout types.Timeouts) (*BroadcastGroup, error) {
	return &BroadcastGroup{
		timeout:   timeout,
		groupName: groupName,
		clients:   servers,
		limiter:   limiter,
		servers:   serverNames,

		pathCache: pathCache,
	}, nil
}

func (bg BroadcastGroup) Name() string {
	return bg.groupName
}

func (c BroadcastGroup) Backends() []string {
	return c.servers
}

func (bg *BroadcastGroup) Fetch(ctx context.Context, request *protov3.MultiFetchRequest) (*protov3.MultiFetchResponse, *types.Stats, error) {
	resCh := make(chan *types.ServerFetchResponse, len(bg.clients))
	ctx, cancel := context.WithTimeout(ctx, bg.timeout.Render)
	defer cancel()
	for _, client := range bg.clients {
		go func(client types.ServerClient) {
			var r types.ServerFetchResponse
			err := bg.limiter.Enter(ctx, bg.groupName)
			if err != nil {
				return
			}
			r.Response, r.Stats, r.Err = client.Fetch(ctx, request)
			resCh <- &r
			bg.limiter.Leave(ctx, bg.groupName)
		}(client)
	}

	var result types.ServerFetchResponse
	var err error
	for {
		select {
		case r := <-resCh:
			if r.Err != nil {
				err = types.ErrNonFatalErrors
				continue
			}

			if r.Response != nil {
				if result.Response == nil {
					result = *r
				} else {
					result.Merge(r)
				}
			}
		case <-ctx.Done():
			err = types.ErrTimeoutExceeded
			break
		}
	}

	return result.Response, result.Stats, err
}

func (bg *BroadcastGroup) Find(ctx context.Context, request *protov3.MultiGlobRequest) (*protov3.MultiGlobResponse, *types.Stats, error) {
	logger := zapwriter.Logger("broadcastGroup").With(zap.String("groupName", bg.groupName))
	resCh := make(chan *types.ServerFindResponse, len(bg.clients))
	ctx, cancel := context.WithTimeout(ctx, bg.timeout.Find)
	defer cancel()
	for _, client := range bg.clients {
		go func(client types.ServerClient) {
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
			var r types.ServerFindResponse
			r.Response, r.Stats, r.Err = client.Find(ctx, request)
			resCh <- &r
			bg.limiter.Leave(ctx, bg.groupName)
			logger.Debug("got result")
		}(client)
	}

	var result types.ServerFindResponse
	var err error
	responseCounts := 0
GATHER:
	for {
		select {
		case r := <-resCh:
			responseCounts++
			if r.Err == nil {
				if r.Response != nil {
					if result.Response == nil {
						result = *r
					} else {
						result.Merge(r)
					}
				}
			} else {
				err = types.ErrNonFatalErrors
			}

			if responseCounts == len(bg.clients) {
				break GATHER
			}
		case <-ctx.Done():
			err = types.ErrTimeoutExceeded
			break
		}
	}

	return result.Response, result.Stats, err
}

func (bg *BroadcastGroup) Info(ctx context.Context, request *protov3.MultiMetricsInfoRequest) (*protov3.ZipperInfoResponse, *types.Stats, error) {
	return nil, nil, types.ErrNotImplementedYet
}

func (bg *BroadcastGroup) List(ctx context.Context) (*protov3.ListMetricsResponse, *types.Stats, error) {
	return nil, nil, types.ErrNotImplementedYet
}
func (bg *BroadcastGroup) Stats(ctx context.Context) (*protov3.MetricDetailsResponse, *types.Stats, error) {
	return nil, nil, types.ErrNotImplementedYet
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
		go func(client types.ServerClient) {
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
