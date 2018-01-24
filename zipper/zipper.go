package zipper

import (
	"context"
	"math"
	_ "net/http/pprof"
	"time"

	pbgrpc "github.com/go-graphite/carbonzipper/carbonzippergrpcpb"
	pb3 "github.com/go-graphite/carbonzipper/carbonzipperpb3"
	"github.com/go-graphite/carbonzipper/limiter"
	"github.com/go-graphite/carbonzipper/pathcache"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Zipper provides interface to Zipper-related functions
type Zipper struct {
	// Limiter limits our concurrency to a particular server
	limiter     limiter.ServerLimiter
	probeTicker *time.Ticker
	ProbeQuit   chan struct{}
	ProbeForce  chan int

	timeoutAfterAllStarted time.Duration
	timeout                time.Duration
	timeoutConnect         time.Duration
	timeoutKeepAlive       time.Duration
	keepAliveInterval      time.Duration

	searchConfigured bool
	searchBackends   ServerClient
	searchPrefix     string

	searchCache pathcache.PathCache

	// Will broadcast to all servers there
	storeBackends             ServerClient
	concurrencyLimitPerServer int

	sendStats func(*Stats)

	logger *zap.Logger
}

type nameLeaf struct {
	name string
	leaf bool
}

func sanitizeTimouts(timeouts Timeouts) Timeouts {
	if timeouts.Render == 0 {
		timeouts.Render = 10000 * time.Second
	}
	if timeouts.Find == 0 {
		timeouts.Find = 10000 * time.Second
	}
	if timeouts.AfterStarted == 0 {
		timeouts.AfterStarted = 2 * time.Second
	}

	if timeouts.Connect == 0 {
		timeouts.Connect = 200 * time.Millisecond
	}

	return timeouts
}

// NewZipper allows to create new Zipper
func NewZipper(sender func(*Stats), config *Config, logger *zap.Logger) (*Zipper, error) {
	var err error
	prefix := config.CarbonSearch.Prefix
	var searchClientGroup ServerClient

	config.Timeouts = sanitizeTimouts(config.Timeouts)

	if config.CarbonSearch.Backend != "" {
		searchClientGroup, err = NewClientProtobufGroup("search", []string{config.CarbonSearch.Backend}, config.ConcurrencyLimitPerServer, config.MaxIdleConnsPerHost, config.Timeouts, config.KeepAliveInterval)
		if err != nil {
			return nil, err
		}
	} else {
		searchClientGroup, err = NewClientGRPCGroup("search", config.CarbonSearchV2.Backends, config.Timeouts)
		if err != nil {
			return nil, err
		}
		prefix = config.CarbonSearchV2.Prefix
	}
	searchClients := []ServerClient{searchClientGroup}
	// func NewBroadcastGroup(groupName string, servers []ServerClient, pathCache pathcache.PathCache, concurencyLimit int, timeout time.Duration) (*BroadcastGroup, error) {
	searchBackends, err := NewBroadcastGroup("search", searchClients, config.PathCache, config.ConcurrencyLimitPerServer, config.Timeouts)

	storeClients := make([]ServerClient, 0)
	if config.Backends != nil && len(config.Backends) != 0 {
		for _, backend := range config.Backends {
			client, err := NewClientProtobufGroup(backend, []string{backend}, config.ConcurrencyLimitPerServer, config.MaxIdleConnsPerHost, config.Timeouts, config.KeepAliveInterval)
			if err != nil {
				return nil, err
			}
			storeClients = append(storeClients, client)
		}
	} else {
		for _, backend := range config.BackendsV2 {
			concurencyLimit := config.ConcurrencyLimitPerServer
			timeouts := config.Timeouts
			if backend.Timeouts != nil {
				timeouts = *backend.Timeouts
			}
			if backend.ConcurrencyLimit != nil {
				concurencyLimit = *backend.ConcurrencyLimit
			}
			if backend.LBMethod == RoundRobinLB {
				var client ServerClient
				if backend.Protocol == GRPC {
					client, err = NewClientGRPCGroup(backend.GroupName, backend.Servers, timeouts)
					if err != nil {
						return nil, err
					}
				} else {
					client, err = NewClientProtobufGroup(backend.GroupName, backend.Servers, concurencyLimit, config.MaxIdleConnsPerHost, timeouts, config.KeepAliveInterval)
					if err != nil {
						return nil, err
					}
				}
				storeClients = append(storeClients, client)
			} else {
				for _, server := range backend.ServerGroup.Servers {
					var client ServerClient
					if backend.Protocol == GRPC {
						client, err = NewClientGRPCGroup(server, []string{server}, timeouts)
						if err != nil {
							return nil, err
						}
					} else {
						client, err = NewClientProtobufGroup(server, []string{server}, concurencyLimit, config.MaxIdleConnsPerHost, timeouts, config.KeepAliveInterval)
						if err != nil {
							return nil, err
						}
					}

					storeClients = append(storeClients, client)
				}
			}
		}
	}
	storeBackends, err := NewBroadcastGroup("root", storeClients, config.PathCache, config.ConcurrencyLimitPerServer, config.Timeouts)

	z := &Zipper{
		probeTicker: time.NewTicker(10 * time.Minute),
		ProbeQuit:   make(chan struct{}),
		ProbeForce:  make(chan int),

		sendStats: sender,

		searchCache: config.SearchCache,

		storeBackends:             storeBackends,
		searchBackends:            searchBackends,
		searchPrefix:              prefix,
		searchConfigured:          len(prefix) > 0 && len(searchBackends.clients) > 0,
		concurrencyLimitPerServer: config.ConcurrencyLimitPerServer,
		keepAliveInterval:         config.KeepAliveInterval,
		timeoutAfterAllStarted:    config.Timeouts.AfterStarted,
		timeout:                   config.Timeouts.Render,
		timeoutConnect:            config.Timeouts.Connect,
		logger:                    logger,
	}

	logger.Info("zipper config",
		zap.Any("config", config),
	)

	go z.probeTlds()

	z.ProbeForce <- 1
	return z, nil
}

func (z *Zipper) doProbe(logger *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), z.timeout)
	_, err := z.storeBackends.ProbeTLDs(ctx)
	if err != nil {
		logger.Error("failed to probe tlds",
			zap.Error(err),
		)
	}
	cancel()
}

func (z *Zipper) probeTlds() {
	logger := zapwriter.Logger("probe")
	for {
		select {
		case <-z.probeTicker.C:
			z.doProbe(logger)
		case <-z.ProbeForce:
			z.doProbe(logger)
		case <-z.ProbeQuit:
			z.probeTicker.Stop()
			return
		}
	}
}

// GRPC-compatible methods
func (z Zipper) FetchGRPC(ctx context.Context, request *pbgrpc.MultiFetchRequest) (*pbgrpc.MultiFetchResponse, *Stats, error) {
	return z.storeBackends.Fetch(ctx, request)
}

func (z Zipper) FindGRPC(ctx context.Context, request *pbgrpc.MultiGlobRequest) (*pbgrpc.MultiGlobResponse, *Stats, error) {
	return z.storeBackends.Find(ctx, request)
}

func (z Zipper) InfoGRPC(ctx context.Context, request *pbgrpc.MultiMetricsInfoRequest) (*pbgrpc.ZipperInfoResponse, *Stats, error) {
	res, stats, err := z.storeBackends.Info(ctx, request)
	if err != nil {
		return nil, stats, err
	}

	r := &pbgrpc.ZipperInfoResponse{}
	for _, i := range res.Info {
		r.Responses = append(r.Responses, pbgrpc.MetricsInfoResponse{
			Server: res.Server,
			Info:   &i,
		})
	}
	return r, stats, nil
}
func (z Zipper) ListGRPC(ctx context.Context) (*pbgrpc.ListMetricsResponse, *Stats, error) {
	return z.storeBackends.List(ctx)
}
func (z Zipper) StatsGRPC(ctx context.Context) (*pbgrpc.MetricDetailsResponse, *Stats, error) {
	return z.storeBackends.Stats(ctx)
}

// PB3-compatible methods
func (z Zipper) FetchPB(ctx context.Context, query []string, startTime, stopTime int32) (*pb3.MultiFetchResponse, *Stats, error) {
	request := &pbgrpc.MultiFetchRequest{}
	for _, q := range query {
		request.Metrics = append(request.Metrics, pbgrpc.FetchRequest{
			Name:      q,
			StartTime: uint32(startTime),
			StopTime:  uint32(stopTime),
		})
	}

	grpcRes, stats, err := z.FetchGRPC(ctx, request)
	if err != nil {
		return nil, nil, err
	}

	var res pb3.MultiFetchResponse
	for i := range grpcRes.Metrics {
		vals := make([]float64, 0, len(grpcRes.Metrics[i].Values))
		isAbsent := make([]bool, 0, len(grpcRes.Metrics[i].Values))
		for _, v := range grpcRes.Metrics[i].Values {
			if math.IsNaN(v) {
				vals = append(vals, 0)
				isAbsent = append(isAbsent, true)
			} else {
				vals = append(vals, v)
				isAbsent = append(isAbsent, false)
			}
		}
		res.Metrics = append(res.Metrics,
			pb3.FetchResponse{
				Name:      grpcRes.Metrics[i].Name,
				StartTime: int32(grpcRes.Metrics[i].StartTime),
				StopTime:  int32(grpcRes.Metrics[i].StopTime),
				StepTime:  int32(grpcRes.Metrics[i].Metadata.StepTime),
				Values:    vals,
				IsAbsent:  isAbsent,
			})
	}

	return &res, stats, nil
}

func (z Zipper) FindPB(ctx context.Context, query []string) ([]*pb3.GlobResponse, *Stats, error) {
	request := &pbgrpc.MultiGlobRequest{
		Metrics: query,
	}
	grpcReses, stats, err := z.FindGRPC(ctx, request)
	if err != nil {
		return nil, nil, err
	}

	reses := make([]*pb3.GlobResponse, 0, len(grpcReses.Metrics))
	for _, grpcRes := range grpcReses.Metrics {

		res := &pb3.GlobResponse{
			Name: grpcRes.Name,
		}

		for _, v := range grpcRes.Matches {
			match := pb3.GlobMatch{
				Path:   v.Path,
				IsLeaf: v.IsLeaf,
			}
			res.Matches = append(res.Matches, match)
		}
		reses = append(reses, res)
	}

	return reses, stats, nil
}

func (z Zipper) InfoPB(ctx context.Context, targets []string) (*pb3.ZipperInfoResponse, *Stats, error) {
	request := &pbgrpc.MultiMetricsInfoRequest{
		Names: targets,
	}
	grpcRes, stats, err := z.InfoGRPC(ctx, request)
	if err != nil {
		return nil, nil, err
	}

	res := &pb3.ZipperInfoResponse{}

	for _, v := range grpcRes.Responses {
		rets := make([]pb3.Retention, 0, len(v.Info.Retentions))
		for _, ret := range v.Info.Retentions {
			rets = append(rets, pb3.Retention{
				SecondsPerPoint: int32(ret.SecondsPerPoint),
				NumberOfPoints:  int32(ret.NumberOfPoints),
			})
		}
		i := &pb3.InfoResponse{
			Name:              v.Info.Name,
			AggregationMethod: v.Info.AggregationMethod,
			MaxRetention:      int32(v.Info.MaxRetention),
			XFilesFactor:      v.Info.XFilesFactor,
			Retentions:        rets,
		}
		res.Responses = append(res.Responses, pb3.ServerInfoResponse{
			Server: v.Server,
			Info:   i,
		})
	}

	return res, stats, nil
}
func (z Zipper) ListPB(ctx context.Context) (*pb3.ListMetricsResponse, *Stats, error) {
	grpcRes, stats, err := z.ListGRPC(ctx)
	if err != nil {
		return nil, nil, err
	}

	res := &pb3.ListMetricsResponse{
		Metrics: grpcRes.Metrics,
	}
	return res, stats, nil
}
func (z Zipper) StatsPB(ctx context.Context) (*pb3.MetricDetailsResponse, *Stats, error) {
	grpcRes, stats, err := z.StatsGRPC(ctx)
	if err != nil {
		return nil, nil, err
	}

	metrics := make(map[string]*pb3.MetricDetails, len(grpcRes.Metrics))
	for k, v := range grpcRes.Metrics {
		metrics[k] = &pb3.MetricDetails{
			Size_:   v.Size_,
			ModTime: v.ModTime,
			ATime:   v.ATime,
			RdTime:  v.RdTime,
		}
	}

	res := &pb3.MetricDetailsResponse{
		FreeSpace:  grpcRes.FreeSpace,
		TotalSpace: grpcRes.TotalSpace,
		Metrics:    metrics,
	}

	return res, stats, nil
}
