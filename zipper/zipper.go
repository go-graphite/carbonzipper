package zipper

import (
	"context"
	"math"
	_ "net/http/pprof"
	"time"

	"github.com/go-graphite/carbonzipper/limiter"
	"github.com/go-graphite/carbonzipper/pathcache"
	"github.com/go-graphite/carbonzipper/zipper/broadcast"
	"github.com/go-graphite/carbonzipper/zipper/metadata"
	"github.com/go-graphite/carbonzipper/zipper/types"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/pkg/errors"

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

	timeout           time.Duration
	timeoutConnect    time.Duration
	timeoutKeepAlive  time.Duration
	keepAliveInterval time.Duration

	searchConfigured bool
	searchBackends   types.ServerClient
	searchPrefix     string

	searchCache pathcache.PathCache

	// Will broadcast to all servers there
	storeBackends             types.ServerClient
	concurrencyLimitPerServer int

	sendStats func(*types.Stats)

	logger *zap.Logger
}

type nameLeaf struct {
	name string
	leaf bool
}

func sanitizeTimouts(timeouts types.Timeouts) types.Timeouts {
	if timeouts.Render == 0 {
		timeouts.Render = 10000 * time.Second
	}
	if timeouts.Find == 0 {
		timeouts.Find = 10000 * time.Second
	}

	if timeouts.Connect == 0 {
		timeouts.Connect = 200 * time.Millisecond
	}

	return timeouts
}

func createBackendsV2(logger *zap.Logger, backends types.BackendsV2, pathCache pathcache.PathCache) ([]types.ServerClient, error) {
	storeClients := make([]types.ServerClient, 0)
	var err error
	for _, backend := range backends.Backends {
		concurencyLimit := backends.ConcurrencyLimitPerServer
		timeouts := backends.Timeouts
		tries := backends.MaxTries
		maxIdleConnsPerHost := backends.MaxIdleConnsPerHost
		keepAliveInterval := backends.KeepAliveInterval

		if backend.Timeouts != nil {
			timeouts = *backend.Timeouts
		}
		if backend.ConcurrencyLimit != nil {
			concurencyLimit = *backend.ConcurrencyLimit
		}
		if backend.MaxTries == nil {
			backend.MaxTries = &tries
		}
		if backend.MaxIdleConnsPerHost == nil {
			backend.MaxIdleConnsPerHost = &maxIdleConnsPerHost
		}
		if backend.KeepAliveInterval == nil {
			backend.KeepAliveInterval = &keepAliveInterval
		}

		var client types.ServerClient
		logger.Debug("creating lb group",
			zap.String("name", backend.GroupName),
			zap.Strings("servers", backend.Servers),
			zap.Any("type", backend.LBMethod),
		)
		if backend.LBMethod == types.RoundRobinLB {
			metadata.Metadata.RLock()
			backendInit, ok := metadata.Metadata.ProtocolInits[backend.Protocol]
			metadata.Metadata.RUnlock()
			if !ok {
				var protocols []string
				metadata.Metadata.RLock()
				for p := range metadata.Metadata.SupportedProtocols {
					protocols = append(protocols, p)
				}
				metadata.Metadata.RUnlock()
				logger.Error("unknown backend protocol",
					zap.Any("backend", backend),
					zap.Strings("supported_backends", protocols),
				)
				return nil, errors.New("unknown backend protocol")
			}
			client, err = backendInit(backend)
			if err != nil {
				return nil, err
			}
		} else {
			config := backend
			metadata.Metadata.RLock()
			backendInit, ok := metadata.Metadata.ProtocolInits[backend.Protocol]
			metadata.Metadata.RUnlock()
			if !ok {
				var protocols []string
				metadata.Metadata.RLock()
				for p := range metadata.Metadata.SupportedProtocols {
					protocols = append(protocols, p)
				}
				metadata.Metadata.RUnlock()
				logger.Error("unknown backend protocol",
					zap.Any("backend", backend),
					zap.Strings("supported_protocols", protocols),
				)
				return nil, errors.New("unknown backend protocol")
			}
			backends := make([]types.ServerClient, 0, len(backend.Servers))
			for _, server := range backend.Servers {
				config.Servers = []string{server}
				client, err = backendInit(backend)
				backends = append(backends, client)
			}

			client, err = broadcast.NewBroadcastGroup(backend.GroupName, backends, pathCache, concurencyLimit, timeouts)
			if err != nil {
				return nil, err
			}
		}
		storeClients = append(storeClients, client)
	}
	return storeClients, nil
}

/*
type BackendsV2 struct {
	Backends                  []BackendV2   `yaml:"backends"`
	MaxIdleConnsPerHost       int           `yaml:"maxIdleConnsPerHost"`
	ConcurrencyLimitPerServer int           `yaml:"concurrencyLimit"`
	Timeouts                  Timeouts      `yaml:"timeouts"`
	KeepAliveInterval         time.Duration `yaml:"keepAliveInterval"`
	MaxTries                  int           `yaml:"maxTries"`
	MaxGlobs                  int           `yaml:"maxGlobs"`
}

type BackendV2 struct {
	GroupName           string         `yaml:"groupName"`
	Protocol            string         `yaml:"backendProtocol"`
	LBMethod            LBMethod       `yaml:"lbMethod"` // Valid: rr/roundrobin, broadcast/all
	Servers             []string       `yaml:"servers"`
	Timeouts            *Timeouts      `yaml:"timeouts"`
	ConcurrencyLimit    *int           `yaml:"concurrencyLimit"`
	KeepAliveInterval   *time.Duration `yaml:"keepAliveInterval"`
	MaxIdleConnsPerHost *int           `yaml:"maxIdleConnsPerHost"`
	MaxTries            *int           `yaml:"maxTries"`
	MaxGlobs            int            `yaml:"maxGlobs"`
}
*/

// NewZipper allows to create new Zipper
func NewZipper(sender func(*types.Stats), config *types.Config, logger *zap.Logger) (*Zipper, error) {
	var err error
	config.Timeouts = sanitizeTimouts(config.Timeouts)

	var searchClients []types.ServerClient
	// Convert old config format to new one
	if config.CarbonSearch.Backend != "" {
		config.CarbonSearchV2.BackendsV2 = types.BackendsV2{
			Backends: []types.BackendV2{{
				GroupName:           config.CarbonSearch.Backend,
				Protocol:            "carbonapiv2",
				LBMethod:            types.RoundRobinLB,
				Servers:             []string{config.CarbonSearch.Backend},
				Timeouts:            &config.Timeouts,
				ConcurrencyLimit:    &config.ConcurrencyLimitPerServer,
				KeepAliveInterval:   &config.KeepAliveInterval,
				MaxIdleConnsPerHost: &config.MaxIdleConnsPerHost,
				MaxTries:            &config.MaxTries,
				MaxGlobs:            config.MaxGlobs,
			}},
			MaxIdleConnsPerHost:       config.MaxIdleConnsPerHost,
			ConcurrencyLimitPerServer: config.ConcurrencyLimitPerServer,
			Timeouts:                  config.Timeouts,
			KeepAliveInterval:         config.KeepAliveInterval,
			MaxTries:                  config.MaxTries,
			MaxGlobs:                  config.MaxGlobs,
		}
		config.CarbonSearchV2.Prefix = config.CarbonSearch.Prefix
	}

	searchClients, err = createBackendsV2(logger, config.CarbonSearchV2.BackendsV2, config.PathCache)
	if err != nil {
		return nil, err
	}
	prefix := config.CarbonSearchV2.Prefix

	searchBackends, err := broadcast.NewBroadcastGroup("search", searchClients, config.PathCache, config.ConcurrencyLimitPerServer, config.Timeouts)
	if err != nil {
		return nil, err
	}

	storeClients := make([]types.ServerClient, 0)
	// Convert old config format to new one
	if config.Backends != nil && len(config.Backends) != 0 {
		config.BackendsV2 = types.BackendsV2{
			Backends:                  make([]types.BackendV2, 0, len(config.Backends)),
			MaxIdleConnsPerHost:       config.MaxIdleConnsPerHost,
			ConcurrencyLimitPerServer: config.ConcurrencyLimitPerServer,
			Timeouts:                  config.Timeouts,
			KeepAliveInterval:         config.KeepAliveInterval,
			MaxTries:                  config.MaxTries,
			MaxGlobs:                  config.MaxGlobs,
		}
		for _, backend := range config.Backends {
			backend := types.BackendV2{
				GroupName:           config.CarbonSearch.Backend,
				Protocol:            "carbonapiv2",
				LBMethod:            types.RoundRobinLB,
				Servers:             []string{backend},
				Timeouts:            &config.Timeouts,
				ConcurrencyLimit:    &config.ConcurrencyLimitPerServer,
				KeepAliveInterval:   &config.KeepAliveInterval,
				MaxIdleConnsPerHost: &config.MaxIdleConnsPerHost,
				MaxTries:            &config.MaxTries,
				MaxGlobs:            config.MaxGlobs,
			}
			config.BackendsV2.Backends = append(config.BackendsV2.Backends, backend)
		}
	}

	storeClients, err = createBackendsV2(logger, config.BackendsV2, config.PathCache)
	storeBackends, err := broadcast.NewBroadcastGroup("root", storeClients, config.PathCache, config.ConcurrencyLimitPerServer, config.Timeouts)

	z := &Zipper{
		probeTicker: time.NewTicker(10 * time.Minute),
		ProbeQuit:   make(chan struct{}),
		ProbeForce:  make(chan int),

		sendStats: sender,

		searchCache: config.SearchCache,

		storeBackends:             storeBackends,
		searchBackends:            searchBackends,
		searchPrefix:              prefix,
		searchConfigured:          len(prefix) > 0 && len(searchBackends.Backends()) > 0,
		concurrencyLimitPerServer: config.ConcurrencyLimitPerServer,
		keepAliveInterval:         config.KeepAliveInterval,
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
func (z Zipper) FetchGRPC(ctx context.Context, request *protov3.MultiFetchRequest) (*protov3.MultiFetchResponse, *types.Stats, error) {
	return z.storeBackends.Fetch(ctx, request)
}

func (z Zipper) FindGRPC(ctx context.Context, request *protov3.MultiGlobRequest) (*protov3.MultiGlobResponse, *types.Stats, error) {
	return z.storeBackends.Find(ctx, request)
}

func (z Zipper) InfoGRPC(ctx context.Context, request *protov3.MultiMetricsInfoRequest) (*protov3.ZipperInfoResponse, *types.Stats, error) {
	return z.storeBackends.Info(ctx, request)
}

func (z Zipper) ListGRPC(ctx context.Context) (*protov3.ListMetricsResponse, *types.Stats, error) {
	return z.storeBackends.List(ctx)
}
func (z Zipper) StatsGRPC(ctx context.Context) (*protov3.MetricDetailsResponse, *types.Stats, error) {
	return z.storeBackends.Stats(ctx)
}

// PB3-compatible methods
func (z Zipper) FetchPB(ctx context.Context, query []string, startTime, stopTime int32) (*protov2.MultiFetchResponse, *types.Stats, error) {
	request := &protov3.MultiFetchRequest{}
	for _, q := range query {
		request.Metrics = append(request.Metrics, protov3.FetchRequest{
			Name:      q,
			StartTime: uint32(startTime),
			StopTime:  uint32(stopTime),
		})
	}

	grpcRes, stats, err := z.FetchGRPC(ctx, request)
	if err != nil {
		return nil, nil, err
	}

	var res protov2.MultiFetchResponse
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
			protov2.FetchResponse{
				Name:      grpcRes.Metrics[i].Name,
				StartTime: int32(grpcRes.Metrics[i].StartTime),
				StopTime:  int32(grpcRes.Metrics[i].StopTime),
				StepTime:  int32(grpcRes.Metrics[i].StepTime),
				Values:    vals,
				IsAbsent:  isAbsent,
			})
	}

	return &res, stats, nil
}

func (z Zipper) FindPB(ctx context.Context, query []string) ([]*protov2.GlobResponse, *types.Stats, error) {
	request := &protov3.MultiGlobRequest{
		Metrics: query,
	}
	grpcReses, stats, err := z.FindGRPC(ctx, request)
	if err != nil {
		return nil, nil, err
	}

	reses := make([]*protov2.GlobResponse, 0, len(grpcReses.Metrics))
	for _, grpcRes := range grpcReses.Metrics {

		res := &protov2.GlobResponse{
			Name: grpcRes.Name,
		}

		for _, v := range grpcRes.Matches {
			match := protov2.GlobMatch{
				Path:   v.Path,
				IsLeaf: v.IsLeaf,
			}
			res.Matches = append(res.Matches, match)
		}
		reses = append(reses, res)
	}

	return reses, stats, nil
}

func (z Zipper) InfoPB(ctx context.Context, targets []string) (*protov2.ZipperInfoResponse, *types.Stats, error) {
	request := &protov3.MultiMetricsInfoRequest{
		Names: targets,
	}
	grpcRes, stats, err := z.InfoGRPC(ctx, request)
	if err != nil {
		return nil, nil, err
	}

	res := &protov2.ZipperInfoResponse{}

	for k, i := range grpcRes.Info {
		for _, v := range i.Metrics {
			rets := make([]protov2.Retention, 0, len(v.Retentions))
			for _, ret := range v.Retentions {
				rets = append(rets, protov2.Retention{
					SecondsPerPoint: int32(ret.SecondsPerPoint),
					NumberOfPoints:  int32(ret.NumberOfPoints),
				})
			}
			i := &protov2.InfoResponse{
				Name:              v.Name,
				AggregationMethod: v.ConsolidationFunc,
				MaxRetention:      int32(v.MaxRetention),
				XFilesFactor:      v.XFilesFactor,
				Retentions:        rets,
			}
			res.Responses = append(res.Responses, protov2.ServerInfoResponse{
				Server: k,
				Info:   i,
			})
		}
	}

	return res, stats, nil
}
func (z Zipper) ListPB(ctx context.Context) (*protov2.ListMetricsResponse, *types.Stats, error) {
	grpcRes, stats, err := z.ListGRPC(ctx)
	if err != nil {
		return nil, nil, err
	}

	res := &protov2.ListMetricsResponse{
		Metrics: grpcRes.Metrics,
	}
	return res, stats, nil
}
func (z Zipper) StatsPB(ctx context.Context) (*protov2.MetricDetailsResponse, *types.Stats, error) {
	grpcRes, stats, err := z.StatsGRPC(ctx)
	if err != nil {
		return nil, nil, err
	}

	metrics := make(map[string]*protov2.MetricDetails, len(grpcRes.Metrics))
	for k, v := range grpcRes.Metrics {
		metrics[k] = &protov2.MetricDetails{
			Size_:   v.Size_,
			ModTime: v.ModTime,
			ATime:   v.ATime,
			RdTime:  v.RdTime,
		}
	}

	res := &protov2.MetricDetailsResponse{
		FreeSpace:  grpcRes.FreeSpace,
		TotalSpace: grpcRes.TotalSpace,
		Metrics:    metrics,
	}

	return res, stats, nil
}
