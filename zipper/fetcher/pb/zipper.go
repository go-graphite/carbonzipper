package pb

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"time"

	pb3 "github.com/go-graphite/carbonzipper/carbonzipperpb3"
	"github.com/go-graphite/carbonzipper/limiter"
	"github.com/go-graphite/carbonzipper/pathcache"
	util "github.com/go-graphite/carbonzipper/util/zipperctx"
	"github.com/go-graphite/carbonzipper/zipper"

	"strings"

	"github.com/lomik/zapwriter"
	"github.com/satori/go.uuid"
	"go.uber.org/zap"

	"google.golang.org/grpc"
)

type grpcClient struct {
	server string

	conn     *grpc.ClientConn
	dialerrc chan error
}

// Zipper provides interface to Zipper-related functions
type Zipper struct {
	storageClient *http.Client
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
	searchBackends   []string
	searchPrefix     string

	pathCache   pathcache.PathCache
	searchCache pathcache.PathCache

	backends []string
	// Will broadcast to all servers there
	backendsV2                []zipper.BackendV2
	concurrencyLimitPerServer int
	maxIdleConnsPerHost       int

	sendStats func(*zipper.Stats)

	getter *zipper.PBGetter
}

type nameLeaf struct {
	name string
	leaf bool
}

// NewZipper allows to create new Zipper
func NewZipper(sender func(*zipper.Stats), config *zipper.Config) *Zipper {
	logger := zapwriter.Logger("new_zipper")

	var searchBackends []string
	prefix := config.CarbonSearch.Prefix
	if config.CarbonSearch.Backend != "" {
		searchBackends = append(searchBackends, config.CarbonSearch.Backend)
	} else {
		searchBackends = append(searchBackends, config.CarbonSearchV2.Backends...)
		prefix = config.CarbonSearchV2.Prefix
	}

	z := &Zipper{
		probeTicker: time.NewTicker(10 * time.Minute),
		ProbeQuit:   make(chan struct{}),
		ProbeForce:  make(chan int),

		sendStats: sender,

		pathCache:   config.PathCache,
		searchCache: config.SearchCache,

		storageClient:             &http.Client{},
		backends:                  config.Backends,
		searchBackends:            searchBackends,
		searchPrefix:              prefix,
		searchConfigured:          len(prefix) > 0 && len(searchBackends) > 0,
		concurrencyLimitPerServer: config.ConcurrencyLimitPerServer,
		maxIdleConnsPerHost:       config.MaxIdleConnsPerHost,
		keepAliveInterval:         config.KeepAliveInterval,
		timeoutAfterAllStarted:    config.Timeouts.AfterStarted,
		timeout:                   config.Timeouts.Render,
		timeoutConnect:            config.Timeouts.Connect,
	}

	logger.Info("zipper config",
		zap.Any("config", config),
	)

	if z.concurrencyLimitPerServer != 0 {
		limiterServers := z.backends
		if z.searchConfigured {
			limiterServers = append(limiterServers, z.searchBackends...)
		}

		z.getter = zipper.NewPBGetter(limiterServers, z.concurrencyLimitPerServer, logger, config.Timeouts)
	} else {
		z.getter = zipper.NewPBGetter([]string{}, 0, logger, config.Timeouts)
	}

	// configure the storage client
	z.storageClient.Transport = &http.Transport{
		MaxIdleConnsPerHost: z.maxIdleConnsPerHost,
		DialContext: (&net.Dialer{
			Timeout:   z.timeoutConnect,
			KeepAlive: z.keepAliveInterval,
			DualStack: true,
		}).DialContext,
	}

	go z.probeTlds()

	z.ProbeForce <- 1
	return z
}

var errNoResponses = fmt.Errorf("no responses fetched from upstream")
var errNoMetricsFetched = fmt.Errorf("no metrics in the response")

func mergeResponses(responses []zipper.ServerResponse, stats *zipper.Stats) ([]string, *pb3.MultiFetchResponse) {
	logger := zapwriter.Logger("zipper_render")

	servers := make([]string, 0, len(responses))
	metrics := make(map[string][]pb3.FetchResponse)

	for _, r := range responses {
		var d pb3.MultiFetchResponse
		err := d.Unmarshal(r.Response)
		if err != nil {
			logger.Error("error decoding protobuf response",
				zap.String("server", r.Server),
				zap.Error(err),
			)
			logger.Debug("response hexdump",
				zap.String("response", hex.Dump(r.Response)),
			)
			stats.RenderErrors++
			continue
		}
		stats.MemoryUsage += int64(d.Size())
		for _, m := range d.Metrics {
			metrics[m.GetName()] = append(metrics[m.GetName()], m)
		}
		servers = append(servers, r.Server)
	}

	var multi pb3.MultiFetchResponse

	if len(metrics) == 0 {
		return servers, nil
	}

	for name, decoded := range metrics {
		logger.Debug("decoded response",
			zap.String("name", name),
			zap.Any("decoded", decoded),
		)

		if len(decoded) == 1 {
			logger.Debug("only one decoded response to merge",
				zap.String("name", name),
			)
			m := decoded[0]
			multi.Metrics = append(multi.Metrics, m)
			continue
		}

		// Use the metric with the highest resolution as our base
		var highest int
		for i, d := range decoded {
			if d.GetStepTime() < decoded[highest].GetStepTime() {
				highest = i
			}
		}
		decoded[0], decoded[highest] = decoded[highest], decoded[0]

		metric := decoded[0]

		mergeValues(&metric, decoded, stats)
		multi.Metrics = append(multi.Metrics, metric)
	}

	stats.MemoryUsage += int64(multi.Size())

	return servers, &multi
}

func mergeValues(metric *pb3.FetchResponse, decoded []pb3.FetchResponse, stats *zipper.Stats) {
	logger := zapwriter.Logger("zipper_render")

	var responseLengthMismatch bool
	for i := range metric.Values {
		if !metric.IsAbsent[i] || responseLengthMismatch {
			continue
		}

		// found a missing value, find a replacement
		for other := 1; other < len(decoded); other++ {

			m := decoded[other]

			if len(m.Values) != len(metric.Values) {
				logger.Error("unable to merge ovalues",
					zap.Int("metric_values", len(metric.Values)),
					zap.Int("response_values", len(m.Values)),
				)
				// TODO(dgryski): we should remove
				// decoded[other] from the list of responses to
				// consider but this assumes that decoded[0] is
				// the 'highest resolution' response and thus
				// the one we want to keep, instead of the one
				// we want to discard

				stats.RenderErrors++
				responseLengthMismatch = true
				break
			}

			// found one
			if !m.IsAbsent[i] {
				metric.IsAbsent[i] = false
				metric.Values[i] = m.Values[i]
			}
		}
	}
}

func infoUnpackPB(responses []zipper.ServerResponse, stats *zipper.Stats) map[string]pb3.InfoResponse {
	logger := zapwriter.Logger("zipper_info").With(zap.String("handler", "info"))

	decoded := make(map[string]pb3.InfoResponse)
	for _, r := range responses {
		if r.Response == nil {
			continue
		}
		var d pb3.InfoResponse
		err := d.Unmarshal(r.Response)
		if err != nil {
			logger.Error("error decoding protobuf response",
				zap.String("server", r.Server),
				zap.Error(err),
			)
			logger.Debug("response hexdump",
				zap.String("response", hex.Dump(r.Response)),
			)
			stats.InfoErrors++
			continue
		}
		decoded[r.Server] = d
	}

	logger.Debug("info request",
		zap.Any("decoded_response", decoded),
	)

	return decoded
}

func findUnpackPB(responses []zipper.ServerResponse, stats *zipper.Stats) ([]pb3.GlobMatch, map[string][]string) {
	logger := zapwriter.Logger("zipper_find").With(zap.String("handler", "findUnpackPB"))

	// metric -> [server1, ... ]
	paths := make(map[string][]string)
	seen := make(map[nameLeaf]bool)

	var metrics []pb3.GlobMatch
	for _, r := range responses {
		var metric pb3.GlobResponse
		err := metric.Unmarshal(r.Response)
		if err != nil {
			logger.Error("error decoding protobuf response",
				zap.String("server", r.Server),
				zap.Error(err),
			)
			logger.Debug("response hexdump",
				zap.String("response", hex.Dump(r.Response)),
			)
			stats.FindErrors += 1
			continue
		}

		for _, match := range metric.Matches {
			n := nameLeaf{match.Path, match.IsLeaf}
			_, ok := seen[n]
			if !ok {
				// we haven't seen this name yet
				// add the metric to the list of metrics to return
				metrics = append(metrics, match)
				seen[n] = true
			}
			// add the server to the list of servers that know about this metric
			p := paths[match.Path]
			p = append(p, r.Server)
			paths[match.Path] = p
		}
	}

	return metrics, paths
}

func (z *Zipper) doProbe() {
	stats := &zipper.Stats{}
	logger := zapwriter.Logger("probe")
	// Generate unique ID on every restart
	uuid := uuid.NewV4()
	ctx := util.SetUUID(context.Background(), uuid.String())

	responses := z.getter.FindPB(ctx, z.backends, "*", stats, z.timeoutAfterAllStarted)

	if len(responses) == 0 {
		logger.Info("TLD Probe returned empty set")
		return
	}

	_, paths := findUnpackPB(responses, stats)

	z.sendStats(stats)

	incompleteResponse := false
	if len(responses) != len(z.backends) {
		incompleteResponse = true
	}

	logger.Info("TLD Probe run results",
		zap.String("carbonzipper_uuid", uuid.String()),
		zap.Int("paths_count", len(paths)),
		zap.Int("responses_received", len(responses)),
		zap.Int("backends", len(z.backends)),
		zap.Bool("incomplete_response", incompleteResponse),
	)

	// update our cache of which servers have which metrics
	for k, v := range paths {
		z.pathCache.Set(k, v)
		logger.Debug("TLD Probe",
			zap.String("path", k),
			zap.Strings("servers", v),
			zap.String("carbonzipper_uuid", uuid.String()),
		)
	}
}

func (z *Zipper) probeTlds() {
	for {
		select {
		case <-z.probeTicker.C:
			z.doProbe()
		case <-z.ProbeForce:
			z.doProbe()
		case <-z.ProbeQuit:
			z.probeTicker.Stop()
			return
		}
	}
}

func (z *Zipper) fetchCarbonsearchResponse(ctx context.Context, logger *zap.Logger, url string, stats *zipper.Stats) []string {
	// Send query to SearchBackend. The result is []queries for StorageBackends
	searchResponse := z.getter.FindPB(ctx, z.searchBackends, url, stats, z.timeoutAfterAllStarted)
	m, err := findUnpackPB(searchResponse, stats)
	if err != nil {
		return nil
	}

	queries := make([]string, 0, len(m))
	for _, v := range m {
		queries = append(queries, v.Path)
	}
	return queries
}

func (z *Zipper) Render(ctx context.Context, logger *zap.Logger, target string, from, until int32) (*pb3.MultiFetchResponse, *zipper.Stats, error) {
	stats := &zipper.Stats{}

	var serverList []string
	var ok bool
	var responses []zipper.ServerResponse
	if z.searchConfigured && strings.HasPrefix(target, z.searchPrefix) {
		stats.SearchRequests++

		var metrics []string
		if metrics, ok = z.searchCache.Get(target); !ok || metrics == nil || len(metrics) == 0 {
			stats.SearchCacheMisses++
			metrics = z.fetchCarbonsearchResponse(ctx, logger, target, stats)
			z.searchCache.Set(target, metrics)
		} else {
			stats.SearchCacheHits++
		}

		for _, target := range metrics {
			// lookup the server list for this metric, or use all the servers if it's unknown
			if serverList, ok = z.pathCache.Get(target); !ok || serverList == nil || len(serverList) == 0 {
				stats.CacheMisses++
				serverList = z.backends
			} else {
				stats.CacheHits++
			}

			newResponses := z.getter.FetchPB(ctx, serverList, target, int(from), int(until), stats, z.timeoutAfterAllStarted)
			responses = append(responses, newResponses...)
		}
	} else {
		// lookup the server list for this metric, or use all the servers if it's unknown
		if serverList, ok = z.pathCache.Get(target); !ok || serverList == nil || len(serverList) == 0 {
			stats.CacheMisses++
			serverList = z.backends
		} else {
			stats.CacheHits++
		}

		responses = z.getter.FetchPB(ctx, serverList, target, int(from), int(until), stats, z.timeoutAfterAllStarted)
	}

	for i := range responses {
		stats.MemoryUsage += int64(len(responses[i].Response))
	}

	if len(responses) == 0 {
		return nil, stats, errNoResponses
	}

	servers, metrics := mergeResponses(responses, stats)

	if metrics == nil {
		return nil, stats, errNoMetricsFetched
	}

	z.pathCache.Set(target, servers)

	return metrics, stats, nil
}

func (z *Zipper) Info(ctx context.Context, logger *zap.Logger, target string) (map[string]pb3.InfoResponse, *zipper.Stats, error) {
	stats := &zipper.Stats{}
	var serverList []string
	var ok bool

	// lookup the server list for this metric, or use all the servers if it's unknown
	if serverList, ok = z.pathCache.Get(target); !ok || serverList == nil || len(serverList) == 0 {
		stats.CacheMisses++
		serverList = z.backends
	} else {
		stats.CacheHits++
	}

	responses := z.getter.InfoPB(ctx, serverList, target, stats, z.timeoutAfterAllStarted)

	if len(responses) == 0 {
		stats.InfoErrors++
		return nil, stats, errNoResponses
	}

	infos := infoUnpackPB(responses, stats)
	return infos, stats, nil
}

func (z *Zipper) Find(ctx context.Context, logger *zap.Logger, query string) ([]pb3.GlobMatch, *zipper.Stats, error) {
	stats := &zipper.Stats{}
	queries := []string{query}

	rewrite, _ := url.Parse("http://127.0.0.1/metrics/find/")

	v := url.Values{
		"query":  queries,
		"format": []string{"protobuf"},
	}
	rewrite.RawQuery = v.Encode()

	if z.searchConfigured && strings.HasPrefix(query, z.searchPrefix) {
		stats.SearchRequests++
		// 'completer' requests are translated into standard Find requests with
		// a trailing '*' by graphite-web
		if strings.HasSuffix(query, "*") {
			searchCompleterResponse := z.getter.FindPB(ctx, z.searchBackends, query, stats, z.timeoutAfterAllStarted)
			matches, _ := findUnpackPB(searchCompleterResponse, stats)
			// this is a completer request, and so we should return the set of
			// virtual metrics returned by carbonsearch verbatim, rather than trying
			// to find them on the stores
			return matches, stats, nil
		}
		var ok bool
		if queries, ok = z.searchCache.Get(query); !ok || queries == nil || len(queries) == 0 {
			stats.SearchCacheMisses++
			queries = z.fetchCarbonsearchResponse(ctx, logger, query, stats)
			z.searchCache.Set(query, queries)
		} else {
			stats.SearchCacheHits++
		}
	}

	var metrics []pb3.GlobMatch
	// TODO(nnuss): Rewrite the result queries to a series of brace expansions based on TLD?
	// [a.b, a.c, a.dee.eee.eff, x.y] => [ "a.{b,c,dee.eee.eff}", "x.y" ]
	// Be mindful that carbonserver's default MaxGlobs is 10
	for _, query := range queries {

		v.Set("query", query)
		rewrite.RawQuery = v.Encode()

		var tld string
		if i := strings.IndexByte(query, '.'); i > 0 {
			tld = query[:i]
		}

		// lookup tld in our map of where they live to reduce the set of
		// servers we bug with our find
		var backends []string
		var ok bool
		if backends, ok = z.pathCache.Get(tld); !ok || backends == nil || len(backends) == 0 {
			stats.CacheMisses++
			backends = z.backends
		} else {
			stats.CacheHits++
		}

		responses := z.getter.FindPB(ctx, backends, query, stats, z.timeoutAfterAllStarted)

		if len(responses) == 0 {
			return nil, stats, errNoResponses
		}

		m, paths := findUnpackPB(responses, stats)
		metrics = append(metrics, m...)

		// update our cache of which servers have which metrics
		allServers := make([]string, 0)
		for k, v := range paths {
			z.pathCache.Set(k, v)
			allServers = append(allServers, v...)
		}
		z.pathCache.Set(query, allServers)
	}

	return metrics, stats, nil
}
