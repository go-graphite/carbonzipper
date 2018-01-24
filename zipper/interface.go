package zipper

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-graphite/carbonzipper/pathcache"

	pbgrpc "github.com/go-graphite/carbonzipper/carbonzippergrpcpb"
	pb3 "github.com/go-graphite/carbonzipper/carbonzipperpb3"
)

// Stats provides zipper-related statistics
type Stats struct {
	Timeouts          int64
	FindErrors        int64
	RenderErrors      int64
	InfoErrors        int64
	SearchRequests    int64
	SearchCacheHits   int64
	SearchCacheMisses int64

	MemoryUsage int64

	CacheMisses int64
	CacheHits   int64

	Servers       []string
	FailedServers []string
}

func (s *Stats) Merge(stats *Stats) {
	s.Timeouts += stats.Timeouts
	s.FindErrors += stats.FindErrors
	s.RenderErrors += stats.RenderErrors
	s.InfoErrors += stats.InfoErrors
	s.SearchRequests += stats.SearchRequests
	s.SearchCacheHits += stats.SearchCacheHits
	s.SearchCacheMisses += stats.SearchCacheMisses
	s.MemoryUsage += stats.MemoryUsage
	s.CacheMisses += stats.CacheMisses
	s.CacheHits += stats.CacheHits
	s.Servers = append(s.Servers, stats.Servers...)
	s.FailedServers = append(s.FailedServers, stats.FailedServers...)
}

var ErrUnknownLBMethod = fmt.Errorf("unknown lb method")

type LBMethod int

const (
	RoundRobinLB LBMethod = iota
	BroadcastLB
)

func (m *LBMethod) UnmarshalJSON(data []byte) error {
	method := strings.ToLower(string(data))
	switch method {
	case "roundrobin", "rr":
		*m = RoundRobinLB
	case "broadcast", "all":
		*m = BroadcastLB
	default:
		return ErrUnknownLBMethod
	}
	return nil
}

func (m LBMethod) MarshalJSON() ([]byte, error) {
	switch m {
	case RoundRobinLB:
		return json.Marshal("RoundRobin")
	case BroadcastLB:
		return json.Marshal("Broadcast")
	}

	return nil, ErrUnknownLBMethod
}

// Protocol defines protocol
type Protocol int

var ErrUnknownProtocol = fmt.Errorf("unknown protocol")

const (
	Protobuf Protocol = iota
	GRPC
)

func (m *Protocol) UnmarshalJSON(data []byte) error {
	method := strings.ToLower(string(data))
	switch method {
	case "protobuf", "pb", "pb3":
		*m = Protobuf
	case "grpc":
		*m = GRPC
	default:
		return ErrUnknownProtocol
	}
	return nil
}

func (m Protocol) MarshalJSON() ([]byte, error) {
	switch m {
	case Protobuf:
		return json.Marshal("protobuf")
	case GRPC:
		return json.Marshal("grpc")
	}

	return nil, ErrUnknownProtocol
}

type ServerGroup struct {
	LBMethod LBMethod `yaml:"lbMethod"` // Valid: rr/roundrobin, broadcast/all
	Servers  []string `yaml:"servers"`
	Timeouts          *Timeouts
	ConcurrencyLimit  *int
}

type BackendV2 struct {
	GroupName string   `yaml:"groupName"`
	Protocol  Protocol `yaml:"backendProtocol"`
	ServerGroup
}

// Timeouts is a global structure that contains configuration for zipper Timeouts
type Timeouts struct {
	Global       time.Duration `yaml:"global"`
	Find         time.Duration `yaml:"find"`
	Render       time.Duration `yaml:"render"`
	AfterStarted time.Duration `yaml:"afterStarted"`
	Connect      time.Duration `yaml:"connect"`
}

// CarbonSearch is a global structure that contains carbonsearch related configuration bits
type CarbonSearch struct {
	Backend string `yaml:"backend"`
	Prefix  string `yaml:"prefix"`
}

type CarbonSearchV2 struct {
	Type     Protocol `yaml:"backendProtocol"`
	Backends []string `yaml:"backends"`
	Prefix   string   `yaml:"prefix"`
}

// Config is a structure that contains zipper-related configuration bits
type Config struct {
	ConcurrencyLimitPerServer int
	MaxIdleConnsPerHost       int
	Backends                  []string
	BackendsV2                []BackendV2

	CarbonSearch   CarbonSearch
	CarbonSearchV2 CarbonSearchV2

	PathCache         pathcache.PathCache
	SearchCache       pathcache.PathCache
	Timeouts          Timeouts
	KeepAliveInterval time.Duration `yaml:"keepAliveInterval"`
}

type ServerResponse struct {
	Server   string
	Response []byte
}

type ServerClient interface {
	Name() string

	Fetch(ctx context.Context, request *pbgrpc.MultiFetchRequest) (*pbgrpc.MultiFetchResponse, *Stats, error)
	Find(ctx context.Context, request *pbgrpc.MultiGlobRequest) (*pbgrpc.MultiGlobResponse, *Stats, error)
	Info(ctx context.Context, request *pbgrpc.MultiMetricsInfoRequest) (*pbgrpc.MultiMetricsInfoResponse, *Stats, error)

	List(ctx context.Context) (*pbgrpc.ListMetricsResponse, *Stats, error)
	Stats(ctx context.Context) (*pbgrpc.MetricDetailsResponse, *Stats, error)

	ProbeTLDs(ctx context.Context) ([]string, error)
}

type Fetcher interface {
	// PB-compatible methods
	FetchPB(ctx context.Context, query []string, startTime, stopTime int32) (*pb3.MultiFetchResponse, *Stats, error)
	FindPB(ctx context.Context, query []string) (*pb3.GlobResponse, *Stats, error)

	InfoPB(ctx context.Context, targets []string) (*pb3.ZipperInfoResponse, *Stats, error)
	ListPB(ctx context.Context) (*pb3.ListMetricsResponse, *Stats, error)
	StatsPB(ctx context.Context) (*pb3.MetricDetailsResponse, *Stats, error)

	// GRPC-compatible methods
	FetchGRPC(ctx context.Context, request *pbgrpc.MultiFetchRequest) (*pbgrpc.MultiFetchResponse, *Stats, error)
	FindGRPC(ctx context.Context, request *pbgrpc.MultiGlobRequest) ([]*pbgrpc.MultiGlobResponse, *Stats, error)

	InfoGRPC(ctx context.Context, request *pbgrpc.MultiMetricsInfoRequest) (*pbgrpc.ZipperInfoResponse, *Stats, error)
	ListGRPC(ctx context.Context) (*pbgrpc.ListMetricsResponse, *Stats, error)
	StatsGRPC(ctx context.Context) (*pbgrpc.MetricDetailsResponse, *Stats, error)
}
