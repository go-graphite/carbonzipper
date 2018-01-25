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

var ErrUnknownLBMethodFmt = "unknown lb method: '%v', supported: %v"

type LBMethod int

const (
	RoundRobinLB LBMethod = iota
	BroadcastLB
)

func (p LBMethod) keys(m map[string]LBMethod) []string {
	res := make([]string, 0)
	for k := range m {
		res = append(res, k)
	}
	return res
}

var supportedLBMethods = map[string]LBMethod{
	"roundrobin": RoundRobinLB,
	"rr":         RoundRobinLB,
	"any":        RoundRobinLB,
	"broadcast":  BroadcastLB,
	"all":        BroadcastLB,
}

func (m *LBMethod) FromString(method string) error {
	var ok bool
	if *m, ok = supportedLBMethods[strings.ToLower(method)]; !ok {
		return fmt.Errorf(ErrUnknownLBMethodFmt, method, m.keys(supportedLBMethods))
	}
	return nil
}

func (m *LBMethod) UnmarshalJSON(data []byte) error {
	method := strings.ToLower(string(data))
	return m.FromString(method)
}

func (m *LBMethod) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var method string
	err := unmarshal(&method)
	if err != nil {
		return err
	}

	return m.FromString(method)
}

func (m LBMethod) MarshalJSON() ([]byte, error) {
	switch m {
	case RoundRobinLB:
		return json.Marshal("RoundRobin")
	case BroadcastLB:
		return json.Marshal("Broadcast")
	}

	return nil, fmt.Errorf(ErrUnknownLBMethodFmt, m, m.keys(supportedLBMethods))
}

// Protocol defines protocol
type Protocol int

var ErrUnknownProtocolFmt = "unknown protocol: '%v', supported: %v"

const (
	Protobuf Protocol = iota
	GRPC
)

var supportedProtocols = map[string]Protocol{
	"protobuf": Protobuf,
	"pb":       Protobuf,
	"pb3":      Protobuf,
	"grpc":     GRPC,
}

func (p Protocol) keys(m map[string]Protocol) []string {
	res := make([]string, 0)
	for k := range m {
		res = append(res, k)
	}
	return res
}

func (m *Protocol) FromString(method string) error {
	var ok bool
	if *m, ok = supportedProtocols[strings.ToLower(method)]; !ok {
		return fmt.Errorf(ErrUnknownProtocolFmt, method, m.keys(supportedProtocols))
	}
	return nil
}

func (m *Protocol) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var method string
	err := unmarshal(&method)
	if err != nil {
		return err
	}

	return m.FromString(method)
}

func (m *Protocol) UnmarshalJSON(data []byte) error {
	method := strings.ToLower(string(data))

	return m.FromString(method)
}

func (m Protocol) MarshalJSON() ([]byte, error) {
	switch m {
	case Protobuf:
		return json.Marshal("protobuf")
	case GRPC:
		return json.Marshal("grpc")
	}

	return nil, fmt.Errorf(ErrUnknownProtocolFmt, m, m.keys(supportedProtocols))
}

type BackendV2 struct {
	GroupName        string    `yaml:"groupName"`
	Protocol         Protocol  `yaml:"backendProtocol"`
	LBMethod         LBMethod  `yaml:"lbMethod"` // Valid: rr/roundrobin, broadcast/all
	Servers          []string  `yaml:"servers"`
	Timeouts         *Timeouts `yaml:"timeouts"`
	ConcurrencyLimit *int      `yamp:"concurrencyLimit"`
}

// Timeouts is a global structure that contains configuration for zipper Timeouts
type Timeouts struct {
	Find    time.Duration `yaml:"find"`
	Render  time.Duration `yaml:"render"`
	Connect time.Duration `yaml:"connect"`
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
