package zipper

import (
	"context"
	"math"

	pbgrpc "github.com/go-graphite/carbonzipper/carbonzippergrpcpb"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// RoundRobin is used to connect to backends inside clientGRPCGroups, implements ServerClient interface
type ClientGRPCGroup struct {
	groupName string
	servers   []string

	r        *manual.Resolver
	conn     *grpc.ClientConn
	dialerrc chan error
	cleanup  func()
	timeout Timeouts

	client pbgrpc.CarbonV1Client
}

func NewClientGRPCGroup(groupName string, servers []string, timeout Timeouts) (*ClientGRPCGroup, error) {
	// TODO: Implement normal resolver
	r, cleanup := manual.GenerateAndRegisterManualResolver()
	var resolvedAddrs []resolver.Address
	for _, addr := range servers {
		resolvedAddrs = append(resolvedAddrs, resolver.Address{Addr: addr})
	}

	r.NewAddress(resolvedAddrs)

	opts := []grpc.DialOption{
		grpc.WithUserAgent("carbonzipper"),
		grpc.WithCompressor(grpc.NewGZIPCompressor()),
		grpc.WithDecompressor(grpc.NewGZIPDecompressor()),
		grpc.WithBalancerName("roundrobin"), // TODO: Make that configurable
		grpc.WithMaxMsgSize(math.MaxUint32), // TODO: make that configurable
		grpc.WithInsecure(),                 // TODO: Make configurable
	}

	conn, err := grpc.Dial(r.Scheme()+":///server", opts...)
	if err != nil {
		cleanup()
		return nil, err
	}

	client := &ClientGRPCGroup{
		groupName: groupName,
		servers:   servers,

		r:       r,
		cleanup: cleanup,
		conn:    conn,
		client:  pbgrpc.NewCarbonV1Client(conn),
		timeout: timeout,
	}

	return client, nil
}

func (c ClientGRPCGroup) Name() string {
	return c.groupName
}

func (c ClientGRPCGroup) Fetch(ctx context.Context, request *pbgrpc.MultiFetchRequest) (*pbgrpc.MultiFetchResponse, *Stats, error) {
	stats := &Stats{
		Servers: []string{c.Name()},
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout.Render)
	defer cancel()

	res, err := c.client.FetchMetrics(ctx, request)
	if err != nil {
		stats.RenderErrors++
		stats.FailedServers = stats.Servers
		stats.Servers = []string{}
	}
	stats.MemoryUsage = int64(res.Size())

	return res, stats, err
}

func (c ClientGRPCGroup) Find(ctx context.Context, request *pbgrpc.MultiGlobRequest) (*pbgrpc.MultiGlobResponse, *Stats, error) {
	stats := &Stats{
		Servers: []string{c.Name()},
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout.Find)
	defer cancel()

	res, err := c.client.FindMetrics(ctx, request)
	if err != nil {
		stats.RenderErrors++
		stats.FailedServers = stats.Servers
		stats.Servers = []string{}
	}
	stats.MemoryUsage = int64(res.Size())

	return res, stats, err
}
func (c ClientGRPCGroup) Info(ctx context.Context, request *pbgrpc.MultiMetricsInfoRequest) (*pbgrpc.MultiMetricsInfoResponse, *Stats, error) {
	stats := &Stats{
		Servers: []string{c.Name()},
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout.Render)
	defer cancel()

	res, err := c.client.MetricsInfo(ctx, request)
	if err != nil {
		stats.RenderErrors++
		stats.FailedServers = stats.Servers
		stats.Servers = []string{}
	}
	stats.MemoryUsage = int64(res.Size())

	return res, stats, err
}

func (c ClientGRPCGroup) List(ctx context.Context) (*pbgrpc.ListMetricsResponse, *Stats, error) {
	stats := &Stats{
		Servers: []string{c.Name()},
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout.Render)
	defer cancel()

	res, err := c.client.ListMetrics(ctx, emptyMsg)
	if err != nil {
		stats.RenderErrors++
		stats.FailedServers = stats.Servers
		stats.Servers = []string{}
	}
	stats.MemoryUsage = int64(res.Size())

	return res, stats, err
}
func (c ClientGRPCGroup) Stats(ctx context.Context) (*pbgrpc.MetricDetailsResponse, *Stats, error) {
	stats := &Stats{
		Servers: []string{c.Name()},
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout.Render)
	defer cancel()

	res, err := c.client.Stats(ctx, emptyMsg)
	if err != nil {
		stats.RenderErrors++
		stats.FailedServers = stats.Servers
		stats.Servers = []string{}
	}
	stats.MemoryUsage = int64(res.Size())

	return res, stats, err
}

func (c ClientGRPCGroup) ProbeTLDs(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout.Find)
	defer cancel()

	req := &pbgrpc.MultiGlobRequest{
		Metrics: []string{"*"},
	}
	res, _, err := c.Find(ctx, req)
	if err != nil {
		return nil, err
	}
	var tlds []string
	for _, m := range res.Metrics {
		for _, v := range m.Matches {
			tlds = append(tlds, v.Path)
		}
	}
	return tlds, nil
}
