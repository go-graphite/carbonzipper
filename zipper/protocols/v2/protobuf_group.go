package v2

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"

	"github.com/go-graphite/carbonzipper/limiter"
	cu "github.com/go-graphite/carbonzipper/util/apictx"
	util "github.com/go-graphite/carbonzipper/util/zipperctx"
	"github.com/go-graphite/carbonzipper/zipper/metadata"
	"github.com/go-graphite/carbonzipper/zipper/types"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

func init() {
	aliases := []string{"carbonapi_v2_pb", "proto_v2_pb", "v2_pb", "pb", "pb3", "protobuf", "protobuf3"}
	metadata.Metadata.Lock()
	for _, name := range aliases {
		metadata.Metadata.SupportedProtocols[name] = struct{}{}
		metadata.Metadata.ProtocolInits[name] = NewClientProtoV2Group
		metadata.Metadata.ProtocolInitsWithLimiter[name] = NewClientProtoV2GroupWithLimiter
	}
	defer metadata.Metadata.Unlock()
}

// RoundRobin is used to connect to backends inside clientGroups, implements ServerClient interface
type ClientProtoV2Group struct {
	groupName string
	servers   []string

	client *http.Client

	counter             uint64
	maxIdleConnsPerHost int

	limiter  limiter.ServerLimiter
	logger   *zap.Logger
	timeout  types.Timeouts
	maxTries int
}

func NewClientProtoV2GroupWithLimiter(config types.BackendV2, limiter limiter.ServerLimiter) (*ClientProtoV2Group, error) {
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: *config.MaxIdleConnsPerHost,
			DialContext: (&net.Dialer{
				Timeout:   config.Timeouts.Connect,
				KeepAlive: *config.KeepAliveInterval,
				DualStack: true,
			}).DialContext,
		},
	}

	c := &ClientProtoV2Group{
		groupName: config.GroupName,
		servers:   config.Servers,
		timeout:   *config.Timeouts,
		maxTries:  *config.MaxTries,

		client:  httpClient,
		limiter: limiter,
		logger:  zapwriter.Logger("protobufGroup").With(zap.String("name", config.GroupName)),
	}
	return c, nil
}

func NewClientProtoV2Group(config types.BackendV2) (*ClientProtoV2Group, error) {
	limiter := limiter.NewServerLimiter(config.Servers, *config.ConcurrencyLimit)

	return NewClientProtoV2GroupWithLimiter(config, limiter)
}

func (c *ClientProtoV2Group) pickServer() string {
	if len(c.servers) == 1 {
		// No need to do heavy operations here
		return c.servers[0]
	}
	logger := c.logger.With(zap.String("function", "picker"))
	counter := atomic.AddUint64(&(c.counter), 1)
	idx := counter % uint64(len(c.servers))
	srv := c.servers[int(idx)]
	logger.Debug("picked",
		zap.Uint64("counter", counter),
		zap.Uint64("idx", idx),
		zap.String("server", srv),
	)

	return srv
}

type serverResponse struct {
	server   string
	response []byte
}

func (c *ClientProtoV2Group) doRequest(ctx context.Context, uri string) (*serverResponse, error) {
	server := c.pickServer()

	u, err := url.Parse(server + uri)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req = cu.MarshalCtx(ctx, util.MarshalCtx(ctx, req))

	c.logger.Debug("trying to get slot",
		zap.String("name", c.groupName),
		zap.String("uri", u.String()),
	)

	err = c.limiter.Enter(ctx, c.groupName)
	if err != nil {
		c.logger.Debug("timeout waiting for a slot")
		return nil, err
	}
	defer c.limiter.Leave(ctx, server)

	c.logger.Debug("got slot")

	resp, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		c.logger.Error("error fetching result",
			zap.Error(err),
		)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		c.logger.Error("status not ok, not found",
			zap.Int("status_code", resp.StatusCode),
		)
		return nil, types.ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		c.logger.Error("status not ok",
			zap.Int("status_code", resp.StatusCode),
		)
		return nil, fmt.Errorf(types.ErrFailedToFetchFmt, c.groupName, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.logger.Error("error reading body",
			zap.Error(err),
		)
		return nil, err
	}

	return &serverResponse{server: server, response: body}, nil
}

func (c *ClientProtoV2Group) doQuery(ctx context.Context, uri string) (*serverResponse, error) {
	maxTries := c.maxTries
	if len(c.servers) > maxTries {
		maxTries = len(c.servers)
	}
	var res *serverResponse
	var err error
	for try := 0; try < maxTries; try++ {
		res, err = c.doRequest(ctx, uri)
		if err != nil {
			if err == types.ErrNotFound {
				return nil, err
			}
			continue
		}

		return res, nil
	}

	return nil, err
}

func (c ClientProtoV2Group) Name() string {
	return c.groupName
}

func (c ClientProtoV2Group) Backends() []string {
	return c.servers
}

func (c *ClientProtoV2Group) Fetch(ctx context.Context, request *protov3.MultiFetchRequest) (*protov3.MultiFetchResponse, *types.Stats, error) {
	stats := &types.Stats{}
	rewrite, _ := url.Parse("http://127.0.0.1/render/")

	var targets []string
	for _, m := range request.Metrics {
		targets = append(targets, m.Name)
	}

	v := url.Values{
		"target": targets,
		"format": []string{"protobuf"},
		"from":   []string{strconv.Itoa(int(request.Metrics[0].StartTime))},
		"until":  []string{strconv.Itoa(int(request.Metrics[0].StopTime))},
	}
	rewrite.RawQuery = v.Encode()
	res, err := c.doQuery(ctx, rewrite.RequestURI())
	if err != nil {
		return nil, stats, err
	}

	var metrics protov2.MultiFetchResponse
	err = metrics.Unmarshal(res.response)
	if err != nil {
		return nil, stats, err
	}

	stats.Servers = append(stats.Servers, res.server)

	var r protov3.MultiFetchResponse
	for _, m := range metrics.Metrics {
		r.Metrics = append(r.Metrics, protov3.FetchResponse{
			Name:              m.Name,
			ConsolidationFunc: "average",
			StopTime:          uint32(m.StopTime),
			StartTime:         uint32(m.StartTime),
			StepTime:          uint32(m.StepTime),
			Values:            m.Values,
		})
	}

	return &r, stats, nil
}

func (c *ClientProtoV2Group) Find(ctx context.Context, request *protov3.MultiGlobRequest) (*protov3.MultiGlobResponse, *types.Stats, error) {
	stats := &types.Stats{}
	rewrite, _ := url.Parse("http://127.0.0.1/metrics/find/")

	var r protov3.MultiGlobResponse
	r.Metrics = make([]protov3.GlobResponse, 0)
	var errors []error
	for _, query := range request.Metrics {
		v := url.Values{
			"query":  []string{query},
			"format": []string{"protobuf"},
		}
		rewrite.RawQuery = v.Encode()
		res, err := c.doQuery(ctx, rewrite.RequestURI())
		if err != nil {
			errors = append(errors, err)
			continue
		}
		var globs protov2.GlobResponse
		err = globs.Unmarshal(res.response)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		stats.Servers = append(stats.Servers, res.server)
		matches := make([]protov3.GlobMatch, 0, len(globs.Matches))
		for _, m := range globs.Matches {
			matches = append(matches, protov3.GlobMatch{
				Path:   m.Path,
				IsLeaf: m.IsLeaf,
			})
		}
		r.Metrics = append(r.Metrics, protov3.GlobResponse{
			Name:    globs.Name,
			Matches: matches,
		})
	}

	if len(errors) != 0 {
		strErrors := make([]string, 0, len(errors))
		for _, e := range errors {
			strErrors = append(strErrors, e.Error())
		}
		c.logger.Error("errors occurred while getting results",
			zap.Strings("errors", strErrors),
		)
	}

	if len(r.Metrics) == 0 {
		return nil, stats, types.ErrNoResponseFetched
	}
	return &r, stats, nil
}

func (c *ClientProtoV2Group) Info(ctx context.Context, request *protov3.MultiMetricsInfoRequest) (*protov3.ZipperInfoResponse, *types.Stats, error) {
	return nil, nil, types.ErrNotImplementedYet
}

func (c *ClientProtoV2Group) List(ctx context.Context) (*protov3.ListMetricsResponse, *types.Stats, error) {
	return nil, nil, types.ErrNotImplementedYet
}
func (c *ClientProtoV2Group) Stats(ctx context.Context) (*protov3.MetricDetailsResponse, *types.Stats, error) {
	return nil, nil, types.ErrNotImplementedYet
}

func (c *ClientProtoV2Group) ProbeTLDs(ctx context.Context) ([]string, error) {
	logger := c.logger.With(zap.String("function", "prober"))
	req := &protov3.MultiGlobRequest{
		Metrics: []string{"*"},
	}

	logger.Debug("doing request",
		zap.Any("request", req),
	)

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
