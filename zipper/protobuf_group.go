package zipper

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	pbgrpc "github.com/go-graphite/carbonzipper/carbonzippergrpcpb"
	pb3 "github.com/go-graphite/carbonzipper/carbonzipperpb3"
	"github.com/go-graphite/carbonzipper/limiter"
	cu "github.com/go-graphite/carbonzipper/util/apictx"
	util "github.com/go-graphite/carbonzipper/util/zipperctx"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// RoundRobin is used to connect to backends inside clientGroups, implements ServerClient interface
type ClientProtobufGroup struct {
	groupName string
	servers   []string

	client *http.Client

	counter             uint64
	maxIdleConnsPerHost int

	limiter limiter.ServerLimiter
	logger  *zap.Logger
	timeout Timeouts
}

func NewClientProtobufGroupWithLimiter(groupName string, servers []string, limiter limiter.ServerLimiter, maxIdleConns int, timeout Timeouts, keepAliveInterval time.Duration) (*ClientProtobufGroup, error) {
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxIdleConns,
			DialContext: (&net.Dialer{
				Timeout:   timeout.Connect,
				KeepAlive: keepAliveInterval,
				DualStack: true,
			}).DialContext,
		},
	}

	c := &ClientProtobufGroup{
		groupName: groupName,
		servers:   servers,
		timeout:   timeout,

		client:  httpClient,
		limiter: limiter,
		logger:  zapwriter.Logger("protobufGroup").With(zap.String("name", groupName)),
	}
	return c, nil
}

func NewClientProtobufGroup(groupName string, servers []string, concurencyLimit, maxIdleConns int, timeout Timeouts, keepAliveInterval time.Duration) (*ClientProtobufGroup, error) {
	limiter := limiter.NewServerLimiter(servers, concurencyLimit)

	return NewClientProtobufGroupWithLimiter(groupName, servers, limiter, maxIdleConns, timeout, keepAliveInterval)
}

func (c *ClientProtobufGroup) pickServer() string {
	if len(c.servers) == 1 {
		// No need to do heavy operations here
		return c.servers[0]
	}
	logger := zapwriter.Logger("picker").With(zap.String("groupName", c.groupName))
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

func (c *ClientProtobufGroup) doRequest(ctx context.Context, uri string) (*serverResponse, error) {
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
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		c.logger.Error("status not ok",
			zap.Int("status_code", resp.StatusCode),
		)
		return nil, fmt.Errorf(ErrFailedToFetchFmt, c.groupName, resp.StatusCode)
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

func (c *ClientProtobufGroup) doQuery(ctx context.Context, uri string) (*serverResponse, error) {
	maxTries := 3
	if len(c.servers) > maxTries {
		maxTries = len(c.servers)
	}
	try := 0
	var res *serverResponse
	var err error
	for {
		if try > maxTries {
			break
		}
		try++
		res, err = c.doRequest(ctx, uri)
		if err != nil {
			if err == ErrNotFound {
				return nil, err
			}
			continue
		}

		return res, nil
	}

	return nil, err
}

func (c ClientProtobufGroup) Name() string {
	return c.groupName
}

func (c *ClientProtobufGroup) Fetch(ctx context.Context, request *pbgrpc.MultiFetchRequest) (*pbgrpc.MultiFetchResponse, *Stats, error) {
	stats := &Stats{}
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

	var metrics pb3.MultiFetchResponse
	err = metrics.Unmarshal(res.response)
	if err != nil {
		return nil, stats, err
	}

	stats.Servers = append(stats.Servers, res.server)

	var r pbgrpc.MultiFetchResponse
	for _, m := range metrics.Metrics {
		r.Metrics = append(r.Metrics, pbgrpc.FetchResponse{
			Name:      m.Name,
			StopTime:  uint32(m.StopTime),
			StartTime: uint32(m.StartTime),
			Values:    m.Values,
			Metadata: &pbgrpc.MetricMetadata{
				StepTime:            uint32(m.StepTime),
				AggregationFunction: "avg",
			},
		})
	}

	return &r, stats, nil
}

func (c *ClientProtobufGroup) Find(ctx context.Context, request *pbgrpc.MultiGlobRequest) (*pbgrpc.MultiGlobResponse, *Stats, error) {
	stats := &Stats{}
	rewrite, _ := url.Parse("http://127.0.0.1/metrics/find/")

	var r pbgrpc.MultiGlobResponse
	r.Metrics = make(map[string]pbgrpc.GlobResponse)
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
		var globs pb3.GlobResponse
		err = globs.Unmarshal(res.response)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		stats.Servers = append(stats.Servers, res.server)
		matches := make([]pbgrpc.GlobMatch, 0, len(globs.Matches))
		for _, m := range globs.Matches {
			matches = append(matches, pbgrpc.GlobMatch{
				Path:   m.Path,
				IsLeaf: m.IsLeaf,
			})
		}
		r.Metrics[query] = pbgrpc.GlobResponse{
			Name:    globs.Name,
			Matches: matches,
		}
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
		return nil, stats, ErrNoResponseFetched
	}
	return &r, stats, nil
}

func (c *ClientProtobufGroup) Info(ctx context.Context, request *pbgrpc.MultiMetricsInfoRequest) (*pbgrpc.MultiMetricsInfoResponse, *Stats, error) {
	return nil, nil, ErrNotImplementedYet
}

func (c *ClientProtobufGroup) List(ctx context.Context) (*pbgrpc.ListMetricsResponse, *Stats, error) {
	return nil, nil, ErrNotImplementedYet
}
func (c *ClientProtobufGroup) Stats(ctx context.Context) (*pbgrpc.MetricDetailsResponse, *Stats, error) {
	return nil, nil, ErrNotImplementedYet
}

func (c *ClientProtobufGroup) ProbeTLDs(ctx context.Context) ([]string, error) {
	logger := zapwriter.Logger("probe").With(zap.String("groupName", c.groupName))
	req := &pbgrpc.MultiGlobRequest{
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
