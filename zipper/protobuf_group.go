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
}

func NewClientProtobufGroupWithLimiter(groupName string, servers []string, limiter limiter.ServerLimiter, maxIdleConns int, connectTimeout, keepAliveInterval time.Duration) (*ClientProtobufGroup, error) {
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxIdleConns,
			DialContext: (&net.Dialer{
				Timeout:   connectTimeout,
				KeepAlive: keepAliveInterval,
				DualStack: true,
			}).DialContext,
		},
	}

	c := &ClientProtobufGroup{
		groupName: groupName,
		servers:   servers,

		client:  httpClient,
		limiter: limiter,
		logger:  zapwriter.Logger("protobufGroup").With(zap.String("name", groupName)),
	}
	return c, nil
}

func NewClientProtobufGroup(groupName string, servers []string, concurencyLimit, maxIdleConns int, connectTimeout, keepAliveInterval time.Duration) (*ClientProtobufGroup, error) {
	limiter := limiter.NewServerLimiter(servers, concurencyLimit)

	return NewClientProtobufGroupWithLimiter(groupName, servers, limiter, maxIdleConns, connectTimeout, keepAliveInterval)
}

func (c ClientProtobufGroup) pickServer() string {
	// We don't care about race conditions here
	counter := atomic.LoadUint64(&c.counter)
	atomic.AddUint64(&c.counter, 1)

	return c.servers[int(counter%uint64(len(c.servers)))]
}

type serverResponse struct {
	server   string
	response []byte
}

func (c ClientProtobufGroup) doRequest(ctx context.Context, uri string) (*serverResponse, error) {
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

	c.limiter.Enter(c.groupName)
	defer c.limiter.Leave(server)

	resp, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(ErrFailedToFetchFmt, c.groupName, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return &serverResponse{server: server, response: body}, nil
}

func (c ClientProtobufGroup) doQuery(ctx context.Context, uri string) (*serverResponse, error) {
	done := false
	maxTries := 3
	if len(c.servers) > maxTries {
		maxTries = len(c.servers)
	}
	try := 0
	var res *serverResponse
	var err error
	for !done {
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

		return res, err
	}

	return nil, err
}

func (c ClientProtobufGroup) Name() string {
	return c.groupName
}

func (c ClientProtobufGroup) Fetch(ctx context.Context, request *pbgrpc.MultiFetchRequest) (*pbgrpc.MultiFetchResponse, *Stats, error) {
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

func (c ClientProtobufGroup) Find(ctx context.Context, request *pbgrpc.MultiGlobRequest) (*pbgrpc.MultiGlobResponse, *Stats, error) {
	stats := &Stats{}
	rewrite, _ := url.Parse("http://127.0.0.1/metrics/find/")

	var r pbgrpc.MultiGlobResponse
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

func (c ClientProtobufGroup) Info(ctx context.Context, request *pbgrpc.MultiMetricsInfoRequest) (*pbgrpc.MultiMetricsInfoResponse, *Stats, error) {
	return nil, nil, ErrNotImplementedYet
}

func (c ClientProtobufGroup) List(ctx context.Context, servers []string) (*pbgrpc.ListMetricsResponse, *Stats, error) {
	return nil, nil, ErrNotImplementedYet
}
func (c ClientProtobufGroup) Stats(ctx context.Context, servers []string) (*pbgrpc.MetricDetailsResponse, *Stats, error) {
	return nil, nil, ErrNotImplementedYet
}

func (c ClientProtobufGroup) ProbeTLDs(ctx context.Context) ([]string, error) {
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
