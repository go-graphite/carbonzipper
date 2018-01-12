package zipper

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	pbgrpc "github.com/go-graphite/carbonzipper/carbonzippergrpcpb"
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
		limiter: limiter.NewServerLimiter(servers, concurencyLimit),
		logger:  zapwriter.Logger("protobufGroup").With(zap.String("name", groupName)),
	}
	return c, nil
}

func (c ClientProtobufGroup) pickServer() string {
	// We don't care about race conditions here
	counter := atomic.LoadUint64(&c.counter)
	atomic.AddUint64(&c.counter, 1)

	return c.servers[int(counter%uint64(len(c.servers)))]
}

func (c ClientProtobufGroup) Name() string {
	return c.groupName
}

func (c ClientProtobufGroup) Fetch(ctx context.Context, request *pbgrpc.MultiFetchRequest) (*pbgrpc.MultiFetchResponse, *Stats, error) {

	return nil, nil, nil
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

var errNotImplementedYet = errors.New("this feature is not implemented yet")

func (c ClientProtobufGroup) Find(ctx context.Context, request *pbgrpc.MultiGlobRequest) (*pbgrpc.MultiGlobResponse, *Stats, error) {
	server := c.pickServer()
	logger := c.logger.With(zap.String("server", server))

	logger.Error("not implemented yet",
		zap.Error(errNotImplementedYet),
	)

	return nil, nil, errNotImplementedYet
}
func (c ClientProtobufGroup) Info(ctx context.Context, request *pbgrpc.MultiMetricsInfoRequest) (*pbgrpc.MultiMetricsInfoResponse, *Stats, error) {
	server := c.pickServer()
	logger := c.logger.With(zap.String("server", server))

	logger.Error("not implemented yet",
		zap.Error(errNotImplementedYet),
	)

	return nil, nil, errNotImplementedYet
}

func (c ClientProtobufGroup) List(ctx context.Context, servers []string) (*pbgrpc.ListMetricsResponse, *Stats, error) {
	server := c.pickServer()
	logger := c.logger.With(zap.String("server", server))

	logger.Error("not implemented yet",
		zap.Error(errNotImplementedYet),
	)

	return nil, nil, errNotImplementedYet
}
func (c ClientProtobufGroup) Stats(ctx context.Context, servers []string) (*pbgrpc.MetricDetailsResponse, *Stats, error) {
	server := c.pickServer()
	logger := c.logger.With(zap.String("server", server))

	logger.Error("not implemented yet",
		zap.Error(errNotImplementedYet),
	)

	return nil, nil, errNotImplementedYet
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
	for _, v := range res.Metrics {
		tlds = append(tlds, v.Path)
	}
	return tlds, nil
}
