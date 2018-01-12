package fetcher

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-graphite/carbonzipper/limiter"
	cu "github.com/go-graphite/carbonzipper/util/apictx"
	util "github.com/go-graphite/carbonzipper/util/zipperctx"
	"go.uber.org/zap"

	"google.golang.org/grpc"
)

type GRPCGetter struct {
	//TODO: Migrate to golang.org/x/time/rate?
	limiter limiter.ServerLimiter

	timeouts Timeouts

	logger      *zap.Logger
	servers     []string
	connections map[string]*grpc.ClientConn
}

func NewGRPCGetter(servers []string, l int, logger *zap.Logger, timeouts Timeouts) *GRPCGetter {
	getter := &GRPCGetter{
		limiter:  limiter.NewServerLimiter(servers, l),
		timeouts: timeouts,
		servers:  servers,
		logger:   logger,
	}

	getter.connectToServers()
	go getter.checkConnections()

	return getter
}

func (g *GRPCGetter) connectToServers() {
	var err error
	for _, s := range g.servers {
		v, ok := g.connections[s]
		if (ok && v != nil) || !ok {
			g.connections[s], err = grpc.Dial(s, grpc.WithInsecure())
			if err != nil {
				g.logger.Error("failed to connect to server",
					zap.Error(err),
					zap.String("server", s),
				)
			}
		}
	}
}

func (g *GRPCGetter) checkConnections() {
	for {
		g.connectToServers()
		time.Sleep(200 * time.Millisecond)
	}
}

func (g *GRPCGetter) singleGet(ctx context.Context, uri, server string, ch chan<- ServerResponse, started chan<- struct{}) {
	logger := g.logger.With(zap.String("handler", "singleGet"))

	u, err := url.Parse(server + uri)
	if err != nil {
		logger.Error("error parsing uri",
			zap.String("uri", server+uri),
			zap.Error(err),
		)
		ch <- ServerResponse{server, nil}
		return
	}
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		logger.Error("failed to create new request",
			zap.Error(err),
		)
		ch <- ServerResponse{server, nil}
		return
	}
	req = req.WithContext(ctx)
	req = cu.MarshalCtx(ctx, util.MarshalCtx(ctx, req))

	logger = logger.With(zap.String("query", server+"/"+uri))
	g.limiter.Enter(server)
	started <- struct{}{}
	defer g.limiter.Leave(server)

	resp, err := g.storageClient.Do(req.WithContext(ctx))
	if err != nil {
		logger.Error("query error",
			zap.Error(err),
		)
		ch <- ServerResponse{server, nil}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode != http.StatusNotFound {
			// carbonsserver replies with Not Found if we request a
			// metric that it doesn't have -- makes sense
			logger.Error("bad response code",
				zap.Int("response_code", resp.StatusCode),
			)
		}
		ch <- ServerResponse{server, nil}
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error("error reading body",
			zap.Error(err),
		)
		ch <- ServerResponse{server, nil}
		return
	}

	ch <- ServerResponse{server, body}
}

func (g *GRPCGetter) multiGet(ctx context.Context, servers []string, uri string , fetchTimeout time.Duration) interface{} {
	logger := g.logger.With(zap.String("handler", "multiGet"))
	logger.Debug("querying servers",
		zap.Strings("servers", servers),
		zap.String("uri", uri),
	)

	// buffered channel so the goroutines don't block on send
	ch := make(chan ServerResponse, len(servers))
	startedch := make(chan struct{}, len(servers))

	fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()

	for _, server := range servers {
		go g.SingleGet(fetchCtx, uri, server, ch, startedch)
	}

	var response []ServerResponse

	var responses int
	var started int

GATHER:
	for {
		select {
		case <-startedch:
			started++
		case r := <-ch:
			responses++
			if r.Response != nil {
				response = append(response, r)
			}

			if responses == len(servers) {
				break GATHER
			}
		case <-ctx.Done():
			// Global fetchTimeout exceeded
			return nil
		case <-fetchCtx.Done():
			var servs []string
			for _, r := range response {
				servs = append(servs, r.Server)
			}

			var timeoutedServs []string
			for i := range servers {
				found := false
				for j := range servs {
					if servers[i] == servs[j] {
						found = true
						break
					}
				}
				if !found {
					timeoutedServs = append(timeoutedServs, servers[i])
				}
			}

			logger.Warn("fetchTimeout waiting for more responses",
				zap.String("uri", uri),
				zap.Strings("timeouted_servers", timeoutedServs),
				zap.Strings("answers_from_servers", servs),
			)
			stats.Timeouts++
			break GATHER
		}
	}

	return response
}

func (g *GRPCGetter) FetchPB(ctx context.Context, servers []string, query string, from, until int , timeout time.Duration) []ServerResponse {
	rewrite, _ := url.Parse("http://127.0.0.1/render/")
	v := url.Values{
		"target": []string{query},
		"format": []string{"protobuf"},
		"from":   []string{strconv.Itoa(from)},
		"until":  []string{strconv.Itoa(until)},
	}
	rewrite.RawQuery = v.Encode()

	return g.multiGet(ctx, servers, rewrite.RequestURI(), stats, timeout)
}

func (g *GRPCGetter) FindPB(ctx context.Context, servers []string, query string , timeout time.Duration) []ServerResponse {
	rewrite, _ := url.Parse("http://127.0.0.1/metrics/find/")
	v := url.Values{
		"query":  []string{query},
		"format": []string{"protobuf"},
	}
	rewrite.RawQuery = v.Encode()

	return g.multiGet(ctx, servers, rewrite.RequestURI(), stats, timeout)
}

func (g *GRPCGetter) InfoPB(ctx context.Context, servers []string, query string , timeout time.Duration) []ServerResponse {
	rewrite, _ := url.Parse("http://127.0.0.1/info/")
	v := url.Values{
		"target": []string{query},
		"format": []string{"protobuf"},
	}
	rewrite.RawQuery = v.Encode()

	return g.multiGet(ctx, servers, rewrite.RequestURI(), stats, timeout)
}

func (g *GRPCGetter) ListPB(ctx context.Context, servers []string, timeout time.Duration) []ServerResponse {
	rewrite, _ := url.Parse("http://127.0.0.1/metrics/list/")
	v := url.Values{
		"format": []string{"protobuf"},
	}
	rewrite.RawQuery = v.Encode()

	return g.multiGet(ctx, servers, rewrite.RequestURI(), stats, timeout)
}

func (g *GRPCGetter) StatsPB(ctx context.Context, servers []string , timeout time.Duration) []ServerResponse {
	rewrite, _ := url.Parse("http://127.0.0.1/metrics/stats/")
	v := url.Values{
		"format": []string{"protobuf"},
	}
	rewrite.RawQuery = v.Encode()

	return g.multiGet(ctx, servers, rewrite.RequestURI(), stats, timeout)
}
