package main

import (
	"context"
	"time"

	pb "github.com/go-graphite/carbonzipper/carbonzippergrpcpb"
	gpb "github.com/golang/protobuf/ptypes/empty"

	"fmt"
	"net"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var errNotImplementedYet = fmt.Errorf("feature not implemented yet")
var errNoDataInResponse = fmt.Errorf("no data in response")
var errEmptyRequest = fmt.Errorf("empty request")
var errUnknownError = fmt.Errorf("unknown error")

type GRPCServer struct {
	listener net.Listener
	server   *grpc.Server
}

func (srv *GRPCServer) serve() {
	srv.server.Serve(srv.listener)
}

func (srv GRPCServer) GetVersion(ctx context.Context, in *gpb.Empty) (*pb.ProtocolVersionResponse, error) {
	return &pb.ProtocolVersionResponse{
		Version: 1,
	}, nil
}

func (srv GRPCServer) FetchMetrics(ctx context.Context, in *pb.MultiFetchRequest) (*pb.MultiFetchResponse, error) {
	t0 := time.Now()
	memoryUsage := 0
	logger := zapwriter.Logger("grpc_find").With(
		zap.String("handler", "find"),
	)
	logger.Debug("got find request",
		zap.String("request", "grpc"),
	)

	Metrics.FindRequests.Add(1)

	grpcLogger := zapwriter.Logger("grpc_access").With(
		zap.String("handler", "render"),
		zap.String("format", "grpc"),
	)

	ctx, cancel := context.WithTimeout(ctx, config.Timeouts.Render)
	defer cancel()

	grpcLogger.Debug("got render request",
		zap.Any("request", in.Metrics),
	)

	Metrics.RenderRequests.Add(1)

	response, stats, err := config.zipper.FetchGRPC(ctx, in)
	sendStats(stats)
	if err != nil {
		grpcLogger.Error("failed to fetch data",
			zap.Int("memory_usage_bytes", memoryUsage),
			zap.Error(err),
			zap.Any("request", in),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		return nil, err
	}

	if len(response.Metrics) == 0 {
		return nil, errNoDataInResponse
	}

	grpcLogger.Info("request served",
		zap.Int("memory_usage_bytes", memoryUsage),
		zap.Duration("runtime_seconds", time.Since(t0)),
	)

	return response, nil
}

func (srv GRPCServer) FindMetrics(ctx context.Context, in *pb.MultiGlobRequest) (*pb.MultiGlobResponse, error) {
	t0 := time.Now()
	logger := zapwriter.Logger("grpc_find").With(
		zap.String("handler", "find"),
	)
	logger.Debug("got find request",
		zap.String("request", "grpc"),
	)

	Metrics.FindRequests.Add(1)

	grpcLogger := zapwriter.Logger("grpc_access").With(
		zap.String("handler", "find"),
		zap.String("format", "grpc"),
	)

	ctx, cancel := context.WithTimeout(ctx, config.Timeouts.Find)
	defer cancel()

	response, stats, err := config.zipper.FindGRPC(ctx, in)
	sendStats(stats)
	if err != nil {
		grpcLogger.Error("find error",
			zap.Strings("query", in.Metrics),
			zap.String("reason", err.Error()),
			zap.Duration("runtime_seconds", time.Since(t0)),
		)
		return nil, err
	}

	if len(response.Metrics) == 0 {
		return nil, errNoDataInResponse
	}
	grpcLogger.Info("request served",
		zap.Duration("runtime_seconds", time.Since(t0)),
	)

	return response, nil
}

func (srv GRPCServer) MetricsInfo(ctx context.Context, in *pb.MultiMetricsInfoRequest) (*pb.MultiMetricsInfoResponse, error) {
	return nil, errNotImplementedYet
}

func (srv GRPCServer) ListMetrics(ctx context.Context, in *gpb.Empty) (*pb.ListMetricsResponse, error) {
	return nil, errNotImplementedYet
}

func (srv GRPCServer) Stats(ctx context.Context, in *gpb.Empty) (*pb.MetricDetailsResponse, error) {
	return nil, errNotImplementedYet
}

func NewGRPCServer(address string) (*GRPCServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	srv := GRPCServer{
		listener: listener,
		server:   grpc.NewServer(),
	}

	pb.RegisterCarbonV1Server(srv.server, srv)

	go srv.serve()

	return &srv, nil
}
