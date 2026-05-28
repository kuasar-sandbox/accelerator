// Package server implements gRPC service handlers for cache-ctl.
package server

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

// RegisterHealth registers the gRPC health service on the server.
func RegisterHealth(s *grpc.Server) *health.Server {
	hs := health.NewServer()
	hs.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
	healthgrpc.RegisterHealthServer(s, hs)
	return hs
}
