package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
)

type healthState struct {
	ready atomic.Bool
	check func(context.Context) error
}

type healthDependency interface {
	Ping(context.Context) error
}

func readinessCheck(dependencies ...healthDependency) func(context.Context) error {
	return func(ctx context.Context) error {
		for _, dependency := range dependencies {
			if dependency == nil {
				return errors.New("health dependency is required")
			}
			if err := dependency.Ping(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}

type healthServer struct {
	server *http.Server
	done   chan struct{}

	mu       sync.Mutex
	serveErr error
}

func startHealthServer(
	address string,
	state *healthState,
	ingressHandler http.Handler,
	adminHandler http.Handler,
) (*healthServer, error) {
	if state == nil {
		return nil, errors.New("health state is required")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen health server: %w", err)
	}
	health := &healthServer{
		server: &http.Server{
			Handler:           serviceHandler(state, ingressHandler, adminHandler),
			ReadHeaderTimeout: healthReadHeaderTimeout,
		},
		done: make(chan struct{}),
	}
	go func() {
		err := health.server.Serve(listener)
		health.mu.Lock()
		health.serveErr = err
		health.mu.Unlock()
		close(health.done)
	}()
	return health, nil
}

func serviceHandler(state *healthState, ingressHandler, adminHandler http.Handler) http.Handler {
	health := healthHandler(state)
	mux := http.NewServeMux()
	mux.Handle("/livez", health)
	mux.Handle("/readyz", health)
	if ingressHandler != nil {
		mux.Handle("/v1/", ingressHandler)
	}
	if adminHandler != nil {
		mux.Handle("/admin/v1/", adminHandler)
	}
	return mux
}

func (s *healthServer) shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	if err := s.server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return s.wait()
}

func (s *healthServer) wait() error {
	if s == nil {
		return nil
	}
	<-s.done
	s.mu.Lock()
	err := s.serveErr
	s.mu.Unlock()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func healthHandler(state *healthState) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !state.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		if state.check != nil {
			checkCtx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
			defer cancel()
			if state.check(checkCtx) != nil {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
