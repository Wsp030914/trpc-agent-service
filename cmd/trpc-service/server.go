package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

const serviceReadHeaderTimeout = 5 * time.Second

type serviceServer struct {
	server *http.Server
	done   chan struct{}

	mu       sync.Mutex
	serveErr error
}

func startServiceServer(address string, ingressHandler, adminHandler http.Handler) (*serviceServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen service server: %w", err)
	}
	service := &serviceServer{
		server: &http.Server{
			Handler:           serviceHandler(ingressHandler, adminHandler),
			ReadHeaderTimeout: serviceReadHeaderTimeout,
		},
		done: make(chan struct{}),
	}
	go func() {
		err := service.server.Serve(listener)
		service.mu.Lock()
		service.serveErr = err
		service.mu.Unlock()
		close(service.done)
	}()
	return service, nil
}

func serviceHandler(ingressHandler, adminHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	if ingressHandler != nil {
		mux.Handle("/v1/", ingressHandler)
	}
	if adminHandler != nil {
		mux.Handle("/admin/v1/", adminHandler)
	}
	return mux
}

func (s *serviceServer) shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	if err := s.server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return s.wait()
}

func (s *serviceServer) wait() error {
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
