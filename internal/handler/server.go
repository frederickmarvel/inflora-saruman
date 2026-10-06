package handler

import (
	"context"
	"errors"
	"net/http"
)

type Server struct{ HTTP *http.Server }

func (s *Server) Name() string { return "http" }

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() { errCh <- s.HTTP.ListenAndServe() }()
	select {
	case <-ctx.Done():
		if err := s.HTTP.Shutdown(context.Background()); err != nil {
			return err
		}
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
