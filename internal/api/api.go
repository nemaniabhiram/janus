// Package api serves the janus http api: routing, json decoding, input
// validation and mapping errors to status codes. It never touches raft or
// the clock.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"

	"github.com/nemaniabhiram/janus/internal/lock"
	"github.com/nemaniabhiram/janus/internal/node"
)

const (
	maxBodyBytes    = 4 << 10
	requestIDHeader = "X-Request-Id"
	requestIDBytes  = 8
)

// Backend is the node the api serves from.
type Backend interface {
	Apply(cmd lock.Command) (any, error)
	Status() node.Status
}

type server struct {
	b   Backend
	log *slog.Logger
}

// New returns the http handler for all /v1 routes.
func New(b Backend, log *slog.Logger) http.Handler {
	s := &server{b: b, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/leases", s.createLease)
	mux.HandleFunc("POST /v1/leases/{id}/renew", s.renewLease)
	mux.HandleFunc("DELETE /v1/leases/{id}", s.revokeLease)
	mux.HandleFunc("PUT /v1/locks/{name}", s.acquire)
	mux.HandleFunc("POST /v1/locks/{name}/release", s.release)
	mux.HandleFunc("GET /v1/status", s.status)
	return withRequestID(mux)
}

type ctxKey struct{}

// withRequestID keeps an incoming request id, or makes one, and returns it in
// X-Request-Id. Setting it on the request means a forwarded request carries it.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			b := make([]byte, requestIDBytes)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
			r.Header.Set(requestIDHeader, id)
		}
		w.Header().Set(requestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

func (s *server) logger(r *http.Request) *slog.Logger {
	id, _ := r.Context().Value(ctxKey{}).(string)
	return s.log.With("request_id", id)
}
