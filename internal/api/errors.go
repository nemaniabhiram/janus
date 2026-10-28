package api

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/nemaniabhiram/janus/internal/lock"
	"github.com/nemaniabhiram/janus/internal/node"
)

// apiError carries a stable code for programs and a message for humans,
// modeled on stripe's error shape.
type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string { return e.message }

var (
	errInvalidBody    = &apiError{http.StatusBadRequest, "invalid_body", "request body is not valid json"}
	errBodyTooLarge   = &apiError{http.StatusRequestEntityTooLarge, "body_too_large", fmt.Sprintf("request body is over %d bytes", maxBodyBytes)}
	errInvalidLeaseID = &apiError{http.StatusBadRequest, "invalid_lease_id", "lease_id must be 32 lowercase hex characters"}
	errInvalidTTL     = &apiError{http.StatusBadRequest, "invalid_ttl", fmt.Sprintf("ttl_ms must be between %d and %d", lock.MinLeaseTTL, lock.MaxLeaseTTL)}
	errInvalidName    = &apiError{http.StatusBadRequest, "invalid_name", fmt.Sprintf("lock name must be 1 to %d characters of [A-Za-z0-9._/-]", lock.MaxLockNameLen)}
	errNoLeader       = &apiError{http.StatusServiceUnavailable, "no_leader", "no ready leader, retry shortly"}
	errInternal       = &apiError{http.StatusInternalServerError, "internal", "internal error"}
)

var mapped = []struct {
	err error
	api *apiError
}{
	{lock.ErrLeaseNotFound, &apiError{http.StatusNotFound, "lease_not_found", "lease not found"}},
	{lock.ErrLeaseExpired, &apiError{http.StatusNotFound, "lease_expired", "lease expired"}},
	{lock.ErrLeaseExists, &apiError{http.StatusConflict, "lease_exists", "lease exists with a different ttl"}},
	{lock.ErrLockHeld, &apiError{http.StatusConflict, "lock_held", "lock is held by another lease"}},
	{lock.ErrNotHeld, &apiError{http.StatusConflict, "not_held", "lock is not held by this lease and token"}},
	{lock.ErrCapacity, &apiError{http.StatusTooManyRequests, "capacity_exceeded", "lease or lock capacity exceeded"}},
	{node.ErrNoLeader, errNoLeader},
	{node.ErrOutcomeUnknown, &apiError{http.StatusServiceUnavailable, "outcome_unknown", "the write may or may not have applied, retrying is safe"}},
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (s *server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	e, ok := errors.AsType[*apiError](err)
	if !ok {
		e = errInternal
		for _, m := range mapped {
			if errors.Is(err, m.err) {
				e = m.api
				break
			}
		}
	}
	if e == errInternal {
		s.logger(r).Error("request failed", "route", r.Pattern, "error", err)
	}
	if e == errNoLeader {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, e.status, errorBody{errorDetail{Code: e.code, Message: e.message}})
}
