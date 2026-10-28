// Package client is a thin go client for the janus http api. It does no
// retries and no keepalive: callers retry on 503 and renew on their own ticker.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const leaseIDBytes = 16 // 128 bits

// Stable error codes returned by the api.
const (
	CodeLeaseNotFound  = "lease_not_found"
	CodeLeaseExpired   = "lease_expired"
	CodeLeaseExists    = "lease_exists"
	CodeLockHeld       = "lock_held"
	CodeNotHeld        = "not_held"
	CodeCapacity       = "capacity_exceeded"
	CodeNoLeader       = "no_leader"
	CodeOutcomeUnknown = "outcome_unknown"
)

// APIError is a non-2xx answer from the api.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("janus: %d %s: %s", e.Status, e.Code, e.Message)
}

// Client talks to one janus node. Any node accepts any request.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// New returns a client for the node at baseURL, e.g. http://localhost:8081.
func New(baseURL string) *Client {
	return &Client{BaseURL: baseURL, HTTP: http.DefaultClient}
}

// NewLeaseID returns 128 random bits, hex-encoded. The lease id is a bearer
// capability, so it must come from crypto/rand.
func NewLeaseID() (string, error) {
	b := make([]byte, leaseIDBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate lease id: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// CreateLease creates the lease, or finds it if a retry already did. created
// is false when the lease already existed.
func (c *Client) CreateLease(ctx context.Context, leaseID string, ttl time.Duration) (created bool, err error) {
	body := map[string]any{"lease_id": leaseID, "ttl_ms": ttl.Milliseconds()}
	status, err := c.do(ctx, http.MethodPost, "/v1/leases", body, nil)
	return status == http.StatusCreated, err
}

// RenewLease pushes the lease's deadline out by its ttl.
func (c *Client) RenewLease(ctx context.Context, leaseID string) error {
	_, err := c.do(ctx, http.MethodPost, "/v1/leases/"+url.PathEscape(leaseID)+"/renew", nil, nil)
	return err
}

// RevokeLease deletes the lease and releases its locks. A missing lease is not
// an error.
func (c *Client) RevokeLease(ctx context.Context, leaseID string) error {
	_, err := c.do(ctx, http.MethodDelete, "/v1/leases/"+url.PathEscape(leaseID), nil, nil)
	return err
}

// Acquire tries to take the lock and returns its fencing token. A retry by
// the holder returns the same token.
func (c *Client) Acquire(ctx context.Context, name, leaseID string) (uint64, error) {
	var out struct {
		Token uint64 `json:"token"`
	}
	_, err := c.do(ctx, http.MethodPut, "/v1/locks/"+url.PathEscape(name), map[string]any{"lease_id": leaseID}, &out)
	return out.Token, err
}

// Release frees the lock if leaseID still holds it with token.
func (c *Client) Release(ctx context.Context, name, leaseID string, token uint64) error {
	body := map[string]any{"lease_id": leaseID, "token": token}
	_, err := c.do(ctx, http.MethodPost, "/v1/locks/"+url.PathEscape(name)+"/release", body, nil)
	return err
}

// LockState is a linearizable view of one lock.
type LockState struct {
	Name  string `json:"name"`
	Held  bool   `json:"held"`
	Token uint64 `json:"token"`
}

// Lock reads a lock through the leader.
func (c *Client) Lock(ctx context.Context, name string) (LockState, error) {
	var out LockState
	_, err := c.do(ctx, http.MethodGet, "/v1/locks/"+url.PathEscape(name), nil, &out)
	return out, err
}

// Status is one node's local view of the cluster.
type Status struct {
	NodeID       string `json:"node_id"`
	State        string `json:"state"`
	LeaderID     string `json:"leader_id"`
	Term         uint64 `json:"term"`
	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
	Leases       int    `json:"leases"`
	Locks        int    `json:"locks"`
}

// Status returns the node's local view, without consulting the leader.
func (c *Client) Status(ctx context.Context) (Status, error) {
	var out Status
	_, err := c.do(ctx, http.MethodGet, "/v1/status", nil, &out)
	return out, err
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) (int, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return 0, fmt.Errorf("encode %s %s: %w", method, path, err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, &buf)
	if err != nil {
		return 0, fmt.Errorf("build %s %s: %w", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.HTTP.Do(req)
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", method, path, err)
	}
	// Reading to EOF lets the transport reuse the connection, even for calls
	// that ignore the body.
	defer func() {
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
	}()
	if res.StatusCode >= http.StatusBadRequest {
		var e struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.NewDecoder(res.Body).Decode(&e)
		return res.StatusCode, &APIError{Status: res.StatusCode, Code: e.Error.Code, Message: e.Error.Message}
	}
	if out != nil {
		if err := json.NewDecoder(res.Body).Decode(out); err != nil {
			return res.StatusCode, fmt.Errorf("decode %s %s: %w", method, path, err)
		}
	}
	return res.StatusCode, nil
}
