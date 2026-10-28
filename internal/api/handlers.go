package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/nemaniabhiram/janus/internal/lock"
)

type leaseBody struct {
	LeaseID string `json:"lease_id"`
	TTL     int64  `json:"ttl_ms"`
}

type ttlBody struct {
	TTL int64 `json:"ttl_ms"`
}

type acquireBody struct {
	LeaseID string `json:"lease_id"`
}

type tokenBody struct {
	Token uint64 `json:"token"`
}

type lockBody struct {
	Name  string `json:"name"`
	Held  bool   `json:"held"`
	Token uint64 `json:"token,omitempty"`
}

type releaseBody struct {
	LeaseID string `json:"lease_id"`
	Token   uint64 `json:"token"`
}

type statusBody struct {
	NodeID       string `json:"node_id"`
	State        string `json:"state"`
	LeaderID     string `json:"leader_id"`
	Term         uint64 `json:"term"`
	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
	Leases       int    `json:"leases"`
	Locks        int    `json:"locks"`
}

func (s *server) createLease(w http.ResponseWriter, r *http.Request) {
	var req leaseBody
	if !s.decode(w, r, &req) {
		return
	}
	if !lock.ValidLeaseID(req.LeaseID) {
		s.writeError(w, r, errInvalidLeaseID)
		return
	}
	if !lock.ValidTTL(req.TTL) {
		s.writeError(w, r, errInvalidTTL)
		return
	}
	v, err := s.b.Apply(lock.Command{Type: lock.CmdCreateLease, LeaseID: req.LeaseID, TTL: req.TTL})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	res := v.(lock.LeaseResult)
	status := http.StatusOK
	if res.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, leaseBody{LeaseID: res.LeaseID, TTL: res.TTL})
}

func (s *server) renewLease(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !lock.ValidLeaseID(id) {
		s.writeError(w, r, errInvalidLeaseID)
		return
	}
	v, err := s.b.Apply(lock.Command{Type: lock.CmdRenewLease, LeaseID: id})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ttlBody{TTL: v.(lock.LeaseResult).TTL})
}

func (s *server) revokeLease(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !lock.ValidLeaseID(id) {
		s.writeError(w, r, errInvalidLeaseID)
		return
	}
	if _, err := s.b.Apply(lock.Command{Type: lock.CmdRevokeLease, LeaseID: id}); err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) acquire(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req acquireBody
	if !s.validName(w, r, name) || !s.decode(w, r, &req) {
		return
	}
	if !lock.ValidLeaseID(req.LeaseID) {
		s.writeError(w, r, errInvalidLeaseID)
		return
	}
	v, err := s.b.Apply(lock.Command{Type: lock.CmdAcquire, Name: name, LeaseID: req.LeaseID})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, tokenBody{Token: v.(lock.AcquireResult).Token})
}

func (s *server) release(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req releaseBody
	if !s.validName(w, r, name) || !s.decode(w, r, &req) {
		return
	}
	if !lock.ValidLeaseID(req.LeaseID) {
		s.writeError(w, r, errInvalidLeaseID)
		return
	}
	cmd := lock.Command{Type: lock.CmdRelease, Name: name, LeaseID: req.LeaseID, Token: req.Token}
	if _, err := s.b.Apply(cmd); err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (s *server) readLock(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !s.validName(w, r, name) {
		return
	}
	v, err := s.b.ReadLock(name)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, lockBody{Name: name, Held: v.Held, Token: v.Token})
}

// status answers from the local node, never the leader, and says so in its
// state and leader fields.
func (s *server) status(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusBody(s.b.Status()))
}

func (s *server) validName(w http.ResponseWriter, r *http.Request, name string) bool {
	if !lock.ValidName(name) {
		s.writeError(w, r, errInvalidName)
		return false
	}
	return true
}

func (s *server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(v)
	if err == nil {
		return true
	}
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		s.writeError(w, r, errBodyTooLarge)
	} else {
		s.writeError(w, r, errInvalidBody)
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
