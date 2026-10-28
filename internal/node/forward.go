package node

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/hashicorp/raft"
)

const (
	forwardedHeader = "X-Janus-Forwarded"
	requestIDHeader = "X-Request-Id"
	// forwardTimeout leaves the leader room to finish its own raft apply.
	forwardTimeout = applyTimeout + time.Second
	// forwardIdleConns keeps connections to the leader open under load. Go's
	// default of two per host makes most forwards dial a new connection.
	forwardIdleConns = 256
	// forwardDialTimeout fails fast on a dead leader. Peers share a private
	// network, so a dial this slow means the leader is gone, and the client
	// gets a retryable no_leader instead of hanging through the election.
	forwardDialTimeout = time.Second
)

// Route forwards r to the leader unless this node can serve it. It returns
// true when it has answered the request, either through the leader or with an
// error passed to onErr, so the caller's handler must not run.
func (n *Node) Route(w http.ResponseWriter, r *http.Request, onErr func(http.ResponseWriter, *http.Request, error)) bool {
	if n.ready() {
		return false
	}
	// A forwarded request that lands on a non-leader means leadership moved
	// mid-flight. Answering instead of forwarding again keeps requests from
	// looping.
	if n.raft.State() == raft.Leader || r.Header.Get(forwardedHeader) != "" {
		onErr(w, r, ErrNoLeader)
		return true
	}
	_, leaderID := n.raft.LeaderWithID()
	target, ok := n.httpURLs[leaderID]
	if !ok {
		onErr(w, r, ErrNoLeader)
		return true
	}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Header.Set(forwardedHeader, "1")
		},
		Transport: n.forward,
		// Go's default handler writes a bare 502 that hides whether the leader
		// saw the write.
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			n.log.Warn("forward to leader failed",
				"leader_id", leaderID, "request_id", r.Header.Get(requestIDHeader), "error", err)
			onErr(w, r, forwardError(err))
		},
	}
	proxy.ServeHTTP(w, r)
	return true
}

// forwardError tells a failed dial, where nothing reached the leader, apart
// from failures that may have happened after the leader read the request.
func forwardError(err error) error {
	if opErr, ok := errors.AsType[*net.OpError](err); ok && opErr.Op == "dial" {
		return fmt.Errorf("forward to leader: %w: %w", ErrNoLeader, err)
	}
	return fmt.Errorf("forward to leader: %w: %w", ErrOutcomeUnknown, err)
}

func newForwardTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = (&net.Dialer{Timeout: forwardDialTimeout}).DialContext
	t.ResponseHeaderTimeout = forwardTimeout
	t.MaxIdleConns = forwardIdleConns
	t.MaxIdleConnsPerHost = forwardIdleConns
	return t
}

func parseHTTPURLs(peers []Peer) (map[raft.ServerID]*url.URL, error) {
	urls := make(map[raft.ServerID]*url.URL, len(peers))
	for _, p := range peers {
		u, err := url.Parse(p.HTTPURL)
		if err != nil {
			return nil, fmt.Errorf("peer %s http url: %w", p.ID, err)
		}
		urls[raft.ServerID(p.ID)] = u
	}
	return urls, nil
}
