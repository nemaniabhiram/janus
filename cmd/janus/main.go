// Command janus runs one node of a raft-replicated lock service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/nemaniabhiram/janus/internal/api"
	"github.com/nemaniabhiram/janus/internal/config"
	"github.com/nemaniabhiram/janus/internal/node"
)

const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 15 * time.Second // longer than a forwarded raft apply
	idleTimeout       = 60 * time.Second
	shutdownTimeout   = 10 * time.Second

	raftPoolSize     = 3
	raftDialTimeout  = 10 * time.Second
	snapshotsRetain  = 2
	dataDirPerm      = 0o750
	raftDBFile       = "raft.db"
	snapshotsDirName = "snapshots"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("janus stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	nodeLog := log
	log = log.With("node_id", cfg.NodeID)
	log.Info("starting", "data_dir", cfg.DataDir, "peers", len(cfg.Peers))

	if err := os.MkdirAll(cfg.DataDir, dataDirPerm); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(cfg.DataDir, raftDBFile)})
	if err != nil {
		return fmt.Errorf("open raft store: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Error("close raft store", "error", err)
		}
	}()
	raftLog := node.NewRaftLogger(log)
	snaps, err := raft.NewFileSnapshotStoreWithLogger(filepath.Join(cfg.DataDir, snapshotsDirName), snapshotsRetain, raftLog)
	if err != nil {
		return fmt.Errorf("open snapshot store: %w", err)
	}
	advertise, err := net.ResolveTCPAddr("tcp", cfg.Self().RaftAddr())
	if err != nil {
		return fmt.Errorf("resolve raft address: %w", err)
	}
	bind := net.JoinHostPort("0.0.0.0", strconv.Itoa(config.RaftPort))
	trans, err := raft.NewTCPTransportWithLogger(bind, advertise, raftPoolSize, raftDialTimeout, raftLog)
	if err != nil {
		return fmt.Errorf("start raft transport: %w", err)
	}

	peers := make([]node.Peer, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peers = append(peers, node.Peer{ID: p.ID, RaftAddr: p.RaftAddr(), HTTPURL: "http://" + p.HTTPAddr()})
	}
	n, err := node.New(node.Options{
		ID:            cfg.NodeID,
		Peers:         peers,
		LogStore:      store,
		StableStore:   store,
		SnapshotStore: snaps,
		Transport:     trans,
		Clock:         time.Now,
		Logger:        nodeLog,
	})
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(config.HTTPPort)),
		Handler:           api.New(n, log),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	log.Info("serving", "http_port", config.HTTPPort, "raft_port", config.RaftPort)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serveErr:
		_ = n.Shutdown()
		return fmt.Errorf("serve http: %w", err)
	case <-ctx.Done():
	}
	return shutdown(log, srv, n)
}

// shutdown drains http first, then hands off leadership so the cluster skips
// an election timeout, then stops raft.
func shutdown(log *slog.Logger, srv *http.Server, n *node.Node) error {
	log.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	var errs []error
	if err := srv.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("drain http: %w", err))
	}
	if err := n.TransferLeadership(); err != nil {
		log.Warn("leadership transfer failed", "error", err)
	}
	if err := n.Shutdown(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
