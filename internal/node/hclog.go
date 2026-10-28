package node

import (
	"context"
	"io"
	"log"
	"log/slog"
	"strings"

	"github.com/hashicorp/go-hclog"
)

// slogHCLog routes hashicorp/raft's hclog output into slog, so one structured
// stream holds both.
type slogHCLog struct {
	log   *slog.Logger
	name  string
	level hclog.Level
	args  []any
}

func newHCLog(l *slog.Logger) hclog.Logger {
	return &slogHCLog{log: l, name: "raft", level: hclog.Info}
}

var slogLevels = map[hclog.Level]slog.Level{
	hclog.Trace: slog.LevelDebug,
	hclog.Debug: slog.LevelDebug,
	hclog.Info:  slog.LevelInfo,
	hclog.Warn:  slog.LevelWarn,
	hclog.Error: slog.LevelError,
}

func (h *slogHCLog) Log(level hclog.Level, msg string, args ...any) {
	if level < h.level {
		return
	}
	attrs := append([]any{"component", h.name}, h.args...)
	h.log.Log(context.Background(), slogLevels[level], msg, append(attrs, args...)...)
}

func (h *slogHCLog) Trace(msg string, args ...any) { h.Log(hclog.Trace, msg, args...) }
func (h *slogHCLog) Debug(msg string, args ...any) { h.Log(hclog.Debug, msg, args...) }
func (h *slogHCLog) Info(msg string, args ...any)  { h.Log(hclog.Info, msg, args...) }
func (h *slogHCLog) Warn(msg string, args ...any)  { h.Log(hclog.Warn, msg, args...) }
func (h *slogHCLog) Error(msg string, args ...any) { h.Log(hclog.Error, msg, args...) }

func (h *slogHCLog) IsTrace() bool { return h.level <= hclog.Trace }
func (h *slogHCLog) IsDebug() bool { return h.level <= hclog.Debug }
func (h *slogHCLog) IsInfo() bool  { return h.level <= hclog.Info }
func (h *slogHCLog) IsWarn() bool  { return h.level <= hclog.Warn }
func (h *slogHCLog) IsError() bool { return h.level <= hclog.Error }

func (h *slogHCLog) ImpliedArgs() []any { return h.args }

func (h *slogHCLog) With(args ...any) hclog.Logger {
	c := *h
	c.args = append(append([]any{}, h.args...), args...)
	return &c
}

func (h *slogHCLog) Name() string { return h.name }

func (h *slogHCLog) Named(name string) hclog.Logger { return h.ResetNamed(h.name + "." + name) }

func (h *slogHCLog) ResetNamed(name string) hclog.Logger {
	c := *h
	c.name = name
	return &c
}

func (h *slogHCLog) SetLevel(level hclog.Level) { h.level = level }
func (h *slogHCLog) GetLevel() hclog.Level      { return h.level }

func (h *slogHCLog) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return log.New(h.StandardWriter(opts), "", 0)
}

func (h *slogHCLog) StandardWriter(*hclog.StandardLoggerOptions) io.Writer {
	return lineWriter{h}
}

type lineWriter struct{ h *slogHCLog }

func (w lineWriter) Write(p []byte) (int, error) {
	w.h.Info(strings.TrimSpace(string(p)))
	return len(p), nil
}
