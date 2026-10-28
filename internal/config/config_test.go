package config

import (
	"strings"
	"testing"
)

func TestLoadRejectsPeerEntryWithoutEquals(t *testing.T) {
	env := map[string]string{
		EnvNodeID:  "n1",
		EnvDataDir: "data",
		EnvPeers:   "n1=janus-1,n2",
	}

	_, err := Load(func(k string) string { return env[k] })

	if err == nil || !strings.Contains(err.Error(), EnvPeers) {
		t.Fatalf("want an error naming %s, got %v", EnvPeers, err)
	}
}
