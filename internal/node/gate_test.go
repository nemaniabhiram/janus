package node

import (
	"context"
	"testing"
)

func TestGateOpenedForTermRefusesNextTerm(t *testing.T) {
	var g gate
	g.open(context.Background(), 4)

	if !g.admit(4) {
		t.Fatal("gate refused the term it was opened for")
	}
	if g.admit(5) {
		t.Error("gate admitted a request in a later term")
	}
}
