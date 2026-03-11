package dbresolver

import (
	"context"
	"testing"
)

func TestUsePrimary(t *testing.T) {
	ctx := context.Background()
	ctx = UsePrimary(ctx)

	got := routingHint(ctx)
	if got != RoutingPrimary {
		t.Errorf("routingHint = %v, want RoutingPrimary", got)
	}
}

func TestUseReplica(t *testing.T) {
	ctx := context.Background()
	ctx = UseReplica(ctx)

	got := routingHint(ctx)
	if got != RoutingReplica {
		t.Errorf("routingHint = %v, want RoutingReplica", got)
	}
}

func TestRoutingHint_Default(t *testing.T) {
	ctx := context.Background()
	got := routingHint(ctx)
	if got != RoutingDefault {
		t.Errorf("routingHint = %v, want RoutingDefault", got)
	}
}

func TestRoutingHint_Override(t *testing.T) {
	ctx := context.Background()
	ctx = UsePrimary(ctx)
	ctx = UseReplica(ctx)

	got := routingHint(ctx)
	if got != RoutingReplica {
		t.Errorf("last hint should win: got %v, want RoutingReplica", got)
	}
}
