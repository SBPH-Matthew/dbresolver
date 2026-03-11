package dbresolver

import "context"

type contextKey int

const (
	routingHintKey contextKey = iota
)

// RoutingHint determines which database pool a query should target.
type RoutingHint int

const (
	// RoutingDefault lets the resolver decide based on query analysis.
	RoutingDefault RoutingHint = iota

	// RoutingPrimary forces queries to use a primary (read-write) database.
	RoutingPrimary

	// RoutingReplica forces queries to use a replica (read-only) database.
	RoutingReplica
)

// UsePrimary returns a context that forces the resolver to route queries
// to a primary database. This is essential for read-your-writes consistency
// when you need to read data that was just written and may not yet have
// replicated to replicas.
func UsePrimary(ctx context.Context) context.Context {
	return context.WithValue(ctx, routingHintKey, RoutingPrimary)
}

// UseReplica returns a context that forces the resolver to route queries
// to a replica database, even if the query would normally go to a primary.
func UseReplica(ctx context.Context) context.Context {
	return context.WithValue(ctx, routingHintKey, RoutingReplica)
}

func routingHint(ctx context.Context) RoutingHint {
	if hint, ok := ctx.Value(routingHintKey).(RoutingHint); ok {
		return hint
	}
	return RoutingDefault
}
