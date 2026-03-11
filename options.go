package dbresolver

import (
	"database/sql"
	"time"
)

// Node represents a single database connection with associated metadata
// for load balancing and cross-region routing.
type Node struct {
	db     *sql.DB
	name   string
	weight int
	region string
}

// NodeConfig provides optional metadata when creating a Node.
type NodeConfig struct {
	// Name is a human-readable identifier for this node (e.g., "primary-us-east-1").
	Name string

	// Weight controls traffic distribution with WeightedRoundRobinLB.
	// Higher weight means more traffic. Defaults to 1 if unset or zero.
	Weight int

	// Region identifies the geographic region of this node (e.g., "us-east-1").
	Region string
}

// NewNode wraps an *sql.DB with optional metadata for the resolver.
func NewNode(db *sql.DB, cfg ...NodeConfig) *Node {
	n := &Node{
		db:     db,
		weight: 1,
	}
	if len(cfg) > 0 {
		c := cfg[0]
		n.name = c.Name
		if c.Weight > 0 {
			n.weight = c.Weight
		}
		n.region = c.Region
	}
	return n
}

// DB returns the underlying *sql.DB.
func (n *Node) DB() *sql.DB { return n.db }

// Name returns the node's human-readable name.
func (n *Node) Name() string { return n.name }

// Weight returns the node's load balancing weight.
func (n *Node) Weight() int { return n.weight }

// Region returns the node's geographic region identifier.
func (n *Node) Region() string { return n.region }

// PoolConfig specifies connection pool settings applied to a group of databases.
type PoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// healthCheckConfig holds settings for the background health checker.
type healthCheckConfig struct {
	enable           bool
	interval         time.Duration
	failThreshold    int
	recoverThreshold int
}

// option aggregates all configuration for building a resolver.
type option struct {
	primaries     []*Node
	replicas      []*Node
	lbPolicy      LoadBalancerPolicy
	customLB      LoadBalancer
	queryAnalyzer QueryAnalyzer
	healthCheck   healthCheckConfig
	primaryPool   *PoolConfig
	replicaPool   *PoolConfig
}

// OptionFunc configures the resolver during construction.
type OptionFunc func(*option)

// WithPrimaryDBs registers one or more primary (read-write) database nodes.
// At least one primary is required.
func WithPrimaryDBs(nodes ...*Node) OptionFunc {
	return func(o *option) {
		o.primaries = append(o.primaries, nodes...)
	}
}

// WithReplicaDBs registers one or more replica (read-only) database nodes.
// If no replicas are configured, all queries go to primaries.
func WithReplicaDBs(nodes ...*Node) OptionFunc {
	return func(o *option) {
		o.replicas = append(o.replicas, nodes...)
	}
}

// WithLoadBalancer sets the load balancing strategy for node selection.
// Defaults to RoundRobinLB.
func WithLoadBalancer(policy LoadBalancerPolicy) OptionFunc {
	return func(o *option) {
		o.lbPolicy = policy
	}
}

// WithCustomLoadBalancer provides a user-defined load balancer implementation.
// When set, this takes precedence over WithLoadBalancer.
func WithCustomLoadBalancer(lb LoadBalancer) OptionFunc {
	return func(o *option) {
		o.customLB = lb
	}
}

// WithQueryAnalyzer provides a custom query analyzer for read/write detection.
// Defaults to the built-in analyzer that detects DML, DDL, locking reads,
// RETURNING clauses, and writable CTEs.
func WithQueryAnalyzer(qa QueryAnalyzer) OptionFunc {
	return func(o *option) {
		o.queryAnalyzer = qa
	}
}

// WithHealthCheck enables periodic background health checking.
//   - interval: how often to ping each node (e.g. 5*time.Second)
//   - failThreshold: consecutive failures before marking a node unhealthy
//   - recoverThreshold: consecutive successes before marking a node healthy again
func WithHealthCheck(interval time.Duration, failThreshold, recoverThreshold int) OptionFunc {
	return func(o *option) {
		o.healthCheck = healthCheckConfig{
			enable:           true,
			interval:         interval,
			failThreshold:    failThreshold,
			recoverThreshold: recoverThreshold,
		}
	}
}

// WithPrimaryPoolConfig applies connection pool settings to all primary databases.
func WithPrimaryPoolConfig(cfg PoolConfig) OptionFunc {
	return func(o *option) {
		o.primaryPool = &cfg
	}
}

// WithReplicaPoolConfig applies connection pool settings to all replica databases.
func WithReplicaPoolConfig(cfg PoolConfig) OptionFunc {
	return func(o *option) {
		o.replicaPool = &cfg
	}
}
