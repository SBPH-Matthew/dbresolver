package dbresolver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DB is the primary interface for the database resolver. It mirrors the
// standard database/sql.DB API while transparently routing queries to the
// appropriate primary or replica database.
type DB interface {
	// Standard database/sql methods

	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*Stmt, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)
	ConnContext(ctx context.Context) (*Conn, error)
	PingContext(ctx context.Context) error

	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Prepare(query string) (*Stmt, error)
	Begin() (*Tx, error)
	Ping() error

	// Connection pool settings (applied to all nodes)

	SetConnMaxIdleTime(d time.Duration)
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)

	// Extended methods

	// BeginReadOnlyTx starts a read-only transaction, preferring replicas.
	BeginReadOnlyTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)

	// Primary returns a single primary *sql.DB selected via load balancer.
	Primary() *sql.DB

	// Replica returns a single replica *sql.DB selected via load balancer.
	// Falls back to a primary if no replicas are available.
	Replica() *sql.DB

	// Primaries returns all configured primary *sql.DB instances.
	Primaries() []*sql.DB

	// Replicas returns all configured replica *sql.DB instances.
	Replicas() []*sql.DB

	// Stats returns connection pool statistics for every node.
	Stats() []DBStats

	// Close stops the health checker and closes all underlying databases.
	Close() error
}

// nodePool manages a group of database nodes with load-balanced selection
// and optional health filtering.
type nodePool struct {
	nodes   []*Node
	lb      LoadBalancer
	checker *healthChecker
}

func (p *nodePool) pick() (*Node, error) {
	if p == nil || len(p.nodes) == 0 {
		return nil, ErrNoAvailableDB
	}

	healthy := p.healthyNodes()
	if len(healthy) == 0 {
		return nil, ErrNoAvailableDB
	}

	node := p.lb.Pick(healthy)
	if node == nil {
		return nil, ErrNoAvailableDB
	}
	return node, nil
}

func (p *nodePool) healthyNodes() []*Node {
	if p.checker == nil {
		return p.nodes
	}
	healthy := make([]*Node, 0, len(p.nodes))
	for _, n := range p.nodes {
		if p.checker.isHealthy(n) {
			healthy = append(healthy, n)
		}
	}
	return healthy
}

// resolver is the concrete implementation of the DB interface.
type resolver struct {
	primaryPool   *nodePool
	replicaPool   *nodePool
	analyzer      QueryAnalyzer
	healthChecker *healthChecker
	closed        atomic.Bool
}

var _ DB = (*resolver)(nil)

// --- internal routing helpers ---

func (r *resolver) pickPrimary() (*sql.DB, error) {
	node, err := r.primaryPool.pick()
	if err != nil {
		return nil, err
	}
	return node.db, nil
}

func (r *resolver) pickReplica() (*sql.DB, error) {
	if r.replicaPool == nil {
		return nil, ErrNoAvailableDB
	}
	node, err := r.replicaPool.pick()
	if err != nil {
		return nil, err
	}
	return node.db, nil
}

// pickDBForQuery selects the database for a query based on context hints
// and query analysis. Returns the selected database and whether a replica
// was chosen (for fallback decisions).
func (r *resolver) pickDBForQuery(ctx context.Context, query string) (db *sql.DB, fromReplica bool, err error) {
	hint := routingHint(ctx)

	switch hint {
	case RoutingPrimary:
		db, err = r.pickPrimary()
		return db, false, err
	case RoutingReplica:
		db, err = r.pickReplica()
		if err == nil {
			return db, true, nil
		}
		db, err = r.pickPrimary()
		return db, false, err
	}

	qt := r.analyzer.Analyze(query)
	if qt == QueryTypeWrite {
		db, err = r.pickPrimary()
		return db, false, err
	}

	db, err = r.pickReplica()
	if err == nil {
		return db, true, nil
	}
	db, err = r.pickPrimary()
	return db, false, err
}

func (r *resolver) allNodes() []*Node {
	size := len(r.primaryPool.nodes)
	if r.replicaPool != nil {
		size += len(r.replicaPool.nodes)
	}
	nodes := make([]*Node, 0, size)
	nodes = append(nodes, r.primaryPool.nodes...)
	if r.replicaPool != nil {
		nodes = append(nodes, r.replicaPool.nodes...)
	}
	return nodes
}

// --- DB interface: Exec ---

func (r *resolver) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}
	db, err := r.pickPrimary()
	if err != nil {
		return nil, err
	}
	return db.ExecContext(ctx, query, args...)
}

func (r *resolver) Exec(query string, args ...any) (sql.Result, error) {
	return r.ExecContext(context.Background(), query, args...)
}

// --- DB interface: Query ---

func (r *resolver) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}

	db, fromReplica, err := r.pickDBForQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil && fromReplica && isConnectionError(err) {
		primaryDB, pErr := r.pickPrimary()
		if pErr == nil && primaryDB != db {
			return primaryDB.QueryContext(ctx, query, args...)
		}
	}
	return rows, err
}

func (r *resolver) Query(query string, args ...any) (*sql.Rows, error) {
	return r.QueryContext(context.Background(), query, args...)
}

func (r *resolver) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	db, _, _ := r.pickDBForQuery(ctx, query)
	if db == nil {
		db = r.primaryPool.nodes[0].db
	}
	return db.QueryRowContext(ctx, query, args...)
}

func (r *resolver) QueryRow(query string, args ...any) *sql.Row {
	return r.QueryRowContext(context.Background(), query, args...)
}

// --- DB interface: Prepare ---

func (r *resolver) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}

	qt := r.analyzer.Analyze(query)

	stmt := &Stmt{
		primaryStmts: make(map[*Node]*sql.Stmt),
		replicaStmts: make(map[*Node]*sql.Stmt),
		stmtByDB:     make(map[*sql.DB]*sql.Stmt),
		queryType:    qt,
		primaryPool:  r.primaryPool,
		replicaPool:  r.replicaPool,
	}

	for _, node := range r.primaryPool.nodes {
		s, err := node.db.PrepareContext(ctx, query)
		if err != nil {
			stmt.Close()
			return nil, fmt.Errorf("dbresolver: preparing on primary %q: %w", node.name, err)
		}
		stmt.primaryStmts[node] = s
		stmt.stmtByDB[node.db] = s
		stmt.allStmts = append(stmt.allStmts, s)
	}

	if r.replicaPool != nil {
		for _, node := range r.replicaPool.nodes {
			s, err := node.db.PrepareContext(ctx, query)
			if err != nil {
				continue
			}
			stmt.replicaStmts[node] = s
			stmt.stmtByDB[node.db] = s
			stmt.allStmts = append(stmt.allStmts, s)
		}
	}

	return stmt, nil
}

func (r *resolver) Prepare(query string) (*Stmt, error) {
	return r.PrepareContext(context.Background(), query)
}

// --- DB interface: Transactions ---

func (r *resolver) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}

	node, err := r.primaryPool.pick()
	if err != nil {
		return nil, err
	}

	tx, err := node.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx, sourceDB: node.db}, nil
}

func (r *resolver) Begin() (*Tx, error) {
	return r.BeginTx(context.Background(), nil)
}

func (r *resolver) BeginReadOnlyTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}

	txOpts := &sql.TxOptions{ReadOnly: true}
	if opts != nil {
		txOpts.Isolation = opts.Isolation
	}

	if r.replicaPool != nil {
		node, err := r.replicaPool.pick()
		if err == nil {
			tx, txErr := node.db.BeginTx(ctx, txOpts)
			if txErr == nil {
				return &Tx{tx: tx, sourceDB: node.db}, nil
			}
			if !isConnectionError(txErr) {
				return nil, txErr
			}
		}
	}

	node, err := r.primaryPool.pick()
	if err != nil {
		return nil, err
	}
	tx, err := node.db.BeginTx(ctx, txOpts)
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx, sourceDB: node.db}, nil
}

// --- DB interface: Connection ---

func (r *resolver) ConnContext(ctx context.Context) (*Conn, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}

	var node *Node
	var err error

	hint := routingHint(ctx)
	switch hint {
	case RoutingReplica:
		if r.replicaPool != nil {
			node, err = r.replicaPool.pick()
		}
		if node == nil {
			node, err = r.primaryPool.pick()
		}
	default:
		node, err = r.primaryPool.pick()
	}

	if err != nil {
		return nil, err
	}

	conn, err := node.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn, sourceDB: node.db}, nil
}

// --- DB interface: Ping ---

func (r *resolver) PingContext(ctx context.Context) error {
	if r.closed.Load() {
		return ErrClosed
	}

	nodes := r.allNodes()
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, node *Node) {
			defer wg.Done()
			errs[idx] = node.db.PingContext(ctx)
		}(i, n)
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (r *resolver) Ping() error {
	return r.PingContext(context.Background())
}

// --- DB interface: Pool settings ---

func (r *resolver) SetConnMaxIdleTime(d time.Duration) {
	for _, node := range r.allNodes() {
		node.db.SetConnMaxIdleTime(d)
	}
}

func (r *resolver) SetConnMaxLifetime(d time.Duration) {
	for _, node := range r.allNodes() {
		node.db.SetConnMaxLifetime(d)
	}
}

func (r *resolver) SetMaxIdleConns(n int) {
	for _, node := range r.allNodes() {
		node.db.SetMaxIdleConns(n)
	}
}

func (r *resolver) SetMaxOpenConns(n int) {
	for _, node := range r.allNodes() {
		node.db.SetMaxOpenConns(n)
	}
}

// --- DB interface: Accessors ---

func (r *resolver) Primary() *sql.DB {
	node, _ := r.primaryPool.pick()
	if node != nil {
		return node.db
	}
	if len(r.primaryPool.nodes) > 0 {
		return r.primaryPool.nodes[0].db
	}
	return nil
}

func (r *resolver) Replica() *sql.DB {
	if r.replicaPool != nil {
		node, _ := r.replicaPool.pick()
		if node != nil {
			return node.db
		}
	}
	return r.Primary()
}

func (r *resolver) Primaries() []*sql.DB {
	dbs := make([]*sql.DB, len(r.primaryPool.nodes))
	for i, n := range r.primaryPool.nodes {
		dbs[i] = n.db
	}
	return dbs
}

func (r *resolver) Replicas() []*sql.DB {
	if r.replicaPool == nil {
		return nil
	}
	dbs := make([]*sql.DB, len(r.replicaPool.nodes))
	for i, n := range r.replicaPool.nodes {
		dbs[i] = n.db
	}
	return dbs
}

func (r *resolver) Stats() []DBStats {
	var stats []DBStats
	for _, node := range r.primaryPool.nodes {
		healthy := r.healthChecker == nil || r.healthChecker.isHealthy(node)
		stats = append(stats, DBStats{
			Name:    node.name,
			Role:    RolePrimary,
			Region:  node.region,
			Healthy: healthy,
			Stats:   node.db.Stats(),
		})
	}
	if r.replicaPool != nil {
		for _, node := range r.replicaPool.nodes {
			healthy := r.healthChecker == nil || r.healthChecker.isHealthy(node)
			stats = append(stats, DBStats{
				Name:    node.name,
				Role:    RoleReplica,
				Region:  node.region,
				Healthy: healthy,
				Stats:   node.db.Stats(),
			})
		}
	}
	return stats
}

// --- DB interface: Close ---

func (r *resolver) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}

	if r.healthChecker != nil {
		r.healthChecker.stop()
	}

	nodes := r.allNodes()
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, node *Node) {
			defer wg.Done()
			errs[idx] = node.db.Close()
		}(i, n)
	}
	wg.Wait()
	return errors.Join(errs...)
}
