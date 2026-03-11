package dbresolver

import (
	"context"
	"database/sql"
	"errors"
)

// Stmt is a prepared statement that may span multiple physical databases.
// It routes Exec operations to primaries and Query operations to replicas
// (with automatic fallback to primaries on connection errors).
type Stmt struct {
	// singleStmt is set when the Stmt wraps exactly one *sql.Stmt
	// (e.g. from Tx.PrepareContext or Conn.PrepareContext).
	singleStmt *sql.Stmt

	// Multi-statement fields (set by resolver.PrepareContext)
	primaryStmts map[*Node]*sql.Stmt
	replicaStmts map[*Node]*sql.Stmt
	stmtByDB     map[*sql.DB]*sql.Stmt
	queryType    QueryType
	primaryPool  *nodePool
	replicaPool  *nodePool

	allStmts []*sql.Stmt
}

func (s *Stmt) pickPrimaryStmt() (*sql.Stmt, error) {
	if s.singleStmt != nil {
		return s.singleStmt, nil
	}

	node, err := s.primaryPool.pick()
	if err != nil {
		return nil, err
	}
	if stmt, ok := s.primaryStmts[node]; ok {
		return stmt, nil
	}
	for _, stmt := range s.primaryStmts {
		return stmt, nil
	}
	return nil, ErrNoAvailableDB
}

func (s *Stmt) pickReplicaStmt() (stmt *sql.Stmt, fromReplica bool, err error) {
	if s.singleStmt != nil {
		return s.singleStmt, false, nil
	}

	if s.replicaPool != nil && len(s.replicaStmts) > 0 {
		node, pickErr := s.replicaPool.pick()
		if pickErr == nil {
			if st, ok := s.replicaStmts[node]; ok {
				return st, true, nil
			}
			// Node recovered after prepare; use any available replica stmt
			for _, st := range s.replicaStmts {
				return st, true, nil
			}
		}
	}

	st, err := s.pickPrimaryStmt()
	return st, false, err
}

// Close closes all underlying prepared statements.
func (s *Stmt) Close() error {
	var errs []error
	for _, st := range s.allStmts {
		if err := st.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ExecContext executes the prepared statement with the given arguments.
// Always routes to a primary database.
func (s *Stmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	st, err := s.pickPrimaryStmt()
	if err != nil {
		return nil, err
	}
	return st.ExecContext(ctx, args...)
}

// Exec executes the prepared statement with the given arguments.
func (s *Stmt) Exec(args ...any) (sql.Result, error) {
	return s.ExecContext(context.Background(), args...)
}

// QueryContext executes the prepared statement and returns rows.
// Routes to replicas for read queries and primaries for write queries,
// with automatic primary fallback on replica connection errors.
func (s *Stmt) QueryContext(ctx context.Context, args ...any) (*sql.Rows, error) {
	hint := routingHint(ctx)

	if hint == RoutingPrimary || s.queryType == QueryTypeWrite {
		st, err := s.pickPrimaryStmt()
		if err != nil {
			return nil, err
		}
		return st.QueryContext(ctx, args...)
	}

	st, fromReplica, err := s.pickReplicaStmt()
	if err != nil {
		return nil, err
	}

	rows, err := st.QueryContext(ctx, args...)
	if err != nil && fromReplica && isConnectionError(err) {
		primarySt, pErr := s.pickPrimaryStmt()
		if pErr == nil {
			return primarySt.QueryContext(ctx, args...)
		}
	}
	return rows, err
}

// Query executes the prepared statement and returns rows.
func (s *Stmt) Query(args ...any) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

// QueryRowContext executes the prepared statement and returns at most one row.
func (s *Stmt) QueryRowContext(ctx context.Context, args ...any) *sql.Row {
	hint := routingHint(ctx)

	if hint == RoutingPrimary || s.queryType == QueryTypeWrite {
		st, err := s.pickPrimaryStmt()
		if err != nil {
			return nil
		}
		return st.QueryRowContext(ctx, args...)
	}

	st, _, err := s.pickReplicaStmt()
	if err != nil {
		return nil
	}
	return st.QueryRowContext(ctx, args...)
}

// QueryRow executes the prepared statement and returns at most one row.
func (s *Stmt) QueryRow(args ...any) *sql.Row {
	return s.QueryRowContext(context.Background(), args...)
}
