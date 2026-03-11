package dbresolver

import (
	"context"
	"database/sql"
)

// Tx wraps a single *sql.Tx obtained from a primary or replica node.
// All operations execute on the transaction's underlying connection.
type Tx struct {
	tx       *sql.Tx
	sourceDB *sql.DB
}

// Commit commits the transaction.
func (t *Tx) Commit() error {
	return t.tx.Commit()
}

// Rollback aborts the transaction.
func (t *Tx) Rollback() error {
	return t.tx.Rollback()
}

// ExecContext executes a query within the transaction.
func (t *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Exec executes a query within the transaction.
func (t *Tx) Exec(query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows within the transaction.
func (t *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// Query executes a query that returns rows within the transaction.
func (t *Tx) Query(query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(context.Background(), query, args...)
}

// QueryRowContext executes a query that returns at most one row.
func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

// QueryRow executes a query that returns at most one row.
func (t *Tx) QueryRow(query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(context.Background(), query, args...)
}

// PrepareContext creates a prepared statement within the transaction.
func (t *Tx) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	sqlStmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &Stmt{
		singleStmt: sqlStmt,
		stmtByDB:   map[*sql.DB]*sql.Stmt{t.sourceDB: sqlStmt},
		allStmts:   []*sql.Stmt{sqlStmt},
	}, nil
}

// Prepare creates a prepared statement within the transaction.
func (t *Tx) Prepare(query string) (*Stmt, error) {
	return t.PrepareContext(context.Background(), query)
}

// StmtContext returns a transaction-specific prepared statement from an
// existing resolver Stmt. The returned Stmt operates within this transaction.
func (t *Tx) StmtContext(ctx context.Context, stmt *Stmt) *Stmt {
	var sqlStmt *sql.Stmt

	if stmt.stmtByDB != nil {
		sqlStmt = stmt.stmtByDB[t.sourceDB]
	}

	if sqlStmt == nil && stmt.singleStmt != nil {
		sqlStmt = stmt.singleStmt
	}

	if sqlStmt == nil {
		for _, s := range stmt.primaryStmts {
			sqlStmt = s
			break
		}
	}

	txStmt := t.tx.StmtContext(ctx, sqlStmt)
	return &Stmt{
		singleStmt: txStmt,
		stmtByDB:   map[*sql.DB]*sql.Stmt{t.sourceDB: txStmt},
		allStmts:   []*sql.Stmt{txStmt},
	}
}

// Stmt returns a transaction-specific prepared statement from an existing
// resolver Stmt.
func (t *Tx) Stmt(stmt *Stmt) *Stmt {
	return t.StmtContext(context.Background(), stmt)
}
