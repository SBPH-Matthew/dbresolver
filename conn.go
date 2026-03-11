package dbresolver

import (
	"context"
	"database/sql"
)

// Conn wraps a single *sql.Conn obtained from the resolver. All operations
// execute on the same underlying connection — no read/write routing occurs
// within a Conn.
type Conn struct {
	conn     *sql.Conn
	sourceDB *sql.DB
}

// Close returns the connection to the pool.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// BeginTx starts a transaction on this connection.
func (c *Conn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := c.conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx, sourceDB: c.sourceDB}, nil
}

// ExecContext executes a query on this connection.
func (c *Conn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(ctx, query, args...)
}

// PingContext verifies the connection is still alive.
func (c *Conn) PingContext(ctx context.Context) error {
	return c.conn.PingContext(ctx)
}

// PrepareContext creates a prepared statement on this connection.
func (c *Conn) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	sqlStmt, err := c.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &Stmt{
		singleStmt: sqlStmt,
		stmtByDB:   map[*sql.DB]*sql.Stmt{c.sourceDB: sqlStmt},
		allStmts:   []*sql.Stmt{sqlStmt},
	}, nil
}

// QueryContext executes a query that returns rows on this connection.
func (c *Conn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.conn.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a query that returns at most one row.
func (c *Conn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.conn.QueryRowContext(ctx, query, args...)
}

// Raw executes f with the underlying driver connection. The driverConn must
// not be used outside of f. Refer to sql.Conn.Raw for full documentation.
func (c *Conn) Raw(f func(driverConn any) error) error {
	return c.conn.Raw(f)
}
