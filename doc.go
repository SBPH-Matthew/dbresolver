// Package dbresolver provides a database connection resolver that wraps
// database/sql to intelligently route queries between primary (read-write)
// and replica (read-only) database connections.
//
// Features:
//   - Automatic query analysis to detect read vs write operations
//   - Context-based routing hints for read-your-writes consistency
//   - Cross-regional database support with weighted load balancing
//   - Background health checking with automatic failover
//   - Read-only transaction support on replicas
//   - Replica-to-primary fallback on connection errors
//
// Basic usage:
//
//	db, err := dbresolver.New(
//	    dbresolver.WithPrimaryDBs(dbresolver.NewNode(primaryDB)),
//	    dbresolver.WithReplicaDBs(dbresolver.NewNode(replicaDB)),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Writes go to primary automatically
//	db.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", "alice")
//
//	// Reads go to replica automatically
//	db.QueryContext(ctx, "SELECT * FROM users WHERE id = ?", 1)
//
//	// Force primary for read-your-writes consistency
//	ctx = dbresolver.UsePrimary(ctx)
//	db.QueryContext(ctx, "SELECT * FROM users WHERE id = ?", 1)
package dbresolver
