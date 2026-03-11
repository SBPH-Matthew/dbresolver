# dbresolver

A Go database connection resolver that wraps `database/sql` to intelligently route queries between primary (read-write) and replica (read-only) databases.

Built for production workloads with master-slave replication, cross-region database topologies, and applications requiring read-your-writes consistency.

## Features

- **Automatic query routing** — Detects INSERT, UPDATE, DELETE, DDL, SELECT FOR UPDATE, writable CTEs, and more
- **Context-based hints** — Force primary reads for read-your-writes consistency after writes
- **Cross-region support** — Weighted load balancing to prefer local databases over remote ones
- **Health checking** — Background pings with configurable failure/recovery thresholds
- **Replica fallback** — Automatic fallback to primary when replicas are unavailable or have connection errors
- **Read-only transactions** — Route read-only transactions to replicas
- **Zero external dependencies** — Only uses the Go standard library at runtime

## Installation

```bash
go get github.com/SBPH-Matthew/dbresolver
```

## Quick Start

```go
package main

import (
    "context"
    "database/sql"
    "log"

    "github.com/SBPH-Matthew/dbresolver"
    _ "github.com/lib/pq"
)

func main() {
    primaryDB, _ := sql.Open("postgres", "host=primary port=5432 dbname=myapp")
    replicaDB, _ := sql.Open("postgres", "host=replica port=5432 dbname=myapp")

    db, err := dbresolver.New(
        dbresolver.WithPrimaryDBs(dbresolver.NewNode(primaryDB)),
        dbresolver.WithReplicaDBs(dbresolver.NewNode(replicaDB)),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ctx := context.Background()

    // Writes automatically go to primary
    db.ExecContext(ctx, "INSERT INTO users (name) VALUES ($1)", "alice")

    // Reads automatically go to replica
    db.QueryContext(ctx, "SELECT * FROM users WHERE id = $1", 1)
}
```

## Read-Your-Writes Consistency

After writing data, replicas may not have it yet due to replication lag. Use `UsePrimary` to force the next read to go to the primary:

```go
// Write to primary
db.ExecContext(ctx, "INSERT INTO orders (user_id, total) VALUES ($1, $2)", userID, total)

// Read from primary to avoid replication lag
ctx = dbresolver.UsePrimary(ctx)
row := db.QueryRowContext(ctx, "SELECT id, status FROM orders WHERE user_id = $1 ORDER BY created_at DESC LIMIT 1", userID)
```

You can also force replica usage explicitly:

```go
ctx = dbresolver.UseReplica(ctx)
db.QueryContext(ctx, "SELECT count(*) FROM analytics_events")
```

## Cross-Region Setup

Use weighted load balancing to prefer databases in the same region:

```go
db, err := dbresolver.New(
    dbresolver.WithPrimaryDBs(
        dbresolver.NewNode(primaryUS, dbresolver.NodeConfig{
            Name:   "primary-us-east",
            Region: "us-east-1",
            Weight: 10,
        }),
    ),
    dbresolver.WithReplicaDBs(
        dbresolver.NewNode(replicaUS, dbresolver.NodeConfig{
            Name:   "replica-us-east",
            Region: "us-east-1",
            Weight: 10, // local — high weight
        }),
        dbresolver.NewNode(replicaEU, dbresolver.NodeConfig{
            Name:   "replica-eu-west",
            Region: "eu-west-1",
            Weight: 2, // remote — low weight
        }),
    ),
    dbresolver.WithLoadBalancer(dbresolver.WeightedRoundRobinLB),
)
```

## Health Checking

Enable background health checks to automatically exclude unhealthy nodes:

```go
db, err := dbresolver.New(
    dbresolver.WithPrimaryDBs(dbresolver.NewNode(primary)),
    dbresolver.WithReplicaDBs(dbresolver.NewNode(replica1), dbresolver.NewNode(replica2)),
    dbresolver.WithHealthCheck(
        5*time.Second, // check interval
        3,             // failures before marking unhealthy
        2,             // successes before marking healthy again
    ),
)
```

When all replicas are unhealthy, reads fall back to primaries. When all primaries are unhealthy, write operations return `ErrNoAvailableDB`.

## Read-Only Transactions

Route read-only transactions to replicas for better load distribution:

```go
tx, err := db.BeginReadOnlyTx(ctx, nil)
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback()

rows, _ := tx.QueryContext(ctx, "SELECT * FROM products WHERE category = $1", "electronics")
// ... process rows ...
tx.Commit()
```

## Connection Pool Configuration

Apply pool settings per role to optimize for different workloads:

```go
db, err := dbresolver.New(
    dbresolver.WithPrimaryDBs(dbresolver.NewNode(primary)),
    dbresolver.WithReplicaDBs(dbresolver.NewNode(replica)),
    dbresolver.WithPrimaryPoolConfig(dbresolver.PoolConfig{
        MaxOpenConns:    25,
        MaxIdleConns:    10,
        ConnMaxLifetime: 5 * time.Minute,
    }),
    dbresolver.WithReplicaPoolConfig(dbresolver.PoolConfig{
        MaxOpenConns:    50,
        MaxIdleConns:    25,
        ConnMaxLifetime: 5 * time.Minute,
    }),
)
```

## Load Balancing Strategies

| Strategy | Constant | Description |
|---|---|---|
| Round Robin | `RoundRobinLB` | Cycles through nodes sequentially (default) |
| Random | `RandomLB` | Selects a random node each time |
| Weighted Round Robin | `WeightedRoundRobinLB` | Distributes proportionally to node weights |
| Custom | `WithCustomLoadBalancer(lb)` | Provide your own `LoadBalancer` implementation |

## Query Routing Rules

### Always uses primary:
- `Exec` / `ExecContext`
- `Begin` / `BeginTx` (standard transactions)
- Queries starting with INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, TRUNCATE, etc.
- `SELECT ... FOR UPDATE` / `FOR SHARE`
- Queries containing `RETURNING`
- CTEs with write operations

### Uses replica (with primary fallback):
- `SELECT` queries
- `SHOW`, `DESCRIBE`, `EXPLAIN`
- `BeginReadOnlyTx`

### Context hints override all rules:
- `UsePrimary(ctx)` — forces primary regardless of query type
- `UseReplica(ctx)` — forces replica regardless of query type

## Custom Query Analyzer

If the built-in query analyzer doesn't cover your SQL patterns, provide a custom one:

```go
type myAnalyzer struct{}

func (a *myAnalyzer) Analyze(query string) dbresolver.QueryType {
    // Your custom logic here
    if isReadOnly(query) {
        return dbresolver.QueryTypeRead
    }
    return dbresolver.QueryTypeWrite
}

db, err := dbresolver.New(
    dbresolver.WithPrimaryDBs(dbresolver.NewNode(primary)),
    dbresolver.WithQueryAnalyzer(&myAnalyzer{}),
)
```

## Observability

Get per-node connection pool statistics:

```go
for _, s := range db.Stats() {
    fmt.Printf("%-20s role=%-8s region=%-12s healthy=%-5t open=%d idle=%d\n",
        s.Name, s.Role, s.Region, s.Healthy,
        s.Stats.OpenConnections, s.Stats.Idle)
}
```

## API Reference

### Constructor

| Function | Description |
|---|---|
| `New(opts ...OptionFunc) (DB, error)` | Creates a new resolver. Returns error if no primaries configured. |
| `NewNode(db *sql.DB, cfg ...NodeConfig) *Node` | Wraps a database connection with optional metadata. |

### DB Interface

| Method | Target |
|---|---|
| `ExecContext` / `Exec` | Primary |
| `QueryContext` / `Query` | Replica (auto-detected, with fallback) |
| `QueryRowContext` / `QueryRow` | Replica (auto-detected) |
| `PrepareContext` / `Prepare` | All nodes (routing at execute time) |
| `BeginTx` / `Begin` | Primary |
| `BeginReadOnlyTx` | Replica (with primary fallback) |
| `ConnContext` | Primary (or replica with `UseReplica` hint) |
| `PingContext` / `Ping` | All nodes in parallel |
| `Close` | All nodes in parallel |
| `Stats` | All nodes |

### Context Hints

| Function | Effect |
|---|---|
| `UsePrimary(ctx)` | Forces queries to use a primary database |
| `UseReplica(ctx)` | Forces queries to use a replica database |

## License

MIT
