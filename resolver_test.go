package dbresolver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	return db, mock
}

func newMockDBWithPing(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	return db, mock
}

func TestNew_NoPrimary(t *testing.T) {
	_, err := New()
	if err != ErrNoPrimaryDB {
		t.Errorf("expected ErrNoPrimaryDB, got %v", err)
	}
}

func TestNew_WithPrimary(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectClose()

	resolver, err := New(WithPrimaryDBs(NewNode(db)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resolver.Close()
}

func TestExecContext_UsesPrimary(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	replicaDB, _ := newMockDB(t)

	primaryMock.ExpectExec("INSERT INTO users").
		WillReturnResult(sqlmock.NewResult(1, 1))

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.ExecContext(context.Background(), "INSERT INTO users (name) VALUES (?)", "alice")
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	if err := primaryMock.ExpectationsWereMet(); err != nil {
		t.Errorf("primary expectations not met: %v", err)
	}
}

func TestQueryContext_ReadUsesReplica(t *testing.T) {
	primaryDB, _ := newMockDB(t)
	replicaDB, replicaMock := newMockDB(t)

	replicaMock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "alice"))

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT * FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	rows.Close()

	if err := replicaMock.ExpectationsWereMet(); err != nil {
		t.Errorf("replica expectations not met: %v", err)
	}
}

func TestQueryContext_WriteUsesPrimary(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	replicaDB, _ := newMockDB(t)

	primaryMock.ExpectQuery("INSERT INTO users").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "INSERT INTO users (name) VALUES ('alice') RETURNING id")
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	rows.Close()

	if err := primaryMock.ExpectationsWereMet(); err != nil {
		t.Errorf("primary expectations not met: %v", err)
	}
}

func TestQueryContext_UsePrimaryHint(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	replicaDB, _ := newMockDB(t)

	primaryMock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := UsePrimary(context.Background())
	rows, err := db.QueryContext(ctx, "SELECT * FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	rows.Close()

	if err := primaryMock.ExpectationsWereMet(); err != nil {
		t.Errorf("primary expectations not met: %v", err)
	}
}

func TestQueryContext_NoReplica_FallsToPrimary(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)

	primaryMock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT * FROM users")
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	rows.Close()

	if err := primaryMock.ExpectationsWereMet(); err != nil {
		t.Errorf("primary should handle reads when no replicas: %v", err)
	}
}

func TestQueryContext_SelectForUpdate_UsesPrimary(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	replicaDB, _ := newMockDB(t)

	primaryMock.ExpectQuery("SELECT .+ FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT * FROM users WHERE id = 1 FOR UPDATE")
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	rows.Close()

	if err := primaryMock.ExpectationsWereMet(); err != nil {
		t.Errorf("primary expectations not met: %v", err)
	}
}

func TestBeginTx_UsesPrimary(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	replicaDB, _ := newMockDB(t)

	primaryMock.ExpectBegin()
	primaryMock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	primaryMock.ExpectCommit()

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	_, err = tx.ExecContext(context.Background(), "INSERT INTO users (name) VALUES (?)", "alice")
	if err != nil {
		t.Fatalf("Exec in tx failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if err := primaryMock.ExpectationsWereMet(); err != nil {
		t.Errorf("primary expectations not met: %v", err)
	}
}

func TestBeginReadOnlyTx_UsesReplica(t *testing.T) {
	primaryDB, _ := newMockDB(t)
	replicaDB, replicaMock := newMockDB(t)

	replicaMock.ExpectBegin()
	replicaMock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(42))
	replicaMock.ExpectCommit()

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.BeginReadOnlyTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginReadOnlyTx failed: %v", err)
	}

	rows, err := tx.QueryContext(context.Background(), "SELECT count(*) FROM users")
	if err != nil {
		t.Fatalf("Query in tx failed: %v", err)
	}
	rows.Close()

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if err := replicaMock.ExpectationsWereMet(); err != nil {
		t.Errorf("replica expectations not met: %v", err)
	}
}

func TestPrepareContext_ReadStmt(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	replicaDB, replicaMock := newMockDB(t)

	primaryMock.ExpectPrepare("SELECT")
	replicaMock.ExpectPrepare("SELECT")
	replicaMock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.PrepareContext(context.Background(), "SELECT * FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("PrepareContext failed: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(context.Background(), 1)
	if err != nil {
		t.Fatalf("stmt.QueryContext failed: %v", err)
	}
	rows.Close()

	if err := replicaMock.ExpectationsWereMet(); err != nil {
		t.Errorf("replica expectations not met: %v", err)
	}
}

func TestPrepareContext_WriteStmt(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	replicaDB, replicaMock := newMockDB(t)

	primaryMock.ExpectPrepare("INSERT")
	replicaMock.ExpectPrepare("INSERT")
	primaryMock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.PrepareContext(context.Background(), "INSERT INTO users (name) VALUES (?)")
	if err != nil {
		t.Fatalf("PrepareContext failed: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(context.Background(), "alice")
	if err != nil {
		t.Fatalf("stmt.ExecContext failed: %v", err)
	}

	if err := primaryMock.ExpectationsWereMet(); err != nil {
		t.Errorf("primary expectations not met: %v", err)
	}
}

func TestPingContext_PingsAll(t *testing.T) {
	primaryDB, primaryMock := newMockDBWithPing(t)
	replicaDB, replicaMock := newMockDBWithPing(t)

	primaryMock.ExpectPing()
	replicaMock.ExpectPing()

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("PingContext failed: %v", err)
	}

	if err := primaryMock.ExpectationsWereMet(); err != nil {
		t.Errorf("primary expectations not met: %v", err)
	}
	if err := replicaMock.ExpectationsWereMet(); err != nil {
		t.Errorf("replica expectations not met: %v", err)
	}
}

func TestClose_ClosesAll(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	replicaDB, replicaMock := newMockDB(t)

	primaryMock.ExpectClose()
	replicaMock.ExpectClose()

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := primaryMock.ExpectationsWereMet(); err != nil {
		t.Errorf("primary expectations not met: %v", err)
	}
	if err := replicaMock.ExpectationsWereMet(); err != nil {
		t.Errorf("replica expectations not met: %v", err)
	}
}

func TestClose_Idempotent(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	primaryMock.ExpectClose()

	db, err := New(WithPrimaryDBs(NewNode(primaryDB)))
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := db.Close(); err != ErrClosed {
		t.Errorf("second Close: got %v, want ErrClosed", err)
	}
}

func TestClosed_RejectsOperations(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	primaryMock.ExpectClose()

	db, err := New(WithPrimaryDBs(NewNode(primaryDB)))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	ctx := context.Background()

	if _, err := db.ExecContext(ctx, "INSERT INTO t VALUES (1)"); err != ErrClosed {
		t.Errorf("ExecContext: got %v, want ErrClosed", err)
	}
	if _, err := db.QueryContext(ctx, "SELECT 1"); err != ErrClosed {
		t.Errorf("QueryContext: got %v, want ErrClosed", err)
	}
	if _, err := db.PrepareContext(ctx, "SELECT 1"); err != ErrClosed {
		t.Errorf("PrepareContext: got %v, want ErrClosed", err)
	}
	if _, err := db.BeginTx(ctx, nil); err != ErrClosed {
		t.Errorf("BeginTx: got %v, want ErrClosed", err)
	}
	if err := db.PingContext(ctx); err != ErrClosed {
		t.Errorf("PingContext: got %v, want ErrClosed", err)
	}
}

func TestPrimariesAndReplicas(t *testing.T) {
	p1, m1 := newMockDB(t)
	p2, m2 := newMockDB(t)
	r1, m3 := newMockDB(t)

	m1.ExpectClose()
	m2.ExpectClose()
	m3.ExpectClose()

	db, err := New(
		WithPrimaryDBs(NewNode(p1), NewNode(p2)),
		WithReplicaDBs(NewNode(r1)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	primaries := db.Primaries()
	if len(primaries) != 2 {
		t.Errorf("Primaries: got %d, want 2", len(primaries))
	}

	replicas := db.Replicas()
	if len(replicas) != 1 {
		t.Errorf("Replicas: got %d, want 1", len(replicas))
	}
}

func TestStats_AllNodes(t *testing.T) {
	p1, m1 := newMockDB(t)
	r1, m2 := newMockDB(t)

	m1.ExpectClose()
	m2.ExpectClose()

	db, err := New(
		WithPrimaryDBs(NewNode(p1, NodeConfig{Name: "primary-1", Region: "us-east"})),
		WithReplicaDBs(NewNode(r1, NodeConfig{Name: "replica-1", Region: "eu-west"})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stats := db.Stats()
	if len(stats) != 2 {
		t.Fatalf("Stats: got %d entries, want 2", len(stats))
	}

	if stats[0].Name != "primary-1" || stats[0].Role != RolePrimary || stats[0].Region != "us-east" {
		t.Errorf("primary stats: %+v", stats[0])
	}
	if stats[1].Name != "replica-1" || stats[1].Role != RoleReplica || stats[1].Region != "eu-west" {
		t.Errorf("replica stats: %+v", stats[1])
	}

	for _, s := range stats {
		if !s.Healthy {
			t.Errorf("node %q should be healthy", s.Name)
		}
	}
}

func TestRoundRobinAcrossMultiplePrimaries(t *testing.T) {
	p1, m1 := newMockDB(t)
	p2, m2 := newMockDB(t)

	m1.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	m2.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(2, 1))

	db, err := New(
		WithPrimaryDBs(NewNode(p1), NewNode(p2)),
		WithLoadBalancer(RoundRobinLB),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 2; i++ {
		_, err := db.ExecContext(context.Background(), "INSERT INTO t VALUES (?)", i)
		if err != nil {
			t.Fatalf("ExecContext %d failed: %v", i, err)
		}
	}

	if err := m1.ExpectationsWereMet(); err != nil {
		t.Errorf("primary-1 expectations not met: %v", err)
	}
	if err := m2.ExpectationsWereMet(); err != nil {
		t.Errorf("primary-2 expectations not met: %v", err)
	}
}

func TestWithCustomLoadBalancer(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	primaryMock.ExpectClose()

	customLB := &roundRobinLB{} // just reuse round robin for test
	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithCustomLoadBalancer(customLB),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	got := db.Primary()
	if got != primaryDB {
		t.Error("custom LB should still resolve to the primary")
	}
}

func TestWithQueryAnalyzer_Custom(t *testing.T) {
	primaryDB, primaryMock := newMockDB(t)
	replicaDB, _ := newMockDB(t)

	alwaysWrite := queryAnalyzerFunc(func(q string) QueryType { return QueryTypeWrite })

	primaryMock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	db, err := New(
		WithPrimaryDBs(NewNode(primaryDB)),
		WithReplicaDBs(NewNode(replicaDB)),
		WithQueryAnalyzer(alwaysWrite),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT * FROM users")
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	rows.Close()

	if err := primaryMock.ExpectationsWereMet(); err != nil {
		t.Errorf("custom analyzer should route SELECT to primary: %v", err)
	}
}

// queryAnalyzerFunc adapts a function to the QueryAnalyzer interface.
type queryAnalyzerFunc func(string) QueryType

func (f queryAnalyzerFunc) Analyze(query string) QueryType { return f(query) }
