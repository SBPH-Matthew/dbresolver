package dbresolver

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestHealthChecker_InitiallyHealthy(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	node := NewNode(db, NodeConfig{Name: "test"})
	hc := newHealthChecker(time.Second, 3, 2)
	hc.register([]*Node{node})

	if !hc.isHealthy(node) {
		t.Error("node should be healthy initially")
	}
}

func TestHealthChecker_UnregisteredNode(t *testing.T) {
	hc := newHealthChecker(time.Second, 3, 2)
	node := &Node{name: "unknown"}

	if !hc.isHealthy(node) {
		t.Error("unregistered node should be assumed healthy")
	}
}

func TestHealthChecker_MarkUnhealthy(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}

	node := NewNode(db, NodeConfig{Name: "test"})
	hc := newHealthChecker(50*time.Millisecond, 2, 2)
	hc.register([]*Node{node})

	// Fail pings to trigger unhealthy
	mock.ExpectPing().WillReturnError(errTestPing)
	mock.ExpectPing().WillReturnError(errTestPing)

	hc.start()
	defer hc.stop()

	// Wait for enough check cycles
	time.Sleep(200 * time.Millisecond)

	if hc.isHealthy(node) {
		t.Error("node should be unhealthy after consecutive failures")
	}
}

func TestHealthChecker_Recovery(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}

	node := NewNode(db, NodeConfig{Name: "test"})
	hc := newHealthChecker(time.Second, 2, 2)
	hc.register([]*Node{node})

	// Simulate failures to go unhealthy
	mock.ExpectPing().WillReturnError(errTestPing)
	hc.checkNode(node)
	if !hc.isHealthy(node) {
		t.Fatal("should still be healthy after 1 failure")
	}

	mock.ExpectPing().WillReturnError(errTestPing)
	hc.checkNode(node)
	if hc.isHealthy(node) {
		t.Fatal("should be unhealthy after 2 consecutive failures")
	}

	// Simulate recovery
	mock.ExpectPing()
	hc.checkNode(node)
	if hc.isHealthy(node) {
		t.Fatal("should still be unhealthy after 1 success")
	}

	mock.ExpectPing()
	hc.checkNode(node)
	if !hc.isHealthy(node) {
		t.Fatal("should be healthy after 2 consecutive successes")
	}
}

func TestHealthChecker_Defaults(t *testing.T) {
	hc := newHealthChecker(0, 0, 0)
	if hc.interval != 5*time.Second {
		t.Errorf("default interval: got %v, want 5s", hc.interval)
	}
	if hc.failThreshold != 3 {
		t.Errorf("default failThreshold: got %d, want 3", hc.failThreshold)
	}
	if hc.recoverThreshold != 2 {
		t.Errorf("default recoverThreshold: got %d, want 2", hc.recoverThreshold)
	}
}

func TestHealthChecker_StopIsIdempotent(t *testing.T) {
	hc := newHealthChecker(time.Second, 3, 2)
	hc.start()
	hc.stop()
	// Second stop should not panic or deadlock
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

var errTestPing = &testError{"ping failed"}
