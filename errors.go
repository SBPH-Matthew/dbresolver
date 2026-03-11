package dbresolver

import (
	"database/sql/driver"
	"errors"
	"net"
)

var (
	// ErrNoPrimaryDB is returned when no primary database is configured.
	ErrNoPrimaryDB = errors.New("dbresolver: no primary database configured")

	// ErrNoAvailableDB is returned when no healthy database connection is available.
	ErrNoAvailableDB = errors.New("dbresolver: no available database connection")

	// ErrClosed is returned when operations are attempted on a closed resolver.
	ErrClosed = errors.New("dbresolver: resolver is closed")
)

// isConnectionError reports whether an error indicates a broken or
// unavailable database connection, which warrants fallback to another node.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	if errors.Is(err, driver.ErrBadConn) {
		return true
	}

	return false
}
