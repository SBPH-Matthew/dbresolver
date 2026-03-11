package dbresolver

import "database/sql"

// DBRole identifies whether a database node is a primary or replica.
type DBRole string

const (
	RolePrimary DBRole = "primary"
	RoleReplica DBRole = "replica"
)

// DBStats holds connection pool statistics and metadata for a single node.
type DBStats struct {
	Name    string
	Role    DBRole
	Region  string
	Healthy bool
	Stats   sql.DBStats
}
