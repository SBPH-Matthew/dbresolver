package dbresolver

import "testing"

func TestDefaultQueryAnalyzer(t *testing.T) {
	a := &defaultQueryAnalyzer{}

	tests := []struct {
		name  string
		query string
		want  QueryType
	}{
		// Write operations (DML)
		{"INSERT", "INSERT INTO users (name) VALUES ('alice')", QueryTypeWrite},
		{"UPDATE", "UPDATE users SET name = 'bob' WHERE id = 1", QueryTypeWrite},
		{"DELETE", "DELETE FROM users WHERE id = 1", QueryTypeWrite},
		{"REPLACE", "REPLACE INTO users (id, name) VALUES (1, 'alice')", QueryTypeWrite},
		{"MERGE", "MERGE INTO target USING source ON ...", QueryTypeWrite},
		{"UPSERT", "UPSERT INTO users (id, name) VALUES (1, 'alice')", QueryTypeWrite},

		// DDL operations
		{"CREATE TABLE", "CREATE TABLE users (id INT PRIMARY KEY)", QueryTypeWrite},
		{"ALTER TABLE", "ALTER TABLE users ADD COLUMN email TEXT", QueryTypeWrite},
		{"DROP TABLE", "DROP TABLE users", QueryTypeWrite},
		{"TRUNCATE", "TRUNCATE TABLE users", QueryTypeWrite},

		// Privilege operations
		{"GRANT", "GRANT SELECT ON users TO readonly", QueryTypeWrite},
		{"REVOKE", "REVOKE SELECT ON users FROM readonly", QueryTypeWrite},

		// Stored procedures
		{"CALL", "CALL process_orders()", QueryTypeWrite},

		// Lock
		{"LOCK", "LOCK TABLE users IN EXCLUSIVE MODE", QueryTypeWrite},

		// Read operations
		{"SELECT", "SELECT * FROM users WHERE id = 1", QueryTypeRead},
		{"SELECT with join", "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id", QueryTypeRead},
		{"SELECT with subquery", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)", QueryTypeRead},
		{"SHOW", "SHOW TABLES", QueryTypeRead},
		{"EXPLAIN", "EXPLAIN SELECT * FROM users", QueryTypeRead},

		// Locking reads (should go to primary)
		{"SELECT FOR UPDATE", "SELECT * FROM users WHERE id = 1 FOR UPDATE", QueryTypeWrite},
		{"SELECT FOR SHARE", "SELECT * FROM users WHERE id = 1 FOR SHARE", QueryTypeWrite},
		{"SELECT FOR NO KEY UPDATE", "SELECT * FROM users WHERE id = 1 FOR NO KEY UPDATE", QueryTypeWrite},
		{"SELECT FOR KEY SHARE", "SELECT * FROM users WHERE id = 1 FOR KEY SHARE", QueryTypeWrite},

		// SELECT with RETURNING (some PostgreSQL patterns)
		{"SELECT RETURNING", "SELECT * FROM users RETURNING id", QueryTypeWrite},

		// SELECT INTO (creates a table)
		{"SELECT INTO", "SELECT * INTO new_table FROM users", QueryTypeWrite},
		{"SELECT INTO with FROM after", "SELECT id, name INTO backup FROM users WHERE active = true", QueryTypeWrite},

		// Normal SELECT with INTO in subquery (should be read)
		{"SELECT FROM subquery with INTO", "SELECT * FROM (SELECT * INTO TEMP FROM t) sub", QueryTypeRead},

		// CTE patterns
		{"CTE read", "WITH cte AS (SELECT * FROM users) SELECT * FROM cte", QueryTypeRead},
		{"CTE write final", "WITH cte AS (SELECT * FROM users) INSERT INTO backup SELECT * FROM cte", QueryTypeWrite},
		{"CTE writable (PostgreSQL)", "WITH deleted AS (DELETE FROM expired_sessions RETURNING *) SELECT count(*) FROM deleted", QueryTypeWrite},
		{"CTE with RETURNING", "WITH moved AS (UPDATE orders SET status = 'archived' RETURNING id) SELECT * FROM moved", QueryTypeWrite},

		// Whitespace handling
		{"leading whitespace", "  SELECT * FROM users", QueryTypeRead},
		{"leading newline", "\n\tINSERT INTO users VALUES (1)", QueryTypeWrite},

		// Case insensitivity
		{"lowercase select", "select * from users", QueryTypeRead},
		{"lowercase insert", "insert into users values (1)", QueryTypeWrite},
		{"mixed case", "Select * From users", QueryTypeRead},

		// Empty query
		{"empty", "", QueryTypeRead},
		{"whitespace only", "   ", QueryTypeRead},

		// Parenthesized first keyword
		{"parenthesized INSERT", "(INSERT INTO users VALUES (1))", QueryTypeRead},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := a.Analyze(tt.query)
			if got != tt.want {
				t.Errorf("Analyze(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func BenchmarkDefaultQueryAnalyzer(b *testing.B) {
	a := &defaultQueryAnalyzer{}
	queries := []string{
		"SELECT * FROM users WHERE id = $1",
		"INSERT INTO users (name) VALUES ($1) RETURNING id",
		"WITH cte AS (SELECT * FROM users) SELECT * FROM cte WHERE id > $1",
		"SELECT * FROM orders WHERE id = $1 FOR UPDATE",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Analyze(queries[i%len(queries)])
	}
}
