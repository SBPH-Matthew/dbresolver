package dbresolver

import (
	"strings"
	"unicode"
)

// QueryType classifies a SQL query as either a read or write operation.
type QueryType int

const (
	// QueryTypeRead indicates a read-only query safe for replicas.
	QueryTypeRead QueryType = iota

	// QueryTypeWrite indicates a mutating query that must go to a primary.
	QueryTypeWrite
)

// QueryAnalyzer determines whether a SQL query is a read or write operation.
// Implement this interface to provide custom query classification logic.
type QueryAnalyzer interface {
	Analyze(query string) QueryType
}

type defaultQueryAnalyzer struct{}

var writeKeywords = map[string]struct{}{
	"INSERT":   {},
	"UPDATE":   {},
	"DELETE":   {},
	"REPLACE":  {},
	"MERGE":    {},
	"UPSERT":   {},
	"CREATE":   {},
	"ALTER":    {},
	"DROP":     {},
	"TRUNCATE": {},
	"GRANT":    {},
	"REVOKE":   {},
	"LOCK":     {},
	"CALL":     {},
}

func (a *defaultQueryAnalyzer) Analyze(query string) QueryType {
	q := strings.TrimLeftFunc(query, unicode.IsSpace)
	if len(q) == 0 {
		return QueryTypeRead
	}

	upper := strings.ToUpper(q)
	first := extractFirstWord(upper)

	if _, ok := writeKeywords[first]; ok {
		return QueryTypeWrite
	}

	if first == "WITH" {
		return analyzeCTE(upper)
	}

	if first == "SELECT" {
		return analyzeSelect(upper)
	}

	return QueryTypeRead
}

func extractFirstWord(s string) string {
	for i, c := range s {
		if unicode.IsSpace(c) || c == '(' {
			return s[:i]
		}
	}
	return s
}

// analyzeCTE handles WITH (Common Table Expression) queries which may
// contain write operations in PostgreSQL writable CTEs.
func analyzeCTE(upper string) QueryType {
	// Check the final statement after all CTE definitions.
	// The last ')' closes the final CTE, and the keyword after it
	// is the main statement.
	lastParen := strings.LastIndex(upper, ")")
	if lastParen >= 0 && lastParen < len(upper)-1 {
		remainder := strings.TrimLeftFunc(upper[lastParen+1:], unicode.IsSpace)
		if len(remainder) > 0 {
			first := extractFirstWord(remainder)
			if _, ok := writeKeywords[first]; ok {
				return QueryTypeWrite
			}
		}
	}

	// Detect writable CTEs (PostgreSQL): look for write keywords inside
	// CTE bodies by checking if they appear at positive parenthesis depth.
	for _, kw := range []string{"INSERT", "UPDATE", "DELETE"} {
		if idx := strings.Index(upper, kw); idx > 0 {
			depth := 0
			for i := 0; i < idx; i++ {
				switch upper[i] {
				case '(':
					depth++
				case ')':
					depth--
				}
			}
			if depth > 0 {
				return QueryTypeWrite
			}
		}
	}

	if strings.Contains(upper, "RETURNING") {
		return QueryTypeWrite
	}

	return QueryTypeRead
}

// analyzeSelect handles SELECT queries that may contain locking clauses
// or other patterns that require a primary database.
func analyzeSelect(upper string) QueryType {
	if strings.Contains(upper, " FOR UPDATE") ||
		strings.Contains(upper, " FOR SHARE") ||
		strings.Contains(upper, " FOR NO KEY UPDATE") ||
		strings.Contains(upper, " FOR KEY SHARE") {
		return QueryTypeWrite
	}

	if strings.Contains(upper, " RETURNING ") || strings.HasSuffix(upper, " RETURNING") {
		return QueryTypeWrite
	}

	// SELECT INTO creates a new table (PostgreSQL/SQL Server).
	// Distinguish from "INSERT INTO ... SELECT" by checking that INTO
	// appears before FROM in the query.
	intoIdx := strings.Index(upper, " INTO ")
	if intoIdx > 0 {
		fromIdx := strings.Index(upper, " FROM ")
		if fromIdx < 0 || intoIdx < fromIdx {
			return QueryTypeWrite
		}
	}

	return QueryTypeRead
}
