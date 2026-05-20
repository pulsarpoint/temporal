package cache

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Cache tracks which companies have had their detail profile fetched.
// Backed by a local SQLite file so state survives worker restarts.
type Cache struct {
	db *sql.DB
}

// New opens (or creates) the SQLite database at path and ensures the schema exists.
func New(path string) (*Cache, error) {
	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open cache db %s: %w", path, err)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS enrichment_cache (
			native_id  TEXT NOT NULL,
			source     TEXT NOT NULL,
			fetched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
			PRIMARY KEY (native_id, source)
		)
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create enrichment_cache: %w", err)
	}
	return &Cache{db: db}, nil
}

// Close releases the database connection.
func (c *Cache) Close() error {
	return c.db.Close()
}

// FilterUnfetched returns the subset of nativeIDs that either have never been
// fetched or were last fetched more than ttl ago. Pass ttl=0 to use the
// default 24-hour window.
func (c *Cache) FilterUnfetched(source string, nativeIDs []string, ttl time.Duration) ([]string, error) {
	if len(nativeIDs) == 0 {
		return nil, nil
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}

	placeholders := make([]string, len(nativeIDs))
	args := make([]any, 0, len(nativeIDs)+2)
	args = append(args, source)
	cutoff := time.Now().UTC().Add(-ttl).Format("2006-01-02T15:04:05Z")
	args = append(args, cutoff)
	for i, id := range nativeIDs {
		placeholders[i] = "?"
		args = append(args, id)
	}

	// Returns IDs that are "fresh" (in cache AND fetched within ttl).
	// IDs missing from the result need (re-)fetching.
	query := fmt.Sprintf(
		`SELECT native_id FROM enrichment_cache
		 WHERE source = ? AND fetched_at > ? AND native_id IN (%s)`,
		strings.Join(placeholders, ","),
	)
	rows, err := c.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query cache: %w", err)
	}
	defer rows.Close()

	fresh := make(map[string]struct{}, len(nativeIDs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		fresh[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]string, 0, len(nativeIDs))
	for _, id := range nativeIDs {
		if _, ok := fresh[id]; !ok {
			out = append(out, id)
		}
	}
	return out, nil
}

// MarkFetched records that detail profiles have been successfully fetched for nativeIDs.
// Idempotent: calling it again for an already-cached ID updates fetched_at.
func (c *Cache) MarkFetched(source string, nativeIDs []string) error {
	if len(nativeIDs) == 0 {
		return nil
	}
	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	stmt, err := tx.Prepare(`
		INSERT INTO enrichment_cache (native_id, source)
		VALUES (?, ?)
		ON CONFLICT (native_id, source) DO UPDATE
			SET fetched_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()
	for _, id := range nativeIDs {
		if _, err := stmt.Exec(id, source); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert %s: %w", id, err)
		}
	}
	return tx.Commit()
}
