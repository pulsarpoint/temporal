package cache

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Cache persists enrichment and domain-discovery state across worker restarts.
// Backed by a local SQLite file.
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
		);
		CREATE TABLE IF NOT EXISTS domain_cache (
			native_id   TEXT NOT NULL,
			source      TEXT NOT NULL,
			searched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
			PRIMARY KEY (native_id, source)
		);
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create cache tables: %w", err)
	}
	return &Cache{db: db}, nil
}

// Close releases the database connection.
func (c *Cache) Close() error {
	return c.db.Close()
}

// ── enrichment_cache ──────────────────────────────────────────────────────────

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

	query := fmt.Sprintf(
		`SELECT native_id FROM enrichment_cache
		 WHERE source = ? AND fetched_at > ? AND native_id IN (%s)`,
		strings.Join(placeholders, ","),
	)
	rows, err := c.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query enrichment cache: %w", err)
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

// MarkFetched records that detail profiles have been fetched. Idempotent.
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

// ── domain_cache ──────────────────────────────────────────────────────────────

// FilterNeedsDiscovery returns the subset of nativeIDs that have not yet had a
// domain search. If force is true, all IDs are returned regardless of cache.
func (c *Cache) FilterNeedsDiscovery(source string, nativeIDs []string, force bool) ([]string, error) {
	if len(nativeIDs) == 0 {
		return nil, nil
	}
	if force {
		out := make([]string, len(nativeIDs))
		copy(out, nativeIDs)
		return out, nil
	}

	placeholders := make([]string, len(nativeIDs))
	args := make([]any, 0, len(nativeIDs)+1)
	args = append(args, source)
	for i, id := range nativeIDs {
		placeholders[i] = "?"
		args = append(args, id)
	}

	query := fmt.Sprintf(
		`SELECT native_id FROM domain_cache WHERE source = ? AND native_id IN (%s)`,
		strings.Join(placeholders, ","),
	)
	rows, err := c.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query domain cache: %w", err)
	}
	defer rows.Close()

	already := make(map[string]struct{}, len(nativeIDs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		already[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]string, 0, len(nativeIDs))
	for _, id := range nativeIDs {
		if _, ok := already[id]; !ok {
			out = append(out, id)
		}
	}
	return out, nil
}

// MarkDomainsSearched records that domain discovery was run for the given IDs.
// Idempotent: calling it again updates searched_at.
func (c *Cache) MarkDomainsSearched(source string, nativeIDs []string) error {
	if len(nativeIDs) == 0 {
		return nil
	}
	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	stmt, err := tx.Prepare(`
		INSERT INTO domain_cache (native_id, source)
		VALUES (?, ?)
		ON CONFLICT (native_id, source) DO UPDATE
			SET searched_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()
	for _, id := range nativeIDs {
		if _, err := stmt.Exec(id, source); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert domain_cache %s: %w", id, err)
		}
	}
	return tx.Commit()
}
