package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

// dbPool is the minimal pgx interface needed by GoActivities.
// Satisfied by both *pgxpool.Pool (production) and pgxmock (tests).
type dbPool interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// GoActivities holds dependencies for all Go-side Temporal activities.
type GoActivities struct {
	pool      dbPool
	outputDir string
}

// NewGoActivities constructs GoActivities for production use.
func NewGoActivities(pool *pgxpool.Pool, outputDir string) *GoActivities {
	return &GoActivities{pool: pool, outputDir: outputDir}
}

// NewGoActivitiesForTest constructs GoActivities with a mock pool and temp dir for testing.
func NewGoActivitiesForTest(pool dbPool, outputDir string) *GoActivities {
	return &GoActivities{pool: pool, outputDir: outputDir}
}

// WriteRawInputs inserts raw records into the appropriate source raw_inputs table.
// Idempotent: ON CONFLICT updates last_seen_at only.
func (a *GoActivities) WriteRawInputs(ctx context.Context, params contracts.WriteRawInputsParams) (int, error) {
	switch params.Source {
	case "companies_house":
		return a.writeCompaniesHouse(ctx, params.RunID, params.Records)
	default:
		return 0, fmt.Errorf("unsupported source: %s", params.Source)
	}
}

func (a *GoActivities) writeCompaniesHouse(ctx context.Context, runID string, records []contracts.RawRecord) (int, error) {
	written := 0
	for _, rec := range records {
		if rec.NativeID == "" {
			continue
		}
		_, err := a.pool.Exec(ctx, `
			INSERT INTO companies_house_company_raw_inputs
				(source_pull_run_id, source_native_id, company_number, company_name,
				 company_status, company_type, source_updated_at, raw_payload, payload_hash, run_id)
			VALUES
				(NULL, $1, $1, $2, $3, $4, NULL, $5, $6, $7)
			ON CONFLICT (company_number, payload_hash) DO UPDATE
				SET last_seen_at = now(), run_id = EXCLUDED.run_id
		`, rec.NativeID, rec.Name, rec.Status, rec.CompanyType, []byte(rec.RawJSON), rec.Hash, runID)
		if err != nil {
			return written, fmt.Errorf("insert row %s: %w", rec.NativeID, err)
		}
		written++
	}
	return written, nil
}

// MarkExecutionComplete writes a JSON result file to the configured output directory.
// File name: {run_id}.json
func (a *GoActivities) MarkExecutionComplete(_ context.Context, params contracts.MarkCompleteParams) error {
	type resultFile struct {
		RunID          string   `json:"run_id"`
		CorpscoutRunID string   `json:"corpscout_run_id,omitempty"`
		Source         string   `json:"source"`
		Country        string   `json:"country"`
		RecordsWritten int      `json:"records_written"`
		PagesFetched   int      `json:"pages_fetched"`
		Errors         []string `json:"errors,omitempty"`
		CompletedAt    string   `json:"completed_at"`
	}

	out := resultFile{
		RunID:          params.RunID,
		CorpscoutRunID: params.CorpscoutRunID,
		Source:         params.Source,
		Country:        params.Country,
		RecordsWritten: params.Result.RecordsWritten,
		PagesFetched:   params.Result.PagesFetched,
		Errors:         params.Result.Errors,
		CompletedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	if err := os.MkdirAll(a.outputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	path := filepath.Join(a.outputDir, params.RunID+".json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write result file %s: %w", path, err)
	}

	return nil
}
