package activities

import (
	"context"
	"fmt"

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
	pool dbPool
}

// NewGoActivities constructs GoActivities with a real pgxpool for production use.
func NewGoActivities(pool *pgxpool.Pool) *GoActivities {
	return &GoActivities{pool: pool}
}

// NewGoActivitiesForTest constructs GoActivities with a mock pool for testing.
func NewGoActivitiesForTest(pool dbPool) *GoActivities {
	return &GoActivities{pool: pool}
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

// MarkExecutionComplete marks a temporal_executions row as completed.
func (a *GoActivities) MarkExecutionComplete(ctx context.Context, params contracts.MarkCompleteParams) error {
	_, err := a.pool.Exec(ctx, `
		UPDATE temporal_executions
		SET status          = 'completed',
		    records_written = $2,
		    pages_fetched   = $3,
		    completed_at    = now()
		WHERE id = $1::uuid
	`, params.CorpscoutRunID, params.Result.RecordsWritten, params.Result.PagesFetched)
	if err != nil {
		return fmt.Errorf("mark execution complete: %w", err)
	}
	return nil
}
