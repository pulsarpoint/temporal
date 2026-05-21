package activities

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

// DB is the subset of pgxpool.Pool used by GoActivities (also satisfied by pgxmock).
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// GoActivities holds dependencies for all Go-side Temporal activities.
type GoActivities struct {
	pool DB
}

// NewGoActivities constructs GoActivities. pool must not be nil.
func NewGoActivities(pool *pgxpool.Pool) *GoActivities {
	return &GoActivities{pool: pool}
}

// NewGoActivitiesWithDB is used in tests to inject a mock DB.
func NewGoActivitiesWithDB(db DB) *GoActivities {
	return &GoActivities{pool: db}
}

// ── List-sync activities ──────────────────────────────────────────────────────

// WriteRawInputs writes records to the corpscout DB.
// If Force is false, companies already present in the table are skipped.
func (a *GoActivities) WriteRawInputs(ctx context.Context, params contracts.WriteRawInputsParams) (int, error) {
	written := 0
	for _, rec := range params.Records {
		if rec.NativeID == "" {
			continue
		}
		var inserted bool
		var err error
		switch params.Source {
		case "companies_house":
			inserted, err = a.writeCompaniesHouseRecord(ctx, rec, params.RunID, params.Force)
		case "brreg":
			inserted, err = a.writeBrregRecord(ctx, rec, params.RunID, params.Force)
		default:
			return written, fmt.Errorf("unsupported source: %s", params.Source)
		}
		if err != nil {
			return written, fmt.Errorf("write %s %s: %w", params.Source, rec.NativeID, err)
		}
		if inserted {
			written++
		}
	}
	return written, nil
}

func (a *GoActivities) writeCompaniesHouseRecord(ctx context.Context, rec contracts.RawRecord, runID string, force bool) (bool, error) {
	if !force {
		var exists bool
		if err := a.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM companies_house_company_raw_inputs WHERE company_number = $1)`,
			rec.NativeID,
		).Scan(&exists); err != nil {
			return false, fmt.Errorf("check existence: %w", err)
		}
		if exists {
			return false, nil
		}
	}
	_, err := a.pool.Exec(ctx, `
		INSERT INTO companies_house_company_raw_inputs
			(source_native_id, company_number, company_name, company_status, company_type,
			 raw_payload, payload_hash, run_id)
		VALUES ($1, $1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (company_number, payload_hash) DO UPDATE
			SET last_seen_at = now(), run_id = EXCLUDED.run_id
	`, rec.NativeID, rec.Name, rec.Status, rec.CompanyType, []byte(rec.RawJSON), rec.Hash, runID)
	return err == nil, err
}

func (a *GoActivities) writeBrregRecord(ctx context.Context, rec contracts.RawRecord, runID string, force bool) (bool, error) {
	if !force {
		var exists bool
		if err := a.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM brreg_company_raw_inputs WHERE organization_number = $1)`,
			rec.NativeID,
		).Scan(&exists); err != nil {
			return false, fmt.Errorf("check existence: %w", err)
		}
		if exists {
			return false, nil
		}
	}
	_, err := a.pool.Exec(ctx, `
		INSERT INTO brreg_company_raw_inputs
			(source_native_id, organization_number, organization_name, registration_status,
			 raw_payload, payload_hash, run_id)
		VALUES ($1, $1, $2, $3, $4, $5, $6)
		ON CONFLICT (organization_number, payload_hash) DO UPDATE
			SET last_seen_at = now(), run_id = EXCLUDED.run_id
	`, rec.NativeID, rec.Name, rec.Status, []byte(rec.RawJSON), rec.Hash, runID)
	return err == nil, err
}

// ImportBrregBulk opens the gzip file downloaded by the Python activity, parses the
// JSON of Brreg entities, and bulk-upserts them into brreg_company_raw_inputs
// in batches of 2000 records. The file is deleted after a successful import.
func (a *GoActivities) ImportBrregBulk(ctx context.Context, params contracts.ImportBrregBulkParams) (int, error) {
	f, err := os.Open(params.FilePath)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", params.FilePath, err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return 0, fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close()

	raw, err := io.ReadAll(gr)
	if err != nil {
		return 0, fmt.Errorf("read gzip content: %w", err)
	}

	// Brreg bulk format: {"_embedded":{"enheter":[...]}} — same shape as the list API.
	// Fall back to a direct JSON array if the wrapper is absent.
	var entities []json.RawMessage
	var wrapper struct {
		Embedded struct {
			Enheter []json.RawMessage `json:"enheter"`
		} `json:"_embedded"`
	}
	if err := json.Unmarshal(raw, &wrapper); err == nil && len(wrapper.Embedded.Enheter) > 0 {
		entities = wrapper.Embedded.Enheter
	} else if err := json.Unmarshal(raw, &entities); err != nil {
		return 0, fmt.Errorf("parse bulk JSON: %w", err)
	}
	slog.Info("ImportBrregBulk: parsed entities", "count", len(entities))

	const batchSize = 2000
	written := 0

	for i := 0; i < len(entities); i += batchSize {
		end := i + batchSize
		if end > len(entities) {
			end = len(entities)
		}
		chunk := entities[i:end]

		batch := &pgx.Batch{}
		for _, raw := range chunk {
			var item map[string]interface{}
			if err := json.Unmarshal(raw, &item); err != nil {
				continue
			}
			orgNum, _ := item["organisasjonsnummer"].(string)
			if orgNum == "" {
				continue
			}
			name, _ := item["navn"].(string)
			konkurs, _ := item["konkurs"].(bool)
			underAvvikling, _ := item["underAvvikling"].(bool)
			status := "active"
			if konkurs || underAvvikling {
				status = "dissolved"
			}
			h := sha256.Sum256(raw)
			hash := hex.EncodeToString(h[:])

			batch.Queue(`
				INSERT INTO brreg_company_raw_inputs
					(source_native_id, organization_number, organization_name,
					 registration_status, raw_payload, payload_hash, run_id)
				VALUES ($1, $1, $2, $3, $4, $5, $6)
				ON CONFLICT (organization_number, payload_hash) DO UPDATE
					SET last_seen_at = now(), run_id = EXCLUDED.run_id
			`, orgNum, name, status, []byte(raw), hash, params.RunID)
		}

		results := a.pool.SendBatch(ctx, batch)
		for range chunk {
			if _, err := results.Exec(); err != nil {
				results.Close()
				return written, fmt.Errorf("batch upsert at offset %d: %w", i, err)
			}
		}
		results.Close()

		written += len(chunk)
		activity.RecordHeartbeat(ctx, written)
		slog.Info("ImportBrregBulk: progress", "written", written, "total", len(entities))
	}

	// Clean up — the gzip file is no longer needed.
	if err := os.Remove(params.FilePath); err != nil {
		slog.Warn("ImportBrregBulk: could not delete file", "path", params.FilePath, "error", err)
	}

	return written, nil
}

// MarkExecutionComplete updates the temporal_executions row created by corpscout
// and saves a sync checkpoint so the next run continues from the final cursor.
// If CorpscoutRunID is empty (workflow triggered outside corpscout) the DB update
// is skipped but the checkpoint is still saved.
func (a *GoActivities) MarkExecutionComplete(ctx context.Context, params contracts.MarkCompleteParams) error {
	if params.CorpscoutRunID != "" {
		if _, err := a.pool.Exec(ctx, `
			UPDATE temporal_executions
			SET status          = 'completed',
			    records_written = $1,
			    pages_fetched   = $2,
			    completed_at    = now()
			WHERE id = $3
		`, params.Result.RecordsWritten, params.Result.PagesFetched, params.CorpscoutRunID); err != nil {
			return err
		}
	}
	// Save checkpoint so next trigger resumes from where this run ended.
	if params.FinalCursor != "" {
		if _, err := a.pool.Exec(ctx, `
			INSERT INTO source_sync_checkpoints (source_name, cursor, last_completed_at)
			VALUES ($1, $2, now())
			ON CONFLICT (source_name) DO UPDATE
			    SET cursor            = EXCLUDED.cursor,
			        last_completed_at = EXCLUDED.last_completed_at,
			        updated_at        = now()
		`, params.Source, params.FinalCursor); err != nil {
			slog.Warn("MarkExecutionComplete: save sync checkpoint", "source", params.Source, "error", err)
		}
	}
	return nil
}

// SaveSyncCheckpoint persists the current pagination cursor so the next workflow
// trigger can resume from exactly this point instead of re-scanning from the start.
// Called by ContinueAsNew runs so progress is preserved even if the final run fails.
func (a *GoActivities) SaveSyncCheckpoint(ctx context.Context, params contracts.SaveSyncCheckpointParams) error {
	_, err := a.pool.Exec(ctx, `
		INSERT INTO source_sync_checkpoints (source_name, cursor, last_completed_at)
		VALUES ($1, $2, NULL)
		ON CONFLICT (source_name) DO UPDATE
		    SET cursor     = EXCLUDED.cursor,
		        updated_at = now()
	`, params.Source, params.Cursor)
	return err
}

// ── Domain enrichment activities ──────────────────────────────────────────────

// FilterForDomainDiscovery returns the subset of native IDs that have not yet
// had a domain search. Checks the company_domains table directly.
// If Force is true, all IDs are returned regardless.
func (a *GoActivities) FilterForDomainDiscovery(ctx context.Context, params contracts.FilterForDomainDiscoveryParams) (contracts.FilterForDomainDiscoveryResult, error) {
	if params.Force || len(params.NativeIDs) == 0 {
		return contracts.FilterForDomainDiscoveryResult{NeedDiscovery: params.NativeIDs}, nil
	}

	rows, err := a.pool.Query(ctx,
		`SELECT DISTINCT native_id FROM company_domains WHERE source = $1 AND native_id = ANY($2)`,
		params.Source, params.NativeIDs,
	)
	if err != nil {
		return contracts.FilterForDomainDiscoveryResult{}, fmt.Errorf("query company_domains: %w", err)
	}
	defer rows.Close()

	already := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return contracts.FilterForDomainDiscoveryResult{}, err
		}
		already[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return contracts.FilterForDomainDiscoveryResult{}, err
	}

	need := make([]string, 0, len(params.NativeIDs))
	for _, id := range params.NativeIDs {
		if _, ok := already[id]; !ok {
			need = append(need, id)
		}
	}
	return contracts.FilterForDomainDiscoveryResult{NeedDiscovery: need}, nil
}

// WriteDiscoveredDomains persists domain discovery results to company_domains.
func (a *GoActivities) WriteDiscoveredDomains(ctx context.Context, params contracts.WriteDiscoveredDomainsParams) error {
	for _, d := range params.Discoveries {
		if d.NativeID == "" || d.Domain == "" {
			continue
		}
		_, err := a.pool.Exec(ctx, `
			INSERT INTO company_domains (native_id, source, domain, signal, confidence)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (native_id, source, domain) DO UPDATE
				SET signal       = EXCLUDED.signal,
				    confidence   = EXCLUDED.confidence,
				    last_seen_at = now()
		`, d.NativeID, params.Source, d.Domain, d.Signal, d.Confidence)
		if err != nil {
			return fmt.Errorf("upsert company_domain %s/%s: %w", d.NativeID, d.Domain, err)
		}
	}
	return nil
}

// MarkDomainsSearched is a no-op: domain discovery status is derived from the
// presence of rows in company_domains rather than a separate cache table.
func (a *GoActivities) MarkDomainsSearched(_ context.Context, _ contracts.MarkDomainsSearchedParams) error {
	return nil
}
