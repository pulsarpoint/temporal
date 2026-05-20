package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pulsarpoint/data-pipelines/cache"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

var supportedSources = map[string]bool{
	"companies_house": true,
}

// GoActivities holds dependencies for all Go-side Temporal activities.
// When pool is nil, WriteRawInputs falls back to JSON file output.
type GoActivities struct {
	pool      *pgxpool.Pool // nil = file-only mode
	outputDir string
	cache     *cache.Cache // nil = enrichment tracking disabled
}

// NewGoActivities constructs GoActivities for production.
// Pass a nil pool to run in file-only mode (no DB required).
// Pass a nil cache to disable enrichment tracking.
func NewGoActivities(pool *pgxpool.Pool, outputDir string, c *cache.Cache) *GoActivities {
	return &GoActivities{pool: pool, outputDir: outputDir, cache: c}
}

// WriteRawInputs writes records to the DB when a pool is configured,
// otherwise dumps them as a JSON file in the output directory.
func (a *GoActivities) WriteRawInputs(ctx context.Context, params contracts.WriteRawInputsParams) (int, error) {
	if !supportedSources[params.Source] {
		return 0, fmt.Errorf("unsupported source: %s", params.Source)
	}
	if a.pool != nil {
		return a.writeToDatabase(ctx, params)
	}
	return a.writeToFile(params)
}

func (a *GoActivities) writeToDatabase(ctx context.Context, params contracts.WriteRawInputsParams) (int, error) {
	written := 0
	for _, rec := range params.Records {
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
		`, rec.NativeID, rec.Name, rec.Status, rec.CompanyType, []byte(rec.RawJSON), rec.Hash, params.RunID)
		if err != nil {
			return written, fmt.Errorf("insert row %s: %w", rec.NativeID, err)
		}
		written++
	}
	return written, nil
}

func (a *GoActivities) writeToFile(params contracts.WriteRawInputsParams) (int, error) {
	type batchFile struct {
		RunID          string                `json:"run_id"`
		Source         string                `json:"source"`
		BatchWrittenAt string                `json:"batch_written_at"`
		RecordsCount   int                   `json:"records_count"`
		Records        []contracts.RawRecord `json:"records"`
	}

	out := batchFile{
		RunID:          params.RunID,
		Source:         params.Source,
		BatchWrittenAt: time.Now().UTC().Format(time.RFC3339),
		RecordsCount:   len(params.Records),
		Records:        params.Records,
	}

	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return 0, fmt.Errorf("marshal batch: %w", err)
	}

	if err := os.MkdirAll(a.outputDir, 0o755); err != nil {
		return 0, fmt.Errorf("create output dir: %w", err)
	}

	path := filepath.Join(a.outputDir, params.RunID+"_batch_"+uuid.New().String()+".json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return 0, fmt.Errorf("write batch file %s: %w", path, err)
	}

	return len(params.Records), nil
}

// FilterForEnrichment returns the subset of native IDs that do not yet have
// a cached detail profile. Returns all IDs when the cache is unavailable.
func (a *GoActivities) FilterForEnrichment(_ context.Context, params contracts.FilterForEnrichmentParams) (contracts.FilterForEnrichmentResult, error) {
	if a.cache == nil {
		return contracts.FilterForEnrichmentResult{NeedEnrichment: params.NativeIDs}, nil
	}
	unfetched, err := a.cache.FilterUnfetched(params.Source, params.NativeIDs)
	if err != nil {
		return contracts.FilterForEnrichmentResult{}, fmt.Errorf("filter unfetched: %w", err)
	}
	return contracts.FilterForEnrichmentResult{NeedEnrichment: unfetched}, nil
}

// MarkEnriched records in the local cache that detail profiles have been
// fetched for the given native IDs. Call this after a successful detail fetch.
func (a *GoActivities) MarkEnriched(_ context.Context, params contracts.MarkEnrichedParams) error {
	if a.cache == nil {
		return nil
	}
	if err := a.cache.MarkFetched(params.Source, params.NativeIDs); err != nil {
		return fmt.Errorf("mark fetched: %w", err)
	}
	return nil
}

// MarkExecutionComplete writes a summary JSON file to the output directory.
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
