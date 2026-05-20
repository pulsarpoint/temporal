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
	"brreg":           true,
}

// GoActivities holds dependencies for all Go-side Temporal activities.
// When pool is nil, write activities fall back to JSON file output.
type GoActivities struct {
	pool      *pgxpool.Pool // nil = file-only mode
	outputDir string
	cache     *cache.Cache // nil = caching disabled
}

// NewGoActivities constructs GoActivities.
// Pass nil pool to run in file-only mode. Pass nil cache to disable caching.
func NewGoActivities(pool *pgxpool.Pool, outputDir string, c *cache.Cache) *GoActivities {
	return &GoActivities{pool: pool, outputDir: outputDir, cache: c}
}

// ── List-sync activities ──────────────────────────────────────────────────────

// WriteRawInputs writes records to the DB (or a JSON file in file-only mode).
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

// MarkExecutionComplete writes a summary JSON file to the output directory.
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

// ── Domain enrichment activities ──────────────────────────────────────────────

// FilterForDomainDiscovery returns the subset of native IDs that have not yet
// had a domain search run for them. If force is true, all IDs are returned.
// When the cache is unavailable, all IDs are returned.
func (a *GoActivities) FilterForDomainDiscovery(_ context.Context, params contracts.FilterForDomainDiscoveryParams) (contracts.FilterForDomainDiscoveryResult, error) {
	if a.cache == nil {
		return contracts.FilterForDomainDiscoveryResult{NeedDiscovery: params.NativeIDs}, nil
	}
	need, err := a.cache.FilterNeedsDiscovery(params.Source, params.NativeIDs, params.Force)
	if err != nil {
		return contracts.FilterForDomainDiscoveryResult{}, fmt.Errorf("filter domain cache: %w", err)
	}
	return contracts.FilterForDomainDiscoveryResult{NeedDiscovery: need}, nil
}

// WriteDiscoveredDomains persists domain discovery results to the DB (or a
// JSON file in file-only mode).
func (a *GoActivities) WriteDiscoveredDomains(ctx context.Context, params contracts.WriteDiscoveredDomainsParams) error {
	if len(params.Discoveries) == 0 {
		return nil
	}
	if a.pool != nil {
		return a.writeDiscoveriesToDB(ctx, params)
	}
	return a.writeDiscoveriesToFile(params)
}

func (a *GoActivities) writeDiscoveriesToDB(ctx context.Context, params contracts.WriteDiscoveredDomainsParams) error {
	for _, d := range params.Discoveries {
		if d.NativeID == "" || d.Domain == "" {
			continue
		}
		_, err := a.pool.Exec(ctx, `
			INSERT INTO company_domains (native_id, source, domain, signal, confidence)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (native_id, source, domain) DO UPDATE
				SET signal     = EXCLUDED.signal,
				    confidence = EXCLUDED.confidence,
				    last_seen_at = now()
		`, d.NativeID, params.Source, d.Domain, d.Signal, d.Confidence)
		if err != nil {
			return fmt.Errorf("upsert company_domain %s/%s: %w", d.NativeID, d.Domain, err)
		}
	}
	return nil
}

func (a *GoActivities) writeDiscoveriesToFile(params contracts.WriteDiscoveredDomainsParams) error {
	type domainsFile struct {
		Source      string                     `json:"source"`
		WrittenAt   string                     `json:"written_at"`
		Count       int                        `json:"count"`
		Discoveries []contracts.DomainDiscovery `json:"discoveries"`
	}

	out := domainsFile{
		Source:      params.Source,
		WrittenAt:   time.Now().UTC().Format(time.RFC3339),
		Count:       len(params.Discoveries),
		Discoveries: params.Discoveries,
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal domains: %w", err)
	}
	if err := os.MkdirAll(a.outputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	path := filepath.Join(a.outputDir, "domains_"+params.Source+"_"+uuid.New().String()+".json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write domains file %s: %w", path, err)
	}
	return nil
}

// MarkDomainsSearched marks all IDs in the batch as searched in the domain
// cache, preventing repeat discovery on future workflow runs.
func (a *GoActivities) MarkDomainsSearched(_ context.Context, params contracts.MarkDomainsSearchedParams) error {
	if a.cache == nil {
		return nil
	}
	if err := a.cache.MarkDomainsSearched(params.Source, params.NativeIDs); err != nil {
		return fmt.Errorf("mark domains searched: %w", err)
	}
	return nil
}
