package contracts

import "encoding/json"

// ── Pull workflow inputs ──────────────────────────────────────────────────────

// PullCompaniesHouseInput is the input for the PullCompaniesHouse workflow.
// IDs nil means bulk pull; populated means individual company lookup.
// Force re-inserts records even if already present in the raw_inputs table.
// Cursor, RunID, and Accumulated are set by ContinueAsNew to resume across runs.
type PullCompaniesHouseInput struct {
	Country        string              `json:"country"`
	IDs            []string            `json:"ids,omitempty"`
	CorpscoutRunID string              `json:"corpscout_run_id,omitempty"`
	Force          bool                `json:"force,omitempty"`
	Cursor         string              `json:"cursor,omitempty"`
	RunID          string              `json:"run_id,omitempty"`
	Accumulated    PullCompaniesResult `json:"accumulated,omitempty"`
}

// PullBrregInput is the input for the PullBrreg workflow.
// Mode selects the pipeline path:
//   - "bulk" (default): download the full Brreg export zip in one shot
//   - "incremental": paginate the list API starting from IncrementalFrom date
//
// Cursor, RunID, and Accumulated are carried forward by ContinueAsNew in
// incremental mode to resume across Temporal history-size boundaries.
type PullBrregInput struct {
	CorpscoutRunID  string              `json:"corpscout_run_id,omitempty"`
	Force           bool                `json:"force,omitempty"`
	RunID           string              `json:"run_id,omitempty"`
	OutputDir       string              `json:"output_dir,omitempty"`
	Mode            string              `json:"mode,omitempty"`             // "bulk" | "incremental"
	IncrementalFrom string              `json:"incremental_from,omitempty"` // starting date cursor, e.g. "2026-05-21,0"
	Cursor          string              `json:"cursor,omitempty"`           // ContinueAsNew carry-forward
	Accumulated     PullCompaniesResult `json:"accumulated,omitempty"`      // ContinueAsNew carry-forward
}

// DownloadBrregBulkResult is returned by the download_brreg_bulk Python activity.
type DownloadBrregBulkResult struct {
	FilePath string `json:"file_path"`
	Date     string `json:"date"`
}

// ImportBrregBulkParams is the input for the ImportBrregBulk Go activity.
type ImportBrregBulkParams struct {
	FilePath       string `json:"file_path"`
	RunID          string `json:"run_id"`
	CorpscoutRunID string `json:"corpscout_run_id,omitempty"`
	Force          bool   `json:"force,omitempty"`
}

// PullCompaniesResult is returned by the pull workflows.
// Actual records are already written to the DB; this is metadata only.
type PullCompaniesResult struct {
	RecordsWritten int      `json:"records_written"`
	PagesFetched   int      `json:"pages_fetched"`
	Errors         []string `json:"errors,omitempty"`
}

// ── List-sync activity types ──────────────────────────────────────────────────

// FetchPageInput is the input for the fetch_*_list Python activities.
type FetchPageInput struct {
	Source  string   `json:"source"`
	Country string   `json:"country"`
	IDs     []string `json:"ids,omitempty"`
	Page    int      `json:"page"`
	Cursor  string   `json:"cursor,omitempty"`
}

// RawRecord is a single raw company record returned by a list activity.
type RawRecord struct {
	NativeID    string          `json:"native_id"`
	Name        string          `json:"name"`
	Status      string          `json:"status"`
	CompanyType string          `json:"company_type,omitempty"`
	RawJSON     json.RawMessage `json:"raw_json"`
	Hash        string          `json:"hash"`
}

// FetchResult is returned by the list Python activities.
type FetchResult struct {
	Records    []RawRecord `json:"records"`
	HasMore    bool        `json:"has_more"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

// WriteRawInputsParams is the input for the WriteRawInputs Go activity.
// Force re-inserts records even if the company already has a row in the table.
type WriteRawInputsParams struct {
	Source  string      `json:"source"`
	RunID   string      `json:"run_id"`
	Force   bool        `json:"force,omitempty"`
	Records []RawRecord `json:"records"`
}

// MarkCompleteParams is the input for the MarkExecutionComplete Go activity.
type MarkCompleteParams struct {
	RunID          string              `json:"run_id"`
	CorpscoutRunID string              `json:"corpscout_run_id,omitempty"`
	Source         string              `json:"source"`
	Country        string              `json:"country"`
	Result         PullCompaniesResult `json:"result"`
	FinalCursor    string              `json:"final_cursor,omitempty"`
}

// SaveSyncCheckpointParams is the input for the SaveSyncCheckpoint Go activity.
type SaveSyncCheckpointParams struct {
	Source string `json:"source"`
	Cursor string `json:"cursor"`
}

// ── Domain enrichment workflow ────────────────────────────────────────────────

// CompanyLookup is a minimal company reference passed to child workflows.
type CompanyLookup struct {
	NativeID string `json:"native_id"`
	Name     string `json:"name"`
}

// EnrichCompanyDomainsInput is the input for the EnrichCompanyDomains workflow.
type EnrichCompanyDomainsInput struct {
	Source    string          `json:"source"`
	Country   string          `json:"country"`
	Companies []CompanyLookup `json:"companies"`
	// Force bypasses the domain_cache and re-runs discovery even for
	// companies already searched.
	Force bool `json:"force,omitempty"`
}

// EnrichCompanyDomainsResult is returned by the EnrichCompanyDomains workflow.
type EnrichCompanyDomainsResult struct {
	CompaniesProcessed int               `json:"companies_processed"`
	DomainsFound       int               `json:"domains_found"`
	Discoveries        []DomainDiscovery `json:"discoveries,omitempty"`
}

// FilterForDomainDiscoveryParams is the input for FilterForDomainDiscovery.
type FilterForDomainDiscoveryParams struct {
	Source    string   `json:"source"`
	NativeIDs []string `json:"native_ids"`
	Force     bool     `json:"force"`
}

// FilterForDomainDiscoveryResult lists company IDs that still need domain search.
type FilterForDomainDiscoveryResult struct {
	NeedDiscovery []string `json:"need_discovery"`
}

// DiscoverDomainsInput is the input for the discover_company_domains Python activity.
type DiscoverDomainsInput struct {
	Source    string          `json:"source"`
	Country   string          `json:"country"`
	Companies []CompanyLookup `json:"companies"`
}

// DomainDiscovery is a single domain candidate discovered for a company.
type DomainDiscovery struct {
	NativeID   string `json:"native_id"`
	Domain     string `json:"domain"`
	Signal     string `json:"signal"`     // "wikidata", "duckduckgo", "certsh", "heuristic"
	Confidence int    `json:"confidence"` // 0-100
}

// DiscoverDomainsResult is returned by the discover_company_domains Python activity.
type DiscoverDomainsResult struct {
	Discoveries []DomainDiscovery `json:"discoveries"`
}

// WriteDiscoveredDomainsParams is the input for the WriteDiscoveredDomains Go activity.
type WriteDiscoveredDomainsParams struct {
	Source      string            `json:"source"`
	Discoveries []DomainDiscovery `json:"discoveries"`
}

// MarkDomainsSearchedParams is the input for the MarkDomainsSearched Go activity.
// AllSearched includes every native_id in the batch, even those with no domain found.
type MarkDomainsSearchedParams struct {
	Source    string   `json:"source"`
	NativeIDs []string `json:"native_ids"`
}

// TranslateBrregInput is the input for the operator-triggered Brreg translation workflow.
type TranslateBrregInput struct {
	IDs           []string `json:"ids,omitempty"`
	PromptVersion string   `json:"prompt_version"`
	Model         string   `json:"model"`
	Accumulated   int      `json:"accumulated,omitempty"`
	FXRateDate    string   `json:"fx_rate_date,omitempty"`
}

// TranslateBrregBatchParams is the input for the batch translation activity.
type TranslateBrregBatchParams struct {
	IDs           []string `json:"ids,omitempty"`
	PromptVersion string   `json:"prompt_version"`
	Model         string   `json:"model"`
	FXRateDate    string   `json:"fx_rate_date,omitempty"`
	WorkflowRunID string   `json:"workflow_run_id"`
	BatchSize     int      `json:"batch_size"`
}

// TranslateBrregBatchResult reports batch progress to the workflow loop.
type TranslateBrregBatchResult struct {
	Claimed    int `json:"claimed"`
	Translated int `json:"translated"`
	Failed     int `json:"failed"`
}
