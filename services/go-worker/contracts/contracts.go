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

// PullGLEIFInput is the input for the PullGLEIF workflow.
type PullGLEIFInput struct {
	CorpscoutRunID string              `json:"corpscout_run_id,omitempty"`
	RunID          string              `json:"run_id,omitempty"`
	Mode           string              `json:"mode,omitempty"`
	DeltaWindow    string              `json:"delta_window,omitempty"`
	OutputDir      string              `json:"output_dir,omitempty"`
	Accumulated    PullCompaniesResult `json:"accumulated,omitempty"`
}

// PullAriregisterInput is the input for the PullAriregister workflow.
type PullAriregisterInput struct {
	CorpscoutRunID string              `json:"corpscout_run_id,omitempty"`
	RunID          string              `json:"run_id,omitempty"`
	Mode           string              `json:"mode,omitempty"`
	OutputDir      string              `json:"output_dir,omitempty"`
	Accumulated    PullCompaniesResult `json:"accumulated,omitempty"`
}

// PullCVRInput is the input for the PullCVR workflow.
type PullCVRInput struct {
	CorpscoutRunID string              `json:"corpscout_run_id,omitempty"`
	RunID          string              `json:"run_id,omitempty"`
	Mode           string              `json:"mode,omitempty"`
	OutputDir      string              `json:"output_dir,omitempty"`
	Accumulated    PullCompaniesResult `json:"accumulated,omitempty"`
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

type DownloadedSourceFile struct {
	Source     string `json:"source"`
	Dataset    string `json:"dataset"`
	FilePath   string `json:"file_path"`
	SnapshotID string `json:"snapshot_id"`
	SHA256     string `json:"sha256"`
	Format     string `json:"format"`
}

type DownloadSourceFilesResult struct {
	Source     string                 `json:"source"`
	SnapshotID string                 `json:"snapshot_id"`
	Files      []DownloadedSourceFile `json:"files"`
}

type DownloadSourceFilesInput struct {
	Source      string   `json:"source"`
	Mode        string   `json:"mode"`
	OutputDir   string   `json:"output_dir"`
	Datasets    []string `json:"datasets,omitempty"`
	SnapshotID  string   `json:"snapshot_id,omitempty"`
	DeltaWindow string   `json:"delta_window,omitempty"`
}

type ImportSourceBulkParams struct {
	Files          []DownloadedSourceFile `json:"files"`
	RunID          string                 `json:"run_id"`
	CorpscoutRunID string                 `json:"corpscout_run_id"`
}

type ImportGLEIFGoldenCopyParams = ImportSourceBulkParams
type ImportAriregisterBulkParams = ImportSourceBulkParams
type ImportCVRBulkParams = ImportSourceBulkParams

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

// TranslateSourceInput is the input for source-generic raw input translation.
type TranslateSourceInput struct {
	Source        string   `json:"source"`
	IDs           []string `json:"ids,omitempty"`
	PromptVersion string   `json:"prompt_version"`
	Model         string   `json:"model"`
	Accumulated   int      `json:"accumulated,omitempty"`
	FXRateDate    string   `json:"fx_rate_date,omitempty"`
}

// PrepareBrregTranslationBatchParams is the input for the Go activity that claims
// raw Brreg rows, loads FX data, and finds translation cache misses.
type PrepareBrregTranslationBatchParams struct {
	IDs           []string `json:"ids,omitempty"`
	PromptVersion string   `json:"prompt_version"`
	Model         string   `json:"model"`
	FXRateDate    string   `json:"fx_rate_date,omitempty"`
	WorkflowRunID string   `json:"workflow_run_id"`
	BatchSize     int      `json:"batch_size"`
}

// TranslateBrregBatchParams is retained for older workers and tests. New
// workflows use PrepareBrregTranslationBatchParams plus the Python DSPy activity.
type TranslateBrregBatchParams = PrepareBrregTranslationBatchParams

// PrepareSourceTranslationBatchParams is the source-generic prepare activity input.
type PrepareSourceTranslationBatchParams struct {
	Source        string   `json:"source"`
	IDs           []string `json:"ids,omitempty"`
	PromptVersion string   `json:"prompt_version"`
	Model         string   `json:"model"`
	FXRateDate    string   `json:"fx_rate_date,omitempty"`
	WorkflowRunID string   `json:"workflow_run_id"`
	BatchSize     int      `json:"batch_size"`
}

type SourceTranslationRowPayload struct {
	ID         string          `json:"id"`
	RawPayload json.RawMessage `json:"raw_payload"`
}

type BrregTranslationRowPayload = SourceTranslationRowPayload

type FXRatePayload struct {
	Source   string             `json:"source,omitempty"`
	RateDate string             `json:"rate_date,omitempty"`
	EURPer   map[string]float64 `json:"eur_per,omitempty"`
}

type TranslationItem struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

type TranslatedTerm struct {
	ID          string `json:"id"`
	Translation string `json:"translation"`
}

type TranslationFailure struct {
	ID    string `json:"id"`
	Error string `json:"error"`
}

type TranslateTermsInput struct {
	Category      string            `json:"category"`
	SourceLang    string            `json:"source_lang,omitempty"`
	TargetLang    string            `json:"target_lang,omitempty"`
	Items         []TranslationItem `json:"items"`
	Model         string            `json:"model,omitempty"`
	PromptVersion string            `json:"prompt_version"`
}

type TranslateTermsResult struct {
	Translations []TranslatedTerm     `json:"translations"`
	Failures     []TranslationFailure `json:"failures"`
	Model        string               `json:"model"`
}

type BrregTranslatedTerm struct {
	ID          string `json:"id"`
	Category    string `json:"category"`
	Text        string `json:"text"`
	Translation string `json:"translation"`
}

type SourceTranslatedTerm = BrregTranslatedTerm

// PrepareBrregTranslationBatchResult carries claimed row payloads and cache
// misses to the workflow. The workflow sends MissesByCategory to Python DSPy.
type PrepareBrregTranslationBatchResult struct {
	Claimed            int                          `json:"claimed"`
	Rows               []BrregTranslationRowPayload `json:"rows"`
	FX                 FXRatePayload                `json:"fx"`
	CachedTranslations map[string]string            `json:"cached_translations"`
	MissesByCategory   map[string][]TranslationItem `json:"misses_by_category"`
}

// PrepareSourceTranslationBatchResult carries claimed row payloads and cache
// misses to the source-generic workflow.
type PrepareSourceTranslationBatchResult struct {
	Claimed            int                           `json:"claimed"`
	Rows               []SourceTranslationRowPayload `json:"rows"`
	FX                 FXRatePayload                 `json:"fx"`
	CachedTranslations map[string]string             `json:"cached_translations"`
	MissesByCategory   map[string][]TranslationItem  `json:"misses_by_category"`
}

// WriteBrregTranslationBatchParams is the input for the Go activity that builds
// raw_payload_en, upserts translation cache rows, and updates raw input status.
type WriteBrregTranslationBatchParams struct {
	PromptVersion      string                       `json:"prompt_version"`
	Model              string                       `json:"model"`
	Rows               []BrregTranslationRowPayload `json:"rows"`
	FX                 FXRatePayload                `json:"fx"`
	CachedTranslations map[string]string            `json:"cached_translations"`
	NewTranslations    []BrregTranslatedTerm        `json:"new_translations"`
}

// WriteSourceTranslationBatchParams is the source-generic write activity input.
type WriteSourceTranslationBatchParams struct {
	Source             string                        `json:"source"`
	PromptVersion      string                        `json:"prompt_version"`
	Model              string                        `json:"model"`
	Rows               []SourceTranslationRowPayload `json:"rows"`
	FX                 FXRatePayload                 `json:"fx"`
	CachedTranslations map[string]string             `json:"cached_translations"`
	NewTranslations    []SourceTranslatedTerm        `json:"new_translations"`
}

// TranslateBrregBatchResult reports batch progress to the workflow loop.
type TranslateBrregBatchResult struct {
	Claimed    int `json:"claimed"`
	Translated int `json:"translated"`
	Failed     int `json:"failed"`
}

type TranslateSourceBatchResult = TranslateBrregBatchResult
