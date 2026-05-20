package contracts

import "encoding/json"

// PullCompaniesHouseInput is the input for the PullCompaniesHouse workflow.
// IDs nil means bulk pull; populated means individual company lookup.
type PullCompaniesHouseInput struct {
	Country        string   `json:"country"`
	IDs            []string `json:"ids,omitempty"`
	CorpscoutRunID string   `json:"corpscout_run_id,omitempty"`
}

// PullBrregInput is the input for the PullBrreg workflow.
// Country is always NO — hardcoded in the workflow.
type PullBrregInput struct {
	IDs            []string `json:"ids,omitempty"`
	CorpscoutRunID string   `json:"corpscout_run_id,omitempty"`
}

// PullCompaniesResult is returned by the PullCompanies workflow.
// Actual records are already written to the DB; this is metadata only.
type PullCompaniesResult struct {
	RecordsWritten int      `json:"records_written"`
	PagesFetched   int      `json:"pages_fetched"`
	Errors         []string `json:"errors,omitempty"`
}

// FetchPageInput is the input for the FetchPage Python activity.
type FetchPageInput struct {
	Source  string   `json:"source"`
	Country string   `json:"country"`
	IDs     []string `json:"ids,omitempty"`
	Page    int      `json:"page"`
	Cursor  string   `json:"cursor,omitempty"`
}

// RawRecord is a single raw company record returned by FetchPage.
// It carries the fields needed to INSERT into raw_inputs tables.
type RawRecord struct {
	NativeID    string          `json:"native_id"`
	Name        string          `json:"name"`
	Status      string          `json:"status"`
	CompanyType string          `json:"company_type,omitempty"`
	RawJSON     json.RawMessage `json:"raw_json"`
	Hash        string          `json:"hash"` // SHA-256 of RawJSON for dedup
}

// FetchResult is returned by the FetchPage Python activity.
type FetchResult struct {
	Records    []RawRecord `json:"records"`
	HasMore    bool        `json:"has_more"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

// WriteRawInputsParams is the input for the WriteRawInputs Go activity.
type WriteRawInputsParams struct {
	Source  string      `json:"source"`
	RunID   string      `json:"run_id"`
	Records []RawRecord `json:"records"`
}

// FilterForEnrichmentParams is the input for the FilterForEnrichment Go activity.
type FilterForEnrichmentParams struct {
	Source    string   `json:"source"`
	NativeIDs []string `json:"native_ids"`
}

// FilterForEnrichmentResult lists company IDs that have no cached detail profile yet.
type FilterForEnrichmentResult struct {
	NeedEnrichment []string `json:"need_enrichment"`
}

// MarkEnrichedParams is the input for the MarkEnriched Go activity.
// Called after company details have been successfully fetched and stored.
type MarkEnrichedParams struct {
	Source    string   `json:"source"`
	NativeIDs []string `json:"native_ids"`
}

// FetchCompanyDetailInput is the input for the fetch_companies_house_detail Python activity.
type FetchCompanyDetailInput struct {
	Source   string `json:"source"`
	NativeID string `json:"native_id"`
}

// CompanyDetailResult is returned by the fetch_companies_house_detail Python activity.
type CompanyDetailResult struct {
	NativeID       string   `json:"native_id"`
	Name           string   `json:"name"`
	Status         string   `json:"status"`
	Type           string   `json:"type,omitempty"`
	DateOfCreation string   `json:"date_of_creation,omitempty"`
	AddressLine1   *string  `json:"address_line_1,omitempty"`
	AddressLine2   *string  `json:"address_line_2,omitempty"`
	Locality       *string  `json:"locality,omitempty"`
	PostalCode     *string  `json:"postal_code,omitempty"`
	Country        *string  `json:"country,omitempty"`
	Region         *string  `json:"region,omitempty"`
	SICCodes       []string `json:"sic_codes,omitempty"`
}

// WriteCompanyDetailsParams is the input for the WriteCompanyDetails Go activity.
type WriteCompanyDetailsParams struct {
	Source  string                `json:"source"`
	Details []CompanyDetailResult `json:"details"`
}

// MarkCompleteParams is the input for the MarkExecutionComplete Go activity.
type MarkCompleteParams struct {
	RunID          string              `json:"run_id"`                     // stable UUID from workflow SideEffect
	CorpscoutRunID string              `json:"corpscout_run_id,omitempty"` // original trigger ID, empty when started from Temporal UI
	Source         string              `json:"source"`
	Country        string              `json:"country"`
	Result         PullCompaniesResult `json:"result"`
}
