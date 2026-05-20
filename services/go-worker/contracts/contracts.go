package contracts

import "encoding/json"

// PullCompaniesInput is the input for the PullCompanies workflow.
// IDs nil means bulk pull; populated means individual lookup.
type PullCompaniesInput struct {
	Source         string   `json:"source"`
	Country        string   `json:"country"`
	IDs            []string `json:"ids,omitempty"`
	CorpscoutRunID string   `json:"corpscout_run_id"`
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

// MarkCompleteParams is the input for the MarkExecutionComplete Go activity.
type MarkCompleteParams struct {
	RunID          string              `json:"run_id"`                     // stable UUID from workflow SideEffect
	CorpscoutRunID string              `json:"corpscout_run_id,omitempty"` // original trigger ID, empty when started from Temporal UI
	Source         string              `json:"source"`
	Country        string              `json:"country"`
	Result         PullCompaniesResult `json:"result"`
}
