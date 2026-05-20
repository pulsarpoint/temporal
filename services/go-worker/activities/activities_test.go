package activities_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

func TestWriteRawInputs_CompaniesHouse(t *testing.T) {
	dir := t.TempDir()
	acts := activities.NewGoActivities(nil, dir)

	rec := contracts.RawRecord{
		NativeID:    "12345678",
		Name:        "ACME LTD",
		Status:      "active",
		CompanyType: "ltd",
		RawJSON:     json.RawMessage(`{"company_number":"12345678"}`),
		Hash:        "abc123",
	}

	written, err := acts.WriteRawInputs(context.Background(), contracts.WriteRawInputsParams{
		Source:  "companies_house",
		RunID:   "run-001",
		Records: []contracts.RawRecord{rec},
	})
	require.NoError(t, err)
	require.Equal(t, 1, written)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.True(t, strings.HasPrefix(entries[0].Name(), "run-001_batch_"))

	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(data, &result))
	require.Equal(t, "run-001", result["run_id"])
	require.Equal(t, "companies_house", result["source"])
	require.InDelta(t, 1, result["records_count"], 0)
}

func TestWriteRawInputs_UnsupportedSource(t *testing.T) {
	acts := activities.NewGoActivities(nil, t.TempDir())
	_, err := acts.WriteRawInputs(context.Background(), contracts.WriteRawInputsParams{
		Source:  "unknown_source",
		RunID:   "run-001",
		Records: []contracts.RawRecord{},
	})
	require.ErrorContains(t, err, "unsupported source")
}

func TestMarkExecutionComplete(t *testing.T) {
	dir := t.TempDir()
	acts := activities.NewGoActivities(nil, dir)

	runID := "550e8400-e29b-41d4-a716-446655440000"
	err := acts.MarkExecutionComplete(context.Background(), contracts.MarkCompleteParams{
		RunID:   runID,
		Source:  "companies_house",
		Country: "GB",
		Result:  contracts.PullCompaniesResult{RecordsWritten: 42, PagesFetched: 3},
	})
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(dir, runID+".json"))
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(data, &result))
	require.Equal(t, runID, result["run_id"])
	require.Equal(t, "companies_house", result["source"])
	require.Equal(t, "GB", result["country"])
	require.InDelta(t, 42, result["records_written"], 0)
	require.InDelta(t, 3, result["pages_fetched"], 0)
	require.NotEmpty(t, result["completed_at"])
}
