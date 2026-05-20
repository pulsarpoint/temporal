package activities_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

func TestWriteRawInputs_CompaniesHouse(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rec := contracts.RawRecord{
		NativeID:    "12345678",
		Name:        "ACME LTD",
		Status:      "active",
		CompanyType: "ltd",
		RawJSON:     json.RawMessage(`{"company_number":"12345678"}`),
		Hash:        "abc123",
	}

	mock.ExpectExec("INSERT INTO companies_house_company_raw_inputs").
		WithArgs("12345678", "ACME LTD", "active", "ltd",
			[]byte(`{"company_number":"12345678"}`), "abc123", "run-001").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	acts := activities.NewGoActivitiesForTest(mock, t.TempDir())
	written, err := acts.WriteRawInputs(context.Background(), contracts.WriteRawInputsParams{
		Source:  "companies_house",
		RunID:   "run-001",
		Records: []contracts.RawRecord{rec},
	})
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWriteRawInputs_UnsupportedSource(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	acts := activities.NewGoActivitiesForTest(mock, t.TempDir())
	_, err = acts.WriteRawInputs(context.Background(), contracts.WriteRawInputsParams{
		Source:  "unknown_source",
		RunID:   "run-001",
		Records: []contracts.RawRecord{},
	})
	require.ErrorContains(t, err, "unsupported source")
}

func TestMarkExecutionComplete(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	dir := t.TempDir()
	acts := activities.NewGoActivitiesForTest(mock, dir)

	runID := "550e8400-e29b-41d4-a716-446655440000"
	err = acts.MarkExecutionComplete(context.Background(), contracts.MarkCompleteParams{
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
