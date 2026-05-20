package activities_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

func newMock(t *testing.T) pgxmock.PgxPoolIface {
	t.Helper()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mock.ExpectationsWereMet()) })
	return mock
}

func TestWriteRawInputs_CompaniesHouse_NewRecord(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rec := contracts.RawRecord{
		NativeID:    "12345678",
		Name:        "ACME LTD",
		Status:      "active",
		CompanyType: "ltd",
		RawJSON:     json.RawMessage(`{"company_number":"12345678"}`),
		Hash:        "abc123",
	}

	// Existence check returns false (new company).
	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs("12345678").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))

	// Insert succeeds.
	mock.ExpectExec(`INSERT INTO companies_house_company_raw_inputs`).
		WithArgs("12345678", "ACME LTD", "active", "ltd",
			[]byte(`{"company_number":"12345678"}`), "abc123", "run-001").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	written, err := acts.WriteRawInputs(context.Background(), contracts.WriteRawInputsParams{
		Source:  "companies_house",
		RunID:   "run-001",
		Force:   false,
		Records: []contracts.RawRecord{rec},
	})
	require.NoError(t, err)
	require.Equal(t, 1, written)
}

func TestWriteRawInputs_CompaniesHouse_SkipsExisting(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rec := contracts.RawRecord{NativeID: "12345678", Name: "ACME LTD", Status: "active",
		RawJSON: json.RawMessage(`{}`), Hash: "h1"}

	// Existence check returns true — record already in DB.
	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs("12345678").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))

	written, err := acts.WriteRawInputs(context.Background(), contracts.WriteRawInputsParams{
		Source:  "companies_house",
		RunID:   "run-001",
		Force:   false,
		Records: []contracts.RawRecord{rec},
	})
	require.NoError(t, err)
	require.Equal(t, 0, written) // skipped
}

func TestWriteRawInputs_CompaniesHouse_ForceUpserts(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rec := contracts.RawRecord{NativeID: "12345678", Name: "ACME LTD", Status: "active",
		RawJSON: json.RawMessage(`{}`), Hash: "h1"}

	// No existence check — force skips it.
	mock.ExpectExec(`INSERT INTO companies_house_company_raw_inputs`).
		WithArgs("12345678", "ACME LTD", "active", "", []byte(`{}`), "h1", "run-001").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	written, err := acts.WriteRawInputs(context.Background(), contracts.WriteRawInputsParams{
		Source:  "companies_house",
		RunID:   "run-001",
		Force:   true,
		Records: []contracts.RawRecord{rec},
	})
	require.NoError(t, err)
	require.Equal(t, 1, written)
}

func TestWriteRawInputs_Brreg_NewRecord(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rec := contracts.RawRecord{
		NativeID: "123456789",
		Name:     "NORSK AS",
		Status:   "AKTIV",
		RawJSON:  json.RawMessage(`{"organisasjonsnummer":"123456789"}`),
		Hash:     "bh1",
	}

	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs("123456789").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))

	mock.ExpectExec(`INSERT INTO brreg_company_raw_inputs`).
		WithArgs("123456789", "NORSK AS", "AKTIV",
			[]byte(`{"organisasjonsnummer":"123456789"}`), "bh1", "run-002").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	written, err := acts.WriteRawInputs(context.Background(), contracts.WriteRawInputsParams{
		Source:  "brreg",
		RunID:   "run-002",
		Records: []contracts.RawRecord{rec},
	})
	require.NoError(t, err)
	require.Equal(t, 1, written)
}

func TestWriteRawInputs_UnsupportedSource(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	_, err := acts.WriteRawInputs(context.Background(), contracts.WriteRawInputsParams{
		Source:  "unknown_source",
		RunID:   "run-001",
		Records: []contracts.RawRecord{{NativeID: "1", RawJSON: json.RawMessage(`{}`), Hash: "h"}},
	})
	require.ErrorContains(t, err, "unsupported source")
}

func TestMarkExecutionComplete_UpdatesDB(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	mock.ExpectExec(`UPDATE temporal_executions`).
		WithArgs(42, 3, "exec-uuid-123").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	err := acts.MarkExecutionComplete(context.Background(), contracts.MarkCompleteParams{
		RunID:          "wf-run-id",
		CorpscoutRunID: "exec-uuid-123",
		Source:         "companies_house",
		Country:        "GB",
		Result:         contracts.PullCompaniesResult{RecordsWritten: 42, PagesFetched: 3},
	})
	require.NoError(t, err)
}

func TestMarkExecutionComplete_NoopWithoutCorpscoutRunID(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	// No DB call expected.
	err := acts.MarkExecutionComplete(context.Background(), contracts.MarkCompleteParams{
		RunID:  "wf-run-id",
		Source: "companies_house",
	})
	require.NoError(t, err)
}

func TestFilterForDomainDiscovery_FiltersAlreadySearched(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	ids := []string{"id1", "id2", "id3"}
	mock.ExpectQuery(`SELECT DISTINCT native_id FROM company_domains`).
		WithArgs("companies_house", ids).
		WillReturnRows(pgxmock.NewRows([]string{"native_id"}).AddRow("id1"))

	result, err := acts.FilterForDomainDiscovery(context.Background(), contracts.FilterForDomainDiscoveryParams{
		Source:    "companies_house",
		NativeIDs: ids,
		Force:     false,
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"id2", "id3"}, result.NeedDiscovery)
}

func TestFilterForDomainDiscovery_ForceReturnsAll(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	ids := []string{"id1", "id2"}
	// No DB query expected when force=true.
	result, err := acts.FilterForDomainDiscovery(context.Background(), contracts.FilterForDomainDiscoveryParams{
		Source:    "companies_house",
		NativeIDs: ids,
		Force:     true,
	})
	require.NoError(t, err)
	require.Equal(t, ids, result.NeedDiscovery)
}
