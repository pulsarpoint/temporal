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

func TestImportCompaniesHouseSICCodes_UpsertsCSVRows(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	path := filepath.Join(t.TempDir(), "sic.csv")
	require.NoError(t, os.WriteFile(path, []byte("SIC Code,Description\r\n62012,Business and domestic software development\r\n01110,\"Growing of cereals (except rice), leguminous crops and oil seeds\"\r\n"), 0o600))

	mock.ExpectExec(`INSERT INTO companies_house_sic_codes`).
		WithArgs("62012", "Business and domestic software development", (*string)(nil), (*string)(nil), "https://example.test/sic.csv", "sha123", pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`INSERT INTO companies_house_sic_codes`).
		WithArgs("01110", "Growing of cereals (except rice), leguminous crops and oil seeds", (*string)(nil), (*string)(nil), "https://example.test/sic.csv", "sha123", pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	count, err := acts.ImportCompaniesHouseSICCodes(context.Background(), contracts.DownloadedSourceFile{
		FilePath:  path,
		SHA256:    "sha123",
		Source:    "companies_house_sic",
		Dataset:   "sic_codes",
		Format:    "csv",
		SourceURL: "https://example.test/sic.csv",
	})
	require.NoError(t, err)
	require.Equal(t, 2, count)
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

func TestFilterForDomainDiscoveryBrregUsesRawInputDomainBridge(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rawID1 := "7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"
	rawID2 := "4d80f241-0e9e-48b2-b4b4-8f919a5ff34d"
	mock.ExpectQuery(`SELECT DISTINCT raw_input_id::text FROM brreg_raw_input_domains`).
		WithArgs([]string{rawID1, rawID2}).
		WillReturnRows(pgxmock.NewRows([]string{"raw_input_id"}).AddRow(rawID1))

	result, err := acts.FilterForDomainDiscovery(context.Background(), contracts.FilterForDomainDiscoveryParams{
		Source:    "brreg",
		NativeIDs: []string{"810202572", "999999999"},
		Companies: []contracts.CompanyLookup{
			{NativeID: "810202572", Name: "BORTIGARD AS", RawInputID: rawID1},
			{NativeID: "999999999", Name: "NEW AS", RawInputID: rawID2},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"999999999"}, result.NeedDiscovery)
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

func TestWriteDiscoveredDomainsBrregWritesRawInputBridge(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rawID := "7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"
	actionID := "25dbfdd1-6971-4498-8061-d296d1651986"
	domainID := "030c7e19-f08b-487c-a8c1-41cb969a0b59"

	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO domains`).
		WithArgs("bortigard.no").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(domainID))
	mock.ExpectExec(`INSERT INTO brreg_raw_input_domains`).
		WithArgs(rawID, domainID, actionID, "search", int16(80), pgxmock.AnyArg(), false).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	err := acts.WriteDiscoveredDomains(context.Background(), contracts.WriteDiscoveredDomainsParams{
		Source: "brreg",
		Companies: []contracts.CompanyLookup{{
			NativeID:   "810202572",
			Name:       "BORTIGARD AS",
			RawInputID: rawID,
		}},
		ActionIDs: map[string]string{"810202572": actionID},
		Discoveries: []contracts.DomainDiscovery{{
			NativeID:   "810202572",
			Domain:     "https://bortigard.no/",
			Signal:     "duckduckgo",
			Confidence: 80,
		}},
	})
	require.NoError(t, err)
}

func TestWriteDiscoveredDomainsBrregForceReactivatesRemovedConnection(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rawID := "7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"
	actionID := "25dbfdd1-6971-4498-8061-d296d1651986"
	domainID := "030c7e19-f08b-487c-a8c1-41cb969a0b59"

	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO domains`).
		WithArgs("bortigard.no").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(domainID))
	mock.ExpectExec(`INSERT INTO brreg_raw_input_domains`).
		WithArgs(rawID, domainID, actionID, "heuristic", int16(70), pgxmock.AnyArg(), true).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	err := acts.WriteDiscoveredDomains(context.Background(), contracts.WriteDiscoveredDomainsParams{
		Source: "brreg",
		Companies: []contracts.CompanyLookup{{
			NativeID:   "810202572",
			Name:       "BORTIGARD AS",
			RawInputID: rawID,
		}},
		ActionIDs: map[string]string{"810202572": actionID},
		Force:     true,
		Discoveries: []contracts.DomainDiscovery{{
			NativeID:   "810202572",
			Domain:     "bortigard.no",
			Signal:     "heuristic",
			Confidence: 70,
		}},
	})
	require.NoError(t, err)
}

func TestMarkRawInputActionEventsAppendsEvents(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	actionID := "7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"
	mock.ExpectExec(`INSERT INTO brreg_raw_input_action_events`).
		WithArgs(actionID, "running", "domain discovery started", "").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err := acts.MarkRawInputActionEvents(context.Background(), contracts.MarkRawInputActionEventsParams{
		ActionIDs: map[string]string{"810202572": actionID},
		Status:    "running",
		Message:   "domain discovery started",
	})
	require.NoError(t, err)
}
