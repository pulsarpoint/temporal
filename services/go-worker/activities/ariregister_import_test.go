package activities_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

func TestImportAriregisterBulk_MergesBasicAndFinancialRecords(t *testing.T) {
	db := &recordingDB{}
	acts := activities.NewGoActivitiesWithDB(db)

	written, err := acts.ImportAriregisterBulk(context.Background(), contracts.ImportAriregisterBulkParams{
		RunID: "run-ari-001",
		Files: []contracts.DownloadedSourceFile{
			{
				Source:     "ariregister",
				Dataset:    "basic",
				FilePath:   "../testdata/ariregister_basic_sample.json",
				SnapshotID: "snapshot-ari",
				Format:     "json",
			},
			{
				Source:     "ariregister",
				Dataset:    "financials",
				FilePath:   "../testdata/ariregister_financials_sample.json",
				SnapshotID: "snapshot-ari",
				Format:     "json",
			},
		},
	})

	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Len(t, db.entries, 1)

	entry := db.entries[0]
	require.Contains(t, entry.query, "INSERT INTO ariregister_company_raw_inputs")
	require.Contains(t, entry.query, "ON CONFLICT (registry_code, payload_hash)")
	requireNoTranslationStatusInsert(t, entry.query)
	require.Equal(t, "10000001", entry.args[0])
	require.Equal(t, "10000001", entry.args[1])
	require.Equal(t, "Example Estonia OÜ", entry.args[2])
	require.Equal(t, "registered", entry.args[3])
	require.Equal(t, "Private limited company", entry.args[4])
	require.Equal(t, "EE100000001", entry.args[5])
	require.Equal(t, "https://example.ee", entry.args[6])
	require.Equal(t, "info@example.ee", entry.args[7])
	require.Equal(t, "+3725550100", entry.args[8])
	rawPayload := entry.args[10].([]byte)
	require.Equal(t, sha256Hex(rawPayload), entry.args[11])
	require.Equal(t, "run-ari-001", entry.args[12])

	var payload map[string]any
	require.NoError(t, json.Unmarshal(rawPayload, &payload))
	require.Equal(t, "10000001", payload["registry_code"])
	financials := payload["financials"].([]any)
	require.Len(t, financials, 1)
	report := financials[0].(map[string]any)
	require.Equal(t, float64(2024), report["year"])
	require.Equal(t, float64(1250000), report["revenue"])
	require.Equal(t, float64(175000), report["profit"])
	require.Equal(t, float64(18), report["employee_count"])
}

func TestImportAriregisterBulk_IncludesSourceDatasetsOnInsertError(t *testing.T) {
	db := newFailingRecordingDB()
	acts := activities.NewGoActivitiesWithDB(db)

	_, err := acts.ImportAriregisterBulk(context.Background(), contracts.ImportAriregisterBulkParams{
		RunID: "run-ari-error",
		Files: []contracts.DownloadedSourceFile{
			{
				Source:   "ariregister",
				Dataset:  "financials",
				FilePath: "../testdata/ariregister_financials_sample.json",
				Format:   "json",
			},
			{
				Source:   "ariregister",
				Dataset:  "basic",
				FilePath: "../testdata/ariregister_basic_sample.json",
				Format:   "json",
			},
		},
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "ariregister:basic")
	require.ErrorContains(t, err, "ariregister:financials")
	require.ErrorContains(t, err, "batch offset 0")
}
