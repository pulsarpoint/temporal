package activities_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

func writeTempJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	file, err := os.CreateTemp(t.TempDir(), "source-*.json")
	require.NoError(t, err)
	_, err = file.Write(raw)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	return file.Name()
}

func TestImportCVRBulk_InsertsCompanyPayloadFromJSONL(t *testing.T) {
	db := &recordingDB{}
	acts := activities.NewGoActivitiesWithDB(db)

	written, err := acts.ImportCVRBulk(context.Background(), contracts.ImportCVRBulkParams{
		RunID: "run-cvr-001",
		Files: []contracts.DownloadedSourceFile{{
			Source:     "cvr",
			Dataset:    "companies",
			FilePath:   "../testdata/cvr_company_sample.jsonl",
			SnapshotID: "snapshot-cvr",
			Format:     "jsonl",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Len(t, db.entries, 1)

	entry := db.entries[0]
	require.Contains(t, entry.query, "INSERT INTO cvr_company_raw_inputs")
	require.Contains(t, entry.query, "ON CONFLICT (cvr_number, payload_hash)")
	requireNoTranslationStatusInsert(t, entry.query)
	require.Equal(t, "12345678", entry.args[0])
	require.Equal(t, "12345678", entry.args[1])
	require.Equal(t, "Example Denmark ApS", entry.args[2])
	require.Equal(t, "NORMAL", entry.args[3])
	require.Equal(t, "Anpartsselskab", entry.args[4])
	require.Equal(t, "https://example.dk", entry.args[5])
	require.Equal(t, "hello@example.dk", entry.args[6])
	require.Equal(t, "+4512345678", entry.args[7])
	rawPayload := entry.args[9].([]byte)
	require.Equal(t, sha256Hex(rawPayload), entry.args[10])
	require.Equal(t, "run-cvr-001", entry.args[11])

	var payload map[string]any
	require.NoError(t, json.Unmarshal(rawPayload, &payload))
	require.Equal(t, "12345678", payload["cvr_number"])
	require.NotEmpty(t, payload["roles"])
	require.NotEmpty(t, payload["owners"])
	require.NotEmpty(t, payload["beneficial_owners"])
	require.NotEmpty(t, payload["financials"])
}

func TestImportCVRBulk_AccumulatesFragmentsForSameCVR(t *testing.T) {
	path := writeTempJSONL(t,
		map[string]any{
			"cvr_number":   "87654321",
			"company_name": "Merged Denmark ApS",
			"roles":        []map[string]any{{"name": "Alice Example", "role": "director"}},
			"owners":       []map[string]any{{"name": "Owner A", "ownership_percent": 60}},
			"financials":   []map[string]any{{"year": 2023, "revenue": 1000}},
		},
		map[string]any{
			"cvr_number":        "87654321",
			"roles":             []map[string]any{{"name": "Bob Example", "role": "chair"}},
			"owners":            []map[string]any{{"name": "Owner B", "ownership_percent": 40}},
			"beneficial_owners": []map[string]any{{"name": "Beneficial B", "ownership_percent": 40}},
			"financials":        []map[string]any{{"year": 2024, "revenue": 2000}},
		},
	)
	db := &recordingDB{}
	acts := activities.NewGoActivitiesWithDB(db)

	written, err := acts.ImportCVRBulk(context.Background(), contracts.ImportCVRBulkParams{
		RunID: "run-cvr-merge",
		Files: []contracts.DownloadedSourceFile{{
			Source:   "cvr",
			Dataset:  "companies",
			FilePath: path,
			Format:   "jsonl",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Len(t, db.entries, 1)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(db.entries[0].args[9].([]byte), &payload))
	require.Len(t, payload["roles"], 2)
	require.Len(t, payload["owners"], 2)
	require.Len(t, payload["beneficial_owners"], 1)
	require.Len(t, payload["financials"], 2)
	require.Equal(t, "Alice Example", payload["roles"].([]any)[0].(map[string]any)["name"])
	require.Equal(t, "Bob Example", payload["roles"].([]any)[1].(map[string]any)["name"])
}

func TestImportCVRBulk_IncludesSourceDatasetsOnInsertError(t *testing.T) {
	db := newFailingRecordingDB()
	acts := activities.NewGoActivitiesWithDB(db)

	_, err := acts.ImportCVRBulk(context.Background(), contracts.ImportCVRBulkParams{
		RunID: "run-cvr-error",
		Files: []contracts.DownloadedSourceFile{{
			Source:   "cvr",
			Dataset:  "companies",
			FilePath: "../testdata/cvr_company_sample.jsonl",
			Format:   "jsonl",
		}},
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "cvr:companies")
	require.ErrorContains(t, err, "batch offset 0")
}

func writeTempJSONL(t *testing.T, values ...any) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "source-*.jsonl")
	require.NoError(t, err)
	encoder := json.NewEncoder(file)
	for _, value := range values {
		require.NoError(t, encoder.Encode(value))
	}
	require.NoError(t, file.Close())
	return file.Name()
}
