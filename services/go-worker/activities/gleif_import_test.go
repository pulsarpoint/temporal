package activities_test

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

type batchEntry struct {
	query string
	args  []any
}

type recordingDB struct {
	entries []batchEntry
	execErr error
}

func (db *recordingDB) Begin(context.Context) (pgx.Tx, error) {
	panic("Begin is not used by import tests")
}

func (db *recordingDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	panic("Exec is not used by import tests")
}

func (db *recordingDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	panic("Query is not used by import tests")
}

func (db *recordingDB) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("QueryRow is not used by import tests")
}

func (db *recordingDB) SendBatch(_ context.Context, batch *pgx.Batch) pgx.BatchResults {
	db.entries = append(db.entries, extractBatchEntries(batch)...)
	return &recordingBatchResults{remaining: batch.Len(), err: db.execErr}
}

type recordingBatchResults struct {
	remaining int
	err       error
}

func (r *recordingBatchResults) Exec() (pgconn.CommandTag, error) {
	if r.err != nil {
		return pgconn.CommandTag{}, r.err
	}
	if r.remaining > 0 {
		r.remaining--
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (r *recordingBatchResults) Query() (pgx.Rows, error) {
	panic("Query is not used by import tests")
}

func (r *recordingBatchResults) QueryRow() pgx.Row {
	panic("QueryRow is not used by import tests")
}

func (r *recordingBatchResults) Close() error {
	return nil
}

func extractBatchEntries(batch *pgx.Batch) []batchEntry {
	batchValue := reflect.ValueOf(batch).Elem()
	queuedQueries := batchValue.FieldByName("QueuedQueries")
	if !queuedQueries.IsValid() {
		queuedQueries = batchValue.FieldByName("queuedQueries")
	}
	entries := make([]batchEntry, 0, queuedQueries.Len())
	for i := 0; i < queuedQueries.Len(); i++ {
		queryValue := queuedQueries.Index(i).Elem().FieldByName("SQL")
		if !queryValue.IsValid() {
			queryValue = queuedQueries.Index(i).Elem().FieldByName("query")
		}
		argsValue := queuedQueries.Index(i).Elem().FieldByName("Arguments")
		if !argsValue.IsValid() {
			argsValue = queuedQueries.Index(i).Elem().FieldByName("arguments")
		}
		query := reflect.NewAt(queryValue.Type(), unsafe.Pointer(queryValue.UnsafeAddr())).Elem().Interface().(string)
		args := reflect.NewAt(argsValue.Type(), unsafe.Pointer(argsValue.UnsafeAddr())).Elem().Interface().([]any)
		entries = append(entries, batchEntry{query: query, args: args})
	}
	return entries
}

func requireJSONContains(t *testing.T, raw []byte, assertions map[string]any) {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal(raw, &payload))
	for key, expected := range assertions {
		require.Equal(t, expected, payload[key])
	}
}

func sha256Hex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func TestImportGLEIFGoldenCopy_InsertsValidRecordsAndSkipsMissingLEI(t *testing.T) {
	db := &recordingDB{}
	acts := activities.NewGoActivitiesWithDB(db)

	written, err := acts.ImportGLEIFGoldenCopy(context.Background(), contracts.ImportGLEIFGoldenCopyParams{
		RunID: "run-gleif-001",
		Files: []contracts.DownloadedSourceFile{{
			Source:     "gleif",
			Dataset:    "lei2",
			FilePath:   "../testdata/gleif_lei2_sample.json",
			SnapshotID: "snapshot-1",
			Format:     "json",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, 2, written)
	require.Len(t, db.entries, 2)

	first := db.entries[0]
	require.Contains(t, first.query, "INSERT INTO gleif_company_raw_inputs")
	require.Contains(t, first.query, "ON CONFLICT (lei, payload_hash)")
	require.Equal(t, "5493001KJTIIGC8Y1R12", first.args[0])
	require.Equal(t, "5493001KJTIIGC8Y1R12", first.args[1])
	require.Equal(t, "ACME GLOBAL LTD", first.args[2])
	require.Equal(t, "ACTIVE", first.args[3])
	require.Equal(t, "GB", first.args[4])
	rawPayload := first.args[5].([]byte)
	require.Equal(t, sha256Hex(rawPayload), first.args[6])
	require.Equal(t, "run-gleif-001", first.args[7])
	requireJSONContains(t, rawPayload, map[string]any{
		"id": "5493001KJTIIGC8Y1R12",
	})
}

func TestImportGLEIFGoldenCopy_UsesDirectArrayFallback(t *testing.T) {
	path := writeTempJSON(t, []map[string]any{{
		"lei":                 "506700GE1G29325QX363",
		"legalName":           "ARRAY FALLBACK PLC",
		"entityStatus":        "ACTIVE",
		"headquartersCountry": "US",
	}})
	db := &recordingDB{}
	acts := activities.NewGoActivitiesWithDB(db)

	written, err := acts.ImportGLEIFGoldenCopy(context.Background(), contracts.ImportGLEIFGoldenCopyParams{
		RunID: "run-gleif-array",
		Files: []contracts.DownloadedSourceFile{{
			Source:   "gleif",
			Dataset:  "lei2",
			FilePath: path,
			Format:   "json",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Len(t, db.entries, 1)
	require.Equal(t, "506700GE1G29325QX363", db.entries[0].args[1])
}

func TestImportGLEIFGoldenCopy_ParsesOfficialGoldenCopyRecordsShape(t *testing.T) {
	path := writeTempJSON(t, map[string]any{
		"records": []map[string]any{{
			"LEI": map[string]any{"$": "54930084UKLVMY22DS16"},
			"Entity": map[string]any{
				"LegalName":    map[string]any{"$": "OFFICIAL GLEIF LTD"},
				"EntityStatus": map[string]any{"$": "ACTIVE"},
				"HeadquartersAddress": map[string]any{
					"Country": map[string]any{"$": "GB"},
				},
			},
		}},
	})
	db := &recordingDB{}
	acts := activities.NewGoActivitiesWithDB(db)

	written, err := acts.ImportGLEIFGoldenCopy(context.Background(), contracts.ImportGLEIFGoldenCopyParams{
		RunID: "run-gleif-official",
		Files: []contracts.DownloadedSourceFile{{
			Source:   "gleif",
			Dataset:  "lei2",
			FilePath: path,
			Format:   "json",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Len(t, db.entries, 1)
	require.Equal(t, "54930084UKLVMY22DS16", db.entries[0].args[1])
	require.Equal(t, "OFFICIAL GLEIF LTD", db.entries[0].args[2])
	require.Equal(t, "ACTIVE", db.entries[0].args[3])
	require.Equal(t, "GB", db.entries[0].args[4])
}

func TestImportGLEIFGoldenCopy_SniffsZipBytesWhenFormatIsJSON(t *testing.T) {
	path := writeTempZipJSON(t, "latest.json", map[string]any{
		"data": []map[string]any{{
			"id": "984500E9CB074M63DE44",
			"attributes": map[string]any{
				"entity": map[string]any{
					"legalName": map[string]any{"name": "ZIPPED GLEIF LTD"},
					"status":    "ACTIVE",
					"headquartersAddress": map[string]any{
						"country": "US",
					},
				},
			},
		}},
	})
	db := &recordingDB{}
	acts := activities.NewGoActivitiesWithDB(db)

	written, err := acts.ImportGLEIFGoldenCopy(context.Background(), contracts.ImportGLEIFGoldenCopyParams{
		RunID: "run-gleif-zip-json",
		Files: []contracts.DownloadedSourceFile{{
			Source:   "gleif",
			Dataset:  "lei2",
			FilePath: path,
			Format:   "json",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Len(t, db.entries, 1)
	require.Equal(t, "984500E9CB074M63DE44", db.entries[0].args[1])
	require.Equal(t, "ZIPPED GLEIF LTD", db.entries[0].args[2])
}

func TestImportGLEIFGoldenCopy_VerifiesSourceSHA256BeforeParsing(t *testing.T) {
	path := writeTempJSON(t, []map[string]any{{
		"lei":       "549300E9W6GHTJY1Y829",
		"legalName": "HASH CHECK PLC",
	}})
	db := &recordingDB{}
	acts := activities.NewGoActivitiesWithDB(db)

	written, err := acts.ImportGLEIFGoldenCopy(context.Background(), contracts.ImportGLEIFGoldenCopyParams{
		RunID: "run-gleif-hash",
		Files: []contracts.DownloadedSourceFile{{
			Source:   "gleif",
			Dataset:  "lei2",
			FilePath: path,
			SHA256:   strings.Repeat("0", 64),
			Format:   "json",
		}},
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "sha256 mismatch")
	require.Equal(t, 0, written)
	require.Empty(t, db.entries)
}

func newFailingRecordingDB() *recordingDB {
	return &recordingDB{execErr: errors.New("insert failed")}
}

func requireNoTranslationStatusInsert(t *testing.T, query string) {
	t.Helper()
	require.False(t, strings.Contains(strings.ToLower(query), "translation_status"), "translation_status should use the DB default")
}

func writeTempZipJSON(t *testing.T, name string, value any) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "source-*.json")
	require.NoError(t, err)
	zipWriter := zip.NewWriter(file)
	entry, err := zipWriter.Create(name)
	require.NoError(t, err)
	require.NoError(t, json.NewEncoder(entry).Encode(value))
	require.NoError(t, zipWriter.Close())
	require.NoError(t, file.Close())
	return file.Name()
}
