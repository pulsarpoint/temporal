package activities_test

import (
	"archive/zip"
	"context"
	"encoding/json"
	"os"
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

func TestImportAriregisterBulk_ImportsOfficialSimpleDataCSVZip(t *testing.T) {
	db := &recordingDB{}
	acts := activities.NewGoActivitiesWithDB(db)
	csvZip := writeTempZipCSV(t, "ettevotja_rekvisiidid__lihtandmed.csv", "\ufeffnimi;ariregistri_kood;ettevotja_oiguslik_vorm;ettevotja_oigusliku_vormi_alaliik;kmkr_nr;ettevotja_staatus;ettevotja_staatus_tekstina;ettevotja_esmakande_kpv;ettevotja_aadress;asukoht_ettevotja_aadressis;asukoha_ehak_kood;asukoha_ehak_tekstina;indeks_ettevotja_aadressis;ads_adr_id;ads_ads_oid;ads_normaliseeritud_taisaadress;teabesysteemi_link\n"+
		"Example Estonia OÜ;10000001;Osaühing;;EE100000001;R;Registrisse kantud;05.06.2023;;Regati pst 12;0596;Pirita linnaosa, Tallinn, Harju maakond;11911;2363082;;Harju maakond, Tallinn, Pirita linnaosa, Regati pst 12;https://ariregister.rik.ee/est/company/10000001\n")

	written, err := acts.ImportAriregisterBulk(context.Background(), contracts.ImportAriregisterBulkParams{
		RunID: "run-ari-csv",
		Files: []contracts.DownloadedSourceFile{{
			Source:     "ariregister",
			Dataset:    "simple-data",
			FilePath:   csvZip,
			SnapshotID: "snapshot-ari",
			Format:     "csv.zip",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Len(t, db.entries, 1)

	entry := db.entries[0]
	require.Equal(t, "10000001", entry.args[0])
	require.Equal(t, "10000001", entry.args[1])
	require.Equal(t, "Example Estonia OÜ", entry.args[2])
	require.Equal(t, "Registrisse kantud", entry.args[3])
	require.Equal(t, "Osaühing", entry.args[4])
	require.Equal(t, "EE100000001", entry.args[5])
	require.Nil(t, entry.args[6], "registry information link must not be treated as company website")
	require.Equal(t, "EE", entry.args[9])
	rawPayload := entry.args[10].([]byte)
	require.Equal(t, sha256Hex(rawPayload), entry.args[11])
	require.Equal(t, "run-ari-csv", entry.args[12])

	var payload map[string]any
	require.NoError(t, json.Unmarshal(rawPayload, &payload))
	require.Equal(t, "10000001", payload["registry_code"])
	require.Equal(t, "Example Estonia OÜ", payload["legal_name"])
	require.Equal(t, "Registrisse kantud", payload["registration_status"])
	require.Equal(t, "Osaühing", payload["legal_form"])
	require.Equal(t, "EE100000001", payload["vat_number"])
	require.NotContains(t, payload, "website")
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

func writeTempZipCSV(t *testing.T, name, content string) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "source-*.csv.zip")
	require.NoError(t, err)
	zipWriter := zip.NewWriter(file)
	entry, err := zipWriter.Create(name)
	require.NoError(t, err)
	_, err = entry.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, zipWriter.Close())
	require.NoError(t, file.Close())
	return file.Name()
}
