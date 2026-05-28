package activities_test

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

func TestImportBrregBulkLimitsImportedEntities(t *testing.T) {
	db := &recordingDB{}
	acts := activities.NewGoActivitiesWithDB(db)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(acts.ImportBrregBulk)
	filePath := writeBrregBulkGzip(t, `{
		"_embedded": {
			"enheter": [
				{"organisasjonsnummer":"111111111","navn":"FIRST AS"},
				{"organisasjonsnummer":"222222222","navn":"SECOND AS"}
			]
		}
	}`)

	result, err := env.ExecuteActivity(acts.ImportBrregBulk, contracts.ImportBrregBulkParams{
		FilePath: filePath,
		RunID:    "run-brreg",
		Limit:    1,
	})
	require.NoError(t, err)
	var written int
	require.NoError(t, result.Get(&written))

	require.Equal(t, 1, written)
	require.Len(t, db.entries, 1)

	entry := db.entries[0]
	require.Contains(t, entry.query, "INSERT INTO brreg_company_raw_inputs")
	require.Equal(t, "111111111", entry.args[0])
	require.Equal(t, "FIRST AS", entry.args[1])
	require.Equal(t, "run-brreg", entry.args[5])
}

func writeBrregBulkGzip(t *testing.T, payload string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "brreg.json.gz")
	file, err := os.Create(path)
	require.NoError(t, err)
	writer := gzip.NewWriter(file)
	_, err = writer.Write([]byte(payload))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())
	return path
}
