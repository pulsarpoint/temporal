package activities_test

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

func TestSourceTranslationWrite_MissingIndividualTermMarksRowFailedWithSafeError(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE cvr_company_raw_inputs`).
		WithArgs("row-1", "translation failed for one or more fields").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	result, err := acts.WriteSourceTranslationBatch(context.Background(), contracts.WriteSourceTranslationBatchParams{
		Source:        "cvr",
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		Rows: []contracts.SourceTranslationRowPayload{
			{ID: "row-1", RawPayload: []byte(`{"cvr_number":"12345678","company_type":"Anpartsselskab"}`)},
		},
	})
	require.NoError(t, err)
	require.Equal(t, contracts.TranslateSourceBatchResult{Claimed: 1, Failed: 1}, result)
}
