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

func TestPrepareSourceTranslationBatch_NormalRunPreservesPendingOnlyClaim(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	mock.ExpectQuery(`translation_status = 'pending'`).
		WithArgs("run-1", 10, 50, nil).
		WillReturnRows(pgxmock.NewRows([]string{"id", "raw_payload"}))

	result, err := acts.PrepareSourceTranslationBatch(context.Background(), contracts.PrepareSourceTranslationBatchParams{
		Source:        "cvr",
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		WorkflowRunID: "run-1",
		BatchSize:     50,
	})
	require.NoError(t, err)
	require.Zero(t, result.Claimed)
}

func TestPrepareSourceTranslationBatch_ExplicitIDsCanRetryFailedRows(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	mock.ExpectQuery(`translation_status IN \('pending', 'failed'\)`).
		WithArgs("run-1", 10, 50, []string{"row-1"}).
		WillReturnRows(pgxmock.NewRows([]string{"id", "raw_payload"}))

	result, err := acts.PrepareSourceTranslationBatch(context.Background(), contracts.PrepareSourceTranslationBatchParams{
		Source:        "cvr",
		IDs:           []string{"row-1"},
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		WorkflowRunID: "run-1",
		BatchSize:     50,
	})
	require.NoError(t, err)
	require.Zero(t, result.Claimed)
}

func TestPrepareSourceTranslationBatch_BrregUsesLifecycleState(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	mock.ExpectQuery(`bri.state = \$7`).
		WithArgs("run-1", 10, 50, nil, []string{"notdone"}, true, "input").
		WillReturnRows(pgxmock.NewRows([]string{"id", "raw_payload"}))

	result, err := acts.PrepareSourceTranslationBatch(context.Background(), contracts.PrepareSourceTranslationBatchParams{
		Source:        "brreg",
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		WorkflowRunID: "run-1",
		BatchSize:     50,
	})
	require.NoError(t, err)
	require.Zero(t, result.Claimed)
}

func TestPrepareSourceTranslationBatch_BrregStateFilterUsesLifecycleState(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	mock.ExpectQuery(`bri.state = \$7`).
		WithArgs("run-1", 10, 50, nil, []string{"notdone"}, false, "input").
		WillReturnRows(pgxmock.NewRows([]string{"id", "raw_payload"}))

	result, err := acts.PrepareSourceTranslationBatch(context.Background(), contracts.PrepareSourceTranslationBatchParams{
		Source:        "brreg",
		Filters:       map[string]string{"state": "raw"},
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		WorkflowRunID: "run-1",
		BatchSize:     50,
	})
	require.NoError(t, err)
	require.Zero(t, result.Claimed)
}

func TestPrepareSourceTranslationBatch_BrregTranslationActionFilter(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	mock.ExpectQuery(`latest_translation_action_status = ANY`).
		WithArgs("run-1", 10, 50, nil, []string{"failed"}, false, "input").
		WillReturnRows(pgxmock.NewRows([]string{"id", "raw_payload"}))

	result, err := acts.PrepareSourceTranslationBatch(context.Background(), contracts.PrepareSourceTranslationBatchParams{
		Source:        "brreg",
		Filters:       map[string]string{"translation_action_status": "failed"},
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		WorkflowRunID: "run-1",
		BatchSize:     50,
	})
	require.NoError(t, err)
	require.Zero(t, result.Claimed)
}
