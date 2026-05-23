package activities_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

func TestPrepareBrregTranslationBatch_LooksUpTranslationCacheInBulk(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithTranslationDeps(mock, nil, func(context.Context, string) (activities.FXRateSet, error) {
		return activities.FXRateSet{}, nil
	})
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	mock.ExpectQuery(`UPDATE brreg_company_raw_inputs`).
		WithArgs("run-1", 10, 50, nil, []string{"notdone"}, true, "input").
		WillReturnRows(pgxmock.NewRows([]string{"id", "raw_payload"}).
			AddRow("row-1", []byte(`{
				"organisasjonsform":{"beskrivelse":"Aksjeselskap"},
				"aktivitet":["Eie aksjer"]
			}`)).
			AddRow("row-2", []byte(`{
				"organisasjonsform":{"beskrivelse":"Aksjeselskap"},
				"aktivitet":["Konsulentvirksomhet"]
			}`)))

	categories := []string{"activity", "activity", "org_form"}
	hashes := []string{
		testTranslationHash("Eie aksjer"),
		testTranslationHash("Konsulentvirksomhet"),
		testTranslationHash("Aksjeselskap"),
	}
	mock.ExpectQuery(`FROM unnest\(\$1::text\[\], \$2::text\[\]\) AS r\(category, original_hash\)`).
		WithArgs(categories, hashes, "no", "en", "v1", "qwen3:6b").
		WillReturnRows(pgxmock.NewRows([]string{"category", "original_hash", "translated_text"}).
			AddRow("activity", testTranslationHash("Eie aksjer"), "Own shares").
			AddRow("org_form", testTranslationHash("Aksjeselskap"), "Limited company"))

	result, err := acts.PrepareBrregTranslationBatch(context.Background(), contracts.PrepareBrregTranslationBatchParams{
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		WorkflowRunID: "run-1",
		BatchSize:     50,
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"org_form\x00Aksjeselskap": "Limited company",
		"activity\x00Eie aksjer":   "Own shares",
	}, result.CachedTranslations)
	require.Equal(t, []contracts.TranslationItem{
		{ID: "t0", Text: "Konsulentvirksomhet"},
	}, result.MissesByCategory["activity"])
	logOutput := logs.String()
	require.Contains(t, logOutput, `"msg":"source translation cache stats"`)
	require.Contains(t, logOutput, `"source":"brreg"`)
	require.Contains(t, logOutput, `"category":"activity"`)
	require.Contains(t, logOutput, `"hits":1`)
	require.Contains(t, logOutput, `"misses":1`)
	require.Contains(t, logOutput, `"category":"org_form"`)
	require.Contains(t, logOutput, `"hits":1`)
	require.Contains(t, logOutput, `"misses":0`)
}

func TestPrepareBrregTranslationBatch_NormalRunPreservesPendingOnlyClaim(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithTranslationDeps(mock, nil, func(context.Context, string) (activities.FXRateSet, error) {
		return activities.FXRateSet{}, nil
	})

	mock.ExpectQuery(`v_brreg_raw_input_action_attributes`).
		WithArgs("run-1", 10, 50, nil, []string{"notdone"}, true, "input").
		WillReturnRows(pgxmock.NewRows([]string{"id", "raw_payload"}))

	result, err := acts.PrepareBrregTranslationBatch(context.Background(), contracts.PrepareBrregTranslationBatchParams{
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		WorkflowRunID: "run-1",
		BatchSize:     50,
	})
	require.NoError(t, err)
	require.Zero(t, result.Claimed)
}

func TestPrepareBrregTranslationBatch_ExplicitIDsCanRetryFailedRows(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithTranslationDeps(mock, nil, func(context.Context, string) (activities.FXRateSet, error) {
		return activities.FXRateSet{}, nil
	})

	mock.ExpectQuery(`latest_translation_action_status = ANY`).
		WithArgs("run-1", 10, 50, []string{"row-1"}, []string{"notdone", "failed", "cancelled", "skipped"}, true, "input").
		WillReturnRows(pgxmock.NewRows([]string{"id", "raw_payload"}))

	result, err := acts.PrepareBrregTranslationBatch(context.Background(), contracts.PrepareBrregTranslationBatchParams{
		IDs:           []string{"row-1"},
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		WorkflowRunID: "run-1",
		BatchSize:     50,
	})
	require.NoError(t, err)
	require.Zero(t, result.Claimed)
}

func TestPrepareBrregTranslationBatch_ActionStatusFilterCanRetryFailedRows(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithTranslationDeps(mock, nil, func(context.Context, string) (activities.FXRateSet, error) {
		return activities.FXRateSet{}, nil
	})

	mock.ExpectQuery(`latest_translation_action_status = ANY`).
		WithArgs("run-1", 10, 50, nil, []string{"failed"}, false, "input").
		WillReturnRows(pgxmock.NewRows([]string{"id", "raw_payload"}))

	result, err := acts.PrepareBrregTranslationBatch(context.Background(), contracts.PrepareBrregTranslationBatchParams{
		Filters:       map[string]string{"translation_action_status": "failed"},
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		WorkflowRunID: "run-1",
		BatchSize:     50,
	})
	require.NoError(t, err)
	require.Zero(t, result.Claimed)
}

func TestWriteBrregTranslationBatch_UpsertsTranslationCacheInBulk(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rawPayload := []byte(`{
		"organisasjonsnummer":"831909242",
		"navn":"CERI HOLDING AS",
		"organisasjonsform":{"kode":"AS","beskrivelse":"Aksjeselskap"},
		"aktivitet":["Eie aksjer"]
	}`)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO translation_cache`).
		WithArgs(
			[]string{"activity", "org_form"},
			[]string{testTranslationHash("Eie aksjer"), testTranslationHash("Aksjeselskap")},
			[]string{"Eie aksjer", "Aksjeselskap"},
			[]string{"Own shares", "Limited company"},
			"no",
			"en",
			"v1",
			"qwen3:6b",
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 2))
	mock.ExpectExec(`translation_status = 'translated'`).
		WithArgs("row-1", pgxmock.AnyArg(), "qwen3:6b", "v1", nil, nil).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec(`INSERT INTO brreg_raw_input_action_events`).
		WithArgs("row-1", "succeeded", "translation completed", nil).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	result, err := acts.WriteBrregTranslationBatch(context.Background(), contracts.WriteBrregTranslationBatchParams{
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		Rows: []contracts.BrregTranslationRowPayload{
			{ID: "row-1", RawPayload: rawPayload},
		},
		NewTranslations: []contracts.BrregTranslatedTerm{
			{Category: "org_form", Text: "Aksjeselskap", Translation: "Limited company"},
			{Category: "activity", Text: "Eie aksjer", Translation: "Own shares"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, contracts.TranslateBrregBatchResult{Claimed: 1, Translated: 1}, result)
}

func TestWriteBrregTranslationBatch_MissingTermMarksActionFailed(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rawPayload := []byte(`{
		"organisasjonsnummer":"831909242",
		"navn":"CERI HOLDING AS",
		"organisasjonsform":{"kode":"AS","beskrivelse":"Aksjeselskap"}
	}`)

	mock.ExpectBegin()
	mock.ExpectExec(`translation_status = 'failed'`).
		WithArgs("row-1", "translation failed for one or more fields").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec(`INSERT INTO brreg_raw_input_action_events`).
		WithArgs("row-1", "failed", "translation failed", "translation failed for one or more fields").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	result, err := acts.WriteBrregTranslationBatch(context.Background(), contracts.WriteBrregTranslationBatchParams{
		PromptVersion: "v1",
		Model:         "qwen3:6b",
		Rows: []contracts.BrregTranslationRowPayload{
			{ID: "row-1", RawPayload: rawPayload},
		},
	})
	require.NoError(t, err)
	require.Equal(t, contracts.TranslateBrregBatchResult{Claimed: 1, Failed: 1}, result)
}

func testTranslationHash(text string) string {
	normalized := strings.ToLower(strings.TrimSpace(text))
	hash := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(hash[:])
}
