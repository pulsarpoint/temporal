package activities_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	mock.ExpectQuery(`UPDATE brreg_company_raw_inputs`).
		WithArgs("run-1", 10, 50, nil).
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
}

func TestPrepareBrregTranslationBatch_NormalRunPreservesPendingOnlyClaim(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithTranslationDeps(mock, nil, func(context.Context, string) (activities.FXRateSet, error) {
		return activities.FXRateSet{}, nil
	})

	mock.ExpectQuery(`translation_status = 'pending'`).
		WithArgs("run-1", 10, 50, nil).
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

	mock.ExpectQuery(`translation_status IN \('pending', 'failed'\)`).
		WithArgs("run-1", 10, 50, []string{"row-1"}).
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
	mock.ExpectExec(`UPDATE brreg_company_raw_inputs`).
		WithArgs("row-1", pgxmock.AnyArg(), "qwen3:6b", "v1", nil, nil).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
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

func testTranslationHash(text string) string {
	normalized := strings.ToLower(strings.TrimSpace(text))
	hash := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(hash[:])
}
