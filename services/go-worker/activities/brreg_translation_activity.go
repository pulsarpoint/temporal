package activities

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

func (a *GoActivities) TranslateBrregBatch(ctx context.Context, params contracts.TranslateBrregBatchParams) (contracts.TranslateBrregBatchResult, error) {
	return contracts.TranslateBrregBatchResult{}, fmt.Errorf("TranslateBrregBatch has been replaced by PrepareBrregTranslationBatch, TranslateTermsWithDSPy, and WriteBrregTranslationBatch")
}

func (a *GoActivities) PrepareBrregTranslationBatch(ctx context.Context, params contracts.PrepareBrregTranslationBatchParams) (contracts.PrepareBrregTranslationBatchResult, error) {
	result, err := a.PrepareSourceTranslationBatch(ctx, contracts.PrepareSourceTranslationBatchParams{
		Source:        "brreg",
		IDs:           params.IDs,
		Filters:       params.Filters,
		PromptVersion: params.PromptVersion,
		Model:         params.Model,
		FXRateDate:    params.FXRateDate,
		WorkflowRunID: params.WorkflowRunID,
		BatchSize:     params.BatchSize,
	})
	return contracts.PrepareBrregTranslationBatchResult(result), err
}

func (a *GoActivities) WriteBrregTranslationBatch(ctx context.Context, params contracts.WriteBrregTranslationBatchParams) (contracts.TranslateBrregBatchResult, error) {
	result, err := a.WriteSourceTranslationBatch(ctx, contracts.WriteSourceTranslationBatchParams{
		Source:             "brreg",
		PromptVersion:      params.PromptVersion,
		Model:              params.Model,
		Rows:               params.Rows,
		FX:                 params.FX,
		CachedTranslations: params.CachedTranslations,
		NewTranslations:    params.NewTranslations,
	})
	return contracts.TranslateBrregBatchResult(result), err
}

func termKey(category, text string) string {
	return category + "\x00" + text
}

func translationCacheLookupKey(term BrregTranslationTerm) string {
	return translationCacheLookupHashKey(term.Category, translationHash(term.Text))
}

func translationCacheLookupHashKey(category, originalHash string) string {
	return category + "\x00" + originalHash
}

func translationHash(text string) string {
	normalized := strings.ToLower(strings.TrimSpace(text))
	hash := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(hash[:])
}
