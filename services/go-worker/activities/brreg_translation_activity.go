package activities

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

const brregTranslationLeaseMinutes = 10

type brregTranslationRow struct {
	ID         string
	RawPayload json.RawMessage
}

func (a *GoActivities) TranslateBrregBatch(ctx context.Context, params contracts.TranslateBrregBatchParams) (contracts.TranslateBrregBatchResult, error) {
	return contracts.TranslateBrregBatchResult{}, fmt.Errorf("TranslateBrregBatch has been replaced by PrepareBrregTranslationBatch, TranslateTermsWithDSPy, and WriteBrregTranslationBatch")
}

func (a *GoActivities) PrepareBrregTranslationBatch(ctx context.Context, params contracts.PrepareBrregTranslationBatchParams) (contracts.PrepareBrregTranslationBatchResult, error) {
	result, err := a.PrepareSourceTranslationBatch(ctx, contracts.PrepareSourceTranslationBatchParams{
		Source:        "brreg",
		IDs:           params.IDs,
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

func (a *GoActivities) claimBrregTranslationRows(ctx context.Context, params contracts.TranslateBrregBatchParams) ([]brregTranslationRow, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if len(params.IDs) > 0 {
		rows, err = a.pool.Query(ctx, `
			UPDATE brreg_company_raw_inputs
			SET translation_status = 'translating',
			    translation_attempts = translation_attempts + 1,
			    translation_error = NULL,
			    translation_lease_by = $1,
			    translation_lease_until = now() + ($2 * interval '1 minute'),
			    updated_at = now()
			WHERE id IN (
			    SELECT id FROM brreg_company_raw_inputs
			    WHERE (
			        translation_status IN ('pending', 'failed')
			        OR (translation_status = 'translating' AND (translation_lease_until < now() OR translation_lease_by = $1))
			    )
			    AND id::text = ANY($4)
			    ORDER BY created_at
			    LIMIT $3
			    FOR UPDATE SKIP LOCKED
			)
			RETURNING id::text, raw_payload
		`, params.WorkflowRunID, brregTranslationLeaseMinutes, params.BatchSize, params.IDs)
	} else {
		rows, err = a.pool.Query(ctx, `
			UPDATE brreg_company_raw_inputs
			SET translation_status = 'translating',
			    translation_attempts = translation_attempts + 1,
			    translation_error = NULL,
			    translation_lease_by = $1,
			    translation_lease_until = now() + ($2 * interval '1 minute'),
			    updated_at = now()
			WHERE id IN (
			    SELECT id FROM brreg_company_raw_inputs
			    WHERE translation_status = 'pending'
			       OR (translation_status = 'translating' AND (translation_lease_until < now() OR translation_lease_by = $1))
			    ORDER BY created_at
			    LIMIT $3
			    FOR UPDATE SKIP LOCKED
			)
			RETURNING id::text, raw_payload
		`, params.WorkflowRunID, brregTranslationLeaseMinutes, params.BatchSize)
	}
	if err != nil {
		return nil, fmt.Errorf("claim brreg translation rows: %w", err)
	}
	defer rows.Close()

	claimed := []brregTranslationRow{}
	for rows.Next() {
		var row brregTranslationRow
		if err := rows.Scan(&row.ID, &row.RawPayload); err != nil {
			return nil, fmt.Errorf("scan brreg translation row: %w", err)
		}
		claimed = append(claimed, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (a *GoActivities) lookupTranslationCacheBulk(ctx context.Context, terms []BrregTranslationTerm, params contracts.TranslateBrregBatchParams) (map[string]string, error) {
	cached := map[string]string{}
	if len(terms) == 0 {
		return cached, nil
	}

	categories := make([]string, 0, len(terms))
	hashes := make([]string, 0, len(terms))
	for _, term := range terms {
		categories = append(categories, term.Category)
		hashes = append(hashes, translationHash(term.Text))
	}

	rows, err := a.pool.Query(ctx, `
		SELECT tc.category, tc.original_hash, tc.translated_text
		FROM (
			SELECT DISTINCT category, original_hash
			FROM unnest($1::text[], $2::text[]) AS r(category, original_hash)
		) AS requested
		JOIN translation_cache tc
		  ON tc.category = requested.category
		 AND tc.original_hash = requested.original_hash
		 AND tc.source_lang = 'no'
		 AND tc.target_lang = 'en'
		 AND tc.prompt_version = $3
		 AND tc.model = $4
	`, categories, hashes, params.PromptVersion, params.Model)
	if err != nil {
		return nil, fmt.Errorf("lookup translation cache bulk: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var category, originalHash, translated string
		if err := rows.Scan(&category, &originalHash, &translated); err != nil {
			return nil, fmt.Errorf("scan translation cache bulk row: %w", err)
		}
		cached[translationCacheLookupHashKey(category, originalHash)] = translated
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate translation cache bulk rows: %w", err)
	}
	return cached, nil
}

func (a *GoActivities) writeBrregTranslationResults(
	ctx context.Context,
	params contracts.WriteBrregTranslationBatchParams,
	fx FXRateSet,
	translations BrregTranslationSet,
	newTranslations map[string]BrregTranslationTerm,
	successPayloads map[string]json.RawMessage,
	failures map[string]string,
) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin brreg translation write: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
			slog.Warn("rollback brreg translation write", "error", err)
		}
	}()

	if err := upsertTranslationCacheBulk(ctx, tx, params, translations, newTranslations); err != nil {
		return err
	}

	for id, payload := range successPayloads {
		var fxSource any
		var fxRateDate any
		if fx.Source != "" {
			fxSource = fx.Source
			fxRateDate = fx.RateDate
		}
		if _, err := tx.Exec(ctx, `
			UPDATE brreg_company_raw_inputs
			SET raw_payload_en = $2,
			    translation_status = 'translated',
			    translation_error = NULL,
			    translation_model = $3,
			    translation_prompt_version = $4,
			    translation_fx_source = $5,
			    translation_fx_rate_date = $6,
			    translated_at = now(),
			    translation_lease_by = NULL,
			    translation_lease_until = NULL,
			    updated_at = now()
			WHERE id = $1
		`, id, []byte(payload), params.Model, params.PromptVersion, fxSource, fxRateDate); err != nil {
			return fmt.Errorf("mark brreg row translated: %w", err)
		}
	}

	for id, reason := range failures {
		if _, err := tx.Exec(ctx, `
			UPDATE brreg_company_raw_inputs
			SET raw_payload_en = NULL,
			    translation_status = 'failed',
			    translation_error = $2,
			    translation_lease_by = NULL,
			    translation_lease_until = NULL,
			    updated_at = now()
			WHERE id = $1
		`, id, reason); err != nil {
			return fmt.Errorf("mark brreg row failed: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit brreg translation write: %w", err)
	}
	return nil
}

func upsertTranslationCacheBulk(
	ctx context.Context,
	tx pgx.Tx,
	params contracts.WriteBrregTranslationBatchParams,
	translations BrregTranslationSet,
	newTranslations map[string]BrregTranslationTerm,
) error {
	if len(newTranslations) == 0 {
		return nil
	}

	keys := make([]string, 0, len(newTranslations))
	for key := range newTranslations {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	categories := []string{}
	hashes := []string{}
	originals := []string{}
	translatedTexts := []string{}
	for _, key := range keys {
		term := newTranslations[key]
		translated := translations[term.Text]
		if translated == "" {
			continue
		}
		categories = append(categories, term.Category)
		hashes = append(hashes, translationHash(term.Text))
		originals = append(originals, term.Text)
		translatedTexts = append(translatedTexts, translated)
	}
	if len(categories) == 0 {
		return nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO translation_cache (
			category, original_hash, source_lang, target_lang, prompt_version, model, original_text, translated_text
		)
		SELECT category, original_hash, 'no', 'en', $5, $6, original_text, translated_text
		FROM unnest(
			$1::text[],
			$2::text[],
			$3::text[],
			$4::text[]
		) AS t(category, original_hash, original_text, translated_text)
		ON CONFLICT (category, original_hash, source_lang, target_lang, prompt_version, model)
		DO UPDATE SET translated_text = EXCLUDED.translated_text
	`, categories, hashes, originals, translatedTexts, params.PromptVersion, params.Model); err != nil {
		return fmt.Errorf("bulk upsert translation cache: %w", err)
	}
	return nil
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
