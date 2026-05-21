package activities

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
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
	if params.BatchSize <= 0 {
		params.BatchSize = 50
	}
	if params.PromptVersion == "" {
		params.PromptVersion = "v1"
	}
	if params.Model == "" {
		params.Model = "qwen3:6b"
	}
	if params.WorkflowRunID == "" {
		params.WorkflowRunID = "manual"
	}
	if a.translator == nil {
		return contracts.TranslateBrregBatchResult{}, fmt.Errorf("brreg translator is not configured")
	}
	if a.loadRates == nil {
		return contracts.TranslateBrregBatchResult{}, fmt.Errorf("brreg FX loader is not configured")
	}

	rows, err := a.claimBrregTranslationRows(ctx, params)
	if err != nil {
		return contracts.TranslateBrregBatchResult{}, err
	}
	result := contracts.TranslateBrregBatchResult{Claimed: len(rows)}
	if len(rows) == 0 {
		return result, nil
	}

	needsFX := false
	uniqueTerms := map[string]BrregTranslationTerm{}
	for _, row := range rows {
		if BrregPayloadNeedsFX(row.RawPayload) {
			needsFX = true
		}
		terms, err := ExtractBrregTranslationTerms(row.RawPayload)
		if err != nil {
			return result, err
		}
		for _, term := range terms {
			key := termKey(term.Category, term.Text)
			uniqueTerms[key] = term
		}
	}

	fx := FXRateSet{}
	if needsFX {
		fx, err = a.loadRates(ctx, params.FXRateDate)
		if err != nil {
			return result, fmt.Errorf("load brreg FX rates: %w", err)
		}
	}

	translations := BrregTranslationSet{}
	missesByCategory := map[string]map[string]string{}
	for _, term := range uniqueTerms {
		translated, ok, err := a.lookupTranslationCache(ctx, term, params)
		if err != nil {
			return result, err
		}
		if ok {
			translations[term.Text] = translated
			continue
		}
		if missesByCategory[term.Category] == nil {
			missesByCategory[term.Category] = map[string]string{}
		}
		missesByCategory[term.Category][term.Text] = ""
	}

	newTranslations := map[string]BrregTranslationTerm{}
	for category, inputs := range missesByCategory {
		translated, err := a.translator.TranslateMap(ctx, category, inputs)
		if err != nil {
			return result, err
		}
		for original, english := range translated {
			if english == "" {
				continue
			}
			translations[original] = english
			newTranslations[termKey(category, original)] = BrregTranslationTerm{Category: category, Text: original}
		}
	}

	successPayloads := map[string]json.RawMessage{}
	failures := map[string]string{}
	for _, row := range rows {
		payload, err := BuildBrregRawPayloadEn(ctx, row.RawPayload, translations, fx)
		if err != nil {
			failures[row.ID] = err.Error()
			continue
		}
		successPayloads[row.ID] = payload
	}

	if err := a.writeBrregTranslationResults(ctx, params, fx, translations, newTranslations, successPayloads, failures); err != nil {
		return result, err
	}
	result.Translated = len(successPayloads)
	result.Failed = len(failures)
	return result, nil
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

func (a *GoActivities) lookupTranslationCache(ctx context.Context, term BrregTranslationTerm, params contracts.TranslateBrregBatchParams) (string, bool, error) {
	var translated string
	err := a.pool.QueryRow(ctx, `
		SELECT translated_text
		FROM translation_cache
		WHERE category = $1
		  AND original_hash = $2
		  AND source_lang = 'no'
		  AND target_lang = 'en'
		  AND prompt_version = $3
		  AND model = $4
	`, term.Category, translationHash(term.Text), params.PromptVersion, params.Model).Scan(&translated)
	if err == nil {
		return translated, true, nil
	}
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	return "", false, fmt.Errorf("lookup translation cache: %w", err)
}

func (a *GoActivities) writeBrregTranslationResults(
	ctx context.Context,
	params contracts.TranslateBrregBatchParams,
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

	for _, term := range newTranslations {
		translated := translations[term.Text]
		if translated == "" {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO translation_cache (
				category, original_hash, source_lang, target_lang, prompt_version, model, original_text, translated_text
			)
			VALUES ($1, $2, 'no', 'en', $3, $4, $5, $6)
			ON CONFLICT (category, original_hash, source_lang, target_lang, prompt_version, model)
			DO UPDATE SET translated_text = EXCLUDED.translated_text
		`, term.Category, translationHash(term.Text), params.PromptVersion, params.Model, term.Text, translated); err != nil {
			return fmt.Errorf("upsert translation cache: %w", err)
		}
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

func termKey(category, text string) string {
	return category + "\x00" + text
}

func translationHash(text string) string {
	normalized := strings.ToLower(strings.TrimSpace(text))
	hash := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(hash[:])
}
