package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

const sourceTranslationLeaseMinutes = 10

type sourceTranslationRow struct {
	ID         string
	RawPayload json.RawMessage
}

type SourceTranslationConfig struct {
	SourceName      string
	TableName       string
	SourceLang      string
	TargetLang      string
	PromptVersion   string
	Model           string
	ClaimSQL        string
	WriteSuccessSQL string
	WriteFailureSQL string
	BuildPayloadEn  func(context.Context, json.RawMessage, SourceTranslationSet, FXRateSet) (json.RawMessage, error)
	ExtractTerms    func(json.RawMessage) ([]SourceTranslationTerm, error)
}

func (a *GoActivities) PrepareSourceTranslationBatch(ctx context.Context, params contracts.PrepareSourceTranslationBatchParams) (contracts.PrepareSourceTranslationBatchResult, error) {
	cfg, err := sourceTranslationConfig(params.Source)
	if err != nil {
		return contracts.PrepareSourceTranslationBatchResult{}, err
	}
	normalizeSourceTranslationPrepareParams(&params, cfg)

	rows, err := a.claimSourceTranslationRows(ctx, cfg, params)
	if err != nil {
		return contracts.PrepareSourceTranslationBatchResult{}, err
	}
	result := contracts.PrepareSourceTranslationBatchResult{
		Claimed:            len(rows),
		Rows:               make([]contracts.SourceTranslationRowPayload, 0, len(rows)),
		CachedTranslations: map[string]string{},
		MissesByCategory:   map[string][]contracts.TranslationItem{},
	}
	if len(rows) == 0 {
		return result, nil
	}

	needsFX := false
	uniqueTerms := map[string]SourceTranslationTerm{}
	for _, row := range rows {
		if cfg.SourceName == "brreg" && BrregPayloadNeedsFX(row.RawPayload) {
			needsFX = true
		}
		terms, err := cfg.ExtractTerms(row.RawPayload)
		if err != nil {
			return contracts.PrepareSourceTranslationBatchResult{}, err
		}
		result.Rows = append(result.Rows, contracts.SourceTranslationRowPayload{
			ID:         row.ID,
			RawPayload: row.RawPayload,
		})
		for _, term := range terms {
			uniqueTerms[termKey(term.Category, term.Text)] = term
		}
	}

	fx := FXRateSet{}
	if needsFX {
		if a.loadRates == nil {
			return contracts.PrepareSourceTranslationBatchResult{}, fmt.Errorf("%s FX loader is not configured", cfg.SourceName)
		}
		fx, err = a.loadRates(ctx, params.FXRateDate)
		if err != nil {
			return contracts.PrepareSourceTranslationBatchResult{}, fmt.Errorf("load %s FX rates: %w", cfg.SourceName, err)
		}
		result.FX = contracts.FXRatePayload{
			Source:   fx.Source,
			RateDate: fx.RateDate,
			EURPer:   fx.EURPer,
		}
	}

	sortedTerms := sortedSourceTerms(uniqueTerms)
	cached, err := a.lookupSourceTranslationCacheBulk(ctx, cfg, sortedTerms, params.PromptVersion, params.Model)
	if err != nil {
		return contracts.PrepareSourceTranslationBatchResult{}, err
	}

	categoryCounters := map[string]int{}
	for _, term := range sortedTerms {
		if translated, ok := cached[translationCacheLookupKey(term)]; ok {
			result.CachedTranslations[term.Text] = translated
			continue
		}
		next := categoryCounters[term.Category]
		categoryCounters[term.Category] = next + 1
		result.MissesByCategory[term.Category] = append(result.MissesByCategory[term.Category], contracts.TranslationItem{
			ID:   fmt.Sprintf("t%d", next),
			Text: term.Text,
		})
	}
	return result, nil
}

func (a *GoActivities) WriteSourceTranslationBatch(ctx context.Context, params contracts.WriteSourceTranslationBatchParams) (contracts.TranslateSourceBatchResult, error) {
	cfg, err := sourceTranslationConfig(params.Source)
	if err != nil {
		return contracts.TranslateSourceBatchResult{}, err
	}
	if params.PromptVersion == "" {
		params.PromptVersion = cfg.PromptVersion
	}
	if params.Model == "" {
		params.Model = cfg.Model
	}

	result := contracts.TranslateSourceBatchResult{Claimed: len(params.Rows)}
	translations := SourceTranslationSet{}
	for original, translated := range params.CachedTranslations {
		if translated != "" {
			translations[original] = translated
		}
	}
	newTranslations := map[string]SourceTranslationTerm{}
	for _, term := range params.NewTranslations {
		if term.Text == "" || term.Translation == "" {
			continue
		}
		translations[term.Text] = term.Translation
		newTranslations[termKey(term.Category, term.Text)] = SourceTranslationTerm{Category: term.Category, Text: term.Text}
	}

	fx := FXRateSet{
		Source:   params.FX.Source,
		RateDate: params.FX.RateDate,
		EURPer:   params.FX.EURPer,
	}
	successPayloads := map[string]json.RawMessage{}
	failures := map[string]string{}
	for _, row := range params.Rows {
		payload, err := cfg.BuildPayloadEn(ctx, row.RawPayload, translations, fx)
		if err != nil {
			failures[row.ID] = safeTranslationError(err)
			continue
		}
		successPayloads[row.ID] = payload
	}

	if err := a.writeSourceTranslationResults(ctx, cfg, params, fx, translations, newTranslations, successPayloads, failures); err != nil {
		return result, err
	}
	result.Translated = len(successPayloads)
	result.Failed = len(failures)
	return result, nil
}

func sourceTranslationConfig(source string) (SourceTranslationConfig, error) {
	base := SourceTranslationConfig{
		TargetLang:    "en",
		PromptVersion: "v1",
		Model:         "qwen3:6b",
	}
	switch source {
	case "brreg":
		base.SourceName = "brreg"
		base.TableName = "brreg_company_raw_inputs"
		base.SourceLang = "no"
		base.BuildPayloadEn = BuildBrregRawPayloadEn
		base.ExtractTerms = ExtractBrregTranslationTerms
	case "cvr":
		base.SourceName = "cvr"
		base.TableName = "cvr_company_raw_inputs"
		base.SourceLang = "da"
		base.BuildPayloadEn = BuildCVRRawPayloadEn
		base.ExtractTerms = ExtractCVRTranslationTerms
	case "ariregister":
		base.SourceName = "ariregister"
		base.TableName = "ariregister_company_raw_inputs"
		base.SourceLang = "et"
		base.BuildPayloadEn = BuildAriregisterRawPayloadEn
		base.ExtractTerms = ExtractAriregisterTranslationTerms
	default:
		return SourceTranslationConfig{}, fmt.Errorf("unsupported translation source %q", source)
	}
	base.WriteSuccessSQL = sourceWriteSuccessSQL(base.TableName)
	base.WriteFailureSQL = sourceWriteFailureSQL(base.TableName)
	return base, nil
}

func normalizeSourceTranslationPrepareParams(params *contracts.PrepareSourceTranslationBatchParams, cfg SourceTranslationConfig) {
	if params.BatchSize <= 0 {
		params.BatchSize = 50
	}
	if params.PromptVersion == "" {
		params.PromptVersion = cfg.PromptVersion
	}
	if params.Model == "" {
		params.Model = cfg.Model
	}
	if params.WorkflowRunID == "" {
		params.WorkflowRunID = "manual"
	}
}

func sourceClaimSQL(table string, includeFailed bool) string {
	predicate := "translation_status = 'pending'"
	if includeFailed {
		predicate = "translation_status IN ('pending', 'failed')"
	}
	return fmt.Sprintf(`
		UPDATE %s
		SET translation_status = 'translating',
		    translation_attempts = translation_attempts + 1,
		    translation_error = NULL,
		    translation_lease_by = $1,
		    translation_lease_until = now() + ($2 * interval '1 minute'),
		    updated_at = now()
		WHERE id IN (
		    SELECT id FROM %s
		    WHERE (
		        %s
		        OR (translation_status = 'translating' AND (translation_lease_until < now() OR translation_lease_by = $1))
		    )
		    AND ($4::text[] IS NULL OR id::text = ANY($4))
		    ORDER BY created_at
		    LIMIT $3
		    FOR UPDATE SKIP LOCKED
		)
		RETURNING id::text, raw_payload
	`, table, table, predicate)
}

func sourceWriteSuccessSQL(table string) string {
	return fmt.Sprintf(`
		UPDATE %s
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
	`, table)
}

func sourceWriteFailureSQL(table string) string {
	return fmt.Sprintf(`
		UPDATE %s
		SET raw_payload_en = NULL,
		    translation_status = 'failed',
		    translation_error = $2,
		    translation_lease_by = NULL,
		    translation_lease_until = NULL,
		    updated_at = now()
		WHERE id = $1
	`, table)
}

func (a *GoActivities) claimSourceTranslationRows(ctx context.Context, cfg SourceTranslationConfig, params contracts.PrepareSourceTranslationBatchParams) ([]sourceTranslationRow, error) {
	var ids any
	if len(params.IDs) > 0 {
		ids = params.IDs
	}
	claimSQL := cfg.ClaimSQL
	if claimSQL == "" {
		includeFailed := len(params.IDs) > 0 || cfg.SourceName != "brreg"
		claimSQL = sourceClaimSQL(cfg.TableName, includeFailed)
	}
	rows, err := a.pool.Query(ctx, claimSQL, params.WorkflowRunID, sourceTranslationLeaseMinutes, params.BatchSize, ids)
	if err != nil {
		return nil, fmt.Errorf("claim %s translation rows: %w", cfg.SourceName, err)
	}
	defer rows.Close()

	claimed := []sourceTranslationRow{}
	for rows.Next() {
		var row sourceTranslationRow
		if err := rows.Scan(&row.ID, &row.RawPayload); err != nil {
			return nil, fmt.Errorf("scan %s translation row: %w", cfg.SourceName, err)
		}
		claimed = append(claimed, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s translation rows: %w", cfg.SourceName, err)
	}
	return claimed, nil
}

func (a *GoActivities) lookupSourceTranslationCacheBulk(ctx context.Context, cfg SourceTranslationConfig, terms []SourceTranslationTerm, promptVersion, model string) (map[string]string, error) {
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
		 AND tc.source_lang = $3
		 AND tc.target_lang = $4
		 AND tc.prompt_version = $5
		 AND tc.model = $6
	`, categories, hashes, cfg.SourceLang, cfg.TargetLang, promptVersion, model)
	if err != nil {
		return nil, fmt.Errorf("lookup %s translation cache bulk: %w", cfg.SourceName, err)
	}
	defer rows.Close()
	for rows.Next() {
		var category, originalHash, translated string
		if err := rows.Scan(&category, &originalHash, &translated); err != nil {
			return nil, fmt.Errorf("scan %s translation cache bulk row: %w", cfg.SourceName, err)
		}
		cached[translationCacheLookupHashKey(category, originalHash)] = translated
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s translation cache bulk rows: %w", cfg.SourceName, err)
	}
	return cached, nil
}

func (a *GoActivities) writeSourceTranslationResults(
	ctx context.Context,
	cfg SourceTranslationConfig,
	params contracts.WriteSourceTranslationBatchParams,
	fx FXRateSet,
	translations SourceTranslationSet,
	newTranslations map[string]SourceTranslationTerm,
	successPayloads map[string]json.RawMessage,
	failures map[string]string,
) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s translation write: %w", cfg.SourceName, err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := upsertSourceTranslationCacheBulk(ctx, tx, cfg, params.PromptVersion, params.Model, translations, newTranslations); err != nil {
		return err
	}
	for id, payload := range successPayloads {
		var fxSource any
		var fxRateDate any
		if fx.Source != "" {
			fxSource = fx.Source
			fxRateDate = fx.RateDate
		}
		if _, err := tx.Exec(ctx, cfg.WriteSuccessSQL, id, []byte(payload), params.Model, params.PromptVersion, fxSource, fxRateDate); err != nil {
			return fmt.Errorf("mark %s row translated: %w", cfg.SourceName, err)
		}
	}
	for id, reason := range failures {
		if _, err := tx.Exec(ctx, cfg.WriteFailureSQL, id, reason); err != nil {
			return fmt.Errorf("mark %s row failed: %w", cfg.SourceName, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s translation write: %w", cfg.SourceName, err)
	}
	return nil
}

func upsertSourceTranslationCacheBulk(
	ctx context.Context,
	tx pgx.Tx,
	cfg SourceTranslationConfig,
	promptVersion string,
	model string,
	translations SourceTranslationSet,
	newTranslations map[string]SourceTranslationTerm,
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
		SELECT category, original_hash, $5, $6, $7, $8, original_text, translated_text
		FROM unnest(
			$1::text[],
			$2::text[],
			$3::text[],
			$4::text[]
		) AS t(category, original_hash, original_text, translated_text)
		ON CONFLICT (category, original_hash, source_lang, target_lang, prompt_version, model)
		DO UPDATE SET translated_text = EXCLUDED.translated_text
	`, categories, hashes, originals, translatedTexts, cfg.SourceLang, cfg.TargetLang, promptVersion, model); err != nil {
		return fmt.Errorf("bulk upsert %s translation cache: %w", cfg.SourceName, err)
	}
	return nil
}

func sortedSourceTerms(uniqueTerms map[string]SourceTranslationTerm) []SourceTranslationTerm {
	sortedKeys := make([]string, 0, len(uniqueTerms))
	for key := range uniqueTerms {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)
	sortedTerms := make([]SourceTranslationTerm, 0, len(sortedKeys))
	for _, key := range sortedKeys {
		sortedTerms = append(sortedTerms, uniqueTerms[key])
	}
	return sortedTerms
}

func safeTranslationError(error) string {
	return "translation failed for one or more fields"
}
