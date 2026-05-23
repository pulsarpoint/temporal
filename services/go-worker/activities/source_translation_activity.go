package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

const sourceTranslationLeaseMinutes = 10

type sourceTranslationRow struct {
	ID         string
	RawPayload json.RawMessage
}

type sourceTranslationCacheStat struct {
	Hits   int
	Misses int
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
	categoryStats := map[string]sourceTranslationCacheStat{}
	for _, term := range sortedTerms {
		if translated, ok := cached[translationCacheLookupKey(term)]; ok {
			result.CachedTranslations[termKey(term.Category, term.Text)] = translated
			stat := categoryStats[term.Category]
			stat.Hits++
			categoryStats[term.Category] = stat
			continue
		}
		next := categoryCounters[term.Category]
		categoryCounters[term.Category] = next + 1
		result.MissesByCategory[term.Category] = append(result.MissesByCategory[term.Category], contracts.TranslationItem{
			ID:   fmt.Sprintf("t%d", next),
			Text: term.Text,
		})
		stat := categoryStats[term.Category]
		stat.Misses++
		categoryStats[term.Category] = stat
	}
	logSourceTranslationCacheStats(ctx, cfg, params, len(rows), len(sortedTerms), categoryStats)
	return result, nil
}

func logSourceTranslationCacheStats(
	ctx context.Context,
	cfg SourceTranslationConfig,
	params contracts.PrepareSourceTranslationBatchParams,
	rowCount int,
	uniqueTermCount int,
	stats map[string]sourceTranslationCacheStat,
) {
	categories := make([]string, 0, len(stats))
	for category := range stats {
		categories = append(categories, category)
	}
	sort.Strings(categories)
	for _, category := range categories {
		stat := stats[category]
		total := stat.Hits + stat.Misses
		hitRate := 0.0
		if total > 0 {
			hitRate = float64(stat.Hits) / float64(total)
		}
		slog.InfoContext(
			ctx,
			"source translation cache stats",
			"source", cfg.SourceName,
			"category", category,
			"rows", rowCount,
			"unique_terms", uniqueTermCount,
			"hits", stat.Hits,
			"misses", stat.Misses,
			"hit_rate", hitRate,
			"prompt_version", params.PromptVersion,
			"model", params.Model,
		)
	}
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
	for key, translated := range params.CachedTranslations {
		if translated != "" {
			translations[key] = translated
		}
	}
	newTranslations := map[string]SourceTranslationTerm{}
	for _, term := range params.NewTranslations {
		if term.Text == "" || term.Translation == "" {
			continue
		}
		key := termKey(term.Category, term.Text)
		translations[key] = term.Translation
		newTranslations[key] = SourceTranslationTerm{Category: term.Category, Text: term.Text}
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
	stalePredicate := "translation_status = 'translating'"
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
		        OR (%s AND (translation_lease_until < now() OR translation_lease_by = $1))
		    )
		    AND ($4::text[] IS NULL OR id::text = ANY($4))
		    ORDER BY created_at
		    LIMIT $3
		    FOR UPDATE SKIP LOCKED
		)
		RETURNING id::text, raw_payload
	`, table, table, predicate, stalePredicate)
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

func brregSourceClaimSQL() string {
	return `
		WITH candidate AS (
		    SELECT bri.id
		    FROM brreg_company_raw_inputs bri
		    JOIN v_brreg_raw_input_action_attributes baa ON baa.raw_input_id = bri.id
		    WHERE bri.state = $7
		      AND ($4::text[] IS NULL OR bri.id::text = ANY($4))
		      AND (
		          baa.latest_translation_action_status = ANY($5::text[])
		          OR (
		              $6::boolean
		              AND baa.latest_translation_action_status = 'running'
		              AND (bri.translation_lease_until < now() OR bri.translation_lease_by = $1)
		          )
		      )
		    ORDER BY bri.created_at
		    LIMIT $3
		    FOR UPDATE SKIP LOCKED
		),
		claimed AS (
		    UPDATE brreg_company_raw_inputs bri
		    SET translation_status = 'translating',
		        translation_attempts = translation_attempts + 1,
		        translation_error = NULL,
		        translation_lease_by = $1,
		        translation_lease_until = now() + ($2 * interval '1 minute'),
		        updated_at = now()
		    FROM candidate c
		    WHERE bri.id = c.id
		    RETURNING bri.id, bri.raw_payload, bri.payload_hash
		),
		next_attempt AS (
		    SELECT
		        c.id,
		        c.raw_payload,
		        c.payload_hash,
		        COALESCE(MAX(a.attempt), 0) + 1 AS attempt
		    FROM claimed c
		    LEFT JOIN brreg_raw_input_actions a
		      ON a.raw_input_id = c.id
		     AND a.action_type = 'translate'
		    GROUP BY c.id, c.raw_payload, c.payload_hash
		),
		created_actions AS (
		    INSERT INTO brreg_raw_input_actions (
		        raw_input_id,
		        action_type,
		        attempt,
		        payload_hash,
		        trigger,
		        worker_id,
		        workflow_run_id,
		        metadata
		    )
		    SELECT
		        id,
		        'translate',
		        attempt,
		        payload_hash,
		        'workflow',
		        $1,
		        $1,
		        jsonb_build_object('source', 'data-pipelines')
		    FROM next_attempt
		    RETURNING id AS action_id, raw_input_id
		),
		action_events AS (
		    INSERT INTO brreg_raw_input_action_events (action_id, status, message, metadata)
		    SELECT
		        action_id,
		        'running',
		        'translation worker claimed row',
		        '{}'::jsonb
		    FROM created_actions
		)
		SELECT id::text, raw_payload
		FROM next_attempt
	`
}

var brregTranslationActionStatuses = map[string]bool{
	"notdone":   true,
	"queued":    true,
	"running":   true,
	"succeeded": true,
	"failed":    true,
	"skipped":   true,
	"cancelled": true,
}

var brregTranslationLifecycleStates = map[string]bool{
	"input":                true,
	"suggestion_submitted": true,
	"completed":            true,
	"superseded":           true,
}

func brregTranslationClaimFilters(filters map[string]string, includeFailed bool) ([]string, bool, string, error) {
	lifecycleState := "input"
	statuses := []string{"notdone"}
	allowStaleRunning := true
	if includeFailed {
		statuses = []string{"notdone", "failed", "cancelled", "skipped"}
	}
	if filters == nil {
		return statuses, allowStaleRunning, lifecycleState, nil
	}
	if state := filters["state"]; state != "" {
		switch state {
		case "raw":
			lifecycleState = "input"
			statuses = []string{"notdone"}
			allowStaleRunning = false
		case "translation_failed":
			lifecycleState = "input"
			statuses = []string{"failed"}
			allowStaleRunning = false
		default:
			if !brregTranslationLifecycleStates[state] {
				return nil, false, "", fmt.Errorf("unsupported brreg lifecycle state filter %q", state)
			}
			lifecycleState = state
		}
	}
	if status := filters["translation_action_status"]; status != "" {
		if !brregTranslationActionStatuses[status] {
			return nil, false, "", fmt.Errorf("unsupported brreg translation action status filter %q", status)
		}
		if status == "running" {
			statuses = []string{}
			allowStaleRunning = true
		} else {
			statuses = []string{status}
			allowStaleRunning = false
		}
	}
	return statuses, allowStaleRunning, lifecycleState, nil
}

func (a *GoActivities) claimSourceTranslationRows(ctx context.Context, cfg SourceTranslationConfig, params contracts.PrepareSourceTranslationBatchParams) ([]sourceTranslationRow, error) {
	var ids any
	if len(params.IDs) > 0 {
		ids = params.IDs
	}
	claimSQL := cfg.ClaimSQL
	args := []any{params.WorkflowRunID, sourceTranslationLeaseMinutes, params.BatchSize, ids}
	if claimSQL == "" {
		includeFailed := len(params.IDs) > 0
		if cfg.SourceName == "brreg" {
			statuses, allowStaleRunning, lifecycleState, err := brregTranslationClaimFilters(params.Filters, includeFailed)
			if err != nil {
				return nil, err
			}
			claimSQL = brregSourceClaimSQL()
			args = append(args, statuses, allowStaleRunning, lifecycleState)
		} else {
			claimSQL = sourceClaimSQL(cfg.TableName, includeFailed)
		}
	}
	rows, err := a.pool.Query(ctx, claimSQL, args...)
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
		if cfg.SourceName == "brreg" {
			if err := appendBrregTranslationActionEvent(ctx, tx, id, "succeeded", "translation completed", ""); err != nil {
				return err
			}
		}
	}
	for id, reason := range failures {
		if _, err := tx.Exec(ctx, cfg.WriteFailureSQL, id, reason); err != nil {
			return fmt.Errorf("mark %s row failed: %w", cfg.SourceName, err)
		}
		if cfg.SourceName == "brreg" {
			if err := appendBrregTranslationActionEvent(ctx, tx, id, "failed", "translation failed", reason); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s translation write: %w", cfg.SourceName, err)
	}
	return nil
}

func appendBrregTranslationActionEvent(ctx context.Context, tx pgx.Tx, rawInputID, status, message, errorText string) error {
	var actionError any
	if errorText != "" {
		actionError = errorText
	}
	tag, err := tx.Exec(ctx, `
		WITH latest AS (
		    SELECT a.id AS action_id
		    FROM brreg_raw_input_actions a
		    JOIN brreg_company_raw_inputs bri
		      ON bri.id = a.raw_input_id
		     AND bri.payload_hash = a.payload_hash
		    WHERE a.raw_input_id = $1::uuid
		      AND a.action_type = 'translate'
		    ORDER BY a.attempt DESC, a.created_at DESC
		    LIMIT 1
		)
		INSERT INTO brreg_raw_input_action_events (action_id, status, message, error, metadata)
		SELECT action_id, $2, $3, $4, '{}'::jsonb
		FROM latest
	`, rawInputID, status, message, actionError)
	if err != nil {
		return fmt.Errorf("append brreg translation action event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("append brreg translation action event: no latest translate action for raw input %s", rawInputID)
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
		translated := translations[termKey(term.Category, term.Text)]
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
