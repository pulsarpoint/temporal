package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

const translateSourceBatchSize = 50
const translateSourceContinueAfterBatches = 50
const defaultSourceTranslationModel = "qwen3:6b"

func TranslateSourceRawInputs(ctx workflow.Context, input contracts.TranslateSourceInput) (contracts.TranslateSourceBatchResult, error) {
	if input.PromptVersion == "" {
		input.PromptVersion = "v1"
	}
	if input.Model == "" {
		input.Model = defaultSourceTranslationModel
	}
	sourceLang, err := sourceTranslationLanguage(input.Source)
	if err != nil {
		return contracts.TranslateSourceBatchResult{}, err
	}

	goCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines",
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    3,
			InitialInterval:    5 * time.Second,
			MaximumInterval:    time.Minute,
			BackoffCoefficient: 2,
		},
	})
	pythonCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines-python",
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    3,
			InitialInterval:    5 * time.Second,
			MaximumInterval:    time.Minute,
			BackoffCoefficient: 2,
		},
	})

	workflowRunID := workflow.GetInfo(ctx).WorkflowExecution.RunID
	pagesThisRun := 0
	total := contracts.TranslateSourceBatchResult{Translated: input.Accumulated}
	var goAct *activities.GoActivities

	for {
		var prepared contracts.PrepareSourceTranslationBatchResult
		err := workflow.ExecuteActivity(goCtx, goAct.PrepareSourceTranslationBatch, contracts.PrepareSourceTranslationBatchParams{
			Source:        input.Source,
			IDs:           input.IDs,
			Filters:       input.Filters,
			PromptVersion: input.PromptVersion,
			Model:         input.Model,
			FXRateDate:    input.FXRateDate,
			WorkflowRunID: workflowRunID,
			BatchSize:     translateSourceBatchSize,
		}).Get(ctx, &prepared)
		if err != nil {
			return total, err
		}

		result := contracts.TranslateSourceBatchResult{Claimed: prepared.Claimed}
		if prepared.Claimed > 0 {
			newTranslations, err := translateSourceCacheMisses(ctx, pythonCtx, input, sourceLang, prepared.MissesByCategory)
			if err != nil {
				return total, err
			}

			err = workflow.ExecuteActivity(goCtx, goAct.WriteSourceTranslationBatch, contracts.WriteSourceTranslationBatchParams{
				Source:             input.Source,
				PromptVersion:      input.PromptVersion,
				Model:              input.Model,
				Rows:               prepared.Rows,
				FX:                 prepared.FX,
				CachedTranslations: prepared.CachedTranslations,
				NewTranslations:    newTranslations,
			}).Get(ctx, &result)
			if err != nil {
				return total, err
			}
		}

		total.Claimed += result.Claimed
		total.Translated += result.Translated
		total.Failed += result.Failed

		if result.Claimed == 0 {
			break
		}
		if len(input.IDs) > 0 {
			break
		}
		pagesThisRun++
		if pagesThisRun >= translateSourceContinueAfterBatches {
			input.Accumulated = total.Translated
			return total, workflow.NewContinueAsNewError(ctx, TranslateSourceRawInputs, input)
		}
	}
	return total, nil
}

func translateSourceCacheMisses(
	ctx workflow.Context,
	pythonCtx workflow.Context,
	input contracts.TranslateSourceInput,
	sourceLang string,
	missesByCategory map[string][]contracts.TranslationItem,
) ([]contracts.SourceTranslatedTerm, error) {
	items, itemByID := flattenTranslationMisses(missesByCategory)
	newTranslations := []contracts.SourceTranslatedTerm{}
	if len(items) == 0 {
		return newTranslations, nil
	}

	var translated contracts.TranslateTermsResult
	err := workflow.ExecuteActivity(pythonCtx, "TranslateTermsWithDSPy", contracts.TranslateTermsInput{
		Category:      "mixed",
		SourceLang:    sourceLang,
		TargetLang:    "en",
		Items:         items,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
	}).Get(ctx, &translated)
	if err != nil {
		return nil, err
	}
	for _, term := range translated.Translations {
		item, ok := itemByID[term.ID]
		if !ok || term.Translation == "" {
			continue
		}
		newTranslations = append(newTranslations, contracts.SourceTranslatedTerm{
			ID:          item.Item.ID,
			Category:    item.Category,
			Text:        item.Item.Text,
			Translation: term.Translation,
		})
	}
	return newTranslations, nil
}

func sourceTranslationLanguage(source string) (string, error) {
	switch source {
	case "brreg":
		return "no", nil
	case "cvr":
		return "da", nil
	case "ariregister":
		return "et", nil
	default:
		return "", fmt.Errorf("unsupported translation source %q", source)
	}
}
