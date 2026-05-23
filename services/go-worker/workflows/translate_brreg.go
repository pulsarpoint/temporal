package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

const translateBrregBatchSize = 50
const translateBrregContinueAfterBatches = 50
const defaultBrregTranslationModel = "qwen3:6b"

func TranslateBrregRawInputs(ctx workflow.Context, input contracts.TranslateBrregInput) (contracts.TranslateBrregBatchResult, error) {
	if input.PromptVersion == "" {
		input.PromptVersion = "v1"
	}
	if input.Model == "" {
		input.Model = defaultBrregTranslationModel
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
	total := contracts.TranslateBrregBatchResult{Translated: input.Accumulated}
	var goAct *activities.GoActivities

	for {
		var prepared contracts.PrepareBrregTranslationBatchResult
		err := workflow.ExecuteActivity(goCtx, goAct.PrepareBrregTranslationBatch, contracts.PrepareBrregTranslationBatchParams{
			IDs:           input.IDs,
			PromptVersion: input.PromptVersion,
			Model:         input.Model,
			FXRateDate:    input.FXRateDate,
			WorkflowRunID: workflowRunID,
			BatchSize:     translateBrregBatchSize,
		}).Get(ctx, &prepared)
		if err != nil {
			return total, err
		}

		result := contracts.TranslateBrregBatchResult{Claimed: prepared.Claimed}
		if prepared.Claimed > 0 {
			newTranslations, err := translateBrregCacheMisses(ctx, pythonCtx, input, prepared.MissesByCategory)
			if err != nil {
				return total, err
			}

			err = workflow.ExecuteActivity(goCtx, goAct.WriteBrregTranslationBatch, contracts.WriteBrregTranslationBatchParams{
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
		if pagesThisRun >= translateBrregContinueAfterBatches {
			input.Accumulated = total.Translated
			return total, workflow.NewContinueAsNewError(ctx, TranslateBrregRawInputs, input)
		}
	}
	return total, nil
}

func translateBrregCacheMisses(
	ctx workflow.Context,
	pythonCtx workflow.Context,
	input contracts.TranslateBrregInput,
	missesByCategory map[string][]contracts.TranslationItem,
) ([]contracts.BrregTranslatedTerm, error) {
	items, itemByID := flattenTranslationMisses(missesByCategory)
	newTranslations := []contracts.BrregTranslatedTerm{}
	if len(items) == 0 {
		return newTranslations, nil
	}

	var translated contracts.TranslateTermsResult
	err := workflow.ExecuteActivity(pythonCtx, "TranslateTermsWithDSPy", contracts.TranslateTermsInput{
		Category:      "mixed",
		SourceLang:    "no",
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
		newTranslations = append(newTranslations, contracts.BrregTranslatedTerm{
			ID:          item.Item.ID,
			Category:    item.Category,
			Text:        item.Item.Text,
			Translation: term.Translation,
		})
	}
	return newTranslations, nil
}
