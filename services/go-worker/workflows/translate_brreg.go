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

func TranslateBrregRawInputs(ctx workflow.Context, input contracts.TranslateBrregInput) (contracts.TranslateBrregBatchResult, error) {
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

	workflowRunID := workflow.GetInfo(ctx).WorkflowExecution.RunID
	pagesThisRun := 0
	total := contracts.TranslateBrregBatchResult{Translated: input.Accumulated}
	var goAct *activities.GoActivities

	for {
		var result contracts.TranslateBrregBatchResult
		err := workflow.ExecuteActivity(goCtx, goAct.TranslateBrregBatch, contracts.TranslateBrregBatchParams{
			IDs:           input.IDs,
			PromptVersion: input.PromptVersion,
			Model:         input.Model,
			FXRateDate:    input.FXRateDate,
			WorkflowRunID: workflowRunID,
			BatchSize:     translateBrregBatchSize,
		}).Get(ctx, &result)
		if err != nil {
			return total, err
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
