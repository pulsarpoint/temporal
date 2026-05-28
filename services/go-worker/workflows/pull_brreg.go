package workflows

import (
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

const defaultBrregOutputDir = "/var/lib/data-pipelines/results/brreg"

// PullBrreg selects the pipeline mode based on input.Mode:
//
//   - "bulk" (or empty): download the full Brreg export zip, bulk-upsert all
//     records, then mark complete. 3 activity calls, no ContinueAsNew needed.
//
//   - "incremental": paginate the Brreg list API from input.IncrementalFrom
//     (the date stored in the bulk checkpoint). Uses ContinueAsNew every 50
//     pages to keep Temporal history bounded.
func PullBrreg(ctx workflow.Context, input contracts.PullBrregInput) (contracts.PullCompaniesResult, error) {
	if input.Mode == "incremental" {
		return pullBrregIncremental(ctx, input)
	}
	return pullBrregBulk(ctx, input)
}

// ── Bulk path ──────────────────────────────────────────────────────────────────

func pullBrregBulk(ctx workflow.Context, input contracts.PullBrregInput) (contracts.PullCompaniesResult, error) {
	runIDStr := genRunID(ctx, input.RunID)

	outputDir := input.OutputDir
	if outputDir == "" {
		outputDir = defaultBrregOutputDir
	}

	pythonCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines-python",
		StartToCloseTimeout: 20 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    3,
			InitialInterval:    15 * time.Second,
			MaximumInterval:    2 * time.Minute,
			BackoffCoefficient: 2.0,
		},
	})

	goCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines",
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
			InitialInterval: 10 * time.Second,
		},
	})

	shortGoCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines",
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})

	logger := workflow.GetLogger(ctx)
	var goAct *activities.GoActivities

	var dlResult contracts.DownloadBrregBulkResult
	if err := workflow.ExecuteActivity(pythonCtx, "download_brreg_bulk", outputDir).Get(ctx, &dlResult); err != nil {
		return contracts.PullCompaniesResult{}, err
	}
	logger.Info("bulk zip downloaded", "path", dlResult.FilePath, "date", dlResult.Date)

	var written int
	if err := workflow.ExecuteActivity(goCtx, goAct.ImportBrregBulk, contracts.ImportBrregBulkParams{
		FilePath:       dlResult.FilePath,
		RunID:          runIDStr,
		CorpscoutRunID: input.CorpscoutRunID,
		Force:          input.Force,
		Limit:          input.Limit,
	}).Get(ctx, &written); err != nil {
		return contracts.PullCompaniesResult{}, err
	}
	logger.Info("bulk import done", "records_written", written)

	result := contracts.PullCompaniesResult{RecordsWritten: written, PagesFetched: 1}
	finalCursor := "bulk:" + dlResult.Date
	if input.Limit > 0 {
		finalCursor = ""
	}

	if err := workflow.ExecuteActivity(shortGoCtx, goAct.MarkExecutionComplete, contracts.MarkCompleteParams{
		RunID:          runIDStr,
		CorpscoutRunID: input.CorpscoutRunID,
		Source:         "brreg",
		Country:        "NO",
		Result:         result,
		FinalCursor:    finalCursor,
	}).Get(ctx, nil); err != nil {
		return result, err
	}

	return result, nil
}

// ── Incremental path ───────────────────────────────────────────────────────────

func pullBrregIncremental(ctx workflow.Context, input contracts.PullBrregInput) (contracts.PullCompaniesResult, error) {
	runIDStr := genRunID(ctx, input.RunID)

	listCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines-python",
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    10,
			InitialInterval:    5 * time.Second,
			MaximumInterval:    2 * time.Minute,
			BackoffCoefficient: 2.0,
		},
	})

	goCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines",
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5, InitialInterval: 2 * time.Second},
	})

	logger := workflow.GetLogger(ctx)
	var goAct *activities.GoActivities

	total := input.Accumulated
	// First run: start from the date the bulk was completed.
	// ContinueAsNew runs: resume from the last saved cursor.
	cursor := input.Cursor
	if cursor == "" {
		cursor = input.IncrementalFrom
	}
	page := 1
	pagesThisRun := 0

	for {
		var fetchResult contracts.FetchResult
		if err := workflow.ExecuteActivity(listCtx, "fetch_brreg_list", contracts.FetchPageInput{
			Source:  "brreg",
			Country: "NO",
			Page:    page,
			Cursor:  cursor,
		}).Get(ctx, &fetchResult); err != nil {
			return total, err
		}

		if len(fetchResult.Records) == 0 {
			logger.Info("incremental: no records returned, stopping", "cursor", cursor)
			break
		}

		var written int
		if err := workflow.ExecuteActivity(goCtx, goAct.WriteRawInputs, contracts.WriteRawInputsParams{
			Source:  "brreg",
			RunID:   runIDStr,
			Force:   input.Force,
			Records: fetchResult.Records,
		}).Get(ctx, &written); err != nil {
			return total, err
		}
		total.RecordsWritten += written
		total.PagesFetched++
		pagesThisRun++

		if !fetchResult.HasMore {
			break
		}

		cursor = fetchResult.NextCursor
		page++

		if pagesThisRun >= continueAfterPages {
			logger.Info("incremental: ContinueAsNew", "pages_this_run", pagesThisRun)
			if err := workflow.ExecuteActivity(goCtx, goAct.SaveSyncCheckpoint, contracts.SaveSyncCheckpointParams{
				Source: "brreg",
				Cursor: cursor,
			}).Get(ctx, nil); err != nil {
				logger.Warn("incremental: save checkpoint failed", "error", err)
			}
			return total, workflow.NewContinueAsNewError(ctx, PullBrreg, contracts.PullBrregInput{
				CorpscoutRunID:  input.CorpscoutRunID,
				Force:           input.Force,
				Mode:            "incremental",
				IncrementalFrom: input.IncrementalFrom,
				Cursor:          cursor,
				RunID:           runIDStr,
				Accumulated:     total,
			})
		}
	}

	if err := workflow.ExecuteActivity(goCtx, goAct.MarkExecutionComplete, contracts.MarkCompleteParams{
		RunID:          runIDStr,
		CorpscoutRunID: input.CorpscoutRunID,
		Source:         "brreg",
		Country:        "NO",
		Result:         total,
		FinalCursor:    "bulk:" + input.IncrementalFrom[:10], // preserve the original bulk date
	}).Get(ctx, nil); err != nil {
		return total, err
	}

	return total, nil
}

func genRunID(ctx workflow.Context, existing string) string {
	if existing != "" {
		return existing
	}
	var id string
	_ = workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return uuid.New().String()
	}).Get(&id)
	return id
}
