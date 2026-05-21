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

// PullBrreg downloads the full Brønnøysund Register Centre (Brreg) bulk export,
// bulk-upserts all records into the DB, then marks execution complete.
// Unlike the Companies House workflow there is no pagination loop — the entire
// dataset is fetched in a single zip file (~800k records).
//
// Python activity: download_brreg_bulk  (task queue: corpscout-pipelines-python)
// Go activities:   ImportBrregBulk, MarkExecutionComplete
func PullBrreg(ctx workflow.Context, input contracts.PullBrregInput) (contracts.PullCompaniesResult, error) {
	runIDStr := input.RunID
	if runIDStr == "" {
		if err := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
			return uuid.New().String()
		}).Get(&runIDStr); err != nil {
			return contracts.PullCompaniesResult{}, err
		}
	}

	outputDir := input.OutputDir
	if outputDir == "" {
		outputDir = defaultBrregOutputDir
	}

	// Python download activity: just fetches the zip and saves it to the shared FS.
	// Allow up to 20 minutes for the download (~150 MB over the network).
	pythonCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:            "corpscout-pipelines-python",
		StartToCloseTimeout:  20 * time.Minute,
		HeartbeatTimeout:     2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    3,
			InitialInterval:    15 * time.Second,
			MaximumInterval:    2 * time.Minute,
			BackoffCoefficient: 2.0,
		},
	})

	// Go import activity: parse zip + bulk upsert ~800k records.
	// Allow up to 30 minutes; the activity heartbeats every batch so
	// Temporal can detect if the worker crashes.
	goCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:            "corpscout-pipelines",
		StartToCloseTimeout:  30 * time.Minute,
		HeartbeatTimeout:     2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
			InitialInterval: 10 * time.Second,
		},
	})

	logger := workflow.GetLogger(ctx)
	var goAct *activities.GoActivities

	// Step 1: download bulk zip to shared filesystem.
	var dlResult contracts.DownloadBrregBulkResult
	if err := workflow.ExecuteActivity(pythonCtx, "download_brreg_bulk", outputDir).Get(ctx, &dlResult); err != nil {
		return contracts.PullCompaniesResult{}, err
	}
	logger.Info("bulk zip downloaded", "path", dlResult.FilePath, "date", dlResult.Date)

	// Step 2: parse zip and bulk-upsert into brreg_company_raw_inputs.
	var written int
	if err := workflow.ExecuteActivity(goCtx, goAct.ImportBrregBulk, contracts.ImportBrregBulkParams{
		FilePath:       dlResult.FilePath,
		RunID:          runIDStr,
		CorpscoutRunID: input.CorpscoutRunID,
		Force:          input.Force,
	}).Get(ctx, &written); err != nil {
		return contracts.PullCompaniesResult{}, err
	}
	logger.Info("bulk import done", "records_written", written)

	result := contracts.PullCompaniesResult{RecordsWritten: written, PagesFetched: 1}

	// Step 3: mark complete, save bulk checkpoint, enqueue processing.
	shortGoCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines",
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	if err := workflow.ExecuteActivity(shortGoCtx, goAct.MarkExecutionComplete, contracts.MarkCompleteParams{
		RunID:          runIDStr,
		CorpscoutRunID: input.CorpscoutRunID,
		Source:         "brreg",
		Country:        "NO",
		Result:         result,
		FinalCursor:    "bulk:" + dlResult.Date,
	}).Get(ctx, nil); err != nil {
		return result, err
	}

	return result, nil
}
