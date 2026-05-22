package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

const defaultGLEIFOutputDir = "/var/lib/data-pipelines/results/gleif"

func PullGLEIF(ctx workflow.Context, input contracts.PullGLEIFInput) (contracts.PullCompaniesResult, error) {
	runIDStr := genRunID(ctx, input.RunID)
	outputDir := input.OutputDir
	if outputDir == "" {
		outputDir = defaultGLEIFOutputDir
	}

	cursorMode, downloadMode, err := gleifModes(input.Mode)
	if err != nil {
		return contracts.PullCompaniesResult{}, err
	}

	pythonCtx := workflow.WithActivityOptions(ctx, sourceDownloadActivityOptions())
	importCtx := workflow.WithActivityOptions(ctx, sourceImportActivityOptions())
	markCtx := workflow.WithActivityOptions(ctx, markCompleteActivityOptions())

	var download contracts.DownloadSourceFilesResult
	if err := workflow.ExecuteActivity(pythonCtx, "download_gleif_golden_copy", contracts.DownloadSourceFilesInput{
		Source:      "gleif",
		Mode:        downloadMode,
		OutputDir:   outputDir,
		DeltaWindow: input.DeltaWindow,
	}).Get(ctx, &download); err != nil {
		return contracts.PullCompaniesResult{}, err
	}

	var goAct *activities.GoActivities
	var written int
	if err := workflow.ExecuteActivity(importCtx, goAct.ImportGLEIFGoldenCopy, contracts.ImportGLEIFGoldenCopyParams{
		Files:          download.Files,
		RunID:          runIDStr,
		CorpscoutRunID: input.CorpscoutRunID,
	}).Get(ctx, &written); err != nil {
		return contracts.PullCompaniesResult{}, err
	}

	result := contracts.PullCompaniesResult{
		RecordsWritten: written,
		PagesFetched:   sourceFilePageCount(download.Files),
	}

	if err := workflow.ExecuteActivity(markCtx, goAct.MarkExecutionComplete, contracts.MarkCompleteParams{
		RunID:          runIDStr,
		CorpscoutRunID: input.CorpscoutRunID,
		Source:         "gleif",
		Country:        "",
		Result:         result,
		FinalCursor:    cursorMode + ":" + download.SnapshotID,
	}).Get(ctx, nil); err != nil {
		return result, err
	}

	return result, nil
}

func gleifModes(inputMode string) (cursorMode string, downloadMode string, err error) {
	switch inputMode {
	case "", "bulk", "full":
		return "bulk", "full", nil
	case "delta":
		return "delta", "delta", nil
	default:
		return "", "", unsupportedSourceModeError("gleif", inputMode)
	}
}

func unsupportedSourceModeError(source, mode string) error {
	return temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("unsupported %s mode %q", source, mode),
		"InvalidSourceMode",
		nil,
	)
}

func sourceDownloadActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines-python",
		StartToCloseTimeout: 20 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    3,
			InitialInterval:    15 * time.Second,
			MaximumInterval:    2 * time.Minute,
			BackoffCoefficient: 2.0,
		},
	}
}

func sourceImportActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines",
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
			InitialInterval: 10 * time.Second,
		},
	}
}

func markCompleteActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines",
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	}
}

func sourceFilePageCount(files []contracts.DownloadedSourceFile) int {
	return len(files)
}
