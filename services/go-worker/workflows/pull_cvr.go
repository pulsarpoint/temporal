package workflows

import (
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

const defaultCVROutputDir = "/var/lib/data-pipelines/results/cvr"

func PullCVR(ctx workflow.Context, input contracts.PullCVRInput) (contracts.PullCompaniesResult, error) {
	runIDStr := genRunID(ctx, input.RunID)
	outputDir := input.OutputDir
	if outputDir == "" {
		outputDir = defaultCVROutputDir
	}
	mode := input.Mode
	if mode == "" {
		mode = "bulk"
	} else if mode != "bulk" && mode != "incremental" {
		return contracts.PullCompaniesResult{}, unsupportedSourceModeError("cvr", mode)
	}

	pythonCtx := workflow.WithActivityOptions(ctx, sourceDownloadActivityOptions())
	importCtx := workflow.WithActivityOptions(ctx, sourceImportActivityOptions())
	markCtx := workflow.WithActivityOptions(ctx, markCompleteActivityOptions())

	var download contracts.DownloadSourceFilesResult
	if err := workflow.ExecuteActivity(pythonCtx, "download_cvr_file_set", contracts.DownloadSourceFilesInput{
		Source:    "cvr",
		Mode:      mode,
		OutputDir: outputDir,
	}).Get(ctx, &download); err != nil {
		return contracts.PullCompaniesResult{}, err
	}

	var goAct *activities.GoActivities
	var written int
	if err := workflow.ExecuteActivity(importCtx, goAct.ImportCVRBulk, contracts.ImportCVRBulkParams{
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
		Source:         "cvr",
		Country:        "DK",
		Result:         result,
		FinalCursor:    mode + ":" + download.SnapshotID,
	}).Get(ctx, nil); err != nil {
		return result, err
	}

	return result, nil
}
