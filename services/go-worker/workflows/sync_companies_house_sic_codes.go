package workflows

import (
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

const defaultCompaniesHouseSICOutputDir = "/var/lib/data-pipelines/results/companies_house_sic"

func SyncCompaniesHouseSICCodes(ctx workflow.Context, input contracts.DownloadSourceFilesInput) (int, error) {
	outputDir := input.OutputDir
	if outputDir == "" {
		outputDir = defaultCompaniesHouseSICOutputDir
	}
	snapshotID := input.SnapshotID
	source := input.Source
	if source == "" {
		source = "companies_house_sic"
	}

	pythonCtx := workflow.WithActivityOptions(ctx, sourceDownloadActivityOptions())
	importCtx := workflow.WithActivityOptions(ctx, sourceImportActivityOptions())

	var download contracts.DownloadSourceFilesResult
	if err := workflow.ExecuteActivity(pythonCtx, "download_companies_house_sic_codes", contracts.DownloadSourceFilesInput{
		Source:     source,
		Mode:       "full",
		OutputDir:  outputDir,
		SnapshotID: snapshotID,
	}).Get(ctx, &download); err != nil {
		return 0, err
	}

	var goAct *activities.GoActivities
	imported := 0
	for _, file := range download.Files {
		var fileCount int
		if err := workflow.ExecuteActivity(importCtx, goAct.ImportCompaniesHouseSICCodes, file).Get(ctx, &fileCount); err != nil {
			return imported, err
		}
		imported += fileCount
	}

	return imported, nil
}
