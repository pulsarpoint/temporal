package workflows

import (
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

// continueAfterPages is the number of pages fetched before triggering ContinueAsNew
// to prevent workflow history from exceeding Temporal's size limit.
const continueAfterPages = 50

// PullCompaniesHouse paginates the Companies House advanced-search list endpoint
// and writes raw records. Uses ContinueAsNew every continueAfterPages pages to
// keep history size bounded across the full UK company corpus.
//
// Python activity: fetch_companies_house_list  (task queue: corpscout-pipelines-python)
// Go activities:   WriteRawInputs, MarkExecutionComplete
func PullCompaniesHouse(ctx workflow.Context, input contracts.PullCompaniesHouseInput) (contracts.PullCompaniesResult, error) {
	// Reuse RunID across ContinueAsNew cycles so all batches belong to the same pull.
	runIDStr := input.RunID
	if runIDStr == "" {
		if err := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
			return uuid.New().String()
		}).Get(&runIDStr); err != nil {
			return contracts.PullCompaniesResult{}, err
		}
	}

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
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
			InitialInterval: 2 * time.Second,
		},
	})

	logger := workflow.GetLogger(ctx)
	var goAct *activities.GoActivities

	total := input.Accumulated
	cursor := input.Cursor
	page := 1
	pagesThisRun := 0

	for {
		var fetchResult contracts.FetchResult
		if err := workflow.ExecuteActivity(listCtx, "fetch_companies_house_list", contracts.FetchPageInput{
			Source:  "companies_house",
			Country: input.Country,
			IDs:     input.IDs,
			Page:    page,
			Cursor:  cursor,
		}).Get(ctx, &fetchResult); err != nil {
			return total, err
		}

		if len(fetchResult.Records) == 0 {
			logger.Info("no records returned, stopping",
				"country", input.Country, "page", page, "cursor", cursor)
			break
		}

		logger.Info("page fetched", "page", page, "records", len(fetchResult.Records), "has_more", fetchResult.HasMore)

		var written int
		if err := workflow.ExecuteActivity(goCtx, goAct.WriteRawInputs, contracts.WriteRawInputsParams{
			Source:  "companies_house",
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
			logger.Info("no more pages", "country", input.Country, "total_pages", total.PagesFetched)
			break
		}

		cursor = fetchResult.NextCursor
		page++

		if pagesThisRun >= continueAfterPages {
			logger.Info("continuing as new to bound history",
				"pages_this_run", pagesThisRun, "total_pages", total.PagesFetched)
			return total, workflow.NewContinueAsNewError(ctx, PullCompaniesHouse, contracts.PullCompaniesHouseInput{
				Country:        input.Country,
				IDs:            input.IDs,
				CorpscoutRunID: input.CorpscoutRunID,
				Force:          input.Force,
				Cursor:         cursor,
				RunID:          runIDStr,
				Accumulated:    total,
			})
		}
	}

	if err := workflow.ExecuteActivity(goCtx, goAct.MarkExecutionComplete, contracts.MarkCompleteParams{
		RunID:          runIDStr,
		CorpscoutRunID: input.CorpscoutRunID,
		Source:         "companies_house",
		Country:        input.Country,
		Result:         total,
	}).Get(ctx, nil); err != nil {
		return total, err
	}

	return total, nil
}
