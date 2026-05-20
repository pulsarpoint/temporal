package workflows

import (
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

// PullCompanies orchestrates fetching all company records from a source and
// writing them directly to corpscout's raw_inputs tables.
// Python activities run on the "corpscout-pipelines-python" task queue.
// Go activities run on the "corpscout-pipelines" task queue.
func PullCompanies(ctx workflow.Context, input contracts.PullCompaniesInput) (contracts.PullCompaniesResult, error) {
	// Generate a stable run ID. SideEffect records it in workflow history so
	// retries use the same UUID, enabling idempotent DB upserts.
	var runIDStr string
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return uuid.New().String()
	}).Get(&runIDStr); err != nil {
		return contracts.PullCompaniesResult{}, err
	}

	// Options for the Python FetchPage activity (separate task queue).
	fetchCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines-python",
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    10,
			InitialInterval:    5 * time.Second,
			MaximumInterval:    2 * time.Minute,
			BackoffCoefficient: 2.0,
		},
	})

	// Options for Go activities (main task queue).
	writeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines",
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
			InitialInterval: 2 * time.Second,
		},
	})

	logger := workflow.GetLogger(ctx)
	var goAct *activities.GoActivities
	total := contracts.PullCompaniesResult{}
	cursor := ""
	page := 1

	for {
		// Step 1: fetch one page (Python activity, referenced by name string)
		fetchInput := contracts.FetchPageInput{
			Source:  input.Source,
			Country: input.Country,
			IDs:     input.IDs,
			Page:    page,
			Cursor:  cursor,
		}
		var fetchResult contracts.FetchResult
		if err := workflow.ExecuteActivity(fetchCtx, "fetch_page", fetchInput).Get(ctx, &fetchResult); err != nil {
			return total, err
		}

		if len(fetchResult.Records) == 0 {
			logger.Info("fetch_page returned 0 records, stopping",
				"source", input.Source,
				"country", input.Country,
				"page", page,
				"cursor", cursor,
			)
			break
		}

		logger.Info("page fetched", "page", page, "records", len(fetchResult.Records), "has_more", fetchResult.HasMore)

		// Step 2: write records to corpscout DB (Go activity)
		writeParams := contracts.WriteRawInputsParams{
			Source:  input.Source,
			RunID:   runIDStr,
			Records: fetchResult.Records,
		}
		var written int
		if err := workflow.ExecuteActivity(writeCtx, goAct.WriteRawInputs, writeParams).Get(ctx, &written); err != nil {
			return total, err
		}

		total.RecordsWritten += written
		total.PagesFetched++

		if !fetchResult.HasMore {
			logger.Info("no more pages, stopping", "source", input.Source, "country", input.Country, "total_pages", total.PagesFetched)
			break
		}
		cursor = fetchResult.NextCursor
		page++
	}

	// Step 3: write result JSON to output directory
	markParams := contracts.MarkCompleteParams{
		RunID:          runIDStr,
		CorpscoutRunID: input.CorpscoutRunID,
		Source:         input.Source,
		Country:        input.Country,
		Result:         total,
	}
	if err := workflow.ExecuteActivity(writeCtx, goAct.MarkExecutionComplete, markParams).Get(ctx, nil); err != nil {
		return total, err
	}

	return total, nil
}
