package workflows

import (
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

// PullCompaniesHouse paginates the Companies House advanced-search list endpoint,
// writes raw records, and tracks which companies still need detail enrichment.
//
// Python activity: fetch_companies_house_list  (task queue: corpscout-pipelines-python)
// Go activities:   WriteRawInputs, FilterForEnrichment, MarkExecutionComplete
//
// TODO Phase 2: call fetch_companies_house_detail for filterResult.NeedEnrichment
func PullCompaniesHouse(ctx workflow.Context, input contracts.PullCompaniesHouseInput) (contracts.PullCompaniesResult, error) {
	var runIDStr string
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return uuid.New().String()
	}).Get(&runIDStr); err != nil {
		return contracts.PullCompaniesResult{}, err
	}

	// Python list activity runs on the dedicated Python task queue.
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

	// Go activities run on the main Go task queue.
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
	total := contracts.PullCompaniesResult{}
	cursor := ""
	page := 1

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
			Records: fetchResult.Records,
		}).Get(ctx, &written); err != nil {
			return total, err
		}
		total.RecordsWritten += written
		total.PagesFetched++

		nativeIDs := make([]string, 0, len(fetchResult.Records))
		for _, r := range fetchResult.Records {
			if r.NativeID != "" {
				nativeIDs = append(nativeIDs, r.NativeID)
			}
		}
		var filterResult contracts.FilterForEnrichmentResult
		if err := workflow.ExecuteActivity(goCtx, goAct.FilterForEnrichment, contracts.FilterForEnrichmentParams{
			Source:    "companies_house",
			NativeIDs: nativeIDs,
		}).Get(ctx, &filterResult); err != nil {
			return total, err
		}
		logger.Info("enrichment filter", "page", page, "total", len(nativeIDs), "need_enrichment", len(filterResult.NeedEnrichment))
		// TODO Phase 2: dispatch fetch_companies_house_detail for filterResult.NeedEnrichment

		if !fetchResult.HasMore {
			logger.Info("no more pages", "country", input.Country, "total_pages", total.PagesFetched)
			break
		}
		cursor = fetchResult.NextCursor
		page++
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
