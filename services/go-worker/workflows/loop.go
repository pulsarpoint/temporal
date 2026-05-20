package workflows

import (
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

// pullParams is the resolved configuration passed to runPullLoop by every
// source-specific workflow.
type pullParams struct {
	Source         string
	Country        string
	IDs            []string
	CorpscoutRunID string
}

// runPullLoop is the shared pagination loop used by all source workflows.
// It pages through FetchPage results, writes each batch, and checks which
// companies still need detail enrichment.
func runPullLoop(ctx workflow.Context, p pullParams) (contracts.PullCompaniesResult, error) {
	// Stable run ID written into workflow history so retries are idempotent.
	var runIDStr string
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return uuid.New().String()
	}).Get(&runIDStr); err != nil {
		return contracts.PullCompaniesResult{}, err
	}

	// Python FetchPage activity runs on the dedicated Python task queue.
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
		fetchInput := contracts.FetchPageInput{
			Source:  p.Source,
			Country: p.Country,
			IDs:     p.IDs,
			Page:    page,
			Cursor:  cursor,
		}
		var fetchResult contracts.FetchResult
		if err := workflow.ExecuteActivity(fetchCtx, "fetch_page", fetchInput).Get(ctx, &fetchResult); err != nil {
			return total, err
		}

		if len(fetchResult.Records) == 0 {
			logger.Info("fetch_page returned 0 records, stopping",
				"source", p.Source, "country", p.Country, "page", page, "cursor", cursor)
			break
		}

		logger.Info("page fetched", "page", page, "records", len(fetchResult.Records), "has_more", fetchResult.HasMore)

		writeParams := contracts.WriteRawInputsParams{
			Source:  p.Source,
			RunID:   runIDStr,
			Records: fetchResult.Records,
		}
		var written int
		if err := workflow.ExecuteActivity(goCtx, goAct.WriteRawInputs, writeParams).Get(ctx, &written); err != nil {
			return total, err
		}
		total.RecordsWritten += written
		total.PagesFetched++

		// Check which companies still need detail enrichment.
		nativeIDs := make([]string, 0, len(fetchResult.Records))
		for _, r := range fetchResult.Records {
			if r.NativeID != "" {
				nativeIDs = append(nativeIDs, r.NativeID)
			}
		}
		var filterResult contracts.FilterForEnrichmentResult
		if err := workflow.ExecuteActivity(goCtx, goAct.FilterForEnrichment, contracts.FilterForEnrichmentParams{
			Source:    p.Source,
			NativeIDs: nativeIDs,
		}).Get(ctx, &filterResult); err != nil {
			return total, err
		}
		logger.Info("enrichment filter", "page", page, "total", len(nativeIDs), "need_enrichment", len(filterResult.NeedEnrichment))
		// TODO: dispatch FetchCompanyDetails for filterResult.NeedEnrichment (Phase 2)

		if !fetchResult.HasMore {
			logger.Info("no more pages", "source", p.Source, "country", p.Country, "total_pages", total.PagesFetched)
			break
		}
		cursor = fetchResult.NextCursor
		page++
	}

	markParams := contracts.MarkCompleteParams{
		RunID:          runIDStr,
		CorpscoutRunID: p.CorpscoutRunID,
		Source:         p.Source,
		Country:        p.Country,
		Result:         total,
	}
	if err := workflow.ExecuteActivity(goCtx, goAct.MarkExecutionComplete, markParams).Get(ctx, nil); err != nil {
		return total, err
	}

	return total, nil
}
