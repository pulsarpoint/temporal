package workflows

import (
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

// PullCompaniesHouse paginates the Companies House advanced-search list endpoint
// and writes raw records. After all pages are fetched it fires off an
// EnrichCompanyDomains child workflow (abandon policy — runs independently).
//
// Python activity: fetch_companies_house_list  (task queue: corpscout-pipelines-python)
// Go activities:   WriteRawInputs, MarkExecutionComplete
func PullCompaniesHouse(ctx workflow.Context, input contracts.PullCompaniesHouseInput) (contracts.PullCompaniesResult, error) {
	var runIDStr string
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return uuid.New().String()
	}).Get(&runIDStr); err != nil {
		return contracts.PullCompaniesResult{}, err
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
	total := contracts.PullCompaniesResult{}
	cursor := ""
	page := 1

	// Accumulated across all pages for the child enrichment workflow.
	var allCompanies []contracts.CompanyLookup

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

		// Collect company lookups for enrichment.
		for _, r := range fetchResult.Records {
			if r.NativeID != "" {
				allCompanies = append(allCompanies, contracts.CompanyLookup{
					NativeID: r.NativeID,
					Name:     r.Name,
				})
			}
		}

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

	// Run domain enrichment as a child workflow and wait for its result.
	// ABANDON policy: if this workflow is killed mid-run, the child keeps going.
	if len(allCompanies) > 0 {
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:         "corpscout-pipelines",
			ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
		})
		childFuture := workflow.ExecuteChildWorkflow(childCtx, EnrichCompanyDomains, contracts.EnrichCompanyDomainsInput{
			Source:    "companies_house",
			Country:   input.Country,
			Companies: allCompanies,
			Force:     false,
		})
		var enrichResult contracts.EnrichCompanyDomainsResult
		if err := childFuture.Get(ctx, &enrichResult); err != nil {
			logger.Warn("domain enrichment child workflow failed", "error", err)
		} else {
			total.DomainsFound = enrichResult.DomainsFound
			total.CompaniesSearched = enrichResult.CompaniesProcessed
			total.Discoveries = enrichResult.Discoveries
			logger.Info("domain enrichment complete",
				"domains_found", enrichResult.DomainsFound,
				"companies_searched", enrichResult.CompaniesProcessed)
		}
	}

	return total, nil
}
