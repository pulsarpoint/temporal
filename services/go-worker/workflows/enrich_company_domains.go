package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

const domainBatchSize = 25

// EnrichCompanyDomains discovers websites for the given companies using
// multiple signals (DuckDuckGo, Wikidata, crt.sh, heuristic DNS).
//
// The domain_cache SQLite table prevents repeat searches: companies already
// searched are skipped unless Force=true.
//
// Python activity: discover_company_domains  (task queue: corpscout-pipelines-python)
// Go activities:   FilterForDomainDiscovery, WriteDiscoveredDomains, MarkDomainsSearched
func EnrichCompanyDomains(ctx workflow.Context, input contracts.EnrichCompanyDomainsInput) (contracts.EnrichCompanyDomainsResult, error) {
	goCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines",
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
			InitialInterval: 5 * time.Second,
		},
	})

	// Each batch can take up to ~90s (25 companies × ~3s per company across signals).
	pythonCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines-python",
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    3,
			InitialInterval:    30 * time.Second,
			MaximumInterval:    5 * time.Minute,
			BackoffCoefficient: 2.0,
		},
	})

	logger := workflow.GetLogger(ctx)
	var goAct *activities.GoActivities

	// Collect all native IDs to check against the cache.
	nativeIDs := make([]string, 0, len(input.Companies))
	for _, c := range input.Companies {
		nativeIDs = append(nativeIDs, c.NativeID)
	}

	var filterResult contracts.FilterForDomainDiscoveryResult
	if err := workflow.ExecuteActivity(goCtx, goAct.FilterForDomainDiscovery, contracts.FilterForDomainDiscoveryParams{
		Source:    input.Source,
		NativeIDs: nativeIDs,
		Force:     input.Force,
	}).Get(ctx, &filterResult); err != nil {
		return contracts.EnrichCompanyDomainsResult{}, err
	}

	if len(filterResult.NeedDiscovery) == 0 {
		if err := markRawInputActionEvents(ctx, goCtx, goAct, input.ActionIDs, nativeIDs, "succeeded", "domain discovery already completed", ""); err != nil {
			return contracts.EnrichCompanyDomainsResult{}, err
		}
		logger.Info("all companies already searched for domains, nothing to do",
			"total", len(input.Companies), "source", input.Source)
		return contracts.EnrichCompanyDomainsResult{}, nil
	}

	if alreadyDone := actionIDsOutsideBatch(input.ActionIDs, nativeIDs, filterResult.NeedDiscovery); len(alreadyDone) > 0 {
		if err := workflow.ExecuteActivity(goCtx, goAct.MarkRawInputActionEvents, contracts.MarkRawInputActionEventsParams{
			ActionIDs: alreadyDone,
			Status:    "succeeded",
			Message:   "domain discovery already completed",
		}).Get(ctx, nil); err != nil {
			return contracts.EnrichCompanyDomainsResult{}, err
		}
	}

	logger.Info("starting domain discovery",
		"to_search", len(filterResult.NeedDiscovery), "total", len(input.Companies))

	// Build a lookup map for fast access to company names.
	companyMap := make(map[string]contracts.CompanyLookup, len(input.Companies))
	for _, c := range input.Companies {
		companyMap[c.NativeID] = c
	}

	totalDomainsFound := 0
	var allDiscoveries []contracts.DomainDiscovery
	need := filterResult.NeedDiscovery

	for i := 0; i < len(need); i += domainBatchSize {
		end := min(i+domainBatchSize, len(need))
		batch := need[i:end]

		batchCompanies := make([]contracts.CompanyLookup, 0, len(batch))
		for _, id := range batch {
			if c, ok := companyMap[id]; ok {
				batchCompanies = append(batchCompanies, c)
			}
		}
		batchActionIDs := actionIDsForBatch(input.ActionIDs, batch)
		if len(batchActionIDs) > 0 {
			if err := workflow.ExecuteActivity(goCtx, goAct.MarkRawInputActionEvents, contracts.MarkRawInputActionEventsParams{
				ActionIDs: batchActionIDs,
				Status:    "running",
				Message:   "domain discovery started",
			}).Get(ctx, nil); err != nil {
				return contracts.EnrichCompanyDomainsResult{}, err
			}
		}

		var discoverResult contracts.DiscoverDomainsResult
		discoverErr := workflow.ExecuteActivity(pythonCtx, "discover_company_domains", contracts.DiscoverDomainsInput{
			Source:    input.Source,
			Country:   input.Country,
			Companies: batchCompanies,
		}).Get(ctx, &discoverResult)

		if discoverErr != nil {
			// Log and continue — don't abort the whole workflow over one batch.
			// Still mark as searched so we don't retry endlessly.
			logger.Warn("domain discovery batch failed, marking searched anyway",
				"batch_start", i, "error", discoverErr)
			if len(batchActionIDs) > 0 {
				if err := workflow.ExecuteActivity(goCtx, goAct.MarkRawInputActionEvents, contracts.MarkRawInputActionEventsParams{
					ActionIDs: batchActionIDs,
					Status:    "failed",
					Message:   "domain discovery failed",
					Error:     discoverErr.Error(),
				}).Get(ctx, nil); err != nil {
					return contracts.EnrichCompanyDomainsResult{}, err
				}
			}
		} else if len(discoverResult.Discoveries) > 0 {
			if err := workflow.ExecuteActivity(goCtx, goAct.WriteDiscoveredDomains, contracts.WriteDiscoveredDomainsParams{
				Source:      input.Source,
				Discoveries: discoverResult.Discoveries,
			}).Get(ctx, nil); err != nil {
				return contracts.EnrichCompanyDomainsResult{}, err
			}
			totalDomainsFound += len(discoverResult.Discoveries)
			allDiscoveries = append(allDiscoveries, discoverResult.Discoveries...)
		}

		// Mark whole batch as searched regardless of whether domains were found.
		if err := workflow.ExecuteActivity(goCtx, goAct.MarkDomainsSearched, contracts.MarkDomainsSearchedParams{
			Source:    input.Source,
			NativeIDs: batch,
		}).Get(ctx, nil); err != nil {
			return contracts.EnrichCompanyDomainsResult{}, err
		}
		if discoverErr == nil && len(batchActionIDs) > 0 {
			if err := workflow.ExecuteActivity(goCtx, goAct.MarkRawInputActionEvents, contracts.MarkRawInputActionEventsParams{
				ActionIDs: batchActionIDs,
				Status:    "succeeded",
				Message:   "domain discovery completed",
			}).Get(ctx, nil); err != nil {
				return contracts.EnrichCompanyDomainsResult{}, err
			}
		}

		logger.Info("domain discovery batch done",
			"batch", i/domainBatchSize+1,
			"size", len(batch),
			"found", len(discoverResult.Discoveries))
	}

	return contracts.EnrichCompanyDomainsResult{
		CompaniesProcessed: len(filterResult.NeedDiscovery),
		DomainsFound:       totalDomainsFound,
		Discoveries:        allDiscoveries,
	}, nil
}

func markRawInputActionEvents(
	ctx workflow.Context,
	goCtx workflow.Context,
	goAct *activities.GoActivities,
	actionIDs map[string]string,
	nativeIDs []string,
	status string,
	message string,
	errorText string,
) error {
	ids := actionIDsForBatch(actionIDs, nativeIDs)
	if len(ids) == 0 {
		return nil
	}
	return workflow.ExecuteActivity(goCtx, goAct.MarkRawInputActionEvents, contracts.MarkRawInputActionEventsParams{
		ActionIDs: ids,
		Status:    status,
		Message:   message,
		Error:     errorText,
	}).Get(ctx, nil)
}

func actionIDsForBatch(actionIDs map[string]string, nativeIDs []string) map[string]string {
	out := map[string]string{}
	for _, nativeID := range nativeIDs {
		if actionID := actionIDs[nativeID]; actionID != "" {
			out[nativeID] = actionID
		}
	}
	return out
}

func actionIDsOutsideBatch(actionIDs map[string]string, allNativeIDs, batchNativeIDs []string) map[string]string {
	inBatch := map[string]struct{}{}
	for _, nativeID := range batchNativeIDs {
		inBatch[nativeID] = struct{}{}
	}
	out := map[string]string{}
	for _, nativeID := range allNativeIDs {
		if _, ok := inBatch[nativeID]; ok {
			continue
		}
		if actionID := actionIDs[nativeID]; actionID != "" {
			out[nativeID] = actionID
		}
	}
	return out
}
