package workflows

import (
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

// PullCompaniesHouse fetches all active UK companies from the Companies House API.
// Registered in Temporal as workflow type "PullCompaniesHouse".
func PullCompaniesHouse(ctx workflow.Context, input contracts.PullCompaniesHouseInput) (contracts.PullCompaniesResult, error) {
	return runPullLoop(ctx, pullParams{
		Source:         "companies_house",
		Country:        input.Country,
		IDs:            input.IDs,
		CorpscoutRunID: input.CorpscoutRunID,
	})
}
