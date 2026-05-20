package workflows

import (
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

// PullBrreg fetches Norwegian companies from the Brønnøysund Register Centre (Brreg).
// Country is always "NO" — hardcoded here so the caller doesn't need to pass it.
// Registered in Temporal as workflow type "PullBrreg".
func PullBrreg(ctx workflow.Context, input contracts.PullBrregInput) (contracts.PullCompaniesResult, error) {
	return runPullLoop(ctx, pullParams{
		Source:         "brreg",
		Country:        "NO",
		IDs:            input.IDs,
		CorpscoutRunID: input.CorpscoutRunID,
	})
}
