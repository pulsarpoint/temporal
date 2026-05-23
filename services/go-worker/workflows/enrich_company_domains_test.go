package workflows_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/pulsarpoint/data-pipelines/contracts"
	"github.com/pulsarpoint/data-pipelines/workflows"
)

func TestEnrichCompanyDomainsMarksRawInputActionEvents(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	company := contracts.CompanyLookup{
		NativeID:   "810202572",
		Name:       "BORTIGARD AS",
		RawInputID: "7ffd5bf3-f96e-4907-9ef3-096eb4056ab8",
	}
	env.RegisterActivityWithOptions(
		func(context.Context, contracts.FilterForDomainDiscoveryParams) (contracts.FilterForDomainDiscoveryResult, error) {
			return contracts.FilterForDomainDiscoveryResult{}, nil
		},
		activity.RegisterOptions{Name: "FilterForDomainDiscovery"},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, contracts.MarkRawInputActionEventsParams) error { return nil },
		activity.RegisterOptions{Name: "MarkRawInputActionEvents"},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, contracts.DiscoverDomainsInput) (contracts.DiscoverDomainsResult, error) {
			return contracts.DiscoverDomainsResult{}, nil
		},
		activity.RegisterOptions{Name: "discover_company_domains"},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, contracts.WriteDiscoveredDomainsParams) error { return nil },
		activity.RegisterOptions{Name: "WriteDiscoveredDomains"},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, contracts.MarkDomainsSearchedParams) error { return nil },
		activity.RegisterOptions{Name: "MarkDomainsSearched"},
	)

	env.OnActivity("FilterForDomainDiscovery", mock.Anything, contracts.FilterForDomainDiscoveryParams{
		Source:           "brreg",
		SourceInputTable: "brreg_company_raw_inputs",
		DomainSink:       contracts.DomainSinkBrregRawInputDomains,
		NativeIDs:        []string{"810202572"},
		Companies:        []contracts.CompanyLookup{company},
		Force:            true,
	}).Return(contracts.FilterForDomainDiscoveryResult{NeedDiscovery: []string{"810202572"}}, nil).Once()
	env.OnActivity("MarkRawInputActionEvents", mock.Anything, contracts.MarkRawInputActionEventsParams{
		ActionIDs: map[string]string{"810202572": "action-1"},
		Status:    "running",
		Message:   "domain discovery started",
	}).Return(nil).Once()
	env.OnActivity("discover_company_domains", mock.Anything, contracts.DiscoverDomainsInput{
		Source:    "brreg",
		Country:   "NO",
		Companies: []contracts.CompanyLookup{company},
	}).Return(contracts.DiscoverDomainsResult{Discoveries: []contracts.DomainDiscovery{{
		NativeID:   "810202572",
		Domain:     "bortigard.no",
		Signal:     "heuristic",
		Confidence: 80,
	}}}, nil).Once()
	env.OnActivity("WriteDiscoveredDomains", mock.Anything, contracts.WriteDiscoveredDomainsParams{
		Source:           "brreg",
		SourceInputTable: "brreg_company_raw_inputs",
		DomainSink:       contracts.DomainSinkBrregRawInputDomains,
		Companies:        []contracts.CompanyLookup{company},
		ActionIDs:        map[string]string{"810202572": "action-1"},
		Force:            true,
		Discoveries: []contracts.DomainDiscovery{{
			NativeID:   "810202572",
			Domain:     "bortigard.no",
			Signal:     "heuristic",
			Confidence: 80,
		}},
	}).Return(nil).Once()
	env.OnActivity("MarkDomainsSearched", mock.Anything, contracts.MarkDomainsSearchedParams{
		Source:    "brreg",
		NativeIDs: []string{"810202572"},
	}).Return(nil).Once()
	env.OnActivity("MarkRawInputActionEvents", mock.Anything, contracts.MarkRawInputActionEventsParams{
		ActionIDs: map[string]string{"810202572": "action-1"},
		Status:    "succeeded",
		Message:   "domain discovery completed",
	}).Return(nil).Once()

	env.ExecuteWorkflow(workflows.EnrichCompanyDomains, contracts.EnrichCompanyDomainsInput{
		Source:           "brreg",
		Country:          "NO",
		SourceInputTable: "brreg_company_raw_inputs",
		DomainSink:       contracts.DomainSinkBrregRawInputDomains,
		Companies:        []contracts.CompanyLookup{company},
		ActionIDs:        map[string]string{"810202572": "action-1"},
		Force:            true,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
