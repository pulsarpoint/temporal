package workflows_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
	"github.com/pulsarpoint/data-pipelines/workflows"
)

type PullCompaniesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *PullCompaniesSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	// Register the Python activity stub so OnActivity can mock it by name.
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, input contracts.FetchPageInput) (contracts.FetchResult, error) {
			return contracts.FetchResult{}, nil
		},
		activity.RegisterOptions{Name: "fetch_page"},
	)
	// Register the Go activities so the testsuite can match by method reference.
	var goAct *activities.GoActivities
	s.env.RegisterActivity(goAct)
}

func (s *PullCompaniesSuite) AfterTest(_, _ string) {
	s.env.AssertExpectations(s.T())
}

func TestPullCompaniesSuite(t *testing.T) {
	suite.Run(t, new(PullCompaniesSuite))
}

func (s *PullCompaniesSuite) Test_SinglePage_WritesRecords() {
	fetchResult := contracts.FetchResult{
		Records: []contracts.RawRecord{
			{NativeID: "12345678", Name: "ACME LTD", Status: "active", Hash: "h1"},
			{NativeID: "87654321", Name: "GLOBEX LTD", Status: "active", Hash: "h2"},
		},
		HasMore: false,
	}

	// Python FetchPage activity is referenced by name string "fetch_page"
	s.env.OnActivity("fetch_page", mock.Anything, contracts.FetchPageInput{
		Source: "companies_house", Country: "GB", Page: 1,
	}).Return(fetchResult, nil)

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.WriteRawInputs, mock.Anything, mock.MatchedBy(func(p contracts.WriteRawInputsParams) bool {
		return p.Source == "companies_house" && len(p.Records) == 2
	})).Return(2, nil)

	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(p contracts.MarkCompleteParams) bool {
		return p.CorpscoutRunID == "exec-123" && p.Result.RecordsWritten == 2
	})).Return(nil)

	s.env.ExecuteWorkflow(workflows.PullCompanies, contracts.PullCompaniesInput{
		Source:         "companies_house",
		Country:        "GB",
		CorpscoutRunID: "exec-123",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result contracts.PullCompaniesResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(2, result.RecordsWritten)
	s.Equal(1, result.PagesFetched)
}

func (s *PullCompaniesSuite) Test_MultiPage_FetchesAll() {
	page1 := contracts.FetchResult{
		Records:    []contracts.RawRecord{{NativeID: "00000001", Name: "A", Status: "active", Hash: "h1"}},
		HasMore:    true,
		NextCursor: "2024-01-01,1",
	}
	page2 := contracts.FetchResult{
		Records: []contracts.RawRecord{{NativeID: "00000002", Name: "B", Status: "active", Hash: "h2"}},
		HasMore: false,
	}

	var goAct *activities.GoActivities

	s.env.OnActivity("fetch_page", mock.Anything, contracts.FetchPageInput{
		Source: "companies_house", Country: "GB", Page: 1,
	}).Return(page1, nil)
	s.env.OnActivity("fetch_page", mock.Anything, contracts.FetchPageInput{
		Source: "companies_house", Country: "GB", Page: 2, Cursor: "2024-01-01,1",
	}).Return(page2, nil)

	s.env.OnActivity(goAct.WriteRawInputs, mock.Anything, mock.Anything).Return(1, nil).Times(2)
	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(workflows.PullCompanies, contracts.PullCompaniesInput{
		Source:         "companies_house",
		Country:        "GB",
		CorpscoutRunID: "exec-456",
	})

	s.True(s.env.IsWorkflowCompleted())
	var result contracts.PullCompaniesResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(2, result.RecordsWritten)
	s.Equal(2, result.PagesFetched)
}
