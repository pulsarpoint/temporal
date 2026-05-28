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

type PullBrregSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *PullBrregSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, outputDir string) (contracts.DownloadBrregBulkResult, error) {
			return contracts.DownloadBrregBulkResult{}, nil
		},
		activity.RegisterOptions{Name: "download_brreg_bulk"},
	)
	var goAct *activities.GoActivities
	s.env.RegisterActivity(goAct)
}

func (s *PullBrregSuite) AfterTest(_, _ string) {
	s.env.AssertExpectations(s.T())
}

func TestPullBrregSuite(t *testing.T) {
	suite.Run(t, new(PullBrregSuite))
}

func (s *PullBrregSuite) Test_BulkLimit_PassesLimitAndSkipsCheckpoint() {
	download := contracts.DownloadBrregBulkResult{
		FilePath: "/tmp/brreg.json.gz",
		Date:     "2026-05-21",
	}

	s.env.OnActivity("download_brreg_bulk", mock.Anything, "/tmp/brreg-out").Return(download, nil).Once()

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.ImportBrregBulk, mock.Anything, mock.MatchedBy(func(params contracts.ImportBrregBulkParams) bool {
		return params.RunID == "run-brreg" &&
			params.CorpscoutRunID == "exec-brreg" &&
			params.FilePath == "/tmp/brreg.json.gz" &&
			params.Limit == 1000
	})).Return(1000, nil).Once()

	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(params contracts.MarkCompleteParams) bool {
		return params.RunID == "run-brreg" &&
			params.CorpscoutRunID == "exec-brreg" &&
			params.Source == "brreg" &&
			params.Country == "NO" &&
			params.Result.RecordsWritten == 1000 &&
			params.Result.PagesFetched == 1 &&
			params.FinalCursor == ""
	})).Return(nil).Once()

	s.env.ExecuteWorkflow(workflows.PullBrreg, contracts.PullBrregInput{
		CorpscoutRunID: "exec-brreg",
		RunID:          "run-brreg",
		OutputDir:      "/tmp/brreg-out",
		Mode:           "bulk",
		Limit:          1000,
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result contracts.PullCompaniesResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(1000, result.RecordsWritten)
	s.Equal(1, result.PagesFetched)
}

func (s *PullBrregSuite) Test_BulkWithoutLimitStoresBulkCheckpoint() {
	download := contracts.DownloadBrregBulkResult{
		FilePath: "/tmp/brreg.json.gz",
		Date:     "2026-05-21",
	}

	s.env.OnActivity("download_brreg_bulk", mock.Anything, defaultBrregOutputDirForTest).Return(download, nil).Once()

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.ImportBrregBulk, mock.Anything, mock.MatchedBy(func(params contracts.ImportBrregBulkParams) bool {
		return params.Limit == 0
	})).Return(25, nil).Once()

	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(params contracts.MarkCompleteParams) bool {
		return params.Source == "brreg" &&
			params.Country == "NO" &&
			params.FinalCursor == "bulk:2026-05-21"
	})).Return(nil).Once()

	s.env.ExecuteWorkflow(workflows.PullBrreg, contracts.PullBrregInput{RunID: "run-brreg-default"})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

const defaultBrregOutputDirForTest = "/var/lib/data-pipelines/results/brreg"
