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

type PullAriregisterSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *PullAriregisterSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, input contracts.DownloadSourceFilesInput) (contracts.DownloadSourceFilesResult, error) {
			return contracts.DownloadSourceFilesResult{}, nil
		},
		activity.RegisterOptions{Name: "download_ariregister_dataset"},
	)
	var goAct *activities.GoActivities
	s.env.RegisterActivity(goAct)
}

func (s *PullAriregisterSuite) AfterTest(_, _ string) {
	s.env.AssertExpectations(s.T())
}

func TestPullAriregisterSuite(t *testing.T) {
	suite.Run(t, new(PullAriregisterSuite))
}

func (s *PullAriregisterSuite) Test_Refresh_DownloadsImportsAndMarksComplete() {
	download := contracts.DownloadSourceFilesResult{
		Source:     "ariregister",
		SnapshotID: "ari-2026-05-21",
		Files: []contracts.DownloadedSourceFile{
			{Source: "ariregister", Dataset: "simple-data", FilePath: "/tmp/ari.csv.zip", SnapshotID: "ari-2026-05-21", SHA256: "abc", Format: "csv.zip"},
			{Source: "ariregister", Dataset: "financial", FilePath: "/tmp/ari-fin.csv.zip", SnapshotID: "ari-2026-05-21", SHA256: "def", Format: "csv.zip"},
		},
	}

	s.env.OnActivity("download_ariregister_dataset", mock.Anything, mock.MatchedBy(func(input contracts.DownloadSourceFilesInput) bool {
		return input.Source == "ariregister" &&
			input.Mode == "refresh" &&
			input.OutputDir == "/tmp/ariregister-out"
	})).Return(download, nil).Once()

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.ImportAriregisterBulk, mock.Anything, mock.MatchedBy(func(params contracts.ImportAriregisterBulkParams) bool {
		return params.RunID == "run-ari" &&
			params.CorpscoutRunID == "exec-ari" &&
			len(params.Files) == 2
	})).Return(13, nil).Once()

	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(params contracts.MarkCompleteParams) bool {
		return params.RunID == "run-ari" &&
			params.CorpscoutRunID == "exec-ari" &&
			params.Source == "ariregister" &&
			params.Country == "EE" &&
			params.Result.RecordsWritten == 13 &&
			params.Result.PagesFetched == 2 &&
			params.FinalCursor == "refresh:ari-2026-05-21"
	})).Return(nil).Once()

	s.env.ExecuteWorkflow(workflows.PullAriregister, contracts.PullAriregisterInput{
		CorpscoutRunID: "exec-ari",
		RunID:          "run-ari",
		Mode:           "refresh",
		OutputDir:      "/tmp/ariregister-out",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result contracts.PullCompaniesResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(13, result.RecordsWritten)
	s.Equal(2, result.PagesFetched)
}

func (s *PullAriregisterSuite) Test_EmptyModeDefaultsToRefreshCursor() {
	download := contracts.DownloadSourceFilesResult{
		Source:     "ariregister",
		SnapshotID: "ari-2026-05-21",
		Files:      []contracts.DownloadedSourceFile{{Source: "ariregister", Dataset: "simple-data", FilePath: "/tmp/ari.csv.zip", SnapshotID: "ari-2026-05-21", SHA256: "abc", Format: "csv.zip"}},
	}

	s.env.OnActivity("download_ariregister_dataset", mock.Anything, mock.MatchedBy(func(input contracts.DownloadSourceFilesInput) bool {
		return input.Source == "ariregister" &&
			input.Mode == "refresh" &&
			input.OutputDir == defaultAriregisterOutputDirForTest
	})).Return(download, nil).Once()

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.ImportAriregisterBulk, mock.Anything, mock.Anything).Return(5, nil).Once()
	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(params contracts.MarkCompleteParams) bool {
		return params.Source == "ariregister" &&
			params.Country == "EE" &&
			params.FinalCursor == "refresh:ari-2026-05-21"
	})).Return(nil).Once()

	s.env.ExecuteWorkflow(workflows.PullAriregister, contracts.PullAriregisterInput{
		RunID: "run-ari-default",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *PullAriregisterSuite) Test_BulkMode_DownloadsImportsAndMarksBulkCursor() {
	download := contracts.DownloadSourceFilesResult{
		Source:     "ariregister",
		SnapshotID: "ari-2026-05-21",
		Files:      []contracts.DownloadedSourceFile{{Source: "ariregister", Dataset: "simple-data", FilePath: "/tmp/ari.csv.zip", SnapshotID: "ari-2026-05-21", SHA256: "abc", Format: "csv.zip"}},
	}

	s.env.OnActivity("download_ariregister_dataset", mock.Anything, mock.MatchedBy(func(input contracts.DownloadSourceFilesInput) bool {
		return input.Source == "ariregister" &&
			input.Mode == "bulk" &&
			input.OutputDir == defaultAriregisterOutputDirForTest
	})).Return(download, nil).Once()

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.ImportAriregisterBulk, mock.Anything, mock.Anything).Return(5, nil).Once()
	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(params contracts.MarkCompleteParams) bool {
		return params.Source == "ariregister" &&
			params.Country == "EE" &&
			params.FinalCursor == "bulk:ari-2026-05-21"
	})).Return(nil).Once()

	s.env.ExecuteWorkflow(workflows.PullAriregister, contracts.PullAriregisterInput{
		RunID: "run-ari-bulk",
		Mode:  "bulk",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *PullAriregisterSuite) Test_InvalidModeFailsBeforeActivities() {
	s.env.ExecuteWorkflow(workflows.PullAriregister, contracts.PullAriregisterInput{
		RunID: "run-ari-invalid",
		Mode:  "typo",
	})

	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	s.Error(err)
	s.Contains(err.Error(), "unsupported ariregister mode")
}

const defaultAriregisterOutputDirForTest = "/var/lib/data-pipelines/results/ariregister"
