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

type PullCVRSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *PullCVRSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, input contracts.DownloadSourceFilesInput) (contracts.DownloadSourceFilesResult, error) {
			return contracts.DownloadSourceFilesResult{}, nil
		},
		activity.RegisterOptions{Name: "download_cvr_file_set"},
	)
	var goAct *activities.GoActivities
	s.env.RegisterActivity(goAct)
}

func (s *PullCVRSuite) AfterTest(_, _ string) {
	s.env.AssertExpectations(s.T())
}

func TestPullCVRSuite(t *testing.T) {
	suite.Run(t, new(PullCVRSuite))
}

func (s *PullCVRSuite) Test_Bulk_DownloadsImportsAndMarksComplete() {
	download := contracts.DownloadSourceFilesResult{
		Source:     "cvr",
		SnapshotID: "cvr-2026-05-21",
		Files: []contracts.DownloadedSourceFile{
			{Source: "cvr", Dataset: "company", FilePath: "/tmp/cvr-company.json", SnapshotID: "cvr-2026-05-21", SHA256: "abc", Format: "json"},
			{Source: "cvr", Dataset: "unit", FilePath: "/tmp/cvr-unit.json", SnapshotID: "cvr-2026-05-21", SHA256: "def", Format: "json"},
		},
	}

	s.env.OnActivity("download_cvr_file_set", mock.Anything, mock.MatchedBy(func(input contracts.DownloadSourceFilesInput) bool {
		return input.Source == "cvr" &&
			input.Mode == "bulk" &&
			input.OutputDir == "/tmp/cvr-out"
	})).Return(download, nil).Once()

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.ImportCVRBulk, mock.Anything, mock.MatchedBy(func(params contracts.ImportCVRBulkParams) bool {
		return params.RunID == "run-cvr" &&
			params.CorpscoutRunID == "exec-cvr" &&
			len(params.Files) == 2
	})).Return(25, nil).Once()

	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(params contracts.MarkCompleteParams) bool {
		return params.RunID == "run-cvr" &&
			params.CorpscoutRunID == "exec-cvr" &&
			params.Source == "cvr" &&
			params.Country == "DK" &&
			params.Result.RecordsWritten == 25 &&
			params.Result.PagesFetched == 2 &&
			params.FinalCursor == "bulk:cvr-2026-05-21"
	})).Return(nil).Once()

	s.env.ExecuteWorkflow(workflows.PullCVR, contracts.PullCVRInput{
		CorpscoutRunID: "exec-cvr",
		RunID:          "run-cvr",
		Mode:           "bulk",
		OutputDir:      "/tmp/cvr-out",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result contracts.PullCompaniesResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(25, result.RecordsWritten)
	s.Equal(2, result.PagesFetched)
}

func (s *PullCVRSuite) Test_Incremental_StoresIncrementalCursor() {
	download := contracts.DownloadSourceFilesResult{
		Source:     "cvr",
		SnapshotID: "cvr-incremental-2026-05-21",
		Files:      []contracts.DownloadedSourceFile{{Source: "cvr", Dataset: "company", FilePath: "/tmp/cvr-incremental.json", SnapshotID: "cvr-incremental-2026-05-21", SHA256: "abc", Format: "json"}},
	}

	s.env.OnActivity("download_cvr_file_set", mock.Anything, mock.MatchedBy(func(input contracts.DownloadSourceFilesInput) bool {
		return input.Source == "cvr" && input.Mode == "incremental"
	})).Return(download, nil).Once()

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.ImportCVRBulk, mock.Anything, mock.Anything).Return(3, nil).Once()
	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(params contracts.MarkCompleteParams) bool {
		return params.Source == "cvr" &&
			params.Country == "DK" &&
			params.FinalCursor == "incremental:cvr-incremental-2026-05-21"
	})).Return(nil).Once()

	s.env.ExecuteWorkflow(workflows.PullCVR, contracts.PullCVRInput{
		RunID: "run-cvr-incremental",
		Mode:  "incremental",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *PullCVRSuite) Test_InvalidModeFailsBeforeActivities() {
	s.env.ExecuteWorkflow(workflows.PullCVR, contracts.PullCVRInput{
		RunID: "run-cvr-invalid",
		Mode:  "typo",
	})

	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	s.Error(err)
	s.Contains(err.Error(), "unsupported cvr mode")
}
