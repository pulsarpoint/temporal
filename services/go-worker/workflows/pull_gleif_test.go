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

type PullGLEIFSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *PullGLEIFSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, input contracts.DownloadSourceFilesInput) (contracts.DownloadSourceFilesResult, error) {
			return contracts.DownloadSourceFilesResult{}, nil
		},
		activity.RegisterOptions{Name: "download_gleif_golden_copy"},
	)
	var goAct *activities.GoActivities
	s.env.RegisterActivity(goAct)
}

func (s *PullGLEIFSuite) AfterTest(_, _ string) {
	s.env.AssertExpectations(s.T())
}

func TestPullGLEIFSuite(t *testing.T) {
	suite.Run(t, new(PullGLEIFSuite))
}

func (s *PullGLEIFSuite) Test_Bulk_DownloadsImportsAndMarksComplete() {
	download := contracts.DownloadSourceFilesResult{
		Source:     "gleif",
		SnapshotID: "gleif-2026-05-21",
		Files: []contracts.DownloadedSourceFile{
			{Source: "gleif", Dataset: "lei2", FilePath: "/tmp/gleif.json", SnapshotID: "gleif-2026-05-21", SHA256: "abc", Format: "json"},
		},
	}

	s.env.OnActivity("download_gleif_golden_copy", mock.Anything, mock.MatchedBy(func(input contracts.DownloadSourceFilesInput) bool {
		return input.Source == "gleif" &&
			input.Mode == "full" &&
			input.OutputDir == "/tmp/gleif-out"
	})).Return(download, nil).Once()

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.ImportGLEIFGoldenCopy, mock.Anything, mock.MatchedBy(func(params contracts.ImportGLEIFGoldenCopyParams) bool {
		return params.RunID == "run-gleif" &&
			params.CorpscoutRunID == "exec-gleif" &&
			len(params.Files) == 1 &&
			params.Files[0].FilePath == "/tmp/gleif.json"
	})).Return(42, nil).Once()

	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(params contracts.MarkCompleteParams) bool {
		return params.RunID == "run-gleif" &&
			params.CorpscoutRunID == "exec-gleif" &&
			params.Source == "gleif" &&
			params.Country == "" &&
			params.Result.RecordsWritten == 42 &&
			params.Result.PagesFetched == 1 &&
			params.FinalCursor == "bulk:gleif-2026-05-21"
	})).Return(nil).Once()

	s.env.ExecuteWorkflow(workflows.PullGLEIF, contracts.PullGLEIFInput{
		CorpscoutRunID: "exec-gleif",
		RunID:          "run-gleif",
		Mode:           "bulk",
		OutputDir:      "/tmp/gleif-out",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result contracts.PullCompaniesResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(42, result.RecordsWritten)
	s.Equal(1, result.PagesFetched)
}

func (s *PullGLEIFSuite) Test_Delta_StoresDeltaCursor() {
	download := contracts.DownloadSourceFilesResult{
		Source:     "gleif",
		SnapshotID: "gleif-delta-2026-05-21T10",
		Files:      []contracts.DownloadedSourceFile{{Source: "gleif", Dataset: "lei2", FilePath: "/tmp/gleif-delta.json", SnapshotID: "gleif-delta-2026-05-21T10", SHA256: "def", Format: "json"}},
	}

	s.env.OnActivity("download_gleif_golden_copy", mock.Anything, mock.MatchedBy(func(input contracts.DownloadSourceFilesInput) bool {
		return input.Source == "gleif" &&
			input.Mode == "delta" &&
			input.DeltaWindow == "PT1H"
	})).Return(download, nil).Once()

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.ImportGLEIFGoldenCopy, mock.Anything, mock.Anything).Return(7, nil).Once()
	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(params contracts.MarkCompleteParams) bool {
		return params.Source == "gleif" &&
			params.Country == "" &&
			params.FinalCursor == "delta:gleif-delta-2026-05-21T10"
	})).Return(nil).Once()

	s.env.ExecuteWorkflow(workflows.PullGLEIF, contracts.PullGLEIFInput{
		RunID:       "run-gleif-delta",
		Mode:        "delta",
		DeltaWindow: "PT1H",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}
