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

type SyncCompaniesHouseSICCodesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *SyncCompaniesHouseSICCodesSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, input contracts.DownloadSourceFilesInput) (contracts.DownloadSourceFilesResult, error) {
			return contracts.DownloadSourceFilesResult{}, nil
		},
		activity.RegisterOptions{Name: "download_companies_house_sic_codes"},
	)
	var goAct *activities.GoActivities
	s.env.RegisterActivity(goAct)
}

func (s *SyncCompaniesHouseSICCodesSuite) AfterTest(_, _ string) {
	s.env.AssertExpectations(s.T())
}

func TestSyncCompaniesHouseSICCodesSuite(t *testing.T) {
	suite.Run(t, new(SyncCompaniesHouseSICCodesSuite))
}

func (s *SyncCompaniesHouseSICCodesSuite) Test_DownloadsAndImportsSICCodes() {
	file := contracts.DownloadedSourceFile{
		Source:     "companies_house_sic",
		Dataset:    "sic_codes",
		FilePath:   "/tmp/sic.csv",
		SnapshotID: "2026-05-22",
		SHA256:     "sha123",
		Format:     "csv",
		SourceURL:  "https://example.test/sic.csv",
	}

	s.env.OnActivity("download_companies_house_sic_codes", mock.Anything, mock.MatchedBy(func(input contracts.DownloadSourceFilesInput) bool {
		return input.Source == "companies_house_sic" &&
			input.Mode == "full" &&
			input.OutputDir == "/tmp/sic-out" &&
			input.SnapshotID == "2026-05-22"
	})).Return(contracts.DownloadSourceFilesResult{
		Source:     "companies_house_sic",
		SnapshotID: "2026-05-22",
		Files:      []contracts.DownloadedSourceFile{file},
	}, nil).Once()

	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.ImportCompaniesHouseSICCodes, mock.Anything, file).Return(733, nil).Once()

	s.env.ExecuteWorkflow(workflows.SyncCompaniesHouseSICCodes, contracts.DownloadSourceFilesInput{
		OutputDir:  "/tmp/sic-out",
		SnapshotID: "2026-05-22",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var imported int
	s.NoError(s.env.GetWorkflowResult(&imported))
	s.Equal(733, imported)
}
