package workflows_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/pulsarpoint/data-pipelines/contracts"
	"github.com/pulsarpoint/data-pipelines/workflows"
)

type translateBrregWorkflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func TestTranslateBrregWorkflowSuite(t *testing.T) {
	suite.Run(t, new(translateBrregWorkflowSuite))
}

func (s *translateBrregWorkflowSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, params contracts.TranslateBrregBatchParams) (contracts.TranslateBrregBatchResult, error) {
			return contracts.TranslateBrregBatchResult{}, nil
		},
		activity.RegisterOptions{Name: "TranslateBrregBatch"},
	)
}

func (s *translateBrregWorkflowSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
}

func (s *translateBrregWorkflowSuite) TestStopsWhenClaimedIsZero() {
	s.env.ExecuteWorkflow(workflows.TranslateBrregRawInputs, contracts.TranslateBrregInput{
		PromptVersion: "v1",
		Model:         "qwen3:6b",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result contracts.TranslateBrregBatchResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(0, result.Claimed)
	s.Equal(0, result.Translated)
	s.Equal(0, result.Failed)
}
