package workflows_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
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
		func(ctx context.Context, params contracts.PrepareBrregTranslationBatchParams) (contracts.PrepareBrregTranslationBatchResult, error) {
			return contracts.PrepareBrregTranslationBatchResult{}, nil
		},
		activity.RegisterOptions{Name: "PrepareBrregTranslationBatch"},
	)
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, params contracts.WriteBrregTranslationBatchParams) (contracts.TranslateBrregBatchResult, error) {
			return contracts.TranslateBrregBatchResult{}, nil
		},
		activity.RegisterOptions{Name: "WriteBrregTranslationBatch"},
	)
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, params contracts.TranslateTermsInput) (contracts.TranslateTermsResult, error) {
			return contracts.TranslateTermsResult{}, nil
		},
		activity.RegisterOptions{Name: "TranslateTermsWithDSPy"},
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

func (s *translateBrregWorkflowSuite) TestTranslatesCacheMissesWithPythonDSPyActivity() {
	s.env.OnActivity("PrepareBrregTranslationBatch", mock.Anything, mock.Anything).
		Return(contracts.PrepareBrregTranslationBatchResult{
			Claimed: 1,
			Rows: []contracts.BrregTranslationRowPayload{
				{ID: "row-1", RawPayload: []byte(`{"navn":"TEST AS"}`)},
			},
			MissesByCategory: map[string][]contracts.TranslationItem{
				"capital_type": {
					{ID: "t0", Text: "Aksjekapital"},
				},
			},
		}, nil).
		Once()
	s.env.OnActivity("TranslateTermsWithDSPy", mock.Anything, contracts.TranslateTermsInput{
		Category:      "capital_type",
		Model:         "custom-model",
		PromptVersion: "v2",
		Items: []contracts.TranslationItem{
			{ID: "t0", Text: "Aksjekapital"},
		},
	}).Return(contracts.TranslateTermsResult{
		Model: "custom-model",
		Translations: []contracts.TranslatedTerm{
			{ID: "t0", Translation: "Share capital"},
		},
	}, nil).Once()
	s.env.OnActivity("WriteBrregTranslationBatch", mock.Anything, mock.MatchedBy(func(params contracts.WriteBrregTranslationBatchParams) bool {
		return len(params.NewTranslations) == 1 &&
			params.NewTranslations[0].ID == "t0" &&
			params.NewTranslations[0].Category == "capital_type" &&
			params.NewTranslations[0].Text == "Aksjekapital" &&
			params.NewTranslations[0].Translation == "Share capital"
	})).Return(contracts.TranslateBrregBatchResult{Claimed: 1, Translated: 1}, nil).Once()
	s.env.OnActivity("PrepareBrregTranslationBatch", mock.Anything, mock.Anything).
		Return(contracts.PrepareBrregTranslationBatchResult{}, nil).
		Once()

	s.env.ExecuteWorkflow(workflows.TranslateBrregRawInputs, contracts.TranslateBrregInput{
		PromptVersion: "v2",
		Model:         "custom-model",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result contracts.TranslateBrregBatchResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(1, result.Claimed)
	s.Equal(1, result.Translated)
	s.Equal(0, result.Failed)
}
