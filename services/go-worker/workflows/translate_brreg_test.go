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
	s.env.OnActivity("PrepareBrregTranslationBatch", mock.Anything, mock.MatchedBy(func(params contracts.PrepareBrregTranslationBatchParams) bool {
		return params.PromptVersion == "v2" &&
			params.Model == "custom-model" &&
			params.Filters["translation_action_status"] == "failed"
	})).
		Return(contracts.PrepareBrregTranslationBatchResult{
			Claimed: 1,
			Rows: []contracts.BrregTranslationRowPayload{
				{ID: "row-1", RawPayload: []byte(`{"navn":"TEST AS"}`)},
			},
			MissesByCategory: map[string][]contracts.TranslationItem{
				"activity": {
					{ID: "t0", Text: "Eie aksjer"},
				},
				"capital_type": {
					{ID: "t0", Text: "Aksjekapital"},
				},
			},
		}, nil).
		Once()
	s.env.OnActivity("TranslateTermsWithDSPy", mock.Anything, contracts.TranslateTermsInput{
		Category:      "mixed",
		SourceLang:    "no",
		TargetLang:    "en",
		Model:         "custom-model",
		PromptVersion: "v2",
		Items: []contracts.TranslationItem{
			{ID: "activity:t0", Category: "activity", Text: "Eie aksjer"},
			{ID: "capital_type:t0", Category: "capital_type", Text: "Aksjekapital"},
		},
	}).Return(contracts.TranslateTermsResult{
		Model: "custom-model",
		Translations: []contracts.TranslatedTerm{
			{ID: "activity:t0", Translation: "Own shares"},
			{ID: "capital_type:t0", Translation: "Share capital"},
		},
	}, nil).Once()
	s.env.OnActivity("WriteBrregTranslationBatch", mock.Anything, mock.MatchedBy(func(params contracts.WriteBrregTranslationBatchParams) bool {
		if len(params.NewTranslations) != 2 {
			return false
		}
		byCategory := map[string]contracts.BrregTranslatedTerm{}
		for _, term := range params.NewTranslations {
			byCategory[term.Category] = term
		}
		return byCategory["activity"].ID == "t0" &&
			byCategory["activity"].Text == "Eie aksjer" &&
			byCategory["activity"].Translation == "Own shares" &&
			byCategory["capital_type"].ID == "t0" &&
			byCategory["capital_type"].Text == "Aksjekapital" &&
			byCategory["capital_type"].Translation == "Share capital"
	})).Return(contracts.TranslateBrregBatchResult{Claimed: 1, Translated: 1}, nil).Once()
	s.env.OnActivity("PrepareBrregTranslationBatch", mock.Anything, mock.Anything).
		Return(contracts.PrepareBrregTranslationBatchResult{}, nil).
		Once()

	s.env.ExecuteWorkflow(workflows.TranslateBrregRawInputs, contracts.TranslateBrregInput{
		PromptVersion: "v2",
		Model:         "custom-model",
		Filters:       map[string]string{"translation_action_status": "failed"},
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result contracts.TranslateBrregBatchResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(1, result.Claimed)
	s.Equal(1, result.Translated)
	s.Equal(0, result.Failed)
}
