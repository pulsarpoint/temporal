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

type translateSourceWorkflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func TestSourceTranslationWorkflowSuite(t *testing.T) {
	suite.Run(t, new(translateSourceWorkflowSuite))
}

func (s *translateSourceWorkflowSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, params contracts.PrepareSourceTranslationBatchParams) (contracts.PrepareSourceTranslationBatchResult, error) {
			return contracts.PrepareSourceTranslationBatchResult{}, nil
		},
		activity.RegisterOptions{Name: "PrepareSourceTranslationBatch"},
	)
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, params contracts.WriteSourceTranslationBatchParams) (contracts.TranslateSourceBatchResult, error) {
			return contracts.TranslateSourceBatchResult{}, nil
		},
		activity.RegisterOptions{Name: "WriteSourceTranslationBatch"},
	)
	s.env.RegisterActivityWithOptions(
		func(ctx context.Context, params contracts.TranslateTermsInput) (contracts.TranslateTermsResult, error) {
			return contracts.TranslateTermsResult{}, nil
		},
		activity.RegisterOptions{Name: "TranslateTermsWithDSPy"},
	)
}

func (s *translateSourceWorkflowSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
}

func (s *translateSourceWorkflowSuite) TestCVRUsesDanishSourceLanguageAndGenericActivities() {
	s.env.OnActivity("PrepareSourceTranslationBatch", mock.Anything, mock.MatchedBy(func(params contracts.PrepareSourceTranslationBatchParams) bool {
		return params.Source == "cvr" &&
			params.PromptVersion == "v2" &&
			params.Model == "custom-model" &&
			params.Filters["translation_action_status"] == "failed" &&
			params.BatchSize == 50
	})).Return(contracts.PrepareSourceTranslationBatchResult{
		Claimed: 1,
		Rows: []contracts.SourceTranslationRowPayload{
			{ID: "row-1", RawPayload: []byte(`{"company_name":"Example Denmark ApS"}`)},
		},
		MissesByCategory: map[string][]contracts.TranslationItem{
			"activity": {
				{ID: "t0", Text: "Konsulentydelser"},
			},
			"legal_form": {
				{ID: "t0", Text: "Anpartsselskab"},
			},
		},
	}, nil).Once()
	s.env.OnActivity("TranslateTermsWithDSPy", mock.Anything, contracts.TranslateTermsInput{
		Category:      "mixed",
		SourceLang:    "da",
		TargetLang:    "en",
		Model:         "custom-model",
		PromptVersion: "v2",
		Items: []contracts.TranslationItem{
			{ID: "activity:t0", Category: "activity", Text: "Konsulentydelser"},
			{ID: "legal_form:t0", Category: "legal_form", Text: "Anpartsselskab"},
		},
	}).Return(contracts.TranslateTermsResult{
		Model: "custom-model",
		Translations: []contracts.TranslatedTerm{
			{ID: "activity:t0", Translation: "Consulting services"},
			{ID: "legal_form:t0", Translation: "Private limited company"},
		},
	}, nil).Once()
	s.env.OnActivity("WriteSourceTranslationBatch", mock.Anything, mock.MatchedBy(func(params contracts.WriteSourceTranslationBatchParams) bool {
		if params.Source != "cvr" || len(params.NewTranslations) != 2 {
			return false
		}
		byCategory := map[string]contracts.SourceTranslatedTerm{}
		for _, term := range params.NewTranslations {
			byCategory[term.Category] = term
		}
		return byCategory["activity"].Text == "Konsulentydelser" &&
			byCategory["activity"].Translation == "Consulting services" &&
			byCategory["legal_form"].Text == "Anpartsselskab" &&
			byCategory["legal_form"].Translation == "Private limited company"
	})).Return(contracts.TranslateSourceBatchResult{Claimed: 1, Translated: 1}, nil).Once()
	s.env.OnActivity("PrepareSourceTranslationBatch", mock.Anything, mock.Anything).
		Return(contracts.PrepareSourceTranslationBatchResult{}, nil).
		Once()

	s.env.ExecuteWorkflow(workflows.TranslateSourceRawInputs, contracts.TranslateSourceInput{
		Source:        "cvr",
		PromptVersion: "v2",
		Model:         "custom-model",
		Filters:       map[string]string{"translation_action_status": "failed"},
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result contracts.TranslateSourceBatchResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(1, result.Claimed)
	s.Equal(1, result.Translated)
	s.Equal(0, result.Failed)
}
