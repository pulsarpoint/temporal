package activities_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
)

func TestBuildCVRRawPayloadEn_NormalizesPayloadAndKeepsIdentifiersUntranslated(t *testing.T) {
	raw := json.RawMessage(`{
		"cvr_number": "12345678",
		"company_name": "Example Denmark ApS",
		"registration_status": "NORMAL",
		"company_type": "Anpartsselskab",
		"website": "https://example.dk",
		"email": "hello@example.dk",
		"phone": "+4512345678",
		"addresses": [{"street": "Testvej 1", "city": "København", "country": "Danmark"}],
		"industries": [{"code": "620100", "description": "Computerprogrammering"}],
		"roles": [{"name": "Jane Example", "role": "Direktør"}],
		"owners": [{"name": "Example Holding ApS", "ownership_type": "Direkte ejerskab"}],
		"beneficial_owners": [{"name": "Jane Example", "ownership_type": "Reel ejer"}],
		"financials": [{"year": 2024, "note": "Årsrapport mangler"}]
	}`)

	translated, err := activities.BuildCVRRawPayloadEn(context.Background(), raw, activities.SourceTranslationSet{
		"NORMAL":                "Normal",
		"Anpartsselskab":        "Private limited company",
		"Computerprogrammering": "Computer programming",
		"Direktør":              "Director",
		"Direkte ejerskab":      "Direct ownership",
		"Reel ejer":             "Beneficial owner",
		"Årsrapport mangler":    "Annual report missing",
	}, activities.FXRateSet{})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(translated, &payload))

	identity := payload["identity"].(map[string]any)
	require.Equal(t, "12345678", identity["registration_number"])
	require.Equal(t, "Example Denmark ApS", identity["name"])

	require.Equal(t, "Private limited company", payload["legal_form"])
	require.Equal(t, "Normal", payload["status"])

	contacts := payload["contacts"].(map[string]any)
	require.Equal(t, "https://example.dk", contacts["website"])
	require.Equal(t, "hello@example.dk", contacts["email"])
	require.Equal(t, "+4512345678", contacts["phone"])

	roles := payload["roles"].([]any)
	role := roles[0].(map[string]any)
	require.Equal(t, "Jane Example", role["name"])
	require.Equal(t, "Director", role["role"])

	sourceFragments := payload["source_fragments"].(map[string]any)
	require.Equal(t, "NORMAL", sourceFragments["registration_status"])
}

func TestBuildCVRRawPayloadEn_MissingIndividualTermFailsSafely(t *testing.T) {
	raw := json.RawMessage(`{
		"cvr_number": "12345678",
		"company_name": "Example Denmark ApS",
		"company_type": "Anpartsselskab"
	}`)

	_, err := activities.BuildCVRRawPayloadEn(context.Background(), raw, activities.SourceTranslationSet{}, activities.FXRateSet{})
	require.ErrorContains(t, err, "missing translation")
	require.NotContains(t, err.Error(), "SELECT")
}

func TestBuildAriregisterRawPayloadEn_NormalizesPayloadAndKeepsIdentifiersUntranslated(t *testing.T) {
	raw := json.RawMessage(`{
		"registry_code": "10000001",
		"legal_name": "Example Estonia OÜ",
		"registration_status": "registrisse kantud",
		"legal_form": "Osaühing",
		"vat_number": "EE100000001",
		"website": "https://example.ee",
		"email": "info@example.ee",
		"phone": "+3725550100",
		"activities": [{"code": "62011", "description": "Programmeerimine"}],
		"registry_card_persons": [{"name": "Mari Example", "role": "Juhatuse liige"}],
		"shareholders": [{"name": "Example Holding OÜ", "ownership_type": "Osanik"}],
		"beneficial_owners": [{"name": "Mari Example", "ownership_type": "Tegelik kasusaaja"}],
		"financials": [{"year": 2024, "indicator": "Müügitulu"}]
	}`)

	translated, err := activities.BuildAriregisterRawPayloadEn(context.Background(), raw, activities.SourceTranslationSet{
		"registrisse kantud": "Registered",
		"Osaühing":           "Private limited company",
		"Programmeerimine":   "Programming",
		"Juhatuse liige":     "Management board member",
		"Osanik":             "Shareholder",
		"Tegelik kasusaaja":  "Beneficial owner",
		"Müügitulu":          "Revenue",
	}, activities.FXRateSet{})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(translated, &payload))

	identity := payload["identity"].(map[string]any)
	require.Equal(t, "10000001", identity["registration_number"])
	require.Equal(t, "Example Estonia OÜ", identity["name"])
	require.Equal(t, "EE100000001", identity["vat_number"])

	require.Equal(t, "Private limited company", payload["legal_form"])
	require.Equal(t, "Registered", payload["status"])

	persons := payload["registry_card_persons"].([]any)
	person := persons[0].(map[string]any)
	require.Equal(t, "Mari Example", person["name"])
	require.Equal(t, "Management board member", person["role"])
}
