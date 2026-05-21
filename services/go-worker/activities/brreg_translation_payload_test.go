package activities_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
)

func TestBuildBrregRawPayloadEn_CapitalConvertedToUSD(t *testing.T) {
	raw := json.RawMessage(`{
		"organisasjonsnummer": "831909242",
		"navn": "CERI HOLDING AS",
		"konkurs": false,
		"registrertIForetaksregisteret": true,
		"organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
		"kapital": {"belop": 30000, "valuta": "NOK", "antallAksjer": 100, "type": "Aksjekapital"},
		"aktivitet": ["Investeringsselskap"],
		"vedtektsfestetFormaal": ["Eie aksjer"],
		"frivilligMvaRegistrertBeskrivelser": [],
		"forretningsadresse": {
			"adresse": ["Storengveien 50D"],
			"poststed": "STABEKK",
			"postnummer": "1368",
			"kommune": "BÆRUM",
			"kommunenummer": "3201",
			"landkode": "NO",
			"land": "Norge"
		}
	}`)

	translated, err := activities.BuildBrregRawPayloadEn(context.Background(), raw, activities.BrregTranslationSet{
		"Aksjeselskap":        "Limited company",
		"Aksjekapital":        "Share capital",
		"Investeringsselskap": "Investment company",
		"Eie aksjer":          "Own shares",
	}, activities.FXRateSet{
		Source:   "ECB",
		RateDate: "2026-05-21",
		EURPer: map[string]float64{
			"EUR": 1.0,
			"NOK": 11.5,
			"USD": 1.09,
		},
	})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(translated, &payload))

	require.Equal(t, "831909242", payload["organization_number"])
	require.Equal(t, "CERI HOLDING AS", payload["name"])
	require.Equal(t, []any{"Investment company"}, payload["activities"])
	require.Equal(t, []any{"Own shares"}, payload["statutory_purpose"])

	capital := payload["capital"].(map[string]any)
	require.Equal(t, "USD", capital["currency"])
	require.Equal(t, "NOK", capital["original_currency"])
	require.Equal(t, float64(30000), capital["original_amount"])
	require.Equal(t, float64(2843.48), capital["amount"])
	require.Equal(t, float64(284348), capital["amount_usd_cents"])
	require.Equal(t, "Share capital", capital["type"])

	exchangeRate := capital["exchange_rate"].(map[string]any)
	require.Equal(t, "ECB", exchangeRate["source"])
	require.Equal(t, "2026-05-21", exchangeRate["rate_date"])
	require.Equal(t, "NOK", exchangeRate["source_currency"])
	require.Equal(t, "USD", exchangeRate["target_currency"])
}

func TestBuildBrregRawPayloadEn_MissingTranslationFails(t *testing.T) {
	raw := json.RawMessage(`{
		"organisasjonsnummer": "831909242",
		"navn": "CERI HOLDING AS",
		"organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"}
	}`)

	_, err := activities.BuildBrregRawPayloadEn(context.Background(), raw, activities.BrregTranslationSet{}, activities.FXRateSet{})
	require.ErrorContains(t, err, "missing translation")
}
