package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

type BrregTranslationSet = SourceTranslationSet

type FXRateSet struct {
	Source   string
	RateDate string
	EURPer   map[string]float64
}

type BrregTranslationTerm = SourceTranslationTerm

func ExtractBrregTranslationTerms(raw json.RawMessage) ([]BrregTranslationTerm, error) {
	var src map[string]any
	if err := decodeJSONMap(raw, &src); err != nil {
		return nil, fmt.Errorf("decode brreg payload: %w", err)
	}
	terms := []BrregTranslationTerm{}
	appendNestedTerm := func(key, category string) {
		obj, ok := src[key].(map[string]any)
		if !ok || obj == nil {
			return
		}
		if text, ok := obj["beskrivelse"].(string); ok && text != "" {
			terms = append(terms, BrregTranslationTerm{Category: category, Text: text})
		}
	}
	appendNestedTerm("organisasjonsform", "org_form")
	appendNestedTerm("institusjonellSektorkode", "sector_code")
	appendNestedTerm("naeringskode1", "industry_code")
	appendNestedTerm("naeringskode2", "industry_code")
	appendNestedTerm("naeringskode3", "industry_code")
	if capital, ok := src["kapital"].(map[string]any); ok {
		if text, ok := capital["type"].(string); ok && text != "" {
			terms = append(terms, BrregTranslationTerm{Category: "capital_type", Text: text})
		}
	}
	appendArrayTerms := func(key, category string) {
		values, ok := src[key].([]any)
		if !ok {
			return
		}
		for _, value := range values {
			if text, ok := value.(string); ok && text != "" {
				terms = append(terms, BrregTranslationTerm{Category: category, Text: text})
			}
		}
	}
	appendArrayTerms("aktivitet", "activity")
	appendArrayTerms("vedtektsfestetFormaal", "statutory_purpose")
	appendArrayTerms("frivilligMvaRegistrertBeskrivelser", "vat_description")
	return terms, nil
}

func BrregPayloadNeedsFX(raw json.RawMessage) bool {
	var src map[string]any
	if err := decodeJSONMap(raw, &src); err != nil {
		return false
	}
	capital, ok := src["kapital"].(map[string]any)
	if !ok || capital == nil {
		return false
	}
	_, hasAmount := capital["belop"]
	_, hasCurrency := capital["valuta"]
	return hasAmount || hasCurrency
}

func BuildBrregRawPayloadEn(_ context.Context, raw json.RawMessage, translations BrregTranslationSet, fx FXRateSet) (json.RawMessage, error) {
	var src map[string]any
	if err := decodeJSONMap(raw, &src); err != nil {
		return nil, fmt.Errorf("decode brreg payload: %w", err)
	}

	out := map[string]any{}
	copyString(out, src, "organisasjonsnummer", "organization_number")
	copyString(out, src, "navn", "name")
	copyString(out, src, "hjemmeside", "website")
	copyString(out, src, "stiftelsesdato", "founded_date")
	copyString(out, src, "registreringsdatoEnhetsregisteret", "registered_date")
	copyBool(out, src, "konkurs", "is_bankrupt")
	copyBool(out, src, "underAvvikling", "is_under_liquidation")
	copyBool(out, src, "underTvangsavviklingEllerTvangsopplosning", "is_forced_dissolution")
	copyBool(out, src, "erIKonsern", "is_in_group")
	copyBool(out, src, "registrertIMvaregisteret", "in_vat_register")
	copyBool(out, src, "registrertIForetaksregisteret", "in_business_register")
	copyBool(out, src, "harRegistrertAntallAnsatte", "has_registered_employees")
	copyAny(out, src, "sisteInnsendteAarsregnskap", "last_annual_report_year")

	if value, ok, err := translatedCodeDescription(src, "organisasjonsform", translations); err != nil {
		return nil, err
	} else if ok {
		out["organization_form"] = value
	}
	if value, ok, err := translatedCodeDescription(src, "institusjonellSektorkode", translations); err != nil {
		return nil, err
	} else if ok {
		out["sector_code"] = value
	}
	for _, pair := range []struct {
		from string
		to   string
	}{
		{"naeringskode1", "industry_code_1"},
		{"naeringskode2", "industry_code_2"},
		{"naeringskode3", "industry_code_3"},
	} {
		if value, ok, err := translatedCodeDescription(src, pair.from, translations); err != nil {
			return nil, err
		} else if ok {
			out[pair.to] = value
		} else {
			out[pair.to] = nil
		}
	}

	if activities, err := translatedArray(src, "aktivitet", translations); err != nil {
		return nil, err
	} else {
		out["activities"] = activities
	}
	if purpose, err := translatedArray(src, "vedtektsfestetFormaal", translations); err != nil {
		return nil, err
	} else {
		out["statutory_purpose"] = purpose
	}
	if vat, err := translatedArray(src, "frivilligMvaRegistrertBeskrivelser", translations); err != nil {
		return nil, err
	} else {
		out["vat_descriptions"] = vat
	}

	if capital, ok, err := buildCapital(src, translations, fx); err != nil {
		return nil, err
	} else if ok {
		out["capital"] = capital
	}

	out["business_address"] = addressObject(src, "forretningsadresse")
	out["postal_address"] = addressObject(src, "postadresse")

	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("encode english brreg payload: %w", err)
	}
	return encoded, nil
}

func decodeJSONMap(raw json.RawMessage, dst *map[string]any) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	return dec.Decode(dst)
}

func copyString(out, src map[string]any, from, to string) {
	if value, ok := src[from].(string); ok {
		out[to] = value
	}
}

func copyBool(out, src map[string]any, from, to string) {
	if value, ok := src[from].(bool); ok {
		out[to] = value
	}
}

func copyAny(out, src map[string]any, from, to string) {
	if value, ok := src[from]; ok {
		out[to] = value
	}
}

func translatedCodeDescription(src map[string]any, key string, translations BrregTranslationSet) (map[string]any, bool, error) {
	obj, ok := src[key].(map[string]any)
	if !ok || obj == nil {
		return nil, false, nil
	}
	out := map[string]any{}
	if code, ok := obj["kode"].(string); ok {
		out["code"] = code
	}
	if description, ok := obj["beskrivelse"].(string); ok && description != "" {
		translated, err := requireTranslation(translations, description)
		if err != nil {
			return nil, false, err
		}
		out["description"] = translated
	}
	return out, true, nil
}

func translatedArray(src map[string]any, key string, translations BrregTranslationSet) ([]string, error) {
	values, ok := src[key].([]any)
	if !ok {
		return []string{}, nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		text, ok := value.(string)
		if !ok || text == "" {
			continue
		}
		translated, err := requireTranslation(translations, text)
		if err != nil {
			return nil, err
		}
		out = append(out, translated)
	}
	return out, nil
}

func buildCapital(src map[string]any, translations BrregTranslationSet, fx FXRateSet) (map[string]any, bool, error) {
	obj, ok := src["kapital"].(map[string]any)
	if !ok || obj == nil {
		return nil, false, nil
	}

	out := map[string]any{}
	amount, hasAmount, err := numberAsFloat(obj["belop"])
	if err != nil {
		return nil, false, fmt.Errorf("capital amount: %w", err)
	}
	currency, hasCurrency := obj["valuta"].(string)
	if hasAmount || hasCurrency {
		if !hasAmount || !hasCurrency || currency == "" {
			return nil, false, fmt.Errorf("incomplete capital currency data")
		}
		usdCents, err := fx.ToUSDCents(amount, currency)
		if err != nil {
			return nil, false, err
		}
		out["amount"] = float64(usdCents) / 100
		out["currency"] = "USD"
		out["amount_usd_cents"] = usdCents
		out["original_amount"] = amount
		out["original_currency"] = currency
		out["exchange_rate"] = map[string]any{
			"source":              fx.Source,
			"rate_date":           fx.RateDate,
			"source_currency":     currency,
			"target_currency":     "USD",
			"source_rate_per_eur": fx.EURPer[currency],
			"target_rate_per_eur": fx.EURPer["USD"],
		}
	}
	if shares, ok, err := numberAsFloat(obj["antallAksjer"]); err != nil {
		return nil, false, fmt.Errorf("capital shares: %w", err)
	} else if ok {
		out["shares"] = shares
	}
	if capitalType, ok := obj["type"].(string); ok && capitalType != "" {
		translated, err := requireTranslation(translations, capitalType)
		if err != nil {
			return nil, false, err
		}
		out["type"] = translated
	}
	return out, true, nil
}

func (fx FXRateSet) ToUSDCents(amount float64, currency string) (int64, error) {
	if amount < 0 {
		return 0, fmt.Errorf("negative amount not supported")
	}
	if currency == "USD" {
		return int64(math.Round(amount * 100)), nil
	}
	usdRate, ok := fx.EURPer["USD"]
	if !ok || usdRate <= 0 {
		return 0, fmt.Errorf("USD rate not found")
	}
	srcRate, ok := fx.EURPer[currency]
	if !ok || srcRate <= 0 {
		return 0, fmt.Errorf("currency %q not found", currency)
	}
	return int64(math.Round((amount / srcRate) * usdRate * 100)), nil
}

func numberAsFloat(value any) (float64, bool, error) {
	switch typed := value.(type) {
	case nil:
		return 0, false, nil
	case json.Number:
		n, err := typed.Float64()
		if err != nil {
			return 0, false, err
		}
		return n, true, nil
	case float64:
		return typed, true, nil
	case int:
		return float64(typed), true, nil
	case int64:
		return float64(typed), true, nil
	default:
		return 0, false, fmt.Errorf("unexpected number type %T", value)
	}
}

func addressObject(src map[string]any, key string) map[string]any {
	obj, ok := src[key].(map[string]any)
	if !ok || obj == nil {
		return nil
	}
	out := map[string]any{}
	copyAny(out, obj, "adresse", "street")
	copyString(out, obj, "poststed", "city")
	copyString(out, obj, "postnummer", "postal_code")
	copyString(out, obj, "kommune", "municipality")
	copyString(out, obj, "kommunenummer", "municipality_number")
	copyString(out, obj, "landkode", "country_code")
	copyString(out, obj, "land", "country")
	return out
}

func requireTranslation(translations BrregTranslationSet, text string) (string, error) {
	translated := translations[text]
	if translated == "" {
		return "", fmt.Errorf("missing translation for %q", text)
	}
	return translated, nil
}
