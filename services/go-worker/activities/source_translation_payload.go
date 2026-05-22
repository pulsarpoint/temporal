package activities

import (
	"context"
	"encoding/json"
	"fmt"
)

type SourceTranslationSet map[string]string

type SourceTranslationTerm struct {
	Category string
	Text     string
}

func ExtractCVRTranslationTerms(raw json.RawMessage) ([]SourceTranslationTerm, error) {
	var src map[string]any
	if err := decodeJSONMap(raw, &src); err != nil {
		return nil, fmt.Errorf("decode cvr payload: %w", err)
	}
	terms := []SourceTranslationTerm{}
	appendScalarTerm(&terms, src, "company_type", "legal_form")
	appendScalarTerm(&terms, src, "registration_status", "status")
	appendArrayObjectTerms(&terms, src, "industries", "description", "industry")
	appendArrayObjectTerms(&terms, src, "roles", "role", "role")
	appendArrayObjectTerms(&terms, src, "owners", "ownership_type", "ownership_type")
	appendArrayObjectTerms(&terms, src, "owners", "purpose", "purpose")
	appendArrayObjectTerms(&terms, src, "beneficial_owners", "ownership_type", "ownership_type")
	appendArrayObjectTerms(&terms, src, "beneficial_owners", "purpose", "purpose")
	appendScalarTerm(&terms, src, "signing_rule", "signing_rule")
	appendArrayObjectTerms(&terms, src, "signing_rules", "rule", "signing_rule")
	appendArrayObjectTerms(&terms, src, "financials", "note", "financial_note")
	return terms, nil
}

func ExtractAriregisterTranslationTerms(raw json.RawMessage) ([]SourceTranslationTerm, error) {
	var src map[string]any
	if err := decodeJSONMap(raw, &src); err != nil {
		return nil, fmt.Errorf("decode ariregister payload: %w", err)
	}
	terms := []SourceTranslationTerm{}
	appendScalarTerm(&terms, src, "legal_form", "legal_form")
	appendScalarTerm(&terms, src, "registration_status", "status")
	appendArrayObjectTerms(&terms, src, "activities", "description", "activity")
	appendArrayObjectTerms(&terms, src, "registry_card_persons", "role", "role")
	appendArrayObjectTerms(&terms, src, "shareholders", "ownership_type", "ownership_type")
	appendArrayObjectTerms(&terms, src, "beneficial_owners", "ownership_type", "ownership_type")
	appendArrayObjectTerms(&terms, src, "financials", "indicator", "financial_indicator")
	return terms, nil
}

func BuildCVRRawPayloadEn(_ context.Context, raw json.RawMessage, translations SourceTranslationSet, _ FXRateSet) (json.RawMessage, error) {
	var src map[string]any
	if err := decodeJSONMap(raw, &src); err != nil {
		return nil, fmt.Errorf("decode cvr payload: %w", err)
	}

	out := map[string]any{
		"identity": map[string]any{
			"registration_number": stringValue(src, "cvr_number"),
			"name":                stringValue(src, "company_name"),
		},
		"addresses":        copyArray(src, "addresses"),
		"contacts":         contactsObject(src),
		"source_fragments": sourceFragments(src, "registration_status", "company_type"),
	}
	var err error
	if out["industries"], err = translateObjectArray(src, "industries", translations, map[string]string{"description": "industry"}); err != nil {
		return nil, err
	}
	if out["roles"], err = translateObjectArray(src, "roles", translations, map[string]string{"role": "role"}); err != nil {
		return nil, err
	}
	if out["owners"], err = translateObjectArray(src, "owners", translations, map[string]string{"ownership_type": "ownership_type", "purpose": "purpose"}); err != nil {
		return nil, err
	}
	if out["beneficial_owners"], err = translateObjectArray(src, "beneficial_owners", translations, map[string]string{"ownership_type": "ownership_type", "purpose": "purpose"}); err != nil {
		return nil, err
	}
	if value, ok, err := translateScalar(src, "signing_rule", "signing_rule", translations); err != nil {
		return nil, err
	} else if ok {
		out["signing_rule"] = value
	}
	if out["signing_rules"], err = translateObjectArray(src, "signing_rules", translations, map[string]string{"rule": "signing_rule"}); err != nil {
		return nil, err
	}
	if out["financials"], err = translateObjectArray(src, "financials", translations, map[string]string{"note": "financial_note"}); err != nil {
		return nil, err
	}
	if value, ok, err := translateScalar(src, "company_type", "legal_form", translations); err != nil {
		return nil, err
	} else if ok {
		out["legal_form"] = value
	}
	if value, ok, err := translateScalar(src, "registration_status", "status", translations); err != nil {
		return nil, err
	} else if ok {
		out["status"] = value
	}

	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("encode english cvr payload: %w", err)
	}
	return encoded, nil
}

func BuildAriregisterRawPayloadEn(_ context.Context, raw json.RawMessage, translations SourceTranslationSet, _ FXRateSet) (json.RawMessage, error) {
	var src map[string]any
	if err := decodeJSONMap(raw, &src); err != nil {
		return nil, fmt.Errorf("decode ariregister payload: %w", err)
	}

	out := map[string]any{
		"identity": map[string]any{
			"registration_number": stringValue(src, "registry_code"),
			"name":                stringValue(src, "legal_name"),
			"vat_number":          stringValue(src, "vat_number"),
		},
		"addresses":        copyArray(src, "addresses"),
		"contacts":         contactsObject(src),
		"source_fragments": sourceFragments(src, "registration_status", "legal_form"),
	}
	var err error
	if out["activities"], err = translateObjectArray(src, "activities", translations, map[string]string{"description": "activity"}); err != nil {
		return nil, err
	}
	if out["registry_card_persons"], err = translateObjectArray(src, "registry_card_persons", translations, map[string]string{"role": "role"}); err != nil {
		return nil, err
	}
	if out["shareholders"], err = translateObjectArray(src, "shareholders", translations, map[string]string{"ownership_type": "ownership_type"}); err != nil {
		return nil, err
	}
	if out["beneficial_owners"], err = translateObjectArray(src, "beneficial_owners", translations, map[string]string{"ownership_type": "ownership_type"}); err != nil {
		return nil, err
	}
	if out["financials"], err = translateObjectArray(src, "financials", translations, map[string]string{"indicator": "financial_indicator"}); err != nil {
		return nil, err
	}
	if value, ok, err := translateScalar(src, "legal_form", "legal_form", translations); err != nil {
		return nil, err
	} else if ok {
		out["legal_form"] = value
	}
	if value, ok, err := translateScalar(src, "registration_status", "status", translations); err != nil {
		return nil, err
	} else if ok {
		out["status"] = value
	}

	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("encode english ariregister payload: %w", err)
	}
	return encoded, nil
}

func appendScalarTerm(terms *[]SourceTranslationTerm, src map[string]any, key, category string) {
	if text := stringValue(src, key); text != "" {
		*terms = append(*terms, SourceTranslationTerm{Category: category, Text: text})
	}
}

func appendArrayObjectTerms(terms *[]SourceTranslationTerm, src map[string]any, arrayKey, field, category string) {
	values, ok := src[arrayKey].([]any)
	if !ok {
		return
	}
	for _, value := range values {
		obj, ok := value.(map[string]any)
		if !ok {
			continue
		}
		appendScalarTerm(terms, obj, field, category)
	}
}

func translateObjectArray(src map[string]any, key string, translations SourceTranslationSet, fields map[string]string) ([]any, error) {
	values, ok := src[key].([]any)
	if !ok {
		return []any{}, nil
	}
	out := make([]any, 0, len(values))
	for _, value := range values {
		obj, ok := value.(map[string]any)
		if !ok {
			out = append(out, value)
			continue
		}
		copied := copyMap(obj)
		for field, category := range fields {
			text := stringValue(obj, field)
			if text == "" {
				continue
			}
			translated, err := requireTranslation(translations, category, text)
			if err != nil {
				return nil, err
			}
			copied[field] = translated
		}
		out = append(out, copied)
	}
	return out, nil
}

func translateScalar(src map[string]any, key, category string, translations SourceTranslationSet) (string, bool, error) {
	text := stringValue(src, key)
	if text == "" {
		return "", false, nil
	}
	translated, err := requireTranslation(translations, category, text)
	if err != nil {
		return "", false, err
	}
	return translated, true, nil
}

func stringValue(src map[string]any, key string) string {
	value, ok := src[key].(string)
	if !ok {
		return ""
	}
	return value
}

func contactsObject(src map[string]any) map[string]any {
	out := map[string]any{}
	copyString(out, src, "website", "website")
	copyString(out, src, "email", "email")
	copyString(out, src, "phone", "phone")
	return out
}

func copyArray(src map[string]any, key string) []any {
	values, ok := src[key].([]any)
	if !ok {
		return []any{}
	}
	return values
}

func sourceFragments(src map[string]any, keys ...string) map[string]any {
	out := map[string]any{}
	for _, key := range keys {
		if value, ok := src[key]; ok {
			out[key] = value
		}
	}
	return out
}

func copyMap(src map[string]any) map[string]any {
	out := make(map[string]any, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}
