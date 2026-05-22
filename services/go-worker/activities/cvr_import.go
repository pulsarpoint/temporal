package activities

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

type cvrCompanyPayload struct {
	CVRNumber          string            `json:"cvr_number"`
	CompanyName        string            `json:"company_name,omitempty"`
	RegistrationStatus string            `json:"registration_status,omitempty"`
	CompanyType        string            `json:"company_type,omitempty"`
	Website            string            `json:"website,omitempty"`
	Email              string            `json:"email,omitempty"`
	Phone              string            `json:"phone,omitempty"`
	Roles              []json.RawMessage `json:"roles,omitempty"`
	Owners             []json.RawMessage `json:"owners,omitempty"`
	BeneficialOwners   []json.RawMessage `json:"beneficial_owners,omitempty"`
	Financials         []json.RawMessage `json:"financials,omitempty"`
}

func (a *GoActivities) ImportCVRBulk(ctx context.Context, params contracts.ImportCVRBulkParams) (int, error) {
	companies := make(map[string]cvrCompanyPayload)
	sourceDatasets := sourceDatasetSummary(params.Files)
	for _, file := range params.Files {
		if err := mergeCVRCompanyRawInputs(file, companies); err != nil {
			return 0, fmt.Errorf("import %s %s %s: %w", file.Source, file.Dataset, file.FilePath, err)
		}
		recordHeartbeat(ctx, map[string]any{
			"source":  file.Source,
			"dataset": file.Dataset,
			"file":    file.FilePath,
			"records": len(companies),
		})
		slog.Info("parsed CVR source file",
			"source", file.Source,
			"dataset", file.Dataset,
			"file_path", file.FilePath,
			"records", len(companies),
			"run_id", params.RunID,
		)
	}

	records := make([]cvrCompanyPayload, 0, len(companies))
	cvrNumbers := make([]string, 0, len(companies))
	for cvrNumber := range companies {
		cvrNumbers = append(cvrNumbers, cvrNumber)
	}
	sort.Strings(cvrNumbers)
	for _, cvrNumber := range cvrNumbers {
		records = append(records, companies[cvrNumber])
	}
	written, err := a.insertCVRCompanyRawInputs(ctx, records, params.RunID, sourceDatasets)
	if err != nil {
		return written, fmt.Errorf("upsert cvr raw inputs from %s: %w", sourceDatasets, err)
	}
	return written, nil
}

func mergeCVRCompanyRawInputs(file contracts.DownloadedSourceFile, companies map[string]cvrCompanyPayload) error {
	reader, err := openDownloadedSourceFile(file)
	if err != nil {
		return err
	}
	defer reader.Close()

	handleRecord := func(rawRecord json.RawMessage) error {
		payload, ok, err := cvrCompanyPayloadFromRaw(rawRecord)
		if err != nil || !ok {
			return err
		}
		companies[payload.CVRNumber] = mergeCVRCompanyPayload(companies[payload.CVRNumber], payload)
		return nil
	}

	if isJSONLSource(file) {
		if err := forEachJSONLine(reader, handleRecord); err != nil {
			return err
		}
	} else if err := streamJSONRecords(reader, nil, handleRecord); err != nil {
		return err
	}
	return nil
}

func cvrCompanyPayloadFromRaw(rawRecord json.RawMessage) (cvrCompanyPayload, bool, error) {
	var record map[string]json.RawMessage
	if err := json.Unmarshal(rawRecord, &record); err != nil {
		return cvrCompanyPayload{}, false, fmt.Errorf("parse CVR record: %w", err)
	}
	cvrNumber := rawJSONScalarString(record["cvr_number"])
	if cvrNumber != "" {
		return cvrCompanyPayload{
			CVRNumber:          cvrNumber,
			CompanyName:        rawJSONScalarString(record["company_name"]),
			RegistrationStatus: rawJSONScalarString(record["registration_status"]),
			CompanyType:        rawJSONScalarString(record["company_type"]),
			Website:            rawJSONScalarString(record["website"]),
			Email:              rawJSONScalarString(record["email"]),
			Phone:              rawJSONScalarString(record["phone"]),
			Roles:              rawJSONFragments(record["roles"]),
			Owners:             rawJSONFragments(record["owners"]),
			BeneficialOwners:   rawJSONFragments(record["beneficial_owners"]),
			Financials:         rawJSONFragments(record["financials"]),
		}, true, nil
	}
	return cvrCompanyPayloadFromDatafordelerRaw(rawRecord)
}

func cvrCompanyPayloadFromDatafordelerRaw(rawRecord json.RawMessage) (cvrCompanyPayload, bool, error) {
	var record map[string]any
	decoder := json.NewDecoder(bytes.NewReader(rawRecord))
	decoder.UseNumber()
	if err := decoder.Decode(&record); err != nil {
		return cvrCompanyPayload{}, false, fmt.Errorf("parse Datafordeler CVR record: %w", err)
	}
	var merged cvrCompanyPayload
	for _, object := range cvrDatafordelerObjects(record) {
		next := cvrCompanyPayloadFromDatafordelerObject(object.kind, object.values)
		if next.CVRNumber == "" {
			continue
		}
		merged = mergeCVRCompanyPayload(merged, next)
	}
	if merged.CVRNumber == "" {
		return cvrCompanyPayload{}, false, nil
	}
	return merged, true, nil
}

type cvrDatafordelerObject struct {
	kind   string
	values map[string]any
}

func cvrDatafordelerObjects(record map[string]any) []cvrDatafordelerObject {
	if record == nil {
		return nil
	}
	objects := []cvrDatafordelerObject{{kind: "record", values: record}}
	for _, wrapper := range []string{"_source", "source", "data"} {
		if nested := anyMap(record, wrapper); nested != nil {
			objects = append(objects, cvrDatafordelerObjects(nested)...)
		}
	}
	for _, wrapper := range []string{
		"Vrvirksomhed",
		"Virksomhed",
		"Navn",
		"Emailadresse",
		"E-mailadresse",
		"ElektroniskPost",
		"Telefonnummer",
		"Hjemmeside",
	} {
		kind := normalizeCVRKey(wrapper)
		if nested := anyMap(record, wrapper); nested != nil {
			objects = append(objects, cvrDatafordelerObject{kind: kind, values: nested})
		}
		for _, nested := range anyMapArray(record, wrapper) {
			objects = append(objects, cvrDatafordelerObject{kind: kind, values: nested})
		}
	}
	return objects
}

func cvrCompanyPayloadFromDatafordelerObject(kind string, values map[string]any) cvrCompanyPayload {
	cvrNumber := firstNonEmptyString(
		anyString(values, "cvr_number"),
		anyString(values, "cvrNummer"),
		anyString(values, "CVRNummer"),
		anyString(values, "cvrnummer"),
	)
	if cvrNumber == "" {
		return cvrCompanyPayload{}
	}

	payload := cvrCompanyPayload{CVRNumber: cvrNumber}
	payload.CompanyName = firstNonEmptyString(
		anyString(values, "company_name"),
		anyString(values, "navn"),
		anyString(values, "name"),
		anyStringPath(values, "virksomhedMetadata", "nyesteNavn", "navn"),
	)
	payload.RegistrationStatus = firstNonEmptyString(
		anyString(values, "registration_status"),
		anyString(values, "virksomhedsstatus"),
		anyString(values, "virksomhedStatus"),
		anyString(values, "status"),
		anyStringPath(values, "virksomhedMetadata", "nyesteStatus"),
	)
	payload.CompanyType = firstNonEmptyString(
		anyString(values, "company_type"),
		anyString(values, "virksomhedsformKortBeskrivelse"),
		anyStringPath(values, "virksomhedsform", "kortBeskrivelse"),
		anyStringPath(values, "virksomhedsform", "langBeskrivelse"),
		anyStringPath(values, "virksomhedMetadata", "nyesteVirksomhedsform", "kortBeskrivelse"),
		anyStringPath(values, "virksomhedMetadata", "nyesteVirksomhedsform", "langBeskrivelse"),
	)
	payload.Website = firstNonEmptyString(anyString(values, "website"), contactValueForKind(kind, values, "hjemmeside"), contactValue(values, "hjemmeside"))
	payload.Email = firstNonEmptyString(anyString(values, "email"), contactValueForKind(kind, values, "emailadresse"), contactValueForKind(kind, values, "elektroniskpost"), contactValue(values, "elektroniskPost"), contactValue(values, "emailadresse"))
	payload.Phone = firstNonEmptyString(anyString(values, "phone"), contactValueForKind(kind, values, "telefonnummer"), contactValue(values, "telefonNummer"), contactValue(values, "telefonnummer"))
	return payload
}

func contactValueForKind(kind string, values map[string]any, expectedKind string) string {
	if kind != expectedKind {
		return ""
	}
	return firstNonEmptyString(anyString(values, "kontaktoplysning"), anyString(values, "value"))
}

func contactValue(values map[string]any, key string) string {
	value := anyValue(values, key)
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]any:
		return firstNonEmptyString(anyString(typed, "kontaktoplysning"), anyString(typed, "value"))
	case []any:
		for _, item := range typed {
			if nested, ok := item.(map[string]any); ok {
				if value := firstNonEmptyString(anyString(nested, "kontaktoplysning"), anyString(nested, "value")); value != "" {
					return value
				}
			}
			if value := anyScalarString(item); value != "" {
				return value
			}
		}
	}
	return ""
}

func anyStringPath(values map[string]any, path ...string) string {
	var current any = values
	for _, key := range path {
		currentMap, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = anyValue(currentMap, key)
	}
	return anyScalarString(current)
}

func anyString(values map[string]any, key string) string {
	return anyScalarString(anyValue(values, key))
}

func anyMap(values map[string]any, key string) map[string]any {
	nested, _ := anyValue(values, key).(map[string]any)
	return nested
}

func anyMapArray(values map[string]any, key string) []map[string]any {
	items, _ := anyValue(values, key).([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if nested, ok := item.(map[string]any); ok {
			out = append(out, nested)
		}
	}
	return out
}

func anyValue(values map[string]any, key string) any {
	if values == nil {
		return nil
	}
	if value, ok := values[key]; ok {
		return value
	}
	normalizedKey := normalizeCVRKey(key)
	for existingKey, value := range values {
		if normalizeCVRKey(existingKey) == normalizedKey {
			return value
		}
	}
	return nil
}

func anyScalarString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case int:
		return fmt.Sprintf("%d", typed)
	default:
		return ""
	}
}

func normalizeCVRKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	replacer := strings.NewReplacer("_", "", "-", "", " ", "", "æ", "ae", "ø", "oe", "å", "aa")
	return replacer.Replace(key)
}

func (a *GoActivities) insertCVRCompanyRawInputs(ctx context.Context, records []cvrCompanyPayload, runID, sourceDatasets string) (int, error) {
	written := 0
	for start := 0; start < len(records); start += sourceImportBatchSize {
		end := min(start+sourceImportBatchSize, len(records))
		batch := &pgx.Batch{}
		for _, company := range records[start:end] {
			rawPayload, err := json.Marshal(company)
			if err != nil {
				return written, fmt.Errorf("marshal CVR payload %s: %w", company.CVRNumber, err)
			}
			batch.Queue(`
				INSERT INTO cvr_company_raw_inputs (
					source_native_id, cvr_number, company_name, registration_status,
					company_type, website, email, phone, country_iso2,
					raw_payload, payload_hash, run_id
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				ON CONFLICT (cvr_number, payload_hash) DO UPDATE
					SET last_seen_at = now(), run_id = EXCLUDED.run_id
			`, company.CVRNumber, company.CVRNumber, nullableString(company.CompanyName),
				nullableString(company.RegistrationStatus), nullableString(company.CompanyType),
				nullableString(company.Website), nullableString(company.Email), nullableString(company.Phone),
				"DK", rawPayload, hashBytes(rawPayload), runID)
		}
		if err := execBatch(ctx, a.pool, batch); err != nil {
			return written, fmt.Errorf("%s batch offset %d: %w", sourceDatasets, start, err)
		}
		written += end - start
		recordHeartbeat(ctx, written)
	}
	return written, nil
}

func rawJSONScalarString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return value
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		return number.String()
	}
	return ""
}

func forEachJSONLine(reader io.Reader, handle func(json.RawMessage) error) error {
	buffered := bufio.NewReader(reader)
	for {
		line, err := buffered.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) != 0 {
			rawRecord := append(json.RawMessage(nil), bytes.TrimSpace(line)...)
			if handleErr := handle(rawRecord); handleErr != nil {
				return handleErr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read JSONL: %w", err)
		}
	}
}

func isJSONLSource(file contracts.DownloadedSourceFile) bool {
	format := strings.ToLower(file.Format)
	path := strings.ToLower(file.FilePath)
	return format == "jsonl" || strings.HasSuffix(format, ".jsonl") || strings.HasSuffix(path, ".jsonl")
}

func rawJSONFragments(raw json.RawMessage) []json.RawMessage {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	if raw[0] != '[' {
		return []json.RawMessage{cloneRawMessage(raw)}
	}
	var fragments []json.RawMessage
	if err := json.Unmarshal(raw, &fragments); err != nil {
		return []json.RawMessage{cloneRawMessage(raw)}
	}
	nonEmpty := make([]json.RawMessage, 0, len(fragments))
	for _, fragment := range fragments {
		if cloned := cloneRawMessage(fragment); len(cloned) != 0 {
			nonEmpty = append(nonEmpty, cloned)
		}
	}
	return nonEmpty
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func mergeCVRCompanyPayload(existing cvrCompanyPayload, next cvrCompanyPayload) cvrCompanyPayload {
	if existing.CVRNumber == "" {
		return next
	}
	existing.CompanyName = firstNonEmptyString(next.CompanyName, existing.CompanyName)
	existing.RegistrationStatus = firstNonEmptyString(next.RegistrationStatus, existing.RegistrationStatus)
	existing.CompanyType = firstNonEmptyString(next.CompanyType, existing.CompanyType)
	existing.Website = firstNonEmptyString(next.Website, existing.Website)
	existing.Email = firstNonEmptyString(next.Email, existing.Email)
	existing.Phone = firstNonEmptyString(next.Phone, existing.Phone)
	existing.Roles = appendRawFragments(existing.Roles, next.Roles)
	existing.Owners = appendRawFragments(existing.Owners, next.Owners)
	existing.BeneficialOwners = appendRawFragments(existing.BeneficialOwners, next.BeneficialOwners)
	existing.Financials = appendRawFragments(existing.Financials, next.Financials)
	return existing
}

func appendRawFragments(existing, next []json.RawMessage) []json.RawMessage {
	for _, fragment := range next {
		if cloned := cloneRawMessage(fragment); len(cloned) != 0 {
			existing = append(existing, cloned)
		}
	}
	return existing
}
