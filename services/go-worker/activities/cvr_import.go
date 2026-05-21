package activities

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

type cvrCompanyPayload struct {
	CVRNumber          string          `json:"cvr_number"`
	CompanyName        string          `json:"company_name,omitempty"`
	RegistrationStatus string          `json:"registration_status,omitempty"`
	CompanyType        string          `json:"company_type,omitempty"`
	Website            string          `json:"website,omitempty"`
	Email              string          `json:"email,omitempty"`
	Phone              string          `json:"phone,omitempty"`
	Roles              json.RawMessage `json:"roles,omitempty"`
	Owners             json.RawMessage `json:"owners,omitempty"`
	BeneficialOwners   json.RawMessage `json:"beneficial_owners,omitempty"`
	Financials         json.RawMessage `json:"financials,omitempty"`
}

func (a *GoActivities) ImportCVRBulk(ctx context.Context, params contracts.ImportCVRBulkParams) (int, error) {
	companies := make(map[string]cvrCompanyPayload)
	for _, file := range params.Files {
		records, err := readCVRCompanyRawInputs(file)
		if err != nil {
			return 0, fmt.Errorf("import %s %s %s: %w", file.Source, file.Dataset, file.FilePath, err)
		}
		for _, record := range records {
			companies[record.CVRNumber] = mergeCVRCompanyPayload(companies[record.CVRNumber], record)
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
	written, err := a.insertCVRCompanyRawInputs(ctx, records, params.RunID)
	if err != nil {
		return written, fmt.Errorf("upsert cvr raw inputs: %w", err)
	}
	return written, nil
}

func readCVRCompanyRawInputs(file contracts.DownloadedSourceFile) ([]cvrCompanyPayload, error) {
	raw, err := readDownloadedSourceFile(file)
	if err != nil {
		return nil, err
	}
	var rawRecords []json.RawMessage
	if err := json.Unmarshal(raw, &rawRecords); err != nil {
		scanner := bufio.NewScanner(bytes.NewReader(raw))
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			rawRecords = append(rawRecords, append(json.RawMessage(nil), line...))
		}
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("read JSONL: %w", err)
		}
	}

	companies := make([]cvrCompanyPayload, 0, len(rawRecords))
	for _, rawRecord := range rawRecords {
		var record map[string]json.RawMessage
		if err := json.Unmarshal(rawRecord, &record); err != nil {
			return nil, fmt.Errorf("parse CVR record: %w", err)
		}
		cvrNumber := rawJSONScalarString(record["cvr_number"])
		if cvrNumber == "" {
			continue
		}
		companies = append(companies, cvrCompanyPayload{
			CVRNumber:          cvrNumber,
			CompanyName:        rawJSONScalarString(record["company_name"]),
			RegistrationStatus: rawJSONScalarString(record["registration_status"]),
			CompanyType:        rawJSONScalarString(record["company_type"]),
			Website:            rawJSONScalarString(record["website"]),
			Email:              rawJSONScalarString(record["email"]),
			Phone:              rawJSONScalarString(record["phone"]),
			Roles:              cloneRawMessage(record["roles"]),
			Owners:             cloneRawMessage(record["owners"]),
			BeneficialOwners:   cloneRawMessage(record["beneficial_owners"]),
			Financials:         cloneRawMessage(record["financials"]),
		})
	}
	return companies, nil
}

func (a *GoActivities) insertCVRCompanyRawInputs(ctx context.Context, records []cvrCompanyPayload, runID string) (int, error) {
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
			return written, fmt.Errorf("batch offset %d: %w", start, err)
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

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
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
	existing.Roles = firstNonEmptyRawMessage(next.Roles, existing.Roles)
	existing.Owners = firstNonEmptyRawMessage(next.Owners, existing.Owners)
	existing.BeneficialOwners = firstNonEmptyRawMessage(next.BeneficialOwners, existing.BeneficialOwners)
	existing.Financials = firstNonEmptyRawMessage(next.Financials, existing.Financials)
	return existing
}

func firstNonEmptyRawMessage(values ...json.RawMessage) json.RawMessage {
	for _, value := range values {
		if len(value) != 0 {
			return value
		}
	}
	return nil
}
