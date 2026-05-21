package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

type ariregisterCompanyPayload struct {
	RegistryCode       string            `json:"registry_code"`
	LegalName          string            `json:"legal_name,omitempty"`
	RegistrationStatus string            `json:"registration_status,omitempty"`
	LegalForm          string            `json:"legal_form,omitempty"`
	VATNumber          string            `json:"vat_number,omitempty"`
	Website            string            `json:"website,omitempty"`
	Email              string            `json:"email,omitempty"`
	Phone              string            `json:"phone,omitempty"`
	Financials         []json.RawMessage `json:"financials,omitempty"`
}

func (a *GoActivities) ImportAriregisterBulk(ctx context.Context, params contracts.ImportAriregisterBulkParams) (int, error) {
	companies := make(map[string]*ariregisterCompanyPayload)
	sourceDatasets := sourceDatasetSummary(params.Files)
	for _, file := range params.Files {
		if err := mergeAriregisterFile(file, companies); err != nil {
			return 0, fmt.Errorf("import %s %s %s: %w", file.Source, file.Dataset, file.FilePath, err)
		}
		recordHeartbeat(ctx, map[string]any{
			"source":  file.Source,
			"dataset": file.Dataset,
			"file":    file.FilePath,
			"records": len(companies),
		})
		slog.Info("parsed Ariregister source file",
			"source", file.Source,
			"dataset", file.Dataset,
			"file_path", file.FilePath,
			"records", len(companies),
			"run_id", params.RunID,
		)
	}

	written, err := a.insertAriregisterCompanyRawInputs(ctx, companies, params.RunID, sourceDatasets)
	if err != nil {
		return written, fmt.Errorf("upsert ariregister raw inputs from %s: %w", sourceDatasets, err)
	}
	return written, nil
}

func mergeAriregisterFile(file contracts.DownloadedSourceFile, companies map[string]*ariregisterCompanyPayload) error {
	return forEachJSONRecord(file, []string{"data", "records"}, func(rawRecord json.RawMessage) error {
		var record map[string]any
		if err := json.Unmarshal(rawRecord, &record); err != nil {
			return fmt.Errorf("parse Ariregister record: %w", err)
		}
		registryCode := mapString(record, "registry_code")
		if registryCode == "" {
			return nil
		}
		company := companies[registryCode]
		if company == nil {
			company = &ariregisterCompanyPayload{RegistryCode: registryCode}
			companies[registryCode] = company
		}
		if hasAnyKey(record, "year", "revenue", "profit", "employee_count") {
			company.Financials = append(company.Financials, rawRecord)
			return nil
		}
		company.LegalName = firstNonEmptyString(mapString(record, "legal_name"), company.LegalName)
		company.RegistrationStatus = firstNonEmptyString(mapString(record, "registration_status"), company.RegistrationStatus)
		company.LegalForm = firstNonEmptyString(mapString(record, "legal_form"), company.LegalForm)
		company.VATNumber = firstNonEmptyString(mapString(record, "vat_number"), company.VATNumber)
		company.Website = firstNonEmptyString(mapString(record, "website"), company.Website)
		company.Email = firstNonEmptyString(mapString(record, "email"), company.Email)
		company.Phone = firstNonEmptyString(mapString(record, "phone"), company.Phone)
		return nil
	})
}

func (a *GoActivities) insertAriregisterCompanyRawInputs(ctx context.Context, companies map[string]*ariregisterCompanyPayload, runID, sourceDatasets string) (int, error) {
	records := make([]*ariregisterCompanyPayload, 0, len(companies))
	registryCodes := make([]string, 0, len(companies))
	for registryCode := range companies {
		registryCodes = append(registryCodes, registryCode)
	}
	sort.Strings(registryCodes)
	for _, registryCode := range registryCodes {
		company := companies[registryCode]
		if company.RegistryCode != "" {
			records = append(records, company)
		}
	}

	written := 0
	for start := 0; start < len(records); start += sourceImportBatchSize {
		end := min(start+sourceImportBatchSize, len(records))
		batch := &pgx.Batch{}
		for _, company := range records[start:end] {
			rawPayload, err := json.Marshal(company)
			if err != nil {
				return written, fmt.Errorf("marshal Ariregister payload %s: %w", company.RegistryCode, err)
			}
			batch.Queue(`
				INSERT INTO ariregister_company_raw_inputs (
					source_native_id, registry_code, legal_name, registration_status,
					legal_form, vat_number, website, email, phone, country_iso2,
					raw_payload, payload_hash, run_id
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (registry_code, payload_hash) DO UPDATE
					SET last_seen_at = now(), run_id = EXCLUDED.run_id
			`, company.RegistryCode, company.RegistryCode, nullableString(company.LegalName),
				nullableString(company.RegistrationStatus), nullableString(company.LegalForm),
				nullableString(company.VATNumber), nullableString(company.Website), nullableString(company.Email),
				nullableString(company.Phone), "EE", rawPayload, hashBytes(rawPayload), runID)
		}
		if err := execBatch(ctx, a.pool, batch); err != nil {
			return written, fmt.Errorf("%s batch offset %d: %w", sourceDatasets, start, err)
		}
		written += end - start
		recordHeartbeat(ctx, written)
	}
	return written, nil
}

func hasAnyKey(values map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, ok := values[key]; ok {
			return true
		}
	}
	return false
}

func sourceDatasetSummary(files []contracts.DownloadedSourceFile) string {
	sourceDatasets := make([]string, 0, len(files))
	seen := make(map[string]struct{}, len(files))
	for _, file := range files {
		source := firstNonEmptyString(file.Source, "unknown-source")
		dataset := firstNonEmptyString(file.Dataset, "unknown-dataset")
		sourceDataset := source + ":" + dataset
		if _, ok := seen[sourceDataset]; ok {
			continue
		}
		seen[sourceDataset] = struct{}{}
		sourceDatasets = append(sourceDatasets, sourceDataset)
	}
	sort.Strings(sourceDatasets)
	return strings.Join(sourceDatasets, ", ")
}
