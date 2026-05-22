package activities

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

const defaultCompaniesHouseSICCodesURL = "https://assets.publishing.service.gov.uk/media/5a7f8639e5274a2e87db65e1/SIC07_CH_condensed_list_en.csv"

func (a *GoActivities) ImportCompaniesHouseSICCodes(ctx context.Context, file contracts.DownloadedSourceFile) (int, error) {
	fh, err := os.Open(file.FilePath)
	if err != nil {
		return 0, fmt.Errorf("open Companies House SIC file: %w", err)
	}
	defer fh.Close()

	reader := csv.NewReader(fh)
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return 0, fmt.Errorf("read Companies House SIC header: %w", err)
	}
	codeIndex, descriptionIndex, err := companiesHouseSICHeaderIndexes(header)
	if err != nil {
		return 0, err
	}

	sourceURL := file.SourceURL
	if strings.TrimSpace(sourceURL) == "" {
		sourceURL = defaultCompaniesHouseSICCodesURL
	}
	var sourceSHA any
	if strings.TrimSpace(file.SHA256) != "" {
		sourceSHA = file.SHA256
	}
	retrievedAt := time.Now().UTC()
	imported := 0

	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return imported, fmt.Errorf("read Companies House SIC row: %w", err)
		}
		if codeIndex >= len(record) || descriptionIndex >= len(record) {
			continue
		}
		code := normalizeCompaniesHouseSICCode(record[codeIndex])
		description := strings.TrimSpace(record[descriptionIndex])
		if code == "" || description == "" {
			continue
		}
		if _, err := a.pool.Exec(ctx, `
			INSERT INTO companies_house_sic_codes
				(code, description, section_code, section_description, source_url, source_sha256, retrieved_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (code) DO UPDATE SET
				description = EXCLUDED.description,
				section_code = EXCLUDED.section_code,
				section_description = EXCLUDED.section_description,
				source_url = EXCLUDED.source_url,
				source_sha256 = EXCLUDED.source_sha256,
				retrieved_at = EXCLUDED.retrieved_at,
				updated_at = now()
		`, code, description, (*string)(nil), (*string)(nil), sourceURL, sourceSHA, retrievedAt); err != nil {
			return imported, fmt.Errorf("upsert Companies House SIC code %s: %w", code, err)
		}
		imported++
	}

	return imported, nil
}

func companiesHouseSICHeaderIndexes(header []string) (int, int, error) {
	codeIndex := -1
	descriptionIndex := -1
	for i, value := range header {
		switch normalizeCompaniesHouseSICHeader(value) {
		case "siccode":
			codeIndex = i
		case "description":
			descriptionIndex = i
		}
	}
	if codeIndex < 0 || descriptionIndex < 0 {
		return -1, -1, fmt.Errorf("Companies House SIC CSV missing SIC Code or Description header")
	}
	return codeIndex, descriptionIndex, nil
}

func normalizeCompaniesHouseSICHeader(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, " ", "")
	value = strings.ReplaceAll(value, "_", "")
	return value
}

func normalizeCompaniesHouseSICCode(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return value
		}
	}
	if len(value) >= 5 {
		return value
	}
	return strings.Repeat("0", 5-len(value)) + value
}
