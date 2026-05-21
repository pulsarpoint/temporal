package activities

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/activity"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

const sourceImportBatchSize = 1000

type gleifCompanyRawInput struct {
	lei                     string
	legalName               string
	registrationStatus      string
	headquartersCountryCode string
	rawPayload              []byte
	payloadHash             string
}

func (a *GoActivities) ImportGLEIFGoldenCopy(ctx context.Context, params contracts.ImportGLEIFGoldenCopyParams) (int, error) {
	written := 0
	for _, file := range params.Files {
		records, err := readGLEIFCompanyRawInputs(file)
		if err != nil {
			return written, fmt.Errorf("import %s %s %s: %w", file.Source, file.Dataset, file.FilePath, err)
		}

		fileWritten, err := a.insertGLEIFCompanyRawInputs(ctx, records, params.RunID)
		if err != nil {
			return written, fmt.Errorf("upsert %s %s %s: %w", file.Source, file.Dataset, file.FilePath, err)
		}
		written += fileWritten
		recordHeartbeat(ctx, map[string]any{
			"source":  file.Source,
			"dataset": file.Dataset,
			"file":    file.FilePath,
			"written": written,
		})
		slog.Info("imported GLEIF source file",
			"source", file.Source,
			"dataset", file.Dataset,
			"file_path", file.FilePath,
			"records", fileWritten,
			"run_id", params.RunID,
		)
	}
	return written, nil
}

func readGLEIFCompanyRawInputs(file contracts.DownloadedSourceFile) ([]gleifCompanyRawInput, error) {
	raw, err := readDownloadedSourceFile(file)
	if err != nil {
		return nil, err
	}

	records, err := jsonRecordsFromPayload(raw, "data", "leiRecords")
	if err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}

	inputs := make([]gleifCompanyRawInput, 0, len(records))
	for _, rawRecord := range records {
		var item map[string]any
		if err := json.Unmarshal(rawRecord, &item); err != nil {
			return nil, fmt.Errorf("parse GLEIF record: %w", err)
		}
		lei := firstNonEmptyString(
			mapString(item, "lei"),
			mapString(item, "id"),
			mapString(nestedMap(item, "attributes"), "lei"),
		)
		if lei == "" {
			continue
		}
		entity := nestedMap(nestedMap(item, "attributes"), "entity")
		legalName := firstNonEmptyString(
			mapString(nestedMap(entity, "legalName"), "name"),
			mapString(item, "legalName"),
		)
		status := firstNonEmptyString(
			mapString(entity, "status"),
			mapString(item, "entityStatus"),
		)
		headquartersCountry := firstNonEmptyString(
			mapString(nestedMap(entity, "headquartersAddress"), "country"),
			mapString(item, "headquartersCountry"),
		)
		inputs = append(inputs, gleifCompanyRawInput{
			lei:                     lei,
			legalName:               legalName,
			registrationStatus:      status,
			headquartersCountryCode: headquartersCountry,
			rawPayload:              []byte(rawRecord),
			payloadHash:             hashBytes(rawRecord),
		})
	}
	return inputs, nil
}

func (a *GoActivities) insertGLEIFCompanyRawInputs(ctx context.Context, records []gleifCompanyRawInput, runID string) (int, error) {
	written := 0
	for start := 0; start < len(records); start += sourceImportBatchSize {
		end := min(start+sourceImportBatchSize, len(records))
		batch := &pgx.Batch{}
		for _, record := range records[start:end] {
			batch.Queue(`
				INSERT INTO gleif_company_raw_inputs (
					source_native_id, lei, legal_name, registration_status,
					headquarters_country_code, raw_payload, payload_hash, run_id
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT (lei, payload_hash) DO UPDATE
					SET last_seen_at = now(), run_id = EXCLUDED.run_id
			`, record.lei, record.lei, nullableString(record.legalName), nullableString(record.registrationStatus),
				nullableString(record.headquartersCountryCode), record.rawPayload, record.payloadHash, runID)
		}
		if err := execBatch(ctx, a.pool, batch); err != nil {
			return written, fmt.Errorf("batch offset %d: %w", start, err)
		}
		written += end - start
		recordHeartbeat(ctx, written)
	}
	return written, nil
}

func readDownloadedSourceFile(file contracts.DownloadedSourceFile) ([]byte, error) {
	raw, err := os.ReadFile(file.FilePath)
	if err != nil {
		return nil, fmt.Errorf("open source file: %w", err)
	}

	format := strings.ToLower(file.Format)
	ext := strings.ToLower(filepath.Ext(file.FilePath))
	if format == "gzip" || format == "gz" || strings.HasSuffix(format, ".gz") || ext == ".gz" {
		reader, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("open gzip: %w", err)
		}
		defer reader.Close()
		content, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("read gzip: %w", err)
		}
		return content, nil
	}
	if format == "zip" || strings.HasSuffix(format, ".zip") || ext == ".zip" {
		reader, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			return nil, fmt.Errorf("open zip: %w", err)
		}
		for _, zippedFile := range reader.File {
			if zippedFile.FileInfo().IsDir() {
				continue
			}
			rc, err := zippedFile.Open()
			if err != nil {
				return nil, fmt.Errorf("open zipped file %s: %w", zippedFile.Name, err)
			}
			content, readErr := io.ReadAll(rc)
			closeErr := rc.Close()
			if readErr != nil {
				return nil, fmt.Errorf("read zipped file %s: %w", zippedFile.Name, readErr)
			}
			if closeErr != nil {
				return nil, fmt.Errorf("close zipped file %s: %w", zippedFile.Name, closeErr)
			}
			return content, nil
		}
		return nil, fmt.Errorf("zip contains no files")
	}
	return raw, nil
}

func jsonRecordsFromPayload(raw []byte, wrapperFields ...string) ([]json.RawMessage, error) {
	var wrapped map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wrapped); err == nil {
		for _, field := range wrapperFields {
			if wrapped[field] == nil {
				continue
			}
			var records []json.RawMessage
			if err := json.Unmarshal(wrapped[field], &records); err == nil {
				return records, nil
			}
		}
	}

	var records []json.RawMessage
	if err := json.Unmarshal(raw, &records); err != nil {
		var single map[string]any
		if objectErr := json.Unmarshal(raw, &single); objectErr == nil {
			return []json.RawMessage{append(json.RawMessage(nil), raw...)}, nil
		}
		return nil, err
	}
	return records, nil
}

func execBatch(ctx context.Context, db DB, batch *pgx.Batch) error {
	if batch.Len() == 0 {
		return nil
	}
	results := db.SendBatch(ctx, batch)
	defer results.Close()
	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func recordHeartbeat(ctx context.Context, details ...any) {
	if activity.IsActivity(ctx) {
		activity.RecordHeartbeat(ctx, details...)
	}
}

func hashBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func nestedMap(values map[string]any, key string) map[string]any {
	if values == nil {
		return nil
	}
	nested, _ := values[key].(map[string]any)
	return nested
}

func mapString(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	value, _ := values[key].(string)
	return value
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
