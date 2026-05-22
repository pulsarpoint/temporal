package activities

import (
	"archive/zip"
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
		fileWritten := 0
		records := make([]gleifCompanyRawInput, 0, sourceImportBatchSize)
		flush := func() error {
			if len(records) == 0 {
				return nil
			}
			batchWritten, err := a.insertGLEIFCompanyRawInputs(ctx, records, params.RunID)
			if err != nil {
				return err
			}
			fileWritten += batchWritten
			written += batchWritten
			records = records[:0]
			recordHeartbeat(ctx, map[string]any{
				"source":  file.Source,
				"dataset": file.Dataset,
				"file":    file.FilePath,
				"written": written,
			})
			return nil
		}

		var flushErr error
		err := streamGLEIFCompanyRawInputs(file, func(record gleifCompanyRawInput) error {
			records = append(records, record)
			if len(records) < sourceImportBatchSize {
				return nil
			}
			flushErr = flush()
			return flushErr
		})
		if err != nil {
			if flushErr != nil {
				return written, fmt.Errorf("upsert %s %s %s: %w", file.Source, file.Dataset, file.FilePath, err)
			}
			return written, fmt.Errorf("import %s %s %s: %w", file.Source, file.Dataset, file.FilePath, err)
		}
		if err := flush(); err != nil {
			return written, fmt.Errorf("upsert %s %s %s: %w", file.Source, file.Dataset, file.FilePath, err)
		}
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

func streamGLEIFCompanyRawInputs(file contracts.DownloadedSourceFile, handle func(gleifCompanyRawInput) error) error {
	return forEachJSONRecord(file, []string{"data", "leiRecords", "records"}, func(rawRecord json.RawMessage) error {
		input, ok, err := gleifCompanyRawInputFromRaw(rawRecord)
		if err != nil || !ok {
			return err
		}
		return handle(input)
	})
}

func gleifCompanyRawInputFromRaw(rawRecord json.RawMessage) (gleifCompanyRawInput, bool, error) {
	var item map[string]any
	if err := json.Unmarshal(rawRecord, &item); err != nil {
		return gleifCompanyRawInput{}, false, fmt.Errorf("parse GLEIF record: %w", err)
	}
	lei := firstNonEmptyString(
		mapString(item, "lei"),
		mapString(item, "id"),
		mapString(nestedMap(item, "attributes"), "lei"),
		mapString(nestedMap(item, "LEI"), "$"),
	)
	if lei == "" {
		return gleifCompanyRawInput{}, false, nil
	}
	entity := nestedMap(nestedMap(item, "attributes"), "entity")
	if entity == nil {
		entity = nestedMap(item, "Entity")
	}
	legalName := firstNonEmptyString(
		mapString(nestedMap(entity, "legalName"), "name"),
		mapString(nestedMap(entity, "LegalName"), "$"),
		mapString(nestedMap(entity, "LegalName"), "name"),
		mapString(item, "legalName"),
	)
	status := firstNonEmptyString(
		mapString(entity, "status"),
		mapString(nestedMap(entity, "EntityStatus"), "$"),
		mapString(entity, "EntityStatus"),
		mapString(item, "entityStatus"),
	)
	headquartersAddress := nestedMap(entity, "headquartersAddress")
	if headquartersAddress == nil {
		headquartersAddress = nestedMap(entity, "HeadquartersAddress")
	}
	headquartersCountry := firstNonEmptyString(
		mapString(headquartersAddress, "country"),
		mapString(nestedMap(headquartersAddress, "Country"), "$"),
		mapString(headquartersAddress, "Country"),
		mapString(item, "headquartersCountry"),
	)
	return gleifCompanyRawInput{
		lei:                     lei,
		legalName:               legalName,
		registrationStatus:      status,
		headquartersCountryCode: headquartersCountry,
		rawPayload:              []byte(rawRecord),
		payloadHash:             hashBytes(rawRecord),
	}, true, nil
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

func openDownloadedSourceFile(file contracts.DownloadedSourceFile) (io.ReadCloser, error) {
	source, err := os.Open(file.FilePath)
	if err != nil {
		return nil, fmt.Errorf("open source file: %w", err)
	}
	if file.SHA256 != "" {
		actualHash, err := hashOpenFile(source)
		if err != nil {
			_ = source.Close()
			return nil, err
		}
		if !strings.EqualFold(actualHash, strings.TrimSpace(file.SHA256)) {
			_ = source.Close()
			return nil, fmt.Errorf("sha256 mismatch: expected %s got %s", file.SHA256, actualHash)
		}
	}

	compression, err := downloadedSourceCompression(source, file)
	if err != nil {
		_ = source.Close()
		return nil, err
	}
	if compression == "gzip" {
		reader, err := gzip.NewReader(source)
		if err != nil {
			_ = source.Close()
			return nil, fmt.Errorf("open gzip: %w", err)
		}
		return compoundReadCloser{
			Reader: reader,
			close: func() error {
				return closeAll(reader, source)
			},
		}, nil
	}
	if compression == "zip" {
		stat, err := source.Stat()
		if err != nil {
			_ = source.Close()
			return nil, fmt.Errorf("stat source file: %w", err)
		}
		reader, err := zip.NewReader(source, stat.Size())
		if err != nil {
			_ = source.Close()
			return nil, fmt.Errorf("open zip: %w", err)
		}
		for _, zippedFile := range reader.File {
			if zippedFile.FileInfo().IsDir() {
				continue
			}
			rc, err := zippedFile.Open()
			if err != nil {
				_ = source.Close()
				return nil, fmt.Errorf("open zipped file %s: %w", zippedFile.Name, err)
			}
			return compoundReadCloser{
				Reader: rc,
				close: func() error {
					return closeAll(rc, source)
				},
			}, nil
		}
		_ = source.Close()
		return nil, fmt.Errorf("zip contains no files")
	}
	return source, nil
}

func hashOpenFile(source *os.File) (string, error) {
	hash := sha256.New()
	if _, err := io.Copy(hash, source); err != nil {
		return "", fmt.Errorf("hash source file: %w", err)
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("rewind source file after hash: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func downloadedSourceCompression(source *os.File, file contracts.DownloadedSourceFile) (string, error) {
	magic, err := peekFileMagic(source)
	if err != nil {
		return "", err
	}
	if len(magic) >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		return "gzip", nil
	}
	if len(magic) >= 4 && magic[0] == 'P' && magic[1] == 'K' {
		return "zip", nil
	}

	format := strings.ToLower(file.Format)
	ext := strings.ToLower(filepath.Ext(file.FilePath))
	if format == "gzip" || format == "gz" || strings.HasSuffix(format, ".gz") || ext == ".gz" {
		return "gzip", nil
	}
	if format == "zip" || strings.HasSuffix(format, ".zip") || ext == ".zip" {
		return "zip", nil
	}
	return "", nil
}

func peekFileMagic(source *os.File) ([]byte, error) {
	magic := make([]byte, 4)
	n, err := io.ReadFull(source, magic)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("read source file magic: %w", err)
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind source file after magic read: %w", err)
	}
	return magic[:n], nil
}

type compoundReadCloser struct {
	io.Reader
	close func() error
}

func (c compoundReadCloser) Close() error {
	return c.close()
}

func closeAll(closers ...io.Closer) error {
	var closeErr error
	for _, closer := range closers {
		if err := closer.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func forEachJSONRecord(file contracts.DownloadedSourceFile, wrapperFields []string, handle func(json.RawMessage) error) error {
	reader, err := openDownloadedSourceFile(file)
	if err != nil {
		return err
	}
	defer reader.Close()
	return streamJSONRecords(reader, wrapperFields, handle)
}

func streamJSONRecords(reader io.Reader, wrapperFields []string, handle func(json.RawMessage) error) error {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("parse JSON: %w", err)
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return fmt.Errorf("parse JSON: expected array or object")
	}
	switch delim {
	case '[':
		return streamJSONArrayRecords(decoder, handle)
	case '{':
		return streamJSONObjectRecords(decoder, wrapperFields, handle)
	default:
		return fmt.Errorf("parse JSON: expected array or object")
	}
}

func streamJSONArrayRecords(decoder *json.Decoder, handle func(json.RawMessage) error) error {
	for decoder.More() {
		var rawRecord json.RawMessage
		if err := decoder.Decode(&rawRecord); err != nil {
			return fmt.Errorf("parse JSON record: %w", err)
		}
		if err := handle(rawRecord); err != nil {
			return err
		}
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("parse JSON array: %w", err)
	}
	if token != json.Delim(']') {
		return fmt.Errorf("parse JSON array: expected closing bracket")
	}
	return nil
}

func streamJSONObjectRecords(decoder *json.Decoder, wrapperFields []string, handle func(json.RawMessage) error) error {
	object := make(map[string]json.RawMessage)
	foundRecords := false
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("parse JSON object key: %w", err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return fmt.Errorf("parse JSON object key: expected string")
		}
		if stringInSlice(key, wrapperFields) {
			valueToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("parse JSON field %q: %w", key, err)
			}
			if valueToken != json.Delim('[') {
				return fmt.Errorf("parse JSON field %q: expected array", key)
			}
			if err := streamJSONArrayRecords(decoder, handle); err != nil {
				return err
			}
			foundRecords = true
			continue
		}
		var rawValue json.RawMessage
		if err := decoder.Decode(&rawValue); err != nil {
			return fmt.Errorf("parse JSON field %q: %w", key, err)
		}
		object[key] = append(json.RawMessage(nil), rawValue...)
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("parse JSON object: %w", err)
	}
	if token != json.Delim('}') {
		return fmt.Errorf("parse JSON object: expected closing brace")
	}
	if foundRecords {
		return nil
	}
	rawRecord, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("marshal JSON object record: %w", err)
	}
	return handle(rawRecord)
}

func stringInSlice(value string, candidates []string) bool {
	for _, candidate := range candidates {
		if value == candidate {
			return true
		}
	}
	return false
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
	value, ok := values[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	case map[string]any:
		return mapString(typed, "$")
	default:
		return ""
	}
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
