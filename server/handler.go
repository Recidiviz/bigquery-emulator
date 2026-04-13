package server

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
	"errors"
	"fmt"
	"html"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goccy/bigquery-emulator/internal/contentdata"

	"cloud.google.com/go/storage"
	"github.com/goccy/go-json"
	"github.com/vantaboard/go-googlesqlite"
	"go.uber.org/zap"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/internal/metadata"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/parquet-go/parquet-go"
)

func errorResponse(ctx context.Context, w http.ResponseWriter, e *ServerError) {
	logger.Logger(ctx).WithOptions(zap.AddCallerSkip(1)).Error(string(e.Reason), zap.Error(e))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Status)
	w.Write(e.Response())
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, response interface{}) {
	b, err := json.Marshal(response)
	if err != nil {
		errorResponse(ctx, w, errInternalError(fmt.Sprintf("failed to encode json: %s", err.Error())))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

const (
	discoveryAPIEndpoint    = "/discovery/v1/apis/bigquery/v2/rest"
	newDiscoveryAPIEndpoint = "/$discovery/rest"
	uploadAPIEndpoint       = "/upload/bigquery/v2/projects/{projectId}/jobs"
)

//go:embed resources/discovery.json
var bigqueryAPIJSON []byte

var (
	discoveryAPIOnce     sync.Once
	discoveryAPIResponse map[string]interface{}
)

type discoveryHandler struct {
	server *Server
}

func newDiscoveryHandler(server *Server) *discoveryHandler {
	return &discoveryHandler{server: server}
}

func (h *discoveryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var decodeJSONErr error
	discoveryAPIOnce.Do(func() {
		if err := json.Unmarshal(bigqueryAPIJSON, &discoveryAPIResponse); err != nil {
			decodeJSONErr = err
			return
		}
		addr := h.server.httpServer.Addr
		if !strings.HasPrefix(addr, "http") {
			addr = "http://" + addr
		}
		discoveryAPIResponse["mtlsRootUrl"] = addr
		discoveryAPIResponse["rootUrl"] = addr
		discoveryAPIResponse["baseUrl"] = addr
	})
	if decodeJSONErr != nil {
		errorResponse(ctx, w, errInternalError(decodeJSONErr.Error()))
		return
	}
	encodeResponse(ctx, w, discoveryAPIResponse)
}

type uploadHandler struct{}

type UploadJobConfigurationLoad struct {
	AllowJaggedRows                    bool                                   `json:"allowJaggedRows,omitempty"`
	AllowQuotedNewlines                bool                                   `json:"allowQuotedNewlines,omitempty"`
	Autodetect                         bool                                   `json:"autodetect,omitempty"`
	Clustering                         *bigqueryv2.Clustering                 `json:"clustering,omitempty"`
	CreateDisposition                  string                                 `json:"createDisposition,omitempty"`
	DecimalTargetTypes                 []string                               `json:"decimalTargetTypes,omitempty"`
	DestinationEncryptionConfiguration *bigqueryv2.EncryptionConfiguration    `json:"destinationEncryptionConfiguration,omitempty"`
	DestinationTable                   *bigqueryv2.TableReference             `json:"destinationTable,omitempty"`
	DestinationTableProperties         *bigqueryv2.DestinationTableProperties `json:"destinationTableProperties,omitempty"`
	Encoding                           string                                 `json:"encoding,omitempty"`
	FieldDelimiter                     string                                 `json:"fieldDelimiter,omitempty"`
	HivePartitioningOptions            *bigqueryv2.HivePartitioningOptions    `json:"hivePartitioningOptions,omitempty"`
	IgnoreUnknownValues                bool                                   `json:"ignoreUnknownValues,omitempty"`
	JsonExtension                      string                                 `json:"jsonExtension,omitempty"`
	MaxBadRecords                      int64                                  `json:"maxBadRecords,omitempty"`
	NullMarker                         string                                 `json:"nullMarker,omitempty"`
	ParquetOptions                     *bigqueryv2.ParquetOptions             `json:"parquetOptions,omitempty"`
	PreserveAsciiControlCharacters     bool                                   `json:"preserveAsciiControlCharacters,omitempty"`
	ProjectionFields                   []string                               `json:"projectionFields,omitempty"`
	Quote                              *string                                `json:"quote,omitempty"`
	RangePartitioning                  *bigqueryv2.RangePartitioning          `json:"rangePartitioning,omitempty"`
	Schema                             *bigqueryv2.TableSchema                `json:"schema,omitempty"`
	SchemaInline                       string                                 `json:"schemaInline,omitempty"`
	SchemaInlineFormat                 string                                 `json:"schemaInlineFormat,omitempty"`
	SchemaUpdateOptions                []string                               `json:"schemaUpdateOptions,omitempty"`
	SkipLeadingRows                    json.Number                            `json:"skipLeadingRows,omitempty"`
	SourceFormat                       string                                 `json:"sourceFormat,omitempty"`
	SourceUris                         []string                               `json:"sourceUris,omitempty"`
	TimePartitioning                   *bigqueryv2.TimePartitioning           `json:"timePartitioning,omitempty"`
	UseAvroLogicalTypes                bool                                   `json:"useAvroLogicalTypes,omitempty"`
	WriteDisposition                   string                                 `json:"writeDisposition,omitempty"`
}

type UploadJobConfiguration struct {
	Load *UploadJobConfigurationLoad `json:"load"`
}

type UploadJob struct {
	JobReference  *bigqueryv2.JobReference `json:"jobReference"`
	Configuration *UploadJobConfiguration  `json:"configuration"`
}

func (j *UploadJob) ToJob() *bigqueryv2.Job {
	load := j.Configuration.Load
	skipLeadingRows, _ := load.SkipLeadingRows.Int64()
	return &bigqueryv2.Job{
		JobReference: j.JobReference,
		Configuration: &bigqueryv2.JobConfiguration{
			Load: &bigqueryv2.JobConfigurationLoad{
				AllowJaggedRows:                    load.AllowJaggedRows,
				AllowQuotedNewlines:                load.AllowQuotedNewlines,
				Autodetect:                         load.Autodetect,
				Clustering:                         load.Clustering,
				CreateDisposition:                  load.CreateDisposition,
				DecimalTargetTypes:                 load.DecimalTargetTypes,
				DestinationEncryptionConfiguration: load.DestinationEncryptionConfiguration,
				DestinationTable:                   load.DestinationTable,
				DestinationTableProperties:         load.DestinationTableProperties,
				Encoding:                           load.Encoding,
				FieldDelimiter:                     load.FieldDelimiter,
				HivePartitioningOptions:            load.HivePartitioningOptions,
				IgnoreUnknownValues:                load.IgnoreUnknownValues,
				JsonExtension:                      load.JsonExtension,
				MaxBadRecords:                      load.MaxBadRecords,
				NullMarker:                         load.NullMarker,
				ParquetOptions:                     load.ParquetOptions,
				PreserveAsciiControlCharacters:     load.PreserveAsciiControlCharacters,
				ProjectionFields:                   load.ProjectionFields,
				Quote:                              load.Quote,
				RangePartitioning:                  load.RangePartitioning,
				Schema:                             load.Schema,
				SchemaInline:                       load.SchemaInline,
				SchemaInlineFormat:                 load.SchemaInlineFormat,
				SchemaUpdateOptions:                load.SchemaUpdateOptions,
				SkipLeadingRows:                    skipLeadingRows,
				SourceFormat:                       load.SourceFormat,
				SourceUris:                         load.SourceUris,
				TimePartitioning:                   load.TimePartitioning,
				UseAvroLogicalTypes:                load.UseAvroLogicalTypes,
				WriteDisposition:                   load.WriteDisposition,
			},
		},
	}
}

func (h *uploadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("uploadType") {
	case "multipart":
		h.serveMultipart(w, r)
	case "resumable":
		h.serveResumable(w, r)
	default:
		errorResponse(r.Context(), w, errInvalid(`uploadType should be "multipart" or "resumable"`))
	}
}

func (h *uploadHandler) serveMultipart(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	contentType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(contentType, "multipart/") {
		errorResponse(ctx, w, errInvalid("expecting a multipart message"))
		return
	}
	mul := multipart.NewReader(r.Body, params["boundary"])
	p, err := mul.NextPart()
	if err != nil {
		errorResponse(ctx, w, errInvalid(fmt.Sprintf("failed to load metadata: %s", err.Error())))
		return
	}
	var job UploadJob
	if err := json.NewDecoder(p).Decode(&job); err != nil {
		errorResponse(ctx, w, errInvalid(fmt.Sprintf("failed to decode job: %s", err.Error())))
		return
	}
	uploadJob, err := h.Handle(ctx, &uploadRequest{
		server:  server,
		project: project,
		job:     &job,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}

	p, err = mul.NextPart()
	if err != nil {
		errorResponse(ctx, w, errInvalid(fmt.Sprintf("multipart request is invalid: %s", err.Error())))
		return
	}
	u := &uploadContentHandler{}
	err = u.Handle(ctx, &uploadContentRequest{
		server:  server,
		project: project,
		job:     uploadJob,
		reader:  p,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, uploadJob.Content())
}

func (h *uploadHandler) serveResumable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	var job UploadJob
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		errorResponse(ctx, w, errInvalid(fmt.Sprintf("failed to decode job: %s", err.Error())))
		return
	}
	res, err := h.Handle(ctx, &uploadRequest{
		server:  server,
		project: project,
		job:     &job,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}

	// We need to return a fully-qualified URL for the client to use as an
	// upload endpoint. We can't get this from our server bind address, as
	// it's usually 0.0.0.0 (IPADDR_ANY) which is not something that can be
	// connected to. Instead, use the Host header to figure out where the
	// client thinks it's connecting to and use that. This also handles the
	// case where we're behind a NAT (e.g. in a container), as we don't
	// otherwise know what our external name is.
	//
	// See https://github.com/goccy/bigquery-emulator/issues/160
	addr := fmt.Sprintf("http://%s", r.Host)

	w.Header().Add(
		"Location",
		fmt.Sprintf(
			"%s/upload/bigquery/v2/projects/%s/jobs?uploadType=resumable&upload_id=%s",
			addr,
			project.ID,
			job.JobReference.JobId,
		),
	)
	encodeResponse(ctx, w, res.Content())
}

type uploadRequest struct {
	server  *Server
	project *metadata.Project
	job     *UploadJob
}

func (h *uploadHandler) Handle(ctx context.Context, r *uploadRequest) (*metadata.Job, error) {
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, "")
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	job := metadata.NewJob(r.server.metaRepo, r.project.ID, r.job.JobReference.JobId, r.job.ToJob(), nil, nil)
	if err := r.project.AddJob(ctx, tx.Tx(), job); err != nil {
		return nil, fmt.Errorf("failed to add job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit job: %w", err)
	}
	return job, nil
}

type uploadContentHandler struct{}

func (h *uploadContentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	query := r.URL.Query()
	uploadType := query["uploadType"]
	if len(uploadType) == 0 {
		errorResponse(ctx, w, errInvalid("uploadType parameter is not found"))
		return
	}
	if uploadType[0] != "resumable" {
		errorResponse(ctx, w, errInvalid(fmt.Sprintf("uploadType parameter is not resumable %s", uploadType[0])))
		return
	}
	uploadID := query["upload_id"]
	if len(uploadID) == 0 {
		errorResponse(ctx, w, errInvalid("upload_id parameter is not found"))
		return
	}
	jobID := uploadID[0]
	job, err := project.Job(ctx, jobID)
	if err != nil {
		errorResponse(ctx, w, errJobInternalError(err.Error()))
		return
	}
	if job == nil {
		errorResponse(ctx, w, errJobInternalError(fmt.Sprintf("job %s not found", jobID)))
		return
	}
	if err := h.Handle(ctx, &uploadContentRequest{
		server:  server,
		project: project,
		job:     job,
		reader:  r.Body,
	}); err != nil {
		errorResponse(ctx, w, errJobInternalError(err.Error()))
		return
	}
	content := job.Content()
	content.Status = &bigqueryv2.JobStatus{State: "DONE"}
	encodeResponse(ctx, w, content)
}

type uploadContentRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
	reader  io.Reader
}

func (h *uploadContentHandler) getCandidateName(col string, columnNames []string) string {
	var (
		foundName  string
		foundCount int
	)
	for _, name := range columnNames {
		if strings.Contains(name, col) {
			foundName = name
			foundCount++
		}
	}
	if foundCount == 1 {
		return foundName
	}
	return ""
}

func (h *uploadContentHandler) existsColumnNameInCSVHeader(col string, header []string) bool {
	for _, h := range header {
		if col == h {
			return true
		}
	}
	return false
}

func (h *uploadContentHandler) normalizeColumnNameForJSONData(columnMap map[string]*types.Column, data map[string]interface{}) {
	for k, v := range data {
		if _, exists := columnMap[k]; exists {
			continue
		}
		lowerKey := strings.ToLower(k)
		var (
			foundCount int
			columnName string
		)
		for colName := range columnMap {
			if lowerKey == strings.ToLower(colName) {
				foundCount++
				columnName = colName
			}
		}
		if foundCount == 1 {
			delete(data, k)
			data[columnName] = v
		}
	}
}

// CSV Schema Detection and Type Inference

// dateLayout is the only format BigQuery supports for DATE auto-detection (ISO 8601)
const dateLayout = "2006-01-02" // YYYY-MM-DD

// isNullValue returns true if the value represents NULL
func isNullValue(v string) bool {
	lower := strings.ToLower(strings.TrimSpace(v))
	return lower == "" || lower == "null"
}

// isBool returns true if the value can be auto-detected as a boolean.
// Note: BigQuery can parse 1/0 as booleans but does not auto-detect them.
func isBool(v string) bool {
	lower := strings.ToLower(strings.TrimSpace(v))
	switch lower {
	case "true", "false", "t", "f", "yes", "no", "y", "n":
		return true
	}
	return false
}

// isInteger returns true if the value can be parsed as an integer
func isInteger(v string) bool {
	_, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	return err == nil
}

// isFloat returns true if the value can be parsed as a float
func isFloat(v string) bool {
	_, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	return err == nil
}

// isDate returns true if the value can be parsed as an ISO date (YYYY-MM-DD)
func isDate(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return false
	}
	_, err := time.Parse(dateLayout, v)
	return err == nil
}

// timeLayouts are the formats BigQuery supports for TIME
var timeLayouts = []string{
	"15:04:05.000000", // HH:MM:SS.SSSSSS (microseconds)
	"15:04:05.000",    // HH:MM:SS.SSS (milliseconds)
	"15:04:05",        // HH:MM:SS
}

// isTime returns true if the value can be parsed as a TIME (HH:MM:SS[.SSSSSS])
func isTime(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return false
	}
	for _, layout := range timeLayouts {
		if _, err := time.Parse(layout, v); err == nil {
			return true
		}
	}
	return false
}

// datetimeLayouts are the formats BigQuery supports for DATETIME
var datetimeLayouts = []string{
	"2006-01-02 15:04:05.000000",
	"2006-01-02 15:04:05.000",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.000000",
	"2006-01-02T15:04:05.000",
	"2006-01-02T15:04:05",
}

// isDatetime returns true if the value can be parsed as a DATETIME
// (date + time, no timezone)
func isDatetime(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return false
	}
	// Reject if it looks like a timestamp (has timezone indicator)
	if hasTimezoneIndicator(v) {
		return false
	}
	for _, layout := range datetimeLayouts {
		if _, err := time.Parse(layout, v); err == nil {
			return true
		}
	}
	return false
}

// timestampLayouts for parsing timestamps with timezone
var timestampLayouts = []string{
	// With timezone offset (Z)
	"2006-01-02T15:04:05.000000Z",
	"2006-01-02T15:04:05.000Z",
	"2006-01-02T15:04:05Z",
	"2006-01-02T15:04Z",
	"2006-01-02 15:04:05.000000Z",
	"2006-01-02 15:04:05.000Z",
	"2006-01-02 15:04:05Z",
	"2006-01-02 15:04Z",
	// With timezone offset (+/-HH:MM)
	"2006-01-02T15:04:05.000000-07:00",
	"2006-01-02T15:04:05.000-07:00",
	"2006-01-02T15:04:05-07:00",
	"2006-01-02T15:04-07:00",
	"2006-01-02 15:04:05.000000-07:00",
	"2006-01-02 15:04:05.000-07:00",
	"2006-01-02 15:04:05-07:00",
	"2006-01-02 15:04-07:00",
	"2006-01-02 15:04:05.000000 -07:00",
	"2006-01-02 15:04:05.000 -07:00",
	"2006-01-02 15:04:05 -07:00",
	"2006-01-02 15:04 -07:00",
	// With timezone name (UTC)
	"2006-01-02 15:04:05.000000 UTC",
	"2006-01-02 15:04:05.000 UTC",
	"2006-01-02 15:04:05 UTC",
	"2006-01-02 15:04 UTC",
	// Slash separator variants (BigQuery supports YYYY/MM/DD for timestamps)
	"2006/01/02 15:04:05.000000",
	"2006/01/02 15:04:05.000",
	"2006/01/02 15:04:05",
	"2006/01/02 15:04",
}

// tzOffsetRegex matches timezone offsets at the end of a string
var tzOffsetRegex = regexp.MustCompile(`[+-]\d{2}:?\d{0,2}$`)

// hasTimezoneIndicator checks if a string contains timezone information
func hasTimezoneIndicator(v string) bool {
	// Check for Z suffix
	if strings.HasSuffix(v, "Z") {
		return true
	}
	// Check for UTC suffix
	if strings.HasSuffix(v, " UTC") || strings.HasSuffix(v, "UTC") {
		return true
	}
	// Check for offset pattern like +05:00, -07:00, +0530, -05
	return tzOffsetRegex.MatchString(v)
}

// unixTimestampRegex matches Unix epoch timestamps (integer or scientific notation)
var unixTimestampRegex = regexp.MustCompile(`^-?\d+(\.\d+)?([eE][+-]?\d+)?$`)

// isUnixTimestamp checks if the value looks like a Unix timestamp
func isUnixTimestamp(v string) bool {
	v = strings.TrimSpace(v)
	if !unixTimestampRegex.MatchString(v) {
		return false
	}
	// Parse as float to handle scientific notation
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return false
	}
	// Reasonable Unix timestamp range (1970-01-01 to ~2100)
	// In seconds: 0 to ~4102444800
	// In milliseconds: 0 to ~4102444800000
	// Scientific notation like 1.534680695e12 represents milliseconds
	if f >= 0 && f <= 4102444800 {
		return true // Looks like seconds
	}
	if f >= 1e10 && f <= 4102444800000 {
		return true // Looks like milliseconds
	}
	return false
}

// isTimestamp returns true if the value can be parsed as a TIMESTAMP
// (has timezone info or slash-date format)
// Note: Unix epoch timestamps are NOT auto-detected per BigQuery docs.
func isTimestamp(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return false
	}

	// Note: Unix epoch timestamps are not auto-detected by BigQuery.
	// They are treated as numeric types instead.

	// Check if it has timezone indicator or slash-date format
	hasSlashDate := len(v) >= 10 && v[4] == '/' && v[7] == '/'

	if !hasTimezoneIndicator(v) && !hasSlashDate {
		return false
	}

	// Try parsing with known layouts
	for _, layout := range timestampLayouts {
		if _, err := time.Parse(layout, v); err == nil {
			return true
		}
	}

	return false
}

// allMatch returns true if all values satisfy the predicate
func allMatch(values []string, predicate func(string) bool) bool {
	for _, v := range values {
		if !predicate(v) {
			return false
		}
	}
	return true
}

// selectSampleRows selects a representative sample of rows for type inference
func selectSampleRows(rows [][]string, maxSamples int) [][]string {
	if len(rows) <= maxSamples {
		return rows
	}

	samples := make([][]string, 0, maxSamples)
	samples = append(samples, rows[0]) // First row

	// Evenly space the remaining samples
	remaining := maxSamples - 2
	step := float64(len(rows)-2) / float64(remaining+1)
	for i := 1; i <= remaining; i++ {
		idx := int(float64(i) * step)
		if idx > 0 && idx < len(rows)-1 {
			samples = append(samples, rows[idx])
		}
	}

	samples = append(samples, rows[len(rows)-1]) // Last row
	return samples
}

// inferFieldType infers the BigQuery field type from sample values
func inferFieldType(rows [][]string, colIndex int) string {
	// Collect all non-empty, non-null values
	values := make([]string, 0)
	for _, row := range rows {
		if colIndex < len(row) {
			val := strings.TrimSpace(row[colIndex])
			if !isNullValue(val) {
				values = append(values, val)
			}
		}
	}

	if len(values) == 0 {
		return "STRING" // Default to STRING for all-null columns
	}

	// Try each type in order of specificity
	// Order matters: more restrictive types first

	// 1. BOOL - most restrictive text patterns
	if allMatch(values, isBool) {
		return "BOOL"
	}
	// 2. INTEGER - pure integers only
	if allMatch(values, isInteger) {
		return "INTEGER"
	}
	// 3. FLOAT - numeric with decimals
	if allMatch(values, isFloat) {
		return "FLOAT"
	}
	// 4. TIMESTAMP - has timezone or Unix epoch
	if allMatch(values, isTimestamp) {
		return "TIMESTAMP"
	}
	// 5. DATETIME - date + time, no timezone
	if allMatch(values, isDatetime) {
		return "DATETIME"
	}
	// 6. DATE - date only (YYYY-MM-DD)
	if allMatch(values, isDate) {
		return "DATE"
	}
	// 7. TIME - time only (HH:MM:SS[.SSSSSS])
	if allMatch(values, isTime) {
		return "TIME"
	}

	return "STRING"
}

// detectSchema samples rows from a CSV and infers the schema
func (h *uploadContentHandler) detectSchema(records [][]string, skipLeadingRows int64) (*bigqueryv2.TableSchema, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("empty csv file")
	}

	header := records[0]
	dataRows := records[1:]

	// SkipLeadingRows includes the header row, so when using autodetect
	// (where row 0 is always the header), we should only skip additional
	// rows if SkipLeadingRows > 1
	if skipLeadingRows > 1 {
		skip := int(skipLeadingRows) - 1 // -1 because header is already handled
		if skip < len(dataRows) {
			dataRows = dataRows[skip:]
		}
	}

	if len(dataRows) == 0 {
		// Only header, create schema with STRING types
		fields := make([]*bigqueryv2.TableFieldSchema, len(header))
		for i, colName := range header {
			fields[i] = &bigqueryv2.TableFieldSchema{
				Name: colName,
				Type: "STRING",
				Mode: "NULLABLE",
			}
		}
		return &bigqueryv2.TableSchema{Fields: fields}, nil
	}

	// Sample rows for type inference
	sampleRows := selectSampleRows(dataRows, 10)

	// Infer type for each column
	fields := make([]*bigqueryv2.TableFieldSchema, len(header))
	for i, colName := range header {
		inferredType := inferFieldType(sampleRows, i)
		fields[i] = &bigqueryv2.TableFieldSchema{
			Name: colName,
			Type: inferredType,
			Mode: "NULLABLE",
		}
	}

	return &bigqueryv2.TableSchema{Fields: fields}, nil
}

// parseBoolValue parses various boolean representations.
// Note: 1/0 are kept for explicit schema parsing even though they're not auto-detected.
func parseBoolValue(v string) (bool, error) {
	lower := strings.ToLower(strings.TrimSpace(v))
	switch lower {
	case "true", "t", "yes", "y", "1":
		return true, nil
	case "false", "f", "no", "n", "0":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value: %s", v)
	}
}

// parseAndConvertDate parses an ISO date (YYYY-MM-DD) and returns it
func parseAndConvertDate(v string) (string, error) {
	v = strings.TrimSpace(v)
	if _, err := time.Parse(dateLayout, v); err != nil {
		return "", fmt.Errorf("could not parse date: %s", v)
	}
	return v, nil // Already in ISO format
}

// parseTimeValue parses a time string and returns it in canonical format
func parseTimeValue(v string) (string, error) {
	v = strings.TrimSpace(v)
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, v); err == nil {
			// Return in canonical format HH:MM:SS.SSSSSS
			return t.Format("15:04:05.000000"), nil
		}
	}
	return "", fmt.Errorf("could not parse time: %s", v)
}

// parseDatetimeValue parses a datetime string and returns it in canonical format
func parseDatetimeValue(v string) (string, error) {
	v = strings.TrimSpace(v)
	for _, layout := range datetimeLayouts {
		if t, err := time.Parse(layout, v); err == nil {
			// Return in canonical format YYYY-MM-DD HH:MM:SS.SSSSSS
			return t.Format("2006-01-02 15:04:05.000000"), nil
		}
	}
	return "", fmt.Errorf("could not parse datetime: %s", v)
}

// parseTimestampValue parses a timestamp string and returns it in RFC3339 format
func parseTimestampValue(v string) (string, error) {
	v = strings.TrimSpace(v)

	// Handle Unix timestamp
	if isUnixTimestamp(v) {
		f, _ := strconv.ParseFloat(v, 64)
		var t time.Time
		if f >= 1e10 {
			// Milliseconds - convert to time
			t = time.UnixMilli(int64(f))
		} else {
			// Seconds (may have decimal microseconds)
			sec := int64(f)
			nsec := int64((f - float64(sec)) * 1e9)
			t = time.Unix(sec, nsec)
		}
		return t.UTC().Format(time.RFC3339Nano), nil
	}

	// Try all timestamp layouts
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC().Format(time.RFC3339Nano), nil
		}
	}

	return "", fmt.Errorf("could not parse timestamp: %s", v)
}

// convertCSVValue converts a string value to the appropriate Go type based on the column type
func convertCSVValue(value string, colType types.Type) (interface{}, error) {
	value = strings.TrimSpace(value)

	// Handle NULL values
	if isNullValue(value) {
		return nil, nil
	}

	switch colType {
	case types.INT64:
		return strconv.ParseInt(value, 10, 64)
	case types.FLOAT64:
		return strconv.ParseFloat(value, 64)
	case types.BOOL:
		return parseBoolValue(value)
	case types.DATE:
		return parseAndConvertDate(value)
	case types.TIME:
		return parseTimeValue(value)
	case types.DATETIME:
		return parseDatetimeValue(value)
	case types.TIMESTAMP:
		return parseTimestampValue(value)
	case types.STRING:
		return value, nil
	default:
		return value, nil
	}
}

func (h *uploadContentHandler) Handle(ctx context.Context, r *uploadContentRequest) error {
	content := r.job.Content()
	if content == nil || content.Configuration == nil || content.Configuration.Load == nil {
		return fmt.Errorf("job configuration is incomplete")
	}
	load := content.Configuration.Load
	tableRef := load.DestinationTable
	dataset, err := r.project.Dataset(ctx, tableRef.DatasetId)
	if err != nil {
		return err
	}
	table, err := dataset.Table(ctx, tableRef.TableId)
	if err != nil {
		return err
	}

	// For CSV with autodetect, we need to read the file first to detect schema
	var csvRecords [][]string
	if load.SourceFormat == "CSV" {
		csvRecords, err = csv.NewReader(r.reader).ReadAll()
		if err != nil {
			return fmt.Errorf("failed to read csv: %w", err)
		}
		if len(csvRecords) == 0 {
			return fmt.Errorf("failed to find csv header")
		}

		// If autodetect is enabled and no schema provided, detect schema from CSV
		if load.Autodetect && (load.Schema == nil || len(load.Schema.Fields) == 0) {
			detectedSchema, err := h.detectSchema(csvRecords, load.SkipLeadingRows)
			if err != nil {
				return fmt.Errorf("failed to detect schema: %w", err)
			}
			load.Schema = detectedSchema
		}
	}

	if table == nil {
		if load.CreateDisposition == "CREATE_NEVER" {
			return fmt.Errorf("`%s` is not found", tableRef.TableId)
		}
		if _, err := (&tablesInsertHandler{}).Handle(ctx, &tablesInsertRequest{
			server:  r.server,
			project: r.project,
			dataset: dataset,
			table: &bigqueryv2.Table{
				Schema:         load.Schema,
				TableReference: tableRef,
			},
		}); err != nil {
			return err
		}
		table, err = dataset.Table(ctx, tableRef.TableId)
		if err != nil {
			return err
		}
	}

	tableContent, err := table.Content()
	if err != nil {
		return err
	}
	columnToType := map[string]types.Type{}
	for _, field := range tableContent.Schema.Fields {
		columnToType[field.Name] = types.Type(field.Type)
	}

	sourceFormat := load.SourceFormat
	columns := []*types.Column{}
	data := types.Data{}
	switch sourceFormat {
	case "CSV":
		// csvRecords already populated above
		if len(csvRecords) == 1 {
			return nil // Only header, no data
		}
		header := csvRecords[0]
		var ignoreHeader bool
		for _, col := range header {
			if _, exists := columnToType[col]; !exists {
				ignoreHeader = true
				break
			}
			columns = append(columns, &types.Column{
				Name: col,
				Type: columnToType[col],
			})
		}
		if ignoreHeader {
			columns = []*types.Column{}
			for _, field := range tableContent.Schema.Fields {
				columns = append(columns, &types.Column{
					Name: field.Name,
					Type: types.Type(field.Type),
				})
			}
		}

		// Determine data rows, respecting SkipLeadingRows
		// SkipLeadingRows includes the header row, so we only skip additional
		// rows if SkipLeadingRows > 1 (header already extracted as row 0)
		dataRows := csvRecords[1:]
		if load.SkipLeadingRows > 1 {
			skip := int(load.SkipLeadingRows) - 1 // -1 because header is already handled
			if skip < len(dataRows) {
				dataRows = dataRows[skip:]
			}
		}

		for _, record := range dataRows {
			rowData := map[string]interface{}{}
			if len(record) != len(columns) {
				return fmt.Errorf("invalid column number: found broken row data: %v", record)
			}
			for i := 0; i < len(record); i++ {
				colData := record[i]
				// Convert value based on column type
				converted, err := convertCSVValue(colData, columns[i].Type)
				if err != nil {
					return fmt.Errorf("failed to convert value %q for column %s: %w", colData, columns[i].Name, err)
				}
				rowData[columns[i].Name] = converted
			}
			data = append(data, rowData)
		}
	case "PARQUET":
		b, err := io.ReadAll(r.reader)
		if err != nil {
			return err
		}
		reader := parquet.NewReader(bytes.NewReader(b))
		defer reader.Close()

		for _, f := range load.Schema.Fields {
			columns = append(columns, &types.Column{
				Name: f.Name,
				Type: types.Type(f.Type),
			})
		}

		for i := 0; i < int(reader.NumRows()); i++ {
			var rowData interface{}
			err := reader.Read(&rowData)
			if err != nil {
				return err
			}

			data = append(data, rowData.(map[string]interface{}))
		}
	case "NEWLINE_DELIMITED_JSON":
		for _, f := range tableContent.Schema.Fields {
			columns = append(columns, &types.Column{
				Name: f.Name,
				Type: types.Type(f.Type),
			})
		}
		columnMap := map[string]*types.Column{}
		for _, col := range columns {
			columnMap[col.Name] = col
		}
		decoder := json.NewDecoder(r.reader)
		decoder.UseNumber()
		for decoder.More() {
			d := make(map[string]interface{})
			if err := decoder.Decode(&d); err != nil {
				return err
			}
			h.normalizeColumnNameForJSONData(columnMap, d)
			data = append(data, d)
		}
	default:
		return fmt.Errorf("not support sourceFormat: %s", sourceFormat)
	}
	tableDef := &types.Table{
		ID:      tableRef.TableId,
		Columns: columns,
		Data:    data,
	}
	conn := connectionFromContext(ctx).ConfigureScope(tableRef.ProjectId, tableRef.DatasetId)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.server.contentRepo.AddTableData(ctx, tx, tableRef.ProjectId, tableRef.DatasetId, tableDef, load.WriteDisposition == "WRITE_TRUNCATE"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

const (
	formatOptionsUseInt64TimestampParam = "formatOptions.useInt64Timestamp"
	deleteContentsParam                 = "deleteContents"
	maxResults                          = "maxResults"
	maxResultsDefaultValue              = -1
)

func isDeleteContents(r *http.Request) bool {
	return parseQueryValueAsBool(r, deleteContentsParam)
}

func isFormatOptionsUseInt64Timestamp(r *http.Request) bool {
	return parseQueryValueAsBool(r, formatOptionsUseInt64TimestampParam)
}

func parseQueryValueAsBool(r *http.Request, key string) bool {
	queryValues := r.URL.Query()
	values, exists := queryValues[key]
	if !exists {
		return false
	}
	if len(values) != 1 {
		return false
	}
	b, err := strconv.ParseBool(values[0])
	if err != nil {
		return false
	}
	return b
}

func parseQueryValueAsInt64(r *http.Request, key string, defaultValue int64) int64 {
	queryValues := r.URL.Query()
	values, exists := queryValues[key]
	if !exists {
		return defaultValue
	}
	if len(values) != 1 {
		return defaultValue
	}
	parsed, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func (h *datasetsDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	if err := h.Handle(ctx, &datasetsDeleteRequest{
		server:         server,
		project:        project,
		dataset:        dataset,
		deleteContents: isDeleteContents(r),
	}); err != nil {
		if errors.Is(err, metadata.ErrDatasetInUse) {
			errorResponse(ctx, w, errResourceInUse(err.Error()))
			return
		}

		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
}

type datasetsDeleteRequest struct {
	server         *Server
	project        *metadata.Project
	dataset        *metadata.Dataset
	deleteContents bool
}

func (h *datasetsDeleteHandler) Handle(ctx context.Context, r *datasetsDeleteRequest) error {
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, r.dataset.ID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.dataset.Delete(ctx, tx.Tx(), r.deleteContents); err != nil {
		return fmt.Errorf("failed to delete dataset: %w", err)
	}
	if r.deleteContents {
		tables, err := r.server.metaRepo.FindTablesInDatasetsWithConnection(ctx, tx.Tx(), r.dataset.ProjectID, r.dataset.ID)
		if err != nil {
			return fmt.Errorf("failed to find tables in dataset: %w", err)
		}
		tableIDs := make([]string, len(tables))
		for i, table := range tables {
			tableIDs[i] = table.ID
			if err := table.Delete(ctx, tx.Tx()); err != nil {
				return err
			}
		}

		if err := r.server.contentRepo.DeleteTables(ctx, tx, r.project.ID, r.dataset.ID, tables); err != nil {
			return fmt.Errorf("failed to delete tables: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit delete dataset: %w", err)
	}
	return nil
}

func (h *datasetsGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &datasetsGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *datasetsGetHandler) Handle(ctx context.Context, r *datasetsGetRequest) (*bigqueryv2.Dataset, error) {
	newContent := *r.dataset.Content()
	newContent.DatasetReference = &bigqueryv2.DatasetReference{
		ProjectId: r.project.ID,
		DatasetId: r.dataset.ID,
	}
	return &newContent, nil
}

func (h *datasetsInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	var dataset bigqueryv2.Dataset
	if err := json.NewDecoder(r.Body).Decode(&dataset); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &datasetsInsertRequest{
		server:  server,
		project: project,
		dataset: &dataset,
	})

	if err != nil {
		if errors.Is(err, metadata.ErrDuplicatedDataset) {
			errorResponse(ctx, w, errDuplicate(err.Error()))
			return
		}

		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsInsertRequest struct {
	server  *Server
	project *metadata.Project
	dataset *bigqueryv2.Dataset
}

func (h *datasetsInsertHandler) Handle(ctx context.Context, r *datasetsInsertRequest) (*bigqueryv2.DatasetListDatasets, error) {
	if r.dataset.DatasetReference == nil {
		return nil, fmt.Errorf("DatasetReference is nil")
	}
	datasetID := r.dataset.DatasetReference.DatasetId
	if datasetID == "" {
		return nil, fmt.Errorf("dataset id is empty")
	}
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, datasetID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()

	if err := r.project.AddDataset(
		ctx,
		tx.Tx(),
		metadata.NewDataset(
			r.server.metaRepo,
			r.project.ID,
			datasetID,
			r.dataset,
		),
	); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &bigqueryv2.DatasetListDatasets{
		DatasetReference: &bigqueryv2.DatasetReference{
			ProjectId: r.project.ID,
			DatasetId: datasetID,
		},
		Id:              datasetID,
		FriendlyName:    r.dataset.FriendlyName,
		Kind:            r.dataset.Kind,
		Labels:          r.dataset.Labels,
		Location:        r.dataset.Location,
		ForceSendFields: r.dataset.ForceSendFields,
		NullFields:      r.dataset.NullFields,
	}, nil
}

func (h *datasetsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	res, err := h.Handle(ctx, &datasetsListRequest{
		server:  server,
		project: project,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsListRequest struct {
	server  *Server
	project *metadata.Project
}

func (h *datasetsListHandler) Handle(ctx context.Context, r *datasetsListRequest) (*bigqueryv2.DatasetList, error) {
	datasetsRes := []*bigqueryv2.DatasetListDatasets{}
	datasets, err := r.server.metaRepo.FindDatasetsInProject(ctx, r.project.ID)
	if err != nil {
		return nil, err
	}
	for _, dataset := range datasets {
		content := dataset.Content()
		datasetsRes = append(datasetsRes, &bigqueryv2.DatasetListDatasets{
			DatasetReference: &bigqueryv2.DatasetReference{
				ProjectId: r.project.ID,
				DatasetId: dataset.ID,
			},
			FriendlyName:    content.FriendlyName,
			Id:              dataset.ID,
			Kind:            content.Kind,
			Labels:          content.Labels,
			Location:        content.Location,
			ForceSendFields: content.ForceSendFields,
			NullFields:      content.NullFields,
		})
	}
	return &bigqueryv2.DatasetList{
		Datasets: datasetsRes,
	}, nil
}

func (h *datasetsPatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	var newDataset bigqueryv2.Dataset
	if err := json.NewDecoder(r.Body).Decode(&newDataset); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &datasetsPatchRequest{
		server:     server,
		project:    project,
		dataset:    dataset,
		newDataset: &newDataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsPatchRequest struct {
	server     *Server
	project    *metadata.Project
	dataset    *metadata.Dataset
	newDataset *bigqueryv2.Dataset
}

func (h *datasetsPatchHandler) Handle(ctx context.Context, r *datasetsPatchRequest) (*bigqueryv2.Dataset, error) {
	r.dataset.UpdateContentIfExists(r.newDataset)
	newContent := *r.dataset.Content()
	return &newContent, nil
}

func (h *datasetsUpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	var newDataset bigqueryv2.Dataset
	if err := json.NewDecoder(r.Body).Decode(&newDataset); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &datasetsUpdateRequest{
		server:     server,
		project:    project,
		dataset:    dataset,
		newDataset: &newDataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsUpdateRequest struct {
	server     *Server
	project    *metadata.Project
	dataset    *metadata.Dataset
	newDataset *bigqueryv2.Dataset
}

func (h *datasetsUpdateHandler) Handle(ctx context.Context, r *datasetsUpdateRequest) (*bigqueryv2.Dataset, error) {
	r.dataset.UpdateContent(r.newDataset)
	newContent := *r.dataset.Content()
	return &newContent, nil
}

func (h *jobsCancelHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	res, err := h.Handle(ctx, &jobsCancelRequest{
		server:  server,
		project: project,
		job:     job,
	})
	if err != nil {
		errorResponse(ctx, w, errJobInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsCancelRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
}

func (h *jobsCancelHandler) Handle(ctx context.Context, r *jobsCancelRequest) (*bigqueryv2.JobCancelResponse, error) {
	if err := r.job.Cancel(ctx); err != nil {
		return nil, err
	}
	return &bigqueryv2.JobCancelResponse{Job: r.job.Content()}, nil
}

func (h *jobsDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	if err := h.Handle(ctx, &jobsDeleteRequest{
		server:  server,
		project: project,
		job:     job,
	}); err != nil {
		errorResponse(ctx, w, errJobInternalError(err.Error()))
		return
	}
}

type jobsDeleteRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
}

func (h *jobsDeleteHandler) Handle(ctx context.Context, r *jobsDeleteRequest) error {
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, "")
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.job.Delete(ctx, tx.Tx()); err != nil {
		return fmt.Errorf("failed to delete job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (h *jobsGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	res, err := h.Handle(ctx, &jobsGetRequest{
		server:  server,
		project: project,
		job:     job,
	})
	if err != nil {
		errorResponse(ctx, w, errJobInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsGetRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
}

func (h *jobsGetHandler) Handle(ctx context.Context, r *jobsGetRequest) (*bigqueryv2.Job, error) {
	content := *r.job.Content()
	content.Status = &bigqueryv2.JobStatus{State: "DONE"}
	return &content, nil
}

func (h *jobsGetQueryResultsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	res, err := h.Handle(ctx, &jobsGetQueryResultsRequest{
		server:            server,
		project:           project,
		job:               job,
		maxResults:        parseQueryValueAsInt64(r, maxResults, maxResultsDefaultValue),
		useInt64Timestamp: isFormatOptionsUseInt64Timestamp(r),
	})
	if err != nil {
		errorResponse(ctx, w, errJobInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsGetQueryResultsRequest struct {
	server            *Server
	project           *metadata.Project
	job               *metadata.Job
	useInt64Timestamp bool
	maxResults        int64
}

func (h *jobsGetQueryResultsHandler) Handle(ctx context.Context, r *jobsGetQueryResultsRequest) (*internaltypes.GetQueryResultsResponse, error) {
	response, err := r.job.Wait(ctx)
	if err != nil {
		return nil, err
	}
	rows := internaltypes.Format(response.Schema, response.Rows, r.useInt64Timestamp)

	if r.maxResults == 0 {
		rows = nil
	}

	if r.maxResults != maxResultsDefaultValue {
		rows = rows[:min(int64(len(rows)), r.maxResults)]
	}

	return &internaltypes.GetQueryResultsResponse{
		JobReference: &bigqueryv2.JobReference{
			ProjectId: r.project.ID,
			JobId:     r.job.ID,
		},
		Schema:      response.Schema,
		TotalRows:   response.TotalRows,
		JobComplete: true,
		Rows:        rows,
	}, nil
}

func (h *jobsInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	var job flexibleJob
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &jobsInsertRequest{
		server:  server,
		project: project,
		job:     &job.Job,
	})
	if err != nil {
		errorResponse(ctx, w, errInvalidQuery(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsInsertRequest struct {
	server  *Server
	project *metadata.Project
	job     *bigqueryv2.Job
}

func (h *jobsInsertHandler) tableDefFromQueryResponse(tableID string, response *internaltypes.QueryResponse) (*types.Table, error) {
	columns := []*types.Column{}
	for _, field := range response.Schema.Fields {
		columns = append(columns, types.NewColumnWithSchema(field))
	}
	data := types.Data{}
	for _, row := range response.Rows {
		rowData, err := row.Data()
		if err != nil {
			return nil, err
		}
		data = append(data, rowData)
	}
	return types.NewTableWithSchema(
		&bigqueryv2.Table{
			TableReference: &bigqueryv2.TableReference{
				TableId: tableID,
			},
			Schema: response.Schema,
		},
		data,
	)
}

const (
	gcsEmulatorHostEnvName = "STORAGE_EMULATOR_HOST"
	gcsURIPrefix           = "gs://"
)

func (h *jobsInsertHandler) importFromGCS(ctx context.Context, r *jobsInsertRequest) (*bigqueryv2.Job, error) {
	var opts []option.ClientOption
	if host := os.Getenv(gcsEmulatorHostEnvName); host != "" {
		// When STORAGE_EMULATOR_HOST is set, the storage client automatically
		// detects emulator mode. We only need to add JSON reads and disable auth.
		// Using option.WithEndpoint() can cause double paths with WithJSONReads().
		opts = append(
			opts,
			storage.WithJSONReads(),
			option.WithoutAuthentication(),
		)
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	startTime := time.Now()
	for _, uri := range r.job.Configuration.Load.SourceUris {
		if !strings.HasPrefix(uri, gcsURIPrefix) {
			return nil, fmt.Errorf("load source uri must start with gs://")
		}
		uri = strings.TrimPrefix(uri, gcsURIPrefix)
		paths := strings.Split(uri, "/")
		if len(paths) < 2 {
			return nil, fmt.Errorf("unexpected gcs uri format %s", uri)
		}
		bucketName := paths[0]
		objectPath := strings.Join(paths[1:], "/")
		switch strings.Count(objectPath, "*") {
		case 0:
			reader, err := client.Bucket(bucketName).Object(objectPath).NewReader(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to get gcs object reader for %s: %w", uri, err)
			}
			if err := h.importFromGCSObject(ctx, r, reader); err != nil {
				return nil, err
			}
		case 1:
			splitPath := strings.Split(objectPath, "*")
			prefix := splitPath[0]
			suffix := splitPath[1]
			query := &storage.Query{
				Prefix: prefix,
			}
			query.SetAttrSelection([]string{"Name"})
			it := client.Bucket(bucketName).Objects(ctx, query)
			for {
				attrs, err := it.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return nil, fmt.Errorf("failed to list gcs object for %s: %w", uri, err)
				}
				if strings.HasSuffix(attrs.Name, suffix) {
					reader, err := client.Bucket(bucketName).Object(attrs.Name).NewReader(ctx)
					if err != nil {
						return nil, fmt.Errorf("failed to get gcs object reader for %s: %w", uri, err)
					}
					if err := h.importFromGCSObject(ctx, r, reader); err != nil {
						return nil, err
					}
				}
			}
		default:
			return nil, fmt.Errorf("the number of wildcards in gcs uri must be 0 or 1")
		}
	}
	endTime := time.Now()
	job := r.job
	job.Kind = "bigquery#job"
	job.Configuration.JobType = "LOAD"
	job.SelfLink = fmt.Sprintf(
		"http://%s/bigquery/v2/projects/%s/jobs/%s",
		r.server.httpServer.Addr,
		r.project.ID,
		job.JobReference.JobId,
	)
	job.Status = &bigqueryv2.JobStatus{State: "DONE"}
	job.Statistics = &bigqueryv2.JobStatistics{
		CreationTime: startTime.Unix(),
		StartTime:    startTime.Unix(),
		EndTime:      endTime.Unix(),
	}
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, "")
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.project.AddJob(
		ctx,
		tx.Tx(),
		metadata.NewJob(
			r.server.metaRepo,
			r.project.ID,
			job.JobReference.JobId,
			job,
			nil,
			nil,
		),
	); err != nil {
		return nil, fmt.Errorf("failed to add job: %w", err)
	}
	if !job.Configuration.DryRun {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit job: %w", err)
		}
	}
	return job, nil
}

func (h *jobsInsertHandler) importFromGCSObject(ctx context.Context, r *jobsInsertRequest, reader *storage.Reader) error {
	defer func() {
		_ = reader.Close()
	}()
	job := metadata.NewJob(
		r.server.metaRepo,
		r.project.ID,
		r.job.JobReference.JobId,
		r.job,
		nil,
		nil,
	)
	if err := new(uploadContentHandler).Handle(ctx, &uploadContentRequest{
		server:  r.server,
		project: r.project,
		job:     job,
		reader:  reader,
	}); err != nil {
		return err
	}
	return nil
}

func (h *jobsInsertHandler) exportToGCS(ctx context.Context, r *jobsInsertRequest) (*bigqueryv2.Job, error) {
	var opts []option.ClientOption
	if host := os.Getenv(gcsEmulatorHostEnvName); host != "" {
		// When STORAGE_EMULATOR_HOST is set, the storage client automatically
		// detects emulator mode. We only need to disable auth.
		opts = append(
			opts,
			option.WithoutAuthentication(),
		)
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	startTime := time.Now()
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, "")
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	extract := r.job.Configuration.Extract
	sourceTable := extract.SourceTable
	response, err := r.server.contentRepo.Query(
		ctx,
		tx,
		sourceTable.ProjectId,
		sourceTable.DatasetId,
		fmt.Sprintf("SELECT * FROM `%s`", sourceTable.TableId),
		nil,
	)
	if err != nil {
		return nil, err
	}
	for _, uri := range extract.DestinationUris {
		if !strings.HasPrefix(uri, gcsURIPrefix) {
			return nil, fmt.Errorf("destination uri must start with gs://")
		}
		uri = strings.TrimPrefix(uri, gcsURIPrefix)
		paths := strings.Split(uri, "/")
		if len(paths) < 2 {
			return nil, fmt.Errorf("unexpected gcs uri format %s", uri)
		}
		bucketName := paths[0]
		objectPath := strings.Join(paths[1:], "/")
		bucket := client.Bucket(bucketName)
		_ = bucket.Create(ctx, r.project.ID, nil) // ignore "already exists" error.
		writer := bucket.Object(objectPath).NewWriter(ctx)
		if err := h.exportToGCSWithObject(ctx, response, extract, writer); err != nil {
			return nil, err
		}
	}
	endTime := time.Now()
	job := r.job
	job.Kind = "bigquery#job"
	job.Configuration.JobType = "EXTRACT"
	job.SelfLink = fmt.Sprintf(
		"http://%s/bigquery/v2/projects/%s/jobs/%s",
		r.server.httpServer.Addr,
		r.project.ID,
		job.JobReference.JobId,
	)
	job.Status = &bigqueryv2.JobStatus{State: "DONE"}
	job.Statistics = &bigqueryv2.JobStatistics{
		CreationTime: startTime.Unix(),
		StartTime:    startTime.Unix(),
		EndTime:      endTime.Unix(),
	}
	if err := r.project.AddJob(
		ctx,
		tx.Tx(),
		metadata.NewJob(
			r.server.metaRepo,
			r.project.ID,
			job.JobReference.JobId,
			job,
			nil,
			nil,
		),
	); err != nil {
		return nil, fmt.Errorf("failed to add job: %w", err)
	}
	if !job.Configuration.DryRun {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit job: %w", err)
		}
	}
	return job, nil
}

func (h *jobsInsertHandler) exportToGCSWithObject(ctx context.Context, response *internaltypes.QueryResponse, extract *bigqueryv2.JobConfigurationExtract, writer *storage.Writer) (e error) {
	defer func() {
		if err := writer.Close(); err != nil {
			e = err
		}
	}()
	switch extract.DestinationFormat {
	case "CSV":
		if len(response.Rows) == 0 {
			if _, err := writer.Write(nil); err != nil {
				return fmt.Errorf("failed to empty table data to gcs object: %w", err)
			}
			return nil
		}
		csvWriter := csv.NewWriter(writer)
		var columns []string
		for _, cell := range response.Rows[0].F {
			columns = append(columns, cell.Name)
		}
		if extract.PrintHeader == nil {
			if err := csvWriter.Write(columns); err != nil {
				return fmt.Errorf("failed to encode csv columns: %w", err)
			}
		}
		for _, row := range response.Rows {
			data, err := row.Data()
			if err != nil {
				return fmt.Errorf("failed to get data from table row: %w", err)
			}
			var records []string
			for _, col := range columns {
				value := data[col]
				if value == nil {
					records = append(records, "")
					continue
				}
				if v, ok := value.(string); ok {
					records = append(records, v)
					continue
				}
				jsonValue, err := json.Marshal(value)
				if err != nil {
					return fmt.Errorf("failed to encode row value: %w", err)
				}
				records = append(records, string(jsonValue))
			}
			if err := csvWriter.Write(records); err != nil {
				return fmt.Errorf("failed to encode csv data: %w", err)
			}
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return fmt.Errorf("failed to encode csv data: %w", err)
		}
	case "NEWLINE_DELIMITED_JSON":
		writer.ContentType = "application/json"
		enc := json.NewEncoder(writer)
		for _, row := range response.Rows {
			data, err := row.Data()
			if err != nil {
				return fmt.Errorf("failed to get data from table row: %w", err)
			}
			if err := enc.Encode(data); err != nil {
				return fmt.Errorf("failed to encode table data: %w", err)
			}
		}
	case "PARQUET":
		var opts []parquet.WriterOption
		switch extract.Compression {
		case "GZIP":
			opts = append(opts, parquet.Compression(&parquet.Gzip))
		case "SNAPPY":
			opts = append(opts, parquet.Compression(&parquet.Snappy))
		case "DEFLATE":
			opts = append(opts, parquet.Compression(&parquet.Gzip))
		}
		_ = opts
		fallthrough
	default:
		return fmt.Errorf("failed to export to gcs: unsupported destination format %s", extract.DestinationFormat)
	}
	return nil
}

func (h *jobsInsertHandler) copyFromBigQuery(ctx context.Context, r *jobsInsertRequest, srcTable *bigqueryv2.TableReference) (*bigqueryv2.Job, error) {
	job := r.job
	startTime := time.Now()

	tmpf, err := os.CreateTemp("", "*.jsonl")
	if err != nil {
		return nil, fmt.Errorf("failed to set up temporary file: %w", err)
	}
	defer os.Remove(tmpf.Name())

	enc := json.NewEncoder(tmpf)
	var schema *bigqueryv2.TableSchema

	err = r.server.connMgr.ExecuteWithTransaction(ctx, func(ctx context.Context, tx *connection.Tx) error {
		response, err := r.server.contentRepo.Query(
			ctx,
			tx,
			srcTable.ProjectId,
			srcTable.DatasetId,
			fmt.Sprintf("SELECT * FROM `%s`", srcTable.TableId),
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to execute copy query: %w", err)
		}

		for _, row := range response.Rows {
			rowData, err := row.Data()
			if err != nil {
				return fmt.Errorf("failed to get data from table row: %w", err)
			}

			err = enc.Encode(rowData)
			if err != nil {
				return fmt.Errorf("failed to encode table row as JSON: %w", err)
			}
		}

		schema = response.Schema

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Flush the data to the filesystem and rewind our cursor to the beginning
	// of the file so that all content is readable by the load hanldler
	if err = tmpf.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync temp file: %w", err)
	}

	if _, err = tmpf.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to beginning of file: %w", err)
	}

	// Create a load job rather than passing our copy job into the upload
	// handler. We do this so that we don't have to teach the upload handler
	// how to handle copy jobs (which it really doesn't have to know about
	// otherwise)
	loadJob := &bigqueryv2.Job{
		Configuration: &bigqueryv2.JobConfiguration{
			DryRun: job.Configuration.DryRun,
			Load: &bigqueryv2.JobConfigurationLoad{
				CreateDisposition: job.Configuration.Copy.CreateDisposition,
				DestinationTable:  job.Configuration.Copy.DestinationTable,
				Schema:            schema,
				SourceFormat:      "NEWLINE_DELIMITED_JSON",
				WriteDisposition:  job.Configuration.Copy.WriteDisposition,
			},
		},
	}

	if err = new(uploadContentHandler).Handle(
		ctx,
		&uploadContentRequest{
			server:  r.server,
			project: r.project,
			job: metadata.NewJob(
				r.server.metaRepo,
				r.project.ID,
				r.job.JobReference.JobId,
				loadJob,
				nil,
				nil,
			),
			reader: tmpf,
		},
	); err != nil {
		return nil, fmt.Errorf("failed to import results from source: %w", err)
	}

	job.Kind = "bigquery#job"
	job.Configuration.JobType = "COPY"
	job.SelfLink = fmt.Sprintf(
		"http://%s/bigquery/v2/projects/%s/jobs/%s",
		r.server.httpServer.Addr,
		r.project.ID,
		job.JobReference.JobId,
	)
	job.Status = &bigqueryv2.JobStatus{State: "DONE"}
	job.Statistics = &bigqueryv2.JobStatistics{
		CreationTime: startTime.UnixMilli(),
		StartTime:    startTime.UnixMilli(),
		EndTime:      time.Now().UnixMilli(),
	}

	if !job.Configuration.DryRun {
		// Not great!
		//
		// We need a transaction in order to persist our job
		// information.  Unfortunately the transaction that we were
		// using above got closed (if it stayed open it would conflict
		// with the upload handler) so now we have to create a new one.
		// Even worse, committing the transaction closes the connection
		// so we need a new one of those too.
		//
		// I think this is still presenting correct behavior to the
		// user because if the upload handler failed, we'd report it.
		err = r.server.connMgr.ExecuteWithTransaction(ctx, func(ctx context.Context, tx *connection.Tx) error {
			err = r.project.AddJob(
				ctx,
				tx.Tx(),
				metadata.NewJob(
					r.server.metaRepo,
					r.project.ID,
					job.JobReference.JobId,
					job,
					nil,
					nil,
				),
			)

			if err != nil {
				err = fmt.Errorf("failed to add job: %w", err)
			}

			return err
		})
		if err != nil {
			return nil, err
		}
	}

	return job, nil
}

func (h *jobsInsertHandler) Handle(ctx context.Context, r *jobsInsertRequest) (*bigqueryv2.Job, error) {
	job := r.job
	if job.Configuration == nil {
		return nil, fmt.Errorf("unspecified job configuration")
	}
	if job.Configuration.Query == nil {
		if job.Configuration.Load != nil && len(job.Configuration.Load.SourceUris) != 0 {
			// load from google cloud storage
			job, err := h.importFromGCS(ctx, r)
			if err != nil {
				return nil, fmt.Errorf("failed to import from gcs: %w", err)
			}
			return job, nil
		} else if job.Configuration.Extract != nil && len(job.Configuration.Extract.DestinationUris) != 0 {
			job, err := h.exportToGCS(ctx, r)
			if err != nil {
				return nil, fmt.Errorf("failed to export to gcs: %w", err)
			}
			return job, nil
		} else if job.Configuration.Copy != nil {
			copyConfig := job.Configuration.Copy
			var sourceTable *bigqueryv2.TableReference

			if copyConfig.SourceTable != nil {
				sourceTable = copyConfig.SourceTable
			} else if len(copyConfig.SourceTables) == 0 {
				return nil, fmt.Errorf("cannot copy without a valid source table")
			} else if len(copyConfig.SourceTables) == 1 {
				sourceTable = copyConfig.SourceTables[0]
			} else if len(copyConfig.SourceTables) > 1 {
				return nil, fmt.Errorf("copy from multiple source tables not supported")
			}

			if copyConfig.DestinationTable == nil {
				return nil, fmt.Errorf("cannot copy without a valid destination table")
			}

			return h.copyFromBigQuery(ctx, r, sourceTable)
		}

		return nil, fmt.Errorf("unspecified job configuration query")
	}
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, "")
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	hasDestinationTable := job.Configuration.Query.DestinationTable != nil
	startTime := time.Now()
	response, jobErr := r.server.contentRepo.Query(
		ctx,
		tx,
		r.project.ID,
		"",
		job.Configuration.Query.Query,
		job.Configuration.Query.QueryParameters,
	)
	endTime := time.Now()
	if job.JobReference == nil {
		job.JobReference = &bigqueryv2.JobReference{
			ProjectId: r.project.ID,
			JobId:     randomID(),
		}
	} else if job.JobReference.JobId == "" {
		job.JobReference.JobId = randomID() // generate job id
	}
	if jobErr == nil {
		if hasDestinationTable {
			// insert results to destination table
			tableRef := job.Configuration.Query.DestinationTable
			tableDef, err := h.tableDefFromQueryResponse(tableRef.TableId, response)
			if err != nil {
				return nil, err
			}
			destinationDataset, err := r.server.metaRepo.FindDatasetWithConnection(ctx, tx.Tx(), tableRef.ProjectId, tableRef.DatasetId)
			if err != nil {
				return nil, fmt.Errorf("failed to find destination dataset: %s, err: %s", tableRef.DatasetId, err)
			}
			destinationTable, err := r.server.metaRepo.FindTableWithConnection(ctx, tx.Tx(), destinationDataset.ProjectID, destinationDataset.ID, tableRef.TableId)
			if err != nil {
				return nil, fmt.Errorf("failed to find destination table: %w", err)
			}
			destinationTableExists := destinationTable != nil
			if !destinationTableExists {
				_, err := createTableMetadata(ctx, tx, r.server, r.project, destinationDataset, tableDef.ToBigqueryV2(r.project.ID, tableRef.DatasetId))
				if err != nil {
					return nil, fmt.Errorf("failed to create table: %w", err)
				}
				serverErr := r.server.contentRepo.CreateTable(ctx, tx, tableDef.ToBigqueryV2(r.project.ID, tableRef.DatasetId))
				if serverErr != nil {
					return nil, fmt.Errorf("failed to create table: %w", serverErr)
				}
			}

			if err := r.server.contentRepo.AddTableData(ctx, tx, tableRef.ProjectId, tableRef.DatasetId, tableDef, job.Configuration.Query.WriteDisposition == "WRITE_TRUNCATE"); err != nil {
				return nil, fmt.Errorf("failed to add table data: %w", err)
			}
		} else if response.TotalRows > 0 {
			if err := h.addQueryResultToDynamicDestinationTable(ctx, tx, r, response); err != nil {
				return nil, fmt.Errorf("failed to add query result to dynamic destination table: %w", err)
			}
		}
	}
	job.Kind = "bigquery#job"
	job.Configuration.JobType = "QUERY"
	job.Configuration.Query.Priority = "INTERACTIVE"
	job.SelfLink = fmt.Sprintf(
		"http://%s/bigquery/v2/projects/%s/jobs/%s",
		r.server.httpServer.Addr,
		r.project.ID,
		job.JobReference.JobId,
	)
	status := &bigqueryv2.JobStatus{State: "DONE"}
	if jobErr != nil {
		internalErr := errJobInternalError(jobErr.Error())
		status.ErrorResult = internalErr.ErrorProto()
		status.Errors = []*bigqueryv2.ErrorProto{internalErr.ErrorProto()}
	}
	job.Status = status
	var totalBytes int64
	if response != nil {
		totalBytes = response.TotalBytes
	}
	job.Statistics = &bigqueryv2.JobStatistics{
		Query: &bigqueryv2.JobStatistics2{
			CacheHit:            false,
			StatementType:       "SELECT",
			TotalBytesBilled:    totalBytes,
			TotalBytesProcessed: totalBytes,
		},
		CreationTime:        startTime.Unix(),
		StartTime:           startTime.Unix(),
		EndTime:             endTime.Unix(),
		TotalBytesProcessed: totalBytes,
	}
	if err := r.project.AddJob(
		ctx,
		tx.Tx(),
		metadata.NewJob(
			r.server.metaRepo,
			r.project.ID,
			job.JobReference.JobId,
			job,
			response,
			jobErr,
		),
	); err != nil {
		return nil, fmt.Errorf("failed to add job: %w", err)
	}
	if !job.Configuration.DryRun {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit job: %w", err)
		}
		if response != nil && response.ChangedCatalog.Changed() {
			if err := syncCatalog(ctx, r.server, response.ChangedCatalog); err != nil {
				return nil, err
			}
		}
	}

	return job, nil
}

func syncCatalog(ctx context.Context, server *Server, cat *googlesqlite.ChangedCatalog) error {
	for _, table := range cat.Table.Added {
		if err := addTableMetadata(ctx, server, table); err != nil {
			return err
		}
	}
	for _, table := range cat.Table.Deleted {
		if err := deleteTableMetadata(ctx, server, table); err != nil {
			return err
		}
	}
	return nil
}

func addTableMetadata(ctx context.Context, server *Server, spec *googlesqlite.TableSpec) error {
	if len(spec.NamePath) != 3 {
		return fmt.Errorf("unexpected table name path: %v", spec.NamePath)
	}
	projectID := spec.NamePath[0]
	datasetID := spec.NamePath[1]
	tableID := spec.NamePath[2]
	project, err := server.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		return err
	}
	dataset, err := project.Dataset(ctx, datasetID)
	if err != nil {
		return err
	}
	if dataset == nil {
		return fmt.Errorf("dataset %s is not found", datasetID)
	}
	fields := make([]*bigqueryv2.TableFieldSchema, 0, len(spec.Columns))
	for _, column := range spec.Columns {
		googlesqlType, err := column.Type.ToGoogleSQLType()
		if err != nil {
			return err
		}
		fields = append(fields, types.TableFieldSchemaFromType(column.Name, googlesqlType))
	}
	conn := connectionFromContext(ctx).ConfigureScope(projectID, datasetID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	table := &bigqueryv2.Table{
		TableReference: &bigqueryv2.TableReference{
			ProjectId: projectID,
			DatasetId: datasetID,
			TableId:   tableID,
		},
		Schema: &bigqueryv2.TableSchema{Fields: fields},
	}
	if spec.IsView {
		table.View = &bigqueryv2.ViewDefinition{
			Query: spec.Query,
		}
	}
	if _, err := createTableMetadata(ctx, tx, server, project, dataset, table); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func deleteTableMetadata(ctx context.Context, server *Server, spec *googlesqlite.TableSpec) error {
	if len(spec.NamePath) != 3 {
		return fmt.Errorf("unexpected table name path: %v", spec.NamePath)
	}
	projectID := spec.NamePath[0]
	datasetID := spec.NamePath[1]
	tableID := spec.NamePath[2]
	project, err := server.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		return err
	}
	dataset, err := project.Dataset(ctx, datasetID)
	if err != nil {
		return err
	}
	if dataset == nil {
		return fmt.Errorf("dataset %s is not found", datasetID)
	}
	table, err := dataset.Table(ctx, tableID)
	if err != nil {
		return err
	}
	conn := connectionFromContext(ctx).ConfigureScope(projectID, datasetID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	if err := table.Delete(ctx, tx.Tx()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (h *jobsInsertHandler) addQueryResultToDynamicDestinationTable(ctx context.Context, tx *connection.Tx, r *jobsInsertRequest, response *internaltypes.QueryResponse) error {
	projectID := r.project.ID
	jobID := r.job.JobReference.JobId
	datasetID := "ds_" + jobID
	tableID := jobID

	tableDef, err := h.tableDefFromQueryResponse(tableID, response)
	if err != nil {
		return err
	}
	tableDef.SetupMetadata(projectID, datasetID)
	table := metadata.NewTable(r.server.metaRepo, projectID, datasetID, tableID, tableDef.Metadata)
	dataset := metadata.NewDataset(
		r.server.metaRepo,
		projectID,
		datasetID,
		&bigqueryv2.Dataset{
			Id: fmt.Sprintf("%s:%s", projectID, datasetID),
			DatasetReference: &bigqueryv2.DatasetReference{
				ProjectId: projectID,
				DatasetId: datasetID,
			},
		},
	)
	if err := r.project.AddDataset(ctx, tx.Tx(), dataset); err != nil {
		return err
	}
	if err := r.server.metaRepo.AddTable(ctx, tx.Tx(), table); err != nil {
		return err
	}
	if err := r.server.contentRepo.CreateTable(ctx, tx, tableDef.ToBigqueryV2(projectID, datasetID)); err != nil {
		return err
	}
	if err := r.server.contentRepo.AddTableData(ctx, tx, projectID, datasetID, tableDef, false); err != nil {
		return fmt.Errorf("failed to add table data: %w", err)
	}
	r.job.Configuration.Query.DestinationTable = &bigqueryv2.TableReference{
		DatasetId: datasetID,
		ProjectId: projectID,
		TableId:   tableID,
	}
	return nil
}

func (h *jobsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	res, err := h.Handle(ctx, &jobsListRequest{
		server:  server,
		project: project,
	})
	if err != nil {
		errorResponse(ctx, w, errJobInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsListRequest struct {
	server  *Server
	project *metadata.Project
}

func (h *jobsListHandler) Handle(ctx context.Context, r *jobsListRequest) (*bigqueryv2.JobList, error) {
	jobsList := []*bigqueryv2.JobListJobs{}
	jobs, err := r.project.FetchJobs(ctx)
	if err != nil {
		return nil, err
	}
	for _, job := range jobs {
		content := job.Content()
		jobsList = append(jobsList, &bigqueryv2.JobListJobs{
			Id:           content.Id,
			JobReference: content.JobReference,
			Kind:         content.Kind,
			Statistics:   content.Statistics,
			Status:       content.Status,
			UserEmail:    content.UserEmail,
		})
	}
	return &bigqueryv2.JobList{Jobs: jobsList}, nil
}

func (h *jobsQueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	var req flexibleQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	useInt64Timestamp := false
	if options := req.QueryRequest.FormatOptions; options != nil {
		useInt64Timestamp = options.UseInt64Timestamp
	}
	useInt64Timestamp = useInt64Timestamp || isFormatOptionsUseInt64Timestamp(r)
	res, err := h.Handle(ctx, &jobsQueryRequest{
		server:            server,
		project:           project,
		queryRequest:      &req.QueryRequest,
		useInt64Timestamp: useInt64Timestamp,
	})
	if err != nil {
		errorResponse(ctx, w, errInvalidQuery(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsQueryRequest struct {
	server            *Server
	project           *metadata.Project
	queryRequest      *bigqueryv2.QueryRequest
	useInt64Timestamp bool
}

func (h *jobsQueryHandler) Handle(ctx context.Context, r *jobsQueryRequest) (*internaltypes.QueryResponse, error) {
	var datasetID string
	if r.queryRequest.DefaultDataset != nil {
		datasetID = r.queryRequest.DefaultDataset.DatasetId
	}
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, datasetID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	response, err := r.server.contentRepo.Query(
		ctx,
		tx,
		r.project.ID,
		datasetID,
		r.queryRequest.Query,
		r.queryRequest.QueryParameters,
	)
	if err != nil {
		return nil, err
	}
	if !r.queryRequest.DryRun {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		if response.ChangedCatalog.Changed() {
			if err := syncCatalog(ctx, r.server, response.ChangedCatalog); err != nil {
				return nil, err
			}
		}
	}
	jobID := r.queryRequest.RequestId
	if jobID == "" {
		jobID = randomID() // generate job id
	}
	response.Rows = internaltypes.Format(response.Schema, response.Rows, r.useInt64Timestamp)
	response.JobReference = &bigqueryv2.JobReference{
		ProjectId: r.project.ID,
		JobId:     jobID,
	}
	return response, nil
}

func (h *modelsDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	model := modelFromContext(ctx)
	if err := h.Handle(ctx, &modelsDeleteRequest{
		server:  server,
		project: project,
		dataset: dataset,
		model:   model,
	}); err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
	}
}

type modelsDeleteRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	model   *metadata.Model
}

func (h *modelsDeleteHandler) Handle(ctx context.Context, r *modelsDeleteRequest) error {
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, r.dataset.ID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.dataset.DeleteModel(ctx, tx.Tx(), r.model.ID); err != nil {
		return fmt.Errorf("failed to delete model: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (h *modelsGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	model := modelFromContext(ctx)
	res, err := h.Handle(ctx, &modelsGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
		model:   model,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type modelsGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	model   *metadata.Model
}

func (h *modelsGetHandler) Handle(ctx context.Context, r *modelsGetRequest) (*bigqueryv2.Model, error) {
	return &bigqueryv2.Model{
		BestTrialId:             0,
		CreationTime:            0,
		DefaultTrialId:          0,
		Description:             "",
		EncryptionConfiguration: nil,
		Etag:                    "",
		ExpirationTime:          0,
		FeatureColumns:          nil,
		FriendlyName:            "",
		HparamSearchSpaces:      nil,
		HparamTrials:            nil,
		LabelColumns:            nil,
		Labels:                  nil,
		LastModifiedTime:        0,
		Location:                "",
		ModelReference:          nil,
		ModelType:               "",
		OptimalTrialIds:         nil,
		TrainingRuns:            nil,
	}, nil
}

func (h *modelsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &modelsListRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type modelsListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *modelsListHandler) Handle(ctx context.Context, r *modelsListRequest) (*bigqueryv2.ListModelsResponse, error) {
	response := []*bigqueryv2.Model{}
	models, err := r.dataset.Models(ctx)
	if err != nil {
		return nil, err
	}
	for _, m := range models {
		_ = m
		response = append(response, &bigqueryv2.Model{})
	}
	return &bigqueryv2.ListModelsResponse{
		Models: response,
	}, nil
}

func (h *modelsPatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	model := modelFromContext(ctx)
	res, err := h.Handle(ctx, &modelsPatchRequest{
		server:  server,
		project: project,
		dataset: dataset,
		model:   model,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type modelsPatchRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	model   *metadata.Model
}

func (h *modelsPatchHandler) Handle(ctx context.Context, r *modelsPatchRequest) (*bigqueryv2.Model, error) {
	return &bigqueryv2.Model{}, nil
}

func (h *projectsGetServiceAccountHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	res, err := h.Handle(ctx, &projectsGetServiceAccountRequest{
		server:  server,
		project: project,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type projectsGetServiceAccountRequest struct {
	server  *Server
	project *metadata.Project
}

func (h *projectsGetServiceAccountHandler) Handle(ctx context.Context, r *projectsGetServiceAccountRequest) (*bigqueryv2.GetServiceAccountResponse, error) {
	return &bigqueryv2.GetServiceAccountResponse{}, nil
}

func (h *projectsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &projectsListRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type projectsListRequest struct {
	server *Server
}

func (h *projectsListHandler) Handle(ctx context.Context, r *projectsListRequest) (*bigqueryv2.ProjectList, error) {
	projects, err := r.server.metaRepo.FindAllProjects(ctx)
	if err != nil {
		return nil, err
	}

	projectList := []*bigqueryv2.ProjectListProjects{}
	for _, p := range projects {
		projectList = append(projectList, &bigqueryv2.ProjectListProjects{
			Id: p.ID,
		})
	}
	return &bigqueryv2.ProjectList{
		Projects: projectList,
	}, nil
}

func (h *routinesDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	routine := routineFromContext(ctx)
	if err := h.Handle(ctx, &routinesDeleteRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: routine,
	}); err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
}

type routinesDeleteRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *metadata.Routine
}

func (h *routinesDeleteHandler) Handle(ctx context.Context, r *routinesDeleteRequest) error {
	return fmt.Errorf("unsupported bigquery.routines.delete")
}

func (h *routinesGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	routine := routineFromContext(ctx)
	res, err := h.Handle(ctx, &routinesGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: routine,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *metadata.Routine
}

func (h *routinesGetHandler) Handle(ctx context.Context, r *routinesGetRequest) (*bigqueryv2.Routine, error) {
	return nil, fmt.Errorf("unsupported bigquery.routines.get")
}

func (h *routinesInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	var routine bigqueryv2.Routine
	if err := json.NewDecoder(r.Body).Decode(&routine); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &routinesInsertRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: &routine,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesInsertRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *bigqueryv2.Routine
}

func (h *routinesInsertHandler) Handle(ctx context.Context, r *routinesInsertRequest) (*bigqueryv2.Routine, error) {
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, r.dataset.ID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.server.contentRepo.AddRoutineByMetaData(ctx, tx, r.routine); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return r.routine, nil
}

func (h *routinesListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &routinesListRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *routinesListHandler) Handle(ctx context.Context, r *routinesListRequest) (*bigqueryv2.ListRoutinesResponse, error) {
	var response []*bigqueryv2.Routine
	routines, err := r.dataset.Routines(ctx)
	if err != nil {
		return nil, err
	}
	for _, routine := range routines {
		_ = routine
		response = append(response, &bigqueryv2.Routine{})
	}
	return &bigqueryv2.ListRoutinesResponse{
		Routines: response,
	}, fmt.Errorf("unsupported bigquery.routines.list")
}

func (h *routinesUpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	routine := routineFromContext(ctx)
	res, err := h.Handle(ctx, &routinesUpdateRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: routine,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesUpdateRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *metadata.Routine
}

func (h *routinesUpdateHandler) Handle(ctx context.Context, r *routinesUpdateRequest) (*bigqueryv2.Routine, error) {
	return nil, fmt.Errorf("unsupported bigquery.routines.update")
}

func (h *rowAccessPoliciesGetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesGetIamPolicyRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesGetIamPolicyRequest struct {
	server *Server
}

func (h *rowAccessPoliciesGetIamPolicyHandler) Handle(ctx context.Context, r *rowAccessPoliciesGetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.getIamPolicy")
}

func (h *rowAccessPoliciesListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesListRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *rowAccessPoliciesListHandler) Handle(ctx context.Context, r *rowAccessPoliciesListRequest) (*bigqueryv2.ListRowAccessPoliciesResponse, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.list")
}

func (h *rowAccessPoliciesSetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesSetIamPolicyRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesSetIamPolicyRequest struct {
	server *Server
}

func (h *rowAccessPoliciesSetIamPolicyHandler) Handle(ctx context.Context, r *rowAccessPoliciesSetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.setIamPolicy")
}

func (h *rowAccessPoliciesTestIamPermissionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesTestIamPermissionsRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesTestIamPermissionsRequest struct {
	server *Server
}

func (h *rowAccessPoliciesTestIamPermissionsHandler) Handle(ctx context.Context, r *rowAccessPoliciesTestIamPermissionsRequest) (*bigqueryv2.TestIamPermissionsResponse, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.testIamPermissions")
}

func (h *tabledataInsertAllHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	var req bigqueryv2.TableDataInsertAllRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &tabledataInsertAllRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
		req:     &req,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tabledataInsertAllRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
	req     *bigqueryv2.TableDataInsertAllRequest
}

func normalizeInsertValue(v interface{}, field *bigqueryv2.TableFieldSchema) (interface{}, error) {
	rv := reflect.ValueOf(v)
	kind := rv.Kind()
	if field.Mode == "REPEATED" {
		if kind != reflect.Slice && kind != reflect.Array {
			return nil, fmt.Errorf("invalid value type %T for ARRAY column", v)
		}
		values := make([]interface{}, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			value, err := normalizeInsertValue(rv.Index(i).Interface(), &bigqueryv2.TableFieldSchema{
				Fields: field.Fields,
			})
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}
	if kind == reflect.Map {
		fieldMap := map[string]*bigqueryv2.TableFieldSchema{}
		for _, f := range field.Fields {
			fieldMap[f.Name] = f
		}
		columnNameToValueMap := map[string]interface{}{}
		for _, key := range rv.MapKeys() {
			if key.Kind() != reflect.String {
				return nil, fmt.Errorf("invalid value type %s for STRUCT column", key.Kind())
			}
			columnName := key.Interface().(string)
			value, err := normalizeInsertValue(rv.MapIndex(key).Interface(), fieldMap[columnName])
			if err != nil {
				return nil, err
			}
			columnNameToValueMap[columnName] = value
		}
		fields := make([]map[string]interface{}, 0, len(fieldMap))
		for _, f := range field.Fields {
			value, exists := columnNameToValueMap[f.Name]
			if !exists {
				return nil, fmt.Errorf("failed to find value from %s", f.Name)
			}
			fields = append(fields, map[string]interface{}{f.Name: value})
		}
		return fields, nil
	}
	return v, nil
}

func (h *tabledataInsertAllHandler) Handle(ctx context.Context, r *tabledataInsertAllRequest) (*bigqueryv2.TableDataInsertAllResponse, error) {
	content, err := r.table.Content()
	if err != nil {
		return nil, err
	}
	data := types.Data{}
	for _, row := range r.req.Rows {
		rowData := map[string]interface{}{}
		for k, v := range row.Json {
			rowData[k] = v
		}
		data = append(data, rowData)
	}

	// Validate data against schema before insert
	validationErrors := types.ValidateDataAgainstSchema(content.Schema, data)
	if len(validationErrors) > 0 {
		return buildInsertErrorsResponse(validationErrors, len(data)), nil
	}

	tableDef, err := types.NewTableWithSchema(content, data)
	if err != nil {
		return nil, err
	}
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, r.dataset.ID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.server.contentRepo.AddTableData(ctx, tx, r.project.ID, r.dataset.ID, tableDef, false); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &bigqueryv2.TableDataInsertAllResponse{}, nil
}

// buildInsertErrorsResponse builds a BigQuery-compatible error response for insertAll validation errors.
// When any row has an unknown field, the entire batch is rejected:
// - Invalid rows get an "invalid" error with the unknown field name
// - Valid rows get a "stopped" error (they weren't processed because the batch failed)
func buildInsertErrorsResponse(errors []types.FieldValidationError, totalRows int) *bigqueryv2.TableDataInsertAllResponse {
	invalidRows := make(map[int]bool)
	var insertErrors []*bigqueryv2.TableDataInsertAllResponseInsertErrors

	// Add "invalid" errors for rows with unknown fields
	for _, e := range errors {
		invalidRows[e.RowIndex] = true
		insertErrors = append(insertErrors, &bigqueryv2.TableDataInsertAllResponseInsertErrors{
			Index: int64(e.RowIndex),
			Errors: []*bigqueryv2.ErrorProto{{
				DebugInfo:       "",
				Reason:          "invalid",
				Location:        e.FieldName,
				Message:         fmt.Sprintf("no such field: %s.", e.FieldName),
				ForceSendFields: []string{"DebugInfo"}, // Ensure DebugInfo is included even when empty
			}},
			ForceSendFields: []string{"Index"}, // Ensure Index is included even when 0
		})
	}

	// Add "stopped" errors for valid rows (they weren't inserted because batch failed)
	for i := 0; i < totalRows; i++ {
		if !invalidRows[i] {
			insertErrors = append(insertErrors, &bigqueryv2.TableDataInsertAllResponseInsertErrors{
				Index:           int64(i),
				ForceSendFields: []string{"Index"}, // Ensure Index is included even when 0
				Errors: []*bigqueryv2.ErrorProto{{
					Reason:          "stopped",
					Location:        "",
					DebugInfo:       "",
					Message:         "",
					ForceSendFields: []string{"Location", "DebugInfo", "Message"}, // Ensure all empty fields are included
				}},
			})
		}
	}

	return &bigqueryv2.TableDataInsertAllResponse{InsertErrors: insertErrors}
}

func (h *tabledataListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &tabledataListRequest{
		server:            server,
		project:           project,
		dataset:           dataset,
		table:             table,
		useInt64Timestamp: isFormatOptionsUseInt64Timestamp(r),
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tabledataListRequest struct {
	server            *Server
	project           *metadata.Project
	dataset           *metadata.Dataset
	table             *metadata.Table
	useInt64Timestamp bool
}

func (h *tabledataListHandler) Handle(ctx context.Context, r *tabledataListRequest) (*internaltypes.TableDataList, error) {
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, r.dataset.ID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	response, err := r.server.contentRepo.Query(
		ctx,
		tx,
		r.project.ID,
		r.dataset.ID,
		fmt.Sprintf("SELECT * FROM `%s`", r.table.ID),
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &internaltypes.TableDataList{
		Rows:      internaltypes.Format(response.Schema, response.Rows, r.useInt64Timestamp),
		TotalRows: response.TotalRows,
	}, nil
}

func (h *tablesDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	if err := h.Handle(ctx, &tablesDeleteRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	}); err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
	}
}

type tablesDeleteRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *tablesDeleteHandler) Handle(ctx context.Context, r *tablesDeleteRequest) error {
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, r.dataset.ID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	// delete table metadata
	if err := r.table.Delete(ctx, tx.Tx()); err != nil {
		return err
	}
	// delete table
	if err := r.server.contentRepo.DeleteTables(
		ctx,
		tx,
		r.project.ID,
		r.dataset.ID,
		[]*metadata.Table{r.table},
	); err != nil {
		return fmt.Errorf("failed to delete table %s: %w", r.table.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (h *tablesGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &tablesGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *tablesGetHandler) Handle(ctx context.Context, r *tablesGetRequest) (*bigqueryv2.Table, error) {
	table, err := r.table.Content()
	if err != nil {
		return nil, fmt.Errorf("failed to get table content: %w", err)
	}
	return table, nil
}

func (h *tablesGetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	var req bigqueryv2.GetIamPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &tablesGetIamPolicyRequest{
		server: server,
		req:    &req,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesGetIamPolicyRequest struct {
	server *Server
	req    *bigqueryv2.GetIamPolicyRequest
}

func (h *tablesGetIamPolicyHandler) Handle(ctx context.Context, r *tablesGetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("bigquery.tables.getIamPolicy")
}

func (h *tablesInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var table bigqueryv2.Table
	if err := json.NewDecoder(r.Body).Decode(&table); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}

	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &tablesInsertRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   &table,
	})
	if err != nil {
		errorResponse(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesInsertRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *bigqueryv2.Table
}

func createTableMetadata(ctx context.Context, tx *connection.Tx, server *Server, project *metadata.Project, dataset *metadata.Dataset, table *bigqueryv2.Table) (*bigqueryv2.Table, *ServerError) {
	now := time.Now().UnixMilli()
	table.Id = fmt.Sprintf("%s:%s.%s", project.ID, dataset.ID, table.TableReference.TableId)
	table.CreationTime = now
	table.LastModifiedTime = uint64(now)
	table.Type = string(internaltypes.DefaultTableType) // TODO: need to handle other table types
	if table.View != nil {
		table.Type = string(internaltypes.ViewTableType)
		table.View.Query = strings.TrimRight(table.View.Query, contentdata.ViewQueryEndCutset)
	}
	table.Kind = "bigquery#table"
	table.SelfLink = fmt.Sprintf(
		"http://%s/bigquery/v2/projects/%s/datasets/%s/tables/%s",
		server.httpServer.Addr,
		project.ID,
		dataset.ID,
		table.TableReference.TableId,
	)
	encodedTableData, err := json.Marshal(table)
	if err != nil {
		return nil, errInternalError(err.Error())
	}
	var tableMetadata map[string]interface{}
	if err := json.Unmarshal(encodedTableData, &tableMetadata); err != nil {
		return nil, errInternalError(err.Error())
	}
	if err := dataset.AddTable(
		ctx,
		tx.Tx(),
		metadata.NewTable(
			server.metaRepo,
			project.ID,
			dataset.ID,
			table.TableReference.TableId,
			tableMetadata,
		),
	); err != nil {
		if errors.Is(err, metadata.ErrDuplicatedTable) {
			return nil, errDuplicate(err.Error())
		}
		return nil, errInternalError(err.Error())
	}
	return table, nil
}

func (h *tablesInsertHandler) Handle(ctx context.Context, r *tablesInsertRequest) (*bigqueryv2.Table, *ServerError) {
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, r.dataset.ID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, errInternalError(err.Error())
	}
	defer tx.RollbackIfNotCommitted()

	if r.table.View != nil {
		schema, err := r.server.contentRepo.CreateView(ctx, tx, r.table)
		if err != nil {
			if strings.HasSuffix(err.Error(), "already exists (1)") {
				return nil, errDuplicate(err.Error())
			}
			return nil, errInvalidQuery(err.Error())
		}
		r.table.Schema = schema
	} else if r.table.Schema != nil {
		if err := r.server.contentRepo.CreateTable(ctx, tx, r.table); err != nil {
			if strings.HasSuffix(err.Error(), "already exists (1)") {
				return nil, errDuplicate(err.Error())
			}
			return nil, errInvalidQuery(err.Error())
		}
	}
	table, serverErr := createTableMetadata(ctx, tx, r.server, r.project, r.dataset, r.table)
	if serverErr != nil {
		return nil, serverErr
	}
	if err := tx.Commit(); err != nil {
		return nil, errInternalError(fmt.Errorf("failed to commit table: %w", err).Error())
	}
	return table, nil
}

func (h *tablesListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &tablesListRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *tablesListHandler) Handle(ctx context.Context, r *tablesListRequest) (*bigqueryv2.TableList, error) {
	var response []*bigqueryv2.TableListTables
	tables, err := r.dataset.Tables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to find tables in dataset: %w", err)
	}
	for _, table := range tables {
		content, err := table.Content()
		if err != nil {
			return nil, fmt.Errorf("failed to get table metadata from %s: %w", table.ID, err)
		}
		response = append(response, &bigqueryv2.TableListTables{
			Clustering:        content.Clustering,
			CreationTime:      content.CreationTime,
			ExpirationTime:    content.ExpirationTime,
			FriendlyName:      content.FriendlyName,
			Id:                content.Id,
			Kind:              content.Kind,
			Labels:            content.Labels,
			RangePartitioning: content.RangePartitioning,
			TableReference:    content.TableReference,
			TimePartitioning:  content.TimePartitioning,
			Type:              content.Type,
		})
	}
	return &bigqueryv2.TableList{
		Tables:     response,
		TotalItems: int64(len(tables)),
	}, nil
}

func (h *tablesPatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	var newTable map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&newTable); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}

	if _, found := newTable["schema"]; found {
		errorResponse(ctx, w, errInvalid("schema updates unsupported"))
		return
	}

	res, err := h.Handle(ctx, &tablesPatchRequest{
		server:           server,
		project:          project,
		dataset:          dataset,
		table:            table,
		newTableMetadata: newTable,
	})

	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)

}

type tablesPatchRequest struct {
	server           *Server
	project          *metadata.Project
	dataset          *metadata.Dataset
	table            *metadata.Table
	newTableMetadata map[string]interface{}
}

func (h *tablesPatchHandler) Handle(ctx context.Context, r *tablesPatchRequest) (*bigqueryv2.Table, error) {
	conn := connectionFromContext(ctx).ConfigureScope(r.project.ID, r.dataset.ID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.table.Update(ctx, tx.Tx(), r.newTableMetadata); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	table, err := r.table.Content()
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (h *tablesSetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &tablesSetIamPolicyRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesSetIamPolicyRequest struct {
	server *Server
}

func (h *tablesSetIamPolicyHandler) Handle(ctx context.Context, r *tablesSetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("unsupported bigquery.tables.setIamPolicy")
}

func (h *tablesTestIamPermissionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &tablesTestIamPermissionsRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesTestIamPermissionsRequest struct {
	server *Server
}

func (h *tablesTestIamPermissionsHandler) Handle(ctx context.Context, r *tablesTestIamPermissionsRequest) (*bigqueryv2.TestIamPermissionsResponse, error) {
	return nil, fmt.Errorf("unsupported bigquery.tables.testIamPermissions")
}

func (h *tablesUpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &tablesUpdateRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesUpdateRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *tablesUpdateHandler) Handle(ctx context.Context, r *tablesUpdateRequest) (*bigqueryv2.Table, error) {
	return nil, fmt.Errorf("unsupported bigquery.tables.update")
}

type defaultHandler struct{}

func (h *defaultHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	errorResponse(ctx, w, errInternalError(fmt.Sprintf("unexpected request path: %s", html.EscapeString(r.URL.Path))))
}
