package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/option"
)

// TestResumableUploadURLUsesHostHeader tests that the resumable upload endpoint
// returns a Location header that uses the Host header from the request, rather
// than the server's bind address (which is often 0.0.0.0).
// This is particularly important when the server is behind a NAT or in a container.
func TestResumableUploadURLUsesHostHeader(t *testing.T) {
	const (
		projectID = "test-project"
		datasetID = "test-dataset"
		tableID   = "test-table"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}

	// Create a test dataset and table
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
					types.NewTable(
						tableID,
						[]*types.Column{
							types.NewColumn("id", types.INT64),
							types.NewColumn("name", types.STRING),
						},
						nil,
					),
				),
			),
		),
	); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Close()
	}()

	// Test various Host header values
	testCases := []struct {
		name       string
		hostHeader string
	}{
		{
			name:       "localhost with port",
			hostHeader: "localhost:9050",
		},
		{
			name:       "IP address with port",
			hostHeader: "127.0.0.1:9050",
		},
		{
			name:       "hostname",
			hostHeader: "bigquery-emulator.local",
		},
		{
			name:       "hostname with port",
			hostHeader: "bigquery-emulator.local:8080",
		},
		{
			name:       "container hostname",
			hostHeader: "bigquery-emulator:9050",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a resumable upload request
			jobID := "test-job-" + strings.ReplaceAll(tc.name, " ", "-")
			uploadJob := map[string]interface{}{
				"jobReference": map[string]interface{}{
					"projectId": projectID,
					"jobId":     jobID,
				},
				"configuration": map[string]interface{}{
					"load": map[string]interface{}{
						"destinationTable": map[string]interface{}{
							"projectId": projectID,
							"datasetId": datasetID,
							"tableId":   tableID,
						},
						"sourceFormat": "NEWLINE_DELIMITED_JSON",
					},
				},
			}

			body, err := json.Marshal(uploadJob)
			if err != nil {
				t.Fatalf("failed to marshal upload job: %v", err)
			}

			// Make a POST request to the resumable upload endpoint with a specific Host header
			url := fmt.Sprintf("%s/upload/bigquery/v2/projects/%s/jobs?uploadType=resumable", testServer.URL, projectID)
			req, err := http.NewRequest("POST", url, bytes.NewReader(body))
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}

			// Set the Host header to the test value
			req.Host = tc.hostHeader
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("failed to make request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected status 200, got %d", resp.StatusCode)
			}

			// Check that the Location header uses the Host header value
			location := resp.Header.Get("Location")
			if location == "" {
				t.Fatal("Location header is empty")
			}

			// The Location should start with "http://" followed by the Host header value
			expectedPrefix := fmt.Sprintf("http://%s/upload/bigquery/v2/projects/%s/jobs?uploadType=resumable&upload_id=%s",
				tc.hostHeader, projectID, jobID)

			if !strings.HasPrefix(location, expectedPrefix) {
				t.Errorf("Location header doesn't use Host header.\nExpected prefix: %s\nGot: %s", expectedPrefix, location)
			}

			// Verify it does NOT contain the server's bind address (like 0.0.0.0)
			if strings.Contains(location, "0.0.0.0") {
				t.Errorf("Location header should not contain server bind address 0.0.0.0, got: %s", location)
			}
		})
	}
}

// TestResumableUploadURLWithoutHostHeader tests that the endpoint still works
// even when the Host header is not explicitly set (default behavior).
func TestResumableUploadURLWithoutHostHeader(t *testing.T) {
	const (
		projectID = "test-project"
		datasetID = "test-dataset"
		tableID   = "test-table"
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}

	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
					types.NewTable(
						tableID,
						[]*types.Column{
							types.NewColumn("id", types.INT64),
							types.NewColumn("name", types.STRING),
						},
						nil,
					),
				),
			),
		),
	); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Close()
	}()

	// Create a resumable upload request using the BigQuery client
	// (which should automatically set the Host header)
	bqClient, err := bigquery.NewClient(
		ctx,
		projectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer bqClient.Close()

	// Try to create a load job - this internally uses resumable upload
	source := bigquery.NewReaderSource(strings.NewReader(`{"id":1,"name":"test"}`))
	source.SourceFormat = bigquery.JSON
	loader := bqClient.Dataset(datasetID).Table(tableID).LoaderFrom(source)

	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatalf("failed to run load job: %v", err)
	}

	// Wait for the job to complete
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("job failed: %v", err)
	}

	if status.Err() != nil {
		t.Fatalf("job returned error: %v", status.Err())
	}
}