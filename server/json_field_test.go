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
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

// TestInsertAllJSONField tests that tabledata.insertAll accepts JSON column
// values sent as objects/arrays (not only pre-serialized JSON strings), and
// that unknown keys inside RECORD values produce a per-row insert error
// instead of a panic that surfaces as a retryable 500.
//
// Regression test for a nil pointer dereference in types.normalizeData: a
// JSON column has no nested schema, so looking up the object's keys in the
// field map returned nil and the recursive call dereferenced it.
func TestInsertAllJSONField(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}

	const (
		projectID = "test"
		datasetID = "test_dataset"
		tableID   = "test_table"
	)

	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INT64),
					types.NewColumn("payload", types.JSON),
					types.NewColumn(
						"record",
						types.STRUCT,
						types.ColumnFields(
							types.NewColumn("known", types.STRING),
						),
					),
				},
				nil,
			),
		),
	)

	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Close()
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	insertAllURL := fmt.Sprintf(
		"%s/bigquery/v2/projects/%s/datasets/%s/tables/%s/insertAll",
		testServer.URL, projectID, datasetID, tableID,
	)

	insertAll := func(t *testing.T, body string) *bigqueryv2.TableDataInsertAllResponse {
		t.Helper()
		res, err := http.Post(insertAllURL, "application/json", bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", res.StatusCode)
		}
		var insertRes bigqueryv2.TableDataInsertAllResponse
		if err := json.NewDecoder(res.Body).Decode(&insertRes); err != nil {
			t.Fatal(err)
		}
		return &insertRes
	}

	t.Run("object value for JSON column", func(t *testing.T) {
		res := insertAll(t, `{"rows":[{"json":{"id":1,"payload":{"any":"object","nested":{"a":1}}}}]}`)
		if len(res.InsertErrors) != 0 {
			t.Fatalf("expected no insert errors, got %+v", res.InsertErrors)
		}

		it, err := client.Query(fmt.Sprintf(
			"SELECT TO_JSON_STRING(payload) FROM %s.%s WHERE id = 1", datasetID, tableID,
		)).Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			t.Fatal(err)
		}
		got, ok := row[0].(string)
		if !ok {
			t.Fatalf("expected string, got %T", row[0])
		}
		if got != `{"any":"object","nested":{"a":1}}` {
			t.Errorf("unexpected payload: %s", got)
		}
	})

	t.Run("array value for JSON column", func(t *testing.T) {
		res := insertAll(t, `{"rows":[{"json":{"id":2,"payload":[1,"two",null]}}]}`)
		if len(res.InsertErrors) != 0 {
			t.Fatalf("expected no insert errors, got %+v", res.InsertErrors)
		}
	})

	t.Run("pre-serialized string for JSON column still works", func(t *testing.T) {
		res := insertAll(t, `{"rows":[{"json":{"id":3,"payload":"{\"any\":\"object\"}"}}]}`)
		if len(res.InsertErrors) != 0 {
			t.Fatalf("expected no insert errors, got %+v", res.InsertErrors)
		}
	})

	t.Run("unknown key in RECORD column returns per-row insert error", func(t *testing.T) {
		res := insertAll(t, `{"rows":[{"json":{"id":4,"record":{"known":"v","unknown":"v"}}}]}`)
		if len(res.InsertErrors) == 0 {
			t.Fatal("expected insert errors for unknown nested field, got none")
		}
		errs := res.InsertErrors[0].Errors
		if len(errs) == 0 {
			t.Fatal("expected error details, got none")
		}
		if errs[0].Reason != "invalid" {
			t.Errorf("expected reason %q, got %q", "invalid", errs[0].Reason)
		}
		if !strings.Contains(errs[0].Message, "record.unknown") {
			t.Errorf("expected message to reference %q, got %q", "record.unknown", errs[0].Message)
		}

		// The invalid row must not have been inserted.
		it, err := client.Query(fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.%s WHERE id = 4", datasetID, tableID,
		)).Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row []bigquery.Value
		if err := it.Next(&row); err != nil && err != iterator.Done {
			t.Fatal(err)
		}
		if count, ok := row[0].(int64); ok && count != 0 {
			t.Errorf("expected row with unknown nested field not to be inserted, found %d rows", count)
		}
	})
}
