package server_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/goccy/go-json"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func buildClient(ctx context.Context, project *types.Project, server *server.TestServer) (*bigquery.Client, error) {
	client, err := bigquery.NewClient(
		ctx,
		project.ID,
		option.WithEndpoint(server.URL),
		option.WithoutAuthentication(),
	)

	if err != nil {
		return nil, err
	}
	return client, nil

}

func TestSimpleQuery(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject("test")
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := buildClient(ctx, project, testServer)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	t.Run("array", func(t *testing.T) {
		query := client.Query("SELECT [1, 2, 3] as a")
		it, err := query.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			t.Log("row = ", row)
		}
	})

	t.Run("empty array", func(t *testing.T) {
		query := client.Query("SELECT []")
		it, err := query.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row []bigquery.Value
		for {
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			t.Log("row = ", row)
		}
		if len(row) != 1 || row[0] == nil {
			t.Fatal("Failed to query empty ARRAY")
		}
	})

	t.Run("null array", func(t *testing.T) {
		query := client.Query("SELECT CAST(NULL AS ARRAY<STRING>)")
		it, err := query.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row []bigquery.Value
		for {
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			t.Log("row = ", row)
		}
		if len(row) != 1 || row[0] == nil {
			t.Fatal("Failed to query null ARRAY")
		}
	})

	// Regression test for goccy/bigquery-emulator#316
	t.Run("invalid query", func(t *testing.T) {
		query := client.Query("SELECT!;")
		_, err := query.Read(ctx)
		if err == nil {
			t.Fatal("Expected error, but got nil")
		}
		if !strings.HasSuffix(err.Error(), "invalidQuery") {
			t.Fatal("expected invalid query")
		}
	})

	// Regression test for goccy/bigquery-emulator#316
	t.Run("invalid query", func(t *testing.T) {
		ctx := context.Background()
		client.Dataset("test_ds").Create(ctx, &bigquery.DatasetMetadata{Name: "test_ds"})
		err = client.Dataset("test_ds").Table("test_table").Create(ctx,
			&bigquery.TableMetadata{
				Description: "test!",
				ViewQuery:   "SELECT 1 AS A_FIELD, TEST~!",
			})
		if err == nil {
			t.Fatal("Expected error, but got nil")
		}
		if !strings.HasSuffix(err.Error(), "invalidQuery") {
			t.Fatal("expected invalid query")
		}
	})
}

func TestDataset(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "project"
	)

	bqServer, err := server.New(server.MemoryStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	fooDataset := client.Dataset("foo")
	if err := fooDataset.Create(ctx, &bigquery.DatasetMetadata{
		Name:        "foo",
		Description: "dataset for foo",
		Location:    "Tokyo",
		Labels:      map[string]string{"aaa": "bbb"},
	}); err != nil {
		t.Fatal(err)
	}

	barDataset := client.Dataset("bar")
	if err := barDataset.Create(ctx, &bigquery.DatasetMetadata{
		Name:        "bar",
		Description: "dataset for bar",
		Location:    "Tokyo",
		Labels:      map[string]string{"bbb": "ccc"},
	}); err != nil {
		t.Fatal(err)
	}

	if datasets := findDatasets(t, ctx, client); len(datasets) != 2 {
		t.Fatalf("failed to find datasets")
	}

	md, err := fooDataset.Update(ctx, bigquery.DatasetMetadataToUpdate{
		Name: "foo2",
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if md.Name != "foo2" {
		t.Fatalf("failed to update dataset metadata: md.Name = %s", md.Name)
	}

	if err := fooDataset.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := barDataset.DeleteWithContents(ctx); err != nil {
		t.Fatal(err)
	}

	if datasets := findDatasets(t, ctx, client); len(datasets) != 0 {
		t.Fatalf("failed to find datasets")
	}
}

func findDatasets(t *testing.T, ctx context.Context, client *bigquery.Client) []*bigquery.Dataset {
	t.Helper()
	var datasets []*bigquery.Dataset
	it := client.Datasets(ctx)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		datasets = append(datasets, ds)
	}
	return datasets
}

func TestDataFromJSON(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.JSONSource(filepath.Join("testdata", "data.json"))); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	query := client.Query("SELECT * FROM dataset1.table_a")
	job, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := job.Config(); err != nil {
		t.Fatal(err)
	}
	if _, err := job.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	gotJob, err := client.JobFromID(ctx, job.ID())
	if err != nil {
		t.Fatal(err)
	}
	if gotJob.ID() != job.ID() {
		t.Fatalf("failed to get job expected ID %s. but got %s", job.ID(), gotJob.ID())
	}

	if jobs := findJobs(t, ctx, client); len(jobs) != 1 {
		t.Fatalf("failed to find jobs. expected 1 jobs but found %d jobs", len(jobs))
	}
}

func TestJob(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.YAMLSource(filepath.Join("testdata", "data.yaml"))); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	query := client.Query("SELECT * FROM dataset1.table_a")
	job, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := job.Config(); err != nil {
		t.Fatal(err)
	}
	if _, err := job.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	gotJob, err := client.JobFromID(ctx, job.ID())
	if err != nil {
		t.Fatal(err)
	}
	if gotJob.ID() != job.ID() {
		t.Fatalf("failed to get job expected ID %s. but got %s", job.ID(), gotJob.ID())
	}

	job2, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := job2.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := findJobs(t, ctx, client); len(jobs) != 2 {
		t.Fatalf("failed to find jobs. expected 2 jobs but found %d jobs", len(jobs))
	}
	if err := job2.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := findJobs(t, ctx, client); len(jobs) != 1 {
		t.Fatalf("failed to find jobs. expected 1 jobs but found %d jobs", len(jobs))
	}
}

type JSONTableSaver struct {
	JsonColumn map[string]bigquery.Value
}

// Save implements the bigquery.ValueSaver interface to serialize the "json_column" field as JSON string.
func (s *JSONTableSaver) Save() (row map[string]bigquery.Value, insertID string, err error) {
	attr, err := json.Marshal(s.JsonColumn)
	if err != nil {
		return nil, "", err
	}

	row = map[string]bigquery.Value{
		"json_column": string(attr),
	}

	return row, "1", nil
}

func TestJson(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
		t.Fatal(err)
	}

	project := types.NewProject("test",
		types.NewDataset("dataset",
			types.NewTable("table_a", []*types.Column{
				{Name: "json_column", Type: "JSON", Mode: "REQUIRED"},
			}, nil,
			),
		),
	)

	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	saver := &JSONTableSaver{JsonColumn: map[string]bigquery.Value{
		"a_date":  "2024-01-01",
		"b_float": 1.2,
		"c_nested": map[string]bigquery.Value{
			"test": "struct value",
		},
		"d_list": []bigquery.Value{
			"list item",
			"list item",
		},
	}}

	table := client.Dataset("dataset").Table("table_a")
	if err := table.Inserter().Put(ctx, saver); err != nil {
		t.Fatal(err)
	}

	query := client.Query(`SELECT
		JSON_VALUE(json_column, "$.a_date") AS a_date,
		JSON_VALUE(json_column, "$.b_float") AS b_float,
		JSON_VALUE(json_column, "$.c_nested.test") AS c_nested,
		JSON_VALUE(json_column, "$.d_list[0]") AS d_list,
	FROM dataset.table_a`)
	iter, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var rows [][]bigquery.Value
	for {
		var row []bigquery.Value
		if err := iter.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 rows, got [%d]", len(rows))
	}
	for _, row := range rows {
		if row[0] != "2024-01-01" {
			t.Fatalf(`expected ["2024-01-01"] but got [%s]`, row[0])
		}
		if row[1] != "1.2" {
			t.Fatalf(`expected ["1.2"] but got [%s]`, row[1])
		}
		if row[2] != "struct value" {
			t.Fatalf(`expected ["struct value"] but got [%s]`, row[2])
		}
		if row[3] != "list item" {
			t.Fatalf(`expected ["list item"] but got [%s]`, row[2])
		}
	}
}

func TestFetchData(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.YAMLSource(filepath.Join("testdata", "data.yaml"))); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	query := client.Query("SELECT * FROM dataset1.table_b")
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type TableB struct {
		Num    *big.Rat `bigquery:"num"`
		BigNum *big.Rat `bigquery:"bignum"`
		// INTERVAL type cannot assign to struct directly
		// Interval *bigquery.IntervalValue `bigquery:"interval"`
	}

	var row TableB
	_ = it.Next(&row)
	if row.Num.FloatString(4) != "1.2345" {
		t.Fatalf("failed to get NUMERIC value")
	}
	if row.BigNum.FloatString(12) != "1.234567891234" {
		t.Fatalf("failed to get BIGNUMERIC value")
	}
}

func findJobs(t *testing.T, ctx context.Context, client *bigquery.Client) []*bigquery.Job {
	t.Helper()
	var jobs []*bigquery.Job
	it := client.Jobs(ctx)
	for {
		job, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
	}
	return jobs
}

func TestQuery(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.YAMLSource(filepath.Join("testdata", "data.yaml")),
	); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	const (
		projectName = "test"
	)

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	query := client.Query("SELECT * FROM dataset1.table_a WHERE id = @id")
	query.QueryConfig.Parameters = []bigquery.QueryParameter{
		{Name: "id", Value: 1},
	}
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		t.Log("row = ", row)
	}
}

func TestQueryWithDestination(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.YAMLSource(filepath.Join("testdata", "data.yaml")),
	); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	const (
		projectName = "test"
	)

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	query := client.Query("SELECT id FROM dataset1.table_a")
	query.QueryConfig.Dst = &bigquery.Table{
		ProjectID: projectName,
		DatasetID: "dataset1",
		TableID:   "table_a_materialized",
	}
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		t.Log("row = ", row)
	}

	query = client.Query("SELECT id FROM dataset1.table_a_materialized")

	it, err = query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		t.Log("row = ", row)
	}
}

type TableSchema struct {
	Int      int
	Str      string
	Float    float64
	Struct   *StructType
	Array    []*StructType
	IntArray []int
	Time     time.Time
}

type StructType struct {
	A int
	B string
	C float64
}

func (s *TableSchema) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"Int":      s.Int,
		"Str":      s.Str,
		"Float":    s.Float,
		"Struct":   s.Struct,
		"Array":    s.Array,
		"IntArray": s.IntArray,
		"Time":     s.Time,
	}, "", nil
}

func TestTable(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		table1Name  = "table1"
		table2Name  = "table2"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	table1 := client.Dataset(datasetName).Table(table1Name)
	if err := table1.Create(ctx, nil); err != nil {
		t.Fatalf("%+v", err)
	}

	schema, err := bigquery.InferSchema(TableSchema{})
	if err != nil {
		t.Fatal(err)
	}
	table2 := client.Dataset(datasetName).Table(table2Name)
	if err := table2.Create(ctx, &bigquery.TableMetadata{
		Name:           "table2",
		Schema:         schema,
		ExpirationTime: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	tableIter := client.Dataset(datasetName).Tables(ctx)
	var tableCount int
	for {
		if _, err := tableIter.Next(); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		tableCount++
	}
	if tableCount != 2 {
		t.Fatalf("failed to get tables. expected 2 but got %d", tableCount)
	}
	insertRow := &TableSchema{
		Int:   1,
		Str:   "2",
		Float: 3,
		Struct: &StructType{
			A: 4,
			B: "5",
			C: 6,
		},
		Array: []*StructType{
			{
				A: 7,
				B: "8",
				C: 9,
			},
		},
		IntArray: []int{10},
		Time:     time.Now(),
	}
	if err := table2.Inserter().Put(ctx, []*TableSchema{insertRow}); err != nil {
		t.Fatal(err)
	}
	iter := table2.Read(ctx)
	var rows []*TableSchema
	for {
		var row TableSchema
		if err := iter.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, &row)
	}
	if len(rows) != 1 {
		t.Fatalf("failed to get table data. got rows are %d", len(rows))
	}
	if diff := cmp.Diff(insertRow, rows[0], cmpopts.EquateApproxTime(1*time.Second)); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}

	if _, err := table2.Update(ctx, bigquery.TableMetadataToUpdate{
		Description: "updated table",
	}, ""); err != nil {
		t.Fatal(err)
	}
	if err := table2.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	// recreate table2
	if err := table2.Create(ctx, &bigquery.TableMetadata{
		Name:           "table2",
		Schema:         schema,
		ExpirationTime: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDirectDDL(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "dataset1"
		tableID   = "foo"
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectID, types.NewDataset(datasetID))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
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
	tableName := fmt.Sprintf("%s.%s.%s", projectID, datasetID, tableID)
	if _, err := client.Query(fmt.Sprintf("CREATE TABLE %s(name STRING)", tableName)).Run(ctx); err != nil {
		t.Fatal(err)
	}
	tableIter := client.Dataset(datasetID).Tables(ctx)
	table, err := tableIter.Next()
	if err != nil {
		if err != iterator.Done {
			t.Fatal(err)
		}
	}
	if table == nil {
		t.Fatal("failed to get created table")
	}
	if table.TableID != tableID {
		t.Fatalf("failed to get table. got table-id is %s", table.TableID)
	}
	if _, err := client.Query(`DROP TABLE test.dataset1.foo`).Run(ctx); err != nil {
		t.Fatal(err)
	}
	tableIter = client.Dataset(datasetID).Tables(ctx)
	var tableCount int
	for {
		if _, err := tableIter.Next(); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		tableCount++
	}
	if tableCount != 0 {
		t.Fatalf("failed to drop table. table count is %d", tableCount)
	}
}

func TestView(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table"
		viewName    = "view"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	schema, err := bigquery.InferSchema(TableSchema{})
	if err != nil {
		t.Fatal(err)
	}
	table := client.Dataset(datasetName).Table(tableName)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Name:           "table",
		Schema:         schema,
		ExpirationTime: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	insertRow := &TableSchema{
		Int:   -1,
		Str:   "2",
		Float: 3,
		Struct: &StructType{
			A: 4,
			B: "5",
			C: 6,
		},
		Array: []*StructType{
			{
				A: 7,
				B: "8",
				C: 9,
			},
		},
		IntArray: []int{10},
		Time:     time.Now(),
	}
	if err := table.Inserter().Put(ctx, []*TableSchema{insertRow}); err != nil {
		t.Fatal(err)
	}

	// view

	view := client.Dataset(datasetName).Table(viewName)

	if err := view.Create(ctx, &bigquery.TableMetadata{
		Name:      viewName,
		ViewQuery: "SELECT ABS(Int) AS Int FROM table",
	}); err != nil {
		t.Fatal(err)
	}

	query := client.Query("SELECT * FROM dataset1.view")

	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type ViewRow struct {
		Int int
	}
	var viewRows []*ViewRow
	for {
		var viewRow ViewRow
		if err := it.Next(&viewRow); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		viewRows = append(viewRows, &viewRow)
	}

	if len(viewRows) != 1 {
		t.Fatalf("failed to get view data. view rows length is %d", len(viewRows))
	}
	if viewRows[0].Int != 1 {
		t.Fatal("unexpected view row data")
	}
}

func TestViewEndingInSemicolon(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		viewName    = "view_a"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	view := client.Dataset(datasetName).Table(viewName)

	if err := view.Create(ctx, &bigquery.TableMetadata{
		Name: viewName,
		ViewQuery: `WITH table_rows AS (
			SELECT "Blueberry Muffins" AS item, 2 AS orders
		)
		SELECT item, COUNT(*) AS total_count
		FROM table_rows
		GROUP BY item, orders
		ORDER BY item;`,
	}); err != nil {
		t.Fatal(err)
	}

	query := client.Query("SELECT * FROM dataset1.view_a")

	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type ViewRow struct {
		Item       string
		TotalCount int64
	}
	var viewRows []*ViewRow
	for {
		var viewRow ViewRow
		if err := it.Next(&viewRow); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		viewRows = append(viewRows, &viewRow)
	}

	if len(viewRows) != 1 {
		t.Fatalf("failed to get view data. view rows length is %d", len(viewRows))
	}
	if viewRows[0].Item != "Blueberry Muffins" {
		t.Fatal("unexpected view row data")
	}
}

func TestDuplicateTable(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	table := client.Dataset(datasetName).Table(tableName)
	if err := table.Create(ctx, nil); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := table.Create(ctx, nil); err != nil {
		ge := err.(*googleapi.Error)
		if ge.Code != 409 {
			t.Fatalf("%+v", ge)
		}
	} else {
		t.Fatalf(("Threre should be error, when table name duplicates."))
	}
}

func TestDuplicateTableWithSchema(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	table := client.Dataset(datasetName).Table(tableName)

	schema := bigquery.Schema{
		{Name: "id", Required: true, Type: bigquery.StringFieldType},
		{Name: "data", Required: false, Type: bigquery.StringFieldType},
		{Name: "timestamp", Required: false, Type: bigquery.TimestampFieldType},
	}
	metaData := &bigquery.TableMetadata{Schema: schema}
	if err := table.Create(ctx, metaData); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := table.Create(ctx, metaData); err != nil {
		ge := err.(*googleapi.Error)
		if ge.Code != 409 {
			t.Fatalf("%+v", ge)
		}
	} else {
		t.Fatalf(("There should be error, when table name duplicates."))
	}
}

func TestDuplicateDataset(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	dataset := client.Dataset(datasetName)
	err = dataset.Create(ctx, nil)
	if err == nil || !strings.HasSuffix(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate error; got %s", err)
	}
}

func TestDeleteDatasetInUseJob(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
	)
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}

	project := types.NewProject(projectName, types.NewDataset(datasetName,
		types.NewTable("table1", []*types.Column{
			types.NewColumn("id", types.STRING),
		}, nil),
	))

	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := buildClient(ctx, project, testServer)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	dataset := client.Dataset(datasetName)
	err = dataset.Delete(ctx)
	if err == nil || !strings.HasSuffix(err.Error(), "resourceInUse") {
		t.Fatalf("expected resource in use error; got %s", err)
	}
}

func TestDataFromStruct(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectName,
				types.NewDataset(
					"dataset1",
					types.NewTable(
						"table_a",
						[]*types.Column{
							types.NewColumn("id", types.INTEGER),
							types.NewColumn("name", types.STRING),
						},
						types.Data{
							{
								"id":   1,
								"name": "alice",
							},
							{
								"id":   2,
								"name": "bob",
							},
						},
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
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	query := client.Query("SELECT * FROM dataset1.table_a WHERE id = @id")
	query.QueryConfig.Parameters = []bigquery.QueryParameter{
		{Name: "id", Value: 1},
	}
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		t.Log("row = ", row)
	}
	if err := client.Dataset("dataset1").DeleteWithContents(ctx); err != nil {
		t.Fatal(err)
	}
}

type dataset2Table struct {
	ID    int64
	Name2 string
}

func (t *dataset2Table) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"id":    t.ID,
		"name2": t.Name2,
	}, "", nil
}

func TestMultiDatasets(t *testing.T) {
	ctx := context.Background()

	const (
		projectName  = "test"
		datasetName1 = "dataset1"
		datasetName2 = "dataset2"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectName,
				types.NewDataset(
					"dataset1",
					types.NewTable(
						"a",
						[]*types.Column{
							types.NewColumn("id", types.INTEGER),
							types.NewColumn("name1", types.STRING),
						},
						types.Data{
							{
								"id":    1,
								"name1": "alice",
							},
						},
					),
				),
				types.NewDataset(
					"dataset2",
					types.NewTable(
						"a",
						[]*types.Column{
							types.NewColumn("id", types.INTEGER),
							types.NewColumn("name2", types.STRING),
						},
						types.Data{
							{
								"id":    1,
								"name2": "bob",
							},
						},
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
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	{
		query := client.Query("SELECT * FROM `test.dataset1.a` WHERE id = @id")
		query.QueryConfig.Parameters = []bigquery.QueryParameter{
			{Name: "id", Value: 1},
		}
		it, err := query.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			t.Log("row = ", row)
		}
	}
	{
		query := client.Query("SELECT * FROM `test.dataset2.a` WHERE id = @id")
		query.QueryConfig.Parameters = []bigquery.QueryParameter{
			{Name: "id", Value: 1},
		}
		it, err := query.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			t.Log("row = ", row)
		}
	}
	{
		query := client.Query("SELECT name1 FROM `test.dataset2.a` WHERE id = @id")
		query.QueryConfig.Parameters = []bigquery.QueryParameter{
			{Name: "id", Value: 1},
		}
		if _, err := query.Read(ctx); err == nil {
			t.Fatal("expected error")
		}
	}
	if err := client.Dataset(datasetName2).Table("a").Inserter().Put(
		ctx,
		[]*dataset2Table{{ID: 3, Name2: "name3"}},
	); err != nil {
		t.Fatal(err)
	}
	{
		table := client.Dataset(datasetName1).Table("a")
		if table.DatasetID != datasetName1 {
			t.Fatalf("failed to get table")
		}
		if table.TableID != "a" {
			t.Fatalf("failed to get table")
		}
	}
}

func TestRoutine(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset1"
		routineID = "routine1"
	)
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
				),
			),
		),
	); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectID); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
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

	metaData := &bigquery.RoutineMetadata{
		Type:     "SCALAR_FUNCTION",
		Language: "SQL",
		Body:     "x * 3",
		Arguments: []*bigquery.RoutineArgument{
			{Name: "x", DataType: &bigquery.StandardSQLDataType{TypeKind: "INT64"}},
		},
	}

	routineRef := client.Dataset(datasetID).Routine(routineID)
	if err := routineRef.Create(ctx, metaData); err != nil {
		t.Fatalf("%+v", err)
	}

	query := client.Query("SELECT val, dataset1.routine1(val) FROM UNNEST([1, 2, 3, 4]) AS val")
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var (
		rowNum      int
		expectedSrc = []int64{1, 2, 3, 4}
		expectedDst = []int64{3, 6, 9, 12}
	)
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		if len(row) != 2 {
			t.Fatalf("failed to get row. got length %d", len(row))
		}
		src, ok := row[0].(int64)
		if !ok {
			t.Fatalf("failed to get row[0]. type is %T", row[0])
		}
		dst, ok := row[1].(int64)
		if !ok {
			t.Fatalf("failed to get row[1]. type is %T", row[1])
		}
		if expectedSrc[rowNum] != src {
			t.Fatalf("expected value is %d but got %d", expectedSrc[rowNum], src)
		}
		if expectedDst[rowNum] != dst {
			t.Fatalf("expected value is %d but got %d", expectedDst[rowNum], dst)
		}
		rowNum++
	}
}

func TestRoutineWithQuery(t *testing.T) {
	ctx := context.Background()
	const (
		projectID = "test"
		datasetID = "dataset1"
		routineID = "routine1"
	)
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
				),
			),
		),
	); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectID); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
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
	routineName, err := client.Dataset(datasetID).Routine(routineID).Identifier(bigquery.StandardSQLID)
	if err != nil {
		t.Fatal(err)
	}
	sql := fmt.Sprintf(`
CREATE FUNCTION %s(
  arr ARRAY<STRUCT<name STRING, val INT64>>
) AS (
  (SELECT SUM(IF(elem.name = "foo",elem.val,null)) FROM UNNEST(arr) AS elem)
)`, routineName)
	job, err := client.Query(sql).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := status.Err(); err != nil {
		t.Fatal(err)
	}

	queryText := fmt.Sprintf(`
SELECT %s([
  STRUCT<name STRING, val INT64>("foo", 10),
  STRUCT<name STRING, val INT64>("bar", 40),
  STRUCT<name STRING, val INT64>("foo", 20)
])`, routineName)
	query := client.Query(queryText)
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		if len(row) != 1 {
			t.Fatalf("failed to get row. got length %d", len(row))
		}
		src, ok := row[0].(int64)
		if !ok {
			t.Fatalf("failed to get row[0]. type is %T", row[0])
		}
		if src != 30 {
			t.Fatalf("expected 30 but got %d", src)
		}
	}
}

func TestContentEncoding(t *testing.T) {
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test"))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(context.Background())
	}()

	client := new(http.Client)
	b, err := json.Marshal(bigqueryv2.Job{
		Configuration: &bigqueryv2.JobConfiguration{
			Query: &bigqueryv2.JobConfigurationQuery{
				Query: "SELECT 1",
			},
		},
		JobReference: &bigqueryv2.JobReference{},
	})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	defer writer.Close()
	if _, err := writer.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/projects/test/jobs", testServer.URL), &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("Content-Encoding", "gzip")
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("failed to request with gzip: %s", string(body))
	}
}

func TestCreateTempTable(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				"test",
				types.NewDataset("dataset1"),
			),
		),
	); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		"test",
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	{
		job, err := client.Query("CREATE TEMP TABLE dataset1.tmp ( id INT64 )").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := job.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	{
		job, err := client.Query("CREATE TEMP TABLE dataset1.tmp ( id INT64 )").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := job.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	{
		job, err := client.Query("CREATE TABLE dataset1.tmp ( id INT64 )").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := job.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	{
		job, err := client.Query("CREATE TABLE dataset1.tmp ( id INT64 )").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := job.Wait(ctx); err == nil {
			t.Fatal("expected error")
		}
	}
}

type TestTs struct {
	Name       string    `bigquery:"name"`
	ReportTime time.Time `bigquery:"report_time"`
}

func TestTabledataListInt64Timestamp(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.Dataset(datasetName).Table(tableName).Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{
				Name: "name",
				Type: "STRING",
			},
			{
				Name: "report_time",
				Type: "TIMESTAMP",
			},
		},
	})
	// Insert data
	testData := []TestTs{
		{
			Name:       "test1",
			ReportTime: time.Now().UTC(),
		},
		{
			Name:       "test2",
			ReportTime: time.Now().UTC(),
		},
	}

	u := client.Dataset(datasetName).Table(tableName).Inserter()
	err = u.Put(ctx, testData)
	if err != nil {
		t.Fatalf("failed to insert rows: %s", err)
	}

	// Load the data
	it := client.Dataset(datasetName).Table(tableName).Read(ctx)
	var tData []TestTs
	for {
		var ts TestTs
		err := it.Next(&ts)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		tData = append(tData, ts)
	}
}

func TestQueryWithTimestampType(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	table := client.Dataset(datasetName).Table(tableName)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: []*bigquery.FieldSchema{
			{Name: "ts", Type: bigquery.TimestampFieldType},
		},
	}); err != nil {
		t.Fatalf("%+v", err)
	}

	query := client.Query("SELECT CURRENT_TIMESTAMP() AS ts")
	query.QueryConfig.Dst = &bigquery.Table{
		ProjectID: projectName,
		DatasetID: datasetName,
		TableID:   table.TableID,
	}
	job, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("%+v", err)
	}
}

func TestLoadJSON(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	table := client.Dataset(datasetName).Table(tableName)
	schema := bigquery.Schema{
		{Name: "ID", Type: bigquery.IntegerFieldType},
		{Name: "Name", Type: bigquery.StringFieldType},
	}

	{
		source := bigquery.NewReaderSource(bytes.NewBufferString(`
{"ID": 1, "Name": "John"}
`,
		))
		source.SourceFormat = bigquery.JSON
		source.Schema = schema

		job, err := table.LoaderFrom(source).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.Err() != nil {
			t.Fatal(err)
		}
	}

	{
		source := bigquery.NewReaderSource(bytes.NewBufferString(`
{"ID": 2, "Name": "Joan"}
`,
		))
		source.SourceFormat = bigquery.JSON

		job, err := table.LoaderFrom(source).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.Err() != nil {
			t.Fatal(err)
		}
	}

	query := client.Query(fmt.Sprintf("SELECT * FROM %s.%s ORDER BY ID", datasetName, tableName))
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type row struct {
		ID   int
		Name string
	}
	var rows []*row
	for {
		var r row
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, &r)
	}
	if diff := cmp.Diff([]*row{
		{ID: 1, Name: "John"},
		{ID: 2, Name: "Joan"},
	}, rows); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}

func TestCSVAutodetect(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "test_table"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	// Create dataset only (no table - it should be created with autodetected schema)
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// CSV with various types
	csvData := `name,age,score,active,birth_date
Alice,30,95.5,true,2024-01-15
Bob,25,88.0,false,2024-06-30
`

	table := client.Dataset(datasetName).Table(tableName)
	source := bigquery.NewReaderSource(strings.NewReader(csvData))
	source.SourceFormat = bigquery.CSV
	source.AutoDetect = true

	loader := table.LoaderFrom(source)
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatalf("failed to run load job: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("job wait failed: %v", err)
	}
	if status.Err() != nil {
		t.Fatalf("job failed: %v", status.Err())
	}

	// Verify data was loaded correctly
	query := client.Query("SELECT name, age, score, active, birth_date FROM " + datasetName + "." + tableName + " ORDER BY name")
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	type csvRow struct {
		Name      string     `bigquery:"name"`
		Age       int64      `bigquery:"age"`
		Score     float64    `bigquery:"score"`
		Active    bool       `bigquery:"active"`
		BirthDate civil.Date `bigquery:"birth_date"`
	}

	var rows []*csvRow
	for {
		var r csvRow
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, &r)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Verify first row (Alice)
	if rows[0].Name != "Alice" {
		t.Errorf("expected name=Alice, got %s", rows[0].Name)
	}
	if rows[0].Age != 30 {
		t.Errorf("expected age=30, got %d", rows[0].Age)
	}
	if rows[0].Score != 95.5 {
		t.Errorf("expected score=95.5, got %f", rows[0].Score)
	}
	if rows[0].Active != true {
		t.Errorf("expected active=true, got %v", rows[0].Active)
	}

	// Verify second row (Bob)
	if rows[1].Name != "Bob" {
		t.Errorf("expected name=Bob, got %s", rows[1].Name)
	}
	if rows[1].Age != 25 {
		t.Errorf("expected age=25, got %d", rows[1].Age)
	}
	if rows[1].Active != false {
		t.Errorf("expected active=false, got %v", rows[1].Active)
	}
}

func TestImportFromGCS(t *testing.T) {
	const (
		projectID  = "test"
		datasetID  = "dataset1"
		tableID    = "table_a"
		publicHost = "127.0.0.1"
		bucketName = "test-bucket"
		sourceName = "path/to/data.json"
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INT64),
					types.NewColumn("value", types.INT64),
				},
				nil,
			),
		),
	)
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := 0; i < 3; i++ {
		if err := enc.Encode(map[string]interface{}{
			"id":    i + 1,
			"value": i + 10,
		}); err != nil {
			t.Fatal(err)
		}
	}
	storageServer, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: []fakestorage.Object{
			{
				ObjectAttrs: fakestorage.ObjectAttrs{
					BucketName: bucketName,
					Name:       sourceName,
					Size:       int64(len(buf.Bytes())),
				},
				Content: buf.Bytes(),
			},
		},
		PublicHost: publicHost,
		Scheme:     "http",
	})
	if err != nil {
		t.Fatal(err)
	}

	storageServerURL := storageServer.URL()
	u, err := url.Parse(storageServerURL)
	if err != nil {
		t.Fatal(err)
	}
	storageEmulatorHost := fmt.Sprintf("http://%s:%s", publicHost, u.Port())
	t.Setenv("STORAGE_EMULATOR_HOST", storageEmulatorHost)

	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
		storageServer.Stop()
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

	gcsSourceURL := fmt.Sprintf("gs://%s/%s", bucketName, sourceName)
	gcsRef := bigquery.NewGCSReference(gcsSourceURL)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.AutoDetect = true
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Err() != nil {
		t.Fatal(status.Err())
	}

	query := client.Query(fmt.Sprintf("SELECT * FROM %s.%s", datasetID, tableID))
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type row struct {
		ID    int64
		Value int64
	}
	var rows []*row
	for {
		var r row
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, &r)
	}
	if diff := cmp.Diff([]*row{
		{ID: 1, Value: 10},
		{ID: 2, Value: 11},
		{ID: 3, Value: 12},
	}, rows); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}

func TestImportFromGCSEmulatorWithoutPublicHost(t *testing.T) {
	const (
		projectID  = "test"
		datasetID  = "dataset1"
		tableID    = "table_a"
		host       = "127.0.0.1"
		bucketName = "test-bucket"
		sourceName = "path/to/data.json"
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INT64),
					types.NewColumn("value", types.INT64),
				},
				nil,
			),
		),
	)
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := 0; i < 3; i++ {
		if err := enc.Encode(map[string]interface{}{
			"id":    i + 1,
			"value": i + 10,
		}); err != nil {
			t.Fatal(err)
		}
	}
	storageServer, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: []fakestorage.Object{
			{
				ObjectAttrs: fakestorage.ObjectAttrs{
					BucketName: bucketName,
					Name:       sourceName,
					Size:       int64(len(buf.Bytes())),
				},
				Content: buf.Bytes(),
			},
		},
		Host:   host,
		Scheme: "http",
	})
	if err != nil {
		t.Fatal(err)
	}

	storageServerURL := storageServer.URL()
	u, err := url.Parse(storageServerURL)
	if err != nil {
		t.Fatal(err)
	}
	storageEmulatorHost := fmt.Sprintf("http://%s:%s", host, u.Port())
	t.Setenv("STORAGE_EMULATOR_HOST", storageEmulatorHost)

	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
		storageServer.Stop()
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

	gcsSourceURL := fmt.Sprintf("gs://%s/%s", bucketName, sourceName)
	gcsRef := bigquery.NewGCSReference(gcsSourceURL)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.AutoDetect = true
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Err() != nil {
		t.Fatal(status.Err())
	}

	query := client.Query(fmt.Sprintf("SELECT * FROM %s.%s", datasetID, tableID))
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type row struct {
		ID    int64
		Value int64
	}
	var rows []*row
	for {
		var r row
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, &r)
	}
	if diff := cmp.Diff([]*row{
		{ID: 1, Value: 10},
		{ID: 2, Value: 11},
		{ID: 3, Value: 12},
	}, rows); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}

func TestImportWithWildcardFromGCS(t *testing.T) {
	const (
		projectID  = "test"
		datasetID  = "dataset1"
		tableID    = "table_a"
		publicHost = "127.0.0.1"
		bucketName = "test-bucket"
		sourceName = "path/to/*.json"
	)

	var (
		targetSourceFiles = []string{
			"path/to/data.json",
			"path/to/under/data.json",
		}
		nonTargetSourceFiles = []string{
			"path/not/data.json",
			"path/to/data.csv",
		}
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INT64),
					types.NewColumn("value", types.INT64),
				},
				nil,
			),
		),
	)
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	files := make([]string, len(targetSourceFiles)+len(nonTargetSourceFiles))
	copy(files, targetSourceFiles)
	copy(files[len(targetSourceFiles):], nonTargetSourceFiles)
	var initialObjects []fakestorage.Object
	for i, file := range files {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		for j := 0; j < 3; j++ {
			if err := enc.Encode(map[string]interface{}{
				"id":    i*10 + j + 1,
				"value": (i+1)*10 + j + 1,
			}); err != nil {
				t.Fatal(err)
			}
		}
		initialObjects = append(initialObjects, fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: bucketName,
				Name:       file,
				Size:       int64(len(buf.Bytes())),
			},
			Content: buf.Bytes(),
		})
	}

	storageServer, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: initialObjects,
		PublicHost:     publicHost,
		Scheme:         "http",
	})
	if err != nil {
		t.Fatal(err)
	}

	storageServerURL := storageServer.URL()
	u, err := url.Parse(storageServerURL)
	if err != nil {
		t.Fatal(err)
	}
	storageEmulatorHost := fmt.Sprintf("http://%s:%s", publicHost, u.Port())
	t.Setenv("STORAGE_EMULATOR_HOST", storageEmulatorHost)

	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
		storageServer.Stop()
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

	gcsSourceURL := fmt.Sprintf("gs://%s/%s", bucketName, sourceName)
	gcsRef := bigquery.NewGCSReference(gcsSourceURL)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.AutoDetect = true
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Err() != nil {
		t.Fatal(status.Err())
	}

	query := client.Query(fmt.Sprintf("SELECT * FROM %s.%s", datasetID, tableID))
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type row struct {
		ID    int64
		Value int64
	}
	var rows []*row
	for {
		var r row
		if err := it.Next(&r); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rows = append(rows, &r)
	}
	// WRITE_TRUNCATE truncates the original rows from initialObjects
	if diff := cmp.Diff([]*row{
		{ID: 11, Value: 21},
		{ID: 12, Value: 22},
		{ID: 13, Value: 23},
	}, rows); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}

func TestExportToGCS(t *testing.T) {
	const (
		projectID  = "test"
		datasetID  = "dataset1"
		tableID    = "table_a"
		publicHost = "127.0.0.1"
		bucketName = "test-export-bucket"
	)

	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INT64),
					types.NewColumn("value", types.INT64),
				},
				types.Data{
					{"id": int64(1), "value": int64(21)},
					{"id": int64(2), "value": int64(22)},
					{"id": int64(3), "value": int64(23)},
				},
			),
		),
	)
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	storageServer, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		PublicHost: publicHost,
		Scheme:     "http",
	})
	if err != nil {
		t.Fatal(err)
	}

	storageServerURL := storageServer.URL()
	u, err := url.Parse(storageServerURL)
	if err != nil {
		t.Fatal(err)
	}
	storageEmulatorHost := fmt.Sprintf("http://%s:%s", publicHost, u.Port())
	t.Setenv("STORAGE_EMULATOR_HOST", storageEmulatorHost)

	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
		storageServer.Stop()
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

	type T struct {
		ID    int64
		Value int64
	}

	storageClient, err := storage.NewClient(
		ctx,
		option.WithEndpoint(fmt.Sprintf("%s/storage/v1/", storageEmulatorHost)),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("csv", func(t *testing.T) {
		sourceName := "path/to/data.csv"
		gcsSourceURL := fmt.Sprintf("gs://%s/%s", bucketName, sourceName)
		gcsRef := bigquery.NewGCSReference(gcsSourceURL)
		gcsRef.DestinationFormat = bigquery.CSV
		gcsRef.FieldDelimiter = ","
		extractor := client.DatasetInProject(projectID, datasetID).Table(tableID).ExtractorTo(gcsRef)
		extractor.DisableHeader = true
		job, err := extractor.Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := status.Err(); err != nil {
			t.Fatal(err)
		}
		reader, err := storageClient.Bucket(bucketName).Object(sourceName).NewReader(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		records, err := csv.NewReader(reader).ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		var rows []*T
		for _, record := range records {
			id, err := strconv.ParseInt(record[0], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			value, err := strconv.ParseInt(record[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, &T{ID: id, Value: value})
		}
		if diff := cmp.Diff([]*T{
			{ID: 1, Value: 21},
			{ID: 2, Value: 22},
			{ID: 3, Value: 23},
		}, rows); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})

	t.Run("json", func(t *testing.T) {
		sourceName := "path/to/data.json"
		gcsSourceURL := fmt.Sprintf("gs://%s/%s", bucketName, sourceName)
		gcsRef := bigquery.NewGCSReference(gcsSourceURL)
		gcsRef.DestinationFormat = bigquery.JSON
		extractor := client.DatasetInProject(projectID, datasetID).Table(tableID).ExtractorTo(gcsRef)
		job, err := extractor.Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := status.Err(); err != nil {
			t.Fatal(err)
		}
		reader, err := storageClient.Bucket(bucketName).Object(sourceName).NewReader(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		dec := json.NewDecoder(reader)
		var rows []*T
		for dec.More() {
			var v struct {
				ID    json.Number
				Value json.Number
			}
			if err := dec.Decode(&v); err != nil {
				t.Fatal(err)
			}
			id, err := v.ID.Int64()
			if err != nil {
				t.Fatal(err)
			}
			value, err := v.Value.Int64()
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, &T{
				ID:    id,
				Value: value,
			})
		}
		if diff := cmp.Diff([]*T{
			{ID: 1, Value: 21},
			{ID: 2, Value: 22},
			{ID: 3, Value: 23},
		}, rows); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	})
}

func TestQueryWithNamedParams(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "test_dataset"
		tableID   = "test_table"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("created_at", types.TIMESTAMP),
					types.NewColumn("item", types.STRING),
					types.NewColumn("qty", types.NUMERIC),
				},
				types.Data{
					{
						"created_at": time.Now(),
						"item":       "something",
						"qty":        "123.45",
					},
				},
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
	query := client.Query(`
SELECT
 item, qty
FROM test_dataset.test_table
WHERE
 created_at > @someday AND
 qty >= @min_qty AND
 created_at > @someday - INTERVAL 1 DAY
ORDER BY qty DESC;`)
	query.Parameters = []bigquery.QueryParameter{
		{
			Name:  "min_qty",
			Value: 100,
		},
		{
			Name:  "someday",
			Value: time.Date(2022, 9, 9, 0, 0, 0, 0, time.UTC),
		},
	}

	job, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := status.Err(); err != nil {
		t.Fatal(err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var rowCount int
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		rowCount++
		t.Log(row)
	}
	if rowCount != 1 {
		t.Fatal("failed to get result")
	}

	query = client.Query("SELECT * FROM `test.test_dataset.test_table` WHERE @parameter IS NULL OR 'target text' = @parameter")
	query.Parameters = []bigquery.QueryParameter{
		{
			Name: "parameter",
			Value: &bigquery.QueryParameterValue{
				Type: bigquery.StandardSQLDataType{
					TypeKind: "STRING",
				},
				Value: "test",
			},
		},
	}
	it, err = query.Read(ctx)

	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err != iterator.Done {
				t.Fatal(err)
			}
			break
		}
		if len(row) != 3 {
			t.Fatalf("failed to get row: %v", row)
		}
	}

	query = client.Query("SELECT * FROM UNNEST(@states)")
	query.Parameters = []bigquery.QueryParameter{
		{
			Name:  "states",
			Value: []string{"WA", "VA", "WV", "WY"},
		},
	}
	it, err = query.Read(ctx)

	if err != nil {
		t.Fatal(err)
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err != iterator.Done {
				t.Fatal(err)
			}
			break
		}
		if len(row) != 1 {
			t.Fatalf("failed to get row: %v", row)
		}
	}
}

// TestQueryWithNumericParameters tests issue #58: https://github.com/Recidiviz/bigquery-emulator/issues/58
// Verifies that numeric query parameters (INT64, FLOAT64) work correctly in various SQL contexts,
// particularly in LIMIT clauses where the Node.js client sends numbers as JSON numbers (not strings).
func TestQueryWithNumericParameters(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "test_dataset"
		tableID   = "test_table"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}

	// Create test table with 5 rows of sample data
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INTEGER),
					types.NewColumn("name", types.STRING),
					types.NewColumn("value", types.FLOAT),
				},
				types.Data{
					{"id": 1, "name": "Alice", "value": 10.5},
					{"id": 2, "name": "Bob", "value": 20.7},
					{"id": 3, "name": "Charlie", "value": 30.2},
					{"id": 4, "name": "David", "value": 40.9},
					{"id": 5, "name": "Eve", "value": 50.1},
				},
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

	t.Run("INT64 in LIMIT clause", func(t *testing.T) {
		// This is the exact scenario from issue #58:
		// Query with LIMIT using a numeric parameter
		query := client.Query(`
			SELECT * FROM test_dataset.test_table
			ORDER BY id ASC
			LIMIT @limit
		`)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "limit", Value: 2}, // Pass as int, not string
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatalf("iterator.Next failed: %v", err)
			}
			rows = append(rows, row)
		}

		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		// Verify we got Alice and Bob (id 1 and 2)
		if rows[0][0].(int64) != 1 || rows[1][0].(int64) != 2 {
			t.Errorf("unexpected row ids: got %v and %v", rows[0][0], rows[1][0])
		}
	})

	t.Run("INT64 in WHERE clause", func(t *testing.T) {
		query := client.Query(`
			SELECT * FROM test_dataset.test_table
			WHERE id = @id
		`)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "id", Value: 3}, // Numeric parameter
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatalf("iterator.Next failed: %v", err)
			}
			rows = append(rows, row)
		}

		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0][1].(string) != "Charlie" {
			t.Errorf("expected Charlie, got %v", rows[0][1])
		}
	})

	t.Run("INT64 with comparison operators", func(t *testing.T) {
		query := client.Query(`
			SELECT * FROM test_dataset.test_table
			WHERE id >= @minId
			ORDER BY id ASC
		`)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "minId", Value: 4}, // Numeric parameter
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatalf("iterator.Next failed: %v", err)
			}
			rows = append(rows, row)
		}

		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		// Should get David and Eve (id 4 and 5)
		if rows[0][0].(int64) != 4 || rows[1][0].(int64) != 5 {
			t.Errorf("unexpected row ids: got %v and %v", rows[0][0], rows[1][0])
		}
	})

	t.Run("FLOAT64 parameter", func(t *testing.T) {
		query := client.Query(`
			SELECT * FROM test_dataset.test_table
			WHERE value > @threshold
			ORDER BY id ASC
		`)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "threshold", Value: 25.5}, // Float parameter
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatalf("iterator.Next failed: %v", err)
			}
			rows = append(rows, row)
		}

		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		// Should get Charlie, David, and Eve
		if rows[0][1].(string) != "Charlie" || rows[1][1].(string) != "David" || rows[2][1].(string) != "Eve" {
			t.Errorf("unexpected names: got %v, %v, %v", rows[0][1], rows[1][1], rows[2][1])
		}
	})

	t.Run("Multiple numeric parameters", func(t *testing.T) {
		query := client.Query(`
			SELECT * FROM test_dataset.test_table
			WHERE id >= @minId AND id <= @maxId
			ORDER BY id ASC
		`)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "minId", Value: 2}, // Numeric parameter
			{Name: "maxId", Value: 4}, // Numeric parameter
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatalf("iterator.Next failed: %v", err)
			}
			rows = append(rows, row)
		}

		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		// Should get Bob, Charlie, and David (id 2, 3, 4)
		if rows[0][0].(int64) != 2 || rows[1][0].(int64) != 3 || rows[2][0].(int64) != 4 {
			t.Errorf("unexpected row ids: got %v, %v, %v", rows[0][0], rows[1][0], rows[2][0])
		}
	})

	t.Run("LIMIT with OFFSET", func(t *testing.T) {
		query := client.Query(`
			SELECT * FROM test_dataset.test_table
			ORDER BY id ASC
			LIMIT @limit OFFSET @offset
		`)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "limit", Value: 2}, // Both as numbers
			{Name: "offset", Value: 2},
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatalf("iterator.Next failed: %v", err)
			}
			rows = append(rows, row)
		}

		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		// Should get Charlie and David (skipped Alice and Bob)
		if rows[0][0].(int64) != 3 || rows[1][0].(int64) != 4 {
			t.Errorf("unexpected row ids: got %v and %v", rows[0][0], rows[1][0])
		}
	})

	t.Run("Zero and negative values", func(t *testing.T) {
		// Test zero as parameter value
		query := client.Query(`
			SELECT * FROM test_dataset.test_table
			WHERE id > @minId
			ORDER BY id ASC
		`)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "minId", Value: 0}, // Zero as numeric parameter
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var rowCount int
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatalf("iterator.Next failed: %v", err)
			}
			rowCount++
		}

		if rowCount != 5 {
			t.Fatalf("expected 5 rows with id > 0, got %d", rowCount)
		}

		// Test negative number
		query2 := client.Query(`SELECT @negativeValue as result`)
		query2.Parameters = []bigquery.QueryParameter{
			{Name: "negativeValue", Value: -42},
		}

		it2, err := query2.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var row []bigquery.Value
		if err := it2.Next(&row); err != nil {
			t.Fatalf("iterator.Next failed: %v", err)
		}

		if row[0].(int64) != -42 {
			t.Errorf("expected -42, got %v", row[0])
		}
	})
}

// TestQueryWithNullParameters tests issue #312: https://github.com/goccy/bigquery-emulator/issues/312
// Verifies that null parameters work correctly in IS NULL conditions.
// The issue reported that null string parameters in WHERE clauses with
// "IS NULL OR parameter = value" patterns caused type inference errors.
func TestQueryWithNullParameters(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "test_dataset"
		tableID   = "test_table"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}

	// Create test table with sample data
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INTEGER),
					types.NewColumn("name", types.STRING),
				},
				types.Data{
					{"id": 1, "name": "Alice"},
					{"id": 2, "name": "Bob"},
					{"id": 3, "name": "Charlie"},
				},
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

	t.Run("Null string parameter with IS NULL check", func(t *testing.T) {
		// Test with null parameter - should return all rows
		query := client.Query(`
			SELECT id, name
			FROM test_dataset.test_table
			WHERE @parameter IS NULL OR name = @parameter
			ORDER BY id
		`)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "parameter", Value: bigquery.NullString{}}, // Null parameter
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatalf("iterator.Next failed: %v", err)
			}
			rows = append(rows, row)
		}

		// Since parameter is null, the IS NULL condition is true,
		// so all rows should be returned
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		// Verify we got all three people
		if rows[0][0].(int64) != 1 || rows[1][0].(int64) != 2 || rows[2][0].(int64) != 3 {
			t.Errorf("unexpected row ids: got %v, %v, %v", rows[0][0], rows[1][0], rows[2][0])
		}
	})

	t.Run("Null parameter with specific value fallback", func(t *testing.T) {
		// Test with specific value - should filter
		query := client.Query(`
			SELECT id, name
			FROM test_dataset.test_table
			WHERE @parameter IS NULL OR name = @parameter
			ORDER BY id
		`)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "parameter", Value: "Alice"}, // Non-null parameter
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatalf("iterator.Next failed: %v", err)
			}
			rows = append(rows, row)
		}

		// Should only get Alice
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0][1].(string) != "Alice" {
			t.Errorf("expected Alice, got %v", rows[0][1])
		}
	})

	t.Run("Null numeric parameter", func(t *testing.T) {
		query := client.Query(`
			SELECT id, name
			FROM test_dataset.test_table
			WHERE @numParam IS NULL OR id = @numParam
			ORDER BY id
		`)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "numParam", Value: bigquery.NullInt64{}},
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var rowCount int
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatalf("iterator.Next failed: %v", err)
			}
			rowCount++
		}

		// Should return all rows since parameter is null
		if rowCount != 3 {
			t.Fatalf("expected 3 rows, got %d", rowCount)
		}
	})

	t.Run("Multiple null parameters", func(t *testing.T) {
		query := client.Query(`
			SELECT id, name
			FROM test_dataset.test_table
			WHERE (@param1 IS NULL OR id = @param1)
			  AND (@param2 IS NULL OR name = @param2)
			ORDER BY id
		`)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "param1", Value: bigquery.NullInt64{}},
			{Name: "param2", Value: bigquery.NullString{}},
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("query.Read failed: %v", err)
		}

		var rowCount int
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatalf("iterator.Next failed: %v", err)
			}
			rowCount++
		}

		// Both are null, so all rows match
		if rowCount != 3 {
			t.Fatalf("expected 3 rows, got %d", rowCount)
		}
	})
}

func TestMultipleProject(t *testing.T) {
	const (
		mainProjectID = "main_project"
		mainDatasetID = "main_dataset"
		mainTableID   = "main_table"
		subProjectID  = "sub_project"
		subDatasetID  = "sub_dataset"
		subTableID    = "sub_table"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				mainProjectID,
				types.NewDataset(
					mainDatasetID,
					types.NewTable(
						mainTableID,
						[]*types.Column{
							types.NewColumn("name", types.STRING),
						},
						types.Data{
							{"name": "main-project-name-data"},
						},
					),
				),
			),
		),
		server.StructSource(
			types.NewProject(
				subProjectID,
				types.NewDataset(
					subDatasetID,
					types.NewTable(
						subTableID,
						[]*types.Column{
							types.NewColumn("name", types.STRING),
						},
						types.Data{
							{"name": "sub-project-name-data"},
						},
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
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		mainProjectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	it, err := client.Query("SELECT * FROM sub_project.sub_dataset.sub_table").Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var row []bigquery.Value
	if err := it.Next(&row); err != nil {
		if err != iterator.Done {
			t.Fatal(err)
		}
	}
	if len(row) != 1 {
		t.Fatalf("failed to get row. got length %d", len(row))
	}
	name, ok := row[0].(string)
	if !ok {
		t.Fatalf("failed to get row[0]. type is %T", row[0])
	}
	if name != "sub-project-name-data" {
		t.Fatalf("failed to get data from sub project: %s", name)
	}
}

func TestInformationSchema(t *testing.T) {
	const (
		projectID    = "test"
		datasetID    = "test_dataset"
		subProjectID = "sub"
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
						"INFORMATION_SCHEMA.COLUMNS",
						[]*types.Column{
							types.NewColumn("table_schema", types.STRING),
							types.NewColumn("table_name", types.STRING),
							types.NewColumn("column_name", types.STRING),
						},
						types.Data{
							{
								"table_schema": "test_ds",
								"table_name":   "table_type_graph",
								"column_name":  "id",
							},
						},
					),
				),
			),
			types.NewProject(subProjectID),
		),
	); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	t.Run("query from same project", func(t *testing.T) {
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

		it, err := client.Query("SELECT * FROM test_dataset.INFORMATION_SCHEMA.COLUMNS").Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err != iterator.Done {
					t.Fatal(err)
				}
				break
			}
			if len(row) != 3 {
				t.Fatalf("failed to get row: %v", row)
			}
		}
	})

	t.Run("query from sub project", func(t *testing.T) {
		client, err := bigquery.NewClient(
			ctx,
			subProjectID,
			option.WithEndpoint(testServer.URL),
			option.WithoutAuthentication(),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		it, err := client.Query("SELECT * FROM test.test_dataset.INFORMATION_SCHEMA.COLUMNS").Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err != iterator.Done {
					t.Fatal(err)
				}
				break
			}
			if len(row) != 3 {
				t.Fatalf("failed to get row: %v", row)
			}
		}
	})
}

func TestWriteDisposition(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.YAMLSource(filepath.Join("testdata", "data.yaml"))); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	query := client.Query(`
		CREATE TABLE dataset1.existing_table (field INT64);
		INSERT INTO dataset1.existing_table (field) VALUES (123)
	`)
	job, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	query = client.Query("SELECT 456 AS field")
	query.QueryConfig.WriteDisposition = "WRITE_TRUNCATE"
	query.QueryConfig.Dst = &bigquery.Table{
		ProjectID: projectName,
		DatasetID: "dataset1",
		TableID:   "existing_table",
	}
	_, err = query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}

	query = client.Query("SELECT field FROM dataset1.existing_table")
	rows, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var field []bigquery.Value
	err = rows.Next(&field)
	if err != nil {
		t.Fatal(err)
	}
	if len(field) != 1 || field[0].(int64) != 456 {
		t.Fatalf("expected 456 to have replaced 123; got [%d]", field[0])
	}
}

func TestRowAccessPolicy(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.YAMLSource(filepath.Join("testdata", "data.yaml"))); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	query := client.Query(`
		CREATE ROW ACCESS POLICY apac_filter
		ON dataset1.table_a
		GRANT TO ('user:abc@example.com')
		FILTER USING (name = 'abc');
	`)
	job, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := job.Config(); err != nil {
		t.Fatal(err)
	}
	if _, err := job.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	gotJob, err := client.JobFromID(ctx, job.ID())
	if err != nil {
		t.Fatal(err)
	}
	if gotJob.ID() != job.ID() {
		t.Fatalf("failed to get job expected ID %s. but got %s", job.ID(), gotJob.ID())
	}

	query2 := client.Query(`
		DROP ROW ACCESS POLICY apac_filter
		ON dataset1.table_a;
	`)
	job2, err := query2.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := job.Config(); err != nil {
		t.Fatal(err)
	}
	if _, err := job.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = client.JobFromID(ctx, job2.ID())
	if err != nil {
		t.Fatal(err)
	}
	if gotJob.ID() != job.ID() {
		t.Fatalf("failed to get job expected ID %s. but got %s", job.ID(), gotJob.ID())
	}
}

func TestQueryWithoutDestinationTable(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.YAMLSource(filepath.Join("testdata", "data.yaml"))); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	query := client.Query("SELECT id, name FROM dataset1.table_a WHERE id = 1")
	job, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := job.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	config, err := job.Config()
	if err != nil {
		t.Fatal(err)
	}
	qConfig, ok := config.(*bigquery.QueryConfig)
	if !ok {
		t.Fatal("expected QueryConfig")
	}
	if qConfig.Dst == nil {
		t.Fatal("expected destination table to be set")
	}
	if qConfig.Dst.ProjectID != projectName {
		t.Fatalf("expected project ID %s, got %s", projectName, qConfig.Dst.ProjectID)
	}

	expectedDatasetID := "ds_" + job.ID()
	if qConfig.Dst.DatasetID != expectedDatasetID {
		t.Fatalf("expected dataset ID %s, got %s", expectedDatasetID, qConfig.Dst.DatasetID)
	}
	if qConfig.Dst.TableID != job.ID() {
		t.Fatalf("expected table ID %s, got %s", job.ID(), qConfig.Dst.TableID)
	}

	dynamicTableQuery := client.Query(fmt.Sprintf("SELECT * FROM `%s.%s.%s`",
		qConfig.Dst.ProjectID, qConfig.Dst.DatasetID, qConfig.Dst.TableID))
	it, err := dynamicTableQuery.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var rowCount int
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		rowCount++
	}
	if rowCount != 1 {
		t.Fatalf("expected 1 row in dynamic destination table, got %d", rowCount)
	}

	query2 := client.Query("SELECT id FROM dataset1.table_a WHERE id = 2")
	job2, err := query2.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := job2.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	config2, err := job2.Config()
	if err != nil {
		t.Fatal(err)
	}
	qConfig2 := config2.(*bigquery.QueryConfig)
	if qConfig2.Dst == nil {
		t.Fatal("expected destination table to be set for second job")
	}
	if qConfig.Dst.DatasetID == qConfig2.Dst.DatasetID {
		t.Fatalf("expected different dataset IDs, both got %s", qConfig.Dst.DatasetID)
	}

	expectedDatasetID2 := "ds_" + job2.ID()
	if qConfig2.Dst.DatasetID != expectedDatasetID2 {
		t.Fatalf("expected dataset ID %s, got %s", expectedDatasetID2, qConfig2.Dst.DatasetID)
	}
}

func TestPatchTable(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.YAMLSource(filepath.Join("testdata", "data.yaml"))); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	description := "new description!"
	metadata, err := client.Dataset("dataset1").Table("table_a").Update(
		ctx,
		bigquery.TableMetadataToUpdate{
			Description: description,
		},
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Description != description {
		t.Fatalf("Expected updated description; got [%s]", metadata.Description)
	}

	metadata, err = client.Dataset("dataset1").Table("table_a").Update(
		ctx,
		bigquery.TableMetadataToUpdate{
			Schema: bigquery.Schema{
				{Name: "test_col", Type: bigquery.StringFieldType},
			},
		},
		"",
	)
	if err == nil || !strings.Contains(err.Error(), "schema updates unsupported") {
		t.Fatalf("expected unsupported schema update error; got [%s]", err)
	}
}

func TestViewSchemaHydration(t *testing.T) {
	ctx := context.Background()

	const (
		projectName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.SetProject(projectName); err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.YAMLSource(filepath.Join("testdata", "data.yaml"))); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	view := client.Dataset("dataset1").Table("test_view")

	if err := view.Create(ctx, &bigquery.TableMetadata{
		Name:      "test_view",
		ViewQuery: "SELECT id, name FROM dataset1.table_a",
	}); err != nil {
		t.Fatal(err)
	}

	if viewMetadata, err := client.Dataset("dataset1").Table("test_view").Metadata(ctx); err != nil {
		t.Fatal(err)
	} else {
		if diff := cmp.Diff(viewMetadata.Schema, bigquery.Schema{
			&bigquery.FieldSchema{Name: "id", Type: "INTEGER"},
			&bigquery.FieldSchema{Name: "name", Type: "STRING"},
		}, cmpopts.EquateApproxTime(1*time.Second)); diff != "" {
			t.Errorf("(-want +got):\n%s", diff)
		}
	}
}

// TestQueryWithPositionalParameters tests issue #69: https://github.com/Recidiviz/bigquery-emulator/issues/69
// Verifies that positional query parameters (?) work correctly and are not broken by allow_undeclared_parameters mode.
// The issue reports that v0.6.6-recidiviz.3.5 broke positional parameters because allow_undeclared_parameters was
// enabled globally. According to ZetaSQL docs: "When allow_undeclared_parameters is true, no positional parameters may be provided."
func TestQueryWithPositionalParameters(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "test_dataset"
		tableID   = "test_table"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}

	// Create test table with sample data for testing positional parameters
	project := types.NewProject(
		projectID,
		types.NewDataset(
			datasetID,
			types.NewTable(
				tableID,
				[]*types.Column{
					types.NewColumn("id", types.INTEGER),
					types.NewColumn("name", types.STRING),
					types.NewColumn("value", types.FLOAT),
					types.NewColumn("active", types.BOOLEAN),
				},
				types.Data{
					{"id": 1, "name": "Alice", "value": 10.5, "active": true},
					{"id": 2, "name": "Bob", "value": 20.7, "active": false},
					{"id": 3, "name": "Charlie", "value": 30.2, "active": true},
					{"id": 4, "name": "David", "value": 40.9, "active": false},
					{"id": 5, "name": "Eve", "value": 50.1, "active": true},
				},
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

	t.Run("Multiple positional parameters", func(t *testing.T) {
		query := client.Query(fmt.Sprintf("SELECT id, name, value FROM `%s.%s.%s` WHERE id >= ? AND id <= ? ORDER BY id", projectID, datasetID, tableID))
		query.Parameters = []bigquery.QueryParameter{
			{Value: 2},
			{Value: 4},
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			rows = append(rows, row)
		}

		if len(rows) != 3 {
			t.Fatalf("Expected 3 rows, got %d", len(rows))
		}
		// Verify we got ids 2, 3, 4
		expectedIDs := []int64{2, 3, 4}
		for i, row := range rows {
			if row[0].(int64) != expectedIDs[i] {
				t.Errorf("Row %d: expected id=%d, got %v", i, expectedIDs[i], row[0])
			}
		}
	})

	t.Run("Positional parameters with different types", func(t *testing.T) {
		query := client.Query(fmt.Sprintf("SELECT id, name FROM `%s.%s.%s` WHERE value > ? AND active = ? ORDER BY id", projectID, datasetID, tableID))
		query.Parameters = []bigquery.QueryParameter{
			{Value: 25.0}, // Float
			{Value: true}, // Boolean
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			rows = append(rows, row)
		}

		// Should get Charlie (value=30.2, active=true) and Eve (value=50.1, active=true)
		if len(rows) != 2 {
			t.Fatalf("Expected 2 rows, got %d", len(rows))
		}
		expectedNames := []string{"Charlie", "Eve"}
		for i, row := range rows {
			if row[1].(string) != expectedNames[i] {
				t.Errorf("Row %d: expected name='%s', got %v", i, expectedNames[i], row[1])
			}
		}
	})

	t.Run("Positional parameter in LIMIT clause", func(t *testing.T) {
		query := client.Query(fmt.Sprintf("SELECT id, name FROM `%s.%s.%s` ORDER BY id LIMIT ?", projectID, datasetID, tableID))
		query.Parameters = []bigquery.QueryParameter{
			{Value: 2},
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			rows = append(rows, row)
		}

		if len(rows) != 2 {
			t.Fatalf("Expected 2 rows, got %d", len(rows))
		}
		// Should get first 2 rows: Alice and Bob
		if rows[0][1].(string) != "Alice" {
			t.Errorf("Expected first row name='Alice', got %v", rows[0][1])
		}
		if rows[1][1].(string) != "Bob" {
			t.Errorf("Expected second row name='Bob', got %v", rows[1][1])
		}
	})

	t.Run("Positional parameter with string type", func(t *testing.T) {
		query := client.Query(fmt.Sprintf("SELECT id, name FROM `%s.%s.%s` WHERE name = ? ORDER BY id", projectID, datasetID, tableID))
		query.Parameters = []bigquery.QueryParameter{
			{Value: "Bob"},
		}

		it, err := query.Read(ctx)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		var rows [][]bigquery.Value
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				t.Fatal(err)
			}
			rows = append(rows, row)
		}

		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}
		if rows[0][0].(int64) != 2 {
			t.Errorf("Expected id=2, got %v", rows[0][0])
		}
		if rows[0][1].(string) != "Bob" {
			t.Errorf("Expected name='Bob', got %v", rows[0][1])
		}
	})
}
