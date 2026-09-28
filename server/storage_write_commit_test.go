package server_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/bigquery"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/GoogleCloudPlatform/golang-samples/bigquery/snippets/managedwriter/exampleproto"
	"github.com/goccy/bigquery-emulator/server"
	"google.golang.org/api/option"

	"github.com/goccy/bigquery-emulator/types"
)

type pendingStreamFixture struct {
	writeClient   *managedwriter.Client
	bqClient      *bigquery.Client
	table         *bigquery.Table
	managedStream *managedwriter.ManagedStream
}

// newPendingStreamFixture starts an emulator with the exampleproto.SampleData table
// and opens a PENDING write stream against it.
func newPendingStreamFixture(t *testing.T) *pendingStreamFixture {
	t.Helper()
	const (
		projectID = "test"
		datasetID = "test"
		tableID   = "sample"
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
							types.NewColumn("bool_col", types.BOOL),
							types.NewColumn("bytes_col", types.BYTES),
							types.NewColumn("float64_col", types.FLOAT64),
							types.NewColumn("int64_col", types.INT64),
							types.NewColumn("string_col", types.STRING),
							types.NewColumn("date_col", types.DATE),
							types.NewColumn("datetime_col", types.DATETIME),
							types.NewColumn("geography_col", types.GEOGRAPHY),
							types.NewColumn("numeric_col", types.NUMERIC),
							types.NewColumn("bignumeric_col", types.BIGNUMERIC),
							types.NewColumn("time_col", types.TIME),
							types.NewColumn("timestamp_col", types.TIMESTAMP),
							types.NewColumn("int64_list", types.INT64, types.ColumnMode(types.RepeatedMode)),
							types.NewColumn(
								"struct_col",
								types.STRUCT,
								types.ColumnFields(
									types.NewColumn("sub_int_col", types.INT64),
								),
							),
							types.NewColumn(
								"struct_list",
								types.STRUCT,
								types.ColumnFields(
									types.NewColumn("sub_int_col", types.INT64),
								),
								types.ColumnMode(types.RepeatedMode),
							),
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
	t.Cleanup(func() {
		testServer.Close()
		bqServer.Close()
	})

	opts, err := testServer.GRPCClientOptions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writeClient, err := managedwriter.NewClient(ctx, projectID, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writeClient.Close() })

	bqClient, err := bigquery.NewClient(
		ctx,
		projectID,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bqClient.Close() })

	fullTableName := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID)
	writeStream, err := writeClient.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: fullTableName,
		WriteStream: &storagepb.WriteStream{
			Type: storagepb.WriteStream_PENDING,
		},
	})
	if err != nil {
		t.Fatalf("CreateWriteStream: %v", err)
	}
	descriptorProto, err := adapt.NormalizeDescriptor((&exampleproto.SampleData{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("NormalizeDescriptor: %v", err)
	}
	managedStream, err := writeClient.NewManagedStream(
		ctx,
		managedwriter.WithStreamName(writeStream.GetName()),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		t.Fatalf("NewManagedStream: %v", err)
	}
	t.Cleanup(func() { managedStream.Close() })

	return &pendingStreamFixture{
		writeClient:   writeClient,
		bqClient:      bqClient,
		table:         bqClient.Dataset(datasetID).Table(tableID),
		managedStream: managedStream,
	}
}

func (f *pendingStreamFixture) appendRows(t *testing.T, ctx context.Context, n int, offset int64) {
	t.Helper()
	rows, err := generateExampleMessages(n)
	if err != nil {
		t.Fatalf("generateExampleMessages: %v", err)
	}
	result, err := f.managedStream.AppendRows(ctx, rows, managedwriter.WithOffset(offset))
	if err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	if _, err := result.GetResult(ctx); err != nil {
		t.Fatalf("AppendRows result: %v", err)
	}
}

func (f *pendingStreamFixture) batchCommit(t *testing.T, ctx context.Context) *storagepb.BatchCommitWriteStreamsResponse {
	t.Helper()
	resp, err := f.writeClient.BatchCommitWriteStreams(ctx, &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       managedwriter.TableParentFromStreamName(f.managedStream.StreamName()),
		WriteStreams: []string{f.managedStream.StreamName()},
	})
	if err != nil {
		t.Fatalf("BatchCommitWriteStreams: %v", err)
	}
	return resp
}

// TestBatchCommitRetryAfterFailedCommit verifies that a stream's buffered rows
// survive a BatchCommitWriteStreams call that fails, so that retrying the commit
// still writes them instead of silently committing nothing.
func TestBatchCommitRetryAfterFailedCommit(t *testing.T) {
	ctx := context.Background()
	f := newPendingStreamFixture(t)

	const numRows = 3
	f.appendRows(t, ctx, numRows, 0)
	if _, err := f.managedStream.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Make the commit fail by removing the destination table.
	md, err := f.table.Metadata(ctx)
	if err != nil {
		t.Fatalf("table Metadata: %v", err)
	}
	if err := f.table.Delete(ctx); err != nil {
		t.Fatalf("table Delete: %v", err)
	}

	resp := f.batchCommit(t, ctx)
	if len(resp.GetStreamErrors()) == 0 {
		t.Fatalf("expected the commit into a deleted table to report stream errors")
	}

	// Restore the table and retry the commit.
	if err := f.table.Create(ctx, &bigquery.TableMetadata{Schema: md.Schema}); err != nil {
		t.Fatalf("table Create: %v", err)
	}
	resp = f.batchCommit(t, ctx)
	if len(resp.GetStreamErrors()) > 0 {
		t.Fatalf("unexpected stream errors on retried commit: %v", resp.GetStreamErrors())
	}

	if got := countRows(t, f.table.Read(ctx)); got != numRows {
		t.Fatalf("expected %d rows after retrying the failed commit but got %d", numRows, got)
	}
}

// TestBatchCommitConcurrentWithAppendRows verifies that every row acknowledged by
// AppendRows on a PENDING stream is committed exactly once, even when
// BatchCommitWriteStreams runs while appends are still in flight.
// Run with -race to also surface the unsynchronized access to the stream's rows.
func TestBatchCommitConcurrentWithAppendRows(t *testing.T) {
	ctx := context.Background()
	f := newPendingStreamFixture(t)

	const (
		numAppends   = 50
		rowsPerBatch = 2
	)

	var (
		wg   sync.WaitGroup
		done atomic.Bool
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !done.Load() {
			// Stream errors are ignored: a commit that rejects the not-yet-finalized
			// stream is acceptable, since the rows are then committed by the final commit below.
			if _, err := f.writeClient.BatchCommitWriteStreams(ctx, &storagepb.BatchCommitWriteStreamsRequest{
				Parent:       managedwriter.TableParentFromStreamName(f.managedStream.StreamName()),
				WriteStreams: []string{f.managedStream.StreamName()},
			}); err != nil {
				t.Errorf("BatchCommitWriteStreams: %v", err)
				return
			}
		}
	}()

	var offset int64
	for i := 0; i < numAppends; i++ {
		f.appendRows(t, ctx, rowsPerBatch, offset)
		offset += rowsPerBatch
	}
	done.Store(true)
	wg.Wait()

	if _, err := f.managedStream.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	resp := f.batchCommit(t, ctx)
	if len(resp.GetStreamErrors()) > 0 {
		t.Fatalf("unexpected stream errors on final commit: %v", resp.GetStreamErrors())
	}

	want := numAppends * rowsPerBatch
	if got := countRows(t, f.table.Read(ctx)); got != want {
		t.Fatalf("expected every acknowledged row to be committed exactly once (%d rows) but got %d", want, got)
	}
}
