package server

import (
	"context"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/metadata"
	"go.uber.org/zap"
)

type (
	serverKey     struct{}
	projectKey    struct{}
	datasetKey    struct{}
	jobKey        struct{}
	tableKey      struct{}
	modelKey      struct{}
	routineKey    struct{}
	connectionKey struct{}
)

func withServer(ctx context.Context, server *Server) context.Context {
	return context.WithValue(ctx, serverKey{}, server)
}

func serverFromContext(ctx context.Context) *Server {
	return ctx.Value(serverKey{}).(*Server)
}

func withProject(ctx context.Context, project *metadata.Project) context.Context {
	return context.WithValue(ctx, projectKey{}, project)
}

func connectionFromContext(ctx context.Context) *connection.ManagedConnection {
	return ctx.Value(connectionKey{}).(*connection.ManagedConnection)
}

func withConnection(ctx context.Context, connection *connection.ManagedConnection) context.Context {
	return context.WithValue(ctx, connectionKey{}, connection)
}

func projectFromContext(ctx context.Context) *metadata.Project {
	return ctx.Value(projectKey{}).(*metadata.Project)
}

func withDataset(ctx context.Context, dataset *metadata.Dataset) context.Context {
	return context.WithValue(ctx, datasetKey{}, dataset)
}

func datasetFromContext(ctx context.Context) *metadata.Dataset {
	return ctx.Value(datasetKey{}).(*metadata.Dataset)
}

func withJob(ctx context.Context, job *metadata.Job) context.Context {
	return context.WithValue(ctx, jobKey{}, job)
}

func jobFromContext(ctx context.Context) *metadata.Job {
	return ctx.Value(jobKey{}).(*metadata.Job)
}

func withTable(ctx context.Context, table *metadata.Table) context.Context {
	return context.WithValue(ctx, tableKey{}, table)
}

func tableFromContext(ctx context.Context) *metadata.Table {
	return ctx.Value(tableKey{}).(*metadata.Table)
}

func withModel(ctx context.Context, model *metadata.Model) context.Context {
	return context.WithValue(ctx, modelKey{}, model)
}

func modelFromContext(ctx context.Context) *metadata.Model {
	return ctx.Value(modelKey{}).(*metadata.Model)
}

func withRoutine(ctx context.Context, routine *metadata.Routine) context.Context {
	return context.WithValue(ctx, routineKey{}, routine)
}

func routineFromContext(ctx context.Context) *metadata.Routine {
	return ctx.Value(routineKey{}).(*metadata.Routine)
}

// ResourceLogFields returns route-scoped resource identifiers when present on the context.
func ResourceLogFields(ctx context.Context) []zap.Field {
	var fields []zap.Field
	if v := ctx.Value(projectKey{}); v != nil {
		if p, ok := v.(*metadata.Project); ok && p != nil {
			fields = append(fields, zap.String("project_id", p.ID))
		}
	}
	if v := ctx.Value(datasetKey{}); v != nil {
		if d, ok := v.(*metadata.Dataset); ok && d != nil {
			fields = append(fields, zap.String("dataset_id", d.ID))
		}
	}
	if v := ctx.Value(jobKey{}); v != nil {
		if j, ok := v.(*metadata.Job); ok && j != nil {
			fields = append(fields, zap.String("job_id", j.ID))
		}
	}
	if v := ctx.Value(tableKey{}); v != nil {
		if t, ok := v.(*metadata.Table); ok && t != nil {
			fields = append(fields,
				zap.String("table_id", t.ID),
				zap.String("table_dataset_id", t.DatasetID),
			)
		}
	}
	if v := ctx.Value(modelKey{}); v != nil {
		if m, ok := v.(*metadata.Model); ok && m != nil {
			fields = append(fields, zap.String("model_id", m.ID))
		}
	}
	if v := ctx.Value(routineKey{}); v != nil {
		if rt, ok := v.(*metadata.Routine); ok && rt != nil {
			fields = append(fields, zap.String("routine_id", rt.ID))
		}
	}
	return fields
}
