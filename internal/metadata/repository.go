package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/goccy/bigquery-emulator/internal"
	"github.com/goccy/go-zetasqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
)

var schemata = []string{
	`
CREATE TABLE IF NOT EXISTS projects (
  id         STRING NOT NULL,
  PRIMARY KEY (id)
)`,
	`
CREATE TABLE IF NOT EXISTS jobs (
  id        STRING NOT NULL,
  projectID STRING NOT NULL,
  metadata  STRING,
  result    STRING,
  error     STRING,
  PRIMARY KEY (projectID, id)
)`,
	`
CREATE TABLE IF NOT EXISTS datasets (
  id         STRING NOT NULL,
  projectID  STRING NOT NULL,
  metadata   STRING,
  PRIMARY KEY (projectID, id)
)
`,
	`CREATE TABLE IF NOT EXISTS tables (
  id        STRING NOT NULL,
  projectID STRING NOT NULL,
  datasetID STRING NOT NULL,
  metadata  STRING,
  PRIMARY KEY (projectID, datasetID, id)
)`,
	`
CREATE TABLE IF NOT EXISTS models (
  id        STRING NOT NULL,
  projectID STRING NOT NULL,
  datasetID STRING NOT NULL,
  metadata  STRING,
  PRIMARY KEY (projectID, datasetID, id)
)`,
	`
CREATE TABLE IF NOT EXISTS routines (
  id        STRING NOT NULL,
  projectID STRING NOT NULL,
  datasetID STRING NOT NULL,
  metadata  STRING,
  PRIMARY KEY (projectID, datasetID, id)
)`,
}

type Repository struct {
	db      *sql.DB
	queries *internal.PreparedStatementRepository
}

const (
	StmtFindProject           internal.Statement = `SELECT id FROM projects WHERE id = @id`
	StmtInsertProject         internal.Statement = `INSERT INTO projects (id) VALUES (@id)`
	StmtDeleteProject         internal.Statement = `DELETE FROM projects WHERE id = @id`
	StmtFindJobsInProject     internal.Statement = `SELECT id, projectID, metadata, result, error FROM jobs WHERE projectID = @projectid`
	StmtDeleteTable           internal.Statement = `DELETE FROM tables WHERE projectID = @projectID AND datasetID = @datasetID AND id = @tableID`
	StmtInsertTable           internal.Statement = `INSERT INTO tables (id, projectID, datasetID, metadata) VALUES (@id, @projectID, @datasetID, @metadata)`
	StmtFindTable             internal.Statement = `SELECT id, metadata FROM tables WHERE projectID = @projectID AND datasetID = @datasetID AND id = @tableID`
	StmtUpdateTable           internal.Statement = `UPDATE tables SET metadata = @metadata WHERE projectID = @projectID AND datasetID = @datasetID AND id = @id`
	StmtTableExists           internal.Statement = `SELECT TRUE FROM tables WHERE projectID = @projectID AND datasetID = @datasetID AND id = @tableID`
	StmtFindTablesInDataset   internal.Statement = `SELECT id, datasetID, metadata FROM tables WHERE projectID = @projectID AND datasetID = @datasetID`
	StmtFindModelsInDataset   internal.Statement = `SELECT id, datasetID, metadata FROM models WHERE projectID = @projectID AND datasetID = @datasetID`
	StmtUpdateModel           internal.Statement = `UPDATE models SET metadata = @metadata WHERE projectID = @projectID AND datasetID = @datasetID AND id = @id`
	StmtInsertModel           internal.Statement = `INSERT INTO models (id, projectID, datasetID, metadata) VALUES (@id, @projectID, @datasetID, @metadata)`
	StmtDeleteModel           internal.Statement = `DELETE FROM models WHERE projectID = @projectID AND datasetID = @datasetID AND id = @id`
	StmtFindRoutinesInDataset internal.Statement = `SELECT id, datasetID, metadata FROM routines WHERE projectID = @projectID AND datasetID = @datasetID`
	StmtUpdateRoutine         internal.Statement = `UPDATE routines SET metadata = @metadata WHERE projectID = @projectID AND datasetID = @datasetID AND id = @id`
	StmtInsertRoutine         internal.Statement = `INSERT INTO routines (id, projectID, datasetID, metadata) VALUES (@id, @projectID, @datasetID, @metadata)`
	StmtDeleteRoutine         internal.Statement = `DELETE FROM routines WHERE projectID = @projectID AND datasetID = @datasetID AND id = @id`
	StmtFindJob               internal.Statement = `SELECT id, metadata, result, error FROM jobs WHERE projectID = @projectID AND id = @jobID`
	StmtInsertJob             internal.Statement = `INSERT INTO jobs (id, projectID, metadata, result, error) VALUES (@id, @projectID, @metadata, @result, @error)`
	StmtUpdateJob             internal.Statement = `UPDATE jobs SET metadata = @metadata, result = @result, error = @error WHERE projectID = @projectID AND id = @id`
	StmtDeleteJob             internal.Statement = `DELETE FROM jobs WHERE projectID = @projectID AND id = @jobID`
	StmtFindDataset           internal.Statement = `SELECT id, projectID, metadata FROM datasets WHERE projectID = @projectID AND id = @datasetID`
	StmtInsertDataset         internal.Statement = `INSERT INTO datasets (id, projectID, metadata) VALUES (@id, @projectID, @metadata)`
	StmtDeleteDataset         internal.Statement = `DELETE FROM datasets WHERE projectID = @projectID AND id = @id`
	StmtDatasetExists         internal.Statement = `SELECT TRUE FROM datasets WHERE projectID = @projectID AND id = @datasetID`
	StmtUpdateDataset         internal.Statement = `UPDATE datasets SET metadata = @metadata WHERE projectID = @projectID AND id = @datasetID`
)

var preparedStatements = []internal.Statement{
	StmtFindProject,
	StmtInsertProject,
	StmtDeleteProject,
	StmtFindJobsInProject,
	StmtInsertDataset,
	StmtDeleteTable,
	StmtInsertTable,
	StmtFindTable,
	StmtUpdateTable,
	StmtTableExists,
	StmtFindTablesInDataset,
	StmtFindModelsInDataset,
	StmtFindRoutinesInDataset,
	StmtFindJob,
	StmtInsertJob,
	StmtUpdateJob,
	StmtDeleteJob,
	StmtFindDataset,
	StmtDeleteDataset,
	StmtDatasetExists,
	StmtUpdateDataset,
}

func NewRepository(db *sql.DB) (*Repository, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	for _, ddl := range schemata {
		if _, err := tx.ExecContext(context.Background(), ddl); err != nil {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				return nil, rollbackErr
			}
			return nil, err
		}
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	preparedQueryRepository := internal.NewPreparedStatementRepository(
		db,
		preparedStatements,
	)
	if err != nil {
		return nil, err
	}
	return &Repository{
		db:      db,
		queries: preparedQueryRepository,
	}, nil
}

func (r *Repository) getConnection(ctx context.Context) (*sql.Conn, error) {
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	if err := conn.Raw(func(c interface{}) error {
		zetasqliteConn, ok := c.(*zetasqlite.ZetaSQLiteConn)
		if !ok {
			return fmt.Errorf("failed to get ZetaSQLiteConn from %T", c)
		}
		zetasqliteConn.SetNamePath([]string{})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to setup connection: %w", err)
	}
	return conn, nil
}

func (r *Repository) ProjectFromData(data *types.Project) (*Project, []*Dataset, []*Job) {
	datasets := make([]*Dataset, 0, len(data.Datasets))
	for _, ds := range data.Datasets {
		dataset, _, _, _ := r.DatasetFromData(data.ID, ds)
		datasets = append(datasets, dataset)
	}
	jobs := make([]*Job, 0, len(data.Jobs))
	for _, j := range data.Jobs {
		jobs = append(jobs, r.JobFromData(data.ID, j))
	}
	return NewProject(r, data.ID), datasets, jobs
}

func (r *Repository) DatasetFromData(projectID string, data *types.Dataset) (*Dataset, []*Table, []*Model, []*Routine) {
	tables := make([]*Table, 0, len(data.Tables))
	for _, table := range data.Tables {
		tables = append(tables, r.TableFromData(projectID, data.ID, table))
	}
	models := make([]*Model, 0, len(data.Models))
	for _, model := range data.Models {
		models = append(models, r.ModelFromData(projectID, data.ID, model))
	}
	routines := make([]*Routine, 0, len(data.Routines))
	for _, routine := range data.Routines {
		routines = append(routines, r.RoutineFromData(projectID, data.ID, routine))
	}
	return NewDataset(r, projectID, data.ID, nil), tables, models, routines
}

func (r *Repository) JobFromData(projectID string, data *types.Job) *Job {
	return NewJob(r, projectID, data.ID, nil, nil, nil)
}

func (r *Repository) TableFromData(projectID, datasetID string, data *types.Table) *Table {
	return NewTable(r, projectID, datasetID, data.ID, data.Metadata)
}

func (r *Repository) ModelFromData(projectID, datasetID string, data *types.Model) *Model {
	return NewModel(r, projectID, datasetID, data.ID, data.Metadata)
}

func (r *Repository) RoutineFromData(projectID, datasetID string, data *types.Routine) *Routine {
	return NewRoutine(r, projectID, datasetID, data.ID, data.Metadata)
}

func (r *Repository) FindProjectWithConn(ctx context.Context, tx *sql.Tx, id string) (*Project, error) {
	projects, err := r.findProjects(ctx, tx, []string{id})
	if err != nil {
		return nil, err
	}
	if len(projects) != 1 {
		return nil, nil
	}
	if projects[0].ID != id {
		return nil, nil
	}
	return projects[0], nil
}

func (r *Repository) FindProject(ctx context.Context, id string) (*Project, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	return r.FindProjectWithConn(ctx, tx, id)
}

func (r *Repository) findProjects(ctx context.Context, tx *sql.Tx, ids []string) ([]*Project, error) {
	stmt, err := r.queries.Get(ctx, tx, StmtFindProject)
	if err != nil {
		return nil, err
	}

	projects := []*Project{}
	for _, id := range ids {
		var projectID string
		err := stmt.QueryRowContext(
			ctx,
			sql.Named("id", id),
		).Scan(&projectID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, fmt.Errorf("failed to get projects: %w", err)
		}
		if err != nil {
			return nil, err
		}
		projects = append(projects, NewProject(r, projectID))
	}

	return projects, nil
}

func (r *Repository) FindAllProjects(ctx context.Context) ([]*Project, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	rows, err := tx.QueryContext(ctx, "SELECT id FROM projects")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	projectIDs := []string{}
	for rows.Next() {
		var (
			projectID string
		)
		if err := rows.Scan(&projectID); err != nil {
			return nil, err
		}
		projectIDs = append(projectIDs, projectID)
	}

	projects := []*Project{}
	for _, projectID := range projectIDs {
		projects = append(projects, NewProject(r, projectID))
	}

	return projects, nil
}

func (r *Repository) AddProjectIfNotExists(ctx context.Context, tx *sql.Tx, project *Project) error {
	p, err := r.FindProjectWithConn(ctx, tx, project.ID)
	if err != nil {
		return err
	}
	if p == nil {
		return r.AddProject(ctx, tx, project)
	}
	return nil
}

func (r *Repository) AddProject(ctx context.Context, tx *sql.Tx, project *Project) error {
	stmt, err := r.queries.Get(ctx, tx, StmtInsertProject)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("id", project.ID),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteProject(ctx context.Context, tx *sql.Tx, project *Project) error {
	stmt, err := r.queries.Get(ctx, tx, StmtDeleteProject)
	if err != nil {
		return err
	}

	if _, err := stmt.ExecContext(ctx, sql.Named("id", project.ID)); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindJob(ctx context.Context, projectID, jobID string) (*Job, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	jobs, err := r.findJobs(ctx, tx, projectID, []string{jobID})
	if err != nil {
		return nil, err
	}
	if len(jobs) != 1 {
		return nil, nil
	}
	if jobs[0].ID != jobID {
		return nil, nil
	}
	return jobs[0], nil
}

func (r *Repository) findJobs(ctx context.Context, tx *sql.Tx, projectID string, jobIDs []string) ([]*Job, error) {
	jobs := []*Job{}
	stmt, err := r.queries.Get(ctx, tx, StmtFindJob)
	if err != nil {
		return nil, err
	}

	for _, id := range jobIDs {
		var (
			jobID    string
			metadata string
			result   string
			jobErr   string
		)

		err := stmt.QueryRowContext(
			ctx,
			sql.Named("projectID", projectID),
			sql.Named("jobID", id),
		).Scan(&jobID, &metadata, &result, &jobErr)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}

		var content bigqueryv2.Job
		if len(metadata) > 0 {
			if err := json.Unmarshal([]byte(metadata), &content); err != nil {
				return nil, fmt.Errorf("failed to decode metadata content %s: %w", metadata, err)
			}
		}
		var response internaltypes.QueryResponse
		if len(result) > 0 {
			if err := json.Unmarshal([]byte(result), &response); err != nil {
				return nil, fmt.Errorf("failed to decode job response %s: %w", result, err)
			}
		}
		var resErr error
		if jobErr != "" {
			resErr = errors.New(jobErr)
		}
		jobs = append(
			jobs,
			NewJob(r, projectID, jobID, &content, &response, resErr),
		)
	}

	return jobs, nil
}

func (r *Repository) FindJobsInProject(ctx context.Context, pID string) ([]*Job, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	stmt, err := r.queries.Get(ctx, tx, StmtFindJobsInProject)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(
		ctx,
		sql.Named("projectID", pID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []*Job{}
	for rows.Next() {
		var (
			jobID     string
			projectID string
			metadata  string
			result    string
			jobErr    string
		)
		if err := rows.Scan(&jobID, &projectID, &metadata, &result, &jobErr); err != nil {
			return nil, err
		}
		var content bigqueryv2.Job
		if len(metadata) > 0 {
			if err := json.Unmarshal([]byte(metadata), &content); err != nil {
				return nil, fmt.Errorf("failed to decode metadata content %s: %w", metadata, err)
			}
		}
		var response internaltypes.QueryResponse
		if len(result) > 0 {
			if err := json.Unmarshal([]byte(result), &response); err != nil {
				return nil, fmt.Errorf("failed to decode job response %s: %w", result, err)
			}
		}
		var resErr error
		if jobErr != "" {
			resErr = errors.New(jobErr)
		}
		jobs = append(
			jobs,
			NewJob(r, projectID, jobID, &content, &response, resErr),
		)
	}
	return jobs, nil
}

func (r *Repository) AddJob(ctx context.Context, tx *sql.Tx, job *Job) error {
	metadata, err := json.Marshal(job.content)
	if err != nil {
		return err
	}
	result, err := json.Marshal(job.response)
	if err != nil {
		return err
	}
	var jobErr string
	if job.err != nil {
		jobErr = job.err.Error()
	}
	stmt, err := r.queries.Get(ctx, tx, StmtInsertJob)
	if err != nil {
		return err
	}

	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("id", job.ID),
		sql.Named("projectID", job.ProjectID),
		sql.Named("metadata", string(metadata)),
		sql.Named("result", string(result)),
		sql.Named("error", jobErr),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateJob(ctx context.Context, tx *sql.Tx, job *Job) error {
	metadata, err := json.Marshal(job.content)
	if err != nil {
		return err
	}
	result, err := json.Marshal(job.response)
	if err != nil {
		return err
	}
	var jobErr string
	if job.err != nil {
		jobErr = job.err.Error()
	}
	stmt, err := r.queries.Get(ctx, tx, StmtUpdateJob)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("id", job.ID),
		sql.Named("projectID", job.ProjectID),
		sql.Named("metadata", string(metadata)),
		sql.Named("result", string(result)),
		sql.Named("error", jobErr),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteJob(ctx context.Context, tx *sql.Tx, job *Job) error {
	stmt, err := r.queries.Get(ctx, tx, StmtDeleteJob)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("projectID", job.ProjectID),
		sql.Named("jobID", job.ID),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindDatasetsInProject(ctx context.Context, projectID string) ([]*Dataset, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	rows, err := tx.QueryContext(ctx,
		"SELECT id, metadata FROM datasets WHERE projectID = @projectID",
		sql.Named("projectID", projectID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	datasets := []*Dataset{}
	for rows.Next() {
		var (
			datasetID string
			metadata  string
		)
		if err := rows.Scan(&datasetID, &metadata); err != nil {
			return nil, err
		}
		var content bigqueryv2.Dataset
		if err := json.Unmarshal([]byte(metadata), &content); err != nil {
			return nil, err
		}
		datasets = append(
			datasets,
			NewDataset(r, projectID, datasetID, &content),
		)
	}
	return datasets, nil
}

func (r *Repository) TableExists(ctx context.Context, tx *sql.Tx, projectID, datasetID, tableID string) (bool, error) {
	var exists bool

	stmt, err := r.queries.Get(ctx, tx, StmtTableExists)
	if err != nil {
		return false, err
	}
	err = stmt.QueryRowContext(
		ctx,
		sql.Named("projectID", projectID),
		sql.Named("datasetID", datasetID),
		sql.Named("tableID", tableID),
	).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *Repository) FindDataset(ctx context.Context, projectID, datasetID string) (*Dataset, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	return r.FindDatasetWithConnection(ctx, tx, projectID, datasetID)
}

func (r *Repository) FindDatasetWithConnection(ctx context.Context, tx *sql.Tx, projectID string, datasetID string) (*Dataset, error) {
	datasets, err := r.findDatasets(ctx, tx, projectID, []string{datasetID})
	if err != nil {
		return nil, err
	}
	if len(datasets) != 1 {
		return nil, nil
	}
	if datasets[0].ID != datasetID {
		return nil, nil
	}
	return datasets[0], nil
}

func (r *Repository) findDatasets(ctx context.Context, tx *sql.Tx, projectID string, datasetIDs []string) ([]*Dataset, error) {
	datasets := []*Dataset{}
	stmt, err := r.queries.Get(ctx, tx, StmtFindDataset)
	if err != nil {
		return nil, err
	}
	for _, datasetID := range datasetIDs {
		var (
			resultDatasetID string
			resultProjectID string
			resultMetadata  string
		)
		err := stmt.QueryRowContext(ctx,
			sql.Named("projectID", projectID),
			sql.Named("datasetID", datasetID),
		).Scan(&resultDatasetID, &resultProjectID, &resultMetadata)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}

		var content bigqueryv2.Dataset
		if err := json.Unmarshal([]byte(resultMetadata), &content); err != nil {
			return nil, err
		}
		datasets = append(
			datasets,
			NewDataset(r, projectID, datasetID, &content),
		)
	}

	return datasets, nil
}

func (r *Repository) AddDataset(ctx context.Context, tx *sql.Tx, dataset *Dataset) error {
	metadata, err := json.Marshal(dataset.content)
	if err != nil {
		return err
	}
	stmt, err := r.queries.Get(ctx, tx, StmtInsertDataset)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(ctx,
		sql.Named("id", dataset.ID),
		sql.Named("projectID", dataset.ProjectID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateDataset(ctx context.Context, tx *sql.Tx, dataset *Dataset) error {
	metadata, err := json.Marshal(dataset.content)
	if err != nil {
		return err
	}
	stmt, err := r.queries.Get(ctx, tx, StmtUpdateDataset)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("dataestID", dataset.ID),
		sql.Named("projectID", dataset.ProjectID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteDataset(ctx context.Context, tx *sql.Tx, dataset *Dataset) error {
	stmt, err := r.queries.Get(ctx, tx, StmtDeleteDataset)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		dataset.ProjectID,
		dataset.ID,
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindTable(ctx context.Context, projectID, datasetID, tableID string) (*Table, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	return r.FindTableWithConnection(ctx, tx, projectID, datasetID, tableID)
}

func (r *Repository) FindTableWithConnection(ctx context.Context, tx *sql.Tx, projectID, datasetID, tableID string) (*Table, error) {
	tables, err := r.findTables(ctx, tx, projectID, datasetID, []string{tableID})
	if err != nil {
		return nil, err
	}
	if len(tables) != 1 {
		return nil, nil
	}
	if tables[0].ID != tableID {
		return nil, nil
	}
	return tables[0], nil
}

func (r *Repository) FindTablesInDatasets(ctx context.Context, projectID, datasetID string) ([]*Table, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	return r.FindTablesInDatasetsWithConnection(ctx, tx, projectID, datasetID)
}

func (r *Repository) FindTablesInDatasetsWithConnection(ctx context.Context, tx *sql.Tx, projectID, datasetID string) ([]*Table, error) {
	tablesByDataset, err := r.findTablesInDatasets(ctx, tx, projectID, []string{datasetID})
	if err != nil {
		return nil, err
	}
	tables, exists := tablesByDataset[datasetID]
	if !exists {
		return nil, fmt.Errorf("could not find any tables in dataset [%s]", datasetID)
	}
	return tables, nil
}

func (r *Repository) findTablesInDatasets(ctx context.Context, tx *sql.Tx, projectID string, datasetIDs []string) (map[string][]*Table, error) {
	tables := map[string][]*Table{}

	for _, datasetID := range datasetIDs {
		tables[datasetID] = []*Table{}
	}
	for _, datasetID := range datasetIDs {
		stmt, err := r.queries.Get(ctx, tx, StmtFindTablesInDataset)
		if err != nil {
			return nil, err
		}
		rows, err := stmt.QueryContext(
			ctx,
			sql.Named("projectID", projectID),
			sql.Named("datasetID", datasetID),
		)
		if err != nil {
			rows.Close()
			return nil, err
		}

		for rows.Next() {
			var (
				tableID   string
				datasetID string
				metadata  string
			)
			if err := rows.Scan(&tableID, &datasetID, &metadata); err != nil {
				return nil, err
			}
			var content map[string]interface{}
			if err := json.Unmarshal([]byte(metadata), &content); err != nil {
				return nil, err
			}

			if _, ok := tables[datasetID]; !ok {
				continue
			}

			tables[datasetID] = append(
				tables[datasetID],
				NewTable(r, projectID, datasetID, tableID, content),
			)
		}
		err = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return tables, nil
}

func (r *Repository) findTables(ctx context.Context, tx *sql.Tx, projectID, datasetID string, tableIDs []string) ([]*Table, error) {
	tables := []*Table{}

	stmt, err := r.queries.Get(ctx, tx, StmtFindTable)
	if err != nil {
		return nil, err
	}

	for _, tableID := range tableIDs {
		var (
			resultTableID  string
			resultMetadata string
		)

		err := stmt.QueryRowContext(
			ctx,
			sql.Named("projectID", projectID),
			sql.Named("datasetID", datasetID),
			sql.Named("tableID", tableID),
		).Scan(&resultTableID, &resultMetadata)
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return nil, err
		}

		var content map[string]interface{}
		if err := json.Unmarshal([]byte(resultMetadata), &content); err != nil {
			return nil, err
		}
		tables = append(
			tables,
			NewTable(r, projectID, datasetID, tableID, content),
		)
	}
	return tables, nil
}

func (r *Repository) AddTable(ctx context.Context, tx *sql.Tx, table *Table) error {
	metadata, err := json.Marshal(table.metadata)
	if err != nil {
		return err
	}
	stmt, err := r.queries.Get(ctx, tx, StmtInsertTable)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("id", table.ID),
		sql.Named("projectID", table.ProjectID),
		sql.Named("datasetID", table.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateTable(ctx context.Context, tx *sql.Tx, table *Table) error {
	metadata, err := json.Marshal(table.metadata)
	if err != nil {
		return err
	}
	stmt, err := r.queries.Get(ctx, tx, StmtUpdateTable)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("id", table.ID),
		sql.Named("projectID", table.ProjectID),
		sql.Named("datasetID", table.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteTable(ctx context.Context, tx *sql.Tx, table *Table) error {
	stmt, err := r.queries.Get(ctx, tx, StmtDeleteTable)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("projectID", table.ProjectID),
		sql.Named("datasetID", table.DatasetID),
		sql.Named("tableID", table.ID),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindModelsInDataset(ctx context.Context, projectID, datasetID string) ([]*Model, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	modelsByDataset, err := r.findModelsInDatasets(ctx, tx, projectID, []string{datasetID})
	if err != nil {
		return nil, err
	}
	models, exists := modelsByDataset[datasetID]
	if !exists {
		return nil, fmt.Errorf("could not find any models in dataset [%s]", datasetID)
	}
	return models, nil
}

func (r *Repository) findModelsInDatasets(ctx context.Context, tx *sql.Tx, projectID string, datasetIDs []string) (map[string][]*Model, error) {
	models := map[string][]*Model{}

	stmt, err := r.queries.Get(ctx, tx, StmtFindModelsInDataset)
	if err != nil {
		return nil, err
	}

	for _, datasetID := range datasetIDs {
		models[datasetID] = []*Model{}

		rows, err := stmt.QueryContext(
			ctx,
			sql.Named("projectID", projectID),
			sql.Named("datasetID", datasetID),
		)
		if err != nil {
			rows.Close()
			return nil, err
		}

		for rows.Next() {
			var (
				modelID   string
				datasetID string
				metadata  string
			)
			if err := rows.Scan(&modelID, &datasetID, &metadata); err != nil {
				return nil, err
			}
			var content map[string]interface{}
			if err := json.Unmarshal([]byte(metadata), &content); err != nil {
				return nil, err
			}

			models[datasetID] = append(
				models[datasetID],
				NewModel(r, projectID, datasetID, modelID, content),
			)
		}

		err = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return models, nil
}

func (r *Repository) FindModel(ctx context.Context, projectID, datasetID, modelID string) (*Model, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	models, err := r.findModels(ctx, tx, projectID, datasetID, []string{modelID})
	if err != nil {
		return nil, err
	}
	if len(models) != 1 {
		return nil, nil
	}
	if models[0].ID != modelID {
		return nil, nil
	}
	return models[0], nil
}

func (r *Repository) findModels(ctx context.Context, tx *sql.Tx, projectID, datasetID string, modelIDs []string) ([]*Model, error) {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT id, metadata FROM models WHERE projectID = @projectID AND datasetID = @datasetID AND id IN UNNEST(@modelIDs)",
		sql.Named("projectID", projectID),
		sql.Named("datasetID", datasetID),
		sql.Named("modelIDs", modelIDs),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	models := []*Model{}
	for rows.Next() {
		var (
			modelID  string
			metadata string
		)
		if err := rows.Scan(&modelID, &metadata); err != nil {
			return nil, err
		}
		var content map[string]interface{}
		if err := json.Unmarshal([]byte(metadata), &content); err != nil {
			return nil, err
		}
		models = append(
			models,
			NewModel(r, projectID, datasetID, modelID, content),
		)
	}
	return models, nil
}

func (r *Repository) FindRoutinesInDataset(ctx context.Context, projectID, datasetID string) ([]*Routine, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	routinesByDataset, err := r.findRoutinesInDatasets(ctx, tx, projectID, []string{datasetID})
	if err != nil {
		return nil, err
	}
	routines, exists := routinesByDataset[datasetID]
	if !exists {
		return nil, fmt.Errorf("could not find any routines in dataset [%s]", datasetID)
	}
	return routines, nil
}

func (r *Repository) findRoutinesInDatasets(ctx context.Context, tx *sql.Tx, projectID string, datasetIDs []string) (map[string][]*Routine, error) {
	routines := map[string][]*Routine{}

	stmt, err := r.queries.Get(ctx, tx, StmtFindRoutinesInDataset)
	if err != nil {
		return nil, err
	}
	for _, datasetID := range datasetIDs {
		rows, err := stmt.QueryContext(
			ctx,
			sql.Named("projectID", projectID),
			sql.Named("datasetID", datasetID),
		)
		if err != nil {
			rows.Close()
			return nil, err
		}

		for _, datasetID := range datasetIDs {
			routines[datasetID] = []*Routine{}
		}
		for rows.Next() {
			var (
				routineID string
				datasetID string
				metadata  string
			)
			if err := rows.Scan(&routineID, &datasetID, &metadata); err != nil {
				return nil, err
			}
			var content map[string]interface{}
			if err := json.Unmarshal([]byte(metadata), &content); err != nil {
				return nil, err
			}

			routines[datasetID] = append(
				routines[datasetID],
				NewRoutine(r, projectID, datasetID, routineID, content),
			)
		}
		err = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return routines, nil
}

func (r *Repository) AddModel(ctx context.Context, tx *sql.Tx, model *Model) error {
	metadata, err := json.Marshal(model.metadata)
	if err != nil {
		return err
	}

	stmt, err := r.queries.Get(ctx, tx, StmtInsertModel)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("id", model.ID),
		sql.Named("projectID", model.ProjectID),
		sql.Named("datasetID", model.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateModel(ctx context.Context, tx *sql.Tx, model *Model) error {
	metadata, err := json.Marshal(model.metadata)
	if err != nil {
		return err
	}

	stmt, err := r.queries.Get(ctx, tx, StmtUpdateModel)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("id", model.ID),
		sql.Named("projectID", model.ProjectID),
		sql.Named("datasetID", model.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteModel(ctx context.Context, tx *sql.Tx, model *Model) error {
	stmt, err := r.queries.Get(ctx, tx, StmtDeleteModel)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("projectID", model.ProjectID),
		sql.Named("datasetID", model.DatasetID),
		sql.Named("id", model.ID),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindRoutine(ctx context.Context, projectID, datasetID, routineID string) (*Routine, error) {
	conn, err := r.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	routines, err := r.findRoutines(ctx, tx, projectID, datasetID, []string{routineID})
	if err != nil {
		return nil, err
	}
	if len(routines) != 1 {
		return nil, nil
	}
	if routines[0].ID != routineID {
		return nil, nil
	}
	return routines[0], nil
}

func (r *Repository) findRoutines(ctx context.Context, tx *sql.Tx, projectID, datasetID string, routineIDs []string) ([]*Routine, error) {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT id, metadata FROM routines WHERE projectID = @projectID AND datasetID = @datasetID AND id IN UNNEST(@routineIDs)",
		sql.Named("projectID", projectID),
		sql.Named("datasetID", datasetID),
		sql.Named("routineIDs", routineIDs),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	routines := []*Routine{}
	for rows.Next() {
		var (
			routineID string
			metadata  string
		)
		if err := rows.Scan(&routineID, &metadata); err != nil {
			return nil, err
		}
		var content map[string]interface{}
		if err := json.Unmarshal([]byte(metadata), &content); err != nil {
			return nil, err
		}
		routines = append(
			routines,
			NewRoutine(r, projectID, datasetID, routineID, content),
		)
	}
	return routines, nil
}

func (r *Repository) AddRoutine(ctx context.Context, tx *sql.Tx, routine *Routine) error {
	metadata, err := json.Marshal(routine.metadata)
	if err != nil {
		return err
	}
	stmt, err := r.queries.Get(ctx, tx, StmtInsertRoutine)
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("id", routine.ID),
		sql.Named("projectID", routine.ProjectID),
		sql.Named("datasetID", routine.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateRoutine(ctx context.Context, tx *sql.Tx, routine *Routine) error {
	metadata, err := json.Marshal(routine.metadata)
	if err != nil {
		return err
	}
	stmt, err := r.queries.Get(ctx, tx, StmtUpdateRoutine)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("id", routine.ID),
		sql.Named("projectID", routine.ProjectID),
		sql.Named("datasetID", routine.DatasetID),
		sql.Named("metadata", string(metadata)),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteRoutine(ctx context.Context, tx *sql.Tx, routine *Routine) error {
	stmt, err := r.queries.Get(ctx, tx, StmtDeleteRoutine)
	if err != nil {
		return err
	}
	if _, err := stmt.ExecContext(
		ctx,
		sql.Named("projectID", routine.ProjectID),
		sql.Named("datasetID", routine.DatasetID),
		sql.Named("id", routine.ID),
	); err != nil {
		return err
	}
	return nil
}

func (r *Repository) convertToStrings(v []interface{}) []string {
	ret := make([]string, 0, len(v))
	for _, vv := range v {
		ret = append(ret, vv.(string))
	}
	return ret
}
