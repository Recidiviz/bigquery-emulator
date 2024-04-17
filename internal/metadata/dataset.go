package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

var ErrDuplicatedTable = errors.New("table is already created")

type Dataset struct {
	ID        string
	ProjectID string
	mu        sync.RWMutex
	content   *bigqueryv2.Dataset
	repo      *Repository
}

func (d *Dataset) Content() *bigqueryv2.Dataset {
	return d.content
}

func (d *Dataset) UpdateContentIfExists(newContent *bigqueryv2.Dataset) {
	if newContent.Description != "" {
		d.content.Description = newContent.Description
	}
	if newContent.Etag != "" {
		d.content.Etag = newContent.Etag
	}
	if newContent.FriendlyName != "" {
		d.content.FriendlyName = newContent.FriendlyName
	}
	if d.content.IsCaseInsensitive != newContent.IsCaseInsensitive {
		d.content.IsCaseInsensitive = newContent.IsCaseInsensitive
	}
	if newContent.Kind != "" {
		d.content.Kind = newContent.Kind
	}
	if len(newContent.Labels) != 0 {
		d.content.Labels = newContent.Labels
	}
	if newContent.LastModifiedTime != 0 {
		d.content.LastModifiedTime = newContent.LastModifiedTime
	}
	if newContent.Location != "" {
		d.content.Location = newContent.Location
	}
	if newContent.MaxTimeTravelHours != 0 {
		d.content.MaxTimeTravelHours = newContent.MaxTimeTravelHours
	}
	if newContent.SatisfiesPzs {
		d.content.SatisfiesPzs = newContent.SatisfiesPzs
	}
	if newContent.SelfLink != "" {
		d.content.SelfLink = newContent.SelfLink
	}
}

func (d *Dataset) UpdateContent(newContent *bigqueryv2.Dataset) {
	d.content = newContent
}

func (d *Dataset) Insert(ctx context.Context, tx *sql.Tx) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.repo.AddDataset(ctx, tx, d)
}

func (d *Dataset) Delete(ctx context.Context, tx *sql.Tx) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.repo.DeleteDataset(ctx, tx, d)
}

func (d *Dataset) DeleteModel(ctx context.Context, tx *sql.Tx, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	model, err := d.repo.FindModel(ctx, d.ProjectID, d.ID, id)
	if err != nil {
		return err
	}
	if model != nil {
		return fmt.Errorf("model '%s' is not found in dataset '%s'", id, d.ID)
	}
	if err := model.Delete(ctx, tx); err != nil {
		return err
	}
	return nil
}

func (d *Dataset) AddTable(ctx context.Context, tx *sql.Tx, table *Table) error {
	exists, err := d.repo.TableExists(ctx, tx, d.ProjectID, d.ID, table.ID)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("table %s: %w", table.ID, ErrDuplicatedTable)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if err := table.Insert(ctx, tx); err != nil {
		return err
	}

	if err := d.repo.UpdateDataset(ctx, tx, d); err != nil {
		return err
	}
	return nil
}

func (d *Dataset) Table(ctx context.Context, id string) (*Table, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.repo.FindTable(ctx, d.ProjectID, d.ID, id)
}

func (d *Dataset) Model(ctx context.Context, id string) (*Model, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.repo.FindModel(ctx, d.ProjectID, d.ID, id)
}

func (d *Dataset) Routine(ctx context.Context, id string) (*Routine, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.repo.FindRoutine(ctx, d.ProjectID, d.ID, id)
}

func (d *Dataset) Tables(ctx context.Context) ([]*Table, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.repo.FindTablesInDatasets(ctx, d.ProjectID, d.ID)
}

func (d *Dataset) Models(ctx context.Context) ([]*Model, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.repo.FindModelsInDataset(ctx, d.ProjectID, d.ID)
}

func (d *Dataset) Routines(ctx context.Context) ([]*Routine, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.repo.FindRoutinesInDataset(ctx, d.ProjectID, d.ID)
}

func NewDataset(
	repo *Repository,
	projectID string,
	datasetID string,
	content *bigqueryv2.Dataset,
) *Dataset {
	return &Dataset{
		ID:        datasetID,
		ProjectID: projectID,
		content:   content,
		repo:      repo,
	}
}
