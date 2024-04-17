package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

type Project struct {
	ID   string
	mu   sync.RWMutex
	repo *Repository
}

func (p *Project) DatasetIDs(ctx context.Context) ([]string, error) {
	datasets, err := p.FetchDatasets(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(datasets))
	for i := 0; i < len(datasets); i++ {
		ids[i] = datasets[i].ID
	}
	return ids, nil
}

func (p *Project) Job(ctx context.Context, id string) (*Job, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	job, err := p.repo.FindJob(ctx, p.ID, id)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (p *Project) Dataset(ctx context.Context, id string) (*Dataset, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	dataset, err := p.repo.FindDataset(ctx, p.ID, id)
	if err != nil {
		return nil, err
	}
	return dataset, nil
}

func (p *Project) FetchDatasets(ctx context.Context) ([]*Dataset, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	datasets, err := p.repo.FindDatasetsInProject(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	return datasets, nil
}

func (p *Project) FetchJobs(ctx context.Context) ([]*Job, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	datasets, err := p.repo.FindJobsInProject(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	return datasets, nil
}

func (p *Project) Insert(ctx context.Context, tx *sql.Tx) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.repo.AddProject(ctx, tx, p)
}

func (p *Project) Delete(ctx context.Context, tx *sql.Tx) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.repo.DeleteProject(ctx, tx, p)
}

func (p *Project) AddDataset(ctx context.Context, tx *sql.Tx, dataset *Dataset) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := dataset.Insert(ctx, tx); err != nil {
		return err
	}
	return nil
}

func (p *Project) DeleteDataset(ctx context.Context, tx *sql.Tx, id string) error {
	dataset, err := p.Dataset(ctx, id)
	if err != nil {
		return err
	}
	if dataset == nil {
		return fmt.Errorf("dataset '%s' is not found in project '%s'", id, p.ID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := dataset.Delete(ctx, tx); err != nil {
		return err
	}
	return nil
}

func (p *Project) AddJob(ctx context.Context, tx *sql.Tx, job *Job) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := job.Insert(ctx, tx); err != nil {
		return err
	}
	return nil
}

func (p *Project) DeleteJob(ctx context.Context, tx *sql.Tx, id string) error {
	job, err := p.Job(ctx, id)
	if err != nil {
		return err
	}
	if job == nil {
		return fmt.Errorf("job '%s' is not found in project '%s'", id, p.ID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := job.Delete(ctx, tx); err != nil {
		return err
	}
	return nil
}

func NewProject(repo *Repository, id string) *Project {
	return &Project{
		ID:   id,
		repo: repo,
	}
}
