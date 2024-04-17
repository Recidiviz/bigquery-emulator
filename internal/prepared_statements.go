package internal

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/goccy/go-zetasqlite"
)

type Statement string

type PreparedStatementBuilder func(ctx context.Context, tx *sql.Tx) (*sql.Stmt, error)

type PreparedStatementRepository struct {
	preparedQueries map[Statement]*sql.Stmt
}

func NewPreparedStatementRepository(db *sql.DB, queries []Statement) *PreparedStatementRepository {
	var preparedQueries = map[Statement]*sql.Stmt{}
	ctx := zetasqlite.WithQueryFormattingDisabled(context.Background())
	for _, query := range queries {
		stmt, err := db.PrepareContext(ctx, string(query))
		if err != nil {
			return nil
		}
		preparedQueries[query] = stmt
	}

	return &PreparedStatementRepository{
		preparedQueries: preparedQueries,
	}
}

func (r *PreparedStatementRepository) Get(ctx context.Context, tx *sql.Tx, name Statement) (*sql.Stmt, error) {
	ctx = zetasqlite.WithQueryFormattingDisabled(ctx)
	if stmt, ok := r.preparedQueries[name]; ok {
		return tx.StmtContext(ctx, stmt), nil
	}

	return nil, fmt.Errorf("could not find prepared statement: %s", name)
}
