package connection

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/vantaboard/go-googlesqlite"
)

const (
	// DefaultPoolSize is the default number of pre-initialized connections in the pool.
	// This balances between connection reuse and resource usage.
	DefaultPoolSize = 5
)

type Manager struct {
	db        *sql.DB
	queries   []string
	connPool  []*ManagedConnection
	connChan  chan *ManagedConnection
	poolSize  int
	mu        sync.Mutex
	closeOnce sync.Once
	closed    bool

	// Map transactions to their connection cache for prepared statement retrieval
	txConnMap map[*sql.Tx]*ManagedConnection
	txMu      sync.RWMutex
}

func NewManager(ctx context.Context, db *sql.DB) (*Manager, error) {
	poolSize := DefaultPoolSize
	db.SetConnMaxLifetime(-1) // Keep connections alive
	db.SetConnMaxIdleTime(-1)

	connChan := make(chan *ManagedConnection, poolSize)
	connPool := make([]*ManagedConnection, poolSize)

	manager := &Manager{
		db:        db,
		connPool:  connPool,
		connChan:  connChan,
		txConnMap: make(map[*sql.Tx]*ManagedConnection),
	}

	// Warm pool with managed connections
	for i := 0; i < poolSize; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection %d: %w", i, err)
		}

		managedConnection := &ManagedConnection{
			googlesqliteConnection: conn,
			stmts:                make(map[string]*sql.Stmt),
			queries:              []string{},
			manager:              manager,
		}

		connPool[i] = managedConnection
		connChan <- managedConnection
	}

	return manager, nil
}

// Close gracefully shuts down the repository
func (m *Manager) Close() (err error) {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		m.closed = true
		close(m.connChan)

		for _, conn := range m.connPool {
			if closeErr := conn.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}
	})
	return
}

type Tx struct {
	tx        *sql.Tx
	conn      *ManagedConnection
	manager   *Manager
	committed bool
	finalized bool
}

func (t *Tx) Tx() *sql.Tx {
	return t.tx
}

// unregister removes this transaction from the manager's tracking map.
// It is idempotent and safe to call multiple times.
func (t *Tx) unregister() {
	if t.finalized || t.manager == nil {
		return
	}
	t.manager.txMu.Lock()
	delete(t.manager.txConnMap, t.tx)
	t.manager.txMu.Unlock()
	t.finalized = true
}

func (t *Tx) Conn() *ManagedConnection { return t.conn }

func (t *Tx) RollbackIfNotCommitted() error {
	defer t.unregister()
	if t.committed {
		return nil
	}
	return t.tx.Rollback()
}

func (t *Tx) Commit() error {
	defer t.unregister()
	if err := t.tx.Commit(); err != nil {
		return err
	}
	t.committed = true
	return nil
}

func (t *Tx) SetProjectAndDataset(projectID, datasetID string) {
	t.conn.ProjectID = projectID
	t.conn.DatasetID = datasetID
}

func (t *Tx) MetadataRepoMode() error {
	if err := t.conn.googlesqliteConnection.Raw(func(c interface{}) error {
		googlesqliteConn, ok := c.(*googlesqlite.GoogleSQLiteConn)
		if !ok {
			return fmt.Errorf("failed to get GoogleSQLiteConn from %T", c)
		}
		_ = googlesqliteConn.SetNamePath([]string{})
		return nil
	}); err != nil {
		return fmt.Errorf("failed to setup connection: %w", err)
	}
	return nil
}

func (t *Tx) ContentRepoMode() error {
	if err := t.conn.googlesqliteConnection.Raw(func(c interface{}) error {
		googlesqliteConn, ok := c.(*googlesqlite.GoogleSQLiteConn)
		if !ok {
			return fmt.Errorf("failed to get GoogleSQLiteConn from %T", c)
		}
		if t.conn.DatasetID == "" {
			_ = googlesqliteConn.SetNamePath([]string{t.conn.ProjectID})
		} else {
			_ = googlesqliteConn.SetNamePath([]string{t.conn.ProjectID, t.conn.DatasetID})
		}
		const maxNamePath = 3 // projectID and datasetID and tableID
		googlesqliteConn.SetMaxNamePath(maxNamePath)
		return nil
	}); err != nil {
		return fmt.Errorf("failed to setup connection: %w", err)
	}
	return nil
}

type ManagedConnection struct {
	googlesqliteConnection *sql.Conn
	stmts                map[string]*sql.Stmt
	queries              []string
	mu                   sync.RWMutex
	manager              *Manager // immutable after construction, safe for concurrent reads
	ProjectID            string
	DatasetID            string
}

func (c *ManagedConnection) GetStmt(name string) (*sql.Stmt, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stmt, ok := c.stmts[name]
	if !ok {
		return nil, fmt.Errorf("statement not found: %s", name)
	}
	return stmt, nil
}

func (c *ManagedConnection) Close() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, stmt := range c.stmts {
		if closeErr := stmt.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if closeErr := c.googlesqliteConnection.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return
}

func (c *ManagedConnection) ConfigureScope(projectID, datasetID string) *ManagedConnection {
	c.ProjectID = projectID
	c.DatasetID = datasetID
	return c
}

// Raw executes the given function with the underlying ite connection.
func (c *ManagedConnection) Raw(fn func(interface{}) error) error {
	return c.googlesqliteConnection.Raw(fn)
}

func (c *ManagedConnection) Begin(ctx context.Context) (*Tx, error) {
	tx, err := c.googlesqliteConnection.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	wrappedTx := &Tx{tx: tx, conn: c, manager: c.manager}

	// Register transaction with manager if available
	if c.manager != nil {
		c.manager.txMu.Lock()
		c.manager.txConnMap[tx] = c
		c.manager.txMu.Unlock()
	}

	return wrappedTx, nil
}

// GetStatement retrieves a prepared statement for use within a transaction
func (m *Manager) GetStatement(ctx context.Context, tx *sql.Tx, name string) (*sql.Stmt, error) {
	if tx == nil {
		return nil, fmt.Errorf("transaction is nil")
	}

	m.txMu.RLock()
	conn, ok := m.txConnMap[tx]
	m.txMu.RUnlock()

	if ok {
		// Transaction is tracked in txConnMap, use pre-prepared statement
		return conn.GetStmt(name)
	}

	// Transaction was created externally, prepare statement on the fly (this ideally never happens)
	ctx = googlesqlite.WithQueryFormattingDisabled(ctx)
	return tx.PrepareContext(ctx, name)
}

func (m *Manager) PrepareQueries(queries []string) error {
	for _, conn := range m.connPool {
		// Prepare specified SQLite queries for this connection
		ctx := googlesqlite.WithQueryFormattingDisabled(context.Background())
		for _, query := range queries {
			stmt, err := conn.googlesqliteConnection.PrepareContext(ctx, query)
			if err != nil {
				return fmt.Errorf("failed to prepare statement %s: %w", query, err)
			}
			conn.stmts[query] = stmt
		}
	}
	return nil
}

func (m *Manager) WithManagedConnection(ctx context.Context, fn func(ctx context.Context, conn *ManagedConnection) error) error {
	select {
	case conn := <-m.connChan:
		defer func() {
			m.connChan <- conn
		}()

		return fn(ctx, conn)

	case <-ctx.Done():
		return fmt.Errorf("context cancelled: %w", ctx.Err())
	}
}

func (m *Manager) ExecuteWithTransaction(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error {
	return m.WithManagedConnection(ctx, func(ctx context.Context, conn *ManagedConnection) error {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		// Transaction is automatically registered in Begin() and will be unregistered in Commit()/RollbackIfNotCommitted()

		// Execute the user's function with the transaction
		err = fn(ctx, tx)
		if err != nil {
			if rbErr := tx.RollbackIfNotCommitted(); rbErr != nil {
				return fmt.Errorf("transaction failed: %w (rollback also failed: %v)", err, rbErr)
			}
			return err
		}

		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		return nil
	})
}

func WithManagedConnection[T any](m *Manager, ctx context.Context,
	fn func(context.Context, *ManagedConnection) (T, error)) (T, error) {

	var zero T

	if m.closed {
		return zero, fmt.Errorf("repository is closed")
	}

	select {
	case conn := <-m.connChan:
		defer func() {
			m.connChan <- conn
		}()

		return fn(ctx, conn)

	case <-ctx.Done():
		return zero, fmt.Errorf("context cancelled: %w", ctx.Err())
	}
}

// ExecuteWithTransaction is a convenience wrapper for executing a function with a transaction
// This is useful for migrating code that used getConnection + BeginTx pattern
func ExecuteWithTransaction[T any](m *Manager, ctx context.Context, fn func(context.Context, *sql.Tx) (T, error)) (T, error) {
	return WithManagedConnection[T](m, ctx, func(ctx context.Context, conn *ManagedConnection) (T, error) {
		var zero T
		tx, err := conn.Begin(ctx)
		if err != nil {
			return zero, fmt.Errorf("failed to begin transaction: %w", err)
		}

		// Execute the user's function with the transaction
		result, err := fn(ctx, tx.Tx())
		if err != nil {
			if rbErr := tx.RollbackIfNotCommitted(); rbErr != nil {
				return zero, fmt.Errorf("transaction failed: %w (rollback also failed: %v)", err, rbErr)
			}
			return zero, err
		}

		if err = tx.Commit(); err != nil {
			return zero, fmt.Errorf("failed to commit transaction: %w", err)
		}

		return result, nil
	})
}
