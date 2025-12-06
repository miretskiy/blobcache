package index

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/marcboeker/go-duckdb"
	"github.com/miretskiy/blobcache/base"
)

// Index manages the DuckDB key-value index
type Index struct {
	db         *sql.DB
	stmtPut    *sql.Stmt
	stmtGet    *sql.Stmt
	stmtDelete *sql.Stmt
}

// Entry represents a cached blob's metadata
type Entry struct {
	Key       []byte // Raw key
	SegmentID int64  // Segment ID (0 for per-blob mode)
	Pos       int64  // Position within segment (0 for per-blob mode)
	Size      int
	CTime     int64 // Creation time
}

// Record holds metadata for a single index entry
type Record struct {
	Key       base.Key
	SegmentID int64 // Segment ID (0 for per-blob mode)
	Pos       int64 // Position within segment (0 for per-blob mode)
	Size      int
	CTime     int64
}

// Indexer defines the index operations
type Indexer interface {
	Put(context.Context, base.Key, int, int64) error
	Get(context.Context, base.Key, *Entry) error
	Delete(context.Context, base.Key) error
	Close() error
	TotalSizeOnDisk(context.Context) (int64, error)
	GetOldestEntries(context.Context, int) EntryIteratorInterface
	GetAllKeys(context.Context) ([][]byte, error)
	// PutBatch inserts multiple records atomically
	PutBatch(context.Context, []Record) error
}

// KeyValue holds a key with metadata for bulk insert
type KeyValue struct {
	Key  base.Key
	Size int
}

// NewDuckDBIndex creates or opens a DuckDB index
func NewDuckDBIndex(basePath string) (*Index, error) {
	dbDir := filepath.Join(basePath, "db")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}

	dbPath := filepath.Join(dbDir, "index.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	idx := &Index{db: db}

	if err := idx.initSchema(); err != nil {
		db.Close()
		return nil, err
	}

	if err := idx.prepareStatements(); err != nil {
		db.Close()
		return nil, err
	}

	return idx, nil
}

func (idx *Index) initSchema() error {
	schema := `
		CREATE TABLE IF NOT EXISTS entries (
			key BLOB NOT NULL,
			size INTEGER NOT NULL,
			ctime BIGINT NOT NULL
		);

		CREATE INDEX IF NOT EXISTS idx_key ON entries(key);
		CREATE INDEX IF NOT EXISTS idx_ctime ON entries(ctime);
	`

	_, err := idx.db.Exec(schema)
	return err
}

func (idx *Index) prepareStatements() error {
	var err error

	// No PRIMARY KEY constraint - allows duplicates, latest wins via ORDER BY ctime
	idx.stmtPut, err = idx.db.Prepare(`
		INSERT INTO entries (key, size, ctime)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare put: %w", err)
	}

	idx.stmtGet, err = idx.db.Prepare(
		`SELECT size, ctime FROM entries WHERE key = ? ORDER BY ctime DESC LIMIT 1`)
	if err != nil {
		return fmt.Errorf("failed to prepare get: %w", err)
	}

	idx.stmtDelete, err = idx.db.Prepare("DELETE FROM entries WHERE key = ?")
	if err != nil {
		return fmt.Errorf("failed to prepare delete: %w", err)
	}

	return nil
}

// Put inserts or updates an entry
func (idx *Index) Put(ctx context.Context, key base.Key, size int, ctime int64) error {
	_, err := idx.stmtPut.ExecContext(ctx, key.Raw(), size, ctime)
	return err
}

// Get retrieves an entry (caller provides Entry to avoid allocation)
func (idx *Index) Get(ctx context.Context, key base.Key, entry *Entry) error {
	err := idx.stmtGet.QueryRowContext(ctx, key.Raw()).Scan(
		&entry.Size, &entry.CTime)

	if err == sql.ErrNoRows {
		return ErrNotFound
	}

	return err
}

// Delete removes an entry
func (idx *Index) Delete(ctx context.Context, key base.Key) error {
	result, err := idx.stmtDelete.ExecContext(ctx, key.Raw())
	if err != nil {
		return err
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrNotFound
	}

	return nil
}

// Common errors
var (
	ErrNotFound = errors.New("key not found")
)

// TotalSizeOnDisk returns sum of all blob sizes
func (idx *Index) TotalSizeOnDisk(ctx context.Context) (int64, error) {
	var total sql.NullInt64

	err := idx.db.QueryRowContext(ctx,
		"SELECT SUM(size) FROM entries").Scan(&total)

	if err != nil {
		return 0, err
	}

	if !total.Valid {
		return 0, nil
	}

	return total.Int64, nil
}

// EntryIteratorInterface provides iteration over entries
type EntryIteratorInterface interface {
	Next() bool
	Entry() (Entry, error)
	Err() error
	Close() error
}

// EntryIterator provides iteration over entries (DuckDB implementation)
type EntryIterator struct {
	rows *sql.Rows
	err  error
}

// Next advances to the next entry and returns true if available
func (it *EntryIterator) Next() bool {
	if it.err != nil {
		return false
	}
	return it.rows.Next()
}

// Entry returns the current entry
func (it *EntryIterator) Entry() (Entry, error) {
	var entry Entry
	err := it.rows.Scan(&entry.Key, &entry.Size, &entry.CTime)
	return entry, err
}

// Err returns any error that occurred during iteration
func (it *EntryIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.rows.Err()
}

// Close closes the iterator
func (it *EntryIterator) Close() error {
	if it.rows != nil {
		return it.rows.Close()
	}
	return nil
}

// GetOldestEntries returns iterator over N oldest entries by ctime for eviction
func (idx *Index) GetOldestEntries(ctx context.Context, limit int) EntryIteratorInterface {
	rows, err := idx.db.QueryContext(ctx,
		`SELECT key, size, ctime FROM entries ORDER BY ctime ASC LIMIT ?`,
		limit)
	return &EntryIterator{rows: rows, err: err}
}

// GetAllKeys returns all keys for bloom filter reconstruction
func (idx *Index) GetAllKeys(ctx context.Context) ([][]byte, error) {
	rows, err := idx.db.QueryContext(ctx, "SELECT key FROM entries")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys [][]byte
	for rows.Next() {
		var key []byte
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	return keys, rows.Err()
}

// PutBatch inserts multiple records using DuckDB Appender
func (idx *Index) PutBatch(ctx context.Context, records []Record) error {
	conn, err := idx.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	var appender *duckdb.Appender
	err = conn.Raw(func(dc any) error {
		driverConn := dc.(driver.Conn)
		var err error
		appender, err = duckdb.NewAppenderFromConn(driverConn, "", "entries")
		return err
	})
	if err != nil {
		return err
	}
	defer appender.Close()

	// Append all records
	for _, rec := range records {
		if err := appender.AppendRow(rec.Key.Raw(), rec.Size, rec.CTime); err != nil {
			return err
		}
	}

	return appender.Flush()
}

// TestingGetDB returns the underlying database for testing/benchmarking only
// DO NOT use in production code
func (idx *Index) TestingGetDB() *sql.DB {
	return idx.db
}

// Close closes prepared statements and database
func (idx *Index) Close() error {
	if idx.stmtPut != nil {
		idx.stmtPut.Close()
	}
	if idx.stmtGet != nil {
		idx.stmtGet.Close()
	}
	if idx.stmtDelete != nil {
		idx.stmtDelete.Close()
	}
	return idx.db.Close()
}
