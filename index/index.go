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
	Key     []byte // Raw key (for eviction/iteration)
	ShardID int
	FileID  int64 // Signed to avoid DuckDB uint64 high-bit issues
	Size    int
	CTime   int64
	ATime   int64
}

// KeyValue holds a key with metadata for bulk insert
type KeyValue struct {
	Key  base.Key
	Size int
}

// New creates or opens a DuckDB index
func New(basePath string) (*Index, error) {
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
			key BLOB PRIMARY KEY,
			shard_id INTEGER NOT NULL,
			file_id BIGINT NOT NULL,
			size INTEGER NOT NULL,
			ctime BIGINT NOT NULL,
			mtime BIGINT NOT NULL
		);

		CREATE INDEX IF NOT EXISTS idx_ctime ON entries(ctime);
		CREATE INDEX IF NOT EXISTS idx_mtime ON entries(mtime);
		CREATE INDEX IF NOT EXISTS idx_shard_file ON entries(shard_id, file_id);
	`
	
	_, err := idx.db.Exec(schema)
	return err
}

func (idx *Index) prepareStatements() error {
	var err error
	
	// Note: Can't use ON CONFLICT DO UPDATE with DuckDB when columns are in indexes
	// Use REPLACE instead (delete + insert)
	idx.stmtPut, err = idx.db.Prepare(`
		INSERT OR REPLACE INTO entries (key, shard_id, file_id, size, ctime, mtime)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare put: %w", err)
	}
	
	idx.stmtGet, err = idx.db.Prepare(
		"SELECT shard_id, file_id, size, ctime, mtime FROM entries WHERE key = ?")
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
func (idx *Index) Put(ctx context.Context, key base.Key, size int, ctime, mtime int64) error {
	_, err := idx.stmtPut.ExecContext(ctx,
		key.Raw(), key.ShardID(), int64(key.FileID()), size, ctime, mtime)
	return err
}

// Get retrieves an entry (caller provides Entry to avoid allocation)
func (idx *Index) Get(ctx context.Context, key base.Key, entry *Entry) error {
	err := idx.stmtGet.QueryRowContext(ctx, key.Raw()).Scan(
		&entry.ShardID, &entry.FileID, &entry.Size, &entry.CTime, &entry.ATime)
	
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

// EntryIterator provides iteration over entries
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
	err := it.rows.Scan(&entry.Key, &entry.ShardID, &entry.FileID, &entry.Size, &entry.CTime, &entry.ATime)
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

// GetOldestEntries returns iterator over N oldest entries by mtime for eviction
func (idx *Index) GetOldestEntries(ctx context.Context, limit int) *EntryIterator {
	rows, err := idx.db.QueryContext(ctx,
		"SELECT key, shard_id, file_id, size, ctime, mtime FROM entries ORDER BY mtime ASC LIMIT ?",
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

// NewAppender creates a DuckDB Appender for bulk inserts
func (idx *Index) NewAppender(ctx context.Context) (*duckdb.Appender, func(), error) {
	conn, err := idx.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	
	var appender *duckdb.Appender
	err = conn.Raw(func(dc any) error {
		driverConn := dc.(driver.Conn)
		var err error
		appender, err = duckdb.NewAppenderFromConn(driverConn, "", "entries")
		return err
	})
	
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	
	// Return appender and cleanup function
	cleanup := func() {
		appender.Close()
		conn.Close()
	}
	
	return appender, cleanup, nil
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
