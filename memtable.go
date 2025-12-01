package blobcache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/miretskiy/blobcache/base"
)

// MemTable provides async write buffering with batching
type MemTable struct {
	entries         chan memTableEntry
	batches         chan []memTableEntry // Batches ready for I/O workers
	stopCh          chan struct{}
	wg              sync.WaitGroup
	cache           *Cache
	writeBufferSize int64
	batchPool       sync.Pool
}

type memTableEntry struct {
	key   []byte
	value []byte
}

// newMemTable creates a memtable
func (c *Cache) newMemTable() *MemTable {
	const entryQueueSize = 256 // Small fixed queue to smooth producer spikes

	mt := &MemTable{
		entries:         make(chan memTableEntry, entryQueueSize),
		batches:         make(chan []memTableEntry, c.cfg.MaxInflightBatches),
		stopCh:          make(chan struct{}),
		cache:           c,
		writeBufferSize: c.cfg.WriteBufferSize,
		batchPool: sync.Pool{
			New: func() any {
				return make([]memTableEntry, 0, 1000)
			},
		},
	}

	// Single batcher: accumulates entries
	mt.wg.Add(1)
	go mt.batcher()

	// as many workers as the number of inflight batches.
	for i := 0; i < c.cfg.MaxInflightBatches; i++ {
		mt.wg.Add(1)
		go mt.ioWorker()
	}

	return mt
}

// batcher accumulates entries into size-based batches
func (mt *MemTable) batcher() {
	defer mt.wg.Done()

	batch := mt.getBatch()
	batchSize := int64(0)

	for {
		select {
		case entry := <-mt.entries:
			batch = append(batch, entry)
			batchSize += int64(len(entry.key) + len(entry.value))

			if batchSize >= mt.writeBufferSize {
				mt.batches <- batch
				batch = mt.getBatch()
				batchSize = 0
			}

		case <-mt.stopCh:
			if len(batch) > 0 {
				mt.batches <- batch
			}
			close(mt.batches)
			return
		}
	}
}

// ioWorker processes batches in parallel
func (mt *MemTable) ioWorker() {
	defer mt.wg.Done()
	ctx := context.Background()

	for batch := range mt.batches {
		mt.flush(ctx, batch)
	}
}

// flush writes a batch using Appender, falls back to individual Puts on error
func (mt *MemTable) flush(ctx context.Context, batch []memTableEntry) {
	defer mt.putBatch(batch)

	// Try bulk insert with Appender
	appender, cleanup, err := mt.cache.index.NewAppender(ctx)
	if err != nil {
		// Fallback to individual writes
		fmt.Printf("Warning: fallback appender: %v\n", err)
		mt.flushFallback(ctx, batch)
		return
	}
	defer cleanup()

	now := time.Now().UnixNano()
	bloom := mt.cache.bloom.Load()

	// Append all rows
	for _, e := range batch {
		k := base.NewKey(e.key, mt.cache.cfg.Shards)
		if err := appender.AppendRow(
			k.Raw(),
			k.ShardID(),
			int64(k.FileID()),
			len(e.value),
			now, now,
		); err != nil {
			// Appender failed (likely duplicate key), fallback
			cleanup()
			fmt.Printf("Warning: fallback append row: %v\n", err)

			mt.flushFallback(ctx, batch)
			return
		}
		bloom.Add(e.key)
	}

	// Commit batch
	if err := appender.Flush(); err != nil {
		// Flush failed, fallback
		fmt.Printf("Warning: fallback appender flush: %v\n", err)

		mt.flushFallback(ctx, batch)
		return
	}

	// Write blob files
	for _, e := range batch {
		k := base.NewKey(e.key, mt.cache.cfg.Shards)
		blobPath := filepath.Join(mt.cache.cfg.Path, "blobs",
			fmt.Sprintf("shard-%03d", k.ShardID()),
			fmt.Sprintf("%d.blob", k.FileID()))

		if err := os.WriteFile(blobPath, e.value, 0644); err != nil {
			fmt.Printf("Warning: blob write failed for key %s: %v\n", e.key, err)
		}
	}
}

// flushFallback writes batch using individual Put() calls (handles duplicates)
func (mt *MemTable) flushFallback(ctx context.Context, batch []memTableEntry) {
	for _, e := range batch {
		if err := mt.cache.writeToDisk(ctx, e.key, e.value); err != nil {
			fmt.Printf("Warning: fallback write failed: %v\n", err)
		}
	}
}

// getBatch gets a batch from pool
func (mt *MemTable) getBatch() []memTableEntry {
	batch := mt.batchPool.Get().([]memTableEntry)
	return batch[:0]
}

// putBatch clears and returns batch to pool
func (mt *MemTable) putBatch(batch []memTableEntry) {
	for i := range batch {
		batch[i] = memTableEntry{}
	}
	mt.batchPool.Put(batch[:0])
}

// Add copies key and value, then enqueues for background write
func (mt *MemTable) Add(key, value []byte) error {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return mt.UnsafeAdd(keyCopy, valueCopy)
}

var hist = hdrhistogram.New(1, int64(60*time.Second), 3)

// UnsafeAdd enqueues without copying
func (mt *MemTable) UnsafeAdd(key, value []byte) error {
	now := time.Now()
	defer func() {
		hist.RecordValue(time.Since(now).Nanoseconds())
	}()

	entry := memTableEntry{key: key, value: value}
	select {
	case mt.entries <- entry:
		return nil
	case <-mt.stopCh:
		return ErrClosed
	}
}

// Close shuts down worker
func (mt *MemTable) Close() error {
	select {
	case <-mt.stopCh:
		return nil
	default:
		close(mt.stopCh)
	}
	mt.wg.Wait()
	return nil
}

// Drain waits for all pending writes
func (mt *MemTable) Drain() {
	for len(mt.entries) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
}

var (
	ErrClosed = errors.New("memtable closed")
)
