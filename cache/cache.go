package cache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/migadu/sora/db"
)

const DEFAULT_CACHE_SIZE = 1024 // 1GB
const MAX_MESSAGE_SIZE = 10     // 10 MB
const DATA_DIR = "data"
const INDEX_DB = "cache_index.db"
const CACHE_PURGE_TICK = 12 * time.Hour

const BATCH_PURGE_SIZE = 1000

type Cache struct {
	basePath         string
	maxSizeMegaBytes int64
	db               *sql.DB
	mu               sync.Mutex
	sourceDB         *db.Database
}

func New(basePath string, maxSizeMegaBytes int64, sourceDb *db.Database) (*Cache, error) {
	// Ensure data subdirectory exists
	dataDir := filepath.Join(basePath, DATA_DIR)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache data path %s: %w", dataDir, err)
	}

	dbPath := filepath.Join(basePath, INDEX_DB)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open cache index DB: %w", err)
	}

	if _, err := db.Exec(`PRAGMA journal_mode = WAL;`); err != nil {
		// Log the warning, but allow to proceed as WAL is an optimization.
		log.Printf("[CACHE] WARNING: Failed to set PRAGMA journal_mode = WAL: %v", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS cache_index (
		path TEXT PRIMARY KEY,
		size INTEGER NOT NULL,
		mod_time TIMESTAMP NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_cache_mod_time ON cache_index(mod_time);
	`
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("failed to create cache schema: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close() // Close the DB if ping fails
		return nil, fmt.Errorf("cache DB ping failed: %w", err)
	}
	return &Cache{
		basePath:         basePath,
		maxSizeMegaBytes: maxSizeMegaBytes,
		db:               db,
		sourceDB:         sourceDb,
	}, nil
}

func (c *Cache) Get(domain, user string, uuid uuid.UUID) ([]byte, error) {
	path := c.FilePath(domain, user, uuid)
	return os.ReadFile(path)
}

func (c *Cache) Put(domain, user string, uuid uuid.UUID, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if int64(len(data)) > MAX_MESSAGE_SIZE*1024*1024 {
		return fmt.Errorf("data size %d exceeds MAX_MESSAGE_SIZE %d MB", len(data), MAX_MESSAGE_SIZE)
	}

	path := c.FilePath(domain, user, uuid)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write cache file: %w", err)
	}
	return c.trackFile(path)
}

func (c *Cache) Exists(domain, user string, uuid uuid.UUID) (bool, error) {
	path := c.FilePath(domain, user, uuid)
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (c *Cache) Delete(domain, user string, uuid uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	path := c.FilePath(domain, user, uuid)
	if err := os.Remove(path); err != nil {
		// If the file doesn't exist, we can consider the delete successful for the cache's state.
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove cache file %s: %w", path, err)
		}
	}
	// Always try to remove from index, even if file was already gone.
	if _, err := c.db.Exec(`DELETE FROM cache_index WHERE path = ?`, path); err != nil {
		// Log the error, as this means the index might be out of sync.
		log.Printf("[CACHE] Delete: failed to remove index entry for path %s: %v", path, err)
	}
	return nil
}

func (c *Cache) FilePath(domain, user string, uuid uuid.UUID) string {
	return filepath.Join(c.basePath, DATA_DIR, domain, user, uuid.String())
}

func (c *Cache) MoveIn(path string, domain, user string, uuid uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	target := c.FilePath(domain, user, uuid)
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}
	if err := os.Rename(path, target); err != nil {
		return fmt.Errorf("failed to move file into cache: %w", err)
	}
	return c.trackFile(target)
}

func (c *Cache) trackFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	_, err = c.db.Exec(`INSERT OR REPLACE INTO cache_index (path, size, mod_time) VALUES (?, ?, ?)`, path, info.Size(), info.ModTime())
	return err
}

func (c *Cache) PurgeIfNeeded(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	dataDir := filepath.Join(c.basePath, DATA_DIR)
	var nullTotalSize sql.NullInt64
	row := c.db.QueryRow(`SELECT SUM(size) FROM cache_index`)
	if err := row.Scan(&nullTotalSize); err != nil {
		// An error here (other than sql.ErrNoRows, which Scan handles for Null types) is problematic.
		return fmt.Errorf("failed to get total cache size: %w", err)
	}
	var totalSize int64
	if nullTotalSize.Valid {
		totalSize = nullTotalSize.Int64
	}

	totalSizeMB := totalSize / (1024 * 1024)

	if totalSize <= c.maxSizeMegaBytes*1024*1024 {
		log.Printf("[CACHE] size: %d MB, within limit: %d MB\n", totalSizeMB, c.maxSizeMegaBytes)
		return nil
	}

	log.Printf("[CACHE] size: %d MB, exceeds limit: %d MB\n", totalSizeMB, c.maxSizeMegaBytes)

	rows, err := c.db.Query(`SELECT path, size FROM cache_index ORDER BY mod_time ASC`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var freed int64
	for rows.Next() {
		var path string
		var size int64
		if err := rows.Scan(&path, &size); err != nil {
			continue
		}
		if err := os.Remove(path); err == nil {
			if _, dbErr := c.db.Exec(`DELETE FROM cache_index WHERE path = ?`, path); dbErr != nil {
				log.Printf("[CACHE] PurgeIfNeeded: failed to delete index entry for %s: %v", path, dbErr)
			}
			freed += size
			removeEmptyParents(path, dataDir)
			if totalSize-freed <= c.maxSizeMegaBytes*1024*1024 {
				log.Printf("[CACHE] freed %d bytes, total size now: %d bytes\n", freed, totalSize-freed)
				break
			}
		}
	}
	if err := c.cleanupStaleDirectories(); err != nil {
		log.Printf("[CACHE] PurgeIfNeeded: error during cleanupStaleDirectories: %v", err)
	}
	return nil
}

func removeEmptyParents(path string, stopAt string) {
	for {
		dir := filepath.Dir(path)
		if dir == stopAt || dir == "." || dir == "/" {
			break
		}
		err := os.Remove(dir)
		if err != nil {
			// Not empty or permission denied, stop cleanup
			break
		}
		path = dir
	}
}

func (c *Cache) SyncFromDisk() error {
	var filePaths []string

	dataDir := filepath.Join(c.basePath, DATA_DIR)
	err := filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			filePaths = append(filePaths, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk cache directory: %w", err)
	}

	for _, path := range filePaths {
		c.mu.Lock()
		_ = c.trackFile(path)
		c.mu.Unlock()
	}

	ctx := context.Background()
	if err := c.RemoveStaleDBEntries(ctx); err != nil {
		return fmt.Errorf("failed to remove stale DB entries: %w", err)
	}

	return c.cleanupStaleDirectories()
}

func (c *Cache) StartPurgeLoop(ctx context.Context) {
	go func() {
		// Run immediately on startup
		c.runPurgeCycle(ctx)

		ticker := time.NewTicker(CACHE_PURGE_TICK)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.runPurgeCycle(ctx)
			}
		}
	}()
}

func (c *Cache) runPurgeCycle(ctx context.Context) {
	log.Println("[CACHE] running cache purge cycle")
	if err := c.PurgeIfNeeded(ctx); err != nil {
		log.Printf("[CACHE] purge error: %v\n", err)
	}
	if err := c.RemoveStaleDBEntries(ctx); err != nil {
		log.Printf("[CACHE] stale file cleanup error: %v\n", err)
	}
	if err := c.PurgeOrphanedUUIDs(ctx); err != nil {
		log.Printf("[CACHE] orphan UUID cleanup error: %v\n", err)
	}
}

func (c *Cache) cleanupStaleDirectories() error {
	log.Println("[CACHE] cleaning up stale directories in cache")
	dataDir := filepath.Join(c.basePath, DATA_DIR)
	return filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() || path == dataDir {
			return nil
		}

		// Try to remove the directory — only works if it's empty
		err = os.Remove(path)
		if err != nil && !os.IsNotExist(err) {
			// Directory not empty or another error, ignore
			return nil
		}

		return nil
	})
}

func (c *Cache) PurgeOrphanedUUIDs(ctx context.Context) error {
	log.Println("[CACHE] purging orphaned UUIDs from cache")
	threshold := time.Now().Add(-30 * 24 * time.Hour) // Example: filter entries older than 30 days
	rows, err := c.db.Query(`SELECT path FROM cache_index WHERE mod_time < ?`, threshold)
	if err != nil {
		return err
	}
	defer rows.Close()

	var batch []uuid.UUID
	var paths []string
	purged := 0

	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			log.Printf("[CACHE] PurgeOrphanedUUIDs: error scanning path: %v", err)
			continue
		}
		base := filepath.Base(path)
		id, err := uuid.Parse(base)
		if err != nil {
			log.Printf("[CACHE] PurgeOrphanedUUIDs: error parsing UUID from path %s: %v", path, err)
			continue
		}
		batch = append(batch, id)
		paths = append(paths, path)

		if len(batch) == BATCH_PURGE_SIZE {
			purged += c.purgeBatch(ctx, batch, paths)
			batch = nil
			paths = nil
		}
	}

	if len(batch) > 0 {
		purged += c.purgeBatch(ctx, batch, paths)
	}

	if purged > 0 {
		log.Printf("[CACHE] removed %d orphaned entries\n", purged)
	}
	return nil
}

func (c *Cache) purgeBatch(ctx context.Context, uuids []uuid.UUID, paths []string) int {
	log.Println("[CACHE] purging batch of", len(uuids), "UUIDs")
	dataDir := filepath.Join(c.basePath, DATA_DIR)
	existing, err := c.sourceDB.FindExistingUUIDs(ctx, uuids)
	if err != nil {
		log.Printf("[CACHE] purgeBatch: error finding existing UUIDs from sourceDB: %v", err)
		return 0
	}

	exists := make(map[uuid.UUID]bool)
	for _, id := range existing {
		exists[id] = true
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("[CACHE] purgeBatch: error beginning transaction: %v", err)
		return 0 // Cannot proceed without a transaction for DB deletes
	}
	defer tx.Rollback() // Rollback if not committed

	purged := 0
	for i, id := range uuids {
		if !exists[id] {
			// Attempt to remove the file first
			err := os.Remove(paths[i])
			fileRemovedOrNotExists := err == nil || os.IsNotExist(err)

			if err != nil && !os.IsNotExist(err) {
				log.Printf("[CACHE] purgeBatch: error removing cached file %s: %v", paths[i], err)
				// If file removal failed for a reason other than "not exist",
				// skip deleting the DB entry to avoid orphaning the DB record.
				continue
			}

			// If file was successfully removed or didn't exist, try to remove the DB entry.
			if fileRemovedOrNotExists {
				_, dbErr := tx.ExecContext(ctx, `DELETE FROM cache_index WHERE path = ?`, paths[i])
				if dbErr != nil {
					log.Printf("[CACHE] purgeBatch: error deleting cache index entry for path %s: %v", paths[i], dbErr)
					// Log the error and continue. The transaction will attempt to commit other successful deletes.
					// This might leave an orphaned file if os.Remove succeeded but DB delete failed,
					// but RemoveStaleDBEntries might catch it later if the file is truly gone.
					continue
				}
				removeEmptyParents(paths[i], dataDir) // Only remove parents if both file and DB entry are handled.
				purged++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[CACHE] purgeBatch: error committing transaction: %v", err)
		return 0 // Return 0 as the batch operation wasn't fully successful
	}
	return purged
}

func (c *Cache) RemoveStaleDBEntries(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	rows, err := c.db.QueryContext(ctx, `SELECT path FROM cache_index`)
	if err != nil {
		return fmt.Errorf("failed to query cache_index: %w", err)
	}
	defer rows.Close()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // safe to call even if tx is already committed

	var removed int
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			continue
		}
		if _, err := os.Stat(path); os.IsNotExist(err) {
			_, err = tx.ExecContext(ctx, `DELETE FROM cache_index WHERE path = ?`, path)
			if err != nil {
				log.Printf("Failed to delete path %s: %v", path, err)
				continue
			}
			removed++
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit deletions: %w", err)
	}

	if removed > 0 {
		log.Printf("[CACHE] removed %d stale entries\n", removed)
	}

	return nil
}
