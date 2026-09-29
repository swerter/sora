package db

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/migadu/sora/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// statementCounter counts every statement a connection sends.
type statementCounter struct{ n atomic.Int64 }

func (c *statementCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	c.n.Add(1)
	return ctx
}
func (c *statementCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// stageQueuedBodies inserts n queued single-account bodies at the head of the FIFO queue:
// created_at runs forward from base, while content_hash runs BACKWARDS, so that the order
// the queue is polled in and the order of the hashes disagree.
func stageQueuedBodies(t *testing.T, ctx context.Context, db *Database, prefix string, accountID int64, n int, base string, words int) {
	t.Helper()
	_, err := db.GetWritePool().Exec(ctx, `
		INSERT INTO messages_fts_v2 (content_hash, account_id, text_body, sent_date, created_at)
		SELECT $1 || lpad(($3 - g)::text, 6, '0'), $2,
		       (SELECT string_agg('w' || ((g * 7919 + i * 104729) % 50000), ' ') FROM generate_series(1, $5) i),
		       now(), $4::timestamptz + make_interval(secs => g)
		FROM generate_series(1, $3) g`, prefix, accountID, n, base, words)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.GetWritePool().Exec(context.Background(), `DELETE FROM messages_fts_v2 WHERE content_hash LIKE $1`, prefix+"%")
	})
}

// Production, 2026-09-29: each worker resolved ~29 bodies per 20 s batch while a single
// server-side statement loop did ~139 a second. The time went to round trips: the worker
// sent five statements per body (savepoint, tokenise, v1 copy, fan-out, release) over a
// ~20 ms path, and fetched every polled body's text only to send it straight back. The queue
// fell 14 hours behind. The number of statements a batch sends must not grow with the number
// of bodies in it.
func TestFTSBatchStatementsDoNotScaleWithBodies(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}
	db, _, accountID, _ := setupCleanerTestDatabase(t)
	t.Cleanup(db.Close) // registered first, so it runs after every other cleanup
	ctx := context.Background()

	const bodies = 200
	prefix := fmt.Sprintf("stmtcount_%d_", time.Now().UnixNano())
	stageQueuedBodies(t, ctx, db, prefix, accountID, bodies, "1850-01-01", 20)

	counter := &statementCounter{}
	cfg := db.GetWritePool().Config().ConnConfig.Copy()
	cfg.Tracer = counter
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	before := counter.n.Load()
	n, err := db.ProcessFTSBatch(ctx, tx, bodies)
	require.NoError(t, err)
	sent := counter.n.Load() - before
	require.NoError(t, tx.Commit(ctx))

	require.Equal(t, bodies, n, "every body in the batch must be indexed")
	t.Logf("%d bodies indexed with %d statements", n, sent)
	assert.LessOrEqual(t, sent, int64(20),
		"a batch must index its bodies a chunk at a time; %d statements for %d bodies is a round trip per body", sent, n)
}

// A batch that runs out of time must have spent it on the OLDEST mail. The worker used to
// sort a batch by content hash before indexing, so when the budget ran out it had indexed
// the rows with the lowest hashes, not the oldest: in production the oldest queued body sat
// unindexed for 14 hours while newer mail was indexed around it.
func TestFTSBatchIndexesOldestFirst(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}
	db, _, accountID, _ := setupCleanerTestDatabase(t)
	t.Cleanup(db.Close) // registered first, so it runs after every other cleanup
	ctx := context.Background()

	const bodies = 1500
	prefix := fmt.Sprintf("fifo_%d_", time.Now().UnixNano())
	// Heavy enough (1500 words each) that one short deadline cannot index them all.
	stageQueuedBodies(t, ctx, db, prefix, accountID, bodies, "1851-01-01", 1500)

	batchCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	tx, err := db.GetWritePool().Begin(batchCtx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	n, err := db.ProcessFTSBatch(batchCtx, tx, bodies)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(batchCtx))
	require.Greater(t, n, 0)
	require.Less(t, n, bodies, "the fixture must be too large for one deadline, or this test proves nothing")

	var newestIndexed, oldestQueued time.Time
	require.NoError(t, db.GetWritePool().QueryRow(ctx, `
		SELECT max(created_at) FILTER (WHERE text_body_tsv IS NOT NULL),
		       min(created_at) FILTER (WHERE text_body_tsv IS NULL)
		FROM messages_fts_v2 WHERE content_hash LIKE $1`, prefix+"%").Scan(&newestIndexed, &oldestQueued))
	t.Logf("%d of %d indexed; newest indexed %s, oldest still queued %s", n, bodies, newestIndexed, oldestQueued)
	assert.True(t, newestIndexed.Before(oldestQueued),
		"a partial batch indexed newer mail while older mail stayed queued: it must work oldest first")
}

// One body that PostgreSQL cannot tokenise must cost only that body. Bodies are now indexed
// many per statement, so a bad one fails the whole statement; the batch must fall back to
// isolating it and still index every other body.
func TestFTSBatchIsolatesAnUntokenisableBody(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}
	db, _, accountID, _ := setupCleanerTestDatabase(t)
	t.Cleanup(db.Close) // registered first, so it runs after every other cleanup
	ctx := context.Background()

	prefix := fmt.Sprintf("isolate_%d_", time.Now().UnixNano())
	stageQueuedBodies(t, ctx, db, prefix, accountID, 50, "1852-01-01", 20)

	// More distinct lexemes than a tsvector can hold: to_tsvector raises 54000.
	bad := prefix + "bad"
	_, err := db.GetWritePool().Exec(ctx, `
		INSERT INTO messages_fts_v2 (content_hash, account_id, text_body, sent_date, created_at)
		VALUES ($1, $2, (SELECT string_agg('lexeme' || g, ' ') FROM generate_series(1, 160000) g),
		        now(), '1852-01-01'::timestamptz + interval '25 seconds')`, bad, accountID)
	require.NoError(t, err)

	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	_, err = db.ProcessFTSBatch(ctx, tx, 51)
	require.NoError(t, err, "one untokenisable body must not fail the batch")
	require.NoError(t, tx.Commit(ctx))

	var indexed, poisoned, queued int
	require.NoError(t, db.GetWritePool().QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE length(text_body_tsv) > 0),
		       count(*) FILTER (WHERE length(text_body_tsv) = 0),
		       count(*) FILTER (WHERE text_body_tsv IS NULL)
		FROM messages_fts_v2 WHERE content_hash LIKE $1`, prefix+"%").Scan(&indexed, &poisoned, &queued))
	assert.Equal(t, 50, indexed, "every good body must be indexed")
	assert.Equal(t, 1, poisoned, "only the untokenisable body may be poisoned")
	assert.Equal(t, 0, queued)
}

// A queued body whose content hash already has a vector must COPY it, not tokenise its own
// text again: a body is tokenised at most once however many accounts hold it. The fixture
// makes the two distinguishable, which real data never is.
func TestFTSBatchCopiesAnExistingVectorInsteadOfTokenising(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}
	db, _, accountID, _ := setupCleanerTestDatabase(t)
	t.Cleanup(db.Close) // registered first, so it runs after every other cleanup
	ctx := context.Background()

	hash := fmt.Sprintf("copyonce_%d", time.Now().UnixNano())
	_, err := db.GetWritePool().Exec(ctx, `
		INSERT INTO messages_fts_v2 (content_hash, account_id, text_body_tsv, sent_date)
		VALUES ($1, $2, strip(to_tsvector('simple', 'already indexed')), now())`, hash, accountID+4_000_000)
	require.NoError(t, err)
	_, err = db.GetWritePool().Exec(ctx, `
		INSERT INTO messages_fts_v2 (content_hash, account_id, text_body, sent_date, created_at)
		VALUES ($1, $2, 'tokenised again', now(), '1853-01-01')`, hash, accountID)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.GetWritePool().Exec(context.Background(), `DELETE FROM messages_fts_v2 WHERE content_hash = $1`, hash)
	})

	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	_, err = db.ProcessFTSBatch(ctx, tx, 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	var vector string
	require.NoError(t, db.GetWritePool().QueryRow(ctx,
		`SELECT text_body_tsv::text FROM messages_fts_v2 WHERE content_hash = $1 AND account_id = $2`,
		hash, accountID).Scan(&vector))
	assert.Equal(t, "'already' 'indexed'", vector, "the existing vector must be copied, not recomputed")
}

// The worker strips long base64/hex runs in SQL now, so a body's text never leaves the
// database. The expression must drop exactly what helpers.RemoveLongTokens drops, or every
// vector written from here on would differ from those already in the index.
func TestFTSTokenizeMatchesRemoveLongTokens(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}
	db := setupTestDatabase(t)
	t.Cleanup(db.Close)
	ctx := context.Background()

	long := strings.Repeat("A1b2", 30)
	for i, body := range []string{
		"hello world " + long + " tail",
		"<html><body>" + long + "</body></html> after",
		"tab\t" + strings.Repeat("x", 100) + "\t" + strings.Repeat("y", 101) + "\nnext\rline",
		strings.Repeat("ž", 101) + " " + strings.Repeat("č", 100) + " end",
		"a<" + long + ">b",
		"nbsp " + long + " end",
		"formfeed\f" + long + "\vend",
		"unicode 日本語 " + strings.Repeat("語", 150) + " ok",
		"",
	} {
		var same bool
		require.NoError(t, db.GetReadPool().QueryRow(ctx,
			`SELECT `+ftsTokenizeSQL("$1::text")+` = strip(to_tsvector('simple', $2::text))`,
			body, helpers.RemoveLongTokens(body, 100)).Scan(&same))
		assert.True(t, same, "body %d: SQL tokenisation differs from helpers.RemoveLongTokens", i)
	}
}
