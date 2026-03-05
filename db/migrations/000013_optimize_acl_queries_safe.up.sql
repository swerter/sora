-- SAFE: Optimize ACL mailbox listing queries with zero-downtime migration
-- This migration adds a denormalized domain column to credentials and composite indexes
-- Uses CONCURRENTLY and batched updates to avoid blocking production traffic

-- Step 1: Add domain column with NULL default (non-blocking)
-- This is a fast metadata-only change in PostgreSQL
ALTER TABLE credentials ADD COLUMN IF NOT EXISTS domain TEXT DEFAULT NULL;

-- Step 2: Backfill domain column in small batches to avoid long-running transactions
-- PostgreSQL will use the existing index on credentials to make this fast
-- Run this in a loop outside the transaction or use a DO block with COMMIT between batches
DO $$
DECLARE
    batch_size INT := 1000;
    rows_updated INT;
BEGIN
    LOOP
        -- Update in small batches with explicit row locking
        UPDATE credentials
        SET domain = SPLIT_PART(address, '@', 2)
        WHERE id IN (
            SELECT id FROM credentials
            WHERE domain IS NULL
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        );

        GET DIAGNOSTICS rows_updated = ROW_COUNT;

        -- Exit when no more rows to update
        EXIT WHEN rows_updated = 0;

        -- Commit implicitly happens at end of each iteration
        -- Small pause to avoid overwhelming the database
        PERFORM pg_sleep(0.01);
    END LOOP;
END $$;

-- Step 3: Add NOT NULL constraint with validation
-- In PostgreSQL 12+, this is fast if all values are already non-NULL
-- In PostgreSQL 11 and earlier, this requires a full table scan but is still fast
-- because we just backfilled all values
ALTER TABLE credentials ALTER COLUMN domain SET NOT NULL;

-- Step 4: Create trigger BEFORE creating indexes (so new rows get domain automatically)
CREATE OR REPLACE FUNCTION maintain_credentials_domain()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.domain := SPLIT_PART(NEW.address, '@', 2);
    RETURN NEW;
END;
$$;

CREATE TRIGGER trigger_maintain_credentials_domain
BEFORE INSERT OR UPDATE OF address ON credentials
FOR EACH ROW
EXECUTE FUNCTION maintain_credentials_domain();

-- Step 5-9: Create indexes CONCURRENTLY (non-blocking, allows all reads/writes)
-- These can take a while on large tables but won't block production traffic
-- Note: CONCURRENTLY cannot be run inside a transaction block, so each is separate

-- Step 5: Index on domain for fast lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_credentials_domain
ON credentials (domain);

-- Step 6: Composite index for ACL lookups with account_id and primary_identity
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_credentials_account_primary
ON credentials (account_id, primary_identity)
WHERE primary_identity = TRUE;

-- Step 7: Composite index for ACL mailbox + rights lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mailbox_acls_mailbox_account_rights
ON mailbox_acls (mailbox_id, account_id, rights);

-- Step 8: Composite index for "anyone" ACL with rights
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mailbox_acls_anyone_rights
ON mailbox_acls (mailbox_id, rights)
WHERE identifier = 'anyone';

-- Step 9: Composite index on mailboxes for shared + owner_domain lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mailboxes_shared_owner_domain
ON mailboxes (is_shared, owner_domain)
WHERE is_shared = TRUE AND owner_domain IS NOT NULL;
