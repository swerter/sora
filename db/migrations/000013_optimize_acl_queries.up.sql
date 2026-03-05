-- Optimize ACL mailbox listing queries
-- This migration adds a denormalized domain column to credentials and composite indexes
-- to eliminate SPLIT_PART() function calls and improve ACL query performance

-- Step 1: Create trigger FIRST to handle new rows during migration
-- This ensures any concurrent inserts will populate domain automatically
CREATE OR REPLACE FUNCTION maintain_credentials_domain()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.domain := SPLIT_PART(NEW.address, '@', 2);
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trigger_maintain_credentials_domain
BEFORE INSERT OR UPDATE OF address ON credentials
FOR EACH ROW
EXECUTE FUNCTION maintain_credentials_domain();

-- Step 2: Add domain column to credentials table
-- This eliminates the need for SPLIT_PART(address, '@', 2) in queries
ALTER TABLE credentials ADD COLUMN IF NOT EXISTS domain TEXT;

-- Step 3: Backfill domain column for existing credentials
-- The trigger handles new rows, this handles existing rows
UPDATE credentials
SET domain = SPLIT_PART(address, '@', 2)
WHERE domain IS NULL;

-- Step 4: Make domain column NOT NULL after backfill
-- Safe because: (1) existing rows backfilled, (2) new rows get value from trigger
ALTER TABLE credentials ALTER COLUMN domain SET NOT NULL;

-- Step 5: Create index on domain for fast lookups
CREATE INDEX idx_credentials_domain ON credentials (domain);

-- Step 6: Create composite index for ACL lookups with account_id and primary_identity
-- This optimizes the "anyone" ACL check that joins credentials
CREATE INDEX idx_credentials_account_primary ON credentials (account_id, primary_identity) WHERE primary_identity = TRUE;

-- Step 7: Create composite index for ACL mailbox + rights lookups
-- This optimizes queries that check both ACL existence and rights in one pass
CREATE INDEX idx_mailbox_acls_mailbox_account_rights ON mailbox_acls (mailbox_id, account_id, rights);

-- Step 8: Create composite index for "anyone" ACL with rights
-- This optimizes the "anyone" identifier lookups with rights filtering
CREATE INDEX idx_mailbox_acls_anyone_rights ON mailbox_acls (mailbox_id, rights) WHERE identifier = 'anyone';

-- Step 9: Add composite index on mailboxes for shared + owner_domain lookups
-- This helps with filtering shared mailboxes by owner domain
CREATE INDEX idx_mailboxes_shared_owner_domain ON mailboxes (is_shared, owner_domain)
WHERE is_shared = TRUE AND owner_domain IS NOT NULL;
