-- Rollback ACL query optimizations

-- Drop trigger first
DROP TRIGGER IF EXISTS trigger_maintain_credentials_domain ON credentials;
DROP FUNCTION IF EXISTS maintain_credentials_domain();

-- Drop new indexes
DROP INDEX IF EXISTS idx_mailboxes_shared_owner_domain;
DROP INDEX IF EXISTS idx_mailbox_acls_anyone_rights;
DROP INDEX IF EXISTS idx_mailbox_acls_mailbox_account_rights;
DROP INDEX IF EXISTS idx_credentials_account_primary;
DROP INDEX IF EXISTS idx_credentials_domain;

-- Drop domain column from credentials
ALTER TABLE credentials DROP COLUMN IF EXISTS domain;
