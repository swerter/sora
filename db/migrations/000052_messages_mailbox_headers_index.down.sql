-- Drop the covering index for header SEARCH/SORT. Header searches fall back to filtering
-- the mailbox's heap rows (or, where LIKE is still emitted, the trigram GINs). On a large
-- database use DROP INDEX CONCURRENTLY out-of-band; the statement below then no-ops.

DROP INDEX IF EXISTS idx_messages_mailbox_headers;
