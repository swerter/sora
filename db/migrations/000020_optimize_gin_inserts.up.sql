-- Disable fastupdate on GIN indexes to prevent unpredictable latency spikes.
-- PostgreSQL's GIN index uses a pending list for fast updates. When the list exceeds
-- gin_pending_list_limit, the inserting transaction must synchronously flush the list 
-- to the main index. This can take over 10 seconds, causing the FTS indexing statement
-- timeout to trigger and the message to become unsearchable.
--
-- Setting fastupdate = off forces every insert to directly update the GIN tree.
-- While this slightly increases the baseline cost of an insert, it guarantees
-- predictable latency and prevents the random massive timeout spikes during bulk delivery.

ALTER INDEX idx_message_contents_text_body_tsv SET (fastupdate = off);
ALTER INDEX idx_message_contents_headers_tsv SET (fastupdate = off);
