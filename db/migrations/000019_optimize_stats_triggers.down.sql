-- Migration 019: Revert maintain_mailbox_stats and maintain_custom_flags_cache triggers to FOR EACH ROW

-- ============================================================================
-- 1. DROP STATEMENT-LEVEL TRIGGERS
-- ============================================================================
DROP TRIGGER IF EXISTS trigger_messages_stats_insert_stmt ON messages;
DROP TRIGGER IF EXISTS trigger_messages_stats_delete_stmt ON messages;
DROP TRIGGER IF EXISTS trigger_messages_stats_update_stmt ON messages;

DROP TRIGGER IF EXISTS trigger_zzz_custom_flags_cache_insert_stmt ON messages;
DROP TRIGGER IF EXISTS trigger_zzz_custom_flags_cache_delete_stmt ON messages;
DROP TRIGGER IF EXISTS trigger_zzz_custom_flags_cache_update_stmt ON messages;

-- ============================================================================
-- 2. RECREATE ROW-LEVEL TRIGGERS
-- ============================================================================

CREATE TRIGGER trigger_messages_stats_insert 
    AFTER INSERT ON messages 
    FOR EACH ROW 
    EXECUTE FUNCTION maintain_mailbox_stats();

CREATE TRIGGER trigger_messages_stats_delete 
    AFTER DELETE ON messages 
    FOR EACH ROW 
    EXECUTE FUNCTION maintain_mailbox_stats();

CREATE TRIGGER trigger_messages_stats_update 
    AFTER UPDATE ON messages 
    FOR EACH ROW 
    WHEN (OLD.flags IS DISTINCT FROM NEW.flags OR OLD.expunged_at IS DISTINCT FROM NEW.expunged_at) 
    EXECUTE FUNCTION maintain_mailbox_stats();

CREATE TRIGGER trigger_zzz_custom_flags_cache
    AFTER INSERT OR DELETE ON messages
    FOR EACH ROW
    EXECUTE FUNCTION maintain_custom_flags_cache();

CREATE TRIGGER trigger_zzz_custom_flags_cache_update
    AFTER UPDATE ON messages
    FOR EACH ROW
    WHEN (OLD.custom_flags IS DISTINCT FROM NEW.custom_flags
          OR (OLD.expunged_at IS NULL) IS DISTINCT FROM (NEW.expunged_at IS NULL))
    EXECUTE FUNCTION maintain_custom_flags_cache();
