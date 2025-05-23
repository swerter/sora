CREATE TABLE IF NOT EXISTS users (
	id BIGSERIAL PRIMARY KEY,
	created_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

-- A table to store user passwords and identities
CREATE TABLE IF NOT EXISTS passwords (
	id BIGSERIAL PRIMARY KEY,
	user_id BIGINT REFERENCES users(id),
	username TEXT UNIQUE NOT NULL, 	
	password TEXT NOT NULL,
	primary_identity BOOLEAN DEFAULT FALSE, -- Flag to indicate if this is the primary identity
	created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
	updated_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

-- Index to ensure that a user can have at most one primary identity.
CREATE UNIQUE INDEX IF NOT EXISTS idx_passwords_user_id_one_primary ON passwords (user_id) WHERE primary_identity IS TRUE;

CREATE TABLE IF NOT EXISTS mailboxes (
	id BIGSERIAL PRIMARY KEY,	
	user_id BIGINT REFERENCES users(id),
	highest_uid BIGINT DEFAULT 0 NOT NULL,                       -- The highest UID in the mailbox
	name TEXT NOT NULL,
	uid_validity BIGINT NOT NULL,                                -- Include uid_validity column for IMAP
	parent_id BIGINT REFERENCES mailboxes(id) ON DELETE CASCADE, -- Self-referencing for parent mailbox	
	subscribed BOOLEAN DEFAULT TRUE,  							 -- New field to track mailbox subscription status
	UNIQUE (user_id, name, parent_id), 							 -- Enforce unique mailbox names per user and parent mailbox
	created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
	updated_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

-- Index for faster mailbox lookups by user_id and case insensitive name
CREATE INDEX IF NOT EXISTS idx_mailboxes_lower_name_parent_id ON mailboxes (user_id, LOWER(name), parent_id);

-- Partial unique index for top-level mailboxes
CREATE UNIQUE INDEX IF NOT EXISTS idx_mailboxes_user_id_name_top_level_unique ON mailboxes (user_id, name)
	WHERE parent_id IS NULL;

-- Index for faster mailbox lookups by user_id and subscription status
CREATE INDEX IF NOT EXISTS idx_mailboxes_user_subscribed ON mailboxes (user_id, subscribed);

-- Index for faster mailbox lookups by user_id
CREATE INDEX IF NOT EXISTS idx_mailboxes_user_id ON mailboxes (user_id);

-- Index to speed up parent-child hierarchy lookups
CREATE INDEX IF NOT EXISTS idx_mailboxes_parent_id ON mailboxes (parent_id);

CREATE SEQUENCE IF NOT EXISTS messages_modseq;

CREATE TABLE IF NOT EXISTS messages (
	-- Unique message ID, also the UID of messages in a mailbox
	id BIGSERIAL PRIMARY KEY,       

    -- The user who owns the message
	user_id BIGINT REFERENCES users(id) ON DELETE NO ACTION, 

	uid BIGINT NOT NULL,                -- The message UID in its mailbox
	content_hash VARCHAR(64) NOT NULL,	-- Hash of the message content for deduplication
	uploaded BOOLEAN DEFAULT FALSE,	    -- Flag to indicate if the message was uploaded to S3
	recipients_json JSONB NOT NULL,	    -- JSONB field to store recipients
	message_id TEXT NOT NULL, 		    -- The Message-ID from the message headers
	in_reply_to TEXT,					-- The In-Reply-To header from the message
	subject TEXT,						-- Subject of the message
	sent_date TIMESTAMPTZ NOT NULL,		-- The date the message was sent
	internal_date TIMESTAMPTZ NOT NULL, -- The date the message was received
	flags INTEGER NOT NULL,				-- Bitwise flags for the message (e.g., \Seen, \Flagged)
	size INTEGER NOT NULL,				-- Size of the message in bytes
	body_structure BYTEA NOT NULL,      -- Serialized BodyStructure of the message
	text_body TEXT NOT NULL, 			-- Text body of the message
	text_body_tsv tsvector,				-- Full-text search index for text_body

	--
	-- Keep messages if mailbox is deleted by nullifying the mailbox_id
	--
	mailbox_id BIGINT REFERENCES mailboxes(id) ON DELETE SET NULL, 

	--
	-- Information for restoring messages from S3
	--
	mailbox_path TEXT,			    -- Store the mailbox path for restoring messages

	flags_changed_at TIMESTAMPTZ,     -- Track the last time flags were changed
	expunged_at TIMESTAMPTZ,			-- Track the last time the message was expunged

	created_modseq BIGINT NOT NULL,
	updated_modseq BIGINT,
	expunged_modseq BIGINT,
	created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
	updated_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

-- Index for faster lookups by user_id and mailbox_id
CREATE INDEX IF NOT EXISTS idx_messages_expunged_range ON messages (user_id, content_hash, expunged_at) WHERE expunged_at IS NOT NULL;

-- Index to speed up message lookups by mailbox_id (for listing, searching)
CREATE INDEX IF NOT EXISTS idx_messages_mailbox_id ON messages (mailbox_id);

CREATE INDEX IF NOT EXISTS idx_messages_content_hash ON messages (content_hash);
CREATE INDEX IF NOT EXISTS idx_messages_expunged_content_hash ON messages (content_hash) WHERE expunged_at IS NOT NULL;

-- Index to speed up message lookups by message_id
CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages (LOWER(message_id));

-- Index to speed up message lookups by in_reply_to
CREATE INDEX IF NOT EXISTS idx_messages_in_reply_to ON messages (LOWER(in_reply_to));

-- Index to speed up message lookups by mailbox_id and uid
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_mailbox_id_uid ON messages (mailbox_id, uid);

-- Index to quickly search messages by internal_date (for date-based queries)
CREATE INDEX IF NOT EXISTS idx_messages_internal_date ON messages (internal_date);
CREATE INDEX IF NOT EXISTS idx_messages_sent_date ON messages (sent_date);
CREATE INDEX IF NOT EXISTS idx_messages_flags_changed_at ON messages (flags_changed_at);
CREATE INDEX IF NOT EXISTS idx_messages_expunged_at ON messages (expunged_at);

-- Index for flags to speed up searches for seen messages
CREATE INDEX IF NOT EXISTS idx_messages_flag_seen ON messages ((flags & 1)) WHERE (flags & 1) != 0;

-- Modseq index for fast lookups
CREATE INDEX IF NOT EXISTS idx_messages_created_modseq ON messages (created_modseq);
CREATE INDEX IF NOT EXISTS idx_messages_updated_modseq ON messages (updated_modseq);
CREATE INDEX IF NOT EXISTS idx_messages_expunged_modseq ON messages (expunged_modseq);

-- Index for PollMailbox: efficiently count non-expunged messages per mailbox
CREATE INDEX IF NOT EXISTS idx_messages_mailbox_id_expunged_at_is_null ON messages (mailbox_id) WHERE expunged_at IS NULL;

-- Index for PollMailbox: efficiently filter messages by mailbox_id and expunged_modseq
CREATE INDEX IF NOT EXISTS idx_messages_mailbox_id_expunged_modseq ON messages (mailbox_id, expunged_modseq);

-- Index for faster searches on the subject field
-- This index uses the pg_trgm extension for trigram similarity searches
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_messages_subject_trgm ON messages USING gin (LOWER(subject) gin_trgm_ops);

-- Index for full-text search on the text_body field
CREATE INDEX IF NOT EXISTS idx_messages_text_body_tsv ON messages USING GIN (text_body_tsv);

-- Index recipients_json for faster searches on recipients
-- This index uses the jsonb_path_ops for efficient querying
CREATE INDEX IF NOT EXISTS idx_messages_recipients_json ON messages USING GIN (recipients_json jsonb_path_ops);

-- Pending uploads table for processing messages at own pace
CREATE TABLE IF NOT EXISTS pending_uploads (
	id BIGSERIAL PRIMARY KEY,
	instance_id TEXT NOT NULL, -- Unique identifier for the instance processing the upload, e.g., hostname
	content_hash VARCHAR(64) NOT NULL,	
	attempts INTEGER DEFAULT 0,
	last_attempt TIMESTAMPTZ,
	size INTEGER NOT NULL,
	created_at TIMESTAMPTZ DEFAULT now(),
	updated_at TIMESTAMPTZ DEFAULT now()
);

-- Index for retry loop: ordered by creation time
CREATE INDEX IF NOT EXISTS idx_pending_uploads_created_at ON pending_uploads (created_at);

-- Index to support the primary query in ListPendingUploads
CREATE INDEX IF NOT EXISTS idx_pending_uploads_instance_id_created_at ON pending_uploads (instance_id, created_at);

-- Index for retry attempts (if you ever query by attempt count or want to exclude "too many attempts")
CREATE INDEX IF NOT EXISTS idx_pending_uploads_attempts ON pending_uploads (attempts);

-- Index to quickly check retries by file age
CREATE INDEX IF NOT EXISTS idx_pending_uploads_last_attempt ON pending_uploads (last_attempt);

-- Index on content_hash to speed up deduplication checks
CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_uploads_content_hash ON pending_uploads (content_hash);

--
-- SIEVE scripts
--
CREATE TABLE IF NOT EXISTS sieve_scripts (
	id BIGSERIAL PRIMARY KEY,
	user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
	active BOOLEAN NOT NULL DEFAULT TRUE,
	name TEXT NOT NULL,
	script TEXT NOT NULL,
	created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
	updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
	UNIQUE (user_id, name) 	-- Enforce unique script name per user
);

-- Index to speed up sieve script lookups by user_id
CREATE INDEX IF NOT EXISTS idx_sieve_scripts_user_id ON sieve_scripts (user_id);

-- Index to ensure that a user can have at most one active sieve script.
CREATE UNIQUE INDEX IF NOT EXISTS idx_sieve_scripts_user_id_one_active ON sieve_scripts (user_id) WHERE active IS TRUE;

-- Vacation responses tracking table
CREATE TABLE IF NOT EXISTS vacation_responses (
	id BIGSERIAL PRIMARY KEY,
	user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
	sender_address TEXT NOT NULL,
	response_date TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
	updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
	UNIQUE (user_id, sender_address, response_date) -- Enforce unique responses per user and sender
);

-- Index for faster lookups by user_id and sender_address
CREATE INDEX IF NOT EXISTS idx_vacation_responses_user_sender ON vacation_responses (user_id, sender_address);

-- Index for cleanup of old responses
CREATE INDEX IF NOT EXISTS idx_vacation_responses_response_date ON vacation_responses (response_date);
