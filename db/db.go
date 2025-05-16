package db

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"maps"

	"github.com/emersion/go-imap/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/helpers"
	"golang.org/x/crypto/bcrypt"
)

//go:embed schema.sql
var schema string

// Database holds the SQL connection
type Database struct {
	Pool *pgxpool.Pool
}

// containsFlag checks if a slice of flags contains a specific flag
func (db *Database) containsFlag(flags []imap.Flag, flag imap.Flag) bool {
	for _, f := range flags {
		if f == flag {
			return true
		}
	}
	return false
}

// uidNext     int
// readOnly    bool
// lastPollAt  time.Time
// numMessages int

type MessageUpdate struct {
	UID          imap.UID
	SeqNum       uint32
	BitwiseFlags int
	IsExpunge    bool
}

type MailboxPoll struct {
	Updates     []MessageUpdate
	NumMessages uint32
	ModSeq      uint64
}

// NewDatabase initializes a new SQL database connection
func NewDatabase(ctx context.Context, host, port, user, password, dbname string, debug bool) (*Database, error) {
	// Construct the connection string
	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, password, host, port, dbname)

	// Log the connection string for debugging (without the password)
	log.Printf("Connecting to database: postgres://%s@%s:%s/%s?sslmode=disable",
		user, host, port, dbname)

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Fatalf("Unable to parse connection string: %v", err)
	}

	// Set up custom tracer for query logging
	if debug {
		config.ConnConfig.Tracer = &CustomTracer{}
	}

	// Create a connection pool
	dbPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %v", err)
	}

	// Verify the connection
	if err := dbPool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to the database: %v", err)
	}

	db := &Database{
		Pool: dbPool,
	}

	if err := db.migrate(ctx); err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes the database connection
func (db *Database) Close() {
	if db.Pool != nil {
		db.Pool.Close()
	}
}

// migrate creates necessary tables
func (db *Database) migrate(ctx context.Context) error {
	_, err := db.Pool.Exec(ctx, schema)
	return err
}

// Authenticate verifies the provided username and password, and returns the user ID if successful
func (db *Database) Authenticate(ctx context.Context, userID int64, password string) error {
	var hashedPassword string

	err := db.Pool.QueryRow(ctx, "SELECT password FROM users WHERE id = $1", userID).Scan(&hashedPassword)
	if err != nil {
		if err == pgx.ErrNoRows {
			return errors.New("user not found")
		}
		log.Printf("FATAL Failed to fetch user %d: %v", userID, err)
		return err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password)); err != nil {
		return errors.New("invalid password")
	}

	return nil
}

func (d *Database) InsertMessageCopy(ctx context.Context, srcMessageUID imap.UID, srcMailboxID int64, destMailboxID int64, destMailboxName string) (imap.UID, error) {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return 0, consts.ErrDBBeginTransactionFailed
	}
	defer tx.Rollback(ctx)

	// Lock the message row for update
	_, err = tx.Exec(ctx, `
		SELECT FROM 
			messages 
		WHERE 
			uid = $1 AND
			mailbox_id = $2
		FOR UPDATE;`, srcMessageUID, srcMailboxID)
	if err != nil {
		log.Printf("Failed to lock message row: %v", err)
		return 0, consts.ErrDBQueryFailed
	}

	var highestUID int64
	// Lock the mailbox row for update
	err = tx.QueryRow(ctx, `
		SELECT 
			highest_uid 
		FROM 
			mailboxes 
		WHERE 
			id = $1 
		FOR UPDATE;`, destMailboxID).Scan(&highestUID)
	if err != nil {
		log.Printf("Failed to fetch highest UID: %v", err)
		return 0, consts.ErrDBQueryFailed
	}

	// Update the highest UID
	err = tx.QueryRow(ctx, `
		UPDATE 
			mailboxes 
		SET 
			highest_uid = highest_uid + 1 
		WHERE 
			id = $1 
		RETURNING highest_uid`, destMailboxID).Scan(&highestUID)
	if err != nil {
		log.Printf("Failed to update highest UID: %v", err)
		return 0, consts.ErrDBUpdateFailed
	}

	var newMsgUID imap.UID
	err = tx.QueryRow(ctx, `
		INSERT INTO messages
			(user_id, mailbox_id, mailbox_name, uid, content_hash, message_id, flags, internal_date, size, subject, sent_date, in_reply_to, body_structure, uploaded, recipients_json, text_body, text_body_tsv, created_modseq)
		SELECT
			user_id, $1, $2, $3, content_hash, message_id, flags, internal_date, size, subject, sent_date, in_reply_to, body_structure, uploaded, recipients_json, text_body, text_body_tsv, nextval('messages_modseq')
		FROM
			messages
		WHERE
			mailbox_id = $4 AND
			uid = $5
		RETURNING uid
	`, destMailboxID, destMailboxName, highestUID, srcMailboxID, srcMessageUID).Scan(&newMsgUID)

	// TODO: this should not be a fatal error
	if err != nil {
		// If unique constraint violation, return an error
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
			log.Print("Message with same id already exists in mailbox")
			return 0, consts.ErrDBUniqueViolation
		}
		log.Printf("Failed to insert message into database: %v", err)
		return 0, consts.ErrDBInsertFailed
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return 0, consts.ErrDBCommitTransactionFailed
	}

	return newMsgUID, nil
}

type InsertMessageOptions struct {
	UserID        int64
	MailboxID     int64
	MailboxName   string
	ContentHash   string
	MessageID     string
	Flags         []imap.Flag
	InternalDate  time.Time
	Size          int64
	Subject       string
	PlaintextBody *string
	SentDate      time.Time
	InReplyTo     []string
	BodyStructure *imap.BodyStructure
	Recipients    []helpers.Recipient
}

func (d *Database) InsertMessage(ctx context.Context, options *InsertMessageOptions, upload PendingUpload) (messageID int64, uid int64, err error) {
	saneMessageID := helpers.SanitizeUTF8(options.MessageID)
	if saneMessageID == "" {
		log.Printf("MessageID is empty after sanitization, generating a new one without modifying the message.")
		// Generate a new message ID if not provided
		saneMessageID = fmt.Sprintf("<%d@%s>", time.Now().UnixNano(), options.MailboxName)
	}

	bodyStructureData, err := helpers.SerializeBodyStructureGob(options.BodyStructure)
	if err != nil {
		log.Printf("Failed to serialize BodyStructure: %v", err)
		return 0, 0, consts.ErrSerializationFailed
	}

	if options.InternalDate.IsZero() {
		options.InternalDate = time.Now()
	}

	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return 0, 0, consts.ErrDBBeginTransactionFailed
	}
	defer tx.Rollback(ctx)

	var highestUID int64

	// Lock mailbox and get current UID
	err = tx.QueryRow(ctx, `SELECT highest_uid FROM mailboxes WHERE id = $1 FOR UPDATE;`, options.MailboxID).Scan(&highestUID)
	if err != nil {
		log.Printf("Failed to fetch highest UID: %v", err)
		return 0, 0, consts.ErrDBQueryFailed
	}

	// Bump UID
	err = tx.QueryRow(ctx, `UPDATE mailboxes SET highest_uid = highest_uid + 1 WHERE id = $1 RETURNING highest_uid`, options.MailboxID).Scan(&highestUID)
	if err != nil {
		log.Printf("Failed to update highest UID: %v", err)
		return 0, 0, consts.ErrDBUpdateFailed
	}

	recipientsJSON, err := json.Marshal(options.Recipients)
	if err != nil {
		log.Printf("Failed to marshal recipients: %v", err)
		return 0, 0, consts.ErrSerializationFailed
	}

	inReplyToStr := strings.Join(options.InReplyTo, " ")
	bitwiseFlags := FlagsToBitwise(options.Flags)

	var messageRowId int64

	plaintextBody := ""
	if options.PlaintextBody != nil {
		plaintextBody = *options.PlaintextBody
	}

	// Sanitize inputs
	saneSubject := helpers.SanitizeUTF8(options.Subject)
	saneInReplyToStr := helpers.SanitizeUTF8(inReplyToStr)
	sanePlaintextBody := helpers.SanitizeUTF8(plaintextBody)

	err = tx.QueryRow(ctx, `
		INSERT INTO messages
			(user_id, mailbox_id, mailbox_name, uid, message_id, content_hash, flags, internal_date, size, text_body, text_body_tsv, subject, sent_date, in_reply_to, body_structure, recipients_json, created_modseq)
		VALUES
			(@user_id, @mailbox_id, @mailbox_name, @uid, @message_id, @content_hash, @flags, @internal_date, @size, @text_body, to_tsvector('simple', @text_body), @subject, @sent_date, @in_reply_to, @body_structure, @recipients_json, nextval('messages_modseq'))
		RETURNING id
	`, pgx.NamedArgs{
		"user_id":         options.UserID,
		"mailbox_id":      options.MailboxID,
		"mailbox_name":    options.MailboxName,
		"uid":             highestUID,
		"message_id":      saneMessageID,
		"content_hash":    options.ContentHash,
		"flags":           bitwiseFlags,
		"internal_date":   options.InternalDate,
		"size":            options.Size,
		"text_body":       sanePlaintextBody,
		"subject":         saneSubject,
		"sent_date":       options.SentDate,
		"in_reply_to":     saneInReplyToStr,
		"body_structure":  bodyStructureData,
		"recipients_json": recipientsJSON,
	}).Scan(&messageRowId)

	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
			// Unique constraint violation. Check if it's due to message_id and if we can return the existing message.
			// The saneMessageID was used in the INSERT attempt.

			log.Printf("Unique constraint violation for MessageID '%s' in MailboxID %d. Attempting to find existing message.", saneMessageID, options.MailboxID)
			var existingID, existingUID int64
			// Query within the same transaction. If we return successfully from here,
			// the defer tx.Rollback(ctx) will roll back the attempted INSERT and UID bump.
			queryErr := tx.QueryRow(ctx,
				`SELECT id, uid FROM messages 
					 WHERE user_id = $1 AND mailbox_id = $2 AND message_id = $3 AND expunged_at IS NULL`,
				options.UserID, options.MailboxID, saneMessageID).Scan(&existingID, &existingUID)

			if queryErr == nil {
				log.Printf("Found existing message for MessageID '%s' in MailboxID %d. Returning existing ID: %d, UID: %d. Current transaction will be rolled back.", saneMessageID, options.MailboxID, existingID, existingUID)
				return existingID, existingUID, nil // Return existing message details
			} else if errors.Is(queryErr, pgx.ErrNoRows) {
				// This is unexpected: unique constraint fired, but we can't find the row by message_id.
				// Could be a conflict on UID or another unique constraint.
				log.Printf("Unique constraint violation for MailboxID %d (MessageID '%s'), but no existing non-expunged message found by this MessageID. Falling back to unique violation error. Lookup error: %v", options.MailboxID, saneMessageID, queryErr)
			} else {
				// Error during the lookup query
				log.Printf("Error querying for existing message after unique constraint violation (MailboxID %d, MessageID '%s'): %v. Falling back to unique violation error.", options.MailboxID, saneMessageID, queryErr)
			}

			// Fallback to returning the original unique violation error if MessageID was empty or lookup failed.
			log.Printf("Original unique constraint violation error for MailboxID %d, MessageID '%s': %v", options.MailboxID, saneMessageID, err)
			return 0, 0, consts.ErrDBUniqueViolation // Original error
		}
		log.Printf("Failed to insert message into database: %v", err)
		return 0, 0, consts.ErrDBInsertFailed
	}

	_, err = tx.Exec(ctx, `
	INSERT INTO pending_uploads (instance_id, content_hash, size, created_at)
	VALUES ($1, $2, $3, $4) ON CONFLICT (content_hash) DO NOTHING`,
		upload.InstanceID,
		options.ContentHash,
		upload.Size,
		time.Now(),
	)
	// Delete any other instances of the same upload with the same content hash
	// Mark all messages with the same content hash as uploaded

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return 0, 0, consts.ErrDBCommitTransactionFailed
	}

	return messageRowId, highestUID, nil
}

func (db *Database) MoveMessages(ctx context.Context, ids *[]imap.UID, srcMailboxID, destMailboxID int64) (map[int64]int64, error) {
	// Map to store the original message ID to new UID mapping
	messageUIDMap := make(map[int64]int64)

	// Ensure the destination mailbox exists
	_, err := db.GetMailbox(ctx, destMailboxID)
	if err != nil {
		log.Printf("Failed to fetch mailbox %d: %v", destMailboxID, err)
		return nil, consts.ErrMailboxNotFound
	}

	// Begin a transaction
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return nil, consts.ErrInternalError
	}
	defer tx.Rollback(ctx) // Rollback if any error occurs

	// Move the messages and assign new UIDs in the destination mailbox
	query := `
		WITH inserted_messages AS (
			-- Insert the selected rows into the destination mailbox
			INSERT INTO messages (
					user_id,
					content_hash, 
					uploaded,
					message_id, 
					in_reply_to, 
					subject, 
					sent_date, 
					internal_date, 
					flags, 
					size, 
					body_structure, 
					recipient_json,
					text_body, 
					text_body_tsv, 
					mailbox_id, 
					mailbox_path, 
					flags_changed_at,
					created_modseq
			)
			SELECT
			    user_id,
					content_hash, 
					uploaded,
					message_id, 
					in_reply_to, 
					subject, 
					sent_date, 
					internal_date, 
					flags, 
					size, 
					body_structure, 
					recipient_json,
					text_body, 
					text_body_tsv, 
					$2 AS mailbox_id,  -- Assign to the new mailbox
					mailbox_path, 
					NOW() AS flags_changed_at,
					nextval('messages_modseq')
			FROM messages
			WHERE mailbox_id = $1 AND id = ANY($3)
			RETURNING id
	),
	numbered_messages AS (
			-- Generate new UIDs based on row numbers
			SELECT id, 
						ROW_NUMBER() OVER (ORDER BY id) + (SELECT COALESCE(MAX(id), 0) FROM messages WHERE mailbox_id = $2) AS uid
			FROM inserted_messages
	)
	-- Delete the original messages from the source mailbox
	DELETE FROM messages
	WHERE mailbox_id = $1 AND id = ANY($3)
	RETURNING id;
	`

	// Execute the query
	rows, err := tx.Query(ctx, query, srcMailboxID, destMailboxID, ids)
	if err != nil {
		log.Printf("Failed to move messages: %v", err)
		return nil, fmt.Errorf("failed to move messages: %v", err)
	}
	defer rows.Close()

	// Iterate through the moved messages to map original ID to the new UID
	for rows.Next() {
		var messageID, newUID int64
		if err := rows.Scan(&messageID, &newUID); err != nil {
			return nil, fmt.Errorf("failed to scan message ID and UID: %v", err)
		}
		messageUIDMap[messageID] = newUID
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return nil, consts.ErrInternalError
	}

	return messageUIDMap, nil
}

func selectNumSet(numSet imap.NumSet, baseQuery string, args []any) (string, []any) {
	var conditions []string

	switch set := numSet.(type) {
	case imap.SeqSet:

		if len(set) == 0 {
			return baseQuery + " (1=0)", args // Always false if set is empty
		}
		for _, seqRange := range set {
			condition := "(true"
			if seqRange.Start != 0 {
				args = append(args, seqRange.Start)
				condition += fmt.Sprintf(" AND seqnum >= $%d", len(args))
			}
			if seqRange.Stop != 0 {
				args = append(args, seqRange.Stop)
				condition += fmt.Sprintf(" AND seqnum <= $%d", len(args))
			}
			condition += ")"
			conditions = append(conditions, condition)
		}
	case imap.UIDSet:
		if len(set) == 0 {
			return baseQuery + " (1=0)", args // Always false if set is empty
		}
		for _, uidRange := range set {
			condition := "(true"
			if uidRange.Start != 0 {
				args = append(args, uint32(uidRange.Start))
				condition += fmt.Sprintf(" AND uid >= $%d", len(args))
			}
			if uidRange.Stop != 0 {
				args = append(args, uint32(uidRange.Stop))
				condition += fmt.Sprintf(" AND uid <= $%d", len(args))
			}
			condition += ")"
			conditions = append(conditions, condition)
		}
	default:
		panic("unsupported NumSet type") // unreachable
	}

	if len(conditions) == 0 { // Should be caught by the len(set) == 0 checks above, but as a safeguard
		return baseQuery + " (1=0)", args
	}

	return baseQuery + " (" + strings.Join(conditions, " OR ") + ")", args
}

// GetMessagesBySeqSet fetches messages from the database based on the NumSet and mailbox ID.
// This works for both sequence numbers (SeqSet) and UIDs (UIDSet).
func (db *Database) GetMessagesBySeqSet(ctx context.Context, mailboxID int64, numSet imap.NumSet) ([]Message, error) {
	var messages []Message

	query := `
		SELECT * FROM (
			SELECT user_id, uid, mailbox_id, content_hash, uploaded, flags, internal_date, size, body_structure,
				created_modseq, updated_modseq, expunged_modseq,
				row_number() OVER (ORDER BY id) AS seqnum
			FROM messages
			WHERE
				mailbox_id = $1 AND
				expunged_at IS NULL
		) AS sub WHERE
	`
	args := []any{mailboxID}

	query, args = selectNumSet(numSet, query, args)

	// Execute the query
	rows, err := db.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages: %v", err)
	}
	defer rows.Close()

	// Scan the results and append to the messages slice
	for rows.Next() {
		var msg Message
		var bodyStructureBytes []byte
		if err := rows.Scan(&msg.UserID, &msg.UID, &msg.MailboxID, &msg.ContentHash, &msg.IsUploaded,
			&msg.BitwiseFlags, &msg.InternalDate, &msg.Size, &bodyStructureBytes, &msg.CreatedModSeq,
			&msg.UpdatedModSeq, &msg.ExpungedModSeq, &msg.Seq); err != nil {
			return nil, fmt.Errorf("failed to scan message: %v", err)
		}
		bodyStructure, err := helpers.DeserializeBodyStructureGob(bodyStructureBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize BodyStructure: %v", err)
		}
		msg.BodyStructure = *bodyStructure
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error fetching messages: %v", err)
	}

	return messages, nil
}

func (db *Database) SetMessageFlags(ctx context.Context, messageID imap.UID, mailboxID int64, newFlags []imap.Flag) (*[]imap.Flag, error) {
	var updatedFlagsBitwise int
	flags := FlagsToBitwise(newFlags)
	err := db.Pool.QueryRow(ctx, `
		UPDATE messages
		SET flags = $1, flags_changed_at = $2, updated_modseq = nextval('messages_modseq')
		WHERE uid = $3 AND mailbox_id = $4
		RETURNING flags
	`, flags, time.Now(), messageID, mailboxID).Scan(&updatedFlagsBitwise)
	if err != nil {
		return nil, fmt.Errorf("failed to set message flags for UID %d in mailbox %d: %w", messageID, mailboxID, err)
	}
	updatedFlags := BitwiseToFlags(updatedFlagsBitwise)
	return &updatedFlags, nil
}

func (db *Database) AddMessageFlags(ctx context.Context, messageUID imap.UID, mailboxID int64, newFlags []imap.Flag) (*[]imap.Flag, error) {
	var updatedFlagsBitwise int
	flags := FlagsToBitwise(newFlags)

	err := db.Pool.QueryRow(ctx, `
		UPDATE
			messages
		SET
			flags = flags | $1,
			flags_changed_at = $2,
			updated_modseq = nextval('messages_modseq')
		WHERE
			uid = $3 AND
			mailbox_id = $4
		RETURNING flags`, flags, time.Now(), messageUID, mailboxID).Scan(&updatedFlagsBitwise)
	if err != nil {
		return nil, err
	}

	updatedFlags := BitwiseToFlags(updatedFlagsBitwise)
	return &updatedFlags, nil
}

func (db *Database) RemoveMessageFlags(ctx context.Context, messageID imap.UID, mailboxID int64, newFlags []imap.Flag) (*[]imap.Flag, error) {
	var updatedFlagsBitwise int
	flags := FlagsToBitwise(newFlags)
	negatedFlags := ^flags

	err := db.Pool.QueryRow(ctx, `
	UPDATE
		messages
	SET
		flags = flags & $1,
		flags_changed_at = $2,
		updated_modseq = nextval('messages_modseq')
	WHERE
		uid = $3 AND
		mailbox_id = $4
	RETURNING flags`, negatedFlags, time.Now(), messageID, mailboxID).Scan(&updatedFlagsBitwise)
	if err != nil {
		return nil, err
	}
	updatedFlags := BitwiseToFlags(updatedFlagsBitwise)
	return &updatedFlags, nil
}

func (db *Database) ExpungeMessageUIDs(ctx context.Context, mailboxID int64, uids ...imap.UID) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE messages
		SET expunged_at = NOW(), expunged_modseq = nextval('messages_modseq')
		WHERE mailbox_id = $1 AND uid = ANY($2)
	`, mailboxID, uids)
	if err != nil {
		return err
	}
	return nil
}

// --- Recipients
func (db *Database) GetMessageEnvelope(ctx context.Context, UID imap.UID, mailboxID int64) (*imap.Envelope, error) {
	var envelope imap.Envelope

	var inReplyTo string
	var recipientsJSON []byte

	var messageId int64
	err := db.Pool.QueryRow(ctx, `
        SELECT 
            id, internal_date, subject, in_reply_to, message_id, recipients_json 
        FROM 
            messages
        WHERE 
            uid = $1 AND
						mailbox_id = $2 AND
						expunged_at IS NULL
    `, UID, mailboxID).Scan(
		&messageId,
		&envelope.Date,
		&envelope.Subject,
		&inReplyTo,
		&envelope.MessageID,
		&recipientsJSON,
	)
	if err != nil {
		log.Printf("Failed to fetch envelope fields: %v", err)
		return nil, err
	}

	// Split the In-Reply-To header into individual message IDs
	envelope.InReplyTo = strings.Split(inReplyTo, " ")

	var recipients []helpers.Recipient
	if err := json.Unmarshal(recipientsJSON, &recipients); err != nil {
		log.Printf("Failed to decode recipients JSON: %v", err)
		return nil, err
	}

	for _, recipient := range recipients {
		var addressType, name, emailAddress string
		addressType = recipient.AddressType
		name = recipient.Name
		emailAddress = recipient.EmailAddress

		parts := strings.Split(emailAddress, "@")
		mailboxPart, hostNamePart := parts[0], parts[1]

		address := imap.Address{
			Name:    name,
			Mailbox: mailboxPart,
			Host:    hostNamePart,
		}

		switch addressType {
		case "to":
			envelope.To = append(envelope.To, address)
		case "cc":
			envelope.Cc = append(envelope.Cc, address)
		case "bcc":
			envelope.Bcc = append(envelope.Bcc, address)
		case "reply-to":
			envelope.ReplyTo = append(envelope.ReplyTo, address)
		case "from":
			envelope.From = append(envelope.From, address)
		default:
			log.Printf("Warning: Unhandled address type: %s", addressType)
		}
	}

	return &envelope, nil
}

// buildSearchCriteria builds the SQL WHERE clause for the search criteria
func (db *Database) buildSearchCriteria(criteria *imap.SearchCriteria, paramPrefix string, paramCounter *int) (string, pgx.NamedArgs, error) {
	var conditions []string
	args := pgx.NamedArgs{}

	nextParam := func() string {
		*paramCounter++
		return fmt.Sprintf("%s%d", paramPrefix, *paramCounter)
	}

	// For SeqNum
	for _, seqSet := range criteria.SeqNum {
		seqCond, seqArgs := buildNumSetCondition(seqSet, "seqnum", paramPrefix, paramCounter)
		maps.Copy(args, seqArgs)
		conditions = append(conditions, seqCond)
	}

	// For UID
	for _, uidSet := range criteria.UID {
		uidCond, uidArgs := buildNumSetCondition(uidSet, "uid", paramPrefix, paramCounter)
		maps.Copy(args, uidArgs)
		conditions = append(conditions, uidCond)
	}

	// Date filters
	if !criteria.Since.IsZero() {
		param := nextParam()
		args[param] = criteria.Since
		conditions = append(conditions, fmt.Sprintf("internal_date >= @%s", param))
	}
	if !criteria.Before.IsZero() {
		param := nextParam()
		args[param] = criteria.Before
		conditions = append(conditions, fmt.Sprintf("internal_date <= @%s", param))
	}
	if !criteria.SentSince.IsZero() {
		param := nextParam()
		args[param] = criteria.SentSince
		conditions = append(conditions, fmt.Sprintf("sent_date >= @%s", param))
	}
	if !criteria.SentBefore.IsZero() {
		param := nextParam()
		args[param] = criteria.SentBefore
		conditions = append(conditions, fmt.Sprintf("sent_date <= @%s", param))
	}

	// Message size
	if criteria.Larger > 0 {
		param := nextParam()
		args[param] = criteria.Larger
		conditions = append(conditions, fmt.Sprintf("size > @%s", param))
	}
	if criteria.Smaller > 0 {
		param := nextParam()
		args[param] = criteria.Smaller
		conditions = append(conditions, fmt.Sprintf("size < @%s", param))
	}

	// Body full-text search
	for _, bodyCriteria := range criteria.Body {
		param := nextParam()
		args[param] = bodyCriteria
		conditions = append(conditions, fmt.Sprintf("text_body_tsv @@ plainto_tsquery('simple', @%s)", param))
	}
	if len(criteria.Text) > 0 {
		return "", nil, &imap.Error{
			Type: imap.StatusResponseTypeNo,
			Text: "SEARCH criteria TEXT is not supported",
		}
	}

	// Flags
	for _, flag := range criteria.Flag {
		param := nextParam()
		args[param] = FlagToBitwise(flag)
		conditions = append(conditions, fmt.Sprintf("(flags & @%s) != 0", param))
	}
	for _, flag := range criteria.NotFlag {
		param := nextParam()
		args[param] = FlagToBitwise(flag)
		conditions = append(conditions, fmt.Sprintf("(flags & @%s) = 0", param))
	}

	// Header conditions
	for _, header := range criteria.Header {
		lowerValue := strings.ToLower(header.Value)
		lowerKey := strings.ToLower(header.Key)
		switch lowerKey {
		case "subject":
			param := nextParam()
			args[param] = "%" + lowerValue + "%"
			conditions = append(conditions, fmt.Sprintf("LOWER(subject) LIKE @%s", param))
		case "message-id":
			param := nextParam()
			// if the message ID is wrapped in <messageId>, we need to remove the brackets
			if strings.HasPrefix(lowerValue, "<") && strings.HasSuffix(lowerValue, ">") {
				lowerValue = lowerValue[1 : len(lowerValue)-1]
			}
			args[param] = lowerValue
			conditions = append(conditions, fmt.Sprintf("LOWER(message_id) = @%s", param))
		case "in-reply-to":
			param := nextParam()
			args[param] = lowerValue
			conditions = append(conditions, fmt.Sprintf("LOWER(in_reply_to) = @%s", param))
		case "from", "to", "cc", "bcc", "reply-to":
			recipientJSONParam := nextParam()
			recipientValue := fmt.Sprintf(`[{"type": "%s", "email": "%s"}]`, lowerKey, lowerValue)
			args[recipientJSONParam] = recipientValue
			conditions = append(conditions, fmt.Sprintf(`recipients_json @> @%s::jsonb`, recipientJSONParam))
		default:
			return "", nil, &imap.Error{
				Type: imap.StatusResponseTypeNo,
				Text: "SEARCH criteria generic HEADER is not supported",
			}
		}
	}

	// Recursive NOT
	for _, notCriteria := range criteria.Not {
		subCond, subArgs, err := db.buildSearchCriteria(&notCriteria, paramPrefix, paramCounter)
		if err != nil {
			return "", nil, err
		}
		for k, v := range subArgs {
			args[k] = v
		}
		conditions = append(conditions, fmt.Sprintf("NOT (%s)", subCond))
	}

	// Recursive OR
	for _, orPair := range criteria.Or {
		leftCond, leftArgs, err := db.buildSearchCriteria(&orPair[0], paramPrefix, paramCounter)
		if err != nil {
			return "", nil, err
		}
		rightCond, rightArgs, err := db.buildSearchCriteria(&orPair[1], paramPrefix, paramCounter)
		if err != nil {
			return "", nil, err
		}

		maps.Copy(args, leftArgs)
		maps.Copy(args, rightArgs)

		conditions = append(conditions, fmt.Sprintf("(%s OR %s)", leftCond, rightCond))
	}

	finalCondition := "1=1"
	if len(conditions) > 0 {
		finalCondition = strings.Join(conditions, " AND ")
	}

	return finalCondition, args, nil
}

func buildNumSetCondition(numSet imap.NumSet, columnName string, paramPrefix string, paramCounter *int) (string, pgx.NamedArgs) {
	args := pgx.NamedArgs{}
	var conditions []string

	nextParam := func() string {
		*paramCounter++
		return fmt.Sprintf("%s%d", paramPrefix, *paramCounter)
	}

	switch s := numSet.(type) {
	case imap.SeqSet:
		for _, r := range s {
			if r.Start == r.Stop {
				param := nextParam()
				args[param] = r.Start
				conditions = append(conditions, fmt.Sprintf("%s = @%s", columnName, param))
			} else {
				startParam := nextParam()
				stopParam := nextParam()
				args[startParam] = r.Start
				args[stopParam] = r.Stop
				conditions = append(conditions, fmt.Sprintf("%s BETWEEN @%s AND @%s", columnName, startParam, stopParam))
			}
		}
	case imap.UIDSet:
		for _, r := range s {
			if r.Start == r.Stop {
				param := nextParam()
				args[param] = r.Start
				conditions = append(conditions, fmt.Sprintf("%s = @%s", columnName, param))
			} else {
				startParam := nextParam()
				stopParam := nextParam()
				args[startParam] = r.Start
				args[stopParam] = r.Stop
				conditions = append(conditions, fmt.Sprintf("%s BETWEEN @%s AND @%s", columnName, startParam, stopParam))
			}
		}
	default:
		panic("unsupported NumSet type")
	}

	finalCondition := strings.Join(conditions, " OR ")
	if len(conditions) > 1 {
		finalCondition = "(" + finalCondition + ")"
	}

	return finalCondition, args
}

func (db *Database) GetMessagesWithCriteria(ctx context.Context, mailboxID int64, criteria *imap.SearchCriteria) ([]Message, error) {
	baseQuery := `
	WITH message_seqs AS (
		SELECT
			uid,
			ROW_NUMBER() OVER (ORDER BY id) AS seqnum, -- id is needed for ordering/seqnum
			flags,
			subject,
			internal_date,
			sent_date,
			size,
			message_id,
			in_reply_to,
			recipients_json,
			text_body_tsv -- Include text_body_tsv for full-text search
		FROM messages
		WHERE mailbox_id = @mailboxID AND expunged_at IS NULL
	)
	SELECT uid, seqnum FROM message_seqs` // Only select uid and seqnum for the final result

	paramCounter := 0
	whereCondition, whereArgs, err := db.buildSearchCriteria(criteria, "p", &paramCounter)
	if err != nil {
		return nil, err
	}
	whereArgs["mailboxID"] = mailboxID

	finalQueryString := baseQuery + fmt.Sprintf(" WHERE %s ORDER BY uid", whereCondition)

	rows, err := db.Pool.Query(ctx, finalQueryString, whereArgs)
	if err != nil {
		// It's helpful to log the query and arguments when an error occurs for easier debugging.
		log.Printf("Error executing query: %s\nArgs: %#v\nError: %v", finalQueryString, whereArgs, err)
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var message Message
		// Scan only UID and SeqNum as selected in the final SELECT statement
		if err := rows.Scan(&message.UID, &message.Seq); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return messages, nil
}

func (db *Database) GetMessagesByFlag(ctx context.Context, mailboxID int64, flag imap.Flag) ([]Message, error) {
	// Convert the IMAP flag to its corresponding bitwise value
	bitwiseFlag := FlagToBitwise(flag)
	rows, err := db.Pool.Query(ctx, `
				SELECT uid, content_hash, seqnum FROM (
			SELECT uid, content_hash, ROW_NUMBER() OVER (ORDER BY id) AS seqnum
			FROM messages
			WHERE mailbox_id = $1 AND (flags & $2) != 0 AND expunged_at IS NULL
		) AS sub
	`, mailboxID, bitwiseFlag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.UID, &msg.ContentHash, &msg.Seq); err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

func (db *Database) PollMailbox(ctx context.Context, mailboxID int64, sinceModSeq uint64) (*MailboxPoll, error) {
	// Use a transaction to ensure we have a consistent view of the mailbox
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Fetch messages updated or expunged since last poll
	rows, err := tx.Query(ctx, `
		SELECT id, seq_num, flags, expunged_modseq FROM (
			SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS seq_num, flags, created_modseq, updated_modseq, expunged_modseq
			FROM messages
			WHERE mailbox_id = $1 AND (expunged_modseq IS NULL OR expunged_modseq > $2)
		) AS sub
		WHERE
			(created_modseq <= $2 AND updated_modseq > $2 AND expunged_modseq IS NULL) OR
			(created_modseq <= $2 AND expunged_modseq > $2)
		ORDER BY
			seq_num DESC
	`, mailboxID, sinceModSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to query mailbox updates: %w", err)
	}
	defer rows.Close()

	var updates []MessageUpdate
	for rows.Next() {
		var (
			update         MessageUpdate
			expungedModSeq *int64
		)

		if err := rows.Scan(&update.UID, &update.SeqNum, &update.BitwiseFlags, &expungedModSeq); err != nil {
			return nil, fmt.Errorf("failed to scan mailbox updates: %w", err)
		}

		update.IsExpunge = expungedModSeq != nil

		updates = append(updates, update)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating through mailbox updates: %w", err)
	}

	// Fetch the current number of non-expunged messages in the mailbox
	var numMessages uint32
	err = tx.QueryRow(ctx, `
		SELECT
			COUNT(*)
		FROM
			messages
		WHERE
			mailbox_id = $1 AND
			expunged_at IS NULL
	`, mailboxID).Scan(&numMessages)
	if err != nil {
		return nil, fmt.Errorf("failed to count messages in mailbox: %w", err)
	}

	var currentModSeq uint64
	err = tx.QueryRow(ctx, `SELECT last_value FROM messages_modseq`).Scan(&currentModSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch current modseq: %w", err)
	}

	return &MailboxPoll{
		Updates:     updates,
		NumMessages: numMessages,
		ModSeq:      currentModSeq,
	}, nil
}

func (db *Database) GetUserIDByAddress(ctx context.Context, username string) (int64, error) {
	var userId int64
	username = strings.ToLower(username)
	err := db.Pool.QueryRow(ctx, "SELECT id FROM users WHERE username = $1", username).Scan(&userId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return -1, consts.ErrUserNotFound
		}
		return -1, err
	}
	return userId, nil
}

func (db *Database) ListMessages(ctx context.Context, mailboxID int64) ([]Message, error) {
	var messages []Message

	query := `
			SELECT 
				uid, size, created_modseq, updated_modseq, expunged_modseq, content_hash, uploaded
			FROM 
				messages
			WHERE 
				mailbox_id = $1 AND 
				expunged_at IS NULL
			ORDER BY uid`
	rows, err := db.Pool.Query(ctx, query, mailboxID)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.UID, &msg.Size, &msg.CreatedModSeq, &msg.UpdatedModSeq, &msg.ExpungedModSeq, &msg.ContentHash, &msg.IsUploaded); err != nil {
			return nil, fmt.Errorf("failed to scan message: %v", err)
		}
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error fetching messages: %v", err)
	}

	return messages, nil
}

func (d *Database) ListS3ObjectsToDelete(ctx context.Context, olderThan time.Duration, limit int) ([]string, error) {
	threshold := time.Now().Add(-olderThan).UTC()
	rows, err := d.Pool.Query(ctx, `
		WITH deletable_hashes AS (
			SELECT content_hash
			FROM messages m
			GROUP BY content_hash
			HAVING bool_and(expunged_at IS NOT NULL AND expunged_at < $1)
		)
		SELECT content_hash FROM deletable_hashes
		LIMIT $2;
	`, threshold, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var contentHash string
		if err := rows.Scan(&contentHash); err != nil {
			continue
		}
		result = append(result, contentHash)
	}
	return result, nil
}

func (d *Database) FindExistingContentHashes(ctx context.Context, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := d.Pool.Query(ctx, `SELECT content_hash FROM messages WHERE content_hash = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var chash string
		if err := rows.Scan(&chash); err != nil {
			continue // log or ignore individual scan errors
		}
		result = append(result, chash)
	}

	return result, nil
}
