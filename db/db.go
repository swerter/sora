package db

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"maps"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/google/uuid"
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

// sortedKeys returns the keys of a map in sorted order
func sortedKeys(m pgx.NamedArgs) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
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
func NewDatabase(ctx context.Context, host, port, user, password, dbname string) (*Database, error) {
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
	config.ConnConfig.Tracer = &CustomTracer{}

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

func (db *Database) InsertUser(ctx context.Context, username, password string) error {
	// Hash the password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %v", err)
	}

	// Upsert user into the database (insert or update if the username already exists)
	_, err = db.Pool.Exec(ctx, `
        INSERT INTO users (username, password)
        VALUES ($1, $2)
        ON CONFLICT (username) DO UPDATE
        SET password = EXCLUDED.password
    `, username, hashedPassword)
	if err != nil {
		return fmt.Errorf("failed to upsert test user: %v", err)
	}

	log.Println("User created successfully")

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
			(user_id, mailbox_id, mailbox_name, uid, uuid, message_id, flags, internal_date, size, subject, sent_date, in_reply_to, body_structure, s3_uploaded, recipients_json, text_body, text_body_tsv, created_modseq)
		SELECT
			user_id, $1, $2, $3, uuid, message_id, flags, internal_date, size, subject, sent_date, in_reply_to, body_structure, s3_uploaded, recipients_json, text_body, text_body_tsv, nextval('messages_modseq')
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
	UUID          uuid.UUID
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

	// Lock mailbox and get current UID
	err = tx.QueryRow(ctx, `SELECT highest_uid FROM mailboxes WHERE id = $1 FOR UPDATE;`, options.MailboxID).Scan(&uid)
	if err != nil {
		log.Printf("Failed to fetch highest UID: %v", err)
		return 0, 0, consts.ErrDBQueryFailed
	}

	// Bump UID
	err = tx.QueryRow(ctx, `UPDATE mailboxes SET highest_uid = highest_uid + 1 WHERE id = $1 RETURNING highest_uid`, options.MailboxID).Scan(&uid)
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

	plaintextBody := ""
	if options.PlaintextBody != nil {
		plaintextBody = *options.PlaintextBody
	}

	// Sanitize inputs
	saneSubject := helpers.SanitizeUTF8(options.Subject)
	saneMessageID := helpers.SanitizeUTF8(options.MessageID)
	saneInReplyToStr := helpers.SanitizeUTF8(inReplyToStr)
	sanePlaintextBody := helpers.SanitizeUTF8(plaintextBody)

	err = tx.QueryRow(ctx, `
		INSERT INTO messages
			(user_id, mailbox_id, mailbox_name, uid, message_id, uuid, flags, internal_date, size, text_body, text_body_tsv, subject, sent_date, in_reply_to, body_structure, recipients_json, created_modseq)
		VALUES
			(@user_id, @mailbox_id, @mailbox_name, @uid, @message_id, @uuid, @flags, @internal_date, @size, @text_body, to_tsvector('simple', @text_body), @subject, @sent_date, @in_reply_to, @body_structure, @recipients_json, nextval('messages_modseq'))
		RETURNING id
	`, pgx.NamedArgs{
		"user_id":         options.UserID,
		"mailbox_id":      options.MailboxID,
		"mailbox_name":    options.MailboxName,
		"uid":             uid,
		"message_id":      saneMessageID,
		"uuid":            options.UUID,
		"flags":           bitwiseFlags,
		"internal_date":   options.InternalDate,
		"size":            options.Size,
		"text_body":       sanePlaintextBody,
		"subject":         saneSubject,
		"sent_date":       options.SentDate,
		"in_reply_to":     saneInReplyToStr,
		"body_structure":  bodyStructureData,
		"recipients_json": recipientsJSON,
	}).Scan(&messageID)

	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
			log.Printf("Message with ID %s already exists in mailbox %d", options.MessageID, options.MailboxID)
			return 0, 0, consts.ErrDBUniqueViolation
		}
		log.Printf("Failed to insert message into database: %v", err)
		return 0, 0, consts.ErrDBInsertFailed
	}

	_, err = tx.Exec(ctx, `
	INSERT INTO pending_uploads (message_id, uuid, file_path, s3_path, size, mailbox_name, domain_name, local_part)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		messageID,
		options.UUID,
		upload.FilePath,
		upload.S3Path,
		upload.Size,
		upload.MailboxName,
		upload.DomainName,
		upload.LocalPart,
	)

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return 0, 0, consts.ErrDBCommitTransactionFailed
	}

	return messageID, uid, nil
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
					uuid, 
					s3_uploaded,
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
					deleted_at, 
					flags_changed_at,
					created_modseq
			)
			SELECT
			    user_id,
					uuid, 
					s3_uploaded,
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
					NULL AS deleted_at, 
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

func (db *Database) GetMessageBodyStructure(ctx context.Context, messageUID imap.UID, mailboxID int64) (*imap.BodyStructure, error) {
	var bodyStructureBytes []byte

	err := db.Pool.QueryRow(ctx, `
			SELECT 
				body_structure
			FROM 
				messages
			WHERE 
				uid = $1 AND 
				mailbox_id = $2 AND
				expunged_at IS NULL`, messageUID, mailboxID).Scan(&bodyStructureBytes)
	if err != nil {
		return nil, err
	}

	// Deserialize the JSON string back into BodyStructure
	return helpers.DeserializeBodyStructureGob(bodyStructureBytes)
}

func selectNumSet(numSet imap.NumSet, query string, args []interface{}) (string, []interface{}) {
	query += "(false"
	switch set := numSet.(type) {
	case imap.SeqSet:
		for _, seqRange := range set {
			query += " OR (true"
			if seqRange.Start != 0 {
				args = append(args, seqRange.Start)
				query += fmt.Sprintf(" AND seqnum >= $%d", len(args))
			}
			if seqRange.Stop != 0 {
				args = append(args, seqRange.Stop)
				query += fmt.Sprintf(" AND seqnum <= $%d", len(args))
			}
			query += ")"
		}
	case imap.UIDSet:
		for _, uidRange := range set {
			query += " OR (true"
			if uidRange.Start != 0 {
				args = append(args, uint32(uidRange.Start))
				query += fmt.Sprintf(" AND uid >= $%d", len(args))
			}
			if uidRange.Stop != 0 {
				args = append(args, uint32(uidRange.Stop))
				query += fmt.Sprintf(" AND uid <= $%d", len(args))
			}
			query += ")"
		}
	default:
		panic("unsupported NumSet type") // unreachable
	}
	query += ")"
	return query, args
}

// GetMessagesBySeqSet fetches messages from the database based on the NumSet and mailbox ID.
// This works for both sequence numbers (SeqSet) and UIDs (UIDSet).
func (db *Database) GetMessagesBySeqSet(ctx context.Context, mailboxID int64, numSet imap.NumSet) ([]Message, error) {
	var messages []Message

	query := `
		SELECT * FROM (
			SELECT user_id, uid, mailbox_id, uuid, s3_uploaded, flags, internal_date, size, body_structure,
				row_number() OVER (ORDER BY id) AS seqnum
			FROM messages
			WHERE
				mailbox_id = $1 AND
				expunged_at IS NULL
		) AS sub WHERE
	`
	args := []interface{}{mailboxID}

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
		if err := rows.Scan(&msg.UserID, &msg.UID, &msg.MailboxID, &msg.UUID, &msg.S3Uploaded, &msg.BitwiseFlags, &msg.InternalDate, &msg.Size, &bodyStructureBytes, &msg.Seq); err != nil {
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
	deletedAt := sql.NullTime{}
	if db.containsFlag(newFlags, imap.FlagDeleted) {
		deletedAt = sql.NullTime{Time: time.Now(), Valid: true}
	}
	err := db.Pool.QueryRow(ctx, `
		UPDATE messages
		SET flags = $1, flags_changed_at = $2, deleted_at = $3, updated_modseq = nextval('messages_modseq')
		WHERE uid = $4 AND mailbox_id = $5
		RETURNING flags
	`, flags, time.Now(), deletedAt, messageID, mailboxID).Scan(&updatedFlagsBitwise)
	if err != nil {
		return nil, err
	}
	updatedFlags := BitwiseToFlags(updatedFlagsBitwise)
	return &updatedFlags, nil
}

func (db *Database) AddMessageFlags(ctx context.Context, messageUID imap.UID, mailboxID int64, newFlags []imap.Flag) (*[]imap.Flag, error) {
	var updatedFlagsBitwise int
	flags := FlagsToBitwise(newFlags)

	// Check if the deleted flag is being added
	if db.containsFlag(newFlags, imap.FlagDeleted) {
		err := db.Pool.QueryRow(ctx, `
			UPDATE
				messages
			SET
				flags = flags | $1,
				flags_changed_at = $2,
				deleted_at = $3,
				updated_modseq = nextval('messages_modseq')
			WHERE
				uid = $4 AND
				mailbox_id = $5
			RETURNING flags`, flags, time.Now(), time.Now(), messageUID, mailboxID).Scan(&updatedFlagsBitwise)
		if err != nil {
			return nil, err
		}
	} else {
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
	}

	updatedFlags := BitwiseToFlags(updatedFlagsBitwise)
	return &updatedFlags, nil
}

func (db *Database) RemoveMessageFlags(ctx context.Context, messageID imap.UID, mailboxID int64, newFlags []imap.Flag) (*[]imap.Flag, error) {
	var updatedFlagsBitwise int
	flags := FlagsToBitwise(newFlags)
	negatedFlags := ^flags

	if db.containsFlag(newFlags, imap.FlagDeleted) {
		err := db.Pool.QueryRow(ctx, `
		UPDATE
			messages
		SET
			flags = flags & $1,
			flags_changed_at = $2,
			deleted_at = NULL,
			updated_modseq = nextval('messages_modseq')
		WHERE 
			uid = $3 AND
			mailbox_id = $4
		RETURNING flags`, negatedFlags, time.Now(), messageID, mailboxID).Scan(&updatedFlagsBitwise)
		if err != nil {
			return nil, err
		}
	} else {
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
func (db *Database) buildSearchCriteria(criteria *imap.SearchCriteria, paramPrefix string, paramCounter *int) (string, pgx.NamedArgs) {
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
		}
	}

	// Recursive NOT
	for _, notCriteria := range criteria.Not {
		subCond, subArgs := db.buildSearchCriteria(&notCriteria, paramPrefix, paramCounter)
		for k, v := range subArgs {
			args[k] = v
		}
		conditions = append(conditions, fmt.Sprintf("NOT (%s)", subCond))
	}

	// Recursive OR
	for _, orPair := range criteria.Or {
		leftCond, leftArgs := db.buildSearchCriteria(&orPair[0], paramPrefix, paramCounter)
		rightCond, rightArgs := db.buildSearchCriteria(&orPair[1], paramPrefix, paramCounter)

		maps.Copy(args, leftArgs)
		maps.Copy(args, rightArgs)

		conditions = append(conditions, fmt.Sprintf("(%s OR %s)", leftCond, rightCond))
	}

	finalCondition := "1=1"
	if len(conditions) > 0 {
		finalCondition = strings.Join(conditions, " AND ")
	}

	return finalCondition, args
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

func (db *Database) GetMessagesWithCriteria(ctx context.Context, mailboxID int64, numKind imapserver.NumKind, criteria *imap.SearchCriteria) ([]Message, error) {
	baseQuery := `
	WITH message_seqs AS (
		SELECT uid, ROW_NUMBER() OVER (ORDER BY uid) AS seqnum
		FROM messages
		WHERE mailbox_id = @mailboxID AND expunged_at IS NULL
	)
	SELECT * FROM message_seqs`

	paramCounter := 0
	whereCondition, whereArgs := db.buildSearchCriteria(criteria, "p", &paramCounter)
	whereArgs["mailboxID"] = mailboxID

	query := fmt.Sprintf(" WHERE %s ORDER BY uid", whereCondition)

	// Convert NamedArgs to positional arguments
	positionalArgs := make([]interface{}, 0, len(whereArgs))
	for _, key := range sortedKeys(whereArgs) {
		positionalArgs = append(positionalArgs, whereArgs[key])
	}

	rows, err := db.Pool.Query(ctx, baseQuery+query, positionalArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var message Message
		if err := rows.Scan(&message.UID); err != nil {
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
		SELECT uid, uuid FROM messages WHERE mailbox_id = $1 AND (flags & $2) != 0 AND expunged_at IS NULL
	`, mailboxID, bitwiseFlag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.UID, &msg.UUID); err != nil {
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
				uid, size, uuid, s3_uploaded
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
		if err := rows.Scan(&msg.UID, &msg.Size, &msg.UUID, &msg.S3Uploaded); err != nil {
			return nil, fmt.Errorf("failed to scan message: %v", err)
		}
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error fetching messages: %v", err)
	}

	return messages, nil
}

type S3DeleteCandidate struct {
	UUID      uuid.UUID
	Domain    string
	LocalPart string
}

func (d *Database) ListS3ObjectsToDelete(ctx context.Context, olderThan time.Duration, limit int) ([]S3DeleteCandidate, error) {
	cutoff := time.Now().Add(-olderThan)

	rows, err := d.Pool.Query(ctx, `
		SELECT m.uuid, u.username
		FROM messages m
		JOIN users u ON m.user_id = u.id
		GROUP BY m.uuid, u.username
		HAVING bool_and(m.expunged_at IS NOT NULL AND m.expunged_at < $1)
		LIMIT $2
	`, cutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []S3DeleteCandidate
	for rows.Next() {
		var cand S3DeleteCandidate
		var username string
		if err := rows.Scan(&cand.UUID, &username); err != nil {
			continue
		}
		username = strings.ToLower(strings.TrimSpace(username))
		parts := strings.SplitN(username, "@", 2)
		if len(parts) != 2 {
			continue
		}
		cand.LocalPart = parts[0]
		cand.Domain = parts[1]
		result = append(result, cand)
	}
	return result, nil
}

func (d *Database) FindExistingUUIDs(ctx context.Context, ids []uuid.UUID) ([]uuid.UUID, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := d.Pool.Query(ctx, `SELECT uuid FROM messages WHERE uuid = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			continue // log or ignore individual scan errors
		}
		result = append(result, id)
	}

	return result, nil
}
