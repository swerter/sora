package imap

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/migadu/sora/db"
)

func (s *IMAPSession) Fetch(w *imapserver.FetchWriter, seqSet imap.NumSet, options *imap.FetchOptions) error {
	ctx := context.Background()

	seqSet = s.mailbox.decodeNumSet(seqSet)

	messages, err := s.server.db.GetMessagesBySeqSet(ctx, s.mailbox.ID, seqSet)
	if err != nil {
		return s.internalError("failed to retrieve messages: %v", err)
	}

	for _, msg := range messages {
		if err := s.fetchMessage(w, &msg, options); err != nil {
			return err
		}
	}

	return nil
}

func (s *IMAPSession) fetchMessage(w *imapserver.FetchWriter, msg *db.Message, options *imap.FetchOptions) error {
	m := w.CreateMessage(s.mailbox.sessionTracker.EncodeSeqNum(msg.Seq))
	if m == nil {
		return s.internalError("failed to begin message for UID %d", msg.UID)
	}

	if err := s.writeBasicMessageData(m, msg, options); err != nil {
		return err
	}

	if !msg.IsUploaded {
		log.Printf("UID %d is not yet uploaded, returning flags only", msg.UID)
		return m.Close() // No body/envelope, but valid message record
	}

	if options.Envelope {
		if err := s.writeEnvelope(m, msg.UID, msg.MailboxID); err != nil {
			return err
		}
	}

	if options.BodyStructure != nil {
		if err := s.writeBodyStructure(m, &msg.BodyStructure); err != nil {
			return err
		}
	}

	if len(options.BodySection) > 0 || len(options.BinarySection) > 0 || len(options.BinarySectionSize) > 0 {
		var bodyData []byte
		var err error
		bodyData, err = s.getMessageBody(msg)
		if err != nil {
			log.Printf("Skipping UID %d: %v", msg.UID, err)
			return nil // fallback to FLAGS-only
		}

		if len(options.BodySection) > 0 {
			if err := s.handleBodySections(m, bodyData, options, msg); err != nil {
				return err
			}
		}

		if len(options.BinarySection) > 0 {
			if err := s.handleBinarySections(m, bodyData, options); err != nil {
				return err
			}
		}

		if len(options.BinarySectionSize) > 0 {
			if err := s.handleBinarySectionSize(m, bodyData, options); err != nil {
				return err
			}
		}
	}

	// TODO: Fetch ModSeq (if CONDSTORE is supported)

	if err := m.Close(); err != nil {
		return fmt.Errorf("failed to end message for UID %d: %v", msg.UID, err)
	}

	return nil
}

// Fetch helper to write basic message data (FLAGS, UID, INTERNALDATE, RFC822.SIZE)
func (s *IMAPSession) writeBasicMessageData(m *imapserver.FetchResponseWriter, msg *db.Message, options *imap.FetchOptions) error {
	if options.Flags {
		m.WriteFlags(db.BitwiseToFlags(msg.BitwiseFlags))
	}
	if options.UID {
		m.WriteUID(msg.UID)
	}
	if options.InternalDate {
		m.WriteInternalDate(msg.InternalDate.UTC())
	}
	if options.RFC822Size {
		m.WriteRFC822Size(int64(msg.Size))
	}
	return nil
}

// Fetch helper to write the envelope for a message
func (s *IMAPSession) writeEnvelope(m *imapserver.FetchResponseWriter, messageUID imap.UID, mailboxID int64) error {
	ctx := context.Background()
	envelope, err := s.server.db.GetMessageEnvelope(ctx, messageUID, mailboxID)
	if err != nil {
		return s.internalError("failed to retrieve envelope for message UID %d: %v", messageUID, err)
	}
	m.WriteEnvelope(envelope)
	return nil
}

// Fetch helper to write the body structure for a message
func (s *IMAPSession) writeBodyStructure(m *imapserver.FetchResponseWriter, bodyStructure *imap.BodyStructure) error {
	m.WriteBodyStructure(*bodyStructure) // Use the already deserialized BodyStructure
	return nil
}

// Fetch helper to handle BINARY sections for a message
func (s *IMAPSession) handleBinarySections(w *imapserver.FetchResponseWriter, bodyData []byte, options *imap.FetchOptions) error {
	for _, section := range options.BinarySection {
		buf := imapserver.ExtractBinarySection(bytes.NewReader(bodyData), section)
		wc := w.WriteBinarySection(section, int64(len(buf)))
		_, writeErr := wc.Write(buf)
		closeErr := wc.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

// Fetch helper to handle BINARY.SIZE sections for a message
func (s *IMAPSession) handleBinarySectionSize(w *imapserver.FetchResponseWriter, bodyData []byte, options *imap.FetchOptions) error {
	for _, section := range options.BinarySectionSize {
		n := imapserver.ExtractBinarySectionSize(bytes.NewReader(bodyData), section)
		w.WriteBinarySectionSize(section, n)
	}
	return nil
}

// Fetch helper to handle BODY sections for a message
func (s *IMAPSession) handleBodySections(w *imapserver.FetchResponseWriter, bodyData []byte, options *imap.FetchOptions, msg *db.Message) error {
	for _, section := range options.BodySection {
		buf := imapserver.ExtractBodySection(bytes.NewReader(bodyData), section)
		wc := w.WriteBodySection(section, int64(len(buf)))
		_, writeErr := wc.Write(buf)
		closeErr := wc.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
		// Set \Seen flag when BODY[] is requested (empty section means full body)
		// According to IMAP RFC, fetching the full message body should set \Seen flag
		// unless the PEEK option is specified (BODY.PEEK[])
		if len(section.Part) == 0 && section.Specifier == "" {
			ctx := context.Background()
			// We need to get the message UID from the parent function
			// The msg parameter is available in the parent fetchMessage function
			_, err := s.server.db.AddMessageFlags(ctx, msg.UID, s.mailbox.ID, []imap.Flag{imap.FlagSeen})
			if err != nil {
				log.Printf("Failed to set \\Seen flag for message UID %d: %v", msg.UID, err)
				// Continue despite error - fetching the body is more important than setting the flag
			}
		}
	}
	return nil
}

func (s *IMAPSession) getMessageBody(msg *db.Message) ([]byte, error) {
	if msg.IsUploaded {
		// Try cache first
		data, err := s.server.cache.Get(msg.ContentHash)
		if err == nil && data != nil {
			log.Printf("[CACHE] Hit for UID %d", msg.UID)
			return data, nil
		}

		// Fallback to S3
		log.Printf("[CACHE] Miss. Fetching UID %d from S3 (%s)", msg.UID, msg.ContentHash)
		reader, err := s.server.s3.Get(msg.ContentHash)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve message UID %d from S3: %v", msg.UID, err)
		}
		defer reader.Close()
		data, err = io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		_ = s.server.cache.Put(msg.ContentHash, data)
		return data, nil
	}

	// If not uploaded to S3, fetch from local disk
	log.Printf("Fetching not yet uploaded message UID %d from disk", msg.UID)
	data, err := s.server.uploader.GetLocalFile(msg.ContentHash)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve message UID %d from disk: %v", msg.UID, err)
	}
	if data == nil {
		return nil, fmt.Errorf("message UID %d not found on disk", msg.UID)
	}
	return data, nil
}
