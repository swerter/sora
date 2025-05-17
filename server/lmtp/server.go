package lmtp

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/emersion/go-smtp"
	"github.com/google/uuid"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/server/sieveengine"
	"github.com/migadu/sora/server/uploader"
	"github.com/migadu/sora/storage"
)

type LMTPServerBackend struct {
	addr          string
	hostname      string
	db            *db.Database
	s3            *storage.S3Storage
	appCtx        context.Context
	uploader      *uploader.UploadWorker
	sieve         sieveengine.Executor
	server        *smtp.Server
	externalRelay string
	tlsConfig     *tls.Config // TLS configuration
}

func New(appCtx context.Context, hostname, addr string, s3 *storage.S3Storage, db *db.Database, uploadWorker *uploader.UploadWorker, debug bool, externalRelay string, tlsCertFile, tlsKeyFile string) (*LMTPServerBackend, error) {
	backend := &LMTPServerBackend{
		addr:          addr,
		appCtx:        appCtx,
		hostname:      hostname,
		db:            db,
		s3:            s3,
		uploader:      uploadWorker,
		externalRelay: externalRelay,
	}

	// Setup TLS if certificate and key files are provided
	if tlsCertFile != "" && tlsKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsCertFile, tlsKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
		}
		backend.tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	s := smtp.NewServer(backend)
	s.Addr = addr
	s.Domain = hostname
	s.AllowInsecureAuth = true
	s.LMTP = true
	s.TLSConfig = backend.tlsConfig

	// Save the server instance in the backend
	backend.server = s

	// This server supports only TCP connections
	s.Network = "tcp"

	var debugWriter io.Writer
	if debug {
		debugWriter = os.Stdout
		s.Debug = debugWriter
	}

	return backend, nil
}

func (b *LMTPServerBackend) NewSession(c *smtp.Conn) (smtp.Session, error) {
	// Create a new cancellable context for this session, derived from the server's application context.
	// This ensures the session context is cancelled when the main application context is cancelled.
	sessionCtx, sessionCancel := context.WithCancel(b.appCtx)
	s := &LMTPSession{
		backend: b,
		conn:    c,
		ctx:     sessionCtx,
		cancel:  sessionCancel, // Store the cancel function
	}
	s.RemoteIP = c.Conn().RemoteAddr().String()
	s.Id = uuid.New().String()
	s.HostName = b.hostname
	s.Protocol = "LMTP"

	s.Log("New session")
	return s, nil
}

func (b *LMTPServerBackend) Start(errChan chan error) {
	log.Printf("LMTP listening on %s", b.server.Addr)
	if err := b.server.ListenAndServe(); err != nil {
		// Check if the error is due to context cancellation (graceful shutdown)
		// b.appCtx.Err() will be non-nil if the context was canceled.
		if b.appCtx.Err() == nil {
			errChan <- fmt.Errorf("LMTP server error: %w", err)
		}
	}
}

// Close stops the LMTP server.
func (b *LMTPServerBackend) Close() error {
	if b.server != nil {
		return b.server.Close()
	}
	return nil
}
