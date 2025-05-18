package imap

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/google/uuid"
	"github.com/migadu/sora/cache"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/server/uploader"
	"github.com/migadu/sora/storage"
)

type IMAPServer struct {
	addr      string
	db        *db.Database
	hostname  string
	s3        *storage.S3Storage
	server    *imapserver.Server
	uploader  *uploader.UploadWorker
	cache     *cache.Cache
	appCtx    context.Context // Store the application's parent context
	caps      imap.CapSet
	tlsConfig *tls.Config // TLS configuration
}

func New(appCtx context.Context, hostname, imapAddr string, storage *storage.S3Storage, database *db.Database, uploadWorker *uploader.UploadWorker, cache *cache.Cache, insecureAuth bool, debug bool, tlsCertFile, tlsKeyFile string, insecureSkipVerify ...bool) (*IMAPServer, error) {
	s := &IMAPServer{
		hostname: hostname,
		appCtx:   appCtx, // Store the passed-in application context
		addr:     imapAddr,
		db:       database,
		s3:       storage,
		uploader: uploadWorker,
		cache:    cache,
		caps: imap.CapSet{
			imap.CapIMAP4rev1: struct{}{},
			// imap.CapIMAP4rev2:   struct{}{},
			// imap.CapLiteralPlus: struct{}{},
			// imap.CapSASLIR:      struct{}{},
			// imap.CapAuthPlain:   struct{}{},
			imap.CapMove: struct{}{},
			imap.CapIdle: struct{}{},
			// imap.CapID:          struct{}{},
		},
	}

	// Setup TLS if certificate and key files are provided
	if tlsCertFile != "" && tlsKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsCertFile, tlsKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
		}
		s.tlsConfig = &tls.Config{
			Certificates:             []tls.Certificate{cert},
			MinVersion:               tls.VersionTLS12, // Allow older TLS versions for better compatibility
			ClientAuth:               tls.NoClientCert,
			ServerName:               hostname,
			PreferServerCipherSuites: true, // Prefer server cipher suites over client cipher suites
		}

		// Set InsecureSkipVerify if requested (for self-signed certificates)
		if len(insecureSkipVerify) > 0 && insecureSkipVerify[0] {
			s.tlsConfig.InsecureSkipVerify = true
			log.Printf("WARNING: TLS certificate verification disabled for IMAP server")
		}
	}

	var debugWriter io.Writer
	if debug {
		debugWriter = os.Stdout
	}

	s.server = imapserver.New(&imapserver.Options{
		NewSession:   s.newSession,
		Logger:       log.Default(),
		InsecureAuth: insecureAuth,
		DebugWriter:  debugWriter,
		Caps:         s.caps,
		TLSConfig:    s.tlsConfig,
	})

	return s, nil
}

func (s *IMAPServer) newSession(conn *imapserver.Conn) (imapserver.Session, *imapserver.GreetingData, error) {
	// Create a new cancellable context for this session, derived from the server's application context.
	// This ensures the session context is cancelled when the main application context is cancelled.
	sessionCtx, sessionCancel := context.WithCancel(s.appCtx)

	session := &IMAPSession{
		server: s,
		conn:   conn,
		ctx:    sessionCtx,
		cancel: sessionCancel,
	}

	session.RemoteIP = conn.NetConn().RemoteAddr().String()
	session.Protocol = "IMAP"
	session.Id = uuid.New().String()
	session.HostName = s.hostname

	greeting := &imapserver.GreetingData{
		PreAuth: false,
	}

	return session, greeting, nil
}

func (s *IMAPServer) Serve(imapAddr string) error {
	var listener net.Listener
	var err error

	if s.tlsConfig != nil {
		// Use TLS listener if TLS is configured
		listener, err = tls.Listen("tcp", imapAddr, s.tlsConfig)
		if err != nil {
			return fmt.Errorf("failed to create TLS listener: %w", err)
		}
		log.Printf("IMAP listening with TLS on %s", imapAddr)
	} else {
		// Use regular TCP listener if no TLS
		listener, err = net.Listen("tcp", imapAddr)
		if err != nil {
			return fmt.Errorf("failed to create listener: %w", err)
		}
		log.Printf("IMAP listening on %s", imapAddr)
	}
	defer listener.Close()

	return s.server.Serve(listener)
}

func (s *IMAPServer) Close() {
	log.Println("[IMAP] Closing IMAP server...")
	if s.server != nil {
		// This will close the listener and cause s.server.Serve(listener) to return.
		// It will also start closing active client connections.
		s.server.Close()
	}
}
