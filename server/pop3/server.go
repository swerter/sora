package pop3

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/google/uuid"
)

const MAX_ERRORS_ALLOWED = 3
const ERROR_DELAY = 3 * time.Second
const IDLE_TIMEOUT = 5 * time.Minute // Maximum duration of inactivity before the connection is closed

type POP3Server struct {
	addr      string
	hostname  string
	db        DBer               // Use our DBer interface
	s3        S3StorageInterface // Use our interface
	appCtx    context.Context
	uploader  UploadWorkerInterface // Use our interface
	cache     CacheInterface        // Use our interface
	tlsConfig *tls.Config           // TLS configuration
}

func New(appCtx context.Context, hostname, popAddr string, storage S3StorageInterface, database DBer, uploadWorker UploadWorkerInterface, cache CacheInterface, insecureAuth bool, debug bool, tlsCertFile, tlsKeyFile string, insecureSkipVerify ...bool) (*POP3Server, error) {
	server := &POP3Server{
		hostname: hostname,
		addr:     popAddr,
		db:       database,
		s3:       storage,
		appCtx:   appCtx,
		uploader: uploadWorker,
		cache:    cache,
	}

	// Setup TLS if certificate and key files are provided
	if tlsCertFile != "" && tlsKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsCertFile, tlsKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
		}
		server.tlsConfig = &tls.Config{
			Certificates:             []tls.Certificate{cert},
			MinVersion:               tls.VersionTLS12, // Allow older TLS versions for better compatibility
			ClientAuth:               tls.NoClientCert,
			ServerName:               hostname,
			PreferServerCipherSuites: true, // Prefer server cipher suites over client cipher suites
		}

		// Set InsecureSkipVerify if requested (for self-signed certificates)
		if len(insecureSkipVerify) > 0 && insecureSkipVerify[0] {
			server.tlsConfig.InsecureSkipVerify = true
			log.Printf("WARNING: TLS certificate verification disabled for POP3 server")
		}
	}

	return server, nil
}

func (s *POP3Server) Start(errChan chan error) {
	var listener net.Listener
	var err error

	if s.tlsConfig != nil {
		// Start TLS listener if TLS is configured
		listener, err = tls.Listen("tcp", s.addr, s.tlsConfig)
		if err != nil {
			errChan <- fmt.Errorf("failed to create TLS listener: %w", err)
			return
		}
		log.Printf("POP3 listening with TLS on %s", s.addr)
	} else {
		// Start regular TCP listener if no TLS
		listener, err = net.Listen("tcp", s.addr)
		if err != nil {
			errChan <- fmt.Errorf("failed to create listener: %w", err)
			return
		}
		log.Printf("POP3 listening on %s", s.addr)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			errChan <- err
			return
		}

		sessionCtx, sessionCancel := context.WithCancel(s.appCtx)

		s := &POP3Session{
			server:  s,
			conn:    &conn,
			deleted: make(map[int]bool),
			ctx:     sessionCtx,
			cancel:  sessionCancel,
		}

		s.RemoteIP = (*s.conn).RemoteAddr().String()
		s.Protocol = "POP3"
		s.Id = uuid.New().String()
		s.HostName = s.server.hostname

		go s.handleConnection()
	}
}

func (s *POP3Server) Close() {
	s.db.Close()
}
