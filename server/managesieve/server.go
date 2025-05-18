package managesieve

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"

	"github.com/google/uuid"
)

type ManageSieveServer struct {
	addr      string
	hostname  string
	db        DBer
	appCtx    context.Context // Store the application's parent context
	tlsConfig *tls.Config     // TLS configuration
}

func New(appCtx context.Context, hostname, addr string, database DBer, insecureAuth bool, debug bool, tlsCertFile, tlsKeyFile string, insecureSkipVerify ...bool) (*ManageSieveServer, error) {
	server := &ManageSieveServer{
		hostname: hostname,
		addr:     addr,
		db:       database,
		appCtx:   appCtx,
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
			log.Printf("WARNING: TLS certificate verification disabled for ManageSieve server")
		}
	}

	return server, nil
}

func (s *ManageSieveServer) Start(errChan chan error) {
	var listener net.Listener
	var err error

	if s.tlsConfig != nil {
		// Start TLS listener if TLS is configured
		listener, err = tls.Listen("tcp", s.addr, s.tlsConfig)
		if err != nil {
			errChan <- fmt.Errorf("failed to create TLS listener: %w", err)
			return
		}
		log.Printf("ManageSieve listening with TLS on %s", s.addr)
	} else {
		// Start regular TCP listener if no TLS
		listener, err = net.Listen("tcp", s.addr)
		if err != nil {
			errChan <- fmt.Errorf("failed to create listener: %w", err)
			return
		}
		log.Printf("ManageSieve listening on %s", s.addr)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			errChan <- err
			return
		}

		// Create a new cancellable context for this session
		sessionCtx, sessionCancel := context.WithCancel(s.appCtx)

		session := &ManageSieveSession{
			server: s,
			conn:   &conn,
			reader: bufio.NewReader(conn),
			writer: bufio.NewWriter(conn),
			ctx:    sessionCtx,
			cancel: sessionCancel,
		}

		session.RemoteIP = (*session.conn).RemoteAddr().String()
		session.Protocol = "ManageSieve"
		session.Id = uuid.New().String()
		session.HostName = session.server.hostname

		go session.handleConnection()
	}
}

func (s *ManageSieveServer) Close() {
	// The shared database connection pool is closed by main.go's defer.
	// If ManageSieveServer had its own specific resources to close (e.g., a listener, which it doesn't),
	// they would be closed here. For now, this can be a no-op or just log.
	log.Println("[ManageSieve] Server Close method called.")
}
