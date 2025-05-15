package managesieve

import (
	"bufio"
	"context"
	"log"
	"net"

	"github.com/google/uuid"
	"github.com/migadu/sora/db"
)

type ManageSieveServer struct {
	addr     string
	hostname string
	db       *db.Database
	appCtx   context.Context // Store the application's parent context
}

func New(appCtx context.Context, hostname, addr string, database *db.Database, insecureAuth bool, debug bool) (*ManageSieveServer, error) {
	return &ManageSieveServer{
		hostname: hostname,
		addr:     addr,
		db:       database,
		appCtx:   appCtx,
	}, nil
}

func (s *ManageSieveServer) Start(errChan chan error) {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		errChan <- err
		return
	}
	defer ln.Close()

	log.Printf("ManageSieve listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
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
