package pop3

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/google/uuid"
)

const MAX_ERRORS_ALLOWED = 3
const ERROR_DELAY = 3 * time.Second
const IDLE_TIMEOUT = 5 * time.Minute // Maximum duration of inactivity before the connection is closed

type POP3Server struct {
	addr     string
	hostname string
	db       DBer               // Use our DBer interface
	s3       S3StorageInterface // Use our interface
	appCtx   context.Context
	uploader UploadWorkerInterface // Use our interface
	cache    CacheInterface        // Use our interface
}

func New(appCtx context.Context, hostname, popAddr string, storage S3StorageInterface, database DBer, uploadWorker UploadWorkerInterface, cache CacheInterface, insecureAuth bool, debug bool) (*POP3Server, error) {
	return &POP3Server{
		hostname: hostname,
		addr:     popAddr,
		db:       database,
		s3:       storage,
		appCtx:   appCtx,
		uploader: uploadWorker,
		cache:    cache,
	}, nil
}

func (s *POP3Server) Start(errChan chan error) {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		errChan <- err
		return
	}
	defer ln.Close()

	log.Printf("POP3 listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
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
