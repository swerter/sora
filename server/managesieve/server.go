package managesieve

import (
	"bufio"
	"log"
	"net"

	"github.com/google/uuid"
	"github.com/migadu/sora/db"
)

type ManageSieveServer struct {
	addr     string
	hostname string
	db       *db.Database
}

func New(hostname, popAddr string, database *db.Database, insecureAuth bool, debug bool) (*ManageSieveServer, error) {
	return &ManageSieveServer{
		hostname: hostname,
		addr:     popAddr,
		db:       database,
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

		s := &ManageSieveSession{
			server: s,
			conn:   &conn,
			reader: bufio.NewReader(conn),
			writer: bufio.NewWriter(conn),
		}

		s.RemoteIP = (*s.conn).RemoteAddr().String()
		s.Protocol = "ManageSieve"
		s.Id = uuid.New().String()
		s.HostName = s.server.hostname

		go s.handleConnection()
	}
}

func (s *ManageSieveServer) Close() {
	s.db.Close()
}
