package managesieve

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/foxcpp/go-sieve"
	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/server"
)

type ManageSieveSession struct {
	server.Session
	mutex         sync.Mutex
	server        *ManageSieveServer
	conn          *net.Conn          // Connection to the client
	*server.User                     // User associated with the session
	authenticated bool               // Flag to indicate if the user has been authenticated
	ctx           context.Context    // Context for this session
	cancel        context.CancelFunc // Function to cancel the session's context

	reader *bufio.Reader
	writer *bufio.Writer
}

func (s *ManageSieveSession) handleConnection() {
	defer s.Close()

	s.sendResponse("+OK ManageSieve ready\r\n")

	for {
		line, err := s.reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				s.Log("[MANAGESIEVE] client dropped connection")
			} else {
				s.Log("read error: %v", err)
			}
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		command := strings.ToUpper(parts[0])

		switch command {
		case "LOGIN":
			if len(parts) < 3 {
				s.sendResponse("-ERR Syntax: LOGIN username password\r\n")
				continue
			}
			username := parts[1]
			password := parts[2]

			address, err := server.NewAddress(username)
			if err != nil {
				s.Log("error: %v", err)
				s.sendResponse("-ERR Invalid username\r\n")
				continue
			}

			userID, err := s.server.db.Authenticate(s.ctx, address.FullAddress(), password)
			if err != nil {
				s.sendResponse("-ERR Authentication failed\r\n")
				continue
			}
			s.Log("[MANAGESIEVE] user %s authenticated", address.FullAddress())
			s.authenticated = true
			s.User = server.NewUser(address, userID)
			s.sendResponse("+OK Authenticated\r\n")

		case "LISTSCRIPTS":
			if !s.authenticated {
				s.sendResponse("-ERR Not authenticated\r\n")
				continue
			}
			s.handleListScripts()

		case "GETSCRIPT":
			if !s.authenticated {
				s.sendResponse("-ERR Not authenticated\r\n")
				continue
			}
			if len(parts) < 2 {
				s.sendResponse("-ERR Syntax: GETSCRIPT scriptName\r\n")
				continue
			}
			scriptName := parts[1]
			s.handleGetScript(scriptName)

		case "PUTSCRIPT":
			if !s.authenticated {
				s.sendResponse("-ERR Not authenticated\r\n")
				continue
			}
			if len(parts) < 3 {
				s.sendResponse("-ERR Syntax: PUTSCRIPT scriptName scriptContent\r\n")
				continue
			}
			scriptName := parts[1]
			scriptContent := parts[2]
			s.handlePutScript(scriptName, scriptContent)

		case "SETACTIVE":
			if !s.authenticated {
				s.sendResponse("-ERR Not authenticated\r\n")
				continue
			}
			if len(parts) < 2 {
				s.sendResponse("-ERR Syntax: SETACTIVE scriptName\r\n")
				continue
			}
			scriptName := parts[1]
			s.handleSetActive(scriptName)

		case "DELETESCRIPT":
			if !s.authenticated {
				s.sendResponse("-ERR Not authenticated\r\n")
				continue
			}
			if len(parts) < 2 {
				s.sendResponse("-ERR Syntax: DELETESCRIPT scriptName\r\n")
				continue
			}
			scriptName := parts[1]
			s.handleDeleteScript(scriptName)

		case "NOOP":
			s.sendResponse("+OK\r\n")

		case "LOGOUT":
			s.sendResponse("+OK Goodbye\r\n")
			s.Close()
			return

		default:
			s.sendResponse("-ERR Unknown command\r\n")
		}
	}
}

func (s *ManageSieveSession) sendResponse(response string) {
	s.writer.WriteString(response)
	s.writer.Flush()
}

func (s *ManageSieveSession) handleListScripts() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	scripts, err := s.server.db.GetUserScripts(s.ctx, s.UserID())
	if err != nil {
		s.sendResponse("-ERR Internal server error\r\n")
		return
	}

	if len(scripts) == 0 {
		s.sendResponse("+OK\r\n")
		return
	}

	response := "+OK "
	for _, script := range scripts {
		response += fmt.Sprintf("%s ", script.Name)
	}
	response += "\r\n"
	s.sendResponse(response)
}

func (s *ManageSieveSession) handleGetScript(name string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	script, err := s.server.db.GetScriptByName(s.ctx, name, s.UserID())
	if err != nil {
		s.sendResponse("-ERR No such script\r\n")
		return
	}
	response := fmt.Sprintf("+OK %d\r\n%s\r\n", len(script.Script), script.Script)
	s.sendResponse(response)
}

func (s *ManageSieveSession) handlePutScript(name, content string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	scriptReader := strings.NewReader(content)
	options := sieve.DefaultOptions()
	_, err := sieve.Load(scriptReader, options)
	if err != nil {
		s.sendResponse(fmt.Sprintf("-ERR Script validation failed: %v\r\n", err))
		return
	}

	script, err := s.server.db.GetScriptByName(s.ctx, name, s.UserID())
	if err != nil {
		if err != consts.ErrDBNotFound {
			s.sendResponse("-ERR Internal server error\r\n")
			return
		}
	}
	if script != nil {
		_, err := s.server.db.UpdateScript(s.ctx, script.ID, s.UserID(), name, content)
		if err != nil {
			s.sendResponse("-ERR Internal server error\r\n")
			return
		}
		s.sendResponse("+OK Script updated\r\n")
		return
	}

	_, err = s.server.db.CreateScript(s.ctx, s.UserID(), name, content)
	if err != nil {
		s.sendResponse("-ERR Internal server error\r\n")
		return
	}
	s.sendResponse("+OK Script stored\r\n")
}

func (s *ManageSieveSession) handleSetActive(name string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	script, err := s.server.db.GetScriptByName(s.ctx, name, s.UserID())
	if err != nil {
		if err == consts.ErrDBNotFound {
			s.sendResponse("-ERR No such script\r\n")
			return
		}
		s.sendResponse("-ERR Internal server error\r\n")
		return
	}

	// Validate the script before activating it
	scriptReader := strings.NewReader(script.Script)
	options := sieve.DefaultOptions()
	_, err = sieve.Load(scriptReader, options)
	if err != nil {
		s.sendResponse(fmt.Sprintf("-ERR Script validation failed: %v\r\n", err))
		return
	}

	err = s.server.db.SetScriptActive(s.ctx, script.ID, s.UserID(), true)
	if err != nil {
		s.sendResponse("-ERR Internal server error\r\n")
		return
	}

	s.sendResponse("+OK Script activated\r\n")
}

func (s *ManageSieveSession) handleDeleteScript(name string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	script, err := s.server.db.GetScriptByName(s.ctx, name, s.UserID())
	if err != nil {
		if err != consts.ErrDBNotFound {
			s.sendResponse("-ERR No such script\r\n")
			return
		}
		s.sendResponse("-ERR Internal server error\r\n")
		return
	}

	err = s.server.db.DeleteScript(s.ctx, script.ID, s.UserID())
	if err != nil {
		s.sendResponse("-ERR Internal server error\r\n")
		return
	}
	s.sendResponse("+OK Script deleted\r\n")
}

func (s *ManageSieveSession) Close() error {
	(*s.conn).Close()
	if s.User != nil {
		s.Log("closed")
		s.User = nil
		s.Id = ""
		s.authenticated = false
		if s.cancel != nil {
			s.cancel()
		}
	}
	return nil
}
