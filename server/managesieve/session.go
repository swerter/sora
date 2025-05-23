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
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/server"
)

type ManageSieveSession struct {
	server.Session
	mutex         sync.Mutex
	server        *ManageSieveServer
	conn          *net.Conn          // Connection to the client
	*server.User                     // User associated with the session
	authenticated bool               // Flag to indicate if the user has been authenticated
	scripts       []db.Message       // List of messages in the mailbox as returned by the LIST command
	errorsCount   int                // Number of errors encountered during the session
	ctx           context.Context    // Context for this session
	cancel        context.CancelFunc // Function to cancel the session's context

	reader *bufio.Reader
	writer *bufio.Writer
}

// Context returns the session's context.
func (s *ManageSieveSession) Context() context.Context {
	return s.ctx
}

func (s *ManageSieveSession) handleConnection() {
	defer s.Close()

	// Send initial greeting
	s.sendResponse("+OK ManageSieve ready\r\n")

	for {
		line, err := s.reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// Client closed connection without LOGOUT
				s.Log("client dropped connection")
			} else {
				s.Log("read error: %v", err)
			}
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse command
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

			// While POP3 accepts any kind of username, we will only accept email addresses
			address, err := server.NewAddress(username)
			if err != nil {
				s.Log("error: %v", err)
				s.sendResponse("-ERR Invalid username\r\n")
				continue
			}

			ctx := s.Context() // Use session's context
			userID, err := s.server.db.GetUserIDByAddress(ctx, address.FullAddress())
			if err != nil {
				if err == consts.ErrUserNotFound {
					s.sendResponse("-ERR Unknown user\r\n")
					continue
				}
				s.Log("USER error: %v", err)
				s.sendResponse("-ERR Internal server error\r\n")
				continue
			}

			err = s.server.db.Authenticate(ctx, userID, password)
			if err != nil {
				s.sendResponse("-ERR Authentication failed\r\n")
				continue
			}
			s.Log("authenticated")
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

	scripts, err := s.server.db.GetUserScripts(s.Context(), s.UserID())
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

	script, err := s.server.db.GetScriptByName(s.Context(), name, s.UserID())
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

	// Validate the script first
	scriptReader := strings.NewReader(content)
	options := sieve.DefaultOptions()
	_, err := sieve.Load(scriptReader, options)
	if err != nil {
		s.sendResponse(fmt.Sprintf("-ERR Script validation failed: %v\r\n", err))
		return
	}

	// Check if the script exists
	script, err := s.server.db.GetScriptByName(s.Context(), name, s.UserID())
	if err != nil {
		if err != consts.ErrDBNotFound {
			s.sendResponse("-ERR Internal server error\r\n")
			return
		}
	}
	if script != nil {
		// Script already exists, update it
		_, err := s.server.db.UpdateScript(s.Context(), script.ID, s.UserID(), name, content)
		if err != nil {
			s.sendResponse("-ERR Internal server error\r\n")
			return
		}
		s.sendResponse("+OK Script updated\r\n")
		return
	}

	_, err = s.server.db.CreateScript(s.Context(), s.UserID(), name, content)
	if err != nil {
		s.sendResponse("-ERR Internal server error\r\n")
		return
	}
	s.sendResponse("+OK Script stored\r\n")
}

func (s *ManageSieveSession) handleSetActive(name string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if the script exists
	script, err := s.server.db.GetScriptByName(s.Context(), name, s.UserID())
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

	// Set the script as active
	err = s.server.db.SetScriptActive(s.Context(), script.ID, s.UserID(), true)
	if err != nil {
		s.sendResponse("-ERR Internal server error\r\n")
		return
	}

	s.sendResponse("+OK Script activated\r\n")
}

func (s *ManageSieveSession) handleDeleteScript(name string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if the script exists
	script, err := s.server.db.GetScriptByName(s.Context(), name, s.UserID())
	if err != nil {
		if err != consts.ErrDBNotFound {
			s.sendResponse("-ERR No such script\r\n")
			return
		}
		s.sendResponse("-ERR Internal server error\r\n")
		return
	}

	err = s.server.db.DeleteScript(s.Context(), script.ID, s.UserID())
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
			s.cancel() // Cancel the session's context
		}
	}
	return nil
}
