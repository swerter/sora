package managesieve

import (
	"io"
	"net"
	"time"
)

// PipeConn is a simple implementation of net.Conn using io.Pipe
type PipeConn struct {
	reader       *io.PipeReader
	writer       *io.PipeWriter
	localAddr    net.Addr
	remoteAddr   net.Addr
	readDeadline time.Time
}

// NewPipeConn creates a new PipeConn
func NewPipeConn() (*PipeConn, *PipeConn) {
	// Create two pipes, one for each direction
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	addr1 := &mockAddr{network: "pipe", address: "addr1"}
	addr2 := &mockAddr{network: "pipe", address: "addr2"}

	// Create two connections, each with its own reader and writer
	conn1 := &PipeConn{
		reader:     r1,
		writer:     w2,
		localAddr:  addr1,
		remoteAddr: addr2,
	}

	conn2 := &PipeConn{
		reader:     r2,
		writer:     w1,
		localAddr:  addr2,
		remoteAddr: addr1,
	}

	return conn1, conn2
}

// Read reads data from the connection
func (c *PipeConn) Read(b []byte) (n int, err error) {
	return c.reader.Read(b)
}

// Write writes data to the connection
func (c *PipeConn) Write(b []byte) (n int, err error) {
	return c.writer.Write(b)
}

// Close closes the connection
func (c *PipeConn) Close() error {
	c.reader.Close()
	return c.writer.Close()
}

// LocalAddr returns the local network address
func (c *PipeConn) LocalAddr() net.Addr {
	return c.localAddr
}

// RemoteAddr returns the remote network address
func (c *PipeConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

// SetDeadline sets the read and write deadlines
func (c *PipeConn) SetDeadline(t time.Time) error {
	c.readDeadline = t
	return nil
}

// SetReadDeadline sets the read deadline
func (c *PipeConn) SetReadDeadline(t time.Time) error {
	c.readDeadline = t
	return nil
}

// SetWriteDeadline sets the write deadline
func (c *PipeConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// mockAddr implements net.Addr for testing
type mockAddr struct {
	network string
	address string
}

func (a *mockAddr) Network() string {
	return a.network
}

func (a *mockAddr) String() string {
	return a.address
}
