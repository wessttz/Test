package client

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)

type Pool struct {
	addr    string
	mu      sync.Mutex
	conns   []*conn
	maxSize int
}

type conn struct {
	raw    net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewPool(addr string, maxSize int) *Pool {
	if maxSize <= 0 {
		maxSize = 8
	}
	return &Pool{addr: addr, maxSize: maxSize}
}

func (p *Pool) get() (*conn, error) {
	p.mu.Lock()
	if len(p.conns) > 0 {
		c := p.conns[len(p.conns)-1]
		p.conns = p.conns[:len(p.conns)-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	return p.dial()
}

func (p *Pool) put(c *conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.conns) < p.maxSize {
		p.conns = append(p.conns, c)
	} else {
		c.raw.Close()
	}
}

func (p *Pool) dial() (*conn, error) {
	raw, err := net.Dial("tcp", p.addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", p.addr, err)
	}

	c := &conn{
		raw:    raw,
		reader: bufio.NewReader(raw),
		writer: bufio.NewWriter(raw),
	}

	welcome, err := c.reader.ReadString('\n')
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("failed to read welcome: %w", err)
	}
	if !strings.HasPrefix(welcome, "LURUS") {
		raw.Close()
		return nil, fmt.Errorf("unexpected welcome: %s", welcome)
	}

	return c, nil
}

func (p *Pool) Send(command string) (string, error) {
	c, err := p.get()
	if err != nil {
		return "", err
	}

	_, err = c.writer.WriteString(command + "\n")
	if err != nil {
		c.raw.Close()
		return "", fmt.Errorf("write error: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		c.raw.Close()
		return "", fmt.Errorf("flush error: %w", err)
	}

	var sb strings.Builder
	for {
		line, err := c.reader.ReadString('\n')
		if err != nil {
			c.raw.Close()
			return "", fmt.Errorf("read error: %w", err)
		}
		sb.WriteString(line)
		if !strings.HasSuffix(strings.TrimRight(line, "\n"), "|") {
			break
		}
	}

	p.put(c)
	return strings.TrimRight(sb.String(), "\n"), nil
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.raw.Close()
	}
	p.conns = nil
}

type Client struct {
	pool *Pool
}

func Connect(addr string) *Client {
	return &Client{pool: NewPool(addr, 8)}
}

func (c *Client) Close() { c.pool.Close() }

func (c *Client) Send(cmd string) (string, error) { return c.pool.Send(cmd) }

func (c *Client) Forge(table string, colDefs string) (string, error) {
	return c.Send(fmt.Sprintf("FORGE %s (%s)", table, colDefs))
}

func (c *Client) Push(table string, values string) (string, error) {
	return c.Send(fmt.Sprintf("PUSH %s (%s)", table, values))
}

func (c *Client) Upsert(table string, keyCol string, values string) (string, error) {
	return c.Send(fmt.Sprintf("UPSERT %s KEY %s (%s)", table, keyCol, values))
}

func (c *Client) Pull(table string) (string, error) {
	return c.Send(fmt.Sprintf("PULL %s", table))
}

func (c *Client) PullWhere(table, col, val string) (string, error) {
	return c.Send(fmt.Sprintf("PULL %s WHERE %s = %s", table, col, val))
}

func (c *Client) PullLimit(table string, limit int) (string, error) {
	return c.Send(fmt.Sprintf("PULL %s LIMIT %d", table, limit))
}

func (c *Client) PullWhereLimit(table, col, val string, limit int) (string, error) {
	return c.Send(fmt.Sprintf("PULL %s WHERE %s = %s LIMIT %d", table, col, val, limit))
}

func (c *Client) Count(table string) (string, error) {
	return c.Send(fmt.Sprintf("COUNT %s", table))
}

func (c *Client) CountWhere(table, col, val string) (string, error) {
	return c.Send(fmt.Sprintf("COUNT %s WHERE %s = %s", table, col, val))
}

func (c *Client) Burn(table, col, val string) (string, error) {
	return c.Send(fmt.Sprintf("BURN %s WHERE %s = %s", table, col, val))
}

func (c *Client) Reforge(table, setCol, setVal, whereCol, whereVal string) (string, error) {
	return c.Send(fmt.Sprintf("REFORGE %s SET %s = %s WHERE %s = %s", table, setCol, setVal, whereCol, whereVal))
}

func (c *Client) Drop(table string) (string, error) {
	return c.Send(fmt.Sprintf("DROP %s", table))
}

func (c *Client) Tables() (string, error) { return c.Send("TABLES") }

func (c *Client) Schema(table string) (string, error) {
	return c.Send(fmt.Sprintf("SCHEMA %s", table))
}

func (c *Client) Index(table, col string) (string, error) {
	return c.Send(fmt.Sprintf("INDEX %s ON %s", table, col))
}
