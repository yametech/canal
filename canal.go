/*
Copyright 2019 yametech.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package canal

import (
	"bytes"
	"errors"
	"net"
	"strings"
)

var (
	ErrNotReplication = errors.New("unable to find replication info")
	ErrNotSlave       = errors.New("unable to find slave info")
	ErrNotOnline      = errors.New("slave connection not online")
)

type Config struct {
	addr  string
	conns []net.Conn
	opts  []DialOption

	repl_master bool
}

func iter(i int) []struct{} { return make([]struct{}, i) }

func (c *Config) reconfig() error {
	for i := range c.conns {
		err := c.conns[i].Close()
		if err != nil {
			return err
		}
	}
	for i := range iter(1) {
		conn, err := dial("tcp", c.addr, c.opts...)
		if err != nil {
			return err
		}
		c.conns[i] = conn
	}
	return nil
}

func NewConfig(addr string, opts ...DialOption) (*Config, error) {
	conns := make([]net.Conn, 1)
	for i := range iter(1) {
		conn, err := dial("tcp", addr, opts...)
		if err != nil {
			return nil, err
		}
		conns[i] = conn
	}

	return &Config{
		addr:  addr,
		conns: conns,
		opts:  opts,
	}, nil
}

func (c *Config) ReplMaster()          { c.repl_master = true }
func (c *Config) Connection() net.Conn { return c.conns[0] }

type Canal struct {
	cfg *Config

	cmder CommandDecoder

	ip     string
	port   string
	db     int
	replId string
	offset int64

	wr   *writer
	resp *reader

	ackErrC      chan error
	closeReplica chan struct{}

	redisInfo map[string]map[string]string
}

func NewCanal(cfg *Config) (*Canal, error) {
	c, err := newCanal(cfg)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func newCanal(cfg *Config) (*Canal, error) {
	c := new(Canal)
	c.closeReplica = make(chan struct{})
	c.cfg = cfg
	c.offset = -1

	c.redisInfo = make(map[string]map[string]string)
	err := c.info()
	if err != nil {
		return nil, err
	}

	if !c.cfg.repl_master {
		return c, nil
	}

	if !c.isMaster() {
		ip, port := c.realMaster()
		cfg.addr = strings.Join([]string{ip, port}, ":")
		err := cfg.reconfig()
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

func FromOffsetCanal(cfg *Config, replId string, offset int64) (*Canal, error) {
	c, err := newCanal(cfg)
	if err != nil {
		return nil, err
	}
	c.replId = replId
	c.set(offset)

	return c, nil
}

func (c *Canal) Run(commandDecode CommandDecoder) error {
	if commandDecode == nil {
		return errors.New("command decode is nil")
	}
	c.cmder = commandDecode

	return c.dumpAndParse()
}

func (c *Canal) Close()            { c.closeReplica <- struct{}{} }
func (c *Canal) GetReplId() string { return c.replId }

func (c *Canal) getNetConn() net.Conn { return c.cfg.conns[0] }

func (c *Canal) replconf() error {
	conn := c.getNetConn()
	ip, port, err := getAddr(conn)
	if err != nil {
		return err
	}
	c.ip, c.port = ip, port
	c.resp = newReader(conn)
	c.wr = newWriter(conn)

	version := c.version()
	if version == "" {
		return errors.New("get version error")
	}
	if version > "4.0.0" {
		err := c.wr.writeMultiBulk("REPLCONF", "listening-port", port)
		if err != nil {
			return err
		}
		val, _, err := c.resp.readBulk()
		if err != nil {
			return err
		}
		if !bytes.Equal(val.Str, []byte("OK")) {
			return errors.New("replconf listening port failed")
		}

		err = c.wr.writeMultiBulk("REPLCONF", "ip-address", ip)
		if err != nil {
			return err
		}
		val, _, err = c.resp.readBulk()
		if err != nil {
			return err
		}
		if !bytes.Equal(val.Str, []byte("OK")) {
			return errors.New("replconf ip-address failed")
		}

		err = c.wr.writeMultiBulk("REPLCONF", "capa", "eof")
		if err != nil {
			return err
		}
		val, _, err = c.resp.readBulk()
		if err != nil {
			return err
		}
		if !bytes.Equal(val.Str, []byte("OK")) {
			return errors.New("replconf capa eof failed")
		}

		err = c.wr.writeMultiBulk("REPLCONF", "capa", "psync2")
		if err != nil {
			return err
		}
		val, _, err = c.resp.readBulk()
		if err != nil {
			return err
		}
		if !bytes.Equal(val.Str, []byte("OK")) {
			return errors.New("replconf capa psync2 failed")
		}
	}

	if c.replId == "" {
		c.replId = "?"
	}

	return c.wr.writeMultiBulk("psync", c.replId, c.offset)
}

func (c *Canal) info() error {
	rw := NewRedisReaderWriter(c.cfg.conns[0])
	err := rw.writeMultiBulk("info")
	if err != nil {
		return err
	}

	v, _, err := rw.readBulk()
	if err != nil {
		return err
	}

	strList := strings.Split(v.String(), "\n")
	selection := ""
	for i := range strList {
		line := strings.TrimSpace(strList[i])
		if line == "" || len(line) == 0 {
			continue
		}
		if strings.HasPrefix(line, "#") {
			selection = strings.TrimSpace(line[1:])
			c.redisInfo[selection] = make(map[string]string)
			continue
		}
		contentList := strings.Split(line, ":")
		// 20190925  prevent access index out of bounds
		if len(contentList) < 2 {
			continue
		}
		c.redisInfo[selection][contentList[0]] = contentList[1]
	}

	return nil
}

func (c *Canal) version() string {
	server, ok := c.redisInfo["Server"]
	if !ok {
		return ""
	}
	version, ok := server["redis_version"]
	if !ok {
		return ""
	}
	return version
}

func (c *Canal) realMaster() (string, string) {
	replication, ok := c.redisInfo["Replication"]
	if !ok {
		return "", ""
	}
	host, ok := replication["master_host"]
	if !ok {
		return "", ""
	}
	port, ok := replication["master_port"]
	if !ok {
		return "", ""
	}
	replId, ok := replication["master_replid"]
	if !ok {
		return "", ""
	}
	c.replId = replId

	return host, port
}

func (c *Canal) isMaster() bool {
	replication, ok := c.redisInfo["Replication"]
	if !ok {
		return false
	}
	role, ok := replication["role"]
	if !ok {
		return false
	}

	return role == "master"
}

func getAddr(conn net.Conn) (ip string, port string, err error) {
	addr, ok := conn.LocalAddr().(*net.TCPAddr)
	if !ok {
		return "", "", errors.New("connection is invalid ?")
	}
	return net.SplitHostPort(addr.String())
}
