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
	"fmt"
	"strconv"
	"sync/atomic"
)

func (c *Canal) Command(cmd *Command) error {
	return c.cmder.Command(cmd)
}

func (c *Canal) set(n int64) {
	atomic.StoreInt64(&c.offset, n)
}

func (c *Canal) Increment(n int64) {
	atomic.AddInt64(&c.offset, n)
}

func (c *Canal) Offset() string {
	return fmt.Sprintf("%d", atomic.LoadInt64(&c.offset))
}

func (c *Canal) BeginRDB() {}

func (c *Canal) BeginDatabase(n int) error {
	c.db = n
	cmd, _ := NewCommand("SELECT", fmt.Sprintf("%d", n))
	if err := c.Command(cmd); err != nil {
		return err
	}
	return nil
}

func (c *Canal) Aux(key, value []byte) {
	if string(key) == "repl-offset" {
		i, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			panic(err)
		}
		c.set(i)
	} else if string(key) == "repl-id" {
		c.replId = string(value)
	}
}

func (c *Canal) ResizeDatabase(dbSize, expiresSize uint32) {}

func (c *Canal) EndDatabase(n int) {}

func (c *Canal) Set(key, value []byte, expiry int64) error {
	cmd, _ := NewCommand("SET", string(key), string(value))
	if err := c.Command(cmd); err != nil {
		return err
	}
	return nil
}

func (c *Canal) BeginHash(key []byte, length, expiry int64) {}

func (c *Canal) Hset(key, field, value []byte) error {
	cmd, _ := NewCommand("HSET", string(key), string(field), string(value))
	if err := c.Command(cmd); err != nil {
		return err
	}
	return nil
}
func (c *Canal) EndHash(key []byte) {}

func (c *Canal) BeginSet(key []byte, cardinality, expiry int64) {}

func (c *Canal) Sadd(key, member []byte) error {
	cmd, _ := NewCommand("SADD", string(key), string(member))
	if err := c.Command(cmd); err != nil {
		return err
	}
	return nil
}
func (c *Canal) EndSet(key []byte) {}

func (c *Canal) BeginList(key []byte, length, expiry int64) {}

func (c *Canal) Rpush(key, value []byte) error {
	cmd, _ := NewCommand("RPUSH", string(key), string(value))
	if err := c.Command(cmd); err != nil {
		return err
	}
	return nil
}
func (c *Canal) EndList(key []byte) {}

func (c *Canal) BeginZSet(key []byte, cardinality, expiry int64) {}

func (c *Canal) Zadd(key []byte, score float64, member []byte) error {
	cmd, _ := NewCommand("ZADD", string(key), fmt.Sprintf("%f", score), string(member))
	if err := c.Command(cmd); err != nil {
		return err
	}
	return nil
}
func (c *Canal) EndZSet(key []byte) {}

func (c *Canal) BeginStream(key []byte, cardinality, expiry int64) {}

func (c *Canal) Xadd(key, id, listpack []byte) error {
	cmd, _ := NewCommand("XADD", string(key), string(id), string(listpack))
	if err := c.Command(cmd); err != nil {
		return err
	}
	return nil
}
func (c *Canal) EndStream(key []byte) {}

func (c *Canal) EndRDB() {
	// log.Printf("rdb end")
}
