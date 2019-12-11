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
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

type LeakyBuf struct {
	bufSize  int // size of each buffer
	freeList chan []byte
}

const leakyBufSize = 4108 // data.len(2) + hmacsha1(10) + data(4096)
const maxNBuf = 2048

var leakyBuf = NewLeakyBuf(maxNBuf, leakyBufSize)

// NewLeakyBuf creates a leaky buffer which can hold at most n buffer, each
// with bufSize bytes.
func NewLeakyBuf(n, bufSize int) *LeakyBuf {
	return &LeakyBuf{
		bufSize:  bufSize,
		freeList: make(chan []byte, n),
	}
}

// Get returns a buffer from the leaky buffer or create a new buffer.
func (lb *LeakyBuf) Get() (b []byte) {
	select {
	case b = <-lb.freeList:
	default:
		b = make([]byte, lb.bufSize)
	}
	return
}

// Put add the buffer into the free buffer pool for reuse. Panic if the buffer
// size is not the same with the leaky buffer's. This is intended to expose
// error usage of leaky buffer.
func (lb *LeakyBuf) Put(b []byte) {
	if len(b) != lb.bufSize {
		panic("invalid buffer size that's put into leaky buffer")
	}
	select {
	case lb.freeList <- b[:lb.bufSize]:
	default:
	}
	return
}

var (
	lazyCmdPool = sync.Pool{
		New: func() interface{} {
			return &Command{}
		},
	}
	xmit = leakyBuf
)

func (c *Canal) dump(w io.Writer) error {
	conn := c.getNetConn()
	for {
		buf := xmit.Get()
		_, err := io.CopyBuffer(w, conn, buf)
		if err != nil {
			return err
		}
		xmit.Put(buf)
	}
}

func (c *Canal) dumpAndParse() (err error) {
	err = c.replconf()
	if err != nil {
		return err
	}
	r, w := io.Pipe()

	done := make(chan error, 1)
	go func() {
		err := c.handler(r)
		_ = w.CloseWithError(err)
		done <- err
	}()

	err = c.dump(w)
	_ = w.CloseWithError(err)
	err = <-done

	return err
}

func (c *Canal) handler(rd io.Reader) error {
	resp := newReader(rd)
	one := sync.Once{}
	for {
		select {
		case aerr := <-c.ackErrC:
			return aerr
		default:
		}

		val, n, err := resp.readBulk()
		if err != nil {
			return err
		}

		switch val.Typ {
		case '-', ':', '$':
		case '+':
			if bytes.HasPrefix(val.Str, []byte(`FULLRESYNC`)) {
				resp, err = decodeStream(resp, c)
				if err != nil {
					return err
				}

			} else if bytes.HasPrefix(val.Str, []byte(`CONTINUE`)) {
				ss := strings.Split(val.String(), " ")
				if len(ss) != 2 {
					return fmt.Errorf("%s(%s)", "error CONTINUE resp", val.String())
				}
				c.replId = ss[1]
			}
		case '*':
			cmd := lazyCmdPool.Get().(*Command)
			cmd.Set(buildStrCommand(val.String())...)
			err = c.Command(cmd)
			if err != nil {
				return err
			}
			lazyCmdPool.Put(cmd)
			c.Increment(int64(n))
		default:
			log.Printf("address %s replId %s unknown opcode %v size %d", c.ip, c.replId, val, val.Size)
		}

		one.Do(func() {
			go c.replack()
		})
	}
}

func (c *Canal) replack() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.closeReplica:
			return
		default:
		}
		err := c.wr.writeMultiBulk("replconf", "ack", c.Offset())
		if err != nil {
			c.ackErrC <- err
		}
		<-ticker.C
	}
}
