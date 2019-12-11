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

const max_sz = 4096

var (
	lazyCmdPool = sync.Pool{
		New: func() interface{} {
			return &Command{}
		},
	}
	xmit = sync.Pool{
		New: func() interface{} {
			return make([]byte, max_sz)
		},
	}
)

func (c *Canal) dump(w io.Writer) error {
	conn := c.getNetConn()
	for {
		buf := xmit.Get().([]byte)
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
