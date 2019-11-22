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
	"bufio"
	"io"
)

// writer is a specialized RESP Value type writer.
type writer struct {
	*bufio.Writer
	cur []byte
}

// newWriter returns a new Writer.
func newWriter(w io.Writer) *writer {
	return &writer{
		bufio.NewWriter(w),
		make([]byte, 0),
	}
}

// write cur
func _copy(dest *[]byte, src []byte) {
	*dest = make([]byte, len(src))
	copy(*dest, src)
}

// writeValue writes a RESP Value.
func (wr *writer) writeValue(v Value) error {
	b, err := v.MarshalRESP()
	if err != nil {
		return err
	}
	n, err := wr.Write(b)
	_copy(&wr.cur, b)
	if n < 1 {
		return &ErrProtocol{Msg: "write buf error."}
	}
	return err
}

// writeArray writes a RESP array.
func (wr *writer) writeArray(vals []Value) error { return wr.writeValue(ArrayValue(vals)) }

// writeMultiBulk writes a RESP array which contains one or more bulk strings.
// For more information on RESP arrays and strings please see http://redis.io/topics/protocol.
func (wr *writer) writeMultiBulk(commandName string, args ...interface{}) error {
	err := wr.writeValue(MultiBulkValue(commandName, args...))
	if err != nil {
		return err
	}
	return wr.Flush()
}

// get current command
func (wr *writer) get() []byte { return wr.cur }
