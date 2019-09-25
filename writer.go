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

// writeSimpleString writes a RESP simple string. A simple string has no new lines.
// The carriage return and new line characters are replaced with spaces.
func (wr *writer) writeSimpleString(s string) error { return wr.writeValue(SimpleStringValue(s)) }

// writeBytes writes a RESP bulk string. A bulk string can represent any data.
func (wr *writer) writeBytes(b []byte) error { return wr.writeValue(BytesValue(b)) }

// writeString writes a RESP bulk string. A bulk string can represent any data.
func (wr *writer) writeString(s string) error { return wr.writeValue(StringValue(s)) }

// writeNull writes a RESP null bulk string.
func (wr *writer) writeNull() error { return wr.writeValue(NullValue()) }

// writeError writes a RESP error.
func (wr *writer) writeError(err error) error { return wr.writeValue(ErrorValue(err)) }

// writeInteger writes a RESP integer.
func (wr *writer) writeInteger(i int) error { return wr.writeValue(IntegerValue(i)) }

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
