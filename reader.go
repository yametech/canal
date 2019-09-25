package canal

import (
	"bufio"
	"io"
	"log"
	"strconv"
)

// reader is a specialized RESP Value type reader.
type reader struct {
	*bufio.Reader
}

// newReader returns a Reader for reading Value types.
func newReader(rd io.Reader) *reader {
	return &reader{bufio.NewReader(rd)}
}

// readBulk reads the next Value from Reader.
func (rd *reader) readBulk() (value Value, n int, err error) {
	value, n, err = rd.readValue(false, false)
	return
}

// readMultiBulk reads the next multi bulk Value from Reader.
// A multi bulk value is a RESP ArrayV that contains one or more bulk strings.
// For more information on RESP arrays and strings please see http://redis.io/topics/protocol.
func (rd *reader) readMultiBulk() (value Value, n int, err error) {
	return rd.readValue(true, false)
}

func (rd *reader) readValue(multibulk, child bool) (val Value, n int, err error) {
	var rn int
	var c byte
	c, err = rd.ReadByte()
	if err != nil {
		return NilValue, n, err
	}
	n++
	switch c {
	case '*':
		val, rn, err = rd.readArrayValue(multibulk)
	case '-', '+':
		val, rn, err = rd.readSimpleValue(c)
	case ':':
		val, rn, err = rd.readIntegerValue()
	case '$':
		val, rn, err = rd.readBulkValue()
	case 0x0a:
		return Value{Null: true, Size: 1}, n, nil
	case '0':
		return Value{Null: true, Size: 1}, n, nil
	default:
		log.Printf("opcode error %s", val)
		return Value{}, n, nil
	}
	n += rn
	if err == io.EOF {
		return NilValue, n, io.ErrUnexpectedEOF
	}
	return val, n, err
}

func (rd *reader) readSimpleValue(Typ byte) (val Value, n int, err error) {
	var line []byte
	line, n, err = rd.readLine()
	if err != nil {
		return NilValue, n, err
	}
	return Value{Typ: Type(Typ), Str: line}, n, nil
}

func (rd *reader) readLine() (line []byte, n int, err error) {
	line, _, err = rd.ReadLine()
	if err != nil {
		return
	}
	n += len(line)
	return line, n + 2, nil
}

func (rd *reader) readBulkValue() (val Value, n int, err error) {
	l, rn, err := rd.readInt()
	if err != nil {
		if _, ok := err.(*ErrProtocol); ok {
			return NilValue, n, &ErrProtocol{Msg: "invalid bulk length"}
		}
		return NilValue, n, err
	} else if l < 0 {
		return Value{Typ: '$', Null: true}, n, nil
	} else if l > 512*1024*1024 {
		return NilValue, n, &ErrProtocol{Msg: "invalid bulk length"}
	}
	n += rn

	b := make([]byte, l+2)
	rn, err = io.ReadFull(rd, b)
	if err != nil {
		return NilValue, n, err
	}
	if b[l] != '\r' || b[l+1] != '\n' {
		return NilValue, n, &ErrProtocol{Msg: "invalid bulk line ending"}
	}
	n += rn

	return Value{Typ: '$', Str: b[:l]}, n, nil
}

func (rd *reader) readArrayValue(multibulk bool) (val Value, n int, err error) {
	var rn int
	var l int
	l, rn, err = rd.readInt()
	n += rn
	if err != nil || l > 1024*1024 {
		if _, ok := err.(*ErrProtocol); ok {
			if multibulk {
				return NilValue, n, &ErrProtocol{Msg: "invalid multibulk length"}
			}
			return NilValue, n, &ErrProtocol{Msg: "invalid ArrayV length"}
		}
		return NilValue, n, err
	}
	if l < 0 {
		return Value{Typ: '*', Null: true}, n, nil
	}
	var aval Value
	vals := make([]Value, l)
	for i := 0; i < l; i++ {
		aval, rn, err = rd.readValue(multibulk, true)
		n += rn
		if err != nil {
			return NilValue, n, err
		}
		vals[i] = aval
	}
	return Value{Typ: '*', ArrayV: vals}, n, nil
}

func (rd *reader) readIntegerValue() (val Value, n int, err error) {
	var l int
	l, n, err = rd.readInt()
	if err != nil {
		if _, ok := err.(*ErrProtocol); ok {
			return NilValue, n, &ErrProtocol{Msg: "invalid integer"}
		}
		return NilValue, n, err
	}
	return Value{Typ: ':', IntegerV: l}, n, nil
}

func (rd *reader) readInt() (x int, n int, err error) {
	line, n, err := rd.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}
