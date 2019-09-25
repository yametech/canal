package canal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const bufsz = 4096

// Type represents a Value type
type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
	Rdb          Type = 'R'
)

// TypeName returns name of the underlying RESP type.
func (t Type) String() string {
	switch t {
	default:
		return "Unknown"
	case '+':
		return "SimpleString"
	case '-':
		return "Error"
	case ':':
		return "Integer"
	case '$':
		return "BulkString"
	case '*':
		return "Array"
	case 'R':
		return "RDB"
	}
}

// Value represents the data of a valid RESP type.
type Value struct {
	Typ      Type
	IntegerV int
	Str      []byte
	ArrayV   []Value
	Null     bool
	RDB      bool
	Size     int
}

func (v Value) ReplInfo() (runID string, offset int64) {
	if v.Type() != Rdb {
		return
	}
	buf := bytes.Split(v.Str, []byte(" "))
	if len(buf) < 3 {
		return
	}
	_offset, err := strconv.ParseInt(string(buf[2]), 10, 64)
	if err != nil {
		return
	}
	return string(buf[1]), _offset
}

// Integer converts Value to an int. If Value cannot be converted, Zero is returned.
func (v Value) Integer() int {
	switch v.Typ {
	default:
		n, _ := strconv.ParseInt(v.String(), 10, 64)
		return int(n)
	case ':':
		return v.IntegerV
	}
}

// String converts Value to a string.
func (v Value) String() string {
	if v.Typ == '$' {
		return string(v.Str)
	}
	switch v.Typ {
	case '+', '-':
		return string(v.Str)
	case ':':
		return strconv.FormatInt(int64(v.IntegerV), 10)
	case '*':
		buf := bytes.NewBuffer(nil)
		concatArray(buf, v.ArrayV...)
		return strings.TrimSuffix(buf.String(), " ")
	case '\r':
		return fmt.Sprintf("%s", "\r\n")
	}
	return ""
}

func concatArray(wr io.Writer, vs ...Value) {
	for i := range vs {
		wr.Write([]byte(vs[i].String()))
		wr.Write([]byte{' '})
		concatArray(wr, vs[i].Array()...)
	}
}

// Bytes converts the Value to a byte array. An empty string is converted to a non-nil empty byte array.
// If it's a RESP Null value, nil is returned.
func (v Value) Bytes() []byte {
	switch v.Typ {
	default:
		return []byte(v.String())
	case '$', '+', '-':
		return v.Str
	}
}

// Float converts Value to a float64. If Value cannot be converted
// Zero is returned.
func (v Value) Float() float64 {
	switch v.Typ {
	default:
		f, _ := strconv.ParseFloat(v.String(), 64)
		return f
	case ':':
		return float64(v.IntegerV)
	}
}

// IsNull indicates whether or not the base value is null.
func (v Value) IsNull() bool {
	return v.Null
}

// Bool converts Value to an bool. If Value cannot be converted, false is returned.
func (v Value) Bool() bool {
	return v.Integer() != 0
}

// Error converts the Value to an error. If Value is not an error, nil is returned.
func (v Value) Error() error {
	switch v.Typ {
	case '-':
		return errors.New(string(v.Str))
	}
	return nil
}

// Array converts the Value to a an array.
// If Value is not an array or when it's is a RESP Null value, nil is returned.
func (v Value) Array() []Value {
	if v.Typ == '*' && !v.Null {
		return v.ArrayV
	}
	return nil
}

// Type returns the underlying RESP type.
// The following types are represent valid RESP values.
func (v Value) Type() Type {
	return v.Typ
}

func marshalSimpleRESP(typ Type, b []byte) ([]byte, error) {
	bb := make([]byte, 3+len(b))
	bb[0] = byte(typ)
	copy(bb[1:], b)
	bb[1+len(b)+0] = '\r'
	bb[1+len(b)+1] = '\n'
	return bb, nil
}

func marshalBulkRESP(v Value) ([]byte, error) {
	if v.Null {
		return []byte("$-1\r\n"), nil
	}
	szb := []byte(strconv.FormatInt(int64(len(v.Str)), 10))
	bb := make([]byte, 5+len(szb)+len(v.Str))
	bb[0] = '$'
	copy(bb[1:], szb)
	bb[1+len(szb)+0] = '\r'
	bb[1+len(szb)+1] = '\n'
	copy(bb[1+len(szb)+2:], v.Str)
	bb[1+len(szb)+2+len(v.Str)+0] = '\r'
	bb[1+len(szb)+2+len(v.Str)+1] = '\n'
	return bb, nil
}

func marshalArrayRESP(v Value) ([]byte, error) {
	if v.Null {
		return []byte("*-1\r\n"), nil
	}
	szb := []byte(strconv.FormatInt(int64(len(v.ArrayV)), 10))

	var buf bytes.Buffer
	buf.Grow(3 + len(szb) + 16*len(v.ArrayV)) // prime the buffer
	buf.WriteByte('*')
	buf.Write(szb)
	buf.WriteByte('\r')
	buf.WriteByte('\n')
	for i := 0; i < len(v.ArrayV); i++ {
		data, err := v.ArrayV[i].MarshalRESP()
		if err != nil {
			return nil, err
		}
		buf.Write(data)
	}
	return buf.Bytes(), nil
}

func marshalAnyRESP(v Value) ([]byte, error) {
	switch v.Typ {
	default:
		if v.Typ == 0 && v.Null {
			return []byte("$-1\r\n"), nil
		}
		return nil, errors.New("unknown resp type encountered")
	case '-', '+':
		return marshalSimpleRESP(v.Typ, v.Str)
	case ':':
		return marshalSimpleRESP(v.Typ, []byte(strconv.FormatInt(int64(v.IntegerV), 10)))
	case '$':
		return marshalBulkRESP(v)
	case '*':
		return marshalArrayRESP(v)
	}
}

// Equals compares one value to another value.
func (v Value) Equals(value Value) bool {
	data1, err := v.MarshalRESP()
	if err != nil {
		return false
	}
	data2, err := value.MarshalRESP()
	if err != nil {
		return false
	}
	return string(data1) == string(data2)
}

// MarshalRESP returns the original serialized byte representation of Value.
// For more information on this format please see http://redis.io/topics/protocol.
func (v Value) MarshalRESP() ([]byte, error) {
	return marshalAnyRESP(v)
}

var NilValue = Value{Null: true}

type ErrProtocol struct{ Msg string }

func (err ErrProtocol) Error() string {
	return "Protocol error: " + err.Msg
}

// AnyValue returns a RESP value from an interface.
// This function infers the types. Arrays are not allowed.
func AnyValue(v interface{}) Value {
	switch v := v.(type) {
	default:
		return StringValue(fmt.Sprintf("%v", v))
	case nil:
		return NullValue()
	case int:
		return IntegerValue(int(v))
	case uint:
		return IntegerValue(int(v))
	case int8:
		return IntegerValue(int(v))
	case uint8:
		return IntegerValue(int(v))
	case int16:
		return IntegerValue(int(v))
	case uint16:
		return IntegerValue(int(v))
	case int32:
		return IntegerValue(int(v))
	case uint32:
		return IntegerValue(int(v))
	case int64:
		return IntegerValue(int(v))
	case uint64:
		return IntegerValue(int(v))
	case bool:
		return BoolValue(v)
	case float32:
		return FloatValue(float64(v))
	case float64:
		return FloatValue(float64(v))
	case []byte:
		return BytesValue(v)
	case string:
		return StringValue(v)
	}
}

// SimpleStringValue returns a RESP simple string. A simple string has no new lines. The carriage return and new line characters are replaced with spaces.
func SimpleStringValue(s string) Value { return Value{Typ: '+', Str: []byte(formSingleLine(s))} }

// BytesValue returns a RESP bulk string. A bulk string can represent any data.
func BytesValue(b []byte) Value { return Value{Typ: '$', Str: b} }

// StringValue returns a RESP bulk string. A bulk string can represent any data.
func StringValue(s string) Value { return Value{Typ: '$', Str: []byte(s)} }

// NullValue returns a RESP null bulk string.
func NullValue() Value { return Value{Typ: '$', Null: true} }

// ErrorValue returns a RESP error.
func ErrorValue(err error) Value {
	if err == nil {
		return Value{Typ: '-'}
	}
	return Value{Typ: '-', Str: []byte(err.Error())}
}

// IntegerValue returns a RESP integer.
func IntegerValue(i int) Value { return Value{Typ: ':', IntegerV: i} }

// BoolValue returns a RESP integer representation of a bool.
func BoolValue(t bool) Value {
	if t {
		return Value{Typ: ':', IntegerV: 1}
	}
	return Value{Typ: ':', IntegerV: 0}
}

// FloatValue returns a RESP bulk string representation of a float.
func FloatValue(f float64) Value { return StringValue(strconv.FormatFloat(f, 'f', -1, 64)) }

// ArrayValue returns a RESP array.
func ArrayValue(vals []Value) Value { return Value{Typ: '*', ArrayV: vals} }

func formSingleLine(s string) string {
	bs1 := []byte(s)
	for i := 0; i < len(bs1); i++ {
		switch bs1[i] {
		case '\r', '\n':
			bs2 := make([]byte, len(bs1))
			copy(bs2, bs1)
			bs2[i] = ' '
			i++
			for ; i < len(bs2); i++ {
				switch bs1[i] {
				case '\r', '\n':
					bs2[i] = ' '
				}
			}
			return string(bs2)
		}
	}
	return s
}

// MultiBulkValue returns a RESP array which contains one or more bulk strings.
// For more information on RESP arrays and strings please see http://redis.io/topics/protocol.
func MultiBulkValue(commandName string, args ...interface{}) Value {
	vals := make([]Value, len(args)+1)
	vals[0] = StringValue(commandName)
	for i, arg := range args {
		if rval, ok := arg.(Value); ok && rval.Type() == BulkString {
			vals[i+1] = rval
			continue
		}
		switch arg := arg.(type) {
		default:
			vals[i+1] = StringValue(fmt.Sprintf("%v", arg))
		case []byte:
			vals[i+1] = StringValue(string(arg))
		case string:
			vals[i+1] = StringValue(arg)
		case nil:
			vals[i+1] = NullValue()
		}
	}
	return ArrayValue(vals)
}

func MultiBulkBytes(val Value) ([]byte, int) {
	buf := bytes.NewBuffer(nil)
	switch val.Typ {
	case '+', '-':
		buf.WriteByte(byte(val.Typ))
		buf.WriteString(val.String())
		buf.Write([]byte{'\r', '\n'})
		return buf.Bytes(), len(buf.Bytes())
	case '$', ':':
		buf.WriteByte(byte(val.Typ))
		buf.WriteString(fmt.Sprintf("%d", len(val.String())))
		buf.Write([]byte{'\r', '\n'})
		buf.WriteString(val.String())
		buf.Write([]byte{'\r', '\n'})
		return buf.Bytes(), len(buf.Bytes())
	case '*':
		buf.WriteByte(byte(val.Typ))
		length := len(val.ArrayV)
		buf.WriteString(fmt.Sprintf("%d", length))
		buf.Write([]byte{'\r', '\n'})
		for i := range val.ArrayV {
			bs, _ := MultiBulkBytes(val.ArrayV[i])
			buf.Write(bs)
		}
		return buf.Bytes(), buf.Len()
	}
	return []byte{}, 0
}
