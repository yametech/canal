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
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnyValues(t *testing.T) {
	var vs = []interface{}{
		nil,
		int(10), uint(10), int8(10),
		uint8(10), int16(10), uint16(10),
		int32(10), uint32(10), int64(10),
		uint64(10), bool(true), bool(false),
		float32(10), float64(10),
		[]byte("hello"), string("hello"),
	}
	for i, v := range vs {
		if AnyValue(v).String() == "" && v != nil {
			t.Fatalf("missing string value for #%d: '%v'", i, v)
		}
	}
}

//func TestArrayString(t *testing.T) {
//	arrayValue := Value{
//		Typ: Array,
//		ArrayV: []Value{
//			{
//				Typ: BulkString,
//				Str: []byte("SET"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("A"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("123"),
//			},
//		},
//	}
//
//	arrayStr := arrayValue.String()
//
//	assert.Equal(t, "SET A 123", arrayStr, "should be euqal")
//
//	arrayValue2 := Value{
//		Typ: Array,
//		ArrayV: []Value{
//			{
//				Typ: BulkString,
//				Str: []byte("SADD"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("SK1"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("1"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("2"),
//			},
//		},
//	}
//	assert.Equal(t, "SADD SK1 1 2", arrayValue2.String(), "should be euqal")
//
//	arrayValue3 := Value{
//		Typ: Array,
//		ArrayV: []Value{
//			{
//				Typ: BulkString,
//				Str: []byte("SELECT"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("1"),
//			},
//		},
//	}
//	assert.Equal(t, "SELECT 1", arrayValue3.String(), "should be euqal")
//
//	//Xadd x 1 f1 v1 f2 v2 f3 v3
//
//	arrayValue4 := Value{
//		Typ: Array,
//		ArrayV: []Value{
//			{
//				Typ: BulkString,
//				Str: []byte("XADD"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("x"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("1"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("f1"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("v1"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("f2"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("v2"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("f3"),
//			},
//			{
//				Typ: BulkString,
//				Str: []byte("v3"),
//			},
//		},
//	}
//	assert.Equal(t, "XADD x 1 f1 v1 f2 v2 f3 v3", arrayValue4.String(), "should be euqal")
//}

func TestMarshalStrangeValue(t *testing.T) {
	var v Value
	v.Null = true
	b, err := marshalAnyRESP(v)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "$-1\r\n" {
		t.Fatalf("expected '%v', got '%v'", "$-1\r\n", string(b))
	}
	v.Null = false

	_, err = marshalAnyRESP(v)
	if err == nil || err.Error() != "unknown resp type encountered" {
		t.Fatalf("expected '%v', got '%v'", "unknown resp type encountered", err)
	}
}

//func TestRdbStart(t *testing.T) {
//	fullsync := "+FULLRESYNC 875aa386440719e2d343628d44225b7bed0a0acc 4321"
//	v := Value{Str: []byte(fullsync)}
//
//	assert.Equal(t, Rdb, v.Type(), "should be is rdb type.")
//
//	runid, offset := v.ReplInfo()
//	assert.Equal(t, "875aa386440719e2d343628d44225b7bed0a0acc", runid, "should be equal.")
//	assert.Equal(t, int64(4321), offset, "should be equal.")
//}

func TestMultiBulkBytes(t *testing.T) {
	expected := []byte{'*', '3', '\r', '\n', '$', '3', '\r', '\n', 'S', 'E', 'T', '\r', '\n', '$', '1', '\r', '\n', 'x', '\r', '\n', '$', '4', '\r', '\n', '1', '2', '3', '4', '\r', '\n'}
	v := MultiBulkValue("SET", "x", "1234")
	bs, l := MultiBulkBytes(v)
	assert.Equal(t, string(expected), string(bs), "should be equal.")
	assert.Equal(t, l, len(bs), "should be equal.")
}

func TestManyMultiBulkBytes(t *testing.T) {
	var testSet = []struct {
		v        Value
		expected []byte
	}{
		{MultiBulkValue("SET", "x", "1234"), []byte{'*', '3', '\r', '\n', '$', '3', '\r', '\n', 'S', 'E', 'T', '\r', '\n', '$', '1', '\r', '\n', 'x', '\r', '\n', '$', '4', '\r', '\n', '1', '2', '3', '4', '\r', '\n'}},
		{MultiBulkValue("GET", "x"), []byte("*2\r\n$3\r\nGET\r\n$1\r\nx\r\n")},
	}

	for _, ts := range testSet {
		testMultiBulkBytes(t, ts.v, ts.expected)
	}
}

func testMultiBulkBytes(t *testing.T, v Value, expected []byte) {
	bs, l := MultiBulkBytes(v)
	assert.Equal(t, string(expected), string(bs), "should be equal.")
	assert.Equal(t, l, len(bs), "should be equal.")
}

func TestIntegers(t *testing.T) {
	var n, rn int
	var v Value
	var err error
	data := []byte(":1234567\r\n:-90898\r\n:0\r\n")
	r := newReader(bytes.NewBuffer(data))
	v, rn, err = r.readBulk()
	n += rn
	if err != nil {
		t.Fatal(err)
	}
	if v.Integer() != 1234567 {
		t.Fatalf("invalid integer: expected %d, got %d", 1234567, v.Integer())
	}
	v, rn, err = r.readBulk()
	n += rn
	if err != nil {
		t.Fatal(err)
	}
	if v.Integer() != -90898 {
		t.Fatalf("invalid integer: expected %d, got %d", -90898, v.Integer())
	}
	v, rn, err = r.readBulk()
	n += rn
	if err != nil {
		t.Fatal(err)
	}
	if v.Integer() != 0 {
		t.Fatalf("invalid integer: expected %d, got %d", 0, v.Integer())
	}
	v, rn, err = r.readBulk()
	n += rn
	if err != io.EOF {
		t.Fatalf("invalid error: expected %v, got %v", io.EOF, err)
	}
	if n != len(data) {
		t.Fatalf("invalid read count: expected %d, got %d", len(data), n)
	}
}

func TestFloats(t *testing.T) {
	var n, rn int
	var v Value
	var err error
	data := []byte(":1234567\r\n+-90898\r\n$6\r\n12.345\r\n-90284.987\r\n")
	r := newReader(bytes.NewBuffer(data))
	v, rn, err = r.readBulk()
	n += rn
	if err != nil {
		t.Fatal(err)
	}
	if v.Float() != 1234567 {
		t.Fatalf("invalid integer: expected %v, got %v", 1234567, v.Float())
	}
	v, rn, err = r.readBulk()
	n += rn
	if err != nil {
		t.Fatal(err)
	}
	if v.Float() != -90898 {
		t.Fatalf("invalid integer: expected %v, got %v", -90898, v.Float())
	}
	v, rn, err = r.readBulk()
	n += rn
	if err != nil {
		t.Fatal(err)
	}
	if v.Float() != 12.345 {
		t.Fatalf("invalid integer: expected %v, got %v", 12.345, v.Float())
	}
	v, rn, err = r.readBulk()
	n += rn
	if err != nil {
		t.Fatal(err)
	}
	if v.Float() != 90284.987 {
		t.Fatalf("invalid integer: expected %v, got %v", 90284.987, v.Float())
	}
	v, rn, err = r.readBulk()
	n += rn
	if err != io.EOF {
		t.Fatalf("invalid error: expected %v, got %v", io.EOF, err)
	}
	if n != len(data) {
		t.Fatalf("invalid read count: expected %d, got %d", len(data), n)
	}
}

//func TestTelnetRedisReader(t *testing.T) {
//	rd := newReader(bytes.NewBufferString("SET HELLO WORLD\r\nGET HELLO\r\n"))
//	for i := 0; ; i++ {
//		v, _, err := rd.readMultiBulk()
//		if err != nil {
//			if err == io.EOF {
//				break
//			}
//			t.Fatal(err)
//		}
//		arr := v.Array()
//		switch i {
//		default:
//			t.Fatalf("i is %v, expected 0 or 1", i)
//		case 0:
//			if len(arr) != 3 {
//				t.Fatalf("expected 3, got %v", len(arr))
//			}
//		case 1:
//			if len(arr) != 2 {
//				t.Fatalf("expected 2, got %v", len(arr))
//			}
//		}
//	}
//}

func TestRedisWriter(t *testing.T) {
	expected := "" + "*4\r\n$5\r\nHELLO\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"

	wr := newWriter(nil)
	array := MultiBulkValue("HELLO", 1, 2, 3).Array()

	assert.Equal(t, nil, wr.writeArray(array), "should not error.")

	res := string(wr.get())
	if res != expected {
		t.Fatalf("expected %s, got %s", expected, res)
	}
}

//func TestWriteStringData(t *testing.T) {
//	res := "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$3\r\n123\r\n"
//	wr := newWriter(nil)
//	err := wr.writeMultiBulk("SET", "x", "123")
//	assert.Equal(t, nil, err, "should be not error.")
//	assert.Equal(t, res, string(wr.get()), "should be equal.")
//}
//
//func TestWriter(t *testing.T) {
//	t.Run("Ping", func(t *testing.T) {
//		testWrite(
//			t,
//			"*1\r\n$4\r\nPING\r\n",
//			"PING",
//		)
//	})
//
//	t.Run("Sadd", func(t *testing.T) {
//		testWrite(
//			t,
//			"*5\r\n$4\r\nSADD\r\n$2\r\nS1\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n",
//			"SADD",
//			"S1",
//			1,
//			2,
//			"3",
//		)
//	})
//}

//func testWrite(t *testing.T, expected string, src string, args ...interface{}) {
//	wr := newWriter(nil)
//	assert.Equal(t, nil, wr.writeMultiBulk(src, args...), "should be not error.")
//	assert.Equal(t, expected, string(wr.get()), "should be equal.")
//}

func TestReader(t *testing.T) {

	t.Run("ping", func(t *testing.T) {
		testCommandResult(t, "*1\r\n$4\r\nping\r\n",
			Value{Typ: Array,
				ArrayV: []Value{
					{Typ: BulkString,
						Str: []byte("ping"),
					},
				},
			},
			14,
		)

		testCommandResult(
			t,
			"+PONG\r\n",
			Value{Typ: SimpleString, Str: []byte("PONG")},
			7,
		)

		testCommandResult(
			t,
			"+OK\r\n",
			Value{Typ: SimpleString, Str: []byte("OK")},
			5,
		)

		testCommandResult(
			t,
			"+OK\r\n",
			Value{Typ: SimpleString, Str: []byte("OK")},
			5,
		)

		testCommandResult(
			t,
			"+FULLRESYNC 875aa386440719e2d343628d44225b7bed0a0acc 4321\r\n",
			Value{Typ: SimpleString, Str: []byte("FULLRESYNC 875aa386440719e2d343628d44225b7bed0a0acc 4321")},
			59,
		)

		testCommandResult(
			t,
			"+CONTINUE\r\n",
			Value{Typ: SimpleString, Str: []byte("CONTINUE")},
			11,
		)

	})
}

func testCommandResult(t *testing.T, src string, expected Value, length int) {
	v, l, e := newReader(strings.NewReader(src)).readBulk()
	assert.Nil(t, e)
	assert.Equal(t, v, expected, "Value should be equal.")
	assert.Equal(t, length, l, "Length should be equal.")
}
