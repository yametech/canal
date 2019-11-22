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
	"net"
)

var (
	Dial = dial
)

type RedisReaderWriter struct {
	*reader
	*writer
}

func NewRedisReaderWriter(conn net.Conn) *RedisReaderWriter {
	return &RedisReaderWriter{
		newReader(conn),
		newWriter(conn),
	}
}

func (r *RedisReaderWriter) ReadValue() (Value, error) {
	v, _, err := r.readBulk()
	return v, err
}

func (r *RedisReaderWriter) ReadLine() ([]byte, error) {
	v, _, err := r.readLine()
	return v, err
}

func (r *RedisReaderWriter) Sendcmds(cmd string, args ...interface{}) error {
	return r.writeMultiBulk(cmd, args...)
}

// ValueType of redis type
type ValueType byte

// type value
const (
	TypeString  ValueType = 0
	TypeList    ValueType = 1
	TypeSet     ValueType = 2
	TypeZSet    ValueType = 3
	TypeHash    ValueType = 4
	TypeZSet2   ValueType = 5
	TypeModule  ValueType = 6
	TypeModule2 ValueType = 7

	TypeHashZipmap      ValueType = 9
	TypeListZiplist     ValueType = 10
	TypeSetIntset       ValueType = 11
	TypeZSetZiplist     ValueType = 12
	TypeHashZiplist     ValueType = 13
	TypeListQuicklist   ValueType = 14
	TypeStreamListPacks ValueType = 15
)

const (
	rdbVersion  = 9
	rdb6bitLen  = 0
	rdb14bitLen = 1
	rdb32bitLen = 0x80
	rdb64bitLen = 0x81
	rdbEncVal   = 3
	//rdbLenErr   = math.MaxUint64

	rdbOpCodeModuleAux = 247
	rdbOpCodeIdle      = 248
	rdbOpCodeFreq      = 249
	rdbOpCodeAux       = 250
	rdbOpCodeResizeDB  = 251
	rdbOpCodeExpiryMS  = 252
	rdbOpCodeExpiry    = 253
	rdbOpCodeSelectDB  = 254
	rdbOpCodeEOF       = 255

	//rdbModuleOpCodeEOF    = 0
	//rdbModuleOpCodeSint   = 1
	//rdbModuleOpCodeUint   = 2
	//rdbModuleOpCodeFloat  = 3
	//rdbModuleOpCodeDouble = 4
	//rdbModuleOpCodeString = 5
	//
	//rdbLoadNone  = 0
	//rdbLoadEnc   = (1 << 0)
	//rdbLoadPlain = (1 << 1)
	//rdbLoadSds   = (1 << 2)
	//
	//rdbSaveNode        = 0
	//rdbSaveAofPreamble = (1 << 0)

	rdbEncInt8  = 0
	rdbEncInt16 = 1
	rdbEncInt32 = 2
	rdbEncLZF   = 3

	rdbZiplist6bitlenString  = 0
	rdbZiplist14bitlenString = 1
	rdbZiplist32bitlenString = 2

	rdbZiplistInt16 = 0xc0
	rdbZiplistInt32 = 0xd0
	rdbZiplistInt64 = 0xe0
	rdbZiplistInt24 = 0xf0
	rdbZiplistInt8  = 0xfe
	rdbZiplistInt4  = 15

	//rdbLpHdrSize           = 6
	//rdbLpHdrNumeleUnknown  = math.MaxUint16
	//rdbLpMaxIntEncodingLen = 0
	//rdbLpMaxBacklenSize    = 5
	//rdbLpMaxEntryBacklen   = 34359738367
	//rdbLpEncodingInt       = 0
	//rdbLpEncodingString    = 1

	rdbLpEncoding7BitUint     = 0
	rdbLpEncoding7BitUintMask = 0x80

	rdbLpEncoding6BitStr     = 0x80
	rdbLpEncoding6BitStrMask = 0xC0

	rdbLpEncoding13BitInt     = 0xC0
	rdbLpEncoding13BitIntMask = 0xE0

	rdbLpEncoding12BitStr     = 0xE0
	rdbLpEncoding12BitStrMask = 0xF0

	rdbLpEncoding16BitInt     = 0xF1
	rdbLpEncoding16BitIntMask = 0xFF

	rdbLpEncoding24BitInt     = 0xF2
	rdbLpEncoding24BitIntMask = 0xFF

	rdbLpEncoding32BitInt     = 0xF3
	rdbLpEncoding32BitIntMask = 0xFF

	rdbLpEncoding64BitInt     = 0xF4
	rdbLpEncoding64BitIntMask = 0xFF

	rdbLpEncoding32BitStr     = 0xF0
	rdbLpEncoding32BitStrMask = 0xFF

	rdbLpEOF              = 0xFF
	rdbStreamItemFlagNone = 0 /* No special flags. */
	//rdbStreamItemFlagDeleted     = (1 << 0) /* Entry was deleted. Skip it. */
	rdbStreamItemFlangSameFields = (1 << 1) /* Same fields as master entry. */
)
