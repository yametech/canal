// Thanks to github.com/cupcake/rdb for providing an early version of deocde
package canal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
)

func decodeStream(r io.Reader, d Decoder) (*reader, error) {
	decoder := &rdbDecode{
		d,
		make([]byte, 8),
		bufio.NewReader(r),
	}
	return decoder.decode(false)
}

type rdbDecode struct {
	event  Decoder
	intBuf []byte
	r      *bufio.Reader
}

func (d *rdbDecode) decode(file bool) (*reader, error) {
	err := d.checkHeader(file)
	if err != nil {
		return nil, err
	}
	d.event.BeginRDB()
	var db uint64
	var expiry int64
	var lruIdle uint64
	var lfuFreq int
	firstDB := true
	for {
		objType, err := d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		switch objType {
		case rdbOpCodeFreq:
			b, err := d.r.ReadByte()
			lfuFreq = int(b)
			if err != nil {
				return nil, err
			}
		case rdbOpCodeIdle:
			idle, _, err := d.readLength()
			if err != nil {
				return nil, err
			}
			lruIdle = uint64(idle)
		case rdbOpCodeAux:
			auxKey, err := d.readString()
			if err != nil {
				return nil, err
			}
			auxVal, err := d.readString()
			if err != nil {
				return nil, err
			}
			d.event.Aux(auxKey, auxVal)
		case rdbOpCodeResizeDB:
			dbSize, _, err := d.readLength()
			if err != nil {
				return nil, err
			}
			expiresSize, _, err := d.readLength()
			if err != nil {
				return nil, err
			}
			d.event.ResizeDatabase(uint32(dbSize), uint32(expiresSize))
		case rdbOpCodeExpiryMS:
			_, err := io.ReadFull(d.r, d.intBuf)
			if err != nil {
				return nil, err
			}
			expiry = int64(binary.LittleEndian.Uint64(d.intBuf))
		case rdbOpCodeExpiry:
			_, err := io.ReadFull(d.r, d.intBuf[:4])
			if err != nil {
				return nil, err
			}
			expiry = int64(binary.LittleEndian.Uint32(d.intBuf)) * 1000
		case rdbOpCodeSelectDB:
			if !firstDB {
				d.event.EndDatabase(int(db))
			}
			db, _, err = d.readLength()
			if err != nil {
				return nil, err
			}
			d.event.BeginDatabase(int(db))
		case rdbOpCodeEOF:
			d.event.EndDatabase(int(db))
			d.event.EndRDB()
			crc64DigitBuf := make([]byte, 8)
			_, err := io.ReadFull(d.r, crc64DigitBuf)
			if err != nil {
				return nil, err
			}
			return newReader(d.r), nil
		case rdbOpCodeModuleAux:

		default:
			key, err := d.readString()
			if err != nil {
				return nil, err
			}
			err = d.readObject(key, ValueType(objType), expiry)
			if err != nil {
				return nil, err
			}
			_, _ = lfuFreq, lruIdle
			expiry = 0
			lfuFreq = 0
			lruIdle = 0
		}
	}
}

func (d *rdbDecode) readObject(key []byte, typ ValueType, expiry int64) error {
	switch typ {
	case TypeString:
		value, err := d.readString()
		if err != nil {
			return err
		}
		d.event.Set(key, value, expiry)
	case TypeList:
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.BeginList(key, int64(length), expiry)
		for length > 0 {
			length--
			value, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Rpush(key, value)
		}
		d.event.EndList(key)
	case TypeListQuicklist:
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.BeginList(key, int64(-1), expiry)
		for length > 0 {
			length--
			if err := d.readZiplist(key, 0, false); err != nil {
				panic(err)
			}
		}
		d.event.EndList(key)
	case TypeSet:
		cardinality, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.BeginSet(key, int64(cardinality), expiry)
		for cardinality > 0 {
			cardinality--
			member, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Sadd(key, member)
		}
		d.event.EndSet(key)
	case TypeZSet2:
		fallthrough
	case TypeZSet:
		cardinality, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.BeginZSet(key, int64(cardinality), expiry)
		for cardinality > 0 {
			cardinality--
			member, err := d.readString()
			if err != nil {
				return err
			}
			var score float64
			if typ == TypeZSet2 {
				score, err = d.readBinaryFloat64()
				if err != nil {
					return err
				}
			} else {
				score, err = d.readFloat64()
				if err != nil {
					return err
				}
			}
			d.event.Zadd(key, score, member)
		}
		d.event.EndZSet(key)
	case TypeHash:
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.BeginHash(key, int64(length), expiry)
		for length > 0 {
			length--
			field, err := d.readString()
			if err != nil {
				return err
			}
			value, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Hset(key, field, value)
		}
		d.event.EndHash(key)
	case TypeHashZipmap:
		return d.readZipmap(key, expiry)
	case TypeListZiplist:
		return d.readZiplist(key, expiry, true)
	case TypeSetIntset:
		return d.readIntset(key, expiry)
	case TypeZSetZiplist:
		return d.readZiplistZset(key, expiry)
	case TypeHashZiplist:
		return d.readZiplistHash(key, expiry)
	case TypeStreamListPacks:
		return d.readStream(key, expiry)
	case TypeModule:
		fallthrough
	case TypeModule2:
		return d.readModule(key, expiry)
	default:
		return fmt.Errorf("rdb: unknown object type %d for key %s", typ, key)
	}
	return nil
}

func (d *rdbDecode) readModule(key []byte, expiry int64) error {
	moduleid, _, err := d.readLength()
	if err != nil {
		return err
	}
	return fmt.Errorf("Not supported load module %v", moduleid)
}

func (d *rdbDecode) readStreamID() (uint64, uint64, error) {
	entrys, err := d.readString()
	if err != nil {
		return 0, 0, err
	}
	slb := newSliceBuffer(entrys)
	ms, err := slb.Slice(8)
	if err != nil {
		return 0, 0, err
	}
	seq, err := slb.Slice(8)
	if err != nil {
		return 0, 0, err
	}
	return binary.BigEndian.Uint64(ms), binary.BigEndian.Uint64(seq), nil
}

func (d *rdbDecode) readStream(key []byte, expiry int64) error {
	cardinality, _, err := d.readLength()
	if err != nil {
		return err
	}
	d.event.BeginStream(key, int64(cardinality), expiry)

	data := make([]byte, 0)

	for cardinality > 0 {
		cardinality--

		epoch, sequence, err := d.readStreamID()
		if err != nil {
			return err
		}

		_ = sequence
		lpData, err := d.readString()
		if err != nil {
			return err
		}
		listpack := newSliceBuffer(lpData)

		// skip
		// total-bytes 4
		listpack.Skip(4)
		// num-elements 2
		listpack.Skip(2)

		/*
		 * Master entry
		 * +-------+---------+------------+---------+--/--+---------+---------+-+
		 * | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
		 * +-------+---------+------------+---------+--/--+---------+---------+-+
		 */

		// count
		b, err := readListPackV2(listpack)
		if err != nil {
			return err
		}
		count, err := readuInt(b)
		if err != nil {
			return err
		}

		// deleted
		b, err = readListPackV2(listpack)
		if err != nil {
			return err
		}
		deleted, err := readuInt(b)
		if err != nil {
			return err
		}

		// num_field
		b, err = readListPackV2(listpack)
		if err != nil {
			return err
		}
		num_fields, err := readuInt(b)
		if err != nil {
			return err
		}

		tempFileds := make([][]byte, num_fields)
		for i := uint64(0); i < num_fields; i++ {
			b, err := readListPackV2(listpack)
			if err != nil {
				return err
			}
			tempFileds[i] = b
		}
		if _, err := readListPackV2(listpack); err != nil {
			panic(err)
		}

		_, _, _ = count, deleted, num_fields
		total := count + deleted
		for total > 0 {
			total--
			flag, err := readListPackV2(listpack)
			if err != nil {
				return err
			}
			flagInt, err := readuInt(flag)
			if err != nil {
				return err
			}

			ms, err := readListPackV2(listpack)
			if err != nil {
				return err
			}
			_ms, err := readuInt(ms)
			if err != nil {
				return err
			}

			seq, err := readListPackV2(listpack)
			if err != nil {
				return err
			}
			_seq, err := readuInt(seq)
			if err != nil {
				return err
			}

			epoch += _ms
			sequence += _seq

			delete := false
			if (int(flagInt) & rdbStreamItemFlagNone) != 0 {
				delete = false
			}
			_ = delete
			if (flagInt & rdbStreamItemFlangSameFields) != 0 {
				/*
				* SAMEFIELD
				* +-------+-/-+-------+--------+
				* |value-1|...|value-N|lp-count|
				* +-------+-/-+-------+--------+
				 */

				for i := 0; i < int(num_fields); i++ {
					data = append(data, tempFileds[i]...)
					data = append(data, ' ')
					value, err := readListPackV2(listpack)
					if err != nil {
						return err
					}
					data = append(data, value...)
					data = append(data, ' ')
				}
				d.event.Xadd(key, []byte(fmt.Sprintf("%d-%d", epoch, sequence)), data[0:len(data)-1])
				data = data[:0]
			} else {
				/*
				 * NONEFIELD
				 * +----------+-------+-------+-/-+-------+-------+--------+
				 * |num-fields|field-1|value-1|...|field-N|value-N|lp-count|
				 * +----------+-------+-------+-/-+-------+-------+--------+
				 */
				_numfields, err := readListPackV2(listpack)
				if err != nil {
					return err
				}
				cnt, err := readuInt(_numfields)
				if err != nil {
					return err
				}
				for i := uint64(0); i < cnt; i++ {
					field, err := readListPackV2(listpack)
					if err != nil {
						return err
					}
					data = append(data, field...)
					data = append(data, ' ')

					value, err := readListPackV2(listpack)
					if err != nil {
						return err
					}
					data = append(data, value...)
					data = append(data, ' ')

				}
				d.event.Xadd(key, []byte(fmt.Sprintf("%d-%d", epoch+_ms, sequence)), data[0:len(data)-1])
				data = data[:0]
			}
			if _, err := readListPackV2(listpack); err != nil {
				panic(err)
			} // lp-count
		}

		eb, err := listpack.ReadByte() // lp-end
		if err != nil {
			return err
		}
		if eb != rdbLpEOF {
			return errors.New("rdb Lp eof unexpected.")
		}

	}

	for i := 0; i < 3; i++ {
		_, _, err = d.readLength() // items , last_id, last_seq
		if err != nil {
			return err
		}
	}

	//TODO output consumer groups
	groupsCount, _, err := d.readLength()
	if err != nil {
		return err
	}

	for groupsCount > 0 {
		groupsCount--
		cgname, err := d.readString()
		if err != nil {
			return err
		}
		gIDms, _, err := d.readLength()
		if err != nil {
			return err
		}
		gIDseq, _, err := d.readLength()
		if err != nil {
			return err
		}
		_, _, _ = cgname, gIDms, gIDseq
		// fmt.Printf("cgname=%s last_cg_entry_id %d-%d\n", cgname, gIDms, gIDseq)

		pelSize, _, err := d.readLength()
		if err != nil {
			return err
		}
		for pelSize > 0 {
			pelSize--
			eid := make([]byte, 16) //deliveryTime 8 byte deliveryCount 8 byte
			_, err := io.ReadFull(d.r, eid)
			if err != nil {
				return err
			}
		}

		consumersNum, _, err := d.readLength()
		if err != nil {
			return err
		}
		for consumersNum > 0 {
			consumersNum--
			_, err = d.readString() // cname
			if err != nil {
				return err
			}
			pelSize, _, err := d.readLength() // pending
			if err != nil {
				return err
			}
			for pelSize > 0 {
				pelSize--
				rawid := make([]byte, 16) //eid
				_, err = io.ReadFull(d.r, rawid)
				if err != nil {
					return err
				}
			}
		}
	}
	d.event.EndStream(key)

	return nil
}

func (d *rdbDecode) readZipmap(key []byte, expiry int64) error {
	var length int
	zipmap, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(zipmap)
	lenByte, err := buf.ReadByte()
	if err != nil {
		return err
	}
	if lenByte >= 254 { // we need to count the items manually
		length, err = countZipmapItems(buf)
		length /= 2
		if err != nil {
			return err
		}
	} else {
		length = int(lenByte)
	}
	d.event.BeginHash(key, int64(length), expiry)
	for i := 0; i < length; i++ {
		field, err := readZipmapItem(buf, false)
		if err != nil {
			return err
		}
		value, err := readZipmapItem(buf, true)
		if err != nil {
			return err
		}
		d.event.Hset(key, field, value)
	}
	d.event.EndHash(key)
	return nil
}

func readZipmapItem(buf *sliceBuffer, readFree bool) ([]byte, error) {
	length, free, err := readZipmapItemLength(buf, readFree)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	value, err := buf.Slice(length)
	if err != nil {
		return nil, err
	}
	_, err = buf.Seek(int64(free), 1)
	return value, err
}

func countZipmapItems(buf *sliceBuffer) (int, error) {
	n := 0
	for {
		strLen, free, err := readZipmapItemLength(buf, n%2 != 0)
		if err != nil {
			return 0, err
		}
		if strLen == -1 {
			break
		}
		_, err = buf.Seek(int64(strLen)+int64(free), 1)
		if err != nil {
			return 0, err
		}
		n++
	}
	_, err := buf.Seek(0, 0)
	return n, err
}

func readZipmapItemLength(buf *sliceBuffer, readFree bool) (int, int, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	switch b {
	case 253:
		s, err := buf.Slice(5)
		if err != nil {
			return 0, 0, err
		}
		return int(binary.BigEndian.Uint32(s)), int(s[4]), nil
	case 254:
		return 0, 0, fmt.Errorf("rdb: invalid zipmap item length")
	case 255:
		return -1, 0, nil
	}
	var free byte
	if readFree {
		free, err = buf.ReadByte()
	}
	return int(b), int(free), err
}

/*
 * <encoding-type> <element-data> <element-tot-len>
 *
 * <encoding-type> :
 * |0xxxxxxx| 7 bit unsigned integer
 * |10xxxxxx| 6 bit unsignedinteger as string length. then read the `length` bytes as string.
 * |110xxxxx|xxxxxxxx| 13 bit signed integer |1110xxxx|xxxxxxxx| string with length up to 4095
 * |11110001|xxxxxxxx|xxxxxxxx| next 2 bytes as 16bit int
 * |11110010|xxxxxxxx|xxxxxxxx|xxxxxxxx| next 3 bytes as 24bit int
 * |11110011|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx| next 4 bytes as 32bit int
 * |11110100|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx| xxxxxxxx| next 8 bytes as 64bit long
 * |11110000|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx| next 4 bytes as string length.
 * then read the `length` bytes as string.
 *
 * <element-data> : TBD
 *
 * <element-tot-len> : TBD
 */

func readListPackV2(slice *sliceBuffer) (value []byte, err error) {
	var special byte
	if special, err = slice.ReadByte(); err != nil {
		return nil, err
	}
	if (special & rdbLpEncoding7BitUintMask) == rdbLpEncoding7BitUint { // mask 128 -> 0xxx xxxx
		slice.Skip(1)
		value = i642bytes((uint64(special) & 0x7f)) //  0x7f 127

	} else if (special & rdbLpEncoding6BitStrMask) == rdbLpEncoding6BitStr { // mask 192 -> 00xx xxxx
		len := special & 0x3f // 0x3f  63
		skip := 1 + int(len)
		value, err = slice.First(int(len))
		if err != nil {
			return nil, err
		}
		slice.Skip(int(skip))

	} else if (special & rdbLpEncoding13BitIntMask) == rdbLpEncoding13BitInt { // mask 224 -> 000x xxxx
		next, err := slice.ReadByte()
		if err != nil {
			return nil, err
		}
		value = i642bytes(((uint64(special&0x1f) << 8) | uint64(next)))
		slice.Skip(2)

	} else if (special & rdbLpEncoding16BitIntMask) == rdbLpEncoding16BitInt { // mask 255
		value, err = slice.First(2)
		if err != nil {
			return nil, err
		}
		slice.Skip(3)

	} else if (special & rdbLpEncoding24BitIntMask) == rdbLpEncoding24BitInt {
		value, err = slice.First(3)
		if err != nil {
			return nil, err
		}
		slice.Skip(4)

	} else if (special & rdbLpEncoding32BitIntMask) == rdbLpEncoding32BitInt {
		value, err = slice.First(4)
		if err != nil {
			return nil, err
		}
		slice.Skip(5)

	} else if (special & rdbLpEncoding64BitIntMask) == rdbLpEncoding64BitInt {
		value, err = slice.First(8)
		if err != nil {
			return nil, err
		}
		slice.Skip(9)

	} else if (special & rdbLpEncoding12BitStrMask) == rdbLpEncoding12BitStr {
		b, err := slice.ReadByte()
		if err != nil {
			return nil, err
		}
		len := ((uint64(special) & 0x0f) << 8) | uint64(b)
		value, err = slice.First(int(len))
		if err != nil {
			return nil, err
		}
		slice.Skip(2 + int(len))

	} else if (special & rdbLpEncoding32BitStrMask) == rdbLpEncoding32BitStr {
		len, err := slice.First(4)
		if err != nil {
			return nil, err
		}
		realLength, err := readuInt(len)
		if err != nil {
			return nil, err
		}
		value, err = slice.First(int(realLength))
		if err != nil {
			return nil, err
		}
		slice.Skip(5 + int(realLength))

	} else {
		return nil, fmt.Errorf("Unsupported operation exception %q", special)
	}
	// <element-tot-len>

	return value, nil
}

func readuInt(intBytes []byte) (uint64, error) {
	if len(intBytes) == 3 {
		intBytes = append([]byte{0}, intBytes...)
	}
	bytesBuffer := bytes.NewBuffer(intBytes)
	switch len(intBytes) {
	case 1:
		var tmp uint8
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return uint64(tmp), err
	case 2:
		var tmp uint16
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return uint64(tmp), err
	case 4:
		var tmp uint32
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return uint64(tmp), err
	case 8:
		var tmp uint64
		err := binary.Read(bytesBuffer, binary.BigEndian, &tmp)
		return uint64(tmp), err
	default:
		return 0, errors.New("BytesToInt bytes lenth is invaild!")
	}
}

func i642bytes(i uint64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func (d *rdbDecode) readZiplist(key []byte, expiry int64, addListEvents bool) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	length, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	if addListEvents {
		d.event.BeginList(key, length, expiry)
	}
	for i := int64(0); i < length; i++ {
		entry, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		d.event.Rpush(key, entry)
	}
	if addListEvents {
		d.event.EndList(key)
	}
	return nil
}

func (d *rdbDecode) readZiplistZset(key []byte, expiry int64) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	cardinality, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	cardinality /= 2
	d.event.BeginZSet(key, cardinality, expiry)
	for i := int64(0); i < cardinality; i++ {
		member, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		scoreBytes, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		score, err := strconv.ParseFloat(string(scoreBytes), 64)
		if err != nil {
			return err
		}
		d.event.Zadd(key, score, member)
	}
	d.event.EndZSet(key)
	return nil
}

func (d *rdbDecode) readZiplistHash(key []byte, expiry int64) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	length, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	length /= 2
	d.event.BeginHash(key, length, expiry)
	for i := int64(0); i < length; i++ {
		field, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		value, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		d.event.Hset(key, field, value)
	}
	d.event.EndHash(key)
	return nil
}

func readZiplistLength(buf *sliceBuffer) (int64, error) {
	if _, err := buf.Seek(8, 0); err != nil {
		return 0, err
	} // skip the zlbytes and zltail
	lenBytes, err := buf.Slice(2)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint16(lenBytes)), nil
}

func readZiplistEntry(buf *sliceBuffer) ([]byte, error) {
	prevLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	if prevLen == 254 {
		if _, err := buf.Seek(4, 1); err != nil {
			return nil, err
		} // skip the 4-byte prevlen
	}

	header, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	switch {
	case header>>6 == rdbZiplist6bitlenString:
		return buf.Slice(int(header & 0x3f))
	case header>>6 == rdbZiplist14bitlenString:
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		return buf.Slice((int(header&0x3f) << 8) | int(b))
	case header>>6 == rdbZiplist32bitlenString:
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return buf.Slice(int(binary.BigEndian.Uint32(lenBytes)))
	case header == rdbZiplistInt16:
		intBytes, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)), nil
	case header == rdbZiplistInt32:
		intBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)), nil
	case header == rdbZiplistInt64:
		intBytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(intBytes)), 10)), nil
	case header == rdbZiplistInt24:
		intBytes := make([]byte, 4)
		_, err := buf.Read(intBytes[1:])
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))>>8), 10)), nil
	case header == rdbZiplistInt8:
		b, err := buf.ReadByte()
		return []byte(strconv.FormatInt(int64(int8(b)), 10)), err
	case header>>4 == rdbZiplistInt4:
		return []byte(strconv.FormatInt(int64(header&0x0f)-1, 10)), nil
	}

	return nil, fmt.Errorf("rdb: unknown ziplist header byte: %d", header)
}

func (d *rdbDecode) readIntset(key []byte, expiry int64) error {
	intset, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(intset)
	intSizeBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	intSize := binary.LittleEndian.Uint32(intSizeBytes)

	if intSize != 2 && intSize != 4 && intSize != 8 {
		return fmt.Errorf("rdb: unknown intset encoding: %d", intSize)
	}

	lenBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	cardinality := binary.LittleEndian.Uint32(lenBytes)

	d.event.BeginSet(key, int64(cardinality), expiry)
	for i := uint32(0); i < cardinality; i++ {
		intBytes, err := buf.Slice(int(intSize))
		if err != nil {
			return err
		}
		var intString string
		switch intSize {
		case 2:
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
		}
		d.event.Sadd(key, []byte(intString))
	}
	d.event.EndSet(key)
	return nil
}

func (d *rdbDecode) checkHeader(file bool) error {
	for {
		b, err := d.r.ReadByte()
		if err != nil {
			return err
		}
		if b == 'R' {
			if err := d.r.UnreadByte(); err != nil {
				return err
			}
			break
		}

	}
	header := make([]byte, 9)
	_, err := io.ReadFull(d.r, header)
	if err != nil {
		return err
	}

	if !bytes.Equal(header[:5], []byte("REDIS")) {
		return fmt.Errorf("rdb: invalid file format,header %s", header)
	}

	version, _ := strconv.ParseInt(string(header[5:]), 10, 64)
	if version < 1 || version > rdbVersion {
		return fmt.Errorf("rdb: invalid RDB version number %d", version)
	}

	return nil
}

func (d *rdbDecode) readString() ([]byte, error) {
	length, encoded, err := d.readLength()
	if err != nil {
		return nil, err
	}
	if encoded {
		switch length {
		case rdbEncInt8:
			i, err := d.readUint8()
			return []byte(strconv.FormatInt(int64(int8(i)), 10)), err
		case rdbEncInt16:
			i, err := d.readUint16()
			return []byte(strconv.FormatInt(int64(int16(i)), 10)), err
		case rdbEncInt32:
			i, err := d.readUint32()
			return []byte(strconv.FormatInt(int64(int32(i)), 10)), err
		case rdbEncLZF:
			clen, _, err := d.readLength()
			if err != nil {
				return nil, err
			}
			ulen, _, err := d.readLength()
			if err != nil {
				return nil, err
			}
			compressed := make([]byte, clen)
			_, err = io.ReadFull(d.r, compressed)
			if err != nil {
				return nil, err
			}
			decompressed := lzfDecompress(compressed, int(ulen))
			if len(decompressed) != int(ulen) {
				return nil,
					fmt.Errorf("decompressed string length %d didn't match expected length %d",
						len(decompressed),
						ulen,
					)
			}
			return decompressed, nil
		}
	}

	str := make([]byte, length)
	_, err = io.ReadFull(d.r, str)
	return str, err
}

func (d *rdbDecode) readUint8() (uint8, error) {
	b, err := d.r.ReadByte()
	return uint8(b), err
}

func (d *rdbDecode) readUint16() (uint16, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:2])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(d.intBuf), nil
}

func (d *rdbDecode) readUint32() (uint32, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:4])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(d.intBuf), nil
}

func (d *rdbDecode) readUint64() (uint64, error) {
	_, err := io.ReadFull(d.r, d.intBuf)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(d.intBuf), nil
}

func (d *rdbDecode) readUint32Big() (uint32, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:4])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(d.intBuf), nil
}

func (d *rdbDecode) readBinaryFloat64() (float64, error) {
	floatBytes := make([]byte, 8)
	_, err := io.ReadFull(d.r, floatBytes)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(floatBytes)), nil
}

// Doubles are saved as strings prefixed by an unsigned
// 8 bit integer specifying the length of the representation.
// This 8 bit integer has special values in order to specify the following
// conditions:
// 253: not a number
// 254: + inf
// 255: - inf
func (d *rdbDecode) readFloat64() (float64, error) {
	length, err := d.readUint8()
	if err != nil {
		return 0, err
	}
	switch length {
	case 253:
		return math.NaN(), nil
	case 254:
		return math.Inf(0), nil
	case 255:
		return math.Inf(-1), nil
	default:
		floatBytes := make([]byte, length)
		_, err := io.ReadFull(d.r, floatBytes)
		if err != nil {
			return 0, err
		}
		f, err := strconv.ParseFloat(string(floatBytes), 64)
		return f, err
	}
}

func (d *rdbDecode) readLength() (uint64, bool, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return 0, false, err
	}
	// The first two bits of the first byte are used to indicate the length encoding type
	switch (b & 0xc0) >> 6 {
	case rdb6bitLen:
		// When the first two bits are 00, the next 6 bits are the length.
		return uint64(b & 0x3f), false, nil
	case rdb14bitLen:
		// When the first two bits are 01, the next 14 bits are the length.
		bb, err := d.r.ReadByte()
		if err != nil {
			return 0, false, err
		}
		return (uint64(b&0x3f) << 8) | uint64(bb), false, nil
	case rdb32bitLen:
		bb, err := d.readUint32()
		if err != nil {
			return 0, false, err
		}
		return uint64(bb), false, nil
	case rdb64bitLen:
		bb, err := d.readUint64()
		if err != nil {
			return 0, false, err
		}
		return bb, false, nil
	case rdbEncVal:
		// When the first two bits are 11, the next object is encoded.
		// The next 6 bits indicate the encoding type.
		return uint64(b & 0x3f), true, nil
	default:
		// When the first two bits are 10, the next 6 bits are discarded.
		// The next 4 bytes are the length.
		length, err := d.readUint32Big()
		return uint64(length), false, err
	}

}

//func verifyDump(d []byte) error {
//	if len(d) < 10 {
//		return fmt.Errorf("rdb: invalid dump length")
//	}
//	version := binary.LittleEndian.Uint16(d[len(d)-10:])
//	if version > uint16(rdbVersion) {
//		return fmt.Errorf("rdb: invalid version %d, expecting %d", version, rdbVersion)
//	}
//
//	if binary.LittleEndian.Uint64(d[len(d)-8:]) != Digest(d[:len(d)-8]) {
//		return fmt.Errorf("rdb: invalid CRC checksum")
//	}
//
//	return nil
//}

func lzfDecompress(in []byte, outlen int) []byte {
	out := make([]byte, outlen)
	for i, o := 0, 0; i < len(in); {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			for x := 0; x <= ctrl; x++ {
				out[o] = in[i]
				i++
				o++
			}
		} else {
			length := ctrl >> 5
			if length == 7 {
				length = length + int(in[i])
				i++
			}
			ref := o - ((ctrl & 0x1f) << 8) - int(in[i]) - 1
			i++
			for x := 0; x <= length+1; x++ {
				out[o] = out[ref]
				ref++
				o++
			}
		}
	}
	return out
}
