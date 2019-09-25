package canal

import (
	"io"
)

type ByteReader interface {
	io.Reader
	// io.ByteReader
	io.ByteScanner
}

type OffsetHandler interface {
	Increment(int64)
	Offset() string
}

type CommandDecoder interface {
	Command(cmd *Command) error
}

// A Decodr must be implemented to parse a RDB io.Reader &  parse a command io.Reader
type Decoder interface {
	// BeginDatabase is called when database n Begins.
	// Once a database Begins, another database will not Begin until EndDatabase is called.
	BeginDatabase(n int)
	// Set is called once for each string key.
	Set(key, value []byte, expiry int64)
	// BeginHash is called at the beginning of a hash.
	// Hset will be called exactly length times before EndHash.
	BeginHash(key []byte, length, expiry int64)
	// Hset is called once for each field=value pair in a hash.
	Hset(key, field, value []byte)
	// EndHash is called when there are no more fields in a hash.
	EndHash(key []byte)

	// BeginSet is called at the beginning of a set.
	// Sadd will be called exactly cardinality times before EndSet.
	BeginSet(key []byte, cardinality, expiry int64)
	// Sadd is called once for each member of a set.
	Sadd(key, member []byte)
	// EndSet is called when there are no more fields in a set.
	EndSet(key []byte)

	// BeginStream is called at the beginning of a stream.
	// Xadd will be called exactly length times before EndStream.
	BeginStream(key []byte, cardinality, expiry int64)
	// Xadd is called once for each id in a stream.
	Xadd(key, streamID, listpack []byte)
	// EndHash is called when there are no more fields in a hash.
	EndStream(key []byte)

	// BeginList is called at the beginning of a list.
	// Rpush will be called exactly length times before EndList.
	// If length of the list is not known, then length is -1
	BeginList(key []byte, length, expiry int64)
	// Rpush is called once for each value in a list.
	Rpush(key, value []byte)
	// EndList is called when there are no more values in a list.
	EndList(key []byte)

	// BeginZSet is called at the beginning of a sorted set.
	// Zadd will be called exactly cardinality times before EndZSet.
	BeginZSet(key []byte, cardinality, expiry int64)

	// Zadd is called once for each member of a sorted set.
	Zadd(key []byte, score float64, member []byte)

	// EndZSet is called when there are no more members in a sorted set.
	EndZSet(key []byte)

	// EndDatabase is called at the end of a database.
	EndDatabase(n int)

	RDBDecoder
}

type Closer interface {
	io.Closer
}

type RDBDecoder interface {
	// BeginRDB is called when parsing of a valid RDB file Begins.
	BeginRDB()
	// EndRDB is called when parsing of the RDB file is complete.
	EndRDB()
	// AUX field
	Aux(key, value []byte)
	// ResizeDB hint
	ResizeDatabase(dbSize, expiresSize uint32)
}

// Nop may be embedded in a real Decoder to avoid implementing methods.
type Nop struct{}

func (d Nop) BeginRDB()                                         {}
func (d Nop) BeginDatabase(n int)                               {}
func (d Nop) Aux(key, value []byte)                             {}
func (d Nop) ResizeDatabase(dbSize, expiresSize uint32)         {}
func (d Nop) EndDatabase(n int)                                 {}
func (d Nop) EndRDB()                                           {}
func (d Nop) Set(key, value []byte, expiry int64)               {}
func (d Nop) BeginHash(key []byte, length, expiry int64)        {}
func (d Nop) Hset(key, field, value []byte)                     {}
func (d Nop) EndHash(key []byte)                                {}
func (d Nop) BeginSet(key []byte, cardinality, expiry int64)    {}
func (d Nop) Sadd(key, member []byte)                           {}
func (d Nop) EndSet(key []byte)                                 {}
func (d Nop) BeginList(key []byte, length, expiry int64)        {}
func (d Nop) Rpush(key, value []byte)                           {}
func (d Nop) EndList(key []byte)                                {}
func (d Nop) BeginZSet(key []byte, cardinality, expiry int64)   {}
func (d Nop) Zadd(key []byte, score float64, member []byte)     {}
func (d Nop) EndZSet(key []byte)                                {}
func (d Nop) BeginStream(key []byte, cardinality, expiry int64) {}
func (d Nop) Xadd(key, id, listpack []byte)                     {}
func (d Nop) EndStream(key []byte)                              {}
