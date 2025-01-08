/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"runtime"
	"strings"
	"time"

	"github.com/gocql/gocql/internal"
)

// UnsetValue represents a value used in a query binding that will be ignored by Cassandra.
//
// By setting a field to the unset value Cassandra will ignore the write completely.
// The main advantage is the ability to keep the same prepared statement even when you don't
// want to update some fields, where before you needed to make another prepared statement.
//
// UnsetValue is only available when using the version 4 of the protocol.
var UnsetValue = internal.UnsetColumn{}

// NamedValue produce a value which will bind to the named parameter in a query
func NamedValue(name string, value interface{}) interface{} {
	return &internal.NamedValue{
		Name:  name,
		Value: value,
	}
}

//TODO: We should move protoVersion, frameOp, proto version etc. to internal

type Consistency uint16

const (
	Any         Consistency = 0x00
	One         Consistency = 0x01
	Two         Consistency = 0x02
	Three       Consistency = 0x03
	Quorum      Consistency = 0x04
	All         Consistency = 0x05
	LocalQuorum Consistency = 0x06
	EachQuorum  Consistency = 0x07
	LocalOne    Consistency = 0x0A
)

func (c Consistency) String() string {
	switch c {
	case Any:
		return "ANY"
	case One:
		return "ONE"
	case Two:
		return "TWO"
	case Three:
		return "THREE"
	case Quorum:
		return "QUORUM"
	case All:
		return "ALL"
	case LocalQuorum:
		return "LOCAL_QUORUM"
	case EachQuorum:
		return "EACH_QUORUM"
	case LocalOne:
		return "LOCAL_ONE"
	default:
		return fmt.Sprintf("UNKNOWN_CONS_0x%x", uint16(c))
	}
}

func (c Consistency) MarshalText() (text []byte, err error) {
	return []byte(c.String()), nil
}

func (c *Consistency) UnmarshalText(text []byte) error {
	switch string(text) {
	case "ANY":
		*c = Any
	case "ONE":
		*c = One
	case "TWO":
		*c = Two
	case "THREE":
		*c = Three
	case "QUORUM":
		*c = Quorum
	case "ALL":
		*c = All
	case "LOCAL_QUORUM":
		*c = LocalQuorum
	case "EACH_QUORUM":
		*c = EachQuorum
	case "LOCAL_ONE":
		*c = LocalOne
	default:
		return fmt.Errorf("invalid consistency %q", string(text))
	}

	return nil
}

func ParseConsistency(s string) Consistency {
	var c Consistency
	if err := c.UnmarshalText([]byte(strings.ToUpper(s))); err != nil {
		panic(err)
	}
	return c
}

// ParseConsistencyWrapper wraps gocql.ParseConsistency to provide an err
// return instead of a panic
func ParseConsistencyWrapper(s string) (consistency Consistency, err error) {
	err = consistency.UnmarshalText([]byte(strings.ToUpper(s)))
	return
}

// MustParseConsistency is the same as ParseConsistency except it returns
// an error (never). It is kept here since breaking changes are not good.
// DEPRECATED: use ParseConsistency if you want a panic on parse error.
func MustParseConsistency(s string) (Consistency, error) {
	c, err := ParseConsistencyWrapper(s)
	if err != nil {
		panic(err)
	}
	return c, nil
}

type SerialConsistency uint16

const (
	Serial      SerialConsistency = 0x08
	LocalSerial SerialConsistency = 0x09
)

func (s SerialConsistency) String() string {
	switch s {
	case Serial:
		return "SERIAL"
	case LocalSerial:
		return "LOCAL_SERIAL"
	default:
		return fmt.Sprintf("UNKNOWN_SERIAL_CONS_0x%x", uint16(s))
	}
}

func (s SerialConsistency) MarshalText() (text []byte, err error) {
	return []byte(s.String()), nil
}

func (s *SerialConsistency) UnmarshalText(text []byte) error {
	switch string(text) {
	case "SERIAL":
		*s = Serial
	case "LOCAL_SERIAL":
		*s = LocalSerial
	default:
		return fmt.Errorf("invalid consistency %q", string(text))
	}

	return nil
}

var (
	ErrFrameTooBig = errors.New("frame length is bigger than the maximum allowed")
)

type ObservedFrameHeader struct {
	Version internal.ProtoVersion
	Flags   byte
	Stream  int16
	Opcode  internal.FrameOp
	Length  int32

	// StartHeader is the time we started reading the frame header off the network connection.
	Start time.Time
	// EndHeader is the time we finished reading the frame header off the network connection.
	End time.Time

	// Host is Host of the connection the frame header was read from.
	Host *HostInfo
}

func (f ObservedFrameHeader) String() string {
	return fmt.Sprintf("[observed header version=%s flags=0x%x stream=%d op=%s length=%d]", f.Version, f.Flags, f.Stream, f.Opcode, f.Length)
}

// FrameHeaderObserver is the interface implemented by frame observers / stat collectors.
//
// Experimental, this interface and use may change
type FrameHeaderObserver interface {
	// ObserveFrameHeader gets called on every received frame header.
	ObserveFrameHeader(context.Context, ObservedFrameHeader)
}

// a framer is responsible for reading, writing and parsing frames on a single stream
type framer struct {
	proto byte
	// flags are for outgoing flags, enabling compression and tracing etc
	flags    byte
	compres  Compressor
	headSize int
	// if this frame was read then the header will be here
	header *internal.FrameHeader

	// if tracing flag is set this is not nil
	traceID []byte

	// holds a ref to the whole byte slice for buf so that it can be reset to
	// 0 after a read.
	readBuffer []byte

	buf []byte

	customPayload map[string][]byte
}

func newFramer(compressor Compressor, version byte) *framer {
	buf := make([]byte, internal.DefaultBufSize)
	f := &framer{
		buf:        buf[:0],
		readBuffer: buf,
	}
	var flags byte
	if compressor != nil {
		flags |= internal.FlagCompress
	}
	if version == internal.ProtoVersion5 {
		flags |= internal.FlagBetaProtocol
	}

	version &= internal.ProtoVersionMask

	headSize := 8
	if version > internal.ProtoVersion2 {
		headSize = 9
	}

	f.compres = compressor
	f.proto = version
	f.flags = flags
	f.headSize = headSize

	f.header = nil
	f.traceID = nil

	return f
}

func readHeader(r io.Reader, p []byte) (head internal.FrameHeader, err error) {
	_, err = io.ReadFull(r, p[:1])
	if err != nil {
		return internal.FrameHeader{}, err
	}

	version := p[0] & internal.ProtoVersionMask

	if version < internal.ProtoVersion1 || version > internal.ProtoVersion5 {
		return internal.FrameHeader{}, fmt.Errorf("gocql: unsupported protocol response version: %d", version)
	}

	headSize := 9
	if version < internal.ProtoVersion3 {
		headSize = 8
	}

	_, err = io.ReadFull(r, p[1:headSize])
	if err != nil {
		return internal.FrameHeader{}, err
	}

	p = p[:headSize]

	head.Version = internal.ProtoVersion(p[0])
	head.Flags = p[1]

	if version > internal.ProtoVersion2 {
		if len(p) != 9 {
			return internal.FrameHeader{}, fmt.Errorf("not enough bytes to read header require 9 got: %d", len(p))
		}

		head.Stream = int(int16(p[2])<<8 | int16(p[3]))
		head.Op = internal.FrameOp(p[4])
		head.Length = int(internal.ReadInt(p[5:]))
	} else {
		if len(p) != 8 {
			return internal.FrameHeader{}, fmt.Errorf("not enough bytes to read header require 8 got: %d", len(p))
		}

		head.Stream = int(int8(p[2]))
		head.Op = internal.FrameOp(p[3])
		head.Length = int(internal.ReadInt(p[4:]))
	}

	return head, nil
}

// explicitly enables tracing for the framers outgoing requests
func (f *framer) trace() {
	f.flags |= internal.FlagTracing
}

// explicitly enables the custom payload flag
func (f *framer) payload() {
	f.flags |= internal.FlagCustomPayload
}

// reads a frame form the wire into the framers buffer
func (f *framer) readFrame(r io.Reader, head *internal.FrameHeader) error {
	if head.Length < 0 {
		return fmt.Errorf("frame body length can not be less than 0: %d", head.Length)
	} else if head.Length > internal.MaxFrameSize {
		// need to free up the connection to be used again
		_, err := io.CopyN(ioutil.Discard, r, int64(head.Length))
		if err != nil {
			return fmt.Errorf("error whilst trying to discard frame with invalid length: %v", err)
		}
		return ErrFrameTooBig
	}

	if cap(f.readBuffer) >= head.Length {
		f.buf = f.readBuffer[:head.Length]
	} else {
		f.readBuffer = make([]byte, head.Length)
		f.buf = f.readBuffer
	}

	// assume the underlying reader takes care of timeouts and retries
	n, err := io.ReadFull(r, f.buf)
	if err != nil {
		return fmt.Errorf("unable to read frame body: read %d/%d bytes: %v", n, head.Length, err)
	}

	if head.Flags&internal.FlagCompress == internal.FlagCompress {
		if f.compres == nil {
			return NewErrProtocol("no compressor available with compressed frame body")
		}

		f.buf, err = f.compres.Decode(f.buf)
		if err != nil {
			return err
		}
	}

	f.header = head
	return nil
}

func (f *framer) parseFrame() (frame internal.Frame, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()

	if f.header.Version.Request() {
		return nil, NewErrProtocol("got a request frame from server: %v", f.header.Version)
	}

	if f.header.Flags&internal.FlagTracing == internal.FlagTracing {
		f.readTrace()
	}

	if f.header.Flags&internal.FlagWarning == internal.FlagWarning {
		f.header.Warnings = f.readStringList()
	}

	if f.header.Flags&internal.FlagCustomPayload == internal.FlagCustomPayload {
		f.customPayload = f.readBytesMap()
	}

	// assumes that the frame body has been read into rbuf
	switch f.header.Op {
	case internal.OpError:
		frame = f.parseErrorFrame()
	case internal.OpReady:
		frame = f.parseReadyFrame()
	case internal.OpResult:
		frame, err = f.parseResultFrame()
	case internal.OpSupported:
		frame = f.parseSupportedFrame()
	case internal.OpAuthenticate:
		frame = f.parseAuthenticateFrame()
	case internal.OpAuthChallenge:
		frame = f.parseAuthChallengeFrame()
	case internal.OpAuthSuccess:
		frame = f.parseAuthSuccessFrame()
	case internal.OpEvent:
		frame = f.parseEventFrame()
	default:
		return nil, NewErrProtocol("unknown op in frame header: %s", f.header.Op)
	}

	return
}

func (f *framer) parseErrorFrame() internal.Frame {
	code := f.readInt()
	msg := f.readString()

	errD := errorFrame{
		FrameHeader: *f.header,
		code:        code,
		message:     msg,
	}

	switch code {
	case ErrCodeUnavailable:
		cl := f.readConsistency()
		required := f.readInt()
		alive := f.readInt()
		return &RequestErrUnavailable{
			errorFrame:  errD,
			Consistency: cl,
			Required:    required,
			Alive:       alive,
		}
	case ErrCodeWriteTimeout:
		cl := f.readConsistency()
		received := f.readInt()
		blockfor := f.readInt()
		writeType := f.readString()
		return &RequestErrWriteTimeout{
			errorFrame:  errD,
			Consistency: cl,
			Received:    received,
			BlockFor:    blockfor,
			WriteType:   writeType,
		}
	case ErrCodeReadTimeout:
		cl := f.readConsistency()
		received := f.readInt()
		blockfor := f.readInt()
		dataPresent := f.readByte()
		return &RequestErrReadTimeout{
			errorFrame:  errD,
			Consistency: cl,
			Received:    received,
			BlockFor:    blockfor,
			DataPresent: dataPresent,
		}
	case ErrCodeAlreadyExists:
		ks := f.readString()
		table := f.readString()
		return &RequestErrAlreadyExists{
			errorFrame: errD,
			Keyspace:   ks,
			Table:      table,
		}
	case ErrCodeUnprepared:
		stmtId := f.readShortBytes()
		return &RequestErrUnprepared{
			errorFrame:  errD,
			StatementId: internal.CopyBytes(stmtId), // defensively copy
		}
	case ErrCodeReadFailure:
		res := &RequestErrReadFailure{
			errorFrame: errD,
		}
		res.Consistency = f.readConsistency()
		res.Received = f.readInt()
		res.BlockFor = f.readInt()
		if f.proto > internal.ProtoVersion4 {
			res.ErrorMap = f.readErrorMap()
			res.NumFailures = len(res.ErrorMap)
		} else {
			res.NumFailures = f.readInt()
		}
		res.DataPresent = f.readByte() != 0

		return res
	case ErrCodeWriteFailure:
		res := &RequestErrWriteFailure{
			errorFrame: errD,
		}
		res.Consistency = f.readConsistency()
		res.Received = f.readInt()
		res.BlockFor = f.readInt()
		if f.proto > internal.ProtoVersion4 {
			res.ErrorMap = f.readErrorMap()
			res.NumFailures = len(res.ErrorMap)
		} else {
			res.NumFailures = f.readInt()
		}
		res.WriteType = f.readString()
		return res
	case ErrCodeFunctionFailure:
		res := &RequestErrFunctionFailure{
			errorFrame: errD,
		}
		res.Keyspace = f.readString()
		res.Function = f.readString()
		res.ArgTypes = f.readStringList()
		return res

	case ErrCodeCDCWriteFailure:
		res := &RequestErrCDCWriteFailure{
			errorFrame: errD,
		}
		return res
	case ErrCodeCASWriteUnknown:
		res := &RequestErrCASWriteUnknown{
			errorFrame: errD,
		}
		res.Consistency = f.readConsistency()
		res.Received = f.readInt()
		res.BlockFor = f.readInt()
		return res
	case ErrCodeInvalid, ErrCodeBootstrapping, ErrCodeConfig, ErrCodeCredentials, ErrCodeOverloaded,
		ErrCodeProtocol, ErrCodeServer, ErrCodeSyntax, ErrCodeTruncate, ErrCodeUnauthorized:
		// TODO(zariel): we should have some distinct types for these errors
		return errD
	default:
		panic(fmt.Errorf("unknown error code: 0x%x", errD.code))
	}
}

func (f *framer) readErrorMap() (errMap ErrorMap) {
	errMap = make(ErrorMap)
	numErrs := f.readInt()
	for i := 0; i < numErrs; i++ {
		ip := f.readInetAdressOnly().String()
		errMap[ip] = f.readShort()
	}
	return
}

func (f *framer) writeHeader(flags byte, op internal.FrameOp, stream int) {
	f.buf = f.buf[:0]
	f.buf = append(f.buf,
		f.proto,
		flags,
	)

	if f.proto > internal.ProtoVersion2 {
		f.buf = append(f.buf,
			byte(stream>>8),
			byte(stream),
		)
	} else {
		f.buf = append(f.buf,
			byte(stream),
		)
	}

	// pad out length
	f.buf = append(f.buf,
		byte(op),
		0,
		0,
		0,
		0,
	)
}

func (f *framer) setLength(length int) {
	p := 4
	if f.proto > internal.ProtoVersion2 {
		p = 5
	}

	f.buf[p+0] = byte(length >> 24)
	f.buf[p+1] = byte(length >> 16)
	f.buf[p+2] = byte(length >> 8)
	f.buf[p+3] = byte(length)
}

func (f *framer) finish() error {
	if len(f.buf) > internal.MaxFrameSize {
		// huge app frame, lets remove it so it doesn't bloat the heap
		f.buf = make([]byte, internal.DefaultBufSize)
		return ErrFrameTooBig
	}

	if f.buf[1]&internal.FlagCompress == internal.FlagCompress {
		if f.compres == nil {
			panic("compress flag set with no compressor")
		}

		// TODO: only compress frames which are big enough
		compressed, err := f.compres.Encode(f.buf[f.headSize:])
		if err != nil {
			return err
		}

		f.buf = append(f.buf[:f.headSize], compressed...)
	}
	length := len(f.buf) - f.headSize
	f.setLength(length)

	return nil
}

func (f *framer) writeTo(w io.Writer) error {
	_, err := w.Write(f.buf)
	return err
}

func (f *framer) readTrace() {
	f.traceID = f.readUUID().Bytes()
}

type readyFrame struct {
	internal.FrameHeader
}

func (f *framer) parseReadyFrame() internal.Frame {
	return &readyFrame{
		FrameHeader: *f.header,
	}
}

type supportedFrame struct {
	internal.FrameHeader

	supported map[string][]string
}

// TODO: if we move the body buffer onto the frameHeader then we only need a single
// framer, and can move the methods onto the header.
func (f *framer) parseSupportedFrame() internal.Frame {
	return &supportedFrame{
		FrameHeader: *f.header,

		supported: f.readStringMultiMap(),
	}
}

type writeStartupFrame struct {
	opts map[string]string
}

func (w writeStartupFrame) String() string {
	return fmt.Sprintf("[startup opts=%+v]", w.opts)
}

func (w *writeStartupFrame) buildFrame(f *framer, streamID int) error {
	f.writeHeader(f.flags&^internal.FlagCompress, internal.OpStartup, streamID)
	f.writeStringMap(w.opts)

	return f.finish()
}

type writePrepareFrame struct {
	statement     string
	keyspace      string
	customPayload map[string][]byte
}

func (w *writePrepareFrame) buildFrame(f *framer, streamID int) error {
	if len(w.customPayload) > 0 {
		f.payload()
	}
	f.writeHeader(f.flags, internal.OpPrepare, streamID)
	f.writeCustomPayload(&w.customPayload)
	f.writeLongString(w.statement)

	var flags uint32 = 0
	if w.keyspace != "" {
		if f.proto > internal.ProtoVersion4 {
			flags |= internal.FlagWithPreparedKeyspace
		} else {
			panic(fmt.Errorf("the keyspace can only be set with protocol 5 or higher"))
		}
	}
	if f.proto > internal.ProtoVersion4 {
		f.writeUint(flags)
	}
	if w.keyspace != "" {
		f.writeString(w.keyspace)
	}

	return f.finish()
}

func (f *framer) readTypeInfo() TypeInfo {
	// TODO: factor this out so the same code paths can be used to parse custom
	// types and other types, as much of the logic will be duplicated.
	id := f.readShort()

	simple := NativeType{
		proto: f.proto,
		typ:   Type(id),
	}

	if simple.typ == TypeCustom {
		simple.custom = f.readString()
		if cassType := getApacheCassandraType(simple.custom); cassType != TypeCustom {
			simple.typ = cassType
		}
	}

	switch simple.typ {
	case TypeTuple:
		n := f.readShort()
		tuple := TupleTypeInfo{
			NativeType: simple,
			Elems:      make([]TypeInfo, n),
		}

		for i := 0; i < int(n); i++ {
			tuple.Elems[i] = f.readTypeInfo()
		}

		return tuple

	case TypeUDT:
		udt := UDTTypeInfo{
			NativeType: simple,
		}
		udt.KeySpace = f.readString()
		udt.Name = f.readString()

		n := f.readShort()
		udt.Elements = make([]UDTField, n)
		for i := 0; i < int(n); i++ {
			field := &udt.Elements[i]
			field.Name = f.readString()
			field.Type = f.readTypeInfo()
		}

		return udt
	case TypeMap, TypeList, TypeSet:
		collection := CollectionType{
			NativeType: simple,
		}

		if simple.typ == TypeMap {
			collection.Key = f.readTypeInfo()
		}

		collection.Elem = f.readTypeInfo()

		return collection
	}

	return simple
}

type preparedMetadata struct {
	resultMetadata

	// proto v4+
	pkeyColumns []int

	keyspace string

	table string
}

func (r preparedMetadata) String() string {
	return fmt.Sprintf("[prepared flags=0x%x pkey=%v paging_state=% X columns=%v col_count=%d actual_col_count=%d]", r.flags, r.pkeyColumns, r.pagingState, r.columns, r.colCount, r.actualColCount)
}

func (f *framer) parsePreparedMetadata() preparedMetadata {
	// TODO: deduplicate this from parseMetadata
	meta := preparedMetadata{}

	meta.flags = f.readInt()
	meta.colCount = f.readInt()
	if meta.colCount < 0 {
		panic(fmt.Errorf("received negative column count: %d", meta.colCount))
	}
	meta.actualColCount = meta.colCount

	if f.proto >= internal.ProtoVersion4 {
		pkeyCount := f.readInt()
		pkeys := make([]int, pkeyCount)
		for i := 0; i < pkeyCount; i++ {
			pkeys[i] = int(f.readShort())
		}
		meta.pkeyColumns = pkeys
	}

	if meta.flags&internal.FlagHasMorePages == internal.FlagHasMorePages {
		meta.pagingState = internal.CopyBytes(f.readBytes())
	}

	if meta.flags&internal.FlagNoMetaData == internal.FlagNoMetaData {
		return meta
	}

	globalSpec := meta.flags&internal.FlagGlobalTableSpec == internal.FlagGlobalTableSpec
	if globalSpec {
		meta.keyspace = f.readString()
		meta.table = f.readString()
	}

	var cols []ColumnInfo
	if meta.colCount < 1000 {
		// preallocate columninfo to avoid excess copying
		cols = make([]ColumnInfo, meta.colCount)
		for i := 0; i < meta.colCount; i++ {
			f.readCol(&cols[i], &meta.resultMetadata, globalSpec, meta.keyspace, meta.table)
		}
	} else {
		// use append, huge number of columns usually indicates a corrupt frame or
		// just a huge row.
		for i := 0; i < meta.colCount; i++ {
			var col ColumnInfo
			f.readCol(&col, &meta.resultMetadata, globalSpec, meta.keyspace, meta.table)
			cols = append(cols, col)
		}
	}

	meta.columns = cols

	return meta
}

type resultMetadata struct {
	flags int

	// only if flagPageState
	pagingState []byte

	columns  []ColumnInfo
	colCount int

	// this is a count of the total number of columns which can be scanned,
	// it is at minimum len(columns) but may be larger, for instance when a column
	// is a UDT or tuple.
	actualColCount int
}

func (r *resultMetadata) morePages() bool {
	return r.flags&internal.FlagHasMorePages == internal.FlagHasMorePages
}

func (r resultMetadata) String() string {
	return fmt.Sprintf("[metadata flags=0x%x paging_state=% X columns=%v]", r.flags, r.pagingState, r.columns)
}

func (f *framer) readCol(col *ColumnInfo, meta *resultMetadata, globalSpec bool, keyspace, table string) {
	if !globalSpec {
		col.Keyspace = f.readString()
		col.Table = f.readString()
	} else {
		col.Keyspace = keyspace
		col.Table = table
	}

	col.Name = f.readString()
	col.TypeInfo = f.readTypeInfo()
	switch v := col.TypeInfo.(type) {
	// maybe also UDT
	case TupleTypeInfo:
		// -1 because we already included the tuple column
		meta.actualColCount += len(v.Elems) - 1
	}
}

func (f *framer) parseResultMetadata() resultMetadata {
	var meta resultMetadata

	meta.flags = f.readInt()
	meta.colCount = f.readInt()
	if meta.colCount < 0 {
		panic(fmt.Errorf("received negative column count: %d", meta.colCount))
	}
	meta.actualColCount = meta.colCount

	if meta.flags&internal.FlagHasMorePages == internal.FlagHasMorePages {
		meta.pagingState = internal.CopyBytes(f.readBytes())
	}

	if meta.flags&internal.FlagNoMetaData == internal.FlagNoMetaData {
		return meta
	}

	var keyspace, table string
	globalSpec := meta.flags&internal.FlagGlobalTableSpec == internal.FlagGlobalTableSpec
	if globalSpec {
		keyspace = f.readString()
		table = f.readString()
	}

	var cols []ColumnInfo
	if meta.colCount < 1000 {
		// preallocate columninfo to avoid excess copying
		cols = make([]ColumnInfo, meta.colCount)
		for i := 0; i < meta.colCount; i++ {
			f.readCol(&cols[i], &meta, globalSpec, keyspace, table)
		}

	} else {
		// use append, huge number of columns usually indicates a corrupt frame or
		// just a huge row.
		for i := 0; i < meta.colCount; i++ {
			var col ColumnInfo
			f.readCol(&col, &meta, globalSpec, keyspace, table)
			cols = append(cols, col)
		}
	}

	meta.columns = cols

	return meta
}

type resultVoidFrame struct {
	internal.FrameHeader
}

func (f *resultVoidFrame) String() string {
	return "[result_void]"
}

func (f *framer) parseResultFrame() (internal.Frame, error) {
	kind := f.readInt()

	switch kind {
	case internal.ResultKindVoid:
		return &resultVoidFrame{FrameHeader: *f.header}, nil
	case internal.ResultKindRows:
		return f.parseResultRows(), nil
	case internal.ResultKindKeyspace:
		return f.parseResultSetKeyspace(), nil
	case internal.ResultKindPrepared:
		return f.parseResultPrepared(), nil
	case internal.ResultKindSchemaChanged:
		return f.parseResultSchemaChange(), nil
	}

	return nil, NewErrProtocol("unknown result kind: %x", kind)
}

type resultRowsFrame struct {
	internal.FrameHeader

	meta resultMetadata
	// dont parse the rows here as we only need to do it once
	numRows int
}

func (f *resultRowsFrame) String() string {
	return fmt.Sprintf("[result_rows meta=%v]", f.meta)
}

func (f *framer) parseResultRows() internal.Frame {
	result := &resultRowsFrame{}
	result.meta = f.parseResultMetadata()

	result.numRows = f.readInt()
	if result.numRows < 0 {
		panic(fmt.Errorf("invalid row_count in result frame: %d", result.numRows))
	}

	return result
}

type resultKeyspaceFrame struct {
	internal.FrameHeader
	keyspace string
}

func (r *resultKeyspaceFrame) String() string {
	return fmt.Sprintf("[result_keyspace keyspace=%s]", r.keyspace)
}

func (f *framer) parseResultSetKeyspace() internal.Frame {
	return &resultKeyspaceFrame{
		FrameHeader: *f.header,
		keyspace:    f.readString(),
	}
}

type resultPreparedFrame struct {
	internal.FrameHeader

	preparedID []byte
	reqMeta    preparedMetadata
	respMeta   resultMetadata
}

func (f *framer) parseResultPrepared() internal.Frame {
	frame := &resultPreparedFrame{
		FrameHeader: *f.header,
		preparedID:  f.readShortBytes(),
		reqMeta:     f.parsePreparedMetadata(),
	}

	if f.proto < internal.ProtoVersion2 {
		return frame
	}

	frame.respMeta = f.parseResultMetadata()

	return frame
}

type schemaChangeKeyspace struct {
	internal.FrameHeader

	change   string
	keyspace string
}

func (f schemaChangeKeyspace) String() string {
	return fmt.Sprintf("[event schema_change_keyspace change=%q keyspace=%q]", f.change, f.keyspace)
}

type schemaChangeTable struct {
	internal.FrameHeader

	change   string
	keyspace string
	object   string
}

func (f schemaChangeTable) String() string {
	return fmt.Sprintf("[event schema_change change=%q keyspace=%q object=%q]", f.change, f.keyspace, f.object)
}

type schemaChangeType struct {
	internal.FrameHeader

	change   string
	keyspace string
	object   string
}

type schemaChangeFunction struct {
	internal.FrameHeader

	change   string
	keyspace string
	name     string
	args     []string
}

type schemaChangeAggregate struct {
	internal.FrameHeader

	change   string
	keyspace string
	name     string
	args     []string
}

func (f *framer) parseResultSchemaChange() internal.Frame {
	if f.proto <= internal.ProtoVersion2 {
		change := f.readString()
		keyspace := f.readString()
		table := f.readString()

		if table != "" {
			return &schemaChangeTable{
				FrameHeader: *f.header,
				change:      change,
				keyspace:    keyspace,
				object:      table,
			}
		} else {
			return &schemaChangeKeyspace{
				FrameHeader: *f.header,
				change:      change,
				keyspace:    keyspace,
			}
		}
	} else {
		change := f.readString()
		target := f.readString()

		// TODO: could just use a separate type for each target
		switch target {
		case "KEYSPACE":
			frame := &schemaChangeKeyspace{
				FrameHeader: *f.header,
				change:      change,
			}

			frame.keyspace = f.readString()

			return frame
		case "TABLE":
			frame := &schemaChangeTable{
				FrameHeader: *f.header,
				change:      change,
			}

			frame.keyspace = f.readString()
			frame.object = f.readString()

			return frame
		case "TYPE":
			frame := &schemaChangeType{
				FrameHeader: *f.header,
				change:      change,
			}

			frame.keyspace = f.readString()
			frame.object = f.readString()

			return frame
		case "FUNCTION":
			frame := &schemaChangeFunction{
				FrameHeader: *f.header,
				change:      change,
			}

			frame.keyspace = f.readString()
			frame.name = f.readString()
			frame.args = f.readStringList()

			return frame
		case "AGGREGATE":
			frame := &schemaChangeAggregate{
				FrameHeader: *f.header,
				change:      change,
			}

			frame.keyspace = f.readString()
			frame.name = f.readString()
			frame.args = f.readStringList()

			return frame
		default:
			panic(fmt.Errorf("gocql: unknown SCHEMA_CHANGE target: %q change: %q", target, change))
		}
	}

}

type authenticateFrame struct {
	internal.FrameHeader

	class string
}

func (a *authenticateFrame) String() string {
	return fmt.Sprintf("[authenticate class=%q]", a.class)
}

func (f *framer) parseAuthenticateFrame() internal.Frame {
	return &authenticateFrame{
		FrameHeader: *f.header,
		class:       f.readString(),
	}
}

type authSuccessFrame struct {
	internal.FrameHeader

	data []byte
}

func (a *authSuccessFrame) String() string {
	return fmt.Sprintf("[auth_success data=%q]", a.data)
}

func (f *framer) parseAuthSuccessFrame() internal.Frame {
	return &authSuccessFrame{
		FrameHeader: *f.header,
		data:        f.readBytes(),
	}
}

type authChallengeFrame struct {
	internal.FrameHeader

	data []byte
}

func (a *authChallengeFrame) String() string {
	return fmt.Sprintf("[auth_challenge data=%q]", a.data)
}

func (f *framer) parseAuthChallengeFrame() internal.Frame {
	return &authChallengeFrame{
		FrameHeader: *f.header,
		data:        f.readBytes(),
	}
}

type statusChangeEventFrame struct {
	internal.FrameHeader

	change string
	host   net.IP
	port   int
}

func (t statusChangeEventFrame) String() string {
	return fmt.Sprintf("[status_change change=%s host=%v port=%v]", t.change, t.host, t.port)
}

// essentially the same as statusChange
type topologyChangeEventFrame struct {
	internal.FrameHeader

	change string
	host   net.IP
	port   int
}

func (t topologyChangeEventFrame) String() string {
	return fmt.Sprintf("[topology_change change=%s host=%v port=%v]", t.change, t.host, t.port)
}

func (f *framer) parseEventFrame() internal.Frame {
	eventType := f.readString()

	switch eventType {
	case "TOPOLOGY_CHANGE":
		frame := &topologyChangeEventFrame{FrameHeader: *f.header}
		frame.change = f.readString()
		frame.host, frame.port = f.readInet()

		return frame
	case "STATUS_CHANGE":
		frame := &statusChangeEventFrame{FrameHeader: *f.header}
		frame.change = f.readString()
		frame.host, frame.port = f.readInet()

		return frame
	case "SCHEMA_CHANGE":
		// this should work for all versions
		return f.parseResultSchemaChange()
	default:
		panic(fmt.Errorf("gocql: unknown event type: %q", eventType))
	}

}

type writeAuthResponseFrame struct {
	data []byte
}

func (a *writeAuthResponseFrame) String() string {
	return fmt.Sprintf("[auth_response data=%q]", a.data)
}

func (a *writeAuthResponseFrame) buildFrame(framer *framer, streamID int) error {
	return framer.writeAuthResponseFrame(streamID, a.data)
}

func (f *framer) writeAuthResponseFrame(streamID int, data []byte) error {
	f.writeHeader(f.flags, internal.OpAuthResponse, streamID)
	f.writeBytes(data)
	return f.finish()
}

type queryValues struct {
	value []byte

	// optional name, will set With names for values flag
	name    string
	isUnset bool
}

type queryParams struct {
	consistency Consistency
	// v2+
	skipMeta          bool
	values            []queryValues
	pageSize          int
	pagingState       []byte
	serialConsistency SerialConsistency
	// v3+
	defaultTimestamp      bool
	defaultTimestampValue int64
	// v5+
	keyspace string
}

func (q queryParams) String() string {
	return fmt.Sprintf("[query_params consistency=%v skip_meta=%v page_size=%d paging_state=%q serial_consistency=%v default_timestamp=%v values=%v keyspace=%s]",
		q.consistency, q.skipMeta, q.pageSize, q.pagingState, q.serialConsistency, q.defaultTimestamp, q.values, q.keyspace)
}

func (f *framer) writeQueryParams(opts *queryParams) {
	f.writeConsistency(opts.consistency)

	if f.proto == internal.ProtoVersion1 {
		return
	}

	var flags byte
	if len(opts.values) > 0 {
		flags |= internal.FlagValues
	}
	if opts.skipMeta {
		flags |= internal.FlagSkipMetaData
	}
	if opts.pageSize > 0 {
		flags |= internal.FlagPageSize
	}
	if len(opts.pagingState) > 0 {
		flags |= internal.FlagWithPagingState
	}
	if opts.serialConsistency > 0 {
		flags |= internal.FlagWithSerialConsistency
	}

	names := false

	// protoV3 specific things
	if f.proto > internal.ProtoVersion2 {
		if opts.defaultTimestamp {
			flags |= internal.FlagDefaultTimestamp
		}

		if len(opts.values) > 0 && opts.values[0].name != "" {
			flags |= internal.FlagWithNameValues
			names = true
		}
	}

	if opts.keyspace != "" {
		if f.proto > internal.ProtoVersion4 {
			flags |= internal.FlagWithKeyspace
		} else {
			panic(fmt.Errorf("the keyspace can only be set with protocol 5 or higher"))
		}
	}

	if f.proto > internal.ProtoVersion4 {
		f.writeUint(uint32(flags))
	} else {
		f.writeByte(flags)
	}

	if n := len(opts.values); n > 0 {
		f.writeShort(uint16(n))

		for i := 0; i < n; i++ {
			if names {
				f.writeString(opts.values[i].name)
			}
			if opts.values[i].isUnset {
				f.writeUnset()
			} else {
				f.writeBytes(opts.values[i].value)
			}
		}
	}

	if opts.pageSize > 0 {
		f.writeInt(int32(opts.pageSize))
	}

	if len(opts.pagingState) > 0 {
		f.writeBytes(opts.pagingState)
	}

	if opts.serialConsistency > 0 {
		f.writeConsistency(Consistency(opts.serialConsistency))
	}

	if f.proto > internal.ProtoVersion2 && opts.defaultTimestamp {
		// timestamp in microseconds
		var ts int64
		if opts.defaultTimestampValue != 0 {
			ts = opts.defaultTimestampValue
		} else {
			ts = time.Now().UnixNano() / 1000
		}
		f.writeLong(ts)
	}

	if opts.keyspace != "" {
		f.writeString(opts.keyspace)
	}
}

type writeQueryFrame struct {
	statement string
	params    queryParams

	// v4+
	customPayload map[string][]byte
}

func (w *writeQueryFrame) String() string {
	return fmt.Sprintf("[query statement=%q params=%v]", w.statement, w.params)
}

func (w *writeQueryFrame) buildFrame(framer *framer, streamID int) error {
	return framer.writeQueryFrame(streamID, w.statement, &w.params, w.customPayload)
}

func (f *framer) writeQueryFrame(streamID int, statement string, params *queryParams, customPayload map[string][]byte) error {
	if len(customPayload) > 0 {
		f.payload()
	}
	f.writeHeader(f.flags, internal.OpQuery, streamID)
	f.writeCustomPayload(&customPayload)
	f.writeLongString(statement)
	f.writeQueryParams(params)

	return f.finish()
}

type frameBuilder interface {
	buildFrame(framer *framer, streamID int) error
}

type frameWriterFunc func(framer *framer, streamID int) error

func (f frameWriterFunc) buildFrame(framer *framer, streamID int) error {
	return f(framer, streamID)
}

type writeExecuteFrame struct {
	preparedID []byte
	params     queryParams

	// v4+
	customPayload map[string][]byte
}

func (e *writeExecuteFrame) String() string {
	return fmt.Sprintf("[execute id=% X params=%v]", e.preparedID, &e.params)
}

func (e *writeExecuteFrame) buildFrame(fr *framer, streamID int) error {
	return fr.writeExecuteFrame(streamID, e.preparedID, &e.params, &e.customPayload)
}

func (f *framer) writeExecuteFrame(streamID int, preparedID []byte, params *queryParams, customPayload *map[string][]byte) error {
	if len(*customPayload) > 0 {
		f.payload()
	}
	f.writeHeader(f.flags, internal.OpExecute, streamID)
	f.writeCustomPayload(customPayload)
	f.writeShortBytes(preparedID)
	if f.proto > internal.ProtoVersion1 {
		f.writeQueryParams(params)
	} else {
		n := len(params.values)
		f.writeShort(uint16(n))
		for i := 0; i < n; i++ {
			if params.values[i].isUnset {
				f.writeUnset()
			} else {
				f.writeBytes(params.values[i].value)
			}
		}
		f.writeConsistency(params.consistency)
	}

	return f.finish()
}

// TODO: can we replace BatchStatemt with batchStatement? As they prety much
// duplicate each other
type batchStatment struct {
	preparedID []byte
	statement  string
	values     []queryValues
}

type writeBatchFrame struct {
	typ         BatchType
	statements  []batchStatment
	consistency Consistency

	// v3+
	serialConsistency     SerialConsistency
	defaultTimestamp      bool
	defaultTimestampValue int64

	//v4+
	customPayload map[string][]byte
}

func (w *writeBatchFrame) buildFrame(framer *framer, streamID int) error {
	return framer.writeBatchFrame(streamID, w, w.customPayload)
}

func (f *framer) writeBatchFrame(streamID int, w *writeBatchFrame, customPayload map[string][]byte) error {
	if len(customPayload) > 0 {
		f.payload()
	}
	f.writeHeader(f.flags, internal.OpBatch, streamID)
	f.writeCustomPayload(&customPayload)
	f.writeByte(byte(w.typ))

	n := len(w.statements)
	f.writeShort(uint16(n))

	var flags byte

	for i := 0; i < n; i++ {
		b := &w.statements[i]
		if len(b.preparedID) == 0 {
			f.writeByte(0)
			f.writeLongString(b.statement)
		} else {
			f.writeByte(1)
			f.writeShortBytes(b.preparedID)
		}

		f.writeShort(uint16(len(b.values)))
		for j := range b.values {
			col := b.values[j]
			if f.proto > internal.ProtoVersion2 && col.name != "" {
				// TODO: move this check into the caller and set a flag on writeBatchFrame
				// to indicate using named values
				if f.proto <= internal.ProtoVersion5 {
					return fmt.Errorf("gocql: named query values are not supported in batches, please see https://issues.apache.org/jira/browse/CASSANDRA-10246")
				}
				flags |= internal.FlagWithNameValues
				f.writeString(col.name)
			}
			if col.isUnset {
				f.writeUnset()
			} else {
				f.writeBytes(col.value)
			}
		}
	}

	f.writeConsistency(w.consistency)

	if f.proto > internal.ProtoVersion2 {
		if w.serialConsistency > 0 {
			flags |= internal.FlagWithSerialConsistency
		}
		if w.defaultTimestamp {
			flags |= internal.FlagDefaultTimestamp
		}

		if f.proto > internal.ProtoVersion4 {
			f.writeUint(uint32(flags))
		} else {
			f.writeByte(flags)
		}

		if w.serialConsistency > 0 {
			f.writeConsistency(Consistency(w.serialConsistency))
		}

		if w.defaultTimestamp {
			var ts int64
			if w.defaultTimestampValue != 0 {
				ts = w.defaultTimestampValue
			} else {
				ts = time.Now().UnixNano() / 1000
			}
			f.writeLong(ts)
		}
	}

	return f.finish()
}

type writeOptionsFrame struct{}

func (w *writeOptionsFrame) buildFrame(framer *framer, streamID int) error {
	return framer.writeOptionsFrame(streamID, w)
}

func (f *framer) writeOptionsFrame(stream int, _ *writeOptionsFrame) error {
	f.writeHeader(f.flags&^internal.FlagCompress, internal.OpOptions, stream)
	return f.finish()
}

type writeRegisterFrame struct {
	events []string
}

func (w *writeRegisterFrame) buildFrame(framer *framer, streamID int) error {
	return framer.writeRegisterFrame(streamID, w)
}

func (f *framer) writeRegisterFrame(streamID int, w *writeRegisterFrame) error {
	f.writeHeader(f.flags, internal.OpRegister, streamID)
	f.writeStringList(w.events)

	return f.finish()
}

func (f *framer) readByte() byte {
	if len(f.buf) < 1 {
		panic(fmt.Errorf("not enough bytes in buffer to read byte require 1 got: %d", len(f.buf)))
	}

	b := f.buf[0]
	f.buf = f.buf[1:]
	return b
}

func (f *framer) readInt() (n int) {
	if len(f.buf) < 4 {
		panic(fmt.Errorf("not enough bytes in buffer to read int require 4 got: %d", len(f.buf)))
	}

	n = int(int32(f.buf[0])<<24 | int32(f.buf[1])<<16 | int32(f.buf[2])<<8 | int32(f.buf[3]))
	f.buf = f.buf[4:]
	return
}

func (f *framer) readShort() (n uint16) {
	if len(f.buf) < 2 {
		panic(fmt.Errorf("not enough bytes in buffer to read short require 2 got: %d", len(f.buf)))
	}
	n = uint16(f.buf[0])<<8 | uint16(f.buf[1])
	f.buf = f.buf[2:]
	return
}

func (f *framer) readString() (s string) {
	size := f.readShort()

	if len(f.buf) < int(size) {
		panic(fmt.Errorf("not enough bytes in buffer to read string require %d got: %d", size, len(f.buf)))
	}

	s = string(f.buf[:size])
	f.buf = f.buf[size:]
	return
}

func (f *framer) readLongString() (s string) {
	size := f.readInt()

	if len(f.buf) < size {
		panic(fmt.Errorf("not enough bytes in buffer to read long string require %d got: %d", size, len(f.buf)))
	}

	s = string(f.buf[:size])
	f.buf = f.buf[size:]
	return
}

func (f *framer) readUUID() *UUID {
	if len(f.buf) < 16 {
		panic(fmt.Errorf("not enough bytes in buffer to read uuid require %d got: %d", 16, len(f.buf)))
	}

	// TODO: how to handle this error, if it is a uuid, then sureley, problems?
	u, _ := UUIDFromBytes(f.buf[:16])
	f.buf = f.buf[16:]
	return &u
}

func (f *framer) readStringList() []string {
	size := f.readShort()

	l := make([]string, size)
	for i := 0; i < int(size); i++ {
		l[i] = f.readString()
	}

	return l
}

func (f *framer) readBytesInternal() ([]byte, error) {
	size := f.readInt()
	if size < 0 {
		return nil, nil
	}

	if len(f.buf) < size {
		return nil, fmt.Errorf("not enough bytes in buffer to read bytes require %d got: %d", size, len(f.buf))
	}

	l := f.buf[:size]
	f.buf = f.buf[size:]

	return l, nil
}

func (f *framer) readBytes() []byte {
	l, err := f.readBytesInternal()
	if err != nil {
		panic(err)
	}

	return l
}

func (f *framer) readShortBytes() []byte {
	size := f.readShort()
	if len(f.buf) < int(size) {
		panic(fmt.Errorf("not enough bytes in buffer to read short bytes: require %d got %d", size, len(f.buf)))
	}

	l := f.buf[:size]
	f.buf = f.buf[size:]

	return l
}

func (f *framer) readInetAdressOnly() net.IP {
	if len(f.buf) < 1 {
		panic(fmt.Errorf("not enough bytes in buffer to read inet size require %d got: %d", 1, len(f.buf)))
	}

	size := f.buf[0]
	f.buf = f.buf[1:]

	if !(size == 4 || size == 16) {
		panic(fmt.Errorf("invalid IP size: %d", size))
	}

	if len(f.buf) < 1 {
		panic(fmt.Errorf("not enough bytes in buffer to read inet require %d got: %d", size, len(f.buf)))
	}

	ip := make([]byte, size)
	copy(ip, f.buf[:size])
	f.buf = f.buf[size:]
	return net.IP(ip)
}

func (f *framer) readInet() (net.IP, int) {
	return f.readInetAdressOnly(), f.readInt()
}

func (f *framer) readConsistency() Consistency {
	return Consistency(f.readShort())
}

func (f *framer) readBytesMap() map[string][]byte {
	size := f.readShort()
	m := make(map[string][]byte, size)

	for i := 0; i < int(size); i++ {
		k := f.readString()
		v := f.readBytes()
		m[k] = v
	}

	return m
}

func (f *framer) readStringMultiMap() map[string][]string {
	size := f.readShort()
	m := make(map[string][]string, size)

	for i := 0; i < int(size); i++ {
		k := f.readString()
		v := f.readStringList()
		m[k] = v
	}

	return m
}

func (f *framer) writeByte(b byte) {
	f.buf = append(f.buf, b)
}

func (f *framer) writeCustomPayload(customPayload *map[string][]byte) {
	if len(*customPayload) > 0 {
		if f.proto < internal.ProtoVersion4 {
			panic("Custom payload is not supported with version V3 or less")
		}
		f.writeBytesMap(*customPayload)
	}
}

// these are protocol level binary types
func (f *framer) writeInt(n int32) {
	f.buf = internal.AppendInt(f.buf, n)
}

func (f *framer) writeUint(n uint32) {
	f.buf = internal.AppendUint(f.buf, n)
}

func (f *framer) writeShort(n uint16) {
	f.buf = internal.AppendShort(f.buf, n)
}

func (f *framer) writeLong(n int64) {
	f.buf = internal.AppendLong(f.buf, n)
}

func (f *framer) writeString(s string) {
	f.writeShort(uint16(len(s)))
	f.buf = append(f.buf, s...)
}

func (f *framer) writeLongString(s string) {
	f.writeInt(int32(len(s)))
	f.buf = append(f.buf, s...)
}

func (f *framer) writeStringList(l []string) {
	f.writeShort(uint16(len(l)))
	for _, s := range l {
		f.writeString(s)
	}
}

func (f *framer) writeUnset() {
	// Protocol version 4 specifies that bind variables do not require having a
	// value when executing a statement.   Bind variables without a value are
	// called 'unset'. The 'unset' bind variable is serialized as the int
	// value '-2' without following bytes.
	f.writeInt(-2)
}

func (f *framer) writeBytes(p []byte) {
	// TODO: handle null case correctly,
	//     [bytes]        A [int] n, followed by n bytes if n >= 0. If n < 0,
	//					  no byte should follow and the value represented is `null`.
	if p == nil {
		f.writeInt(-1)
	} else {
		f.writeInt(int32(len(p)))
		f.buf = append(f.buf, p...)
	}
}

func (f *framer) writeShortBytes(p []byte) {
	f.writeShort(uint16(len(p)))
	f.buf = append(f.buf, p...)
}

func (f *framer) writeConsistency(cons Consistency) {
	f.writeShort(uint16(cons))
}

func (f *framer) writeStringMap(m map[string]string) {
	f.writeShort(uint16(len(m)))
	for k, v := range m {
		f.writeString(k)
		f.writeString(v)
	}
}

func (f *framer) writeBytesMap(m map[string][]byte) {
	f.writeShort(uint16(len(m)))
	for k, v := range m {
		f.writeString(k)
		f.writeBytes(v)
	}
}
