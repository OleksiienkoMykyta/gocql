package internal

import "fmt"

type NamedValue struct {
	Name  string
	Value interface{}
}

type UnsetColumn struct{}

func ReadInt(p []byte) int32 {
	return int32(p[0])<<24 | int32(p[1])<<16 | int32(p[2])<<8 | int32(p[3])
}

func AppendBytes(p []byte, d []byte) []byte {
	if d == nil {
		return AppendInt(p, -1)
	}
	p = AppendInt(p, int32(len(d)))
	p = append(p, d...)
	return p
}

func AppendShort(p []byte, n uint16) []byte {
	return append(p,
		byte(n>>8),
		byte(n),
	)
}

func AppendInt(p []byte, n int32) []byte {
	return append(p, byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

func AppendUint(p []byte, n uint32) []byte {
	return append(p, byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

func AppendLong(p []byte, n int64) []byte {
	return append(p,
		byte(n>>56),
		byte(n>>48),
		byte(n>>40),
		byte(n>>32),
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n),
	)
}

const (
	ProtoDirectionMask = 0x80
	ProtoVersionMask   = 0x7F
	ProtoVersion1      = 0x01
	ProtoVersion2      = 0x02
	ProtoVersion3      = 0x03
	ProtoVersion4      = 0x04
	ProtoVersion5      = 0x05

	MaxFrameSize = 256 * 1024 * 1024
)

type ProtoVersion byte

func (p ProtoVersion) Request() bool {
	return p&ProtoDirectionMask == 0x00
}

func (p ProtoVersion) Response() bool {
	return p&ProtoDirectionMask == 0x80
}

func (p ProtoVersion) Version() byte {
	return byte(p) & ProtoVersionMask
}

func (p ProtoVersion) String() string {
	dir := "REQ"
	if p.Response() {
		dir = "RESP"
	}

	return fmt.Sprintf("[version=%d direction=%s]", p.Version(), dir)
}

type FrameOp byte

const (
	// header ops
	OpError         FrameOp = 0x00
	OpStartup       FrameOp = 0x01
	OpReady         FrameOp = 0x02
	OpAuthenticate  FrameOp = 0x03
	OpOptions       FrameOp = 0x05
	OpSupported     FrameOp = 0x06
	OpQuery         FrameOp = 0x07
	OpResult        FrameOp = 0x08
	OpPrepare       FrameOp = 0x09
	OpExecute       FrameOp = 0x0A
	OpRegister      FrameOp = 0x0B
	OpEvent         FrameOp = 0x0C
	OpBatch         FrameOp = 0x0D
	OpAuthChallenge FrameOp = 0x0E
	OpAuthResponse  FrameOp = 0x0F
	OpAuthSuccess   FrameOp = 0x10
)

func (f FrameOp) String() string {
	switch f {
	case OpError:
		return "ERROR"
	case OpStartup:
		return "STARTUP"
	case OpReady:
		return "READY"
	case OpAuthenticate:
		return "AUTHENTICATE"
	case OpOptions:
		return "OPTIONS"
	case OpSupported:
		return "SUPPORTED"
	case OpQuery:
		return "QUERY"
	case OpResult:
		return "RESULT"
	case OpPrepare:
		return "PREPARE"
	case OpExecute:
		return "EXECUTE"
	case OpRegister:
		return "REGISTER"
	case OpEvent:
		return "EVENT"
	case OpBatch:
		return "BATCH"
	case OpAuthChallenge:
		return "AUTH_CHALLENGE"
	case OpAuthResponse:
		return "AUTH_RESPONSE"
	case OpAuthSuccess:
		return "AUTH_SUCCESS"
	default:
		return fmt.Sprintf("UNKNOWN_OP_%d", f)
	}
}

const (
	// result kind
	ResultKindVoid          = 1
	ResultKindRows          = 2
	ResultKindKeyspace      = 3
	ResultKindPrepared      = 4
	ResultKindSchemaChanged = 5

	// rows flags
	FlagGlobalTableSpec int = 0x01
	FlagHasMorePages    int = 0x02
	FlagNoMetaData      int = 0x04

	// query flags
	FlagValues                byte = 0x01
	FlagSkipMetaData          byte = 0x02
	FlagPageSize              byte = 0x04
	FlagWithPagingState       byte = 0x08
	FlagWithSerialConsistency byte = 0x10
	FlagDefaultTimestamp      byte = 0x20
	FlagWithNameValues        byte = 0x40
	FlagWithKeyspace          byte = 0x80

	// prepare flags
	FlagWithPreparedKeyspace uint32 = 0x01

	// header flags
	FlagCompress      byte = 0x01
	FlagTracing       byte = 0x02
	FlagCustomPayload byte = 0x04
	FlagWarning       byte = 0x08
	FlagBetaProtocol  byte = 0x10
)

const (
	ApacheCassandraTypePrefix = "org.apache.cassandra.db.marshal."
)

const MaxFrameHeaderSize = 9

type Frame interface {
	Header() FrameHeader
}

type FrameHeader struct {
	Version  ProtoVersion
	Flags    byte
	Stream   int
	Op       FrameOp
	Length   int
	Warnings []string
}

func (f FrameHeader) String() string {
	return fmt.Sprintf("[header version=%s flags=0x%x stream=%d op=%s length=%d]", f.Version, f.Flags, f.Stream, f.Op, f.Length)
}

func (f FrameHeader) Header() FrameHeader {
	return f
}

const DefaultBufSize = 128
