package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/crypto"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/pkg/utils/runtimeerr"
)

const (
	errCodeRawPrefix = "PROTOCOL_RAW_PREFIX"
	errCodeRawFlags  = "PROTOCOL_RAW_FLAGS"
	errCodeRawBounds = "PROTOCOL_RAW_BOUNDS"
)

var headerMagicBytes = [4]byte{'S', 'W', '1', 0x00}

const (
	maxSenderLen       = 64
	maxAccepterLen     = 64
	maxRouteLen        = 4096
	maxDataLen         = 32 * 1024 * 1024 // capped encrypted payload
	maxDecompressedLen = 32 * 1024 * 1024 // guard against gzip bombs
)

type RawProto struct{}

type RawMessage struct {
	// 应用消息所需的基本组件
	UUID         string
	Conn         net.Conn
	CryptoSecret []byte
	Flags        uint16
	// 准备好的缓冲区
	HeaderBuffer []byte
	DataBuffer   []byte
}

func (proto *RawProto) CNegotiate() error { return nil }

func (proto *RawProto) SNegotiate() error { return nil }

func routeSingleHopTo(uuid, route string) bool {
	uuid = strings.TrimSpace(uuid)
	route = strings.TrimSpace(route)
	if uuid == "" || route == "" {
		return false
	}
	// 多跳 route：这并不表示需要本地交付。
	if strings.Contains(route, ":") {
		return false
	}
	segment := route
	const suppSuffix = "#supp"
	segment = strings.TrimSuffix(segment, suppSuffix)
	return segment == uuid
}

func (message *RawMessage) ConstructHeader() {}

func (message *RawMessage) ConstructData(header *Header, mess interface{}, isPass bool) {
	if message == nil {
		return
	}
	var headerBuffer, dataBuffer bytes.Buffer
	// 首先，构造自己的头部
	if header == nil {
		header = new(Header)
	}
	header.RouteLen = uint32(len([]byte(header.Route)))

	if message.Flags == 0 {
		message.Flags = DefaultProtocolFlags
	}
	if header.Flags == 0 {
		header.Flags = message.Flags
	}
	if header.Flags&^DefaultProtocolFlags != 0 {
		logger.Errorf("unsupported protocol flags %#x", header.Flags)
		message.abortConn()
		return
	}
	if err := validateHeaderBounds(header); err != nil {
		logger.Errorf("invalid header: %v", err)
		message.abortConn()
		return
	}
	headerBuffer.Write(headerMagicBytes[:])
	flagsBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(flagsBuf, header.Flags)
	headerBuffer.Write(flagsBuf)

	senderBytes := []byte(header.Sender)
	accepterBytes := []byte(header.Accepter)
	senderLenBuf := make([]byte, 2)
	accepterLenBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(senderLenBuf, uint16(len(senderBytes)))
	binary.BigEndian.PutUint16(accepterLenBuf, uint16(len(accepterBytes)))

	messageTypeBuf := make([]byte, 2)
	routeLenBuf := make([]byte, 4)
	binary.BigEndian.PutUint16(messageTypeBuf, header.MessageType)
	binary.BigEndian.PutUint32(routeLenBuf, header.RouteLen)

	headerBuffer.Write(senderLenBuf)
	headerBuffer.Write(accepterLenBuf)
	headerBuffer.Write(senderBytes)
	headerBuffer.Write(accepterBytes)
	headerBuffer.Write(messageTypeBuf)
	headerBuffer.Write(routeLenBuf)
	headerBuffer.Write([]byte(header.Route))

	// 检查消息数据是否需要加密
	if !isPass {
		buf, err := marshalMessagePayload(mess)
		if err != nil {
			logger.Errorf("marshal %T failed: %v", mess, err)
			return
		}
		dataBuffer.Write(buf)
	} else {
		mmess := mess.([]byte)
		dataBuffer.Write(mmess)
	}

	message.DataBuffer = dataBuffer.Bytes()
	// 加密和压缩数据
	if !isPass {
		compressed, err := crypto.GzipCompress(message.DataBuffer)
		if err != nil {
			logger.Errorf("compress payload failed: %v", err)
			if message.Conn != nil {
				_ = message.Conn.Close()
			}
			message.DataBuffer = nil
			message.HeaderBuffer = nil
			return
		}
		encrypted, err := crypto.AESEncrypt(compressed, message.CryptoSecret)
		if err != nil {
			logger.Errorf("encrypt payload failed: %v", err)
			if message.Conn != nil {
				_ = message.Conn.Close()
			}
			message.DataBuffer = nil
			message.HeaderBuffer = nil
			return
		}
		if len(encrypted) > maxDataLen {
			logger.Errorf("payload too large after encryption: %d bytes (limit %d)", len(encrypted), maxDataLen)
			if message.Conn != nil {
				_ = message.Conn.Close()
			}
			message.DataBuffer = nil
			message.HeaderBuffer = nil
			return
		}
		message.DataBuffer = encrypted
	}
	if len(message.DataBuffer) > maxDataLen {
		logger.Errorf("payload too large: %d bytes (limit %d)", len(message.DataBuffer), maxDataLen)
		if message.Conn != nil {
			_ = message.Conn.Close()
		}
		message.DataBuffer = nil
		message.HeaderBuffer = nil
		return
	}
	// 计算整个数据的长度
	dataLenBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(dataLenBuf, uint64(len(message.DataBuffer)))
	headerBuffer.Write(dataLenBuf)
	message.HeaderBuffer = headerBuffer.Bytes()
}

func (message *RawMessage) ConstructSuffix() {}

func (message *RawMessage) DeconstructHeader() {}

func (message *RawMessage) DeconstructData() (*Header, interface{}, error) {
	var (
		header         = new(Header)
		messageTypeBuf = make([]byte, 2)
		routeLenBuf    = make([]byte, 4)
		dataLenBuf     = make([]byte, 8)
	)

	var err error
	// 逐个读取头部元素
	if err = message.readHeaderPrefix(header); err != nil {
		return header, nil, err
	}

	_, err = io.ReadFull(message.Conn, messageTypeBuf)
	if err != nil {
		return header, nil, err
	}
	header.MessageType = binary.BigEndian.Uint16(messageTypeBuf)

	_, err = io.ReadFull(message.Conn, routeLenBuf)
	if err != nil {
		return header, nil, err
	}
	header.RouteLen = binary.BigEndian.Uint32(routeLenBuf)

	routeBuf := make([]byte, header.RouteLen)
	_, err = io.ReadFull(message.Conn, routeBuf)
	if err != nil {
		return header, nil, err
	}
	header.Route = string(routeBuf)

	_, err = io.ReadFull(message.Conn, dataLenBuf)
	if err != nil {
		return header, nil, err
	}
	header.DataLen = binary.BigEndian.Uint64(dataLenBuf)
	if header.DataLen > maxDataLen {
		return header, nil, runtimeerr.New(errCodeRawBounds, runtimeerr.SeverityError, false, "payload exceeds limit: %d > %d", header.DataLen, maxDataLen)
	}

	dataBuf := make([]byte, header.DataLen)
	_, err = io.ReadFull(message.Conn, dataBuf)
	if err != nil {
		return header, nil, err
	}

	// 判断当前节点是否应该解密并反序列化该载荷。
	//
	// 当 Accepter 字段使用 TEMP_UUID 时，载荷按逐跳路由语义转发。
	// 只有最后一跳才应该解密它们；中间跳点应直接转发加密后的字节，
	// 这样多跳路由才能在不重复加密的情况下工作。
	shouldDecrypt := message.UUID == ADMIN_UUID ||
		message.UUID == header.Accepter ||
		(header.Accepter == TEMP_UUID && (header.RouteLen == 0 || routeSingleHopTo(message.UUID, header.Route)))

	if shouldDecrypt {
		decrypted, err := crypto.AESDecrypt(dataBuf, message.CryptoSecret)
		if err != nil {
			return header, nil, err
		}
		dataBuf = decrypted // 直接使用dataBuf以节省内存
	} else {
		return header, dataBuf, nil
	}
	// 解压缩数据
	dataBuf, err = crypto.GzipDecompressLimit(dataBuf, maxDecompressedLen)
	if err != nil {
		return header, nil, err
	}
	mess, err := unmarshalMessagePayload(header.MessageType, dataBuf)
	if err != nil {
		return header, nil, err
	}

	return header, mess, nil
}

func (message *RawMessage) DeconstructSuffix() {}

func (message *RawMessage) SendMessage() {
	finalBuffer := append(message.HeaderBuffer, message.DataBuffer...)
	if err := utils.WriteFull(message.Conn, finalBuffer); err != nil {
		logger.Errorf("failed to write raw message: %v", err)
		if message.Conn != nil {
			_ = message.Conn.Close()
		}
	}
	// 不要忘记将两个Buffer都设置为nil!!!
	message.HeaderBuffer = nil
	message.DataBuffer = nil
}

func (message *RawMessage) readHeaderPrefix(header *Header) error {
	prefix := make([]byte, len(headerMagicBytes))
	if _, err := io.ReadFull(message.Conn, prefix); err != nil {
		return err
	}

	if !bytes.Equal(prefix, headerMagicBytes[:]) {
		return runtimeerr.New(errCodeRawPrefix, runtimeerr.SeverityError, false, "不支持的协议帧前缀: %x", prefix)
	}

	flagsBuf := make([]byte, 2)
	senderLenBuf := make([]byte, 2)
	accepterLenBuf := make([]byte, 2)
	if _, err := io.ReadFull(message.Conn, flagsBuf); err != nil {
		return err
	}
	header.Flags = binary.BigEndian.Uint16(flagsBuf)
	if header.Flags&^DefaultProtocolFlags != 0 {
		return runtimeerr.New(errCodeRawFlags, runtimeerr.SeverityError, false, "协议标志不被支持: %#x", header.Flags)
	}

	if _, err := io.ReadFull(message.Conn, senderLenBuf); err != nil {
		return err
	}
	if _, err := io.ReadFull(message.Conn, accepterLenBuf); err != nil {
		return err
	}
	senderLen := binary.BigEndian.Uint16(senderLenBuf)
	accepterLen := binary.BigEndian.Uint16(accepterLenBuf)

	if senderLen > maxSenderLen || accepterLen > maxAccepterLen {
		return runtimeerr.New(errCodeRawBounds, runtimeerr.SeverityError, false, "sender/accepter 长度超限")
	}

	if senderLen > 0 {
		senderBuf := make([]byte, senderLen)
		if _, err := io.ReadFull(message.Conn, senderBuf); err != nil {
			return err
		}
		header.Sender = string(senderBuf)
	}
	if accepterLen > 0 {
		accepterBuf := make([]byte, accepterLen)
		if _, err := io.ReadFull(message.Conn, accepterBuf); err != nil {
			return err
		}
		header.Accepter = string(accepterBuf)
	}
	return nil
}

func validateHeaderBounds(header *Header) error {
	if header == nil {
		return runtimeerr.New(errCodeRawBounds, runtimeerr.SeverityError, false, "nil header")
	}
	if len(header.Sender) > maxSenderLen {
		return runtimeerr.New(errCodeRawBounds, runtimeerr.SeverityError, false, "sender exceeds %d bytes", maxSenderLen)
	}
	if len(header.Accepter) > maxAccepterLen {
		return runtimeerr.New(errCodeRawBounds, runtimeerr.SeverityError, false, "accepter exceeds %d bytes", maxAccepterLen)
	}
	if header.RouteLen != uint32(len([]byte(header.Route))) {
		return runtimeerr.New(errCodeRawBounds, runtimeerr.SeverityError, false, "route length mismatch (%d != %d)", header.RouteLen, len([]byte(header.Route)))
	}
	if header.RouteLen > maxRouteLen {
		return runtimeerr.New(errCodeRawBounds, runtimeerr.SeverityError, false, "route exceeds %d bytes", maxRouteLen)
	}
	return nil
}

func (message *RawMessage) abortConn() {
	if message == nil {
		return
	}
	if message.Conn != nil {
		_ = message.Conn.SetDeadline(time.Now())
		_ = message.Conn.Close()
	}
	message.DataBuffer = nil
	message.HeaderBuffer = nil
}
