package protocol

// DecodePayload 使用生成出来的 codec 对消息载荷进行解码。
func DecodePayload(messageType uint16, data []byte) (interface{}, error) {
	return unmarshalMessagePayload(messageType, data)
}

// EncodePayload 使用生成出来的 codec 对类型化载荷进行编码。
func EncodePayload(msg interface{}) ([]byte, error) {
	return marshalMessagePayload(msg)
}
