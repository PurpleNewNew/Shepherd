package protocol

// DecodePayload decodes a message payload using the generated codecs.
func DecodePayload(messageType uint16, data []byte) (interface{}, error) {
	return unmarshalMessagePayload(messageType, data)
}

// EncodePayload encodes a typed payload using the generated codecs.
func EncodePayload(msg interface{}) ([]byte, error) {
	return marshalMessagePayload(msg)
}

