package thriftlion

import (
	"bytes"
	"fmt"

	"git.apache.org/thrift.git/lib/go/thrift"
	"go.pedge.io/lion"
)

type encoderDecoder struct {
	encoding         string
	tProtocolFactory thrift.TProtocolFactory
}

func newEncoderDecoder(
	encoding string,
	tProtocolFactory thrift.TProtocolFactory,
) *encoderDecoder {
	return &encoderDecoder{
		encoding,
		tProtocolFactory,
	}
}

func (e *encoderDecoder) Encode(entryMessage *lion.EntryMessage) (*lion.EncodedEntryMessage, error) {
	tStruct, ok := entryMessage.Value.(thrift.TStruct)
	if !ok {
		return nil, fmt.Errorf("thriftlion: %T not a thrift.TStruct", entryMessage.Value)
	}
	value, err := e.marshal(tStruct)
	if err != nil {
		return nil, err
	}
	return &lion.EncodedEntryMessage{
		Encoding: e.encoding,
		Name:     getName(tStruct),
		Value:    value,
	}, nil
}

func (e *encoderDecoder) Name(entryMessage *lion.EntryMessage) (string, error) {
	tStruct, ok := entryMessage.Value.(thrift.TStruct)
	if !ok {
		return "", fmt.Errorf("thriftlion: %T not a thrift.TStruct", entryMessage.Value)
	}
	return getName(tStruct), nil
}

func (e *encoderDecoder) Decode(encodedEntryMessage *lion.EncodedEntryMessage) (*lion.EntryMessage, error) {
	tStruct, err := newTStruct(encodedEntryMessage.Name)
	if err != nil {
		return nil, err
	}
	if err := e.unmarshal(encodedEntryMessage.Value, tStruct); err != nil {
		return nil, err
	}
	return &lion.EntryMessage{
		Encoding: encodedEntryMessage.Encoding,
		Value:    tStruct,
	}, nil
}

func (e *encoderDecoder) marshal(tStruct thrift.TStruct) ([]byte, error) {
	tMemoryBuffer := thrift.NewTMemoryBuffer()
	tProtocol := e.tProtocolFactory.GetProtocol(tMemoryBuffer)
	if err := tStruct.Write(tProtocol); err != nil {
		return nil, err
	}
	return tMemoryBuffer.Bytes(), nil
}

func (e *encoderDecoder) unmarshal(data []byte, tStruct thrift.TStruct) error {
	// TODO(pedge): size is not set within TMemoryBuffer, but from the implementation it does not
	// look like it matters, however this relies on an implementation detail
	tMemoryBuffer := &thrift.TMemoryBuffer{
		Buffer: bytes.NewBuffer(data),
	}
	tProtocol := e.tProtocolFactory.GetProtocol(tMemoryBuffer)
	return tStruct.Read(tProtocol)
}
