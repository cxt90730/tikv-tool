package main

import (
	"bytes"
	"github.com/ugorji/go/codec"
	"strings"
	"strconv"
	"errors"
	"encoding/binary"
)

func MsgPackMarshal(v interface{}) ([]byte, error) {
	var buf = new(bytes.Buffer)
	enc := codec.NewEncoder(buf, new(codec.MsgpackHandle))
	err := enc.Encode(v)
	return buf.Bytes(), err
}

func MsgPackUnMarshal(data []byte, v interface{}) error {
	var buf = bytes.NewBuffer(data)
	dec := codec.NewDecoder(buf, new(codec.MsgpackHandle))
	return dec.Decode(v)
}

func ParseToBytes(s string) (bs []byte, err error) {
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	ss := strings.Split(s, " ")
	for _, v := range ss {
		i, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		if i < 0 || i > 255 {
			return nil, errors.New("Invalid bytes")
		}
		bs = append(bs, byte(i))
	}
	return
}

func EncodeUint64(i uint64) []byte {
	var bin [8]byte
	binary.BigEndian.PutUint64(bin[:], i)
	return bin[:]
}

func DecodeUint64(bin []byte) uint64 {
	return binary.BigEndian.Uint64(bin)
}
