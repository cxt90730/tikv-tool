package main

import (
	"fmt"
	"strings"
)

func SetFunc(key, value string) error {
	c := NewClient(global.PDs)
	var k, v = []byte(key), []byte(value)
	var err error
	if global.IsKeyBytes {
		k, err = ParseToBytes(key)
		if err != nil {
			return err
		}
	}
	if global.IsValueBytes {
		v, err = ParseToBytes(value)
		if err != nil {
			return err
		}
	}

	if global.IsMsgPack {
		v, err = MsgPackMarshal(v)
		if err != nil {
			return err
		}
	}
	return c.TxPut(k, v)
}

func GetFunc(key string) error {
	c := NewClient(global.PDs)
	var k []byte
	var err error
	if global.IsKeyBytes {
		k, err = ParseToBytes(key)
		if err != nil {
			return err
		}
	} else {
		k = []byte(key)
	}

	KV, err := c.TxGet(k)
	if err != nil {
		return err
	}

	if global.IsMsgPack {
		var v []byte
		// TODO: transfer type
		err = MsgPackUnMarshal(KV.V, v)
		if err != nil {
			return err
		}
	}

	fmt.Println(string(KV.V))
	return nil
}

func ScanFunc(startKey, endKey string, maxKeys int) (err error) {
	c := NewClient(global.PDs)
	var sk, ek []byte
	sk = []byte(startKey)
	if endKey == "" {
		ek = nil
	} else if strings.Index(endKey, "$") != -1 {
		endKey = strings.ReplaceAll(endKey, "$", string(TableMaxKeySuffix))
		ek = []byte(endKey)
	} else {
		ek = []byte(endKey)
	}

	fmt.Println("Start:", string(sk), "End:", string(ek), "Limit:", maxKeys)
	kvs, err := c.TxScan(sk, ek, maxKeys)
	if err != nil {
		panic(err)
	}

	for _, kv := range kvs {
		fmt.Println(string(kv.K), kv.K)
		fmt.Println(string(kv.V))
		fmt.Println("----------------")
	}
	return nil
}

func DelFunc(key string) error {
	c := NewClient(global.PDs)
	var k []byte
	var err error
	if global.IsKeyBytes {
		k, err = ParseToBytes(key)
		if err != nil {
			return err
		}
	} else {
		k = []byte(key)
	}

	err = c.TxDelete(k)
	if err != nil {
		return err
	}
	fmt.Println("Delete key", string(k), "success.")
	return nil
}
