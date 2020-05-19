package main

import (
	"context"
	"github.com/tikv/client-go/config"
	"fmt"
	"strings"
	"strconv"
	"github.com/tikv/client-go/txnkv"
	"github.com/tikv/client-go/key"
)

type TiKVClient struct {
	txnCli *txnkv.Client
}

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func NewClient(pds string) TiKVClient {
	pd := strings.Split(pds, ",")
	txnCli, err := txnkv.NewClient(context.TODO(), pd, config.Default())
	if err != nil {
		panic(err)
	}
	return TiKVClient{txnCli}
}

// key1 val1 key2 val2 ...
func (c *TiKVClient) TxPut(args ...[]byte) error {
	tx, err := c.txnCli.Begin(context.TODO())
	if err != nil {
		return err
	}

	for i := 0; i < len(args); i += 2 {
		ke, val := args[i], args[i+1]
		err := tx.Set(ke, val)
		if err != nil {
			return err
		}
	}
	return tx.Commit(context.Background())
}

func (c *TiKVClient) TxGet(k []byte) (KV, error) {
	tx, err := c.txnCli.Begin(context.TODO())
	if err != nil {
		return KV{}, err
	}
	v, err := tx.Get(context.TODO(), k)
	if err != nil {
		return KV{}, err
	}
	return KV{K: k, V: v}, nil
}

func (c *TiKVClient) TxDelete(keys ...[]byte) error {
	tx, err := c.txnCli.Begin(context.TODO())
	if err != nil {
		return err
	}
	for _, key := range keys {
		err := tx.Delete(key)
		if err != nil {
			return err
		}
	}
	return tx.Commit(context.Background())
}

func (c *TiKVClient) TxScan(keyPrefix []byte, endKey []byte, limit int) ([]KV, error) {
	tx, err := c.txnCli.Begin(context.TODO())
	if err != nil {
		return nil, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(keyPrefix), key.Key(endKey))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var ret []KV
	for it.Valid() && limit > 0 {
		ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
		limit--
		it.Next(context.TODO())
	}
	return ret, nil
}

func (c *TiKVClient) ScanAll(prefix string, start string, limit int) {
	startKey := []byte(prefix + start)
	endKey := []byte(prefix + string(0xFF))
	kvs, err := c.TxScan(startKey, endKey, limit)
	if err != nil {
		panic(err)
	}

	for _, kv := range kvs {
		fmt.Println(string(kv.K), kv.K)
		fmt.Println(string(kv.V))
		fmt.Println("----------------")
	}
}

func (c *TiKVClient) DeleteBytes(args ...string) {
	for _, arg := range args {
		var data []byte
		sp := strings.Split(arg, " ")
		for i := range sp {
			d, _ := strconv.Atoi(sp[i])
			data = append(data, byte(d))
		}
		c.TxDelete(data)
	}
}

func (c *TiKVClient) DeleteAll(args ...string) {
	for _, arg := range args {
		c.TxDelete([]byte(arg))
	}
}
