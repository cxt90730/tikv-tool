package main

import (
	"github.com/urfave/cli"
	"fmt"
	"os"
	"errors"
	"log"
	. "github.com/journeymidnight/tikv-tool/tikv"
	"strings"
	"strconv"
)

var startKey string
var maxKeys int
var pds string
var isBytes bool

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

func SetFunc(key, value string) error {
	c := NewClient(pds)
	var k, v []byte
	var err error
	if isBytes {
		k, err = ParseToBytes(key)
		if err != nil {
			return err
		}
		v, err = ParseToBytes(value)
		if err != nil {
			return err
		}
	} else {
		k, v = []byte(key), []byte(value)
	}
	return c.TxPut(k, v)
}

//TODO: Show different struct
func GetFunc(key string) error {
	c := NewClient(pds)
	var k []byte
	var err error
	if isBytes {
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
	fmt.Println(string(KV.V))
	return nil
}

var (
	u_prefix = "u\\"
	b_prefix = "b\\"
	m_prefix = "m\\"
	p_prefix = "p\\"
)

func ScanFunc(table string) error {
	var prefix string
	switch TableMap[table] {
	case TableBucket:
		prefix = b_prefix
	case TableUser:
		prefix = u_prefix
	case TableMultipart:
		prefix = m_prefix
	case TableObject:
		prefix =  ""
	case TablePart:
		prefix = prefix
	default:
		return errors.New("Invalid table name.")
	}
	c := NewClient(pds)
	c.ScanAll(prefix, startKey, maxKeys)
	return nil
}

func DelFunc(key string) error {
	c := NewClient(pds)
	var k []byte
	var err error
	if isBytes {
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

func main() {
	app := cli.NewApp()
	app.Name = "TiKV Tool"
	app.Usage = "A simple CLI tool to operate tikv for yig."
	app.Version = "0.0.1"
	app.Action = func(c *cli.Context) error {
		cli.ShowAppHelp(cli.NewContext(app, nil, nil))
		return nil
	}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "pd",
			Value:       "pd1:2379",
			Usage:       "One or a set of pd addresses. e.g: pd1:2379 or pd1:2379,pd2:2378,pd3:2377",
			Destination: &pds,
		},
		cli.BoolFlag{
			Name:        "bytes",
			Usage:       "The key or value is an array of bytes. e.g:[1 2 3 4 5]",
			Destination: &isBytes,
		},
	}

	app.Commands = []cli.Command{
		{
			Name:  "set",
			Usage: "Set a key",
			Action: func(c *cli.Context) error {
				if len(c.Args()) != 2 {
					cli.ShowCommandHelp(cli.NewContext(app, nil, nil), "set")
					return errors.New("Invalid arguments.")
				}
				return SetFunc(c.Args()[0], c.Args()[1])
			},
			ArgsUsage: "<key> <value>",
		},
		{
			Name:  "get",
			Usage: "Get a key",
			Action: func(c *cli.Context) error {
				if len(c.Args()) != 1 {
					cli.ShowCommandHelp(cli.NewContext(app, nil, nil), "get")
					return errors.New("Invalid arguments.")
				}
				return GetFunc(c.Args()[0])
			},
			ArgsUsage: "<key>",
		},
		{
			Name:  "scan",
			Usage: "Scan table keys. Table name currently has: bucket, object, user, multipart, part",
			Action: func(c *cli.Context) error {
				if len(c.Args()) != 1 {
					cli.ShowCommandHelp(cli.NewContext(app, nil, nil), "scan")
					return errors.New("Invalid arguments.")
				}
				return ScanFunc(c.Args()[0])
			},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "startkey,s",
					Value:       "",
					Usage:       "Start object key",
					Destination: &startKey,
				},

				cli.IntFlag{
					Name:        "limit,l",
					Value:       1000,
					Usage:       "Max object keys",
					Destination: &maxKeys,
				},
			},
			ArgsUsage: "<table> [options...]",
		},
		{
			Name:  "del",
			Usage: "Delete a key",
			Action: func(c *cli.Context) error {
				if len(c.Args()) != 1 {
					return errors.New("Invalid arguments.")
				}
				return DelFunc(c.Args()[0])
			},
			ArgsUsage: "<key>",
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
