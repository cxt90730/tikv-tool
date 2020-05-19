package main

import (
	"github.com/urfave/cli"
	"os"
	"errors"
	"log"
)

var startKey, endKey string
var maxKeys int
var table string

type GlobalOption struct {
	PDs          string
	IsKeyBytes   bool
	IsValueBytes bool
	IsMsgPack    bool
	Table        string
	Bucket       string
	Object       string
	Version      string
}

var global GlobalOption

func main() {
	app := cli.NewApp()
	app.Name = "TiKV Tool"
	app.Usage = "A simple CLI tool to operate tikv for yig."
	app.Version = "0.0.1"
	app.Action = func(c *cli.Context) error {
		cli.ShowAppHelp(cli.NewContext(app, nil, nil))
		return nil
	}

	// Global options
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "pd",
			Value:       "pd1:2379",
			Usage:       "One or a set of pd addresses. e.g: pd1:2379 or pd1:2379,pd2:2378,pd3:2377",
			Destination: &global.PDs,
		},
		cli.BoolFlag{
			Name:        "keybytes",
			Usage:       "The key is an array of bytes when set or delete. e.g:[1 2 3 4 5]",
			Destination: &global.IsKeyBytes,
		},
		cli.BoolFlag{
			Name:        "valuebytes",
			Usage:       "The value is an array of bytes when set or get. e.g:[1 2 3 4 5]",
			Destination: &global.IsValueBytes,
		},
		cli.BoolFlag{
			Name:        "msgpack",
			Usage:       "Use msgpack to encode(set) or decode(get)",
			Destination: &global.IsMsgPack,
		},
		cli.StringFlag{
			Name:        "table,t",
			Usage:       "Set table prefix",
			Destination: &global.Table,
		},
		cli.StringFlag{
			Name:        "bucket,B",
			Usage:       "Set bucket",
			Destination: &global.Bucket,
		},
		cli.StringFlag{
			Name:        "object,O",
			Usage:       "Set object",
			Destination: &global.Object,
		},
		cli.StringFlag{
			Name:        "versionid,V",
			Usage:       "Set version id",
			Destination: &global.Version,
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
			Usage: "Scan keys.",
			Action: func(c *cli.Context) error {
				if len(c.Args()) > 0 {
					cli.ShowCommandHelp(cli.NewContext(app, nil, nil), "scan")
					return errors.New("Invalid arguments.")
				}
				return ScanFunc(startKey, endKey, maxKeys)
			},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "startkey,s",
					Value:       "",
					Usage:       "Start object key",
					Destination: &startKey,
				},
				cli.StringFlag{
					Name:        "endkey,e",
					Value:       "",
					Usage:       "End object key",
					Destination: &endKey,
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
