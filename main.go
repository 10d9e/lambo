package main

import (
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

func main() { os.Exit(main1()) }

func main1() int {
	app := &cli.App{
		Name:  "car",
		Usage: "Utility for working with car files",
		Commands: []*cli.Command{
			{
				Name:    "create",
				Usage:   "Create a car file",
				Aliases: []string{"c"},
				Action:  CreateCar,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:      "file",
						Aliases:   []string{"f", "output", "o"},
						Usage:     "The car file to write to",
						TakesFile: true,
					},
					&cli.IntFlag{
						Name:  "version",
						Value: 2,
						Usage: "Write output as a v1 or v2 format car",
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}
