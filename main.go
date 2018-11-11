package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"github.com/uw-labs/substrate"
	_ "github.com/uw-labs/substrate/natsstreaming"
	"github.com/uw-labs/substrate/suburl"
)

func main() {
	app := cli.NewApp()
	app.Name = "plumber"

	app.Commands = []cli.Command{
		{
			Name: "copy",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "sourceurl",
					Usage: "substrate URL for message source",
					Value: fmt.Sprintf("nats-streaming://localhost:4222/source.customer-products?cluster-id=customer-platform-cluster&queue-group=%s", uuid.New().String()),
				},
				cli.StringFlag{
					Name:  "desturl",
					Usage: "substrate URL for message destination",
					Value: fmt.Sprintf("nats-streaming://localhost:4223/subject1?cluster-id=test-cluster"),
				},
			},
			Action: func(c *cli.Context) error {
				err := doCopy(context.Background(), c.String("sourceurl"), c.String("desturl"))
				if err != nil {
					log.Println(err.Error())
				}
				return nil
			},
		},
	}

	app.Run(os.Args)
}

func doCopy(ctx context.Context, sourceURL string, destURL string) error {
	source, err := suburl.NewSource(sourceURL)
	if err != nil {
		return errors.Wrapf(err, "error connecting to source %s", sourceURL)
	}
	defer source.Close()

	dest, err := suburl.NewSink(destURL)
	if err != nil {
		return errors.Wrapf(err, "error connecting to destination %s", destURL)
	}
	defer dest.Close()

	incoming := make(chan substrate.Message, 256)
	acks := make(chan substrate.Message, 16)

	var wg sync.WaitGroup

	pubErrs := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := dest.PublishMessages(ctx, acks, incoming)
		if err != nil {
			pubErrs <- err
		}
	}()

	err = source.ConsumeMessages(context.Background(), incoming, acks)
	if err != nil {
		return err
	}

	wg.Wait()

	return nil
}
