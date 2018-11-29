package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"github.com/uw-labs/substrate"
	_ "github.com/uw-labs/substrate/freezer"
	_ "github.com/uw-labs/substrate/natsstreaming"
	"github.com/uw-labs/substrate/suburl"
	"golang.org/x/sync/errgroup"
)

func main() {
	app := cli.NewApp()
	app.Usage = "Message stream management utility"

	app.Commands = []cli.Command{
		{
			Name:  "copy",
			Usage: "copy messages from one steam to another",
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
				cli.BoolFlag{
					Name:  "progress",
					Usage: "Indicate progress by showing message throughput",
				},
			},
			Action: func(c *cli.Context) error {
				err := doCopy(context.Background(), c.String("sourceurl"), c.String("desturl"), c.Bool("progress"))
				if err != nil {
					log.Println(err.Error())
				}
				return nil
			},
		},
		{
			Name:  "drain",
			Usage: "read messages from a stream, discarding them",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "sourceurl",
					Usage: "substrate URL for message source",
					Value: fmt.Sprintf("nats-streaming://localhost:4222/source.customer-products?cluster-id=customer-platform-cluster&queue-group=%s", uuid.New().String()),
				},
				cli.BoolFlag{
					Name:  "progress",
					Usage: "Indicate progress by showing message throughput",
				},
			},
			Action: func(c *cli.Context) error {
				err := doDrain(context.Background(), c.String("sourceurl"), c.Bool("progress"))
				if err != nil {
					log.Println(err.Error())
				}
				return nil
			},
		},
	}

	app.Run(os.Args)
}

func doCopy(ctx context.Context, sourceURL string, destURL string, progress bool) error {

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

	g, ctx := errgroup.WithContext(ctx)

	var acksOut chan substrate.Message
	if progress {
		acksOut = make(chan substrate.Message, 16)

		g.Go(func() error {
			return count(ctx, acks, acksOut)
		})
	} else {
		acksOut = acks
	}

	g.Go(func() error {
		return dest.PublishMessages(ctx, acks, incoming)
	})

	g.Go(func() error {
		return source.ConsumeMessages(ctx, incoming, acksOut)
	})

	return g.Wait()
}

func doDrain(ctx context.Context, sourceURL string, progress bool) error {
	source, err := suburl.NewSource(sourceURL)
	if err != nil {
		return errors.Wrapf(err, "error connecting to source %s", sourceURL)
	}
	defer source.Close()

	g, ctx := errgroup.WithContext(ctx)

	incoming := make(chan substrate.Message, 256)
	acks := make(chan substrate.Message, 256)

	if progress {
		g.Go(func() error {
			return count(ctx, incoming, acks)
		})
	} else {
		g.Go(func() error {
			return relay(ctx, incoming, acks)
		})
	}

	g.Go(func() error {
		return source.ConsumeMessages(ctx, incoming, acks)
	})

	return g.Wait()
}

func count(ctx context.Context, in <-chan substrate.Message, out chan<- substrate.Message) error {

	counter := make(chan struct{}, 128)

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		total := 0
		t := time.NewTicker(1 * time.Second)
		start := time.Now()
		for {
			select {
			case <-counter:
				total++
			case <-ctx.Done():
				t.Stop()
				return nil
			case <-t.C:
				dur := time.Now().Sub(start)
				if dur > time.Second {
					rate := total / int(dur/time.Second)
					fmt.Printf("total messages so far : %d.  Rate : %d per second\n", total, rate)
				}
			}
		}
	})

	g.Go(func() error {
		for {
			var m substrate.Message
			select {
			case m = <-in:
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case out <- m:
			case <-ctx.Done():
				return ctx.Err()
			}
			counter <- struct{}{}
		}
	})

	return g.Wait()
}

func relay(ctx context.Context, in <-chan substrate.Message, out chan<- substrate.Message) error {
	for {
		var m substrate.Message
		select {
		case m = <-in:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case out <- m:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
