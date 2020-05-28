package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"encoding/ascii85"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"github.com/uw-labs/protoid"
	"github.com/uw-labs/substrate"
	_ "github.com/uw-labs/substrate/freezer"
	_ "github.com/uw-labs/substrate/kafka"
	_ "github.com/uw-labs/substrate/natsstreaming"
	_ "github.com/uw-labs/substrate/proximo"
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
		{
			Name:  "produce",
			Usage: "produce a message to a stream, reading from STDIN",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "desturl",
					Usage: "substrate URL for message destination",
					Value: fmt.Sprintf("nats-streaming://localhost:4222/subject1?cluster-id=test-cluster"),
				},
			},
			Action: func(c *cli.Context) error {
				err := doProduce(context.Background(), c.String("desturl"))
				if err != nil {
					log.Println(err.Error())
				}
				return nil
			},
		},
		{
			Name:  "drain-no-ack",
			Usage: "read messages from a stream, discarding them and never acking them",
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
				err := doDrainNoAck(context.Background(), c.String("sourceurl"))
				if err != nil {
					log.Println(err.Error())
				}
				return nil
			},
		},
		{
			Name:  "consume-all-raw",
			Usage: "read all messages from a stream, writing each message to STDOUT",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "sourceurl",
					Usage: "substrate URL for message source",
					Value: fmt.Sprintf("nats-streaming://localhost:4222/topic1?cluster-id=test-cluster&queue-group=%s", uuid.New().String()),
				},
				cli.BoolFlag{
					Name:  "newline",
					Usage: "separate each message with a newline",
				},
				cli.StringFlag{
					Name:  "timeout",
					Usage: "After comsuming no new messages for this long, exit.  Set to 0s to disable",
					Value: "0s",
				},
			},
			Action: func(c *cli.Context) error {
				err := consumeAllRaw(context.Background(), c.String("sourceurl"), c.Bool("newline"), c.String("timeout"))
				if err != nil {
					log.Println(err.Error())
				}
				return nil
			},
		},
		{
			Name:  "consume-all-ascii85",
			Usage: "read all messages from a stream, writing each message as a line as ascii85 encoded to STDOUT",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "sourceurl",
					Usage: "substrate URL for message source",
					Value: fmt.Sprintf("nats-streaming://localhost:4222/topic1?cluster-id=test-cluster&queue-group=%s", uuid.New().String()),
				},
				cli.StringFlag{
					Name:  "timeout",
					Usage: "After comsuming no new messages for this long, exit.  Set to 0s to disable",
					Value: "0s",
				},
			},
			Action: func(c *cli.Context) error {
				err := consumeAllAscii85(context.Background(), c.String("sourceurl"), c.String("timeout"))
				if err != nil {
					log.Println(err.Error())
				}
				return nil
			},
		},
		{
			Name:  "consume-all-base64",
			Usage: "read all messages from a stream, writing each message as a line as base64 encoded to STDOUT",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "sourceurl",
					Usage: "substrate URL for message source",
					Value: fmt.Sprintf("nats-streaming://localhost:4222/topic1?cluster-id=test-cluster&queue-group=%s", uuid.New().String()),
				},
				cli.StringFlag{
					Name:  "timeout",
					Usage: "After comsuming no new messages for this long, exit.  Set to 0s to disable",
					Value: "0s",
				},
			},
			Action: func(c *cli.Context) error {
				err := consumeAllBase64(context.Background(), c.String("sourceurl"), c.String("timeout"))
				if err != nil {
					log.Println(err.Error())
				}
				return nil
			},
		},
		{
			Name:  "consume-all-protojson",
			Usage: "read all messages from a stream, naively converting proto to JSON and writing to STDOUT",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "sourceurl",
					Usage: "substrate URL for message source",
					Value: fmt.Sprintf("nats-streaming://localhost:4222/sink.emitter.products?cluster-id=test-cluster&queue-group=%s", uuid.New().String()),
				},
				cli.StringFlag{
					Name:  "timeout",
					Usage: "After comsuming no new messages for this long, exit.  Set to 0s to disable",
					Value: "0s",
				},
			},
			Action: func(c *cli.Context) error {
				err := consumeAllProtoJSON(context.Background(), c.String("sourceurl"), c.String("timeout"))
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

func doProduce(ctx context.Context, destURL string) error {
	dest, err := suburl.NewSink(destURL)
	if err != nil {
		return errors.Wrapf(err, "error connecting to destination %s", destURL)
	}

	x := substrate.NewSynchronousMessageSink(dest)
	defer x.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, os.Stdin); err != nil {
		return err
	}

	msg := &message{buf.Bytes()}

	if err := x.PublishMessage(ctx, msg); err != nil {
		return err
	}

	return nil
}

type message struct {
	data []byte
}

func (m *message) Data() []byte {
	return m.data
}

func doDrainNoAck(ctx context.Context, sourceURL string) error {
	source, err := suburl.NewSource(sourceURL)
	if err != nil {
		return errors.Wrapf(err, "error connecting to source %s", sourceURL)
	}
	defer source.Close()

	g, ctx := errgroup.WithContext(ctx)

	incoming := make(chan substrate.Message, 256)
	acks := make(chan substrate.Message, 256)

	g.Go(func() error {
		return source.ConsumeMessages(ctx, incoming, acks)
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-incoming:
				println("got a message")
			}
		}
	})

	return g.Wait()
}

func consumeAllRaw(ctx context.Context, sourceURL string, newline bool, timeout string) error {
	bw := bufio.NewWriter(os.Stdout)
	defer bw.Flush()
	if err := consumeAllGeneric(ctx, sourceURL, func(m substrate.Message) error {
		if _, err := bw.Write(m.Data()); err != nil {
			return err
		}
		if newline {
			if err := bw.WriteByte('\n'); err != nil {
				return err
			}
		}
		return nil
	}, timeout); err != nil {
		return err
	}
	return bw.Flush()
}

func consumeAllAscii85(ctx context.Context, sourceURL string, timeout string) error {
	bw := bufio.NewWriter(os.Stdout)
	defer bw.Flush()
	if err := consumeAllGeneric(ctx, sourceURL, func(m substrate.Message) error {
		be := ascii85.NewEncoder(bw)
		if _, err := be.Write(m.Data()); err != nil {
			return err
		}
		be.Close()
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
		return nil
	}, timeout); err != nil {
		return err
	}
	return bw.Flush()
}

func consumeAllBase64(ctx context.Context, sourceURL string, timeout string) error {
	bw := bufio.NewWriter(os.Stdout)
	defer bw.Flush()
	if err := consumeAllGeneric(ctx, sourceURL, func(m substrate.Message) error {
		be := base64.NewEncoder(base64.StdEncoding, bw)
		if _, err := be.Write(m.Data()); err != nil {
			return err
		}
		be.Close()
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
		return nil
	}, timeout); err != nil {
		return err
	}
	return bw.Flush()
}

func consumeAllProtoJSON(ctx context.Context, sourceURL string, timeout string) error {
	je := json.NewEncoder(os.Stdout)
	je.SetIndent("  ", "  ")
	return consumeAllGeneric(ctx, sourceURL, func(m substrate.Message) error {
		x, err := protoid.Decode(m.Data())
		if err != nil {
			return err
		}
		return je.Encode(x)
	}, timeout)
}

func consumeAllGeneric(ctx context.Context, sourceURL string, mw func(m substrate.Message) error, timeoutString string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	timeout, err := time.ParseDuration(timeoutString)
	if err != nil {
		return fmt.Errorf("failed to parse timeout value : %s", err.Error())
	}

	source, err := suburl.NewSource(sourceURL)
	if err != nil {
		return errors.Wrapf(err, "error connecting to source %s", sourceURL)
	}
	defer source.Close()

	g, ctx := errgroup.WithContext(ctx)

	incoming := make(chan substrate.Message, 256)
	acks := make(chan substrate.Message, 256)

	g.Go(func() error {
		return source.ConsumeMessages(ctx, incoming, acks)
	})

	var c <-chan time.Time

	var t *time.Timer

	if timeout != 0 {
		t = time.NewTimer(timeout)
		c = t.C
	}

	g.Go(func() error {
		for {
			select {
			case <-c:
				cancel()
			case <-ctx.Done():
				return ctx.Err()
			case m := <-incoming:
				if err := mw(m); err != nil {
					return err
				}
				if timeout != 0 {
					t.Reset(timeout)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case acks <- m:
				}
			}
		}
	})

	if err := g.Wait(); err != nil && err != context.Canceled {
		return err
	}
	return err
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
