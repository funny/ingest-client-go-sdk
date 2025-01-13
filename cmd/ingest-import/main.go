package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"log/slog"

	client "github.com/funny/ingest-client-go-sdk/v2"
	"golang.org/x/sync/errgroup"
)

func main() {
	endpoint := flag.String("endpoint", "", "")
	accessKeyId := flag.String("access-key-id", "", "")
	accessKeySecret := flag.String("access-key-secret", "", "")
	batchSize := flag.Int("batch-size", 1000, "")
	concurrency := flag.Int("concurrency", 5, "")

	flag.Parse()

	err := ingestImport(
		*endpoint,
		*accessKeyId,
		*accessKeySecret,
		*batchSize,
		*concurrency,
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func ingestImport(
	endpoint string,
	accessKeyId string,
	accessKeySecret string,
	batchSize int,
	concurrency int,
) error {
	config := client.Config{
		Endpoint:        endpoint,
		AccessKeyID:     accessKeyId,
		AccessKeySecret: accessKeySecret,
	}

	c, err := client.NewClient(config)
	if err != nil {
		return fmt.Errorf("create client: %s", err)
	}

	dec := json.NewDecoder(bufio.NewReader(os.Stdin))
	sent := uint64(0)

	eg, ctx := errgroup.WithContext(context.Background())
	eg.SetLimit(concurrency + 1)
	eg.Go(func() error {
		msgs := client.Messages{}

		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			msg := client.Message{}

			err := dec.Decode(&msg)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("unable to read line: %s", err)
			}
			msgs.Messages = append(msgs.Messages, msg)

			if len(msgs.Messages) == batchSize {
				msgsCopy := msgs
				msgs = client.Messages{}
				eg.Go(func() error {
					defer atomic.AddUint64(&sent, uint64(len(msgsCopy.Messages)))
					return c.Collect(ctx, &msgsCopy)
				})
			}
		}
		if len(msgs.Messages) > 0 {
			err := c.Collect(ctx, &msgs)
			if err != nil {
				return err
			}
			atomic.AddUint64(&sent, uint64(len(msgs.Messages)))
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return err
	}
	slog.Info("message sent", "count", atomic.LoadUint64(&sent))
	return nil
}
