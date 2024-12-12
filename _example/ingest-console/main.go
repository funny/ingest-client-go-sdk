/**
 * ingest-console 是一个实用工具，它支持从标准输入（stdin）读取数据，然后发送到 ingest。
 * 使用方法：
 *
 *    echo '{"type": "Event", "data": {"#event": "login", "#time": 1728904200000}}' | ingest-console -endpoint https://ingest.zh-cn.xmfunny.com -access-key-id xxx -access-key-secret yyy
 */
package main

import (
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

	err := ingestConsole(
		*endpoint,
		*accessKeyId,
		*accessKeySecret,
		*batchSize,
		*concurrency,
	)
	if err != nil {
		panic(err)
	}
}

func ingestConsole(
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
		panic(err)
	}

	dec := json.NewDecoder(os.Stdin)
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
					defer atomic.AddUint64(&sent, uint64(len(msgs.Messages)))
					return c.Collect(ctx, &msgsCopy)
				})
			}
		}
		if len(msgs.Messages) > 0 {
			defer atomic.AddUint64(&sent, uint64(len(msgs.Messages)))
			return c.Collect(ctx, &msgs)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return err
	}
	slog.Info("message sent", "count", sent)
	return nil
}
