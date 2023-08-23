/**
 * SDK 维护者需要注意，修改该程序需要将其拷贝到 README.md
 */
package main

import (
	"context"
	"os"
	"time"

	client "git.sofunny.io/data-analysis/ingest-client-go-sdk"
)

func main() {
	config := client.BufferedClientConfig{
		Endpoint: "http://localhost:8088",

		AccessKeyID:     "admin",
		AccessKeySecret: "adminSecret",

		// turn on debug log to see what happend underneath
		Logger: client.NewLogger(os.Stderr, client.LevelDebug),
	}

	c, err := client.NewBufferedClient(config)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	defer c.Close(ctx)

	for i := 0; i < 2500; i++ {
		msg := &client.Message{
			Type: "Event",
			Data: map[string]interface{}{
				"#event": "login",
				"#time":  time.Now().UnixMilli(),
				"pid":    42,
				"ip":     "127.0.0.1",
			},
		}

		if err := c.Send(ctx, msg); err != nil {
			panic(err)
		}
	}

	// wait a little bit for data to send
	time.Sleep(200 * time.Millisecond)
}
