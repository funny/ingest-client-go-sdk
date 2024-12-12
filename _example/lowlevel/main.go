/**
 * SDK 维护者需要注意，修改该程序需要将其拷贝到 README.md
 */
package main

import (
	"context"
	"time"

	client "github.com/funny/ingest-client-go-sdk"
)

func main() {
	config := client.Config{
		Endpoint: "http://localhost:8088",

		AccessKeyID:     "admin",
		AccessKeySecret: "adminSecret",
	}

	c, err := client.NewClient(config)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	messages := &client.Messages{
		Messages: []client.Message{{
			Type: "Event",
			Data: map[string]interface{}{
				"event": "login",
				"time":  time.Now().Unix(),
				"pid":   42,
				"ip":    "127.0.0.1",
			},
		}},
	}

	if err := c.Collect(ctx, messages); err != nil {
		panic(err)
	}
}
