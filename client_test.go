package client

import (
	"context"
	"testing"
	"time"
)

func TestCollect(t *testing.T) {
	conf := DefaultTestConfig()

	t.Run("send-json", func(t *testing.T) {
		sendMessage(t, conf, 1)
	})

	conf.NoCompression = true
	t.Run("send-json-gzip", func(t *testing.T) {
		sendMessage(t, conf, 1)
	})

	conf.Encoding = "msgpack"
	t.Run("send-msgpack-gzip", func(t *testing.T) {
		sendMessage(t, conf, 1)
	})

	conf.NoCompression = false
	t.Run("send-msgpack", func(t *testing.T) {
		sendMessage(t, conf, 1)
	})

}

func sendMessage(t *testing.T, conf Config, num int) {
	// send some requests
	messages := createMessages(num)
	c, err := NewClient(conf)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := c.Collect(ctx, messages); err != nil {
		t.Fatal(err)
	}
}

func createMessages(num int) *Messages {
	messages := &Messages{}
	var msgs []Message
	for i := 0; i < num; i++ {
		message := Message{}
		message.Type = "Event"

		eventMsg := make(map[string]interface{})
		eventMsg["psd"] = i
		eventMsg["user"] = i
		message.Data = eventMsg

		msgs = append(msgs, message)
	}
	messages.Messages = msgs
	messages.BatchId = time.Now().Format(time.RFC3339)
	return messages
}

func DefaultTestConfig() Config {
	return Config{
		Endpoint: "http://localhost:8080",

		AccessKeyID:     "rQJEk4mz6k",
		AccessKeySecret: "CkYyCc==",

		ClientId: "test",

		Encoding:        "json",
		NoCompression:   false,
		CompressionAlgo: "gzip",

		RetryTimeIntervalInitial: 50 * time.Millisecond,

		Logger: DefaultLogger,
	}
}
