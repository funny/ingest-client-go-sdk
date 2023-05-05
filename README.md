ingest-client-go-sdk
---

## 获取方式

可以通过 go get 的方式来直接获取

```bash
env GOPRIVATE=git.sofunny.io \
	go get git.sofunny.io/data-analysis/ingest-client-go-sdk@latest
```

## 基础用法

```go
package main

import (
	"context"
	"time"

	client "git.sofunny.io/data-analysis/ingest-client-go-sdk"
)

func main() {
	config := client.Config{
		Endpoint: "http://localhost:8088",

		AccessKeyID:     "admin",
		AccessKeySecret: "adminSecret",
	}

	c, err := client.NewClient(config)
	if err != nil {
		return
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
```

## 特性
- 支持序列化，目前支持 JSON 和 msgpack
- 支持压缩操作，目前支持 gzip。压缩支持配置是否开启压缩操作，true 表示不开启，false 表示要开启
- 支持重试机制，当 ingest 那边返回的错误类型为 502、503、504 以及相关网络错误的时候，或者返回的错误类型为 102 （服务器已收到请求并正在处理，可重试），会进行相应的重试操作
- RetryTimeIntervalInitial：配置重试的间隔时间
- RetryTimeIntervalMax：重试时最大的间隔时间
- 重试的总时间会根据传入 Collect 中的 context 的生命周期来控制
- Logger 日志模块
- 支持自动生成 batchID 功能
