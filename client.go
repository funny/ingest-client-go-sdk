package client

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"path"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/vmihailenco/msgpack"
)

type Messages struct {
	BatchId  string    `json:"batchId"`
	Messages []Message `json:"messages"`
}

type Message struct {
	Type string      `name:"type" json:"type"`
	Data interface{} `name:"data" json:"data"`
}

type Error struct {
	StatusCode int
	Status     string
	Message    string `json:"error"`
	Errors     []struct {
		Key   string `json:"key"`
		Error string `json:"error"`
	} `json:"errors"`
}

type Config struct {
	Endpoint string

	AccessKeyID     string
	AccessKeySecret string

	ClientId string

	Encoding                 string        // json or msgpack, default is json
	NoCompression            bool          // set to true to turn off compression
	CompressionAlgo          string        // default is gzip
	RetryTimeIntervalInitial time.Duration // retry interval initial, default is 100ms
	RetryTimeIntervalMax     time.Duration // retry interval max, default is 5m

	Logger Logger
}

type Client struct {
	conf       Config
	httpClient *http.Client
	reqCount   int64
}

var (
	fastjson = jsoniter.ConfigCompatibleWithStandardLibrary
)

func NewClient(config Config) (*Client, error) {
	if config.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if config.Logger == nil {
		config.Logger = DefaultLogger
	}
	if config.RetryTimeIntervalInitial == 0 {
		config.RetryTimeIntervalInitial = 100 * time.Millisecond
	}
	if config.RetryTimeIntervalMax == 0 {
		config.RetryTimeIntervalMax = 5 * time.Second
	}

	switch config.Encoding {
	case "json", "msgpack":
	case "":
		config.Encoding = "json"
	default:
		return nil, fmt.Errorf("unkonwn encoding %s", config.Encoding)
	}

	switch config.CompressionAlgo {
	case "gzip":
	case "":
		config.CompressionAlgo = "gzip"
	default:
		return nil, fmt.Errorf("unkonwn compressionAlgo %s", config.CompressionAlgo)
	}

	return &Client{conf: config, httpClient: &http.Client{}}, nil
}

func (c *Client) Collect(ctx context.Context, messages *Messages) error {
	method := "POST"
	api := "/v1/collect"
	timeInterval := c.conf.RetryTimeIntervalInitial
	timeIntervalMax := c.conf.RetryTimeIntervalMax

	// 序列化 && 压缩数据
	data, err := encoding(c.conf.Encoding, &messages)
	if err != nil {
		return err
	}

	if !c.conf.NoCompression {
		data, err = c.compress(data)
		if err != nil {
			return err
		}
	}

retry:
	req, err := http.NewRequest(method, c.conf.Endpoint+api, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	// Workaround 每隔20个请求清理一次连接，这样能够让每一个 ingest server 收到的请求相对均匀一点
	if atomic.AddInt64(&c.reqCount, 1)%20 == 0 {
		req.Close = true
		c.httpClient.CloseIdleConnections()
	}

	req.Header.Set("Content-Type", path.Join("application", c.conf.Encoding))
	req.Header.Set("X-Ingest-Client-ID", c.conf.ClientId)

	if !c.conf.NoCompression {
		req.Header.Set("Content-Encoding", "gzip")
	}

	req = req.WithContext(ctx)
	if err := c.doRequestWithContext(req, method, api, data); err != nil {
		if isCaredError(err) {
			timeInterval = timeInterval * 2
			if timeInterval >= timeIntervalMax {
				timeInterval = timeIntervalMax
			}
			c.conf.Logger.WithField("err", err.Error()).WithField("wait", timeInterval).WithField("batchId", messages.BatchId).Warn("failed to send request, retry later")
			select {
			case <-time.After(timeInterval):
				goto retry
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return err
	}

	return nil
}

func (c *Client) compress(content []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	if _, err := zw.Write(content); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func encoding(encoding string, v interface{}) ([]byte, error) {
	var err error
	var data []byte
	if encoding == "json" {
		data, err = fastjson.Marshal(v)
		if err != nil {
			return nil, err
		}
	} else if encoding == "msgpack" {
		data, err = msgpack.Marshal(v)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("unsupported compression format")
	}
	return data, nil
}

func (c *Client) doRequestWithContext(req *http.Request, method, api string, data []byte) error {
	timestamp := fmt.Sprint(time.Now().Unix())
	nonce := strconv.Itoa(rand.Int())
	log := c.conf.Logger.WithField("method", method).WithField("api", api)

	req.Header.Set("X-AccessKeyId", c.conf.AccessKeyID)
	req.Header.Set("X-Timestamp", timestamp)
	req.Header.Set("X-Nonce", nonce)

	signature := calculateSignature(method, api, c.conf.AccessKeyID, timestamp, nonce, c.conf.AccessKeySecret, data)
	req.Header.Set("X-Signature", base64.StdEncoding.EncodeToString(signature))

	req.Header.Set("User-Agent", "turbine-ingest-client/unknown")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	log.WithField("status", resp.Status).
		WithField("content", string(responseBody)).
		Trace("got response")

	if resp.StatusCode != 200 {
		rerr := Error{}
		err := json.Unmarshal(responseBody, &rerr)
		if err != nil {
			log.WithField("err", err.Error()).WithField("content", string(responseBody)).Warn("unrecognizable response")
			rerr.Message = string(responseBody)
		}
		rerr.StatusCode = resp.StatusCode
		rerr.Status = resp.Status

		return rerr
	}

	return nil
}

func calculateSignature(method, url, accessKeyId, timestamp, nonce, accessKeySecret string, body []byte) []byte {
	h := hmac.New(sha256.New, []byte(accessKeySecret))

	h.Write([]byte(method))
	h.Write([]byte(url))
	h.Write([]byte(accessKeyId))
	h.Write([]byte(nonce))
	h.Write([]byte(timestamp))
	h.Write(body)

	return h.Sum(nil)
}

func (err Error) Error() string {
	return fmt.Sprintf("http code: %v: %v", err.StatusCode, err.Message)
}

func isCaredError(err error) bool {
	var innerErr Error
	switch {
	case errors.As(err, &innerErr):
		if innerErr.StatusCode == http.StatusTooManyRequests ||
			innerErr.StatusCode == http.StatusRequestTimeout {
			return true
		}
		if innerErr.StatusCode == http.StatusBadGateway ||
			innerErr.StatusCode == http.StatusServiceUnavailable ||
			innerErr.StatusCode == http.StatusGatewayTimeout {
			return true
		}
		return false
	case errors.Is(err, syscall.ETIMEDOUT) || errors.Is(err, syscall.ECONNREFUSED):
		return true
	}
	return false
}
