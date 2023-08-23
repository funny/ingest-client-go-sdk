package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type BufferedClientConfig struct {
	Endpoint string

	AccessKeyID     string
	AccessKeySecret string

	ClientId string

	Encoding                 string        // json or msgpack, default is json
	NoCompression            bool          // set to true to turn off compression
	CompressionAlgo          string        // default is gzip
	RetryTimeIntervalInitial time.Duration // retry interval initial, default is 100ms
	RetryTimeIntervalMax     time.Duration // retry interval max, default is 5m

	MaxMessagesPerBatch int
	MaxDurationPerBatch time.Duration
	MaxConcurrency      int

	Logger Logger
}

type BufferedClient struct {
	conf   BufferedClientConfig
	client *Client

	inMsgs     chan *Message
	outBatches chan *Messages

	nextBatchID int64

	closed          int64
	closeCh         chan interface{}
	batchingLoopDie chan interface{}
	sendingLoopDie  chan interface{}
}

func NewBufferedClient(config BufferedClientConfig) (*BufferedClient, error) {
	clientConfig := Config{
		Endpoint:                 config.Endpoint,
		AccessKeyID:              config.AccessKeyID,
		AccessKeySecret:          config.AccessKeySecret,
		ClientId:                 config.ClientId,
		Encoding:                 config.Encoding,
		NoCompression:            config.NoCompression,
		CompressionAlgo:          config.CompressionAlgo,
		RetryTimeIntervalInitial: config.RetryTimeIntervalInitial,
		RetryTimeIntervalMax:     config.RetryTimeIntervalMax,
		Logger:                   config.Logger,
	}

	client, err := NewClient(clientConfig)
	if err != nil {
		return nil, err
	}

	if config.MaxMessagesPerBatch == 0 {
		config.MaxMessagesPerBatch = 2000
	}

	if config.MaxDurationPerBatch == 0 {
		config.MaxDurationPerBatch = 50 * time.Millisecond
	}
	if config.MaxConcurrency == 0 {
		config.MaxConcurrency = 10
	}

	bc := &BufferedClient{
		conf:   config,
		client: client,

		inMsgs:     make(chan *Message),
		outBatches: make(chan *Messages),

		closed:          0,
		closeCh:         make(chan interface{}),
		batchingLoopDie: make(chan interface{}),
		sendingLoopDie:  make(chan interface{}),
	}

	go bc.batchingLoop()
	go bc.sendingLoop()

	return bc, nil
}

func (bc *BufferedClient) Send(ctx context.Context, message *Message) error {
	if message == nil {
		return fmt.Errorf("message cannot be nil")
	}
	if atomic.LoadInt64(&bc.closed) == 1 {
		return fmt.Errorf("client was closed")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case bc.inMsgs <- message:
	}
	return nil
}

func (bc *BufferedClient) Close(ctx context.Context) error {
	bc.conf.Logger.Debug("calling close client")
	if atomic.CompareAndSwapInt64(&bc.closed, 0, 1) {
		close(bc.closeCh)
	}

	select {
	case <-bc.sendingLoopDie:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (bc *BufferedClient) batchingLoop() {
	defer bc.conf.Logger.Debug("batching loop exited")
	defer close(bc.batchingLoopDie)
	timer := time.NewTimer(bc.conf.MaxDurationPerBatch)
	defer timer.Stop()

	newBatch := func() *Messages {
		batchID := fmt.Sprintf("b-%d-%d", time.Now().UnixMilli(), bc.nextBatchID)
		bc.nextBatchID++

		return &Messages{BatchId: batchID}
	}

	b := newBatch()

	for {
		select {
		case msg := <-bc.inMsgs:
			b.Messages = append(b.Messages, *msg)

			if len(b.Messages) >= bc.conf.MaxMessagesPerBatch {
				bc.conf.Logger.WithField("batchId", b.BatchId).Debug("seal batch for sending (number of message reach limit)")
				bc.outBatches <- b
				b = newBatch()
				timer.Reset(bc.conf.MaxDurationPerBatch)
			}
		case <-timer.C:
			if len(b.Messages) > 0 {
				bc.conf.Logger.WithField("batchId", b.BatchId).Debug("seal batch for sending (batch live duration reach limit)")
				bc.outBatches <- b
				b = newBatch()
			}
		case <-bc.closeCh:
			if len(b.Messages) > 0 {
				bc.conf.Logger.WithField("batchId", b.BatchId).Debug("seal batch for sending (client is closing)")
				bc.outBatches <- b
			}
			return
		}
	}
}

func (bc *BufferedClient) sendingLoop() {
	defer bc.conf.Logger.Debug("sending loop exited")
	defer close(bc.sendingLoopDie)
	sema := make(chan interface{}, bc.conf.MaxConcurrency)
	wg := sync.WaitGroup{}

	for {
		select {
		case b := <-bc.outBatches:
			sema <- nil
			wg.Add(1)

			go func() {
				defer func() { <-sema; wg.Done() }()
				bc.sendBatch(b)
			}()
		case <-bc.batchingLoopDie:
			wg.Wait()
			return
		}
	}
}

func (bc *BufferedClient) sendBatch(b *Messages) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()

	bc.conf.Logger.WithField("batchId", b.BatchId).WithField("messages", len(b.Messages)).Debug("sending batch")
	err := bc.client.Collect(ctx, b)
	if err != nil {
		bc.conf.Logger.WithField("batchId", b.BatchId).WithField("error", err.Error()).Error("failed to send batch")
		return
	}

	elapsed := time.Since(start)

	bc.conf.Logger.WithField("batchId", b.BatchId).WithField("elapsed", elapsed.String()).Debug("batch successfully sent")
}
