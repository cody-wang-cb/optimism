package shadow

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// Metrics defines the metrics interface for shadow engine operations.
type Metrics interface {
	RecordShadowEngineCall(endpoint string, method string, success bool, duration time.Duration)
	RecordShadowEngineDropped(endpoint string, method string)
	RecordShadowEngineQueueDepth(endpoint string, depth int)
}

// EngineClient defines the Engine API methods used by shadow engines.
type EngineClient interface {
	ForkchoiceUpdate(ctx context.Context, state *eth.ForkchoiceState, attr *eth.PayloadAttributes) (*eth.ForkchoiceUpdatedResult, error)
	NewPayload(ctx context.Context, payload *eth.ExecutionPayload, parentBeaconBlockRoot *common.Hash) (*eth.PayloadStatusV1, error)
	GetPayload(ctx context.Context, payloadInfo eth.PayloadInfo) (*eth.ExecutionPayloadEnvelope, error)
}

// ShadowEngine manages fire-and-forget Engine API calls to a single shadow EL endpoint.
type ShadowEngine struct {
	running atomic.Bool

	endpoint string
	client   EngineClient
	log      log.Logger
	metrics  Metrics
	timeout  time.Duration

	// Buffered channel for async calls
	calls chan EngineCall
	stop  chan struct{}
	wg    sync.WaitGroup
}

// NewShadowEngine creates a new ShadowEngine for the given endpoint.
func NewShadowEngine(
	endpoint string,
	client EngineClient,
	log log.Logger,
	metrics Metrics,
	timeout time.Duration,
	bufferSize int,
) *ShadowEngine {
	return &ShadowEngine{
		endpoint: endpoint,
		client:   client,
		log:      log.New("shadow_engine", endpoint),
		metrics:  metrics,
		timeout:  timeout,
		calls:    make(chan EngineCall, bufferSize),
		stop:     make(chan struct{}),
	}
}

// Start begins the background goroutine that processes queued calls.
func (e *ShadowEngine) Start() {
	if !e.running.CompareAndSwap(false, true) {
		return
	}

	e.wg.Add(1)
	go e.loop()
	e.log.Info("Shadow engine started")
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (e *ShadowEngine) Stop() {
	if !e.running.Load() {
		return
	}

	close(e.stop)
	e.wg.Wait()
	e.running.Store(false)
	e.log.Info("Shadow engine stopped")
}

func (e *ShadowEngine) loop() {
	defer e.wg.Done()

	for {
		select {
		case call := <-e.calls:
			e.executeCall(call)
		case <-e.stop:
			// Drain remaining calls with a short timeout
			drainCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			e.drainCalls(drainCtx)
			cancel()
			return
		}
	}
}

func (e *ShadowEngine) drainCalls(ctx context.Context) {
	for {
		select {
		case call := <-e.calls:
			e.executeCall(call)
		case <-ctx.Done():
			return
		default:
			return
		}
	}
}

func (e *ShadowEngine) executeCall(call EngineCall) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	start := time.Now()
	var err error
	method := call.Type.String()

	switch call.Type {
	case CallForkchoiceUpdate:
		_, err = e.client.ForkchoiceUpdate(ctx, call.FC, call.Attr)
	case CallNewPayload:
		_, err = e.client.NewPayload(ctx, call.Payload, call.ParentBeaconBlockRoot)
	case CallGetPayload:
		_, err = e.client.GetPayload(ctx, call.PayloadInfo)
	}

	duration := time.Since(start)
	success := err == nil

	if err != nil {
		e.log.Debug("Shadow engine call failed",
			"method", method,
			"duration", duration,
			"err", err)
	} else {
		e.log.Trace("Shadow engine call succeeded",
			"method", method,
			"duration", duration)
	}

	e.metrics.RecordShadowEngineCall(e.endpoint, method, success, duration)
}

// QueueForkchoiceUpdate queues a ForkchoiceUpdate call (fire-and-forget).
func (e *ShadowEngine) QueueForkchoiceUpdate(fc *eth.ForkchoiceState, attr *eth.PayloadAttributes) {
	e.queueCall(NewForkchoiceUpdateCall(fc, attr))
}

// QueueNewPayload queues a NewPayload call (fire-and-forget).
func (e *ShadowEngine) QueueNewPayload(payload *eth.ExecutionPayload, parentBeaconBlockRoot *common.Hash) {
	e.queueCall(NewNewPayloadCall(payload, parentBeaconBlockRoot))
}

// QueueGetPayload queues a GetPayload call (fire-and-forget).
func (e *ShadowEngine) QueueGetPayload(info eth.PayloadInfo) {
	e.queueCall(NewGetPayloadCall(info))
}

func (e *ShadowEngine) queueCall(call EngineCall) {
	select {
	case e.calls <- call:
		e.metrics.RecordShadowEngineQueueDepth(e.endpoint, len(e.calls))
	default:
		// Channel full, drop the call
		e.log.Warn("Shadow engine queue full, dropping call",
			"method", call.Type.String())
		e.metrics.RecordShadowEngineDropped(e.endpoint, call.Type.String())
	}
}

// Endpoint returns the RPC endpoint address of this shadow engine.
func (e *ShadowEngine) Endpoint() string {
	return e.endpoint
}
