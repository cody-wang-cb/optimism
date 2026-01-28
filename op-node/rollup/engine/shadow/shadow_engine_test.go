package shadow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
)

// mockMetrics tracks metrics for testing
type mockMetrics struct {
	calls      atomic.Int32
	dropped    atomic.Int32
	successful atomic.Int32
	failed     atomic.Int32
}

func (m *mockMetrics) RecordShadowEngineCall(endpoint string, method string, success bool, duration time.Duration) {
	m.calls.Add(1)
	if success {
		m.successful.Add(1)
	} else {
		m.failed.Add(1)
	}
}

func (m *mockMetrics) RecordShadowEngineDropped(endpoint string, method string) {
	m.dropped.Add(1)
}

func (m *mockMetrics) RecordShadowEngineQueueDepth(endpoint string, depth int) {}

// mockEngineClient is a mock implementation of EngineClient for testing
type mockEngineClient struct {
	mu              sync.Mutex
	forkchoiceCalls int
	newPayloadCalls int
	getPayloadCalls int
	delay           time.Duration
	err             error
}

func (m *mockEngineClient) ForkchoiceUpdate(ctx context.Context, state *eth.ForkchoiceState, attr *eth.PayloadAttributes) (*eth.ForkchoiceUpdatedResult, error) {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.forkchoiceCalls++
	if m.err != nil {
		return nil, m.err
	}
	return &eth.ForkchoiceUpdatedResult{PayloadStatus: eth.PayloadStatusV1{Status: eth.ExecutionValid}}, nil
}

func (m *mockEngineClient) NewPayload(ctx context.Context, payload *eth.ExecutionPayload, parentBeaconBlockRoot *common.Hash) (*eth.PayloadStatusV1, error) {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.newPayloadCalls++
	if m.err != nil {
		return nil, m.err
	}
	return &eth.PayloadStatusV1{Status: eth.ExecutionValid}, nil
}

func (m *mockEngineClient) GetPayload(ctx context.Context, payloadInfo eth.PayloadInfo) (*eth.ExecutionPayloadEnvelope, error) {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getPayloadCalls++
	if m.err != nil {
		return nil, m.err
	}
	return &eth.ExecutionPayloadEnvelope{ExecutionPayload: &eth.ExecutionPayload{}}, nil
}

func (m *mockEngineClient) totalCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.forkchoiceCalls + m.newPayloadCalls + m.getPayloadCalls
}

func TestShadowEngine_Lifecycle(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	metrics := &mockMetrics{}
	client := &mockEngineClient{}

	se := NewShadowEngine("http://test:8551", client, logger, metrics, time.Second, 10)

	se.Start()
	require.True(t, se.running.Load())

	se.Stop()
	require.Eventually(t, func() bool {
		return !se.running.Load()
	}, time.Second, 10*time.Millisecond)
}

func TestShadowEngine_QueueFullDropsCall(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	metrics := &mockMetrics{}

	// Buffer size of 1, don't start so queue fills up
	se := NewShadowEngine("http://test:8551", nil, logger, metrics, time.Second, 1)

	se.QueueForkchoiceUpdate(&eth.ForkchoiceState{HeadBlockHash: common.HexToHash("0x1")}, nil)
	se.QueueForkchoiceUpdate(&eth.ForkchoiceState{HeadBlockHash: common.HexToHash("0x2")}, nil) // dropped

	require.Equal(t, int32(1), metrics.dropped.Load())
}

func TestShadowEngine_ExecutesCalls(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	metrics := &mockMetrics{}
	client := &mockEngineClient{}

	se := NewShadowEngine("http://test:8551", client, logger, metrics, time.Second, 10)
	se.Start()
	defer se.Stop()

	// Queue all three call types
	se.QueueForkchoiceUpdate(&eth.ForkchoiceState{HeadBlockHash: common.HexToHash("0x1")}, nil)
	se.QueueNewPayload(&eth.ExecutionPayload{BlockHash: common.HexToHash("0x2")}, nil)
	se.QueueGetPayload(eth.PayloadInfo{ID: eth.PayloadID{1, 2, 3, 4, 5, 6, 7, 8}})

	require.Eventually(t, func() bool {
		return client.totalCalls() == 3
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(3), metrics.successful.Load())
}

func TestShadowEngine_RecordsFailures(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	metrics := &mockMetrics{}

	// Test RPC error
	client := &mockEngineClient{err: errors.New("connection refused")}
	se := NewShadowEngine("http://test:8551", client, logger, metrics, time.Second, 10)
	se.Start()

	se.QueueForkchoiceUpdate(&eth.ForkchoiceState{HeadBlockHash: common.HexToHash("0x1")}, nil)

	require.Eventually(t, func() bool {
		return metrics.failed.Load() == 1
	}, time.Second, 10*time.Millisecond)
	se.Stop()

	// Test timeout
	metrics2 := &mockMetrics{}
	client2 := &mockEngineClient{delay: 500 * time.Millisecond}
	se2 := NewShadowEngine("http://test:8551", client2, logger, metrics2, 50*time.Millisecond, 10)
	se2.Start()

	se2.QueueForkchoiceUpdate(&eth.ForkchoiceState{HeadBlockHash: common.HexToHash("0x1")}, nil)

	require.Eventually(t, func() bool {
		return metrics2.failed.Load() == 1
	}, time.Second, 10*time.Millisecond)
	se2.Stop()
}

func TestForwarder(t *testing.T) {
	logger := testlog.Logger(t, log.LevelDebug)
	metrics := &mockMetrics{}
	client1 := &mockEngineClient{}
	client2 := &mockEngineClient{}

	se1 := NewShadowEngine("http://shadow1:8551", client1, logger, metrics, time.Second, 10)
	se2 := NewShadowEngine("http://shadow2:8551", client2, logger, metrics, time.Second, 10)

	forwarder := NewForwarder([]*ShadowEngine{se1, se2})
	forwarder.Start()

	// Forward all three call types
	forwarder.ForwardForkchoiceUpdate(&eth.ForkchoiceState{HeadBlockHash: common.HexToHash("0x1")}, nil)
	forwarder.ForwardNewPayload(&eth.ExecutionPayload{BlockHash: common.HexToHash("0x2")}, nil)
	forwarder.ForwardGetPayload(eth.PayloadInfo{ID: eth.PayloadID{1, 2, 3, 4, 5, 6, 7, 8}})

	// Both shadows should process all 3 calls
	require.Eventually(t, func() bool {
		return client1.totalCalls() == 3 && client2.totalCalls() == 3
	}, time.Second, 10*time.Millisecond)

	forwarder.Stop()
	require.Eventually(t, func() bool {
		return !se1.running.Load() && !se2.running.Load()
	}, time.Second, 10*time.Millisecond)
}
