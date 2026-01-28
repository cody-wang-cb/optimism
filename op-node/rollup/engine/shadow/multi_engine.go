package shadow

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup/engine"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// MultiEngine wraps a primary ExecEngine and forwards Engine API calls
// to shadow engines in a fire-and-forget manner.
type MultiEngine struct {
	primary  engine.ExecEngine
	shadows  []*ShadowEngine
	log      log.Logger
	started  bool
}

// Compile-time check that MultiEngine implements ExecEngine.
var _ engine.ExecEngine = (*MultiEngine)(nil)

// NewMultiEngine creates a new MultiEngine that wraps the primary engine
// and forwards calls to the given shadow engines.
func NewMultiEngine(
	primary engine.ExecEngine,
	shadows []*ShadowEngine,
	log log.Logger,
) *MultiEngine {
	return &MultiEngine{
		primary: primary,
		shadows: shadows,
		log:     log.New("component", "multi-engine"),
	}
}

// Start begins all shadow engine background goroutines.
func (m *MultiEngine) Start() {
	if m.started {
		return
	}
	for _, s := range m.shadows {
		s.Start()
	}
	m.started = true
	m.log.Info("Started multi-engine with shadow endpoints", "count", len(m.shadows))
}

// Stop signals all shadow engines to stop and waits for them to finish.
func (m *MultiEngine) Stop() {
	if !m.started {
		return
	}
	for _, s := range m.shadows {
		s.Stop()
	}
	m.started = false
	m.log.Info("Stopped multi-engine shadow endpoints")
}

// ForkchoiceUpdate calls primary synchronously and queues to all shadows.
func (m *MultiEngine) ForkchoiceUpdate(
	ctx context.Context,
	fc *eth.ForkchoiceState,
	attr *eth.PayloadAttributes,
) (*eth.ForkchoiceUpdatedResult, error) {
	// Fire-and-forget to shadows BEFORE primary call
	// This ensures shadows get the call even if primary is slow
	for _, s := range m.shadows {
		s.QueueForkchoiceUpdate(fc, attr)
	}

	// Execute primary call synchronously
	return m.primary.ForkchoiceUpdate(ctx, fc, attr)
}

// NewPayload calls primary synchronously and queues to all shadows.
func (m *MultiEngine) NewPayload(
	ctx context.Context,
	payload *eth.ExecutionPayload,
	parentBeaconBlockRoot *common.Hash,
) (*eth.PayloadStatusV1, error) {
	// Fire-and-forget to shadows
	for _, s := range m.shadows {
		s.QueueNewPayload(payload, parentBeaconBlockRoot)
	}

	// Execute primary call synchronously
	return m.primary.NewPayload(ctx, payload, parentBeaconBlockRoot)
}

// GetPayload calls primary synchronously and queues to all shadows.
func (m *MultiEngine) GetPayload(
	ctx context.Context,
	payloadInfo eth.PayloadInfo,
) (*eth.ExecutionPayloadEnvelope, error) {
	// Fire-and-forget to shadows
	for _, s := range m.shadows {
		s.QueueGetPayload(payloadInfo)
	}

	// Execute primary call synchronously
	return m.primary.GetPayload(ctx, payloadInfo)
}

// L2BlockRefByLabel delegates to primary only (read-only, no replication needed).
func (m *MultiEngine) L2BlockRefByLabel(ctx context.Context, label eth.BlockLabel) (eth.L2BlockRef, error) {
	return m.primary.L2BlockRefByLabel(ctx, label)
}

// L2BlockRefByHash delegates to primary only (read-only, no replication needed).
func (m *MultiEngine) L2BlockRefByHash(ctx context.Context, hash common.Hash) (eth.L2BlockRef, error) {
	return m.primary.L2BlockRefByHash(ctx, hash)
}

// Shadows returns the list of shadow engines for inspection.
func (m *MultiEngine) Shadows() []*ShadowEngine {
	return m.shadows
}
