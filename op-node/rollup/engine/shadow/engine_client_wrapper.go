package shadow

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
)

// EngineClientWrapper wraps an EngineClient and forwards Engine API calls
// to shadow engines in a fire-and-forget manner while delegating all other
// methods to the wrapped client.
type EngineClientWrapper struct {
	*sources.EngineClient
	shadows []*ShadowEngine
	log     log.Logger
	started bool
}

// NewEngineClientWrapper creates a wrapper around an EngineClient that
// replicates Engine API calls to the given shadow engines.
func NewEngineClientWrapper(
	client *sources.EngineClient,
	shadows []*ShadowEngine,
	log log.Logger,
) *EngineClientWrapper {
	return &EngineClientWrapper{
		EngineClient: client,
		shadows:      shadows,
		log:          log.New("component", "shadow-engine-wrapper"),
	}
}

// Start begins all shadow engine background goroutines.
func (w *EngineClientWrapper) Start() {
	if w.started {
		return
	}
	for _, s := range w.shadows {
		s.Start()
	}
	w.started = true
	w.log.Info("Started shadow engine wrapper", "shadow_count", len(w.shadows))
}

// Stop signals all shadow engines to stop and waits for them to finish.
func (w *EngineClientWrapper) Stop() {
	if !w.started {
		return
	}
	for _, s := range w.shadows {
		s.Stop()
	}
	w.started = false
	w.log.Info("Stopped shadow engine wrapper")
}

// ForkchoiceUpdate calls the primary client and queues to all shadows.
func (w *EngineClientWrapper) ForkchoiceUpdate(
	ctx context.Context,
	fc *eth.ForkchoiceState,
	attr *eth.PayloadAttributes,
) (*eth.ForkchoiceUpdatedResult, error) {
	// Fire-and-forget to shadows BEFORE primary call
	for _, s := range w.shadows {
		s.QueueForkchoiceUpdate(fc, attr)
	}

	// Execute primary call synchronously
	return w.EngineClient.ForkchoiceUpdate(ctx, fc, attr)
}

// NewPayload calls the primary client and queues to all shadows.
func (w *EngineClientWrapper) NewPayload(
	ctx context.Context,
	payload *eth.ExecutionPayload,
	parentBeaconBlockRoot *common.Hash,
) (*eth.PayloadStatusV1, error) {
	// Fire-and-forget to shadows
	for _, s := range w.shadows {
		s.QueueNewPayload(payload, parentBeaconBlockRoot)
	}

	// Execute primary call synchronously
	return w.EngineClient.NewPayload(ctx, payload, parentBeaconBlockRoot)
}

// GetPayload calls the primary client and queues to all shadows.
func (w *EngineClientWrapper) GetPayload(
	ctx context.Context,
	payloadInfo eth.PayloadInfo,
) (*eth.ExecutionPayloadEnvelope, error) {
	// Fire-and-forget to shadows
	for _, s := range w.shadows {
		s.QueueGetPayload(payloadInfo)
	}

	// Execute primary call synchronously
	return w.EngineClient.GetPayload(ctx, payloadInfo)
}

// Shadows returns the list of shadow engines for inspection.
func (w *EngineClientWrapper) Shadows() []*ShadowEngine {
	return w.shadows
}
