package shadow

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup/engine"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// LeaderCheck is the interface for checking if this node is the leader.
// This is a subset of conductor.SequencerConductor.
type LeaderCheck interface {
	Leader(ctx context.Context) (bool, error)
}

// Forwarder implements engine.ShadowForwarder by forwarding Engine API calls
// to a list of shadow engines in a fire-and-forget manner.
// If a conductor is configured, it only forwards when this node is the leader.
type Forwarder struct {
	shadows   []*ShadowEngine
	conductor LeaderCheck
	log       log.Logger
}

// Compile-time check that Forwarder implements engine.ShadowForwarder.
var _ engine.ShadowForwarder = (*Forwarder)(nil)

// NewForwarder creates a new Forwarder that forwards to the given shadow engines.
// If conductor is non-nil, forwarding only occurs when this node is the leader.
func NewForwarder(shadows []*ShadowEngine, conductor LeaderCheck, log log.Logger) *Forwarder {
	return &Forwarder{
		shadows:   shadows,
		conductor: conductor,
		log:       log,
	}
}

// isLeader checks if this node is the leader. Returns true if no conductor is configured.
func (f *Forwarder) isLeader() bool {
	if f.conductor == nil {
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	isLeader, err := f.conductor.Leader(ctx)
	if err != nil {
		f.log.Warn("Failed to check conductor leadership for shadow forwarding", "err", err)
		return false // Don't forward if we can't determine leadership
	}
	return isLeader
}

// ForwardForkchoiceUpdate queues a ForkchoiceUpdate call to all shadow engines.
func (f *Forwarder) ForwardForkchoiceUpdate(fc *eth.ForkchoiceState, attr *eth.PayloadAttributes) {
	if !f.isLeader() {
		return
	}
	for _, s := range f.shadows {
		s.QueueForkchoiceUpdate(fc, attr)
	}
}

// ForwardNewPayload queues a NewPayload call to all shadow engines.
func (f *Forwarder) ForwardNewPayload(payload *eth.ExecutionPayload, parentBeaconBlockRoot *common.Hash) {
	if !f.isLeader() {
		return
	}
	for _, s := range f.shadows {
		s.QueueNewPayload(payload, parentBeaconBlockRoot)
	}
}

// ForwardGetPayload queues a GetPayload call to all shadow engines.
func (f *Forwarder) ForwardGetPayload(info eth.PayloadInfo) {
	if !f.isLeader() {
		return
	}
	for _, s := range f.shadows {
		s.QueueGetPayload(info)
	}
}

// Start starts all shadow engines.
func (f *Forwarder) Start() {
	for _, s := range f.shadows {
		s.Start()
	}
}

// Stop stops all shadow engines.
func (f *Forwarder) Stop() {
	for _, s := range f.shadows {
		s.Stop()
	}
}
