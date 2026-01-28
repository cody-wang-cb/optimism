package shadow

import (
	"github.com/ethereum/go-ethereum/common"

	"github.com/ethereum-optimism/optimism/op-node/rollup/engine"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// Forwarder implements engine.ShadowForwarder by forwarding Engine API calls
// to a list of shadow engines in a fire-and-forget manner.
type Forwarder struct {
	shadows []*ShadowEngine
}

// Compile-time check that Forwarder implements engine.ShadowForwarder.
var _ engine.ShadowForwarder = (*Forwarder)(nil)

// NewForwarder creates a new Forwarder that forwards to the given shadow engines.
func NewForwarder(shadows []*ShadowEngine) *Forwarder {
	return &Forwarder{shadows: shadows}
}

// ForwardForkchoiceUpdate queues a ForkchoiceUpdate call to all shadow engines.
func (f *Forwarder) ForwardForkchoiceUpdate(fc *eth.ForkchoiceState, attr *eth.PayloadAttributes) {
	for _, s := range f.shadows {
		s.QueueForkchoiceUpdate(fc, attr)
	}
}

// ForwardNewPayload queues a NewPayload call to all shadow engines.
func (f *Forwarder) ForwardNewPayload(payload *eth.ExecutionPayload, parentBeaconBlockRoot *common.Hash) {
	for _, s := range f.shadows {
		s.QueueNewPayload(payload, parentBeaconBlockRoot)
	}
}

// ForwardGetPayload queues a GetPayload call to all shadow engines.
func (f *Forwarder) ForwardGetPayload(info eth.PayloadInfo) {
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
