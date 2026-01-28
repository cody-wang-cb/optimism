package shadow

import (
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// EngineCallType represents the type of Engine API call.
type EngineCallType int

const (
	CallForkchoiceUpdate EngineCallType = iota
	CallNewPayload
	CallGetPayload
)

func (t EngineCallType) String() string {
	switch t {
	case CallForkchoiceUpdate:
		return "ForkchoiceUpdate"
	case CallNewPayload:
		return "NewPayload"
	case CallGetPayload:
		return "GetPayload"
	default:
		return "Unknown"
	}
}

// EngineCall represents a queued Engine API call to be executed asynchronously.
type EngineCall struct {
	Type      EngineCallType
	Timestamp time.Time

	// ForkchoiceUpdate params
	FC   *eth.ForkchoiceState
	Attr *eth.PayloadAttributes

	// NewPayload params
	Payload               *eth.ExecutionPayload
	ParentBeaconBlockRoot *common.Hash

	// GetPayload params
	PayloadInfo eth.PayloadInfo
}

// NewForkchoiceUpdateCall creates an EngineCall for ForkchoiceUpdate.
func NewForkchoiceUpdateCall(fc *eth.ForkchoiceState, attr *eth.PayloadAttributes) EngineCall {
	return EngineCall{
		Type:      CallForkchoiceUpdate,
		Timestamp: time.Now(),
		FC:        fc,
		Attr:      attr,
	}
}

// NewNewPayloadCall creates an EngineCall for NewPayload.
func NewNewPayloadCall(payload *eth.ExecutionPayload, parentBeaconBlockRoot *common.Hash) EngineCall {
	return EngineCall{
		Type:                  CallNewPayload,
		Timestamp:             time.Now(),
		Payload:               payload,
		ParentBeaconBlockRoot: parentBeaconBlockRoot,
	}
}

// NewGetPayloadCall creates an EngineCall for GetPayload.
func NewGetPayloadCall(info eth.PayloadInfo) EngineCall {
	return EngineCall{
		Type:        CallGetPayload,
		Timestamp:   time.Now(),
		PayloadInfo: info,
	}
}
