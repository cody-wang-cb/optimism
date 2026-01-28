package config

import (
	"context"
	"errors"
	"time"

	"github.com/ethereum/go-ethereum/log"
	gn "github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/client"
	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
	"github.com/ethereum-optimism/optimism/op-service/sources"
)

type L2EndpointSetup interface {
	// Setup a RPC client to a L2 execution engine to process rollup blocks with.
	Setup(ctx context.Context, log log.Logger, rollupCfg *rollup.Config, metrics opmetrics.RPCMetricer) (cl client.RPC, rpcCfg *sources.EngineClientConfig, err error)
	Check() error
	// ShadowConfig returns the shadow engine configuration, if any.
	ShadowConfig() ShadowEngineConfig
}

type L2EndpointConfig struct {
	// L2EngineAddr is the address of the L2 Engine JSON-RPC endpoint to use. The engine and eth
	// namespaces must be enabled by the endpoint.
	L2EngineAddr string

	// JWT secrets for L2 Engine API authentication during HTTP or initial Websocket communication.
	// Any value for an IPC connection.
	L2EngineJWTSecret [32]byte

	// L2EngineCallTimeout is the default timeout duration for L2 calls.
	// Defines the maximum time a call to the L2 engine is allowed to take before timing out.
	L2EngineCallTimeout time.Duration

	// Shadow contains configuration for fire-and-forget shadow engine replication.
	Shadow ShadowEngineConfig
}

// ShadowEngineConfig contains configuration for shadow EL endpoints
// that receive fire-and-forget Engine API call replication.
type ShadowEngineConfig struct {
	// Addrs is a list of shadow EL RPC endpoint addresses.
	Addrs []string

	// JWTSecrets contains per-endpoint JWT secrets for authentication.
	// Must have the same length as Addrs if non-empty.
	JWTSecrets [][32]byte

	// Timeout is the RPC timeout for shadow engine calls.
	Timeout time.Duration

	// BufferSize is the size of the async call buffer per shadow engine.
	BufferSize int
}

// Enabled returns true if shadow engine replication is configured.
func (cfg *ShadowEngineConfig) Enabled() bool {
	return len(cfg.Addrs) > 0
}

var _ L2EndpointSetup = (*L2EndpointConfig)(nil)

// ShadowConfig returns the shadow engine configuration.
func (cfg *L2EndpointConfig) ShadowConfig() ShadowEngineConfig {
	return cfg.Shadow
}

func (cfg *L2EndpointConfig) Check() error {
	if cfg.L2EngineAddr == "" {
		return errors.New("empty L2 Engine Address")
	}

	return nil
}

func (cfg *L2EndpointConfig) Setup(ctx context.Context, log log.Logger,
	rollupCfg *rollup.Config, metrics opmetrics.RPCMetricer) (client.RPC, *sources.EngineClientConfig, error) {
	if err := cfg.Check(); err != nil {
		return nil, nil, err
	}
	auth := rpc.WithHTTPAuth(gn.NewJWTAuth(cfg.L2EngineJWTSecret))
	opts := []client.RPCOption{
		client.WithGethRPCOptions(auth),
		client.WithDialAttempts(10),
		client.WithCallTimeout(cfg.L2EngineCallTimeout),
		client.WithRPCRecorder(metrics.NewRecorder("engine-api")),
	}
	l2Node, err := client.NewRPC(ctx, log, cfg.L2EngineAddr, opts...)
	if err != nil {
		return nil, nil, err
	}

	return l2Node, sources.EngineClientDefaultConfig(rollupCfg), nil
}

// PreparedL2Endpoints enables testing with in-process pre-setup RPC connections to L2 engines
type PreparedL2Endpoints struct {
	Client client.RPC
}

func (p *PreparedL2Endpoints) Check() error {
	if p.Client == nil {
		return errors.New("client cannot be nil")
	}
	return nil
}

var _ L2EndpointSetup = (*PreparedL2Endpoints)(nil)

func (p *PreparedL2Endpoints) Setup(ctx context.Context, log log.Logger, rollupCfg *rollup.Config, metrics opmetrics.RPCMetricer) (client.RPC, *sources.EngineClientConfig, error) {
	return p.Client, sources.EngineClientDefaultConfig(rollupCfg), nil
}

// ShadowConfig returns an empty shadow config (not supported for prepared endpoints).
func (p *PreparedL2Endpoints) ShadowConfig() ShadowEngineConfig {
	return ShadowEngineConfig{}
}
