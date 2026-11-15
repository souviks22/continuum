package client

import (
	"errors"
	"time"

	"github.com/souviks22/continuum-go/internal/kv"
	"github.com/souviks22/continuum-go/internal/rpc"
)

const compatibilityMethod = "continuum.compatibility.v1"

// Compatibility describes the formats and command kinds supported by one
// KV-enabled node. Reporting support does not itself activate a feature.
type Compatibility struct {
	RPCWireVersion    uint8
	KVCommandVersion  uint8
	KVSnapshotVersion uint8
	MaxKVCommandKind  uint8
}

func localCompatibility() Compatibility {
	command, snapshot, maxKind := kv.CompatibilityFormats()
	return Compatibility{
		RPCWireVersion:    rpc.WireVersion,
		KVCommandVersion:  command,
		KVSnapshotVersion: snapshot,
		MaxKVCommandKind:  maxKind,
	}
}

func encodeCompatibility(c Compatibility) []byte {
	return []byte{1, c.RPCWireVersion, c.KVCommandVersion, c.KVSnapshotVersion, c.MaxKVCommandKind}
}

func decodeCompatibility(b []byte) (Compatibility, error) {
	if len(b) != 5 || b[0] != 1 {
		return Compatibility{}, errors.New("client: malformed compatibility response")
	}
	return Compatibility{
		RPCWireVersion:    b[1],
		KVCommandVersion:  b[2],
		KVSnapshotVersion: b[3],
		MaxKVCommandKind:  b[4],
	}, nil
}

// RegisterCompatibilityRPC reports the local formats supported by this
// KV-enabled node. Older binaries that lack this method return ErrUnknownMethod
// to callers, which must treat their capabilities as unknown.
func RegisterCompatibilityRPC(endpoint *rpc.Endpoint) error {
	if endpoint == nil {
		return errors.New("client: RegisterCompatibilityRPC requires endpoint")
	}
	return endpoint.Register(compatibilityMethod, func(_ string, request []byte) ([]byte, error) {
		if len(request) != 0 {
			return nil, errors.New("client: compatibility request must be empty")
		}
		return encodeCompatibility(localCompatibility()), nil
	})
}

// QueryCompatibility requests a peer's supported formats. ErrUnknownMethod
// means the peer is older than compatibility reporting and its capabilities
// must be treated as unknown.
func QueryCompatibility(endpoint *rpc.Endpoint, to string, timeout time.Duration, cb func(Compatibility, error)) error {
	if endpoint == nil || to == "" || timeout <= 0 || cb == nil {
		return errors.New("client: compatibility query requires endpoint, peer, timeout and callback")
	}
	return endpoint.Call(to, compatibilityMethod, nil, timeout, func(payload []byte, err error) {
		if err != nil {
			cb(Compatibility{}, err)
			return
		}
		result, err := decodeCompatibility(payload)
		cb(result, err)
	})
}
