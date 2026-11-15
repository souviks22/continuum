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
	RPCWireVersion             uint8
	KVCommandVersion           uint8
	KVSnapshotWriteVersion     uint8
	MaxKVCommandKind           uint8
	KVSnapshotReadVersions     []uint8
	CompatibilityReportVersion uint8
}

func localCompatibility() Compatibility {
	command, snapshotWrite, maxKind, snapshotRead := kv.CompatibilityFormats()
	readVersions := make([]uint8, len(snapshotRead))
	for i, version := range snapshotRead {
		readVersions[i] = uint8(version)
	}
	return Compatibility{
		RPCWireVersion:             rpc.WireVersion,
		KVCommandVersion:           command,
		KVSnapshotWriteVersion:     snapshotWrite,
		MaxKVCommandKind:           maxKind,
		KVSnapshotReadVersions:     readVersions,
		CompatibilityReportVersion: 2,
	}
}

func encodeCompatibility(c Compatibility) []byte {
	b := []byte{2, c.RPCWireVersion, c.KVCommandVersion, c.KVSnapshotWriteVersion, c.MaxKVCommandKind, byte(len(c.KVSnapshotReadVersions))}
	return append(b, c.KVSnapshotReadVersions...)
}

func decodeCompatibility(b []byte) (Compatibility, error) {
	if len(b) == 5 && b[0] == 1 {
		return Compatibility{
			RPCWireVersion:             b[1],
			KVCommandVersion:           b[2],
			KVSnapshotWriteVersion:     b[3],
			MaxKVCommandKind:           b[4],
			CompatibilityReportVersion: 1,
		}, nil
	}
	if len(b) < 6 || b[0] != 2 || int(b[5]) != len(b)-6 {
		return Compatibility{}, errors.New("client: malformed compatibility response")
	}
	readVersions := make([]uint8, len(b)-6)
	for i, version := range b[6:] {
		readVersions[i] = uint8(version)
	}
	return Compatibility{RPCWireVersion: b[1], KVCommandVersion: b[2], KVSnapshotWriteVersion: b[3], MaxKVCommandKind: b[4], KVSnapshotReadVersions: readVersions, CompatibilityReportVersion: 2}, nil
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
