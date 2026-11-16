package client

import (
	"errors"
	"time"

	"github.com/souviks22/continuum/internal/kv"
	"github.com/souviks22/continuum/internal/rpc"
)

const compatibilityMethod = "continuum.compatibility.v1"

// Compatibility describes the formats and command kinds supported by one
// KV-enabled node. Reporting support does not itself activate a feature.
type Compatibility struct {
	RPCWireVersion              uint8
	KVCommandVersion            uint8
	KVSnapshotWriteVersion      uint8
	MaxKVCommandKind            uint8
	KVSnapshotReadVersions      []uint8
	SupportedCompatibilityLevel uint8
	ActiveCompatibilityLevel    uint8
	CompatibilityReportVersion  uint8
}

func localCompatibility(machine *kv.StateMachine) Compatibility {
	command, snapshotWrite, maxKind, supported, active, snapshotRead := machine.CompatibilityFormats()
	readVersions := make([]uint8, len(snapshotRead))
	for i, version := range snapshotRead {
		readVersions[i] = uint8(version)
	}
	return Compatibility{
		RPCWireVersion:              rpc.WireVersion,
		KVCommandVersion:            command,
		KVSnapshotWriteVersion:      snapshotWrite,
		MaxKVCommandKind:            maxKind,
		KVSnapshotReadVersions:      readVersions,
		SupportedCompatibilityLevel: supported,
		ActiveCompatibilityLevel:    active,
		CompatibilityReportVersion:  3,
	}
}

func encodeCompatibility(c Compatibility) []byte {
	b := []byte{3, c.RPCWireVersion, c.KVCommandVersion, c.KVSnapshotWriteVersion, c.MaxKVCommandKind, c.SupportedCompatibilityLevel, c.ActiveCompatibilityLevel, byte(len(c.KVSnapshotReadVersions))}
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
	if len(b) >= 6 && b[0] == 2 && int(b[5]) == len(b)-6 {
		readVersions := make([]uint8, len(b)-6)
		for i, version := range b[6:] {
			readVersions[i] = uint8(version)
		}
		return Compatibility{RPCWireVersion: b[1], KVCommandVersion: b[2], KVSnapshotWriteVersion: b[3], MaxKVCommandKind: b[4], KVSnapshotReadVersions: readVersions, CompatibilityReportVersion: 2}, nil
	}
	if len(b) < 8 || b[0] != 3 || int(b[7]) != len(b)-8 {
		return Compatibility{}, errors.New("client: malformed compatibility response")
	}
	readVersions := make([]uint8, len(b)-8)
	for i, version := range b[8:] {
		readVersions[i] = uint8(version)
	}
	return Compatibility{RPCWireVersion: b[1], KVCommandVersion: b[2], KVSnapshotWriteVersion: b[3], MaxKVCommandKind: b[4], SupportedCompatibilityLevel: b[5], ActiveCompatibilityLevel: b[6], KVSnapshotReadVersions: readVersions, CompatibilityReportVersion: 3}, nil
}

// RegisterCompatibilityRPC reports the local formats supported by this
// KV-enabled node. Older binaries that lack this method return ErrUnknownMethod
// to callers, which must treat their capabilities as unknown.
func RegisterCompatibilityRPC(endpoint *rpc.Endpoint, machine *kv.StateMachine, healthy func() error) error {
	if endpoint == nil || machine == nil || healthy == nil {
		return errors.New("client: RegisterCompatibilityRPC requires endpoint, KV state machine and health check")
	}
	return endpoint.Register(compatibilityMethod, func(_ string, request []byte) ([]byte, error) {
		if len(request) != 0 {
			return nil, errors.New("client: compatibility request must be empty")
		}
		if err := healthy(); err != nil {
			return nil, err
		}
		return encodeCompatibility(localCompatibility(machine)), nil
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
