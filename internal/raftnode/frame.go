package raftnode

import (
	"encoding/binary"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	hardStateFile = "hardstate"
	walDirName    = "wal"
	snapDirName   = "snap"
	raftMethod    = "raft"

	// A proposal is framed as proposer ID u64 | request ID u64 | command, so the
	// member that proposed it can match the applied result to its caller.
	frameHeader = 16

	snapKindLocal     byte = 0 // taken locally; the log legitimately extends below it
	snapKindInstalled byte = 1 // received from a leader; any older log is superseded
)

func encodeFrame(proposer, req uint64, cmd []byte) []byte {
	b := make([]byte, frameHeader, frameHeader+len(cmd))
	binary.LittleEndian.PutUint64(b[0:8], proposer)
	binary.LittleEndian.PutUint64(b[8:16], req)
	return append(b, cmd...)
}

func decodeFrame(b []byte) (proposer, req uint64, cmd []byte, err error) {
	if len(b) < frameHeader {
		return 0, 0, nil, fmt.Errorf("raftnode: committed entry of %d bytes is not a framed proposal", len(b))
	}
	return binary.LittleEndian.Uint64(b[0:8]), binary.LittleEndian.Uint64(b[8:16]), b[frameHeader:], nil
}

// A snapshot's opaque Extra is: kind byte | serialized ConfState.
func encodeExtra(kind byte, cs *pb.ConfState) ([]byte, error) {
	b, err := proto.Marshal(pb.EnsureConfState(cs))
	if err != nil {
		return nil, err
	}
	return append([]byte{kind}, b...), nil
}

func decodeExtra(b []byte) (byte, *pb.ConfState, error) {
	if len(b) < 1 {
		return 0, nil, fmt.Errorf("raftnode: snapshot has no metadata")
	}
	cs := new(pb.ConfState)
	if err := proto.Unmarshal(b[1:], cs); err != nil {
		return 0, nil, err
	}
	return b[0], cs, nil
}
