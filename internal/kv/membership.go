package kv

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// MembershipDigest returns a canonical digest of a Raft configuration for use
// in a compatibility activation command. Each category is sorted and encoded
// with its own length so no two configurations share an encoding.
func MembershipDigest(voters, learners, votersOutgoing, learnersNext []uint64, autoLeave bool) [32]byte {
	b := make([]byte, 0, 16+8*(len(voters)+len(learners)+len(votersOutgoing)+len(learnersNext)))
	for _, group := range [][]uint64{voters, learners, votersOutgoing, learnersNext} {
		ids := append([]uint64(nil), group...)
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		b = binary.AppendUvarint(b, uint64(len(ids)))
		for _, id := range ids {
			b = binary.AppendUvarint(b, id)
		}
	}
	if autoLeave {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return sha256.Sum256(b)
}
