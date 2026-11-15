package kv

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sort"
)

// rev is one entry in a key's history.
type rev struct {
	mod     uint64 // revision (Raft log index) of the write
	sub     uint32 // order of writes within one transaction
	create  uint64
	version uint64
	tomb    bool
	value   []byte
	leaseID uint64
}

// keyHist is a key and its history, ascending by (mod, sub). Never empty.
type keyHist struct {
	key  []byte
	revs []rev
}

// Store is the multi-version key-value store. It is not safe for concurrent
// use: like the rest of the node it lives on one serial context.
type Store struct {
	keys        []*keyHist // sorted by key
	rev         uint64     // revision of the latest write
	compact     uint64     // history below the latest entry at or before this is gone
	watchers    map[uint64]*WatchSubscription
	nextWatcher uint64
	notifying   bool
	notifyAgain bool
}

// NewStore returns an empty store.
func NewStore() *Store { return &Store{watchers: make(map[uint64]*WatchSubscription)} }

// Revision returns the revision of the latest write (0 if none).
func (s *Store) Revision() uint64 { return s.rev }

// CompactRevision returns the revision up to which history has been compacted.
func (s *Store) CompactRevision() uint64 { return s.compact }

const latest = math.MaxUint64

func (s *Store) find(key []byte) (int, bool) {
	i := sort.Search(len(s.keys), func(i int) bool { return bytes.Compare(s.keys[i].key, key) >= 0 })
	return i, i < len(s.keys) && bytes.Equal(s.keys[i].key, key)
}

// at returns the newest entry with mod <= at, or nil.
func (h *keyHist) at(at uint64) *rev {
	if at == latest {
		return &h.revs[len(h.revs)-1]
	}
	i := sort.Search(len(h.revs), func(i int) bool { return h.revs[i].mod > at })
	if i == 0 {
		return nil
	}
	return &h.revs[i-1]
}

func (h *keyHist) kvOf(r *rev) KeyValue {
	return KeyValue{Key: h.key, Value: r.value, CreateRevision: r.create, ModRevision: r.mod, Version: r.version}
}

// inRange reports whether key is selected by (start, end): end empty selects
// exactly start, a single zero byte selects everything >= start.
func inRange(key, start, end []byte) bool {
	switch {
	case len(end) == 0:
		return bytes.Equal(key, start)
	case len(end) == 1 && end[0] == 0:
		return bytes.Compare(key, start) >= 0
	}
	return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0
}

// scan calls fn for every live key selected by (start, end) as of revision at,
// in key order, until fn returns false.
func (s *Store) scan(start, end []byte, at uint64, fn func(h *keyHist, r *rev) bool) {
	i, _ := s.find(start)
	for ; i < len(s.keys); i++ {
		h := s.keys[i]
		// find returned the first key >= start, so the first key outside the
		// range is past its end and nothing later can match.
		if !inRange(h.key, start, end) {
			return
		}
		if r := h.at(at); r != nil && !r.tomb {
			if !fn(h, r) {
				return
			}
		}
	}
}

// RangeResult is the outcome of a read.
type RangeResult struct {
	KVs      []KeyValue
	Count    uint64 // total matches, ignoring Limit
	More     bool
	Revision uint64 // the store's latest revision
}

// RangeOptions control a read.
type RangeOptions struct {
	Limit     uint64
	Revision  uint64 // 0 = latest
	KeysOnly  bool
	CountOnly bool
}

func (s *Store) checkReadRevision(r uint64) error {
	switch {
	case r == 0:
		return nil
	case r > s.rev:
		return fmt.Errorf("%w: revision %d, latest is %d", ErrFutureRevision, r, s.rev)
	case r < s.compact:
		return fmt.Errorf("%w: revision %d, compacted through %d", ErrCompacted, r, s.compact)
	}
	return nil
}

// Range reads the keys selected by (key, rangeEnd). The returned data is copied.
func (s *Store) Range(key, rangeEnd []byte, o RangeOptions) (RangeResult, error) {
	if err := s.checkReadRevision(o.Revision); err != nil {
		return RangeResult{}, err
	}
	res := s.rangeAt(key, rangeEnd, o, o.Revision)
	return res, nil
}

func (s *Store) rangeAt(key, rangeEnd []byte, o RangeOptions, at uint64) RangeResult {
	if at == 0 {
		at = latest
	}
	res := RangeResult{Revision: s.rev}
	s.scan(key, rangeEnd, at, func(h *keyHist, r *rev) bool {
		res.Count++
		if o.CountOnly {
			return true
		}
		if o.Limit > 0 && uint64(len(res.KVs)) >= o.Limit {
			res.More = true
			return true // keep counting
		}
		kv := h.kvOf(r).clone()
		if o.KeysOnly {
			kv.Value = nil
		}
		res.KVs = append(res.KVs, kv)
		return true
	})
	return res
}

// -- transactions -----------------------------------------------------------

func (s *Store) evalCompare(c Compare) bool {
	var h *keyHist
	if i, ok := s.find(c.Key); ok {
		h = s.keys[i]
	}
	var r *rev
	if h != nil {
		if r = h.at(latest); r != nil && r.tomb {
			r = nil
		}
	}
	if c.Target == CompareValue {
		if r == nil {
			return false
		}
		return cmpResult(c.Result, bytes.Compare(r.value, c.Value))
	}
	var have uint64 // a missing key compares as zero
	if r != nil {
		switch c.Target {
		case CompareVersion:
			have = r.version
		case CompareCreate:
			have = r.create
		case CompareMod:
			have = r.mod
		}
	}
	switch {
	case have < c.Int:
		return cmpResult(c.Result, -1)
	case have > c.Int:
		return cmpResult(c.Result, 1)
	}
	return cmpResult(c.Result, 0)
}

func cmpResult(r CompareResult, c int) bool {
	switch r {
	case Equal:
		return c == 0
	case NotEqual:
		return c != 0
	case Greater:
		return c > 0
	case Less:
		return c < 0
	}
	return false
}

// ApplyTxn applies t at revision index, which must exceed every earlier one. It
// returns an error, applying nothing, if any op's read revision is invalid.
func (s *Store) ApplyTxn(index uint64, t *Txn) (TxnResult, error) {
	if index <= s.rev {
		return TxnResult{}, invalid("revision %d is not after the store's %d", index, s.rev)
	}
	ok := true
	for _, c := range t.Compares {
		if !s.evalCompare(c) {
			ok = false
			break
		}
	}
	ops := t.Success
	if !ok {
		ops = t.Failure
	}
	// Validate everything that can fail before changing anything, against the
	// state before the transaction: that is what makes the transaction atomic.
	for _, op := range ops {
		if op.Type == OpRange {
			if err := s.checkReadRevision(op.Revision); err != nil {
				return TxnResult{}, err
			}
		}
	}

	res := TxnResult{Succeeded: ok}
	var sub uint32
	wrote := false
	for _, op := range ops {
		switch op.Type {
		case OpPut, OpPutLease:
			res.Results = append(res.Results, s.put(index, &sub, op))
			wrote = true
		case OpDeleteRange:
			r := s.deleteRange(index, &sub, op)
			wrote = wrote || r.Deleted > 0
			res.Results = append(res.Results, r)
		case OpRange:
			// Reads see the transaction's own earlier writes. A historical read
			// (Revision > 0) sees the state as of that revision instead.
			rr := s.rangeAt(op.Key, op.RangeEnd, RangeOptions{Limit: op.Limit, KeysOnly: op.KeysOnly, CountOnly: op.CountOnly}, op.Revision)
			res.Results = append(res.Results, OpResult{Type: OpRange, KVs: rr.KVs, Count: rr.Count, More: rr.More})
		}
	}
	if wrote {
		s.rev = index
		s.notifyWatchers()
	}
	res.Revision = s.rev
	return res, nil
}

func (s *Store) put(index uint64, sub *uint32, op Op) OpResult {
	out := OpResult{Type: OpPut}
	i, found := s.find(op.Key)
	if !found {
		h := &keyHist{key: bytes.Clone(op.Key)}
		s.keys = append(s.keys, nil)
		copy(s.keys[i+1:], s.keys[i:])
		s.keys[i] = h
	}
	h := s.keys[i]
	create, version := index, uint64(1)
	if len(h.revs) > 0 {
		if last := &h.revs[len(h.revs)-1]; !last.tomb {
			if op.PrevKV {
				kv := h.kvOf(last).clone()
				out.PrevKV = &kv
			}
			create, version = last.create, last.version+1
		}
	}
	h.revs = append(h.revs, rev{mod: index, sub: *sub, create: create, version: version, value: bytes.Clone(op.Value), leaseID: op.LeaseID})
	*sub++
	return out
}

func (s *Store) deleteRange(index uint64, sub *uint32, op Op) OpResult {
	out := OpResult{Type: OpDeleteRange}
	var victims []*keyHist
	s.scan(op.Key, op.RangeEnd, latest, func(h *keyHist, _ *rev) bool {
		victims = append(victims, h)
		return true
	})
	for _, h := range victims {
		last := &h.revs[len(h.revs)-1]
		if op.PrevKV {
			out.PrevKVs = append(out.PrevKVs, h.kvOf(last).clone())
		}
		h.revs = append(h.revs, rev{mod: index, sub: *sub, create: last.create, version: last.version, tomb: true})
		*sub++
		out.Deleted++
	}
	return out
}

// deleteLease appends tombstones for every current key attached to leaseID.
// Keys are visited in sorted order so expiration is deterministic on replicas.
func (s *Store) deleteLease(index, leaseID uint64) uint64 {
	var sub uint32
	var deleted uint64
	for _, h := range s.keys {
		last := &h.revs[len(h.revs)-1]
		if last.tomb || last.leaseID != leaseID {
			continue
		}
		h.revs = append(h.revs, rev{mod: index, sub: sub, create: last.create, version: last.version, tomb: true})
		sub++
		deleted++
	}
	if deleted != 0 {
		s.rev = index
		s.notifyWatchers()
	}
	return deleted
}

// Compact discards history below revision r, keeping each key's state as of r.
// A key whose state at r is deleted disappears entirely if nothing newer exists.
func (s *Store) Compact(r uint64) error {
	switch {
	case r <= s.compact:
		return fmt.Errorf("%w: already compacted through %d", ErrCompacted, s.compact)
	case r > s.rev:
		return fmt.Errorf("%w: revision %d, latest is %d", ErrFutureRevision, r, s.rev)
	}
	kept := s.keys[:0]
	for _, h := range s.keys {
		i := sort.Search(len(h.revs), func(i int) bool { return h.revs[i].mod > r })
		// h.revs[:i] are at or before r; h.revs[i-1] is the state as of r.
		if i > 0 {
			drop := i - 1
			if h.revs[drop].tomb {
				drop = i
			}
			h.revs = append(h.revs[:0:0], h.revs[drop:]...)
		}
		if len(h.revs) > 0 {
			kept = append(kept, h)
		}
	}
	for i := len(kept); i < len(s.keys); i++ {
		s.keys[i] = nil
	}
	s.keys = kept
	s.compact = r
	s.notifyWatchers()
	return nil
}

// -- snapshot ----------------------------------------------------------------

const snapshotVersion = 3

// Snapshot serializes the whole store canonically: two stores with the same
// contents produce identical bytes, which is what makes Hash meaningful.
//
//	version u8 | revision | compactRevision | nKeys {key, nRevs {mod sub create version tomb u8, value}}
func (s *Store) Snapshot() []byte {
	return s.snapshotWithVersion(snapshotVersion)
}

func (s *Store) snapshotV1() ([]byte, error) {
	for _, h := range s.keys {
		for _, r := range h.revs {
			if r.leaseID != 0 {
				return nil, errors.New("kv: cannot write legacy snapshot with leased revisions")
			}
		}
	}
	return s.snapshotWithVersion(1), nil
}

func (s *Store) snapshotWithVersion(version byte) []byte {
	e := &enc{b: []byte{version}}
	e.uv(s.rev)
	e.uv(s.compact)
	e.uv(uint64(len(s.keys)))
	for _, h := range s.keys {
		e.bytes(h.key)
		e.uv(uint64(len(h.revs)))
		for _, r := range h.revs {
			e.uv(r.mod)
			e.uv(uint64(r.sub))
			e.uv(r.create)
			e.uv(r.version)
			e.bool(r.tomb)
			e.bytes(r.value)
			if version >= 2 {
				e.uv(r.leaseID)
			}
		}
	}
	return e.b
}

// Hash is a digest of the store's full contents, for comparing replicas.
func (s *Store) Hash() [32]byte { return sha256.Sum256(s.Snapshot()) }

// Restore replaces the store with a snapshot, validating it fully first: on any
// error the store is left untouched.
func (s *Store) Restore(data []byte) error {
	if len(data) == 0 {
		s.replaceFrom(NewStore())
		return nil
	}
	d := &dec{b: data}
	v := d.u8()
	if d.err == nil && v != 1 && v != snapshotVersion {
		return fmt.Errorf("kv: unsupported snapshot version %d", v)
	}
	ns := &Store{rev: d.uv(), compact: d.uv()}
	if d.err == nil && ns.compact > ns.rev {
		d.fail("compact revision %d beyond revision %d", ns.compact, ns.rev)
	}
	nKeys := d.count(len(d.b))
	for i := 0; i < nKeys && d.err == nil; i++ {
		h := &keyHist{key: bytes.Clone(d.bytes(MaxKeySize))}
		if d.err == nil && (len(h.key) == 0 || (i > 0 && bytes.Compare(ns.keys[i-1].key, h.key) >= 0)) {
			d.fail("keys not strictly ascending at %d", i)
		}
		nRevs := d.count(len(d.b))
		if d.err == nil && nRevs == 0 {
			d.fail("key with no history")
		}
		var prev *rev
		for j := 0; j < nRevs && d.err == nil; j++ {
			r := rev{mod: d.uv()}
			sub := d.uv()
			if sub > math.MaxUint32 {
				d.fail("sub revision %d", sub)
			}
			r.sub = uint32(sub)
			r.create, r.version, r.tomb = d.uv(), d.uv(), d.flag()
			r.value = bytes.Clone(d.bytes(MaxValueSize))
			if v >= 2 {
				r.leaseID = d.uv()
			}
			if d.err != nil {
				break
			}
			if r.mod == 0 || r.mod > ns.rev || r.create == 0 || r.create > r.mod || (!r.tomb && r.version == 0) || (r.tomb && len(r.value) != 0) {
				d.fail("inconsistent entry for key %q", h.key)
			}
			if prev != nil && (r.mod < prev.mod || (r.mod == prev.mod && r.sub <= prev.sub)) {
				d.fail("history out of order for key %q", h.key)
			}
			h.revs = append(h.revs, r)
			prev = &h.revs[len(h.revs)-1]
		}
		ns.keys = append(ns.keys, h)
	}
	if d.err == nil && len(d.b) != 0 {
		d.fail("%d trailing bytes", len(d.b))
	}
	if d.err != nil {
		return fmt.Errorf("kv: bad snapshot: %w", d.err)
	}
	s.replaceFrom(ns)
	return nil
}

// replaceFrom installs validated snapshot data while preserving the Store
// identity and its ephemeral subscriptions. This keeps references returned by
// StateMachine.Store valid across snapshot installation.
func (s *Store) replaceFrom(ns *Store) {
	s.replaceFromQuiet(ns)
	s.notifyWatchers()
}

// replaceFromQuiet installs validated data without notifying subscribers. It is
// used by StateMachine.Restore to publish store and session state together.
func (s *Store) replaceFromQuiet(ns *Store) {
	watchers, next := s.watchers, s.nextWatcher
	notifying, again := s.notifying, s.notifyAgain
	*s = *ns
	s.watchers, s.nextWatcher = watchers, next
	s.notifying, s.notifyAgain = notifying, again
}
