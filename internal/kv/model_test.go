package kv_test

import (
	"bytes"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"sort"
	"testing"

	"github.com/souviks22/continuum/internal/kv"
)

// model is a deliberately naive reference implementation of the store's
// semantics: maps and linear scans, no binary searches or in-place slice tricks.
// Agreement between two independent implementations on thousands of random
// histories is what catches the off-by-one in the clever one.
type mrev struct {
	mod, create, version uint64
	sub                  int
	tomb                 bool
	value                string
}

type model struct {
	keys         map[string][]mrev
	rev, compact uint64
}

func newModel() *model { return &model{keys: map[string][]mrev{}} }

func (m *model) sortedKeys() []string {
	var ks []string
	for k := range m.keys {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// view returns the newest live entry of key as of revision at (0 = now).
func (m *model) view(key string, at uint64) *mrev {
	var best *mrev
	for i := range m.keys[key] {
		r := &m.keys[key][i]
		if at == 0 || r.mod <= at {
			best = r
		}
	}
	if best == nil || best.tomb {
		return nil
	}
	return best
}

func inSel(k, start, end string) bool {
	switch {
	case end == "":
		return k == start
	case end == "\x00":
		return k >= start
	}
	return k >= start && k < end
}

func (m *model) kvOf(k string, r *mrev) kv.KeyValue {
	return kv.KeyValue{Key: []byte(k), Value: []byte(r.value), CreateRevision: r.create, ModRevision: r.mod, Version: r.version}
}

func (m *model) selectKVs(start, end string, at uint64) []kv.KeyValue {
	var out []kv.KeyValue
	for _, k := range m.sortedKeys() {
		if inSel(k, start, end) {
			if r := m.view(k, at); r != nil {
				out = append(out, m.kvOf(k, r))
			}
		}
	}
	return out
}

func (m *model) readRevErr(r uint64) error {
	if r == 0 {
		return nil
	}
	if r > m.rev {
		return kv.ErrFutureRevision
	}
	if r < m.compact {
		return kv.ErrCompacted
	}
	return nil
}

func (m *model) holds(c kv.Compare) bool {
	r := m.view(string(c.Key), 0)
	cmp := func(x int) bool {
		switch c.Result {
		case kv.Equal:
			return x == 0
		case kv.NotEqual:
			return x != 0
		case kv.Greater:
			return x > 0
		}
		return x < 0
	}
	if c.Target == kv.CompareValue {
		if r == nil {
			return false
		}
		return cmp(bytes.Compare([]byte(r.value), c.Value))
	}
	var have uint64
	if r != nil {
		have = map[kv.CompareTarget]uint64{kv.CompareVersion: r.version, kv.CompareCreate: r.create, kv.CompareMod: r.mod}[c.Target]
	}
	switch {
	case have < c.Int:
		return cmp(-1)
	case have > c.Int:
		return cmp(1)
	}
	return cmp(0)
}

func (m *model) apply(idx uint64, t kv.Txn) (kv.TxnResult, error) {
	if idx <= m.rev {
		return kv.TxnResult{}, kv.ErrInvalidCommand
	}
	ok := true
	for _, c := range t.Compares {
		ok = ok && m.holds(c)
	}
	ops := t.Success
	if !ok {
		ops = t.Failure
	}
	for _, op := range ops {
		if op.Type == kv.OpRange {
			if err := m.readRevErr(op.Revision); err != nil {
				return kv.TxnResult{}, err
			}
		}
	}
	res := kv.TxnResult{Succeeded: ok}
	sub, wrote := 0, false
	for _, op := range ops {
		key, end := string(op.Key), string(op.RangeEnd)
		switch op.Type {
		case kv.OpPut:
			o := kv.OpResult{Type: kv.OpPut}
			create, version := idx, uint64(1)
			if cur := m.view(key, 0); cur != nil {
				create, version = cur.create, cur.version+1
				if op.PrevKV {
					p := m.kvOf(key, cur)
					o.PrevKV = &p
				}
			}
			m.keys[key] = append(m.keys[key], mrev{mod: idx, sub: sub, create: create, version: version, value: string(op.Value)})
			sub++
			wrote = true
			res.Results = append(res.Results, o)
		case kv.OpDeleteRange:
			o := kv.OpResult{Type: kv.OpDeleteRange}
			for _, k := range m.sortedKeys() {
				if !inSel(k, key, end) {
					continue
				}
				cur := m.view(k, 0)
				if cur == nil {
					continue
				}
				if op.PrevKV {
					o.PrevKVs = append(o.PrevKVs, m.kvOf(k, cur))
				}
				m.keys[k] = append(m.keys[k], mrev{mod: idx, sub: sub, create: cur.create, version: cur.version, tomb: true})
				sub++
				o.Deleted++
				wrote = true
			}
			res.Results = append(res.Results, o)
		case kv.OpRange:
			all := m.selectKVs(key, end, op.Revision)
			o := kv.OpResult{Type: kv.OpRange, Count: uint64(len(all))}
			if !op.CountOnly {
				for i, x := range all {
					if op.Limit > 0 && uint64(i) >= op.Limit {
						o.More = true
						break
					}
					if op.KeysOnly {
						x.Value = nil
					}
					o.KVs = append(o.KVs, x)
				}
			}
			res.Results = append(res.Results, o)
		}
	}
	if wrote {
		m.rev = idx
	}
	res.Revision = m.rev
	return res, nil
}

func (m *model) doCompact(r uint64) error {
	if r <= m.compact {
		if r > 0 {
			return nil // replicated compaction retries are idempotent
		}
		return kv.ErrCompacted
	}
	if r > m.rev {
		return kv.ErrFutureRevision
	}
	for k, revs := range m.keys {
		last := -1
		for i, x := range revs {
			if x.mod <= r {
				last = i
			}
		}
		if last < 0 {
			continue
		}
		keep := revs[last:]
		if keep[0].tomb {
			keep = keep[1:]
		}
		if len(keep) == 0 {
			delete(m.keys, k)
		} else {
			m.keys[k] = append([]mrev(nil), keep...)
		}
	}
	m.compact = r
	return nil
}

// -- random workload ------------------------------------------------------------

var alphabet = []string{"a", "aa", "ab", "b", "b/1", "b/2", "b/3", "c", "z", "zz"}

func pick(r *mrand.Rand) string { return alphabet[r.IntN(len(alphabet))] }

func randomRange(r *mrand.Rand, rev, compact uint64) kv.Op {
	op := kv.Op{Type: kv.OpRange, Key: b(pick(r))}
	switch r.IntN(4) {
	case 0: // point
	case 1:
		op.RangeEnd = b(pick(r))
	case 2:
		op.RangeEnd = kv.PrefixEnd(op.Key)
	case 3:
		op.RangeEnd = []byte{0}
	}
	if r.IntN(4) == 0 {
		op.Limit = uint64(1 + r.IntN(3))
	}
	if r.IntN(4) == 0 { // sometimes valid, sometimes compacted or future
		op.Revision = uint64(r.IntN(int(rev) + 4))
	}
	op.KeysOnly, op.CountOnly = r.IntN(6) == 0, r.IntN(8) == 0
	return op
}

func randomOps(r *mrand.Rand, n int, rev, compact uint64) []kv.Op {
	var ops []kv.Op
	for i := 0; i < n; i++ {
		switch r.IntN(5) {
		case 0, 1:
			v := ""
			if r.IntN(5) > 0 {
				v = fmt.Sprintf("v%d", r.IntN(1000))
			}
			ops = append(ops, kv.Op{Type: kv.OpPut, Key: b(pick(r)), Value: b(v), PrevKV: r.IntN(3) == 0})
		case 2:
			op := kv.Op{Type: kv.OpDeleteRange, Key: b(pick(r)), PrevKV: r.IntN(3) == 0}
			switch r.IntN(3) {
			case 1:
				op.RangeEnd = b(pick(r))
			case 2:
				op.RangeEnd = kv.PrefixEnd(op.Key)
			}
			ops = append(ops, op)
		default:
			ops = append(ops, randomRange(r, rev, compact))
		}
	}
	return ops
}

func randomCompare(r *mrand.Rand, rev uint64) kv.Compare {
	c := kv.Compare{Key: b(pick(r)), Result: kv.CompareResult(1 + r.IntN(4))}
	switch r.IntN(4) {
	case 0:
		c.Target, c.Int = kv.CompareVersion, uint64(r.IntN(4))
	case 1:
		c.Target, c.Int = kv.CompareCreate, uint64(r.IntN(int(rev)+3))
	case 2:
		c.Target, c.Int = kv.CompareMod, uint64(r.IntN(int(rev)+3))
	case 3:
		c.Target, c.Value = kv.CompareValue, b(fmt.Sprintf("v%d", r.IntN(1000)))
		if r.IntN(3) == 0 {
			c.Value = nil
		}
	}
	return c
}

func randomTxn(r *mrand.Rand, rev, compact uint64) kv.Txn {
	t := kv.Txn{}
	for i := r.IntN(3); i > 0; i-- {
		t.Compares = append(t.Compares, randomCompare(r, rev))
	}
	t.Success = randomOps(r, r.IntN(4), rev, compact)
	t.Failure = randomOps(r, r.IntN(3), rev, compact)
	return t
}

func kindOf(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, kv.ErrCompacted):
		return "compacted"
	case errors.Is(err, kv.ErrFutureRevision):
		return "future"
	case errors.Is(err, kv.ErrInvalidCommand):
		return "invalid"
	}
	return err.Error()
}

func dump(kvs []kv.KeyValue) string { return summary(kvs) }

func TestStoreAgreesWithTheNaiveModelOnRandomHistories(t *testing.T) {
	seeds := uint64(300)
	if testing.Short() {
		seeds = 40
	}
	for seed := uint64(1); seed <= seeds; seed++ {
		r := mrand.New(mrand.NewPCG(seed, 99))
		e, m := newEnv(t), newModel()
		for step := 0; step < 150; step++ {
			rev, comp := e.sm.Store().Revision(), e.sm.Store().CompactRevision()
			if r.IntN(3) == 0 {
				e.skip(uint64(1 + r.IntN(3)))
			}
			var gotRes kv.TxnResult
			var gotErr, wantErr error
			var wantRes kv.TxnResult
			desc := ""
			if r.IntN(9) == 0 {
				target := uint64(r.IntN(int(rev) + 3))
				gotRes, gotErr = e.apply(kv.EncodeCompact(target))
				if wantErr = m.doCompact(target); wantErr == nil {
					wantRes = kv.TxnResult{Succeeded: true, Revision: m.rev}
				}
				desc = fmt.Sprintf("compact(%d)", target)
			} else {
				txn := randomTxn(r, rev, comp)
				cmd, err := txn.Encode(kv.Limits{})
				if err != nil {
					t.Fatalf("seed %d: generated an unencodable txn: %v", seed, err)
				}
				gotRes, gotErr = e.apply(cmd)
				wantRes, wantErr = m.apply(e.idx, txn)
				desc = fmt.Sprintf("txn %+v", txn)
			}
			if kindOf(gotErr) != kindOf(wantErr) {
				t.Fatalf("seed %d step %d %s: store error %q, model error %q", seed, step, desc, kindOf(gotErr), kindOf(wantErr))
			}
			if gotErr == nil && !sameResult(gotRes, wantRes) {
				t.Fatalf("seed %d step %d %s:\n store: %+v\n model: %+v", seed, step, desc, norm(gotRes), norm(wantRes))
			}
			if e.sm.Store().Revision() != m.rev || e.sm.Store().CompactRevision() != m.compact {
				t.Fatalf("seed %d step %d: store at rev %d/compact %d, model at %d/%d", seed, step, e.sm.Store().Revision(), e.sm.Store().CompactRevision(), m.rev, m.compact)
			}

			if step%10 == 9 {
				checkStateMatches(t, seed, step, e, m, r)
				checkSnapshotRoundTrip(t, seed, step, e, m, r)
			}
		}
	}
}

func checkStateMatches(t *testing.T, seed uint64, step int, e *env, m *model, r *mrand.Rand) {
	t.Helper()
	revs := []uint64{0, m.rev}
	for i := 0; i < 3 && m.rev >= m.compact; i++ {
		revs = append(revs, m.compact+uint64(r.IntN(int(m.rev-m.compact)+1)))
	}
	for _, at := range revs {
		got, err := e.sm.Store().Range([]byte{0}, []byte{0}, kv.RangeOptions{Revision: at})
		if err != nil {
			t.Fatalf("seed %d step %d: read at %d: %v", seed, step, at, err)
		}
		if want := m.selectKVs("\x00", "\x00", at); dump(got.KVs) != dump(want) {
			t.Fatalf("seed %d step %d: state as of %d differs\n store: %s\n model: %s", seed, step, at, dump(got.KVs), dump(want))
		}
	}
}

// A restored store must be indistinguishable from the original, now and as the
// two keep evolving: this is what makes snapshot-based recovery safe. The same
// commands go to the live store, the restored twin and the model, which must all
// agree; the twin is then discarded and the live store and model carry on.
func checkSnapshotRoundTrip(t *testing.T, seed uint64, step int, e *env, m *model, r *mrand.Rand) {
	t.Helper()
	snap, _ := e.sm.Snapshot()
	twin := kv.NewStateMachine()
	if err := twin.Restore(snap); err != nil {
		t.Fatalf("seed %d step %d: restore: %v", seed, step, err)
	}
	if twin.Store().Hash() != e.sm.Store().Hash() {
		t.Fatalf("seed %d step %d: a restored store differs from the original", seed, step)
	}
	for i := 0; i < 3; i++ {
		rev, comp := e.sm.Store().Revision(), e.sm.Store().CompactRevision()
		txn := randomTxn(r, rev, comp)
		cmd, _ := txn.Encode(kv.Limits{})
		e.idx++
		live := e.sm.Apply(e.idx, cmd)
		if !bytes.Equal(live, twin.Apply(e.idx, cmd)) {
			t.Fatalf("seed %d step %d: the original and the restored store answered differently", seed, step)
		}
		want, wantErr := m.apply(e.idx, txn)
		got, gotErr := kv.DecodeResult(live)
		if kindOf(gotErr) != kindOf(wantErr) || (gotErr == nil && !sameResult(got, want)) {
			t.Fatalf("seed %d step %d: store and model disagree during the snapshot check", seed, step)
		}
	}
	if twin.Store().Hash() != e.sm.Store().Hash() {
		t.Fatalf("seed %d step %d: the restored store drifted from the original", seed, step)
	}
}
