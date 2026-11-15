package kv_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/souviks22/continuum/internal/kv"
)

// env drives a state machine the way the Raft node does: commands go in as
// bytes at increasing log indexes, results come back as bytes.
type env struct {
	t   *testing.T
	sm  *kv.StateMachine
	idx uint64
}

func newEnv(t *testing.T) *env { return &env{t: t, sm: kv.NewStateMachine(), idx: 10} }

func (e *env) skip(n uint64) { e.idx += n } // log indexes consumed by no-ops, conf changes, ...

func (e *env) apply(cmd []byte) (kv.TxnResult, error) {
	e.idx++
	return kv.DecodeResult(e.sm.Apply(e.idx, cmd))
}

func (e *env) txn(t kv.Txn) kv.TxnResult {
	e.t.Helper()
	b, err := t.Encode(kv.Limits{})
	if err != nil {
		e.t.Fatalf("encode: %v", err)
	}
	r, err := e.apply(b)
	if err != nil {
		e.t.Fatalf("txn failed: %v", err)
	}
	return r
}

func (e *env) txnErr(t kv.Txn) error {
	e.t.Helper()
	b, err := t.Encode(kv.Limits{})
	if err != nil {
		e.t.Fatalf("encode: %v", err)
	}
	_, err = e.apply(b)
	return err
}

func (e *env) put(k, v string) kv.TxnResult {
	e.t.Helper()
	return e.txn(kv.Put([]byte(k), []byte(v)))
}

func (e *env) del(k string) kv.TxnResult {
	e.t.Helper()
	return e.txn(kv.Delete([]byte(k)))
}

func (e *env) compact(rev uint64) error {
	e.t.Helper()
	_, err := e.apply(kv.EncodeCompact(rev))
	return err
}

func (e *env) get(k string) *kv.KeyValue {
	e.t.Helper()
	r, err := e.sm.Store().Range([]byte(k), nil, kv.RangeOptions{})
	if err != nil {
		e.t.Fatalf("get %s: %v", k, err)
	}
	if len(r.KVs) == 0 {
		return nil
	}
	return &r.KVs[0]
}

func (e *env) rangeKVs(start, end string, o kv.RangeOptions) kv.RangeResult {
	e.t.Helper()
	var endB []byte
	if end != "" {
		endB = []byte(end)
	}
	r, err := e.sm.Store().Range([]byte(start), endB, o)
	if err != nil {
		e.t.Fatalf("range: %v", err)
	}
	return r
}

func b(s string) []byte { return []byte(s) }

func getOp(k string) kv.Op { return kv.Op{Type: kv.OpRange, Key: b(k)} }

func putOp(k, v string) kv.Op { return kv.Op{Type: kv.OpPut, Key: b(k), Value: b(v)} }

func modIs(k string, rev uint64) kv.Compare {
	return kv.Compare{Target: kv.CompareMod, Result: kv.Equal, Key: b(k), Int: rev}
}

func summary(kvs []kv.KeyValue) string {
	s := ""
	for _, x := range kvs {
		s += fmt.Sprintf("%s=%s@c%d/m%d/v%d ", x.Key, x.Value, x.CreateRevision, x.ModRevision, x.Version)
	}
	return s
}

func wantErr(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("got %v, want %v", err, target)
	}
}

// norm makes nil and empty slices compare equal so DeepEqual is about content.
func norm(r kv.TxnResult) kv.TxnResult {
	fix := func(kvs []kv.KeyValue) []kv.KeyValue {
		if len(kvs) == 0 {
			return nil
		}
		out := make([]kv.KeyValue, len(kvs))
		for i, x := range kvs {
			if len(x.Value) == 0 {
				x.Value = nil
			}
			out[i] = x
		}
		return out
	}
	out := kv.TxnResult{Succeeded: r.Succeeded, Revision: r.Revision}
	for _, o := range r.Results {
		o.KVs, o.PrevKVs = fix(o.KVs), fix(o.PrevKVs)
		if o.PrevKV != nil {
			c := *o.PrevKV
			if len(c.Value) == 0 {
				c.Value = nil
			}
			o.PrevKV = &c
		}
		out.Results = append(out.Results, o)
	}
	return out
}

func sameResult(a, b kv.TxnResult) bool { return reflect.DeepEqual(norm(a), norm(b)) }
