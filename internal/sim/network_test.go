package sim_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/souviks22/continuum-go/internal/sim"
)

type delivery struct {
	at      time.Duration
	payload string
}

func makeNet(t *testing.T, seed uint64) (*sim.Scheduler, *sim.Network) {
	t.Helper()
	s := sim.NewScheduler()
	return s, sim.NewNetwork(s, seed)
}

func noop(string, []byte) {}

func register(t *testing.T, n *sim.Network, id string, h sim.Handler) {
	t.Helper()
	mustNoErr(t, n.RegisterNode(id, h))
}

func drain(t *testing.T, s *sim.Scheduler) {
	t.Helper()
	_, err := s.RunUntilIdle(sim.Unbounded)
	mustNoErr(t, err)
}

func TestMessageDeliveredWithConfiguredDelay(t *testing.T) {
	s, n := makeNet(t, 0)
	var got []delivery
	register(t, n, "a", noop)
	register(t, n, "b", func(_ string, p []byte) { got = append(got, delivery{s.Now(), string(p)}) })
	mustNoErr(t, n.SetDefaultLink(sim.LinkConfig{MinDelay: 7 * ms, MaxDelay: 7 * ms}))

	mustNoErr(t, n.Send("a", "b", []byte("hello")))
	drain(t, s)

	if want := []delivery{{7 * ms, "hello"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestHandlerReceivesSender(t *testing.T) {
	s, n := makeNet(t, 0)
	var from string
	register(t, n, "a", noop)
	register(t, n, "b", func(f string, _ []byte) { from = f })
	mustNoErr(t, n.Send("a", "b", []byte("x")))
	drain(t, s)
	if from != "a" {
		t.Fatalf("from=%q", from)
	}
}

func TestSendToUnknownNodeErrors(t *testing.T) {
	_, n := makeNet(t, 0)
	register(t, n, "a", noop)
	if err := n.Send("a", "ghost", []byte("x")); err == nil {
		t.Fatal("expected error for unknown destination")
	}
	if err := n.Send("ghost", "a", []byte("x")); err == nil {
		t.Fatal("expected error for unknown sender")
	}
}

func TestDuplicateRegistrationErrors(t *testing.T) {
	_, n := makeNet(t, 0)
	register(t, n, "a", noop)
	if err := n.RegisterNode("a", noop); err == nil {
		t.Fatal("expected error on duplicate registration")
	}
}

func TestLinkConfigValidation(t *testing.T) {
	_, n := makeNet(t, 0)
	bad := []sim.LinkConfig{
		{MinDelay: -1},
		{MinDelay: 5, MaxDelay: 1},
		{LossRate: -0.1},
		{LossRate: 1.1},
		{DuplicateRate: 2},
	}
	for _, cfg := range bad {
		if n.SetDefaultLink(cfg) == nil || n.SetLink("a", "b", cfg) == nil {
			t.Fatalf("config %+v should be rejected", cfg)
		}
	}
}

func TestFullPartitionBlocksBothDirections(t *testing.T) {
	s, n := makeNet(t, 0)
	var got []string
	h := func(_ string, p []byte) { got = append(got, string(p)) }
	register(t, n, "a", h)
	register(t, n, "b", h)
	n.Partition([]string{"a"}, []string{"b"})

	mustNoErr(t, n.Send("a", "b", []byte("x")))
	mustNoErr(t, n.Send("b", "a", []byte("y")))
	drain(t, s)

	if len(got) != 0 {
		t.Fatalf("delivered across partition: %v", got)
	}
}

func TestHealedPartitionAllowsDeliveryAgain(t *testing.T) {
	s, n := makeNet(t, 0)
	var got []string
	register(t, n, "a", noop)
	register(t, n, "b", func(_ string, p []byte) { got = append(got, string(p)) })
	n.Partition([]string{"a"}, []string{"b"})
	n.Heal([]string{"a"}, []string{"b"})

	mustNoErr(t, n.Send("a", "b", []byte("x")))
	drain(t, s)

	if !reflect.DeepEqual(got, []string{"x"}) {
		t.Fatalf("got %v", got)
	}
}

func TestAsymmetricPartitionBlocksOnlyOneDirection(t *testing.T) {
	s, n := makeNet(t, 0)
	var byA, byB []string
	register(t, n, "a", func(_ string, p []byte) { byA = append(byA, string(p)) })
	register(t, n, "b", func(_ string, p []byte) { byB = append(byB, string(p)) })
	n.Block("b", "a") // a can reach b, b cannot reach a

	mustNoErr(t, n.Send("a", "b", []byte("from_a")))
	mustNoErr(t, n.Send("b", "a", []byte("from_b")))
	drain(t, s)

	if !reflect.DeepEqual(byB, []string{"from_a"}) || len(byA) != 0 {
		t.Fatalf("byA=%v byB=%v", byA, byB)
	}
}

func TestLossRateOneDropsEveryMessage(t *testing.T) {
	s, n := makeNet(t, 0)
	count := 0
	register(t, n, "a", noop)
	register(t, n, "b", func(string, []byte) { count++ })
	mustNoErr(t, n.SetDefaultLink(sim.LinkConfig{LossRate: 1.0}))
	for i := 0; i < 20; i++ {
		mustNoErr(t, n.Send("a", "b", []byte{byte(i)}))
	}
	drain(t, s)
	if count != 0 {
		t.Fatalf("delivered %d", count)
	}
}

func TestLossRateZeroDropsNothing(t *testing.T) {
	s, n := makeNet(t, 3)
	count := 0
	register(t, n, "a", noop)
	register(t, n, "b", func(string, []byte) { count++ })
	for i := 0; i < 500; i++ {
		mustNoErr(t, n.Send("a", "b", []byte{1}))
	}
	drain(t, s)
	if count != 500 {
		t.Fatalf("delivered %d of 500", count)
	}
}

func TestDuplicateRateOneDeliversEveryMessageTwice(t *testing.T) {
	s, n := makeNet(t, 1)
	var got []string
	register(t, n, "a", noop)
	register(t, n, "b", func(_ string, p []byte) { got = append(got, string(p)) })
	mustNoErr(t, n.SetDefaultLink(sim.LinkConfig{MinDelay: 1 * ms, MaxDelay: 1 * ms, DuplicateRate: 1.0}))

	mustNoErr(t, n.Send("a", "b", []byte("x")))
	drain(t, s)

	if !reflect.DeepEqual(got, []string{"x", "x"}) {
		t.Fatalf("got %v", got)
	}
}

func TestVariableDelayCanReorderMessages(t *testing.T) {
	// Reordering is not a knob; it emerges from variable delay. Search seeds
	// for one where the second message overtakes the first, to demonstrate the
	// mechanism rather than assert on probabilistic behavior for any seed.
	for seed := uint64(0); seed < 50; seed++ {
		s, n := makeNet(t, seed)
		var got []string
		register(t, n, "a", noop)
		register(t, n, "b", func(_ string, p []byte) { got = append(got, string(p)) })
		mustNoErr(t, n.SetDefaultLink(sim.LinkConfig{MinDelay: 1 * ms, MaxDelay: 20 * ms}))

		mustNoErr(t, n.Send("a", "b", []byte("first")))
		mustNoErr(t, n.Send("a", "b", []byte("second")))
		drain(t, s)

		if reflect.DeepEqual(got, []string{"second", "first"}) {
			return
		}
	}
	t.Fatal("no seed in range produced reordering; delay-based reordering may be broken")
}

func TestSameSeedIsFullyDeterministic(t *testing.T) {
	run := func(seed uint64) []delivery {
		s, n := makeNet(t, seed)
		var got []delivery
		register(t, n, "a", noop)
		register(t, n, "b", func(_ string, p []byte) { got = append(got, delivery{s.Now(), string(p)}) })
		mustNoErr(t, n.SetDefaultLink(sim.LinkConfig{MinDelay: 1 * ms, MaxDelay: 10 * ms, LossRate: 0.3, DuplicateRate: 0.2}))
		for i := 0; i < 100; i++ {
			mustNoErr(t, n.Send("a", "b", []byte{byte(i)}))
		}
		drain(t, s)
		return got
	}
	a, b := run(42), run(42)
	if len(a) == 0 || !reflect.DeepEqual(a, b) {
		t.Fatalf("runs diverged or empty: %d vs %d deliveries", len(a), len(b))
	}
	if reflect.DeepEqual(a, run(43)) {
		t.Fatal("different seeds produced identical runs; seed is being ignored")
	}
}

func TestPerLinkOverrideTakesPrecedenceOverDefault(t *testing.T) {
	s, n := makeNet(t, 0)
	var at []time.Duration
	register(t, n, "a", noop)
	register(t, n, "b", func(string, []byte) { at = append(at, s.Now()) })
	mustNoErr(t, n.SetDefaultLink(sim.LinkConfig{MinDelay: 100 * ms, MaxDelay: 100 * ms}))
	mustNoErr(t, n.SetLink("a", "b", sim.LinkConfig{MinDelay: 3 * ms, MaxDelay: 3 * ms}))

	mustNoErr(t, n.Send("a", "b", []byte("x")))
	drain(t, s)

	if !reflect.DeepEqual(at, []time.Duration{3 * ms}) {
		t.Fatalf("got %v", at)
	}
}

func TestPayloadIsCopiedPerDelivery(t *testing.T) {
	s, n := makeNet(t, 0)
	var got []string
	register(t, n, "a", noop)
	register(t, n, "b", func(_ string, p []byte) {
		got = append(got, string(p)) // snapshot before mutating
		p[0] = 'Z'                   // a receiver scribbling on its copy must not leak into the duplicate
	})
	mustNoErr(t, n.SetDefaultLink(sim.LinkConfig{MinDelay: 1 * ms, MaxDelay: 1 * ms, DuplicateRate: 1.0}))

	buf := []byte("abc")
	mustNoErr(t, n.Send("a", "b", buf))
	buf[0] = 'X' // sender reusing its buffer after Send must not leak into delivery
	drain(t, s)

	if !reflect.DeepEqual(got, []string{"abc", "abc"}) {
		t.Fatalf("got %q, want both deliveries to be the original \"abc\"", got)
	}
}
