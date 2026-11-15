package sim

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"time"
)

// Handler receives a delivered message. It runs on the simulation goroutine,
// inside a scheduled event.
type Handler func(from string, payload []byte)

// LinkConfig describes conditions on one directed edge (from -> to). The
// reverse direction has its own, independently configurable LinkConfig.
type LinkConfig struct {
	MinDelay      time.Duration
	MaxDelay      time.Duration
	LossRate      float64 // probability in [0, 1] that a send is dropped
	DuplicateRate float64 // probability in [0, 1] that a send is delivered twice
}

// DefaultLinkConfig is a lossless link with a fixed 1ms delay.
var DefaultLinkConfig = LinkConfig{MinDelay: time.Millisecond, MaxDelay: time.Millisecond}

// Validate reports whether the config is usable.
func (c LinkConfig) Validate() error {
	if c.MinDelay < 0 || c.MaxDelay < c.MinDelay {
		return fmt.Errorf("sim: require 0 <= MinDelay <= MaxDelay, got %v..%v", c.MinDelay, c.MaxDelay)
	}
	if c.LossRate < 0 || c.LossRate > 1 {
		return fmt.Errorf("sim: LossRate must be in [0, 1], got %v", c.LossRate)
	}
	if c.DuplicateRate < 0 || c.DuplicateRate > 1 {
		return fmt.Errorf("sim: DuplicateRate must be in [0, 1], got %v", c.DuplicateRate)
	}
	return nil
}

type edge struct{ from, to string }

// Network is the fault-injection surface of the simulator.
//
//   - Partitions are sets of blocked directed edges, not "two groups", because
//     asymmetric partitions (A reaches B, B cannot reach A) are a distinct,
//     real failure mode that a symmetric model would hide.
//   - Reordering is not a separate knob: it falls out of variable per-link
//     delay. Nothing here preserves send order; any ordering guarantee has to
//     be rebuilt above this layer.
//   - Loss and duplication are independent per link.
//   - Crash is a network-delivery fact: a down node neither sends nor receives.
//
// Everything random comes from one seeded RNG, so a run is reproducible from
// one seed. Network is not safe for concurrent use; see the package doc.
type Network struct {
	sched       *Scheduler
	rng         *rand.Rand
	handlers    map[string]Handler
	defaultLink LinkConfig
	links       map[edge]LinkConfig
	blocked     map[edge]struct{}
	down        map[string]struct{}
}

// NewNetwork returns a network driven by sched. All randomness derives from
// seed.
func NewNetwork(sched *Scheduler, seed uint64) *Network {
	return &Network{
		sched:       sched,
		rng:         rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15)),
		handlers:    make(map[string]Handler),
		defaultLink: DefaultLinkConfig,
		links:       make(map[edge]LinkConfig),
		blocked:     make(map[edge]struct{}),
		down:        make(map[string]struct{}),
	}
}

// RegisterNode attaches a handler for node id. Nodes start up.
func (n *Network) RegisterNode(id string, h Handler) error {
	if _, ok := n.handlers[id]; ok {
		return fmt.Errorf("sim: node %q already registered", id)
	}
	n.handlers[id] = h
	return nil
}

func (n *Network) checkNode(id string) error {
	if _, ok := n.handlers[id]; !ok {
		return fmt.Errorf("sim: unknown node %q", id)
	}
	return nil
}

// -- node lifecycle ---------------------------------------------------------
//
// Two timing rules, chosen to match real socket behavior:
//   - Send checks the sender's liveness immediately: a dead process cannot
//     originate a send.
//   - Delivery checks the receiver's liveness at delivery time, not send time.
//     A message in flight when the receiver crashes is lost; a message sent
//     while the receiver was down but delivered after it restarted arrives.

// IsUp reports whether node id's process is up.
func (n *Network) IsUp(id string) bool {
	_, down := n.down[id]
	return !down
}

// CrashNow marks the node down immediately.
func (n *Network) CrashNow(id string) error {
	if err := n.checkNode(id); err != nil {
		return err
	}
	n.down[id] = struct{}{}
	return nil
}

// RestartNow brings the node back up. onRestart, if non-nil, runs first while
// the node is still marked down, so recovery logic (e.g. WAL replay) completes
// before any message can reach the node.
func (n *Network) RestartNow(id string, onRestart func()) error {
	if err := n.checkNode(id); err != nil {
		return err
	}
	if onRestart != nil {
		onRestart()
	}
	delete(n.down, id)
	return nil
}

// CrashAt schedules a crash at virtual time at.
func (n *Network) CrashAt(at time.Duration, id string) (*Handle, error) {
	if err := n.checkNode(id); err != nil {
		return nil, err
	}
	return n.sched.ScheduleAt(at, func() { _ = n.CrashNow(id) }), nil
}

// RestartAt schedules a restart at virtual time at; see RestartNow.
func (n *Network) RestartAt(at time.Duration, id string, onRestart func()) (*Handle, error) {
	if err := n.checkNode(id); err != nil {
		return nil, err
	}
	return n.sched.ScheduleAt(at, func() { _ = n.RestartNow(id, onRestart) }), nil
}

// -- link configuration ----------------------------------------------------

// SetLink configures the directed edge from -> to.
func (n *Network) SetLink(from, to string, cfg LinkConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	n.links[edge{from, to}] = cfg
	return nil
}

// SetDefaultLink configures every edge without a per-link override.
func (n *Network) SetDefaultLink(cfg LinkConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	n.defaultLink = cfg
	return nil
}

func (n *Network) linkFor(from, to string) LinkConfig {
	if cfg, ok := n.links[edge{from, to}]; ok {
		return cfg
	}
	return n.defaultLink
}

// -- partitions -------------------------------------------------------------

// Block blocks the directed edge from -> to. Call it in both directions for a
// symmetric cut.
func (n *Network) Block(from, to string) { n.blocked[edge{from, to}] = struct{}{} }

// Unblock reverses Block.
func (n *Network) Unblock(from, to string) { delete(n.blocked, edge{from, to}) }

// IsBlocked reports whether the directed edge from -> to is blocked.
func (n *Network) IsBlocked(from, to string) bool {
	_, ok := n.blocked[edge{from, to}]
	return ok
}

// Partition blocks every edge between the two groups, in both directions.
func (n *Network) Partition(a, b []string) {
	for _, x := range a {
		for _, y := range b {
			n.Block(x, y)
			n.Block(y, x)
		}
	}
}

// Heal reverses Partition.
func (n *Network) Heal(a, b []string) {
	for _, x := range a {
		for _, y := range b {
			n.Unblock(x, y)
			n.Unblock(y, x)
		}
	}
}

// -- sending ----------------------------------------------------------------

// Send queues payload for delivery from -> to under the link's conditions.
// Drops (crashed sender, partition, loss) are silent and return nil, as on a
// real network. An error means the call itself was wrong (unknown node).
//
// The payload is copied once per delivery, so the caller may reuse its buffer
// and a duplicated message's two deliveries cannot alias each other. A real
// network gives the same guarantee.
func (n *Network) Send(from, to string, payload []byte) error {
	if err := n.checkNode(from); err != nil {
		return fmt.Errorf("sender: %w", err)
	}
	if err := n.checkNode(to); err != nil {
		return fmt.Errorf("destination: %w", err)
	}
	if !n.IsUp(from) || n.IsBlocked(from, to) {
		return nil
	}

	link := n.linkFor(from, to)

	// RNG draws are unconditional (e.g. loss is drawn even when LossRate is 0),
	// so the number of draws per Send depends only on whether the message was
	// duplicated, not on the configured values.
	if n.rng.Float64() < link.LossRate {
		return nil
	}
	n.scheduleDelivery(from, to, payload, link)
	if n.rng.Float64() < link.DuplicateRate {
		n.scheduleDelivery(from, to, payload, link)
	}
	return nil
}

func (n *Network) scheduleDelivery(from, to string, payload []byte, link LinkConfig) {
	span := int64(link.MaxDelay - link.MinDelay)
	delay := link.MinDelay + time.Duration(n.rng.Int64N(span+1))
	msg := bytes.Clone(payload)
	n.sched.ScheduleAfter(delay, func() { n.deliver(from, to, msg) })
}

func (n *Network) deliver(from, to string, payload []byte) {
	if !n.IsUp(to) {
		return // receiver crashed after the message was sent
	}
	n.handlers[to](from, payload)
}
