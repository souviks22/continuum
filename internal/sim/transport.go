package sim

import (
	"fmt"

	"github.com/souviks22/continuum-go/internal/transport"
)

var _ transport.Transport = (*Transport)(nil)

// Transport is a transport.Transport backed by the simulated Network. Each node
// gets its own instance, mirroring how a real transport is bound to one local
// identity rather than shared across nodes.
type Transport struct {
	id      string
	net     *Network
	handler transport.ReceiveHandler
}

// NewTransport registers node id on net and returns its transport.
func NewTransport(id string, net *Network) (*Transport, error) {
	t := &Transport{id: id, net: net}
	if err := net.RegisterNode(id, t.onReceive); err != nil {
		return nil, err
	}
	return t, nil
}

// Send implements transport.Transport. The network copies the payload per
// delivery, which is stricter than the ownership rule requires.
func (t *Transport) Send(to string, payload []byte) error {
	return t.net.Send(t.id, to, payload)
}

// SetReceiveHandler implements transport.Transport.
func (t *Transport) SetReceiveHandler(h transport.ReceiveHandler) { t.handler = h }

func (t *Transport) onReceive(from string, payload []byte) {
	if t.handler == nil {
		// A message with nowhere to go is a wiring bug, not a network fault.
		panic(fmt.Sprintf("sim: node %q received a message before a receive handler was set", t.id))
	}
	t.handler(from, payload)
}
