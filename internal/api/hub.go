package api

import (
	"bytes"
	"encoding/json"
	"sync"
)

// The broadcast hub.
//
// One producer (the 1 Hz stats ticker, plus the run goroutine) fans out to N
// browsers. The single rule that shapes everything here: a slow reader must
// never be able to stall the ticker. Every subscriber gets a small buffered
// channel and a drop-the-oldest policy — a dashboard that fell behind sees a
// gap in its trace, which is honest, rather than holding up every other
// dashboard, which is not.

// subBuffer is how many frames a subscriber may fall behind before the oldest
// is dropped. Four seconds of stats is enough slack for a GC pause or a
// momentarily wedged TCP window, and small enough that a genuinely dead client
// is not carrying a minute of stale frames.
const subBuffer = 4

// hub fans SSE frames out to every connected subscriber.
type hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}

	// dropped counts frames discarded by the slow-reader policy. It exists so
	// the behaviour is observable in tests and logs rather than silent.
	dropped int64
}

func newHub() *hub {
	return &hub{subs: make(map[chan []byte]struct{})}
}

// maxSubscribers caps concurrent SSE clients. Each one holds a goroutine and a
// file descriptor for as long as it stays connected, and /api/stream is not
// rate-limited (a dashboard legitimately opens one on load), so without a
// ceiling a trivial script could exhaust descriptors on a public endpoint.
const maxSubscribers = 256

// subscribe registers a new client and returns its frame channel. It returns
// nil when the hub is at capacity; the caller must answer 503.
func (h *hub) subscribe() chan []byte {
	ch := make(chan []byte, subBuffer)
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.subs) >= maxSubscribers {
		return nil
	}
	h.subs[ch] = struct{}{}
	return ch
}

// unsubscribe removes a client. Safe to call twice.
func (h *hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

// broadcast delivers one pre-encoded SSE frame to everyone. It never blocks:
// if a subscriber's buffer is full its oldest frame is discarded to make room.
func (h *hub) broadcast(frame []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- frame:
		default:
			// Drop the oldest, then retry once. The second default can only be
			// reached if another goroutine refilled the slot in between, which
			// cannot happen (broadcast is the only writer and holds the lock),
			// but the belt-and-braces default is what guarantees "never block".
			select {
			case <-ch:
				h.dropped++
			default:
			}
			select {
			case ch <- frame:
			default:
				h.dropped++
			}
		}
	}
}

func (h *hub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

func (h *hub) droppedFrames() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dropped
}

// sseFrame encodes one named event into the wire format. Encoding happens once
// per frame, not once per subscriber, which is the whole reason the hub carries
// bytes rather than values.
func sseFrame(event string, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(len(body) + len(event) + 16)
	buf.WriteString("event: ")
	buf.WriteString(event)
	buf.WriteString("\ndata: ")
	// A newline inside data would split the frame. Our payloads are compact
	// JSON, which never contains a raw newline, but the guard costs nothing.
	buf.Write(bytes.ReplaceAll(body, []byte("\n"), []byte(" ")))
	buf.WriteString("\n\n")
	return buf.Bytes(), nil
}
