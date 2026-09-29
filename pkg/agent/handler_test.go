// Ligolo-ng
// Copyright (C) 2025 Nicolas Chatelain (nicocha30)

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package agent

import (
	"bytes"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nicocha30/ligolo-ng/pkg/protocol"
)

// fakeConn is a net.Conn backed by an in-memory request buffer. Close is
// recorded and also enforced: reads and writes after it fail with net.ErrClosed
// like a real conn, so a close that happens too early cannot go unnoticed.
type fakeConn struct {
	req    *bytes.Reader
	closes atomic.Int32

	mu   sync.Mutex
	resp bytes.Buffer
}

func newFakeConn(request []byte) *fakeConn {
	return &fakeConn{req: bytes.NewReader(request)}
}

func (c *fakeConn) isClosed() bool { return c.closes.Load() > 0 }

func (c *fakeConn) Read(p []byte) (int, error) {
	if c.isClosed() {
		return 0, net.ErrClosed
	}
	return c.req.Read(p)
}

func (c *fakeConn) Write(p []byte) (int, error) {
	if c.isClosed() {
		return 0, net.ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resp.Write(p)
}

func (c *fakeConn) Close() error {
	c.closes.Add(1)
	return nil
}

// response returns the bytes the handler wrote back to the proxy.
func (c *fakeConn) response() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.resp.Bytes()...)
}

func (c *fakeConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *fakeConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (c *fakeConn) SetDeadline(t time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(t time.Time) error { return nil }

var _ net.Conn = (*fakeConn)(nil)

// stubListener is a net.Listener that records Close, used to check that closing
// a listener by id actually closes the underlying listener.
type stubListener struct {
	closed atomic.Bool
}

func (s *stubListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (s *stubListener) Close() error              { s.closed.Store(true); return nil }
func (s *stubListener) Addr() net.Addr            { return &net.TCPAddr{} }

var _ net.Listener = (*stubListener)(nil)

func encodePacket(t *testing.T, payload interface{}) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := protocol.NewEncoder(&buf)
	if err := enc.Encode(payload); err != nil {
		t.Fatalf("encoding %T: %v", payload, err)
	}
	return buf.Bytes()
}

// decodeResponse decodes the single packet the handler wrote back. It fails the
// test if nothing was written, which is what distinguishes a handler that
// answered the request from one that merely closed the stream.
func decodeResponse(t *testing.T, raw []byte) interface{} {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("handler wrote no response to the proxy")
	}
	dec := protocol.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	return dec.Payload
}

// stateSizes reports the size of the two bookkeeping maps.
func stateSizes() (conns, listeners int) {
	stateMu.Lock()
	defer stateMu.Unlock()
	return len(listenerConntrack), len(listenerMap)
}

// TestHandleConnAnswersAndClosesOneShotRequests covers the stream leak. Every
// one-shot request must write its response and then close the stream exactly
// once: without the close the proxy's FIN leaves the stream half-closed and it
// is never reaped from the session, leaking a stream per request.
//
// Each case asserts the response as well as the close, so a handler that closes
// the stream without doing the work does not pass.
func TestHandleConnAnswersAndClosesOneShotRequests(t *testing.T) {
	tests := []struct {
		name    string
		request []byte
		check   func(t *testing.T, payload interface{})
	}{
		{
			// The path the leak was reported on: a scan produces a failed dial
			// per closed port, each one a one-shot request that must be reaped.
			name: "failed connect probe",
			request: encodePacket(t, protocol.ConnectRequestPacket{
				Net:       protocol.Networkv4,
				Transport: protocol.TransportTCP,
				Address:   "127.0.0.1",
				Port:      1,
			}),
			check: func(t *testing.T, payload interface{}) {
				resp, err := protocol.PayloadAs[protocol.ConnectResponsePacket](payload)
				if err != nil {
					t.Fatal(err)
				}
				if resp.Established {
					t.Fatal("dial to 127.0.0.1:1 reported Established; the probe was expected to fail")
				}
			},
		},
		{
			name:    "info request",
			request: encodePacket(t, protocol.InfoRequestPacket{}),
			check: func(t *testing.T, payload interface{}) {
				resp, err := protocol.PayloadAs[protocol.InfoReplyPacket](payload)
				if err != nil {
					t.Fatal(err)
				}
				if resp.SessionID != sessionID {
					t.Fatalf("SessionID = %q, want %q", resp.SessionID, sessionID)
				}
				if resp.Name == "" {
					t.Fatal("InfoReplyPacket.Name is empty")
				}
			},
		},
		{
			name:    "listener close for unknown id",
			request: encodePacket(t, protocol.ListenerCloseRequestPacket{ListenerID: -999999}),
			check: func(t *testing.T, payload interface{}) {
				resp, err := protocol.PayloadAs[protocol.ListenerCloseResponsePacket](payload)
				if err != nil {
					t.Fatal(err)
				}
				if !resp.Err {
					t.Fatal("closing an unknown listener id reported success")
				}
			},
		},
		{
			// Drives getConn at its real call site.
			name:    "sock request for unknown id",
			request: encodePacket(t, protocol.ListenerSockRequestPacket{SockID: -999999}),
			check: func(t *testing.T, payload interface{}) {
				resp, err := protocol.PayloadAs[protocol.ListenerSockResponsePacket](payload)
				if err != nil {
					t.Fatal(err)
				}
				if !resp.Err {
					t.Fatal("unknown SockID reported success")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := newFakeConn(tc.request)
			HandleConn(conn)

			if got := conn.closes.Load(); got != 1 {
				t.Fatalf("stream closed %d times, want exactly 1 (0 leaks it, >1 is a double close)", got)
			}
			tc.check(t, decodeResponse(t, conn.response()))
		})
	}
}

// TestHandleConnClosesStreamWithoutUsableRequest covers the two paths that do no
// work at all: undecodable input and a stream that carries nothing. Both are
// still streams the agent has to reap.
func TestHandleConnClosesStreamWithoutUsableRequest(t *testing.T) {
	tests := []struct {
		name    string
		request []byte
	}{
		{"decode error", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{"empty stream", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := newFakeConn(tc.request)
			HandleConn(conn)
			if got := conn.closes.Load(); got != 1 {
				t.Fatalf("stream closed %d times, want exactly 1", got)
			}
		})
	}
}

// TestHandleConnListenerCloseRemovesAndClosesListener checks the other half of
// the bookkeeping fix: takeListener must remove the entry (or listenerMap grows
// without bound) and the listener itself must actually be closed.
func TestHandleConnListenerCloseRemovesAndClosesListener(t *testing.T) {
	_, baseListeners := stateSizes()

	lis := &stubListener{}
	id := registerListener(lis)

	if _, got := stateSizes(); got != baseListeners+1 {
		t.Fatalf("listenerMap size %d after registerListener, want %d", got, baseListeners+1)
	}

	conn := newFakeConn(encodePacket(t, protocol.ListenerCloseRequestPacket{ListenerID: id}))
	HandleConn(conn)

	resp, err := protocol.PayloadAs[protocol.ListenerCloseResponsePacket](decodeResponse(t, conn.response()))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Err {
		t.Fatalf("closing a registered listener failed: %s", resp.ErrString)
	}
	if !lis.closed.Load() {
		t.Fatal("listener was removed from the map but never closed")
	}
	if _, got := stateSizes(); got != baseListeners {
		t.Fatalf("listenerMap size %d after close, want %d; the entry leaked", got, baseListeners)
	}
}

// TestHandleConnDoesNotCloseHandedOffStream is the inverse of the leak tests.
// The UDP reverse-listener branch passes conn to a background relay goroutine
// that owns and closes it, so HandleConn must NOT close it on return. An
// unconditional defer conn.Close() would satisfy every other test here while
// killing UDP reverse listeners outright, so this is the case that pins the
// handoff guard down.
func TestHandleConnDoesNotCloseHandedOffStream(t *testing.T) {
	agentSide, proxySide := net.Pipe()
	defer proxySide.Close()

	returned := make(chan struct{})
	go func() {
		HandleConn(agentSide)
		close(returned)
	}()

	proxyEnc := protocol.NewEncoder(proxySide)
	if err := proxyEnc.Encode(protocol.ListenerRequestPacket{
		Network: "udp",
		Address: "127.0.0.1:0",
	}); err != nil {
		t.Fatalf("sending listener request: %v", err)
	}

	dec := protocol.NewDecoder(proxySide)
	if err := dec.Decode(); err != nil {
		t.Fatalf("reading listener response: %v", err)
	}
	resp, err := protocol.PayloadAs[protocol.ListenerResponsePacket](dec.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Err {
		t.Fatalf("agent refused the UDP listener: %s", resp.ErrString)
	}

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("HandleConn did not return after handing the stream off")
	}

	stateMu.Lock()
	entry, ok := listenerMap[resp.ListenerID]
	stateMu.Unlock()
	if !ok {
		t.Fatalf("listener %d was not registered", resp.ListenerID)
	}
	udpConn, ok := entry.(*net.UDPConn)
	if !ok {
		t.Fatalf("listener %d is %T, want *net.UDPConn", resp.ListenerID, entry)
	}
	defer func() {
		if lis, ok := takeListener(resp.ListenerID); ok {
			if c, ok := lis.(*net.UDPConn); ok {
				c.Close()
			}
		}
	}()

	// HandleConn has returned. If it closed the stream anyway, the relay
	// goroutine can no longer reach the proxy and this datagram is lost.
	client, err := net.DialUDP("udp", nil, udpConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dialling the agent's UDP listener: %v", err)
	}
	defer client.Close()

	probe := []byte("ligolo-handoff-probe")
	if _, err := client.Write(probe); err != nil {
		t.Fatalf("sending probe datagram: %v", err)
	}

	if err := proxySide.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(probe))
	if _, err := io.ReadFull(proxySide, got); err != nil {
		t.Fatalf("relay could not forward to the tunnel after HandleConn returned, so the handoff guard is broken: %v", err)
	}
	if !bytes.Equal(got, probe) {
		t.Fatalf("tunnel carried %q, want %q", got, probe)
	}
}

// TestListenerStateCrossGoroutineAccess mirrors how the bookkeeping is really
// used: registerConn runs in a listener's accept loop while getConn and
// deleteConn run in separate HandleConn goroutines. Unguarded, this is a fatal
// "concurrent map writes"; it also fails under -race.
func TestListenerStateCrossGoroutineAccess(t *testing.T) {
	const workers = 16
	const perWorker = 100

	ids := make(chan int32, workers*perWorker)

	var producers sync.WaitGroup
	for i := 0; i < workers; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for j := 0; j < perWorker; j++ {
				ids <- registerConn(newFakeConn(nil))
			}
		}()
	}
	go func() {
		producers.Wait()
		close(ids)
	}()

	var missing atomic.Int32
	var consumers sync.WaitGroup
	for i := 0; i < workers; i++ {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for id := range ids {
				if _, ok := getConn(id); !ok {
					missing.Add(1)
					continue
				}
				deleteConn(id)
			}
		}()
	}
	consumers.Wait()

	if got := missing.Load(); got != 0 {
		t.Fatalf("%d connections were invisible to the consumer goroutine after registerConn returned", got)
	}
}

// TestHandleConnConcurrentWithRegistration drives HandleConn concurrently with
// registerConn so that any bookkeeping touched directly inside the handler,
// rather than through the guarded helpers, is reported by -race.
func TestHandleConnConcurrentWithRegistration(t *testing.T) {
	const workers = 16
	const perWorker = 50

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				id := registerConn(newFakeConn(nil))
				deleteConn(id)
			}
		}()
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				// Negative ids are never handed out, so getConn always misses
				// and the handler returns after writing its error response.
				sockID := int32(-1 - (worker*perWorker + j))
				HandleConn(newFakeConn(encodePacket(t, protocol.ListenerSockRequestPacket{SockID: sockID})))
			}
		}(i)
	}
	wg.Wait()
}

// TestBookkeepingReturnsToBaseline guards against the leak the mutex fix is
// wrapped around: entries must be removed, not just made unreachable. A no-op
// deleteConn or a takeListener that never deletes grows these maps forever.
func TestBookkeepingReturnsToBaseline(t *testing.T) {
	baseConns, baseListeners := stateSizes()

	const n = 50
	connIDs := make([]int32, 0, n)
	listenerIDs := make([]int32, 0, n)
	for i := 0; i < n; i++ {
		connIDs = append(connIDs, registerConn(newFakeConn(nil)))
		listenerIDs = append(listenerIDs, registerListener(&stubListener{}))
	}

	if conns, listeners := stateSizes(); conns != baseConns+n || listeners != baseListeners+n {
		t.Fatalf("after registering %d of each: conns %d (want %d), listeners %d (want %d)",
			n, conns, baseConns+n, listeners, baseListeners+n)
	}

	for _, id := range connIDs {
		deleteConn(id)
	}
	for _, id := range listenerIDs {
		if _, ok := takeListener(id); !ok {
			t.Fatalf("takeListener(%d) reported the listener missing", id)
		}
	}

	conns, listeners := stateSizes()
	if conns != baseConns {
		t.Errorf("listenerConntrack leaked: %d entries, want %d", conns, baseConns)
	}
	if listeners != baseListeners {
		t.Errorf("listenerMap leaked: %d entries, want %d", listeners, baseListeners)
	}
}
