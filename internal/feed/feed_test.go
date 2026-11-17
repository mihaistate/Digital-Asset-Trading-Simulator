package feed

import (
	"context"
	"example.com/digital-asset-simulator/internal/engine"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSnapshotAndAbsoluteUpdate(t *testing.T) {
	e := engine.New(engine.DefaultConfig(), "test")
	if err := Apply(e, Message{Type: "snapshot", Product: engine.Product, Bids: [][]string{{"100.00", "1"}}, Asks: [][]string{{"101.00", "2"}, {"102.00", "3"}}}); err != nil {
		t.Fatal(err)
	}
	if err := Apply(e, Message{Type: "l2update", Product: engine.Product, Changes: [][]string{{"sell", "101", "0"}, {"sell", "102", "0.5"}}}); err != nil {
		t.Fatal(err)
	}
	s := e.State()
	if len(s.Asks) != 1 || s.Asks[0].Quantity != 50000000 {
		t.Fatal(s.Asks)
	}
	if err := Apply(e, Message{Type: "l2update", Product: engine.Product, Changes: [][]string{{"sell", "103", "1"}, {"bad"}}}); err == nil {
		t.Fatal("bad tuple")
	}
	if len(e.State().Asks) != 1 {
		t.Fatal("partial update")
	}
}
func TestWebsocketSession(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		var subscription map[string]any
		if err = c.ReadJSON(&subscription); err != nil {
			return
		}
		if subscription["type"] != "subscribe" {
			t.Error(subscription)
		}
		_ = c.WriteJSON(Message{Type: "snapshot", Product: engine.Product, Bids: [][]string{{"100", "1"}}, Asks: [][]string{{"101", "1"}}})
		_ = c.WriteJSON(Message{Type: "l2update", Product: engine.Product, Changes: [][]string{{"sell", "101", "0.5"}}})
	}))
	defer server.Close()
	e := engine.New(engine.DefaultConfig(), "test")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = session(ctx, e, "ws"+strings.TrimPrefix(server.URL, "http"))
	if len(e.State().Asks) != 1 || e.State().Asks[0].Quantity != 50000000 {
		t.Fatal(e.State())
	}
}
