package feed

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"example.com/digital-asset-simulator/internal/engine"
	"github.com/gorilla/websocket"
)

// Demo uses deterministic synthetic prices; no connection to real trading venues.
func Demo(ctx context.Context, e *engine.Engine) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	tick := 0
	for {
		mid := int64(6000000 + math.Round(18000*math.Sin(float64(tick)/12)+3000*math.Sin(float64(tick)/3)))
		bids, asks := []engine.InputLevel{}, []engine.InputLevel{}
		for i := int64(0); i < 15; i++ {
			q := int64(3000000) + ((int64(tick)+i)%7)*500000
			bids = append(bids, engine.InputLevel{Price: mid - 100 - i*100, Quantity: q})
			asks = append(asks, engine.InputLevel{Price: mid + 100 + i*100, Quantity: q})
		}
		_ = e.Snapshot(bids, asks)
		tick++
		select {
		case <-ctx.Done():
			e.SetConnection(false, "demo stopped")
			return
		case <-ticker.C:
		}
	}
}

type Message struct {
	Type    string     `json:"type"`
	Product string     `json:"product_id"`
	Bids    [][]string `json:"bids"`
	Asks    [][]string `json:"asks"`
	Changes [][]string `json:"changes"`
	Message string     `json:"message"`
}

func parseLevels(rows [][]string) ([]engine.InputLevel, error) {
	result := make([]engine.InputLevel, 0, len(rows))
	for _, row := range rows {
		if len(row) != 2 {
			return nil, errors.New("invalid level tuple")
		}
		p, err := engine.ParseDecimal(row[0], 2)
		if err != nil {
			return nil, err
		}
		q, err := engine.ParseDecimal(row[1], 8)
		if err != nil {
			return nil, err
		}
		result = append(result, engine.InputLevel{Price: p, Quantity: q})
	}
	return result, nil
}

// Apply rejects malformed batches before applying any part of them.
func Apply(e *engine.Engine, m Message) error {
	if m.Type == "error" {
		return fmt.Errorf("exchange: %s", m.Message)
	}
	if m.Type != "snapshot" && m.Type != "l2update" {
		return nil
	}
	if m.Product != engine.Product {
		return nil
	}
	if m.Type == "snapshot" {
		b, err := parseLevels(m.Bids)
		if err != nil {
			return err
		}
		a, err := parseLevels(m.Asks)
		if err != nil {
			return err
		}
		return e.Snapshot(b, a)
	}
	changes := []engine.Change{}
	for _, row := range m.Changes {
		if len(row) != 3 {
			return errors.New("invalid change tuple")
		}
		levels, err := parseLevels([][]string{{row[1], row[2]}})
		if err != nil {
			return err
		}
		changes = append(changes, engine.Change{Side: row[0], Price: levels[0].Price, Quantity: levels[0].Quantity})
	}
	if len(changes) == 0 {
		return nil
	}
	return e.Update(changes)
}
func session(ctx context.Context, e *engine.Engine, url string) error {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	c, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	c.SetReadLimit(8 * 1024 * 1024)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = c.Close()
		case <-done:
		}
	}()
	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err = c.WriteJSON(map[string]any{"type": "subscribe", "product_ids": []string{engine.Product}, "channels": []string{"level2_batch", "heartbeat"}}); err != nil {
		return err
	}
	e.SetConnection(true, "connected; awaiting snapshot")
	for {
		_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, raw, err := c.ReadMessage()
		if err != nil {
			return err
		}
		var m Message
		if err = json.Unmarshal(raw, &m); err != nil {
			return err
		}
		if err = Apply(e, m); err != nil {
			return err
		}
	}
}
func Coinbase(ctx context.Context, e *engine.Engine) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		err := session(ctx, e, "wss://ws-feed.exchange.coinbase.com")
		e.SetConnection(false, fmt.Sprintf("disconnected; resynchronizing: %v", err))
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

// Replay CSV columns: timestamp,bid,ask,bid_size,ask_size. One snapshot per row.
func Replay(ctx context.Context, e *engine.Engine, path string, interval time.Duration) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return err
	}
	expected := []string{"timestamp", "bid", "ask", "bid_size", "ask_size"}
	if len(header) != len(expected) {
		return errors.New("invalid replay header")
	}
	for i := range expected {
		if header[i] != expected[i] {
			return errors.New("invalid replay header")
		}
	}
	for {
		row, err := r.Read()
		if err == io.EOF {
			e.SetConnection(false, "replay complete")
			return nil
		}
		if err != nil {
			return err
		}
		if len(row) != 5 {
			return errors.New("invalid replay row")
		}
		if _, err = time.Parse(time.RFC3339Nano, row[0]); err != nil {
			return err
		}
		b, err := parseLevels([][]string{{row[1], row[3]}})
		if err != nil {
			return err
		}
		a, err := parseLevels([][]string{{row[2], row[4]}})
		if err != nil {
			return err
		}
		if err = e.Snapshot(b, a); err != nil {
			return err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
