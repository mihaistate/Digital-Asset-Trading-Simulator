package engine

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func setup(t *testing.T) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.MaxPosition = 2 * Scale
	e := New(cfg, "test")
	if err := e.Snapshot([]InputLevel{{9900, Scale}, {9800, Scale}}, []InputLevel{{10000, Scale}, {10100, Scale}}); err != nil {
		t.Fatal(err)
	}
	return e
}
func request(id, side, kind, q, p string) Request {
	return Request{ClientID: id, Product: Product, Side: side, Type: kind, Quantity: q, LimitPrice: p}
}
func submit(t *testing.T, e *Engine, r Request) Order {
	t.Helper()
	o, err := e.Submit(r)
	if err != nil {
		t.Fatal(err)
	}
	return o
}
func TestDecimal(t *testing.T) {
	for _, s := range []string{"", "-1", "NaN", "1e2", " 1", "1.001", "1."} {
		if _, err := ParseDecimal(s, 2); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
	n, err := ParseDecimal("100.01000000", 2)
	if err != nil || n != 10001 {
		t.Fatal(n, err)
	}
	if format(-101, 2) != "-1.01" {
		t.Fatal("format")
	}
}
func TestMarketWalkFeesAndPnL(t *testing.T) {
	e := setup(t)
	o := submit(t, e, request("b", "buy", "market", "1.5", ""))
	if o.Status != "FILLED" || o.Filled != Quantity(150000000) {
		t.Fatal(o)
	}
	s := e.State()
	if len(s.Fills) != 2 || s.Portfolio.CostBasis != 15066 || s.Portfolio.Cash != 9984934 {
		t.Fatalf("%+v", s.Portfolio)
	}
	sell := submit(t, e, request("s", "sell", "market", "1.5", ""))
	if sell.Status != "CANCELLED" || sell.Filled != Quantity(Scale) {
		t.Fatal(sell)
	} // 98 is outside the 1%% market collar.
	s = e.State()
	if s.Portfolio.Position != Quantity(50000000) || s.Portfolio.Realized != -154 {
		t.Fatalf("%+v", s.Portfolio)
	}
	// Exact conservation: equity - initial cash = realized + unrealized.
	if int64(s.Portfolio.Equity)-e.cfg.InitialCash != int64(s.Portfolio.Realized+s.Portfolio.Unrealized) {
		t.Fatal("P&L identity")
	}
}
func TestRestingPartialAndCancel(t *testing.T) {
	e := setup(t)
	o := submit(t, e, request("l", "buy", "limit", "1.5", "99.00"))
	if o.Status != "OPEN" {
		t.Fatal(o)
	}
	if e.State().Portfolio.ReservedCash != 14865 {
		t.Fatal(e.State().Portfolio)
	}
	if err := e.Snapshot([]InputLevel{{9700, Scale}}, []InputLevel{{9800, 50000000}, {10000, Scale}}); err != nil {
		t.Fatal(err)
	}
	s := e.State()
	if s.Orders[0].Status != "PARTIALLY_FILLED" || s.Orders[0].Filled != Quantity(50000000) {
		t.Fatal(s.Orders)
	}
	cancelled, err := e.Cancel(o.ID)
	if err != nil || cancelled.Status != "CANCELLED" || e.State().Portfolio.ReservedCash != 0 {
		t.Fatal(cancelled, err)
	}
}
func TestNoReuseOfConsumedDepth(t *testing.T) {
	e := setup(t)
	submit(t, e, request("a", "buy", "market", "1", ""))
	o := submit(t, e, request("b", "buy", "limit", "0.5", "100.00"))
	if o.Status != "OPEN" {
		t.Fatal(o)
	}
	if err := e.Update([]Change{{"sell", 10000, 50000000}}); err != nil {
		t.Fatal(err)
	}
	if e.State().Orders[1].Status != "FILLED" {
		t.Fatal(e.State().Orders)
	}
}
func TestPriceTimePriority(t *testing.T) {
	e := setup(t)
	submit(t, e, request("first", "buy", "limit", "0.5", "99.00"))
	submit(t, e, request("second", "buy", "limit", "0.5", "99.00"))
	if err := e.Snapshot([]InputLevel{{9700, Scale}}, []InputLevel{{9800, 50000000}}); err != nil {
		t.Fatal(err)
	}
	s := e.State()
	if s.Orders[0].Status != "FILLED" || s.Orders[1].Status != "OPEN" {
		t.Fatal(s.Orders)
	}
}
func TestRiskPendingAndInventory(t *testing.T) {
	e := New(DefaultConfig(), "test")
	_ = e.Snapshot([]InputLevel{{5999900, Scale}}, []InputLevel{{6000100, Scale}})
	for _, r := range []Request{request("large", "buy", "market", "0.2", ""), request("short", "sell", "market", "0.01", "")} {
		if o := submit(t, e, r); o.Status != "REJECTED" {
			t.Fatal(o)
		}
	}
	for _, id := range []string{"one", "two", "three"} {
		submit(t, e, request(id, "buy", "limit", "0.16", "59900.00"))
	}
	if o := submit(t, e, request("four", "buy", "limit", "0.16", "59900.00")); o.Status != "REJECTED" {
		t.Fatal(o)
	}
	e = setup(t)
	submit(t, e, request("buy", "buy", "market", "1", ""))
	submit(t, e, request("held", "sell", "limit", "0.8", "110.00"))
	if o := submit(t, e, request("again", "sell", "market", "0.3", "")); o.Status != "REJECTED" {
		t.Fatal(o)
	}
}
func TestCashReservation(t *testing.T) {
	e := setup(t)
	e.cfg.InitialCash = 15000
	e.cash = 15000
	submit(t, e, request("a", "buy", "limit", "1", "99.00"))
	if o := submit(t, e, request("b", "buy", "limit", "1", "99.00")); o.Status != "REJECTED" {
		t.Fatal(o)
	}
}
func TestIdempotentConcurrentRequests(t *testing.T) {
	e := setup(t)
	r := request("same", "buy", "market", "0.1", "")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.Submit(r); err != nil {
				t.Error(err)
			}
			_ = e.State()
		}()
	}
	wg.Wait()
	s := e.State()
	if len(s.Orders) != 1 || len(s.Fills) != 1 {
		t.Fatal(s)
	}
	r.Quantity = "0.2"
	if _, err := e.Submit(r); err != ErrConflict {
		t.Fatal(err)
	}
}
func TestStaleDisconnectAndKill(t *testing.T) {
	e := setup(t)
	now := e.now()
	e.now = func() time.Time { return now.Add(10 * time.Second) }
	if o := submit(t, e, request("stale", "buy", "market", "0.1", "")); o.Status != "REJECTED" {
		t.Fatal(o)
	}
	e.now = time.Now
	submit(t, e, request("open", "buy", "limit", "0.1", "90.00"))
	e.Halt(true)
	if e.State().Portfolio.ReservedCash != 0 {
		t.Fatal("holds not released")
	}
	if o := submit(t, e, request("halt", "buy", "market", "0.1", "")); o.Status != "REJECTED" {
		t.Fatal(o)
	}
	e.Halt(false)
	e.SetConnection(false, "test")
	if !e.State().Stale {
		t.Fatal("disconnect not stale")
	}
	if err := e.Update([]Change{{"sell", 10000, Scale}}); err == nil {
		t.Fatal("update without snapshot")
	}
}
func TestInvalidBatchDoesNotPartiallyApply(t *testing.T) {
	e := setup(t)
	if err := e.Update([]Change{{"buy", 9900, 200}, {"bogus", 10000, 1}}); err == nil {
		t.Fatal("bad side")
	}
	if e.State().Bids[0].Quantity != Quantity(Scale) {
		t.Fatal("partially applied")
	}
	if err := e.Update([]Change{{"buy", 11000, 1}}); err == nil {
		t.Fatal("crossed")
	}
	if e.State().Ready {
		t.Fatal("crossed marked ready")
	}
}
func TestJSONAndRejectedNoMutation(t *testing.T) {
	e := setup(t)
	initial := e.State().Portfolio
	for _, r := range []Request{request("x", "buy", "market", "-1", ""), request("y", "buy", "limit", "1", "1.001")} {
		if _, err := e.Submit(r); err == nil {
			t.Fatal("accepted malformed order")
		}
	}
	if e.State().Portfolio != initial {
		t.Fatal("invalid order changed account")
	}
	raw, err := json.Marshal(e.State())
	if err != nil || !json.Valid(raw) {
		t.Fatal(err)
	}
	var decoded map[string]any
	_ = json.Unmarshal(raw, &decoded)
	if decoded["portfolio"].(map[string]any)["cash"] != "100000.00" {
		t.Fatal(string(raw))
	}
}

func TestMoreAggressiveRestingPriceGetsLiquidityFirst(t *testing.T) {
	e := setup(t)
	submit(t, e, request("older", "buy", "limit", "0.5", "98.00"))
	submit(t, e, request("newer", "buy", "limit", "0.5", "99.00"))
	if err := e.Snapshot([]InputLevel{{9600, Scale}}, []InputLevel{{9700, 50000000}}); err != nil {
		t.Fatal(err)
	}
	s := e.State()
	if s.Orders[0].Status != "OPEN" || s.Orders[1].Status != "FILLED" {
		t.Fatal(s.Orders)
	}
}
func TestManyActionsPreserveAccounting(t *testing.T) {
	e := setup(t)
	for i := 0; i < 300; i++ {
		p := int64(10000 + i%23)
		if err := e.Snapshot([]InputLevel{{p - 100, Scale}}, []InputLevel{{p, Scale}}); err != nil {
			t.Fatal(err)
		}
		side := "buy"
		if i%3 == 0 {
			side = "sell"
		}
		r := request(fmt.Sprintf("trial-%d", i), side, "market", "0.01", "")
		submit(t, e, r)
		s := e.State()
		if s.Portfolio.Cash < 0 || s.Portfolio.AvailableCash < 0 || s.Portfolio.Position < 0 || int64(s.Portfolio.Position) > e.cfg.MaxPosition {
			t.Fatal(s.Portfolio)
		}
		if int64(s.Portfolio.Equity)-e.cfg.InitialCash != int64(s.Portfolio.Realized+s.Portfolio.Unrealized) {
			t.Fatal("P&L conservation", s.Portfolio)
		}
	}
}
