// Package engine owns one paper account and one BTC-USD book.
// Every public operation is serialized by mu so fills and risk reservations are atomic.
package engine

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const Product = "BTC-USD"

type Level struct {
	Price     Price    `json:"price"`
	Quantity  Quantity `json:"quantity"`
	Available Quantity `json:"available"`
}
type InputLevel struct {
	Price    int64
	Quantity int64
}
type Change struct {
	Side     string
	Price    int64
	Quantity int64
}
type Request struct {
	ClientID   string `json:"client_order_id"`
	Product    string `json:"product"`
	Side       string `json:"side"`
	Type       string `json:"type"`
	Quantity   string `json:"quantity"`
	LimitPrice string `json:"limit_price,omitempty"`
	Source     string `json:"source,omitempty"`
}
type Fill struct {
	ID       string    `json:"id"`
	OrderID  string    `json:"order_id"`
	Side     string    `json:"side"`
	Price    Price     `json:"price"`
	Quantity Quantity  `json:"quantity"`
	Fee      Price     `json:"fee"`
	Time     time.Time `json:"time"`
}
type Order struct {
	ID         string    `json:"id"`
	ClientID   string    `json:"client_order_id"`
	Side       string    `json:"side"`
	Type       string    `json:"type"`
	Source     string    `json:"source"`
	Quantity   Quantity  `json:"quantity"`
	Filled     Quantity  `json:"filled_quantity"`
	LimitPrice Price     `json:"limit_price"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason,omitempty"`
	Created    time.Time `json:"created_at"`
	cap        int64
	request    Request
}
type Event struct {
	ID      int64     `json:"id"`
	Type    string    `json:"type"`
	OrderID string    `json:"order_id,omitempty"`
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}
type Portfolio struct {
	Cash             Price    `json:"cash"`
	ReservedCash     Price    `json:"reserved_cash"`
	AvailableCash    Price    `json:"available_cash"`
	Position         Quantity `json:"position"`
	ReservedPosition Quantity `json:"reserved_position"`
	CostBasis        Price    `json:"cost_basis"`
	Realized         Price    `json:"realized_pnl"`
	Unrealized       Price    `json:"unrealized_pnl"`
	Equity           Price    `json:"equity"`
	Fees             Price    `json:"fees"`
}
type State struct {
	Product     string    `json:"product"`
	Sequence    int64     `json:"sequence"`
	Mode        string    `json:"mode"`
	Connected   bool      `json:"connected"`
	Ready       bool      `json:"ready"`
	Stale       bool      `json:"stale"`
	Halted      bool      `json:"halted"`
	FeedMessage string    `json:"feed_message"`
	Updated     time.Time `json:"updated_at"`
	Bids        []Level   `json:"bids"`
	Asks        []Level   `json:"asks"`
	Spread      Price     `json:"spread"`
	Mid         Price     `json:"mid"`
	Portfolio   Portfolio `json:"portfolio"`
	Orders      []Order   `json:"orders"`
	Fills       []Fill    `json:"fills"`
	Events      []Event   `json:"events"`
	Risk        Config    `json:"risk"`
}
type Config struct {
	InitialCash      int64         `json:"initial_cash_cents"`
	MaxOrderNotional int64         `json:"max_order_notional_cents"`
	MaxPosition      int64         `json:"max_position_satoshis"`
	FeeBPS           int64         `json:"fee_bps"`
	SlippageBPS      int64         `json:"slippage_bps"`
	MaxOpenOrders    int           `json:"max_open_orders"`
	MaxRecords       int           `json:"max_records"`
	StaleAfter       time.Duration `json:"stale_after_ns"`
}

func DefaultConfig() Config {
	return Config{10000000, 1000000, 50000000, 10, 100, 20, 10000, 5 * time.Second}
}

type Engine struct {
	mu                                    sync.Mutex
	cfg                                   Config
	mode                                  string
	now                                   func() time.Time
	bids, asks, usedBids, usedAsks        map[int64]int64
	connected, ready, halted              bool
	feedMessage                           string
	updated                               time.Time
	sequence                              int64
	cash, position, basis, realized, fees int64
	orders                                []*Order
	clients                               map[string]*Order
	fills                                 []Fill
	events                                []Event
	next                                  int64
}

func New(cfg Config, mode string) *Engine {
	return &Engine{cfg: cfg, mode: mode, now: time.Now, bids: map[int64]int64{}, asks: map[int64]int64{}, usedBids: map[int64]int64{}, usedAsks: map[int64]int64{}, cash: cfg.InitialCash, clients: map[string]*Order{}, orders: []*Order{}, fills: []Fill{}, events: []Event{}}
}
func active(o *Order) bool { return o.Status == "OPEN" || o.Status == "PARTIALLY_FILLED" }
func (e *Engine) event(kind, id, message string) {
	e.sequence++
	e.events = append(e.events, Event{e.sequence, kind, id, message, e.now().UTC()})
	if len(e.events) > 200 {
		e.events = e.events[len(e.events)-200:]
	}
}
func (e *Engine) SetConnection(connected bool, message string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.connected = connected
	e.feedMessage = message
	if !connected {
		e.ready = false
	}
	e.event("FEED", "", message)
}
func validateLevels(levels []InputLevel) error {
	for _, l := range levels {
		if l.Price <= 0 || l.Price > 100000000 || l.Quantity < 0 || l.Quantity > 1000000000000 {
			return errors.New("invalid price/quantity in book")
		}
	}
	return nil
}
func maps(levels []InputLevel) map[int64]int64 {
	m := map[int64]int64{}
	for _, l := range levels {
		if l.Quantity > 0 {
			m[l.Price] = l.Quantity
		}
	}
	return m
}
func best(m map[int64]int64, high bool) int64 {
	var p int64
	for price, q := range m {
		if q > 0 && (p == 0 || (high && price > p) || (!high && price < p)) {
			p = price
		}
	}
	return p
}
func validBook(b, a map[int64]int64) bool {
	bid, ask := best(b, true), best(a, false)
	return bid > 0 && ask > bid
}
func (e *Engine) Snapshot(bids, asks []InputLevel) error {
	if err := validateLevels(bids); err != nil {
		return err
	}
	if err := validateLevels(asks); err != nil {
		return err
	}
	b, a := maps(bids), maps(asks)
	if !validBook(b, a) {
		return errors.New("empty or crossed book")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.bids = b
	e.asks = a
	e.usedBids = map[int64]int64{}
	e.usedAsks = map[int64]int64{}
	e.connected = true
	e.ready = true
	e.updated = e.now()
	e.feedMessage = "snapshot synchronized"
	e.sequence++
	e.matchAll()
	return nil
}
func clone(m map[int64]int64) map[int64]int64 {
	c := map[int64]int64{}
	for k, v := range m {
		c[k] = v
	}
	return c
}
func (e *Engine) Update(changes []Change) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.ready {
		return errors.New("snapshot required before updates")
	}
	b, a := clone(e.bids), clone(e.asks)
	for _, c := range changes {
		if c.Side != "buy" && c.Side != "sell" {
			return errors.New("invalid book side")
		}
		if err := validateLevels([]InputLevel{{c.Price, c.Quantity}}); err != nil {
			return err
		}
		m := b
		if c.Side == "sell" {
			m = a
		}
		if c.Quantity == 0 {
			delete(m, c.Price)
		} else {
			m[c.Price] = c.Quantity
		}
	}
	if !validBook(b, a) {
		e.ready = false
		return errors.New("empty or crossed book; resnapshot required")
	}
	e.bids = b
	e.asks = a
	for _, c := range changes {
		if c.Side == "buy" {
			delete(e.usedBids, c.Price)
		} else {
			delete(e.usedAsks, c.Price)
		}
	}
	e.updated = e.now()
	e.sequence++
	e.matchAll()
	return nil
}
func (e *Engine) stale() bool { return !e.ready || e.now().Sub(e.updated) > e.cfg.StaleAfter }
func (e *Engine) reserve(o *Order) (int64, int64) {
	if !active(o) {
		return 0, 0
	}
	q := int64(o.Quantity - o.Filled)
	if o.Side == "sell" {
		return 0, q
	}
	gross := mulDiv(q, o.cap, Scale, true)
	return gross + mulDiv(gross, e.cfg.FeeBPS, 10000, true), 0
}
func (e *Engine) reservations(exclude *Order) (cash, qty, buys int64, open int) {
	for _, o := range e.orders {
		if o == exclude || !active(o) {
			continue
		}
		c, q := e.reserve(o)
		cash += c
		qty += q
		if o.Side == "buy" {
			buys += int64(o.Quantity - o.Filled)
		}
		open++
	}
	return
}

var ErrConflict = errors.New("client order id already used for a different request")

func (e *Engine) Submit(r Request) (Order, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if r.ClientID == "" || len(r.ClientID) > 100 {
		return Order{}, errors.New("client_order_id must contain 1-100 characters")
	}
	if existing, ok := e.clients[r.ClientID]; ok {
		if existing.request != r {
			return Order{}, ErrConflict
		}
		return *existing, nil
	}
	if r.Product != Product {
		return Order{}, errors.New("only BTC-USD is supported")
	}
	if r.Side != "buy" && r.Side != "sell" {
		return Order{}, errors.New("side must be buy or sell")
	}
	if r.Type != "market" && r.Type != "limit" {
		return Order{}, errors.New("type must be market or limit")
	}
	q, err := ParseDecimal(r.Quantity, 8)
	if err != nil || q <= 0 || q > 10000000000 {
		return Order{}, errors.New("quantity must be positive, <=100 BTC and have <=8 decimals")
	}
	var p int64
	if r.Type == "limit" {
		p, err = ParseDecimal(r.LimitPrice, 2)
		if err != nil || p <= 0 || p > 100000000 {
			return Order{}, errors.New("limit_price must be positive, <=1000000 USD and have <=2 decimals")
		}
	} else if r.LimitPrice != "" {
		return Order{}, errors.New("market orders cannot have limit_price")
	}
	if len(r.Source) > 100 {
		return Order{}, errors.New("source too long")
	}
	if len(e.orders) >= e.cfg.MaxRecords {
		return Order{}, errors.New("session order capacity reached; restart for a new session")
	}
	e.next++
	o := &Order{ID: fmt.Sprintf("ord-%06d", e.next), ClientID: r.ClientID, Side: r.Side, Type: r.Type, Source: r.Source, Quantity: Quantity(q), LimitPrice: Price(p), Status: "OPEN", Created: e.now().UTC(), request: r, cap: p}
	e.orders = append(e.orders, o)
	e.clients[r.ClientID] = o
	e.event("ORDER_ACCEPTED", o.ID, "request validated")
	reject := func(reason string) (Order, error) {
		o.Status = "REJECTED"
		o.Reason = reason
		e.event("ORDER_REJECTED", o.ID, reason)
		return *o, nil
	}
	if e.halted {
		return reject("kill switch is enabled")
	}
	if e.stale() {
		return reject("market data unavailable or stale")
	}
	if r.Type == "market" {
		reference := best(e.asks, false)
		if r.Side == "buy" {
			o.cap = mulDiv(reference, 10000+e.cfg.SlippageBPS, 10000, true)
		} else {
			reference = best(e.bids, true)
			o.cap = mulDiv(reference, 10000-e.cfg.SlippageBPS, 10000, false)
		}
	}
	checkPrice := o.cap
	if r.Side == "sell" {
		checkPrice = best(e.bids, true)
		if p > checkPrice {
			checkPrice = p
		}
	}
	if mulDiv(q, checkPrice, Scale, true) > e.cfg.MaxOrderNotional {
		return reject("maximum order notional exceeded")
	}
	heldCash, heldQty, pendingBuys, open := e.reservations(o)
	if open >= e.cfg.MaxOpenOrders {
		return reject("maximum open order count exceeded")
	}
	if r.Side == "buy" {
		cost, _ := e.reserve(o)
		if cost > e.cash-heldCash {
			return reject("insufficient unreserved cash")
		}
		if e.position+pendingBuys+q > e.cfg.MaxPosition {
			return reject("maximum position including pending buys exceeded")
		}
	} else if q > e.position-heldQty {
		return reject("insufficient unreserved BTC; short selling disabled")
	}
	e.match(o)
	if r.Type == "market" && active(o) {
		o.Status = "CANCELLED"
		o.Reason = "unfilled market remainder cancelled (IOC / slippage limit)"
		e.event("ORDER_CANCELLED", o.ID, o.Reason)
	}
	return *o, nil
}
func prices(m map[int64]int64, descending bool) []int64 {
	p := make([]int64, 0, len(m))
	for k := range m {
		p = append(p, k)
	}
	sort.Slice(p, func(i, j int) bool {
		if descending {
			return p[i] > p[j]
		}
		return p[i] < p[j]
	})
	return p
}
func (e *Engine) matchAll() {
	if e.halted || e.stale() {
		return
	}
	pending := make([]*Order, 0)
	for _, o := range e.orders {
		if active(o) {
			pending = append(pending, o)
		}
	}
	sort.SliceStable(pending, func(i, j int) bool {
		a, b := pending[i], pending[j]
		if a.Side != b.Side {
			return a.Side == "buy"
		}
		if a.cap == b.cap {
			return false
		}
		if a.Side == "buy" {
			return a.cap > b.cap
		}
		return a.cap < b.cap
	})
	for _, o := range pending {
		e.match(o)
	}
}
func (e *Engine) match(o *Order) {
	book, used := e.asks, e.usedAsks
	if o.Side == "sell" {
		book, used = e.bids, e.usedBids
	}
	for _, p := range prices(book, o.Side == "sell") {
		if (o.Side == "buy" && p > o.cap) || (o.Side == "sell" && p < o.cap) {
			break
		}
		q := int64(o.Quantity - o.Filled)
		available := book[p] - used[p]
		if q > available {
			q = available
		}
		if q <= 0 {
			continue
		}
		// Record capacity is bounded; never silently lose a fill.
		if len(e.fills) >= e.cfg.MaxRecords*10 {
			o.Status = "CANCELLED"
			o.Reason = "fill capacity reached"
			e.event("ORDER_CANCELLED", o.ID, o.Reason)
			return
		}
		gross := mulDiv(q, p, Scale, o.Side == "buy")
		fee := mulDiv(gross, e.cfg.FeeBPS, 10000, true)
		if o.Side == "buy" {
			held, _, _, _ := e.reservations(o)
			if gross+fee > e.cash-held {
				o.Status = "CANCELLED"
				o.Reason = "insufficient cash after fill rounding"
				e.event("ORDER_CANCELLED", o.ID, o.Reason)
				return
			}
			e.cash -= gross + fee
			e.position += q
			e.basis += gross + fee
		} else {
			allocated := e.basis
			if q < e.position {
				allocated = mulDiv(e.basis, q, e.position, false)
			}
			e.cash += gross - fee
			e.position -= q
			e.basis -= allocated
			e.realized += gross - fee - allocated
		}
		e.fees += fee
		used[p] += q
		o.Filled += Quantity(q)
		o.Status = "PARTIALLY_FILLED"
		if o.Filled == o.Quantity {
			o.Status = "FILLED"
		}
		f := Fill{fmt.Sprintf("fill-%06d", len(e.fills)+1), o.ID, o.Side, Price(p), Quantity(q), Price(fee), e.now().UTC()}
		e.fills = append(e.fills, f)
		e.event("FILL", o.ID, fmt.Sprintf("%s %s BTC @ %s", o.Side, format(q, 8), format(p, 2)))
		if o.Status == "FILLED" {
			return
		}
	}
}
func (e *Engine) Cancel(id string) (Order, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, o := range e.orders {
		if o.ID == id {
			if active(o) {
				o.Status = "CANCELLED"
				o.Reason = "cancelled by user"
				e.event("ORDER_CANCELLED", o.ID, o.Reason)
			}
			return *o, nil
		}
	}
	return Order{}, errors.New("order not found")
}
func (e *Engine) Halt(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.halted = enabled
	if enabled {
		for _, o := range e.orders {
			if active(o) {
				o.Status = "CANCELLED"
				o.Reason = "kill switch"
				e.event("ORDER_CANCELLED", o.ID, o.Reason)
			}
		}
	}
	e.event("RISK", "", fmt.Sprintf("kill switch=%t", enabled))
}
func (e *Engine) State() State {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := State{Product: Product, Sequence: e.sequence, Mode: e.mode, Connected: e.connected, Ready: e.ready, Stale: e.stale(), Halted: e.halted, FeedMessage: e.feedMessage, Updated: e.updated, Risk: e.cfg, Orders: []Order{}, Fills: []Fill{}, Events: []Event{}}
	levels := func(book, used map[int64]int64, desc bool) []Level {
		result := []Level{}
		for _, p := range prices(book, desc) {
			q := book[p] - used[p]
			if q < 0 {
				q = 0
			}
			result = append(result, Level{Price(p), Quantity(book[p]), Quantity(q)})
			if len(result) == 15 {
				break
			}
		}
		return result
	}
	s.Bids = levels(e.bids, e.usedBids, true)
	s.Asks = levels(e.asks, e.usedAsks, false)
	bid, ask := best(e.bids, true), best(e.asks, false)
	if bid > 0 && ask > bid {
		s.Spread = Price(ask - bid)
		s.Mid = Price((ask + bid) / 2)
	}
	held, qty, _, _ := e.reservations(nil)
	mark := mulDiv(e.position, bid, Scale, false)
	s.Portfolio = Portfolio{Price(e.cash), Price(held), Price(e.cash - held), Quantity(e.position), Quantity(qty), Price(e.basis), Price(e.realized), Price(mark - e.basis), Price(e.cash + mark), Price(e.fees)}
	start := len(e.orders) - 100
	if start < 0 {
		start = 0
	}
	for _, o := range e.orders[start:] {
		s.Orders = append(s.Orders, *o)
	}
	start = len(e.fills) - 100
	if start < 0 {
		start = 0
	}
	s.Fills = append(s.Fills, e.fills[start:]...)
	s.Events = append(s.Events, e.events...)
	return s
}
