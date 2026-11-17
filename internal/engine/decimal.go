package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

const Scale int64 = 100000000

// ParseDecimal accepts plain nonnegative decimal strings; no float conversion.
func ParseDecimal(s string, places int) (int64, error) {
	if s == "" || strings.TrimSpace(s) != s {
		return 0, errors.New("expected a decimal string")
	}
	parts := strings.Split(s, ".")
	if len(parts) > 2 || parts[0] == "" {
		return 0, errors.New("invalid decimal")
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
		if fraction == "" {
			return 0, errors.New("invalid decimal")
		}
	}
	// Exchange feeds sometimes pad cents with zeroes.
	for len(fraction) > places && strings.HasSuffix(fraction, "0") {
		fraction = strings.TrimSuffix(fraction, "0")
	}
	if len(fraction) > places {
		return 0, fmt.Errorf("maximum %d decimal places", places)
	}
	digits := parts[0] + fraction + strings.Repeat("0", places-len(fraction))
	for _, r := range digits {
		if r < '0' || r > '9' {
			return 0, errors.New("invalid decimal")
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, errors.New("decimal out of range")
	}
	return n, nil
}
func format(n int64, places int) string {
	sign := ""
	if n < 0 {
		sign = "-"
		n = -n
	}
	power := int64(1)
	for i := 0; i < places; i++ {
		power *= 10
	}
	return fmt.Sprintf("%s%d.%0*d", sign, n/power, places, n%power)
}

// All monetary products use wide intermediates, rounding conservatively.
func mulDiv(a, b, d int64, ceil bool) int64 {
	n := new(big.Int).Mul(big.NewInt(a), big.NewInt(b))
	if ceil {
		n.Add(n, big.NewInt(d-1))
	}
	n.Quo(n, big.NewInt(d))
	return n.Int64()
}

type Price int64

func (p Price) MarshalJSON() ([]byte, error) { return json.Marshal(format(int64(p), 2)) }

type Quantity int64

func (q Quantity) MarshalJSON() ([]byte, error) { return json.Marshal(format(int64(q), 8)) }
