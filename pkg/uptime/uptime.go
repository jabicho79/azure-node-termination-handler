// Package uptime reads the system uptime.
package uptime

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// FromFile returns a function that reads the uptime from a file in
// /proc/uptime format ("12345.67 890.12"): the first field is seconds since
// boot.
func FromFile(path string) func() (time.Duration, error) {
	return func() (time.Duration, error) {
		b, err := os.ReadFile(path)
		if err != nil {
			return 0, fmt.Errorf("reading uptime: %w", err)
		}
		fields := strings.Fields(string(b))
		if len(fields) == 0 {
			return 0, fmt.Errorf("parsing uptime from %s: empty file", path)
		}
		s, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return 0, fmt.Errorf("parsing uptime from %s: %w", path, err)
		}
		if math.IsNaN(s) || math.IsInf(s, 0) || s < 0 {
			return 0, fmt.Errorf("parsing uptime from %s: invalid value %q", path, fields[0])
		}
		return time.Duration(s * float64(time.Second)), nil
	}
}
