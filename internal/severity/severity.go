// Package severity defines the four health-check result levels.
package severity

import "encoding/json"

// Severity is an integer so findings can be compared with > or sorted.
type Severity int

const (
	OK       Severity = iota // ✓ passed – no action needed
	INFO                     // ⓘ advisory – review at your pace
	WARN                     // ⚠ fix before the next incident window
	CRITICAL                 // ✗ fix now – outage or data-loss risk
)

func (s Severity) String() string {
	switch s {
	case OK:
		return "OK"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case CRITICAL:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// MarshalJSON outputs the string label ("OK", "WARN" …) not the integer.
func (s Severity) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// Max returns whichever severity is higher.
func Max(a, b Severity) Severity {
	if b > a {
		return b
	}
	return a
}
