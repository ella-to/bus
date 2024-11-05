package bus_test

import (
	"testing"

	"ella.to/bus"
)

func TestMatchSubject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		subject  string
		pattern  string
		expected bool
	}{
		// Exact matches
		{"a.b.c", "a.b.c", true},
		{"a.b", "a.b", true},

		// Single wildcard '*'
		{"a.b.c", "a.*.c", true},
		{"a.b.d", "a.*.d", true},
		{"a.x.y", "a.*.*", true},
		{"a.b", "a.*", true},
		{"ab.c", "ab.*", true},

		// Catch-all '>'
		{"a.b.c", "a.>", true},
		{"a.b.c.d", "a.>", true},
		{"a.b.c.d.e", "a.>", true},
		{"a.b", "a.>", true},

		// Mixed wildcard '*' and catch-all '>'
		{"a.b.c", "a.*.>", true},
		{"a.b.c.d", "a.*.>", true},
		{"a.b.c", "a.*.*", true},

		// Non-matching cases
		{"a.b.c", "a.b.d", false},
		{"a.b.c", "a.b", false},
		{"a.b.c", "a.c.>", false},
		{"a.b.c", "a.*.d", false},

		// No wildcards
		{"a.b.c", "a.b.d", false},
		{"a.b.c.d", "a.b.c", false},

		// Edge cases
		{"", "", true},              // Both empty
		{"a.b.c", "", false},        // Empty pattern
		{"", "a.b.c", false},        // Empty subject
		{"a.b.c", "a.b.c.d", false}, // Pattern longer than subject
		{"a.b.c.d", "a.b.c", false}, // Subject longer than pattern
		{"a.b.c", ">", true},        // Catch-all matches everything
		{"a.b.c", "*", false},       // '*' doesn't span across dots
		{"a.b.c", "*.*", false},     // Wildcards must match dot-separated segments
		{"a.b.c", "*.*.*", true},    // Wildcards must match dot-separated segments
	}

	for _, test := range tests {
		result := bus.MatchSubject(test.subject, test.pattern)
		if result != test.expected {
			t.Errorf("MatchSubject(%q, %q) = %v; want %v", test.subject, test.pattern, result, test.expected)
		}
	}
}

func BenchmarkMatchSubject(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bus.MatchSubject("a.b.c", "a.*.c")
	}
}
