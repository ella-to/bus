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

func TestValidateSubject(t *testing.T) {
	tests := []struct {
		name    string
		subject string
		wantErr bool
		errMsg  string
	}{
		// Valid subjects
		{"Valid simple subject", "a.b.c", false, ""},
		{"Valid subject with *", "a.b.*", false, ""},
		{"Valid subject with >", "a.b.>", false, ""},

		// Invalid subjects
		{"Should always start with alphanumeric 1", ".a.b", true, "subject should not starts with dot"},
		{"Should always start with alphanumeric 2", "*", true, "subject should not starts with *"},
		{"Should always start with alphanumeric 3", ">", true, "subject should not starts with >"},
		{"Empty subject", "", true, "subject is empty"},
		{"Starts with .", ".a.b", true, "subject should not starts with dot"},
		{"Ends with .", "a.b.", true, "subject should not ends with dot"},
		{"Contains spaces", "a. b.c", true, "subject should not have spaces"},
		{"Series of dots", "a..b.c", true, "subject should not have series of dots one after another"},
		{"Invalid character", "a.b.c$", true, "subject should have only consists of alphanumerics, dots, *, > and _"},
		{"Middle >", "a.b>.c", true, "subject should not have anything after >"},
		{"No dot before >", "a.b>", true, "subject should have a dot before >"},
		{"No dot before *", "a.b*", true, "subject should have a dot before *"},
		{"No dot after *", "a.*b", true, "subject should have a dot after *"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := bus.ValidateSubject(tt.subject)
			if (err != nil) != tt.wantErr {
				t.Errorf("expected error: %v, got: %v", tt.wantErr, err)
			}

			if tt.wantErr && err.Error() != tt.errMsg {
				t.Errorf("expected error message: %v, got: %v", tt.errMsg, err.Error())
			}
		})
	}
}
