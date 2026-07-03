package bus

import (
	"encoding/json"
	"testing"
	"time"
)

var benchEvent = Event{
	Id:        "e_cu1p3negk05q0chhe123",
	TraceId:   "trace-1234",
	Subject:   "a.b.c",
	CreatedAt: time.Date(2025, 1, 18, 6, 55, 35, 123456789, time.UTC),
	Payload:   json.RawMessage(`{"key": "value", "n": 42}`),
}

func BenchmarkEventAppendJSON(b *testing.B) {
	b.ReportAllocs()
	var buf []byte
	for b.Loop() {
		buf = benchEvent.appendJSON(buf[:0])
	}
}

func BenchmarkExtractIdSubject(b *testing.B) {
	record := benchEvent.appendJSON(nil)

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := extractIdSubject(record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTryParseEvent(b *testing.B) {
	record := benchEvent.appendJSON(nil)

	b.ReportAllocs()
	var e Event
	for b.Loop() {
		if _, err := tryParseEvent(record, &e); err != nil {
			b.Fatal(err)
		}
	}
}

func TestExtractIdSubject(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		id      string
		subject string
		wantErr bool
	}{
		{
			name:    "simple",
			input:   `{"id":"e_1","subject":"a.b.c","created_at":"2025-01-18T06:55:35Z"}`,
			id:      "e_1",
			subject: "a.b.c",
		},
		{
			name:    "subject before id with payload",
			input:   `{"subject":"a.b","payload":{"x":[1,2,{"y":"}"}]},"id":"e_2","index":10}`,
			id:      "e_2",
			subject: "a.b",
		},
		{
			name:    "escaped strings",
			input:   `{"id":"e_3","key":"we\"ird","subject":"a.b","created_at":"2025-01-18T06:55:35Z"}`,
			id:      "e_3",
			subject: "a.b",
		},
		{
			name:    "missing subject",
			input:   `{"id":"e_4"}`,
			wantErr: true,
		},
		{
			name:    "not json",
			input:   `hello`,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, subject, err := extractIdSubject([]byte(tc.input))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got id=%q subject=%q", id, subject)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if id != tc.id || subject != tc.subject {
				t.Fatalf("expected (%q, %q), got (%q, %q)", tc.id, tc.subject, id, subject)
			}
		})
	}
}

func TestAppendJSONStringEscaping(t *testing.T) {
	in := "a\"b\\c\nd\re\tf\x01g"
	out := string(appendJSONString(nil, in))
	want := "a\\\"b\\\\c\\nd\\re\\tf\\u0001g"
	if out != want {
		t.Fatalf("expected %q, got %q", want, out)
	}

	// round trip through the parser
	quoted := append(append([]byte{'"'}, out...), '"')
	got, _, err := parseString(quoted)
	if err != nil {
		t.Fatal(err)
	}
	if got != in {
		t.Fatalf("round trip mismatch: expected %q, got %q", in, got)
	}
}

func TestParseStringUnicodeEscapes(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"\"\\u0041\"", "A"},
		{"\"\\u00e9\"", "é"},
		{"\"\\ud83d\\ude00\"", "😀"}, // surrogate pair
		{"\"pre\\u0041post\"", "preApost"},
	}

	for _, tc := range cases {
		got, n, err := parseString([]byte(tc.input))
		if err != nil {
			t.Fatalf("%s: %s", tc.input, err)
		}
		if n != len(tc.input) {
			t.Fatalf("%s: consumed %d bytes, expected %d", tc.input, n, len(tc.input))
		}
		if got != tc.want {
			t.Fatalf("%s: expected %q, got %q", tc.input, tc.want, got)
		}
	}
}
