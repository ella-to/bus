package sse

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
)

var splitData = []byte("\n\n")

func In[T any](ctx context.Context, r io.ReadCloser) <-chan *T {
	out := make(chan *T, 1)

	scanner := bufio.NewScanner(r)

	// Set the scanner's split function to split on "\n\n"
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		// Return nothing if at end of file and no data passed
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		idx := bytes.Index(data, splitData)
		if idx >= 0 {
			return idx + 2, data[:idx], nil
		}

		if atEOF {
			return len(data), data, nil
		}

		// We need more data
		return 0, nil, nil
	})

	secondPart := func(prefix, value string) (string, bool) {
		if !strings.HasPrefix(value, prefix) {
			return "", false
		}
		return strings.TrimSpace(value[len(prefix):]), true
	}

	// Close the reader when the context is cancelled
	// this is make sure the scanner.Scan() will return false
	// and the goroutine will exit
	go func() {
		<-ctx.Done()
		r.Close()
	}()

	go func() {
		defer close(out)
		for scanner.Scan() {
			item := scanner.Text()
			lines := strings.Split(item, "\n")

			if len(lines) != 3 {
				continue
			}

			identifier, ok := secondPart("id:", lines[0])
			if !ok {
				continue
			}

			// ignore id for now
			_, err := strconv.ParseInt(identifier, 10, 64)
			if err != nil {
				continue
			}

			// ignore event for now
			_, ok = secondPart("event:", lines[1])
			if !ok {
				continue
			}

			data, ok := secondPart("data:", lines[2])
			if !ok {
				continue
			}

			var msg T

			err = json.Unmarshal([]byte(data), &msg)
			if err != nil {
				continue
			}

			out <- &msg
		}
	}()

	return out
}
