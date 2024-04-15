package sse

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"strconv"
)

func Receive(ctx context.Context, r io.ReadCloser) <-chan *Payload {
	out := make(chan *Payload, 1)

	scanner := bufio.NewScanner(r)

	// Set the scanner's split function to split on "\n\n"
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		// Return nothing if at end of file and no data passed
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		idx := bytes.Index(data, doubleEnters)
		if idx >= 0 {
			return idx + 2, data[:idx], nil
		}

		if atEOF {
			return len(data), data, nil
		}

		// We need more data
		return 0, nil, nil
	})

	secondPart := func(prefix, value []byte) ([]byte, bool) {
		if !bytes.HasPrefix(value, prefix) {
			return nil, false
		}
		return bytes.TrimSpace(value[len(prefix):]), true
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
			item := scanner.Bytes()

			lines := bytes.Split(item, singleEnter)

			if len(lines) != 3 {
				continue
			}

			identifier, ok := secondPart(idPrefix, lines[0])
			if !ok {
				continue
			}

			// ignore id for now
			id, err := strconv.ParseInt(string(identifier), 10, 64)
			if err != nil {
				continue
			}

			// ignore event for now
			event, ok := secondPart(eventPrefix, lines[1])
			if !ok {
				continue
			}

			data, ok := secondPart(dataPrefix, lines[2])
			if !ok {
				continue
			}

			msg := &Payload{
				Id:    id,
				Event: string(event),
				Data:  data,
			}

			out <- msg
		}
	}()

	return out
}
