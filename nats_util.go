package bus

import (
	"fmt"
	"net"
	"strconv"
)

func isPortOpen(port int) bool {
	addr := ":" + strconv.Itoa(port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return false // Port is not open
	}
	ln.Close() // Close the listener if the port is open
	return true
}

// findOpenPortInRange returns the first open port within a given range.
func findOpenPortInRange(start, end int) (int, error) {
	for port := start; port <= end; port++ {
		if isPortOpen(port) {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no open ports found in range %d-%d", start, end)
}
