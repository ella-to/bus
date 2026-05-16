package bus

// trackCopy implements the streaming copy state machine used by Event.Read.
// It mirrors the upstream implementation so that JSON output is byte-for-byte
// identical with ella.to/bus.
type trackCopy struct {
	currenState int
	index       int
}

func (s *trackCopy) reset() {
	s.currenState = 0
	s.index = 0
}

func (s *trackCopy) Copy(dst, src []byte, state int) int {
	if s.currenState != state {
		s.currenState = state
		s.index = 0
	}
	n := copy(dst, src[s.index:])
	s.index += n
	return n
}
