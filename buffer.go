package bus

type trackCopy struct {
	currenState int
	index       int
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
