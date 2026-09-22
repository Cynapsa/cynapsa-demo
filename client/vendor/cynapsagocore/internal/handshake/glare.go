package handshake

// ResolveGlare chooses the same winner at both peers: bytewise initiator
// identity first, unpredictable attempt ID second. Invalid attempts lose.
func ResolveGlare(left, right Attempt) Attempt {
	leftValid, rightValid := validAttempt(left), validAttempt(right)
	if !leftValid {
		if rightValid {
			return right
		}
		return Attempt{}
	}
	if !rightValid {
		return left
	}
	if left.InitiatorID < right.InitiatorID || (left.InitiatorID == right.InitiatorID && left.ID < right.ID) {
		return left
	}
	return right
}
