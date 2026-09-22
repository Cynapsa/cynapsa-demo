package peer

// Rank identifies only the private preferred outbound implementation.
type Rank uint8

const (
	RankUnknown Rank = iota
	RankLive
	RankDurable
)

func selectRank(state State) Rank {
	if state.PreferredRank == RankLive {
		return RankLive
	}
	return RankDurable
}
