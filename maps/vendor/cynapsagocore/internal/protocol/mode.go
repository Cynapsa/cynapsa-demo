package protocol

// Mode identifies private message interaction semantics.
type Mode string

const (
	ModeMessage  Mode = "msg"
	ModeRequest  Mode = "rpc"
	ModeResponse Mode = "rpc_response"
)

const (
	wireModeMessage uint8 = iota
	wireModeRequest
	wireModeResponse
)

// Validate rejects unknown message modes.
func (m Mode) Validate() error {
	switch m {
	case ModeMessage, ModeRequest, ModeResponse:
		return nil
	default:
		return ErrInvalidMode
	}
}

func (m Mode) wireValue() (uint8, error) {
	switch m {
	case ModeMessage:
		return wireModeMessage, nil
	case ModeRequest:
		return wireModeRequest, nil
	case ModeResponse:
		return wireModeResponse, nil
	default:
		return 0, ErrInvalidMode
	}
}

func modeFromWire(v uint8) (Mode, error) {
	switch v {
	case wireModeMessage:
		return ModeMessage, nil
	case wireModeRequest:
		return ModeRequest, nil
	case wireModeResponse:
		return ModeResponse, nil
	default:
		return "", ErrInvalidMode
	}
}
