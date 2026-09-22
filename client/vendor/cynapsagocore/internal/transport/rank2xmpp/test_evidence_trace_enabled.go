//go:build cynapsa_test_evidence

package rank2xmpp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

var rank2EvidenceOutputMu sync.Mutex

func emitRank2Evidence(record rank2EvidenceRecord, err error) {
	output := struct {
		rank2EvidenceRecord
		At        string `json:"at"`
		ErrorType string `json:"error_type,omitempty"`
		Error     string `json:"error,omitempty"`
		CauseType string `json:"cause_type,omitempty"`
		Cause     string `json:"cause,omitempty"`
		WireStage string `json:"wire_stage,omitempty"`
	}{rank2EvidenceRecord: record, At: time.Now().UTC().Format(time.RFC3339Nano)}
	if err != nil {
		output.ErrorType = fmt.Sprintf("%T", err)
		output.Error = boundedEvidenceError(err)
		cause := err
		for errors.Unwrap(cause) != nil {
			cause = errors.Unwrap(cause)
		}
		if cause != err {
			output.CauseType = fmt.Sprintf("%T", cause)
			output.Cause = boundedEvidenceError(cause)
		}
		var wire *wireError
		if errors.As(err, &wire) {
			switch wire.stage {
			case wireNotStarted:
				output.WireStage = "not-started"
			case wireAmbiguous:
				output.WireStage = "ambiguous"
			case wireComplete:
				output.WireStage = "complete"
			default:
				output.WireStage = "invalid"
			}
		}
	}
	encoded, marshalErr := json.Marshal(output)
	if marshalErr != nil {
		return
	}
	rank2EvidenceOutputMu.Lock()
	_, _ = fmt.Fprintf(os.Stderr, "CYNAPSA_RANK2_EVIDENCE %s\n", encoded)
	rank2EvidenceOutputMu.Unlock()
}

func boundedEvidenceError(err error) string {
	if err == nil {
		return ""
	}
	message := strings.ReplaceAll(err.Error(), "\n", " ")
	if len(message) > 256 {
		message = message[:256]
	}
	return message
}

type rank2EvidenceConn struct {
	net.Conn
	stage string
}

func wrapRank2EvidenceConn(conn net.Conn, stage string) net.Conn {
	if conn == nil {
		return nil
	}
	return &rank2EvidenceConn{Conn: conn, stage: stage}
}

func (conn *rank2EvidenceConn) Close() error {
	emitRank2Evidence(rank2EvidenceRecord{
		Event:  "raw_socket_close",
		Source: "net_conn",
		Stage:  conn.stage,
		Caller: rank2EvidenceCaller(),
	}, nil)
	return conn.Conn.Close()
}

func (conn *rank2EvidenceConn) SetDeadline(deadline time.Time) error {
	conn.emitDeadline("set_deadline", deadline)
	return conn.Conn.SetDeadline(deadline)
}

func (conn *rank2EvidenceConn) SetReadDeadline(deadline time.Time) error {
	conn.emitDeadline("set_read_deadline", deadline)
	return conn.Conn.SetReadDeadline(deadline)
}

func (conn *rank2EvidenceConn) SetWriteDeadline(deadline time.Time) error {
	conn.emitDeadline("set_write_deadline", deadline)
	return conn.Conn.SetWriteDeadline(deadline)
}

func (conn *rank2EvidenceConn) emitDeadline(event string, deadline time.Time) {
	if deadline.IsZero() {
		return
	}
	emitRank2Evidence(rank2EvidenceRecord{
		Event:    event,
		Source:   "net_conn",
		Stage:    conn.stage,
		Caller:   rank2EvidenceCaller(),
		Deadline: deadline.UTC().Format(time.RFC3339Nano),
	}, nil)
}

func rank2EvidenceCaller() string {
	callers := make([]uintptr, 16)
	count := runtime.Callers(3, callers)
	frames := runtime.CallersFrames(callers[:count])
	for {
		frame, more := frames.Next()
		if !strings.Contains(frame.Function, "rank2EvidenceConn") &&
			!strings.Contains(frame.Function, "emitDeadline") {
			return fmt.Sprintf("%s:%d", frame.Function, frame.Line)
		}
		if !more {
			return ""
		}
	}
}
