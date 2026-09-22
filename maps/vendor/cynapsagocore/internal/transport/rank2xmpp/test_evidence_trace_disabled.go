//go:build !cynapsa_test_evidence

package rank2xmpp

import "net"

func emitRank2Evidence(rank2EvidenceRecord, error) {}

func wrapRank2EvidenceConn(conn net.Conn, _ string) net.Conn { return conn }
