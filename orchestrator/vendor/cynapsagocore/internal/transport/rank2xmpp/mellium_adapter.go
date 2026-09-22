package rank2xmpp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/xml"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"mellium.im/sasl"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	streamManagementNamespace   = "urn:xmpp:sm:3"
	custodyNamespace            = "urn:cynapsa:mesh-custody:1"
	xep0199PingNamespace        = "urn:xmpp:ping"
	resumeAuthorityIDPrefix     = "cynapsa-resume-authority-"
	maximumJingleIQErrorBytes   = 16 << 10
	maximumJingleIQErrorTokens  = 64
	maximumJingleIQErrorText    = 4 << 10
	melliumGracefulCloseTimeout = 250 * time.Millisecond
)

type custodyAcceptedXML struct {
	XMLName      xml.Name     `xml:"urn:cynapsa:mesh-custody:1 accepted"`
	MessageID    string       `xml:"message-id,attr"`
	Data         string       `xml:",chardata"`
	Unknown      []xmlUnknown `xml:",any"`
	UnknownAttrs []xml.Attr   `xml:",any,attr"`
}

type MelliumConfig struct {
	TLSConfig                    *tls.Config
	SASLMechanisms               []sasl.Mechanism
	ReceiveCapacity              int
	StreamManagementCapacity     int
	StreamManagementByteCapacity int64
	MaximumFrameBytes            int
	StanzaBudgetBytes            int
	CredentialUsableUntil        time.Time
	CredentialNow                func() time.Time
	MeshID                       string
}

// MelliumDialer creates isolated session state; it performs no network I/O
// until Client invokes the explicit TLS/authentication/bind phases.
type MelliumDialer struct {
	config   MelliumConfig
	endpoint Endpoint
}

func NewMelliumDialer(meshEndpoint string, config MelliumConfig) (*MelliumDialer, error) {
	endpoint, err := ParseEndpoint(meshEndpoint)
	if err != nil || len(config.SASLMechanisms) == 0 || config.ReceiveCapacity <= 0 || config.ReceiveCapacity > transport.MaximumReceiveQueue || config.StreamManagementCapacity <= 0 || config.StreamManagementCapacity > 65536 || config.StreamManagementByteCapacity <= 0 || config.StreamManagementByteCapacity > MaximumStreamManagementBytes || config.StreamManagementByteCapacity < int64(config.StanzaBudgetBytes) || config.MaximumFrameBytes <= 0 || config.MaximumFrameBytes > transport.MaximumControlFrameBytes || config.StanzaBudgetBytes <= 0 || config.StanzaBudgetBytes > 4<<20 || config.CredentialUsableUntil.IsZero() != (config.CredentialNow == nil) || !config.CredentialUsableUntil.IsZero() && (!config.CredentialUsableUntil.After(config.CredentialNow().UTC()) || config.CredentialUsableUntil.Location() != time.UTC) {
		return nil, ErrInvalidConfig
	}
	for _, mechanism := range config.SASLMechanisms {
		if mechanism.Name == "" {
			return nil, ErrInvalidConfig
		}
	}
	config.TLSConfig, err = tlsConfigForEndpoint(endpoint, config.TLSConfig)
	if err != nil {
		return nil, err
	}
	config.SASLMechanisms = append([]sasl.Mechanism(nil), config.SASLMechanisms...)
	return &MelliumDialer{config: config, endpoint: endpoint}, nil
}
func (d *MelliumDialer) Dial(ctx context.Context) (Session, error) {
	if d == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	return newMelliumSession(d.config, d.endpoint), nil
}

type melliumSession struct {
	config                     MelliumConfig
	endpoint                   Endpoint
	mu                         sync.Mutex
	authorityIngress           sync.Mutex
	conn                       net.Conn
	username                   string
	password                   []byte
	meshID                     string
	resource                   string
	session                    *xmpp.Session
	management                 *StreamManagement
	events                     chan Event
	controlEvents              chan Event
	controlBudget              *transport.InboundBudget
	serveDone                  chan serveResult
	generation                 uint64
	ctx                        context.Context
	cancel                     context.CancelFunc
	closed                     bool
	suspended                  bool
	rejected                   []Stanza
	resumeReplay               []Stanza
	resumeCorrelated           map[string]Stanza
	resumeCorrelatedGeneration uint64
	resumeGeneration           uint64
	resumeBarrier              *resumeAuthorityBarrier
	resumeBarrierGeneration    uint64
	resumeMarkerSeen           bool
	resumeResultQueued         bool
	writeGate                  chan struct{}
	correlatedGate             chan struct{}
	authorityFence             func(uint64) bool
	controlLaneActive          bool
	inboundBudget              *transport.InboundBudget
	closing                    chan struct{}
	closeDone                  chan struct{}
	closeErr                   error
	serveWG                    sync.WaitGroup
	receiveWG                  sync.WaitGroup
	issuedJingles              map[string]issuedJingle
	issuedJingleCapacity       int
	issuedJingleActive         int
	issuedJingleTombstones     []string
	// afterResumeAccepted is an unexported synchronization seam used by the
	// wire-level regression suite to place cancellation on the exact boundary
	// between an accepted <resumed/> and local post-commit publication. Runtime
	// sessions leave it nil.
	afterResumeAccepted func()
}

func newMelliumSession(config MelliumConfig, endpoint Endpoint) *melliumSession {
	writeGate := make(chan struct{}, 1)
	writeGate <- struct{}{}
	correlatedGate := make(chan struct{}, 1)
	correlatedGate <- struct{}{}
	capacity := config.ReceiveCapacity
	if capacity <= 0 {
		capacity = 1
	}
	budget, _ := transport.NewInboundBudget(capacity, transport.MaximumControlFrameBytes)
	controlBudget, _ := transport.NewInboundBudget(capacity, transport.MaximumControlFrameBytes)
	return &melliumSession{config: config, endpoint: endpoint, meshID: config.MeshID, events: make(chan Event, capacity), controlEvents: make(chan Event, capacity), controlBudget: controlBudget, serveDone: make(chan serveResult, 4), writeGate: writeGate, correlatedGate: correlatedGate, inboundBudget: budget, closing: make(chan struct{}), issuedJingles: make(map[string]issuedJingle), issuedJingleCapacity: config.StreamManagementCapacity}
}

func (s *melliumSession) bindInboundBudget(budget *transport.InboundBudget) error {
	if s == nil || budget == nil {
		return ErrInvalidConfig
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.conn != nil || s.session != nil {
		return ErrUnavailable
	}
	s.inboundBudget = budget
	return nil
}

type serveResult struct {
	generation uint64
	err        error
}

type melliumSessionGenerationKey struct{}

type bareServerResultKind uint8

const (
	bareServerResultUnowned bareServerResultKind = iota
	bareServerResultResumeAuthority
	bareServerResultEntityTime
	bareServerResultResumedExternalService
)

type bareServerResultOwnership struct {
	kind       bareServerResultKind
	generation uint64
	management *StreamManagement
	sequence   uint32
	record     Stanza
}

// setAuthorityIngressFence binds private authority control to the exact
// Client session owner. It is called before bounded event handoff or ack.
func (s *melliumSession) setAuthorityIngressFence(fence func(uint64) bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.authorityFence = fence
	s.controlLaneActive = fence != nil
	s.mu.Unlock()
}

func (s *melliumSession) fenceAuthorityIngress(ctx context.Context) bool {
	sessionGeneration, ok := ctx.Value(melliumSessionGenerationKey{}).(uint64)
	if !ok || sessionGeneration == 0 {
		return false
	}
	s.authorityIngress.Lock()
	defer s.authorityIngress.Unlock()
	s.mu.Lock()
	current, fence := s.generation, s.authorityFence
	s.mu.Unlock()
	return sessionGeneration == current && fence != nil && fence(sessionGeneration)
}

type wireStage uint8

const (
	wireNotStarted wireStage = iota
	wireAmbiguous
	wireComplete
)

type wireError struct {
	stage wireStage
	cause error
}

func (e *wireError) Error() string { return "durable transport: wire send failed" }
func (e *wireError) Unwrap() error { return e.cause }

type negotiationPhases struct {
	mu      sync.Mutex
	current establishmentPhase
}

func newNegotiationPhases() *negotiationPhases {
	return &negotiationPhases{current: establishmentNetworkTLS}
}

func (p *negotiationPhases) begin(phase establishmentPhase) {
	p.mu.Lock()
	p.current = phase
	p.mu.Unlock()
}

func (p *negotiationPhases) complete(phase establishmentPhase) {
	p.mu.Lock()
	switch phase {
	case establishmentNetworkTLS:
		p.current = establishmentAuthentication
	case establishmentAuthentication:
		p.current = establishmentIdentityBinding
	case establishmentIdentityBinding:
		p.current = establishmentStreamManagement
	default:
		p.current = phase
	}
	p.mu.Unlock()
}

func (p *negotiationPhases) failure(err error) error {
	p.mu.Lock()
	phase := p.current
	p.mu.Unlock()
	return phaseError(phaseForDependencyError(phase, err), err)
}

// trackMelliumFeature attaches a closed, typed phase to errors from Mellium's
// combined session negotiation. TLS is handshaken here so certificate and
// handshake failures cannot surface later while the SASL feature is starting.
func trackMelliumFeature(feature xmpp.StreamFeature, phase establishmentPhase, phases *negotiationPhases) xmpp.StreamFeature {
	parse := feature.Parse
	if parse != nil {
		feature.Parse = func(ctx context.Context, decoder *xml.Decoder, start *xml.StartElement) (bool, interface{}, error) {
			phases.begin(phase)
			required, data, err := parse(ctx, decoder, start)
			if err != nil {
				return required, data, phaseError(phaseForDependencyError(phase, err), err)
			}
			return required, data, nil
		}
	}
	negotiate := feature.Negotiate
	if negotiate != nil {
		feature.Negotiate = func(ctx context.Context, session *xmpp.Session, data interface{}) (xmpp.SessionState, io.ReadWriter, error) {
			phases.begin(phase)
			mask, rw, err := negotiate(ctx, session, data)
			if err != nil {
				return mask, rw, phaseError(phaseForDependencyError(phase, err), err)
			}
			if phase == establishmentNetworkTLS {
				if tlsConn, ok := rw.(interface{ HandshakeContext(context.Context) error }); ok {
					if err = tlsConn.HandshakeContext(ctx); err != nil {
						return mask, nil, phaseError(establishmentNetworkTLS, err)
					}
				}
			}
			if phase == establishmentAuthentication {
				resetXMLTokenBounds(session.Conn())
			}
			phases.complete(phase)
			return mask, rw, nil
		}
	}
	return feature
}

func (s *melliumSession) acquireWrite(ctx context.Context) error {
	if s == nil || ctx == nil {
		return ErrInvalidConfig
	}
	s.mu.Lock()
	if s.writeGate == nil {
		s.writeGate = make(chan struct{}, 1)
		s.writeGate <- struct{}{}
	}
	gate := s.writeGate
	s.mu.Unlock()
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *melliumSession) releaseWrite() {
	s.mu.Lock()
	gate := s.writeGate
	s.mu.Unlock()
	select {
	case gate <- struct{}{}:
	default:
		panic("rank2xmpp: write gate released without ownership")
	}
}

func (s *melliumSession) acquireCorrelated(ctx context.Context) error {
	if s == nil || ctx == nil {
		return ErrInvalidConfig
	}
	s.mu.Lock()
	if s.correlatedGate == nil {
		s.correlatedGate = make(chan struct{}, 1)
		s.correlatedGate <- struct{}{}
	}
	gate := s.correlatedGate
	s.mu.Unlock()
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *melliumSession) releaseCorrelated() {
	s.mu.Lock()
	gate := s.correlatedGate
	s.mu.Unlock()
	select {
	case gate <- struct{}{}:
	default:
		panic("rank2xmpp: correlated gate released without ownership")
	}
}

// terminalGateReader releases the write gate as soon as Mellium has consumed
// the complete IQ payload. Mellium still owns its own output lock while it
// writes the closing tag and awaits the correlated result.
type terminalGateReader struct {
	source  xml.TokenReader
	release func()
	once    sync.Once
}

func (r *terminalGateReader) Token() (xml.Token, error) {
	token, err := r.source.Token()
	if err != nil {
		r.done()
	}
	return token, err
}

func (r *terminalGateReader) done() {
	if r != nil {
		r.once.Do(r.release)
	}
}

func (s *melliumSession) sendTrackedIQElement(ctx context.Context, session *xmpp.Session, management *StreamManagement, record Stanza, payload xml.TokenReader, iq stanza.IQ) (xmlstream.TokenReadCloser, uint32, error) {
	if s == nil || ctx == nil || session == nil || management == nil || payload == nil {
		return nil, 0, ErrInvalidConfig
	}
	if err := s.acquireWrite(ctx); err != nil {
		return nil, 0, err
	}
	reader := &terminalGateReader{source: payload, release: s.releaseWrite}
	defer reader.done()
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	s.mu.Lock()
	current := !s.closed && !s.suspended && s.session == session && s.management == management && s.ctx != nil
	lifetime := s.ctx
	s.mu.Unlock()
	if !current {
		return nil, 0, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if err := lifetime.Err(); err != nil {
		return nil, 0, ErrUnavailable
	}
	sequence, err := management.RecordSentTracked(record)
	if err != nil {
		return nil, 0, err
	}
	operation, cancel := context.WithCancel(ctx)
	stopLifetime := context.AfterFunc(lifetime, cancel)
	defer func() {
		stopLifetime()
		cancel()
	}()
	response, err := session.SendIQElement(operation, reader, iq)
	if err != nil {
		if callerErr := ctx.Err(); callerErr != nil {
			return nil, sequence, callerErr
		}
		if lifetime.Err() != nil {
			return nil, sequence, ErrUnavailable
		}
	}
	return response, sequence, err
}

func (s *melliumSession) ConnectTLS(ctx context.Context, endpoint string) error {
	parsed, err := ParseEndpoint(endpoint)
	if s == nil || ctx == nil || err != nil || parsed != s.endpoint {
		return ErrInvalidConfig
	}
	s.mu.Lock()
	if s.closed || s.conn != nil {
		s.mu.Unlock()
		return ErrUnavailable
	}
	s.mu.Unlock()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", parsed.DialAddress())
	if err != nil {
		return err
	}
	conn = wrapRank2EvidenceConn(conn, "initial")
	if err = ctx.Err(); err != nil {
		_ = conn.Close()
		return err
	}
	s.mu.Lock()
	if s.closed || ctx.Err() != nil {
		s.mu.Unlock()
		_ = conn.Close()
		if err = ctx.Err(); err != nil {
			return err
		}
		return ErrClosed
	}
	s.conn = conn
	s.mu.Unlock()
	return nil
}

func (s *melliumSession) Authenticate(ctx context.Context, username string, password []byte) (string, []byte, error) {
	if s == nil || ctx == nil || len(password) == 0 {
		return "", nil, ErrAuthentication
	}
	parsed, err := jid.Parse(username)
	if err != nil || parsed.Localpart() == "" || parsed.Domainpart() == "" || parsed.Resourcepart() != "" {
		return "", nil, ErrAuthentication
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if s.closed || s.conn == nil {
		return "", nil, ErrUnavailable
	}
	s.username = parsed.String()
	clear(s.password)
	s.password = append(s.password[:0], password...)
	return s.username, nil, nil
}

func (s *melliumSession) BindResource(ctx context.Context, resource string) (string, error) {
	if s == nil || ctx == nil || resource == "" {
		return "", ErrIdentityBinding
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s.closed || s.username == "" {
		return "", ErrUnavailable
	}
	s.resource = resource
	if s.meshID == "" {
		s.meshID = resource
	}
	return s.username + "/" + resource, nil
}

func (s *melliumSession) EnableStreamManagement(ctx context.Context, resumable bool) error {
	if s == nil || ctx == nil || !resumable || !s.credentialUsable() {
		return ErrStreamManagement
	}
	ctx, cancelCredential := s.credentialBoundContext(ctx)
	defer cancelCredential()
	s.mu.Lock()
	if s.closed || s.conn == nil || s.username == "" || s.meshID == "" {
		s.mu.Unlock()
		return ErrUnavailable
	}
	conn := s.conn
	username := s.username
	resource := s.resource
	if resource == "" {
		resource = s.meshID
	}
	password := append([]byte(nil), s.password...)
	s.mu.Unlock()
	defer clear(password)
	bare, err := jid.Parse(username)
	if err != nil {
		return ErrAuthentication
	}
	origin, err := bare.WithResource(resource)
	if err != nil {
		return ErrIdentityBinding
	}
	phases := newNegotiationPhases()
	features := []xmpp.StreamFeature{
		trackMelliumFeature(boundedStartTLS(s.config.TLSConfig.Clone(), s.config.StanzaBudgetBytes), establishmentNetworkTLS, phases),
		trackMelliumFeature(xmpp.SASL("", string(password), s.config.SASLMechanisms...), establishmentAuthentication, phases),
		trackMelliumFeature(exactResourceBinding(resource), establishmentIdentityBinding, phases),
	}
	releaseDeadline := bindConnectionContext(ctx, conn)
	xsession, err := xmpp.NewClientSession(ctx, origin, newXMLTokenBoundedConn(conn, s.config.StanzaBudgetBytes), features...)
	if err != nil {
		releaseDeadline()
		if contextErr := networkContextError(ctx, err); contextErr != nil {
			return contextErr
		}
		return phases.failure(err)
	}
	if xsession.LocalAddr().String() != origin.String() {
		releaseDeadline()
		closeUnpublishedXMPP(xsession, conn)
		if contextErr := networkContextError(ctx, ErrIdentityBinding); contextErr != nil {
			return contextErr
		}
		return phaseError(establishmentIdentityBinding, ErrIdentityBinding)
	}
	management, err := NewStreamManagement(s.config.StreamManagementCapacity, s.config.StreamManagementByteCapacity)
	if err != nil {
		releaseDeadline()
		closeUnpublishedXMPP(xsession, conn)
		if contextErr := networkContextError(ctx, err); contextErr != nil {
			return contextErr
		}
		return phaseError(establishmentStreamManagement, err)
	}
	resumeID, err := enableSM(ctx, xsession)
	if err != nil {
		releaseDeadline()
		closeUnpublishedXMPP(xsession, conn)
		if contextErr := networkContextError(ctx, err); contextErr != nil {
			return contextErr
		}
		return phaseError(establishmentStreamManagement, err)
	}
	if err = management.Enable(resumeID, true); err != nil {
		releaseDeadline()
		closeUnpublishedXMPP(xsession, conn)
		if contextErr := networkContextError(ctx, err); contextErr != nil {
			return contextErr
		}
		return phaseError(establishmentStreamManagement, err)
	}
	releaseDeadline()
	if err = ctx.Err(); err != nil {
		closeUnpublishedXMPP(xsession, conn)
		return err
	}
	owned, cancel := context.WithCancel(context.Background())
	s.authorityIngress.Lock()
	s.mu.Lock()
	if s.closed || s.conn != conn || ctx.Err() != nil {
		s.mu.Unlock()
		s.authorityIngress.Unlock()
		cancel()
		closeUnpublishedXMPP(xsession, conn)
		if err = ctx.Err(); err != nil {
			return err
		}
		return ErrClosed
	}
	s.session = xsession
	s.management = management
	s.ctx = owned
	s.cancel = cancel
	s.generation++
	generation := s.generation
	s.serveWG.Add(1)
	s.mu.Unlock()
	s.authorityIngress.Unlock()
	go s.serve(xsession, owned, generation)
	return nil
}

func enableSM(ctx context.Context, session *xmpp.Session) (string, error) {
	w := session.TokenWriter()
	start := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "enable"}, Attr: []xml.Attr{{Name: xml.Name{Local: "resume"}, Value: "true"}}}
	err := w.EncodeToken(start)
	if err == nil {
		err = w.EncodeToken(start.End())
	}
	if err == nil {
		err = w.Flush()
	}
	_ = w.Close()
	if err != nil {
		return "", err
	}
	r := session.TokenReader()
	defer r.Close()
	decoder := xml.NewTokenDecoder(r)
	token, err := decoder.Token()
	if err != nil {
		return "", err
	}
	enabled, ok := token.(xml.StartElement)
	if !ok || enabled.Name != (xml.Name{Space: streamManagementNamespace, Local: "enabled"}) {
		return "", ErrStreamManagement
	}
	id, err := decodeSMEnabled(r, enabled)
	if err != nil {
		return "", ErrStreamManagement
	}
	return id, nil
}

func (s *melliumSession) Send(ctx context.Context, record Stanza) error {
	if s == nil || ctx == nil {
		return ErrInvalidConfig
	}
	s.mu.Lock()
	session, management, closed := s.session, s.management, s.closed
	s.mu.Unlock()
	if closed || session == nil || management == nil {
		return ErrUnavailable
	}
	if err := s.acquireWrite(ctx); err != nil {
		return err
	}
	defer s.releaseWrite()
	s.mu.Lock()
	suspended := s.suspended
	s.mu.Unlock()
	if suspended {
		return ErrUnavailable
	}
	if err := management.RecordSent(record); err != nil {
		return err
	}
	return finishManagedSend(management, record, s.sendWireLockedTracked(ctx, session, record, true))
}

// interruptIO is a fail-stop operation, not Close. It makes a blocked Mellium
// writer or output-mutex waiter observable by closing the exact raw connection,
// while retaining the XEP-0198 ledger, credentials, and replay state needed by
// PrepareResume or clean-session reconciliation. sendSession joins the
// cancellation callback before releasing Client send ownership, preventing a
// late interrupt from reaching a resumed replacement connection.
func (s *melliumSession) interruptIO() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.suspended = true
	conn := s.conn
	generation := s.generation
	s.mu.Unlock()
	if conn == nil {
		return
	}
	emitRank2Evidence(rank2EvidenceRecord{
		Event:          "socket_interrupted",
		Source:         "mellium",
		Stage:          "interrupt_io",
		WireGeneration: generation,
		Current:        true,
	}, nil)
	_ = conn.SetWriteDeadline(time.Now())
	_ = conn.Close()
}

func finishManagedSend(management *StreamManagement, record Stanza, sendErr error) error {
	if sendErr == nil {
		return nil
	}
	var staged *wireError
	if errors.As(sendErr, &staged) && staged.stage == wireNotStarted {
		if management == nil || management.RollbackLast(record) != nil {
			return ErrStreamManagement
		}
	}
	return sendErr
}

func (s *melliumSession) PendingForReplay(ctx context.Context) ([]Stanza, error) {
	if s == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.writeGate == nil {
		s.writeGate = make(chan struct{}, 1)
		s.writeGate <- struct{}{}
	}
	gate := s.writeGate
	select {
	case <-gate:
	default:
		s.mu.Unlock()
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		s.mu.Lock()
	}
	defer func() {
		select {
		case gate <- struct{}{}:
		default:
			panic("rank2xmpp: write gate released without ownership")
		}
	}()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	management := s.management
	if management == nil {
		s.mu.Unlock()
		return nil, ErrUnavailable
	}
	// Close takes the same lock before closing management. Keeping it through
	// the snapshot makes admission and rejected-record detachment one atomic
	// lifecycle decision: either this call owns a valid snapshot, or Close won.
	pending := management.ResumeRejected()
	s.suspended = true
	rejected := s.rejected
	s.rejected = nil
	s.mu.Unlock()
	if err := validateReplayGraph(rejected, pending); err != nil {
		clearStanzas(rejected)
		clearStanzas(pending)
		return nil, err
	}
	rejected = applicationReplayValidated(rejected)
	pending = applicationReplayValidated(pending)
	return mergeReplay(rejected, pending)
}

func (s *melliumSession) sendWire(ctx context.Context, session *xmpp.Session, record Stanza) error {
	if err := s.acquireWrite(ctx); err != nil {
		return err
	}
	defer s.releaseWrite()
	return s.sendWireLocked(ctx, session, record)
}

func (s *melliumSession) sendWireLocked(ctx context.Context, session *xmpp.Session, record Stanza) error {
	return s.sendWireLockedTracked(ctx, session, record, false)
}

func (s *melliumSession) sendWireLockedTracked(ctx context.Context, session *xmpp.Session, record Stanza, tracked bool) error {
	if record.Kind == StanzaSignal {
		return s.sendJingle(ctx, session, record, tracked)
	}
	if record.Kind == StanzaSignalResult {
		return s.sendJingleResult(ctx, session, record)
	}
	payload, err := EncodeStanzaFrame(record, s.config.MaximumFrameBytes, s.config.StanzaBudgetBytes)
	if err != nil {
		return &wireError{stage: wireNotStarted, cause: err}
	}
	defer clear(payload)
	to, err := jid.Parse(record.To)
	if err != nil {
		return &wireError{stage: wireNotStarted, cause: ErrProtocol}
	}
	reader := xml.NewDecoder(bytes.NewReader(payload))
	message := stanza.Message{XMLName: xml.Name{Space: stanza.NSClient, Local: "message"}, To: to, Type: stanza.ChatMessage}
	if record.Kind == StanzaEnvelope {
		if !validReadinessTyped(record.MessageID, "msg_", 16) {
			return &wireError{stage: wireNotStarted, cause: ErrProtocol}
		}
		message.ID = record.MessageID
	}
	if err = session.SendElement(ctx, reader, message.StartElement()); err != nil {
		return &wireError{stage: wireAmbiguous, cause: err}
	}
	if err = sendSMRequest(ctx, session); err != nil {
		return &wireError{stage: wireComplete, cause: err}
	}
	return nil
}

func (s *melliumSession) sendJingleResult(ctx context.Context, session *xmpp.Session, record Stanza) error {
	to, err := jid.Parse(record.To)
	if err != nil {
		return &wireError{stage: wireNotStarted, cause: ErrProtocol}
	}
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: record.AttemptID, To: to, Type: stanza.ResultIQ}
	if err = session.SendElement(ctx, nilTokenReader{}, iq.StartElement()); err != nil {
		return &wireError{stage: wireAmbiguous, cause: err}
	}
	if err = sendSMRequest(ctx, session); err != nil {
		return &wireError{stage: wireComplete, cause: err}
	}
	return nil
}

func (s *melliumSession) sendJingle(ctx context.Context, session *xmpp.Session, record Stanza, tracked bool) error {
	signal, err := DecodeJingle(record.Data)
	if err != nil || signal.SID != record.AttemptID || !validJingleRoute(signal, record.From, record.To) {
		if err == nil {
			err = ErrProtocol
		}
		return &wireError{stage: wireNotStarted, cause: err}
	}
	to, err := jid.Parse(record.To)
	if err != nil {
		return &wireError{stage: wireNotStarted, cause: ErrProtocol}
	}
	payload, err := EncodeJingle(signal)
	if err != nil {
		return &wireError{stage: wireNotStarted, cause: err}
	}
	defer clear(payload)
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: record.AttemptID, To: to, Type: stanza.SetIQ}
	if err = s.registerIssuedJingle(record, tracked); err != nil {
		return &wireError{stage: wireNotStarted, cause: err}
	}
	defer s.quiesceIssuedJingle(record.AttemptID, record.From, record.To)
	if err = session.SendElement(ctx, xml.NewDecoder(bytes.NewReader(payload)), iq.StartElement()); err != nil {
		return &wireError{stage: wireAmbiguous, cause: err}
	}
	if err = sendSMRequest(ctx, session); err != nil {
		return &wireError{stage: wireComplete, cause: err}
	}
	return nil
}

func sendSMRequest(ctx context.Context, session *xmpp.Session) error {
	start := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "r"}}
	return session.SendElement(ctx, nilTokenReader{}, start)
}

func encodeSMRequest(writer xmlstream.TokenWriter) error {
	start := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "r"}}
	if err := writer.EncodeToken(start); err != nil {
		return err
	}
	return writer.EncodeToken(start.End())
}

type nilTokenReader struct{}

func (nilTokenReader) Token() (xml.Token, error) { return nil, io.EOF }

func (s *melliumSession) Receive(ctx context.Context) (Event, error) {
	if s == nil || ctx == nil {
		return Event{}, ErrInvalidConfig
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return Event{}, ErrClosed
	}
	if s.closing == nil {
		s.closing = make(chan struct{})
	}
	closing := s.closing
	s.receiveWG.Add(1)
	s.mu.Unlock()
	defer s.receiveWG.Done()
	for {
		select {
		case event := <-s.events:
			s.mu.Lock()
			current, closed := s.generation, s.closed
			barrier := s.resumeBarrier
			s.mu.Unlock()
			if closed {
				clearEventOwned(&event)
				return Event{}, ErrClosed
			}
			if event.sessionGeneration != 0 && event.sessionGeneration != current {
				clearEventOwned(&event)
				continue
			}
			// Server replay can place application frames on the peer queue before
			// the strict authority result arrives after the post-resume marker. Hold
			// the one dequeued frame until the independent control worker has
			// processed the replayed control prefix and closes the barrier.
			if barrier != nil {
				select {
				case <-barrier.done:
					if !barrier.ready.Load() {
						clearEventOwned(&event)
						return Event{}, ErrUnavailable
					}
				case <-ctx.Done():
					clearEventOwned(&event)
					return Event{}, ctx.Err()
				case <-closing:
					clearEventOwned(&event)
					return Event{}, ErrClosed
				}
			}
			return event, nil
		case result := <-s.serveDone:
			s.mu.Lock()
			current := s.generation
			s.mu.Unlock()
			if result.generation != current {
				continue
			}
			if result.err == nil {
				return Event{}, ErrClosed
			}
			return Event{}, result.err
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-closing:
			return Event{}, ErrClosed
		}
	}
}

// ReceiveControl drains only server/session control work. Application and
// peer-originated frames are physically unable to enter this queue.
func (s *melliumSession) ReceiveControl(ctx context.Context) (Event, error) {
	if s == nil || ctx == nil {
		return Event{}, ErrInvalidConfig
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return Event{}, ErrClosed
	}
	if s.closing == nil {
		s.closing = make(chan struct{})
	}
	if s.controlEvents == nil {
		capacity := s.config.ReceiveCapacity
		if capacity <= 0 {
			capacity = 1
		}
		s.controlEvents = make(chan Event, capacity)
	}
	closing := s.closing
	s.receiveWG.Add(1)
	s.mu.Unlock()
	defer s.receiveWG.Done()
	for {
		select {
		case event := <-s.controlEvents:
			s.mu.Lock()
			current, closed := s.generation, s.closed
			s.mu.Unlock()
			if closed {
				clearEventOwned(&event)
				return Event{}, ErrClosed
			}
			if !isControlEvent(event.Kind) {
				clearEventOwned(&event)
				_ = s.fenceAuthorityIngress(ctx)
				return Event{}, ErrProtocol
			}
			if event.sessionGeneration != 0 && event.sessionGeneration != current {
				clearEventOwned(&event)
				continue
			}
			return event, nil
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-closing:
			return Event{}, ErrClosed
		}
	}
}

func (s *melliumSession) Resume(ctx context.Context) (bool, error) {
	resumed, err := s.PrepareResume(ctx)
	if err != nil || !resumed {
		return resumed, err
	}
	if err = s.WaitResumeAuthorityResult(ctx); err != nil {
		return false, err
	}
	if err = s.ReplayPrepared(ctx, func(stanza Stanza) bool {
		return stanza.Kind != StanzaEnvelope
	}, func(stanza Stanza) (preparedReplayStanza, bool) {
		return preparedReplayStanza{Stanza: stanza}, true
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (s *melliumSession) PrepareResume(ctx context.Context) (bool, error) {
	if s == nil || ctx == nil || !s.credentialUsable() {
		return false, ErrInvalidConfig
	}
	ctx, cancelCredential := s.credentialBoundContext(ctx)
	defer cancelCredential()
	// Preserve the caller's original operation budget, not its remaining time.
	// Once the server accepts <resume/>, that old deadline no longer owns the
	// replacement stream: the committed session gets one fresh phase with the
	// same bound for local SM publication and all later post-commit work.
	postCommitBudget, postCommitBounded := time.Duration(0), false
	if deadline, ok := ctx.Deadline(); ok {
		postCommitBudget = time.Until(deadline)
		postCommitBounded = true
		if postCommitBudget <= 0 {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return false, context.DeadlineExceeded
		}
	}
	if err := s.acquireWrite(ctx); err != nil {
		return false, err
	}
	defer s.releaseWrite()
	s.mu.Lock()
	if s.closed || s.endpoint.DialAddress() == "" || s.username == "" || s.meshID == "" || s.management == nil {
		s.mu.Unlock()
		return false, ErrUnavailable
	}
	endpoint, username, resource, management := s.endpoint, s.username, s.resource, s.management
	if resource == "" {
		resource = s.meshID
	}
	oldGeneration := s.generation
	afterResumeAccepted := s.afterResumeAccepted
	password := append([]byte(nil), s.password...)
	oldCancel := s.cancel
	oldConn := s.conn
	// Linearize recovery before ResumeState is read during negotiation. A
	// handler that already decoded an old-socket result must commit its SM
	// accounting under s.mu before this edge; every later old-generation result
	// is rejected and left for the server's resumed replay.
	s.suspended = true
	clearStanzas(s.resumeReplay)
	s.resumeReplay = nil
	s.clearResumeCorrelatedLocked()
	s.resumeGeneration = 0
	s.resumeBarrier = nil
	s.resumeBarrierGeneration = 0
	s.resumeMarkerSeen = false
	s.resumeResultQueued = false
	s.mu.Unlock()
	defer clear(password)
	emitRank2Evidence(rank2EvidenceRecord{
		Event:          "resume_attempt_started",
		Source:         "mellium",
		Stage:          "dial",
		WireGeneration: oldGeneration,
		Current:        true,
	}, nil)

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", endpoint.DialAddress())
	if err != nil {
		emitRank2Evidence(rank2EvidenceRecord{
			Event:          "resume_attempt_failed",
			Source:         "mellium",
			Stage:          "dial",
			WireGeneration: oldGeneration,
		}, err)
		return false, err
	}
	conn = wrapRank2EvidenceConn(conn, "resume")
	bare, err := jid.Parse(username)
	if err != nil {
		_ = conn.Close()
		return false, ErrAuthentication
	}
	origin, err := bare.WithResource(resource)
	if err != nil {
		_ = conn.Close()
		return false, ErrIdentityBinding
	}
	result := &resumeNegotiation{}
	defer clearStanzas(result.replay)
	features := []xmpp.StreamFeature{
		boundedStartTLS(s.config.TLSConfig.Clone(), s.config.StanzaBudgetBytes),
		resetXMLStreamFeature(xmpp.SASL("", string(password), s.config.SASLMechanisms...)),
		resumeFeature(management, origin, result),
	}
	releaseDeadline := bindConnectionContext(ctx, conn)
	newSession, err := xmpp.NewClientSession(ctx, origin, newXMLTokenBoundedConn(conn, s.config.StanzaBudgetBytes), features...)
	releaseDeadline()
	if err != nil {
		negotiationErr := err
		emitRank2Evidence(rank2EvidenceRecord{
			Event:          "resume_negotiation_failed",
			Source:         "mellium",
			Stage:          "new_client_session",
			WireGeneration: oldGeneration,
			Resumed:        result.resumed,
		}, negotiationErr)
		_ = conn.Close()
		if result.replayErr != nil {
			return false, result.replayErr
		}
		if len(result.replay) > 0 {
			s.mu.Lock()
			var mergeErr error
			s.rejected, mergeErr = mergeReplay(s.rejected, result.replay)
			s.mu.Unlock()
			if mergeErr != nil {
				return false, mergeErr
			}
		}
		if contextErr := networkContextError(ctx, negotiationErr); contextErr != nil {
			return false, contextErr
		}
		return false, nil
	}
	if !result.resumed {
		emitRank2Evidence(rank2EvidenceRecord{
			Event:          "resume_rejected",
			Source:         "mellium",
			Stage:          "new_client_session",
			WireGeneration: oldGeneration,
		}, nil)
		_ = newSession.Close()
		return false, nil
	}
	if afterResumeAccepted != nil {
		afterResumeAccepted()
	}
	owned, cancel := context.WithCancel(context.Background())
	s.authorityIngress.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.authorityIngress.Unlock()
		cancel()
		closeUnpublishedXMPP(newSession, conn)
		return false, ErrClosed
	}
	s.conn, s.session, s.ctx, s.cancel = conn, newSession, owned, cancel
	s.generation++
	generation := s.generation
	s.suspended = false
	s.resumeReplay = result.replay
	result.replay = nil
	s.resumeCorrelated = make(map[string]Stanza)
	for index := range s.resumeReplay {
		if s.resumeReplay[index].Kind == StanzaExternalServiceQuery {
			s.resumeCorrelated[s.resumeReplay[index].MessageID] = s.resumeReplay[index].clone()
		}
	}
	s.resumeCorrelatedGeneration = generation
	s.resumeGeneration = generation
	s.resumeBarrier = &resumeAuthorityBarrier{done: make(chan struct{})}
	s.resumeBarrierGeneration = generation
	s.resumeMarkerSeen = false
	s.resumeResultQueued = false
	s.mu.Unlock()
	s.authorityIngress.Unlock()
	emitRank2Evidence(rank2EvidenceRecord{
		Event:          "resume_accepted",
		Source:         "mellium",
		Stage:          "published",
		WireGeneration: generation,
		Current:        true,
		Resumed:        true,
	}, nil)
	if oldCancel != nil {
		oldCancel()
	}
	if oldConn != nil {
		_ = oldConn.Close()
	}
	// The server has committed the logical stream. Publish its handled
	// watermark through a fresh, session-owned context before the parser can
	// publish any resumed-stream control result. This both prevents an expired
	// dial/reconnect context from rejecting a good resume and preserves the
	// exact EventHandled -> authority-barrier ordering.
	postCommit, postCommitCancel := freshResumePostCommitContext(owned, generation, postCommitBudget, postCommitBounded)
	if result.handledCount > 0 {
		err = s.emit(postCommit, Event{Kind: EventHandled, HandledThrough: result.handledOrdinal, HandledCount: result.handledCount})
	}
	postCommitCancel()
	if err != nil {
		emitRank2Evidence(rank2EvidenceRecord{
			Event:          "resume_post_commit_failed",
			Source:         "mellium",
			Stage:          "handled_watermark",
			WireGeneration: generation,
			Resumed:        true,
		}, err)
		if s.retireUnservedResumeCandidate(newSession, conn, generation) {
			cancel()
			closeUnpublishedXMPP(newSession, conn)
		}
		return false, err
	}
	s.authorityIngress.Lock()
	s.mu.Lock()
	if s.closed || s.session != newSession || s.generation != generation {
		s.mu.Unlock()
		s.authorityIngress.Unlock()
		return false, ErrClosed
	}
	s.serveWG.Add(1)
	s.mu.Unlock()
	s.authorityIngress.Unlock()
	go s.serve(newSession, owned, generation)
	return true, nil
}

func (s *melliumSession) credentialUsable() bool {
	if s == nil || s.config.CredentialUsableUntil.IsZero() {
		return s != nil
	}
	return s.config.CredentialNow != nil && s.config.CredentialNow().UTC().Before(s.config.CredentialUsableUntil)
}

func (s *melliumSession) boundResource() string {
	if s == nil {
		return ""
	}
	if s.resource != "" {
		return s.resource
	}
	return s.meshID
}

func (s *melliumSession) acceptPeerResource(resource string) bool {
	// Resource text is only an exact-session address. The authenticated
	// authority snapshot and envelope mesh field establish peer scope, so a
	// legacy local resource must be able to communicate with r2 peers during
	// migration.
	return s != nil && resource != ""
}

func (s *melliumSession) credentialBoundContext(parent context.Context) (context.Context, context.CancelFunc) {
	if s == nil || s.config.CredentialUsableUntil.IsZero() {
		return context.WithCancel(parent)
	}
	return context.WithDeadline(parent, s.config.CredentialUsableUntil)
}

// retireUnservedResumeCandidate transfers cleanup ownership only when the
// accepted candidate is still the exact published generation and its parser
// has not started. Close or a replacement that already acquired
// authorityIngress owns its own cancellation and socket close; this path must
// not race either of them with a second Mellium close.
func (s *melliumSession) retireUnservedResumeCandidate(candidate *xmpp.Session, conn net.Conn, generation uint64) bool {
	if s == nil || candidate == nil || conn == nil || generation == 0 {
		return false
	}
	s.authorityIngress.Lock()
	defer s.authorityIngress.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.session != candidate || s.conn != conn || s.generation != generation {
		return false
	}
	s.session = nil
	s.conn = nil
	s.ctx = nil
	s.cancel = nil
	s.generation++
	s.suspended = true
	clearStanzas(s.resumeReplay)
	s.resumeReplay = nil
	s.clearResumeCorrelatedLocked()
	s.resumeGeneration = 0
	s.resumeBarrier = nil
	s.resumeBarrierGeneration = 0
	s.resumeMarkerSeen = false
	s.resumeResultQueued = false
	return true
}

func freshResumePostCommitContext(lifetime context.Context, generation uint64, budget time.Duration, bounded bool) (context.Context, context.CancelFunc) {
	base := lifetime
	cancel := func() {}
	if bounded {
		base, cancel = context.WithTimeout(lifetime, budget)
	}
	return context.WithValue(base, melliumSessionGenerationKey{}, generation), cancel
}

func (s *melliumSession) WaitResumeAuthorityResult(ctx context.Context) error {
	if s == nil || ctx == nil {
		return ErrInvalidConfig
	}
	s.mu.Lock()
	barrier := s.resumeBarrier
	generation := s.resumeBarrierGeneration
	current := !s.closed && generation != 0 && generation == s.generation
	s.mu.Unlock()
	if !current || barrier == nil {
		return ErrUnavailable
	}
	for {
		select {
		case <-barrier.done:
			s.mu.Lock()
			current = s.resumeBarrier == barrier && s.resumeBarrierGeneration == generation && s.generation == generation
			ready := current && barrier.ready.Load()
			rejected := current && barrier.rejected.Load()
			if current {
				s.resumeBarrier = nil
				s.resumeBarrierGeneration = 0
				s.resumeMarkerSeen = false
				s.resumeResultQueued = false
			}
			s.mu.Unlock()
			if rejected {
				return ErrAuthorityRejected
			}
			if !ready {
				return ErrUnavailable
			}
			return nil
		case result := <-s.serveDone:
			if result.generation != generation {
				continue
			}
			if barrier.rejected.Load() {
				return ErrAuthorityRejected
			}
			if result.err == nil {
				return ErrUnavailable
			}
			return result.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// bindConnectionContext makes cancellation observable to network operations
// whose dependency only watches the socket. Cancellation closes the unfinished
// candidate rather than relying on an advisory deadline. The returned release
// function stops or joins the callback, so a stale timeout can never close a
// published live stream.
func bindConnectionContext(ctx context.Context, conn net.Conn) func() {
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = conn.Close()
		close(callbackDone)
	})
	return func() {
		if !stop() {
			<-callbackDone
		}
	}
}

func closeUnpublishedXMPP(session *xmpp.Session, conn net.Conn) {
	if conn != nil {
		_ = conn.Close()
	}
	if session != nil {
		_ = session.Close()
	}
}

// AbortSetup retires only an unpublished candidate. The raw socket is closed
// first so protocol cleanup cannot remain blocked on a half-open peer.
func (s *melliumSession) AbortSetup(context.Context) error {
	if s == nil {
		return ErrInvalidConfig
	}
	s.authorityIngress.Lock()
	s.mu.Lock()
	s.closed = true
	if s.closing == nil {
		s.closing = make(chan struct{})
	}
	select {
	case <-s.closing:
	default:
		close(s.closing)
	}
	s.closeDone = make(chan struct{})
	close(s.closeDone)
	cancel, session, conn, management := s.cancel, s.session, s.conn, s.management
	s.cancel = nil
	s.ctx = nil
	s.session = nil
	s.conn = nil
	s.management = nil
	s.generation = 0
	s.suspended = false
	clearStanzas(s.rejected)
	s.rejected = nil
	clearStanzas(s.resumeReplay)
	s.resumeReplay = nil
	s.clearResumeCorrelatedLocked()
	s.resumeGeneration = 0
	s.resumeBarrier = nil
	s.resumeBarrierGeneration = 0
	s.resumeMarkerSeen = false
	s.resumeResultQueued = false
	clear(s.password)
	s.password = nil
	s.username = ""
	s.meshID = ""
	s.resource = ""
	s.mu.Unlock()
	s.authorityIngress.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
	if management != nil {
		management.Close()
	}
	if session != nil {
		_ = session.Close()
	}
	return nil
}

func networkContextError(ctx context.Context, err error) error {
	if contextErr := exactContextError(ctx); contextErr != nil {
		return contextErr
	}
	deadline, hasDeadline := ctx.Deadline()
	var networkError net.Error
	if hasDeadline && !time.Now().Before(deadline) && errors.As(err, &networkError) && networkError.Timeout() {
		return context.DeadlineExceeded
	}
	return nil
}

func (s *melliumSession) ReplayPrepared(ctx context.Context, allow func(Stanza) bool, prepare func(Stanza) (preparedReplayStanza, bool)) error {
	if s == nil || ctx == nil || allow == nil || prepare == nil {
		return ErrInvalidConfig
	}
	if err := s.acquireWrite(ctx); err != nil {
		return err
	}
	defer s.releaseWrite()
	s.mu.Lock()
	if s.closed || s.session == nil || s.ctx == nil || s.management == nil || s.resumeGeneration == 0 || s.resumeGeneration != s.generation {
		s.mu.Unlock()
		return ErrUnavailable
	}
	session, generation, management := s.session, s.generation, s.management
	local, server, meshID := s.username+"/"+s.boundResource(), "", s.meshID
	if bound, err := jid.Parse(s.username); err == nil {
		server = bound.Domainpart()
	}
	replay := s.resumeReplay
	s.resumeReplay = nil
	s.mu.Unlock()
	defer func() { clearStanzas(replay) }()
	restore := func() {
		s.mu.Lock()
		if !s.closed && s.session == session && s.generation == generation && s.resumeGeneration == generation && len(s.resumeReplay) == 0 {
			s.resumeReplay = replay
			replay = nil
		}
		s.mu.Unlock()
	}
	pingIDs := make(map[string]struct{})
	externalIDs := make(map[string]struct{})
	for _, record := range replay {
		if !allow(record) ||
			isSessionControlStanza(record.Kind) &&
				record.Kind != StanzaSessionPingResult && record.Kind != StanzaExternalServiceQuery ||
			record.Kind == StanzaSessionPingResult && !validSessionPingResultReplay(record, local, server, meshID) ||
			record.Kind == StanzaExternalServiceQuery && !validExternalServiceQueryReplay(record, local, server, meshID) {
			restore()
			return ErrStreamManagement
		}
		if record.Kind == StanzaSessionPingResult {
			if _, duplicate := pingIDs[record.AttemptID]; duplicate {
				restore()
				return ErrStreamManagement
			}
			pingIDs[record.AttemptID] = struct{}{}
		}
		if record.Kind == StanzaExternalServiceQuery {
			if _, duplicate := externalIDs[record.MessageID]; duplicate {
				restore()
				return ErrStreamManagement
			}
			externalIDs[record.MessageID] = struct{}{}
		}
	}
	for _, record := range replay {
		if !management.pendingContains(record) {
			continue
		}
		if record.Kind == StanzaSessionPingResult {
			if err := s.replaySessionPingResultLocked(ctx, session, record); err != nil {
				restore()
				return err
			}
			continue
		}
		if record.Kind == StanzaExternalServiceQuery {
			if err := s.replayExternalServiceQueryLocked(ctx, session, record); err != nil {
				restore()
				return err
			}
			continue
		}
		prepared, ok := prepare(record)
		if !ok {
			restore()
			return ErrStreamManagement
		}
		if err := s.sendWireLocked(ctx, session, prepared.Stanza); err != nil {
			if prepared.Release != nil {
				prepared.Release()
			}
			restore()
			return err
		}
		if prepared.Release != nil {
			prepared.Release()
		}
	}
	s.mu.Lock()
	if s.closed || s.session != session || s.generation != generation || s.resumeGeneration != generation {
		s.mu.Unlock()
		return ErrUnavailable
	}
	s.resumeGeneration = 0
	s.mu.Unlock()
	return nil
}

func (s *melliumSession) replayExternalServiceQueryLocked(ctx context.Context, session *xmpp.Session, record Stanza) error {
	if !validExternalServiceQueryReplay(record, record.From, record.To, record.MeshID) {
		return &wireError{stage: wireNotStarted, cause: ErrProtocol}
	}
	to, err := jid.Parse(record.To)
	if err != nil {
		return &wireError{stage: wireNotStarted, cause: ErrProtocol}
	}
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: record.MessageID, To: to, Type: stanza.GetIQ}
	if err = session.SendElement(ctx, xml.NewDecoder(bytes.NewReader(record.Data)), iq.StartElement()); err != nil {
		return &wireError{stage: wireAmbiguous, cause: err}
	}
	if err = sendSMRequest(ctx, session); err != nil {
		return &wireError{stage: wireComplete, cause: err}
	}
	return nil
}

func validSessionPingResultReplay(record Stanza, local, server, meshID string) bool {
	if record.Kind != StanzaSessionPingResult || local == "" || server == "" || meshID == "" ||
		record.From != local || record.To != server || record.MeshID != meshID || record.AttemptID == "" ||
		record.Ordinal != 0 || record.TransferID != "" || record.MessageID != "" || len(record.Data) != 0 ||
		record.Evidence.TransferID != "" || record.Evidence.MessageID != "" || record.Evidence.Digest != [32]byte{} {
		return false
	}
	from, fromErr := jid.Parse(record.From)
	to, toErr := jid.Parse(record.To)
	return fromErr == nil && toErr == nil && from.Resourcepart() != "" &&
		to.Localpart() == "" && to.Resourcepart() == "" && from.Domainpart() == to.Domainpart()
}

func (s *melliumSession) replaySessionPingResultLocked(ctx context.Context, session *xmpp.Session, record Stanza) error {
	from, err := jid.Parse(record.From)
	if err != nil {
		return &wireError{stage: wireNotStarted, cause: ErrProtocol}
	}
	to, err := jid.Parse(record.To)
	if err != nil {
		return &wireError{stage: wireNotStarted, cause: ErrProtocol}
	}
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: record.AttemptID, From: from, To: to, Type: stanza.ResultIQ}
	if err = session.SendElement(ctx, nilTokenReader{}, iq.StartElement()); err != nil {
		return &wireError{stage: wireAmbiguous, cause: err}
	}
	if err = sendSMRequest(ctx, session); err != nil {
		return &wireError{stage: wireComplete, cause: err}
	}
	return nil
}

func (s *melliumSession) markResumedActive() { s.mu.Lock(); s.suspended = false; s.mu.Unlock() }

func (s *melliumSession) clearResumeCorrelatedLocked() {
	for id, record := range s.resumeCorrelated {
		clearStanzaOwned(&record)
		delete(s.resumeCorrelated, id)
	}
	s.resumeCorrelated = nil
	s.resumeCorrelatedGeneration = 0
}

type resumeNegotiation struct {
	resumed        bool
	replay         []Stanza
	replayErr      error
	handledOrdinal uint64
	handledCount   uint32
}

func resumeFeature(management *StreamManagement, origin jid.JID, result *resumeNegotiation) xmpp.StreamFeature {
	return xmpp.StreamFeature{
		Name:       xml.Name{Space: streamManagementNamespace, Local: "sm"},
		Necessary:  xmpp.Authn,
		Prohibited: xmpp.Ready,
		Parse: func(_ context.Context, decoder *xml.Decoder, start *xml.StartElement) (bool, interface{}, error) {
			var value struct {
				XMLName xml.Name `xml:"urn:xmpp:sm:3 sm"`
			}
			return false, nil, decoder.DecodeElement(&value, start)
		},
		Negotiate: func(_ context.Context, session *xmpp.Session, _ interface{}) (xmpp.SessionState, io.ReadWriter, error) {
			id, handled, ok := management.ResumeState()
			if !ok {
				return 0, nil, ErrStreamManagement
			}
			writer := session.TokenWriter()
			start := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "resume"}, Attr: []xml.Attr{{Name: xml.Name{Local: "previd"}, Value: id}, {Name: xml.Name{Local: "h"}, Value: strconv.FormatUint(uint64(handled), 10)}}}
			err := writer.EncodeToken(start)
			if err == nil {
				err = writer.EncodeToken(start.End())
			}
			if err == nil {
				err = writer.Flush()
			}
			_ = writer.Close()
			if err != nil {
				return 0, nil, err
			}
			reader := session.TokenReader()
			defer reader.Close()
			decoder := xml.NewTokenDecoder(reader)
			token, err := decoder.Token()
			if err != nil {
				return 0, nil, err
			}
			response, ok := token.(xml.StartElement)
			if !ok || response.Name.Space != streamManagementNamespace || response.Name.Local != "resumed" {
				result.replay = management.ResumeRejected()
				return 0, nil, ErrStreamManagement
			}
			serverHandled, err := decodeSMResumed(reader, response, id)
			if err != nil {
				return 0, nil, ErrStreamManagement
			}
			// A correlated session request whose sequence is not covered by
			// the server's resumed h is ambiguous. Those requests fence this
			// stream. The one exception is an exact XEP-0199 IQ result: it has
			// no request-side state and is replayed byte-semantically with the
			// retained ID and route on this continued stream.
			// Decide before applying h so even duplicate-safe application
			// records preceding the fence retain their authoritative order.
			fenced, err := management.pendingControlAfterAck(serverHandled, origin.String(), origin.Domainpart(), origin.Resourcepart())
			if err != nil {
				return 0, nil, err
			}
			if fenced {
				result.replay, result.replayErr = applicationReplay(management.ResumeRejected())
				if result.replayErr != nil {
					return 0, nil, result.replayErr
				}
				return 0, nil, ErrStreamManagement
			}
			ordinal, count, err := management.ApplyAckDetailed(serverHandled)
			if err != nil {
				return 0, nil, err
			}
			replay := management.PendingSnapshot()
			result.resumed, result.replay, result.handledOrdinal, result.handledCount = true, replay, ordinal, count
			session.UpdateAddr(origin)
			return xmpp.Ready, nil, nil
		},
	}
}
func (s *melliumSession) CatchUp(ctx context.Context, limit int) ([]Stanza, error) {
	if s == nil || ctx == nil || limit <= 0 {
		return nil, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *melliumSession) Close(ctx context.Context) error {
	if s == nil || ctx == nil {
		return ErrInvalidConfig
	}
	var closeDone chan struct{}
	s.authorityIngress.Lock()
	s.mu.Lock()
	if s.closed {
		closeDone = s.closeDone
		s.mu.Unlock()
		s.authorityIngress.Unlock()
		if closeDone == nil {
			return ErrClosed
		}
		select {
		case <-closeDone:
			s.mu.Lock()
			result := s.closeErr
			s.mu.Unlock()
			return result
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.closed = true
	s.generation++
	closingGeneration := s.generation
	s.clearIssuedJinglesLocked()
	s.closeDone = make(chan struct{})
	closeDone = s.closeDone
	if s.closing == nil {
		s.closing = make(chan struct{})
	}
	close(s.closing)
	cancel, session, conn, management := s.cancel, s.session, s.conn, s.management
	if management != nil {
		management.Close()
	}
	s.management = nil
	s.mu.Unlock()
	s.authorityIngress.Unlock()
	emitRank2Evidence(rank2EvidenceRecord{
		Event:          "socket_close_requested",
		Source:         "mellium",
		Stage:          "session_close",
		WireGeneration: closingGeneration,
	}, nil)
	if cancel != nil {
		cancel()
	}
	go s.finishClose(session, conn)
	select {
	case <-closeDone:
		s.mu.Lock()
		result := s.closeErr
		s.mu.Unlock()
		return result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *melliumSession) finishClose(session *xmpp.Session, conn net.Conn) {
	result := closeMelliumStream(session, conn, melliumGracefulCloseTimeout)
	s.serveWG.Wait()
	s.receiveWG.Wait()
	for {
		select {
		case event := <-s.events:
			clearEventOwned(&event)
		default:
			goto controlEvents
		}
	}

controlEvents:
	for {
		select {
		case event := <-s.controlEvents:
			clearEventOwned(&event)
		default:
			goto serveResults
		}
	}

serveResults:
	for {
		select {
		case <-s.serveDone:
		default:
			s.mu.Lock()
			clearStanzas(s.resumeReplay)
			s.resumeReplay = nil
			s.clearResumeCorrelatedLocked()
			s.resumeGeneration = 0
			s.resumeBarrier = nil
			s.resumeBarrierGeneration = 0
			s.resumeMarkerSeen = false
			s.resumeResultQueued = false
			clearStanzas(s.rejected)
			s.rejected = nil
			clear(s.password)
			s.password = nil
			s.closeErr = result
			done := s.closeDone
			s.mu.Unlock()
			close(done)
			return
		}
	}
}

func closeMelliumStream(session *xmpp.Session, conn net.Conn, gracefulTimeout time.Duration) error {
	// Published Mellium ownership always installs session and raw connection
	// together. Retain the exact session connection as a defensive fallback so
	// an inconsistent terminal snapshot still has a socket to unblock its
	// bounded closing write.
	if session != nil && conn == nil {
		conn = session.Conn()
	}
	var (
		result     error
		streamDone chan error
		streamErr  error
	)
	// Mellium Session.Close writes the closing stream token and deliberately
	// leaves the underlying connection open. Preserve that protocol ordering:
	// allow one bounded graceful write before raw close unblocks Serve and any
	// in-flight writer holding Mellium's output lock. This owner-private bound
	// is independent of the context used by any caller merely joining Close.
	if session != nil {
		streamDone = make(chan error, 1)
		go func() { streamDone <- session.Close() }()
		timer := time.NewTimer(gracefulTimeout)
		select {
		case streamErr = <-streamDone:
			streamDone = nil
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
	if conn != nil {
		result = errors.Join(result, normalizeMelliumTeardownError(conn.Close()))
	}
	if streamDone != nil {
		// Closing the exact raw connection releases Mellium Session.Close when
		// it was waiting on a socket write or an in-flight writer's output lock.
		streamErr = <-streamDone
	}
	result = errors.Join(result, normalizeMelliumTeardownError(streamErr))
	return result
}

// normalizeMelliumTeardownError recognizes only transport-terminal outcomes
// that mean the peer/raw socket already completed shutdown. It recursively
// removes those components from errors.Join while retaining any independent
// dependency failure.
func normalizeMelliumTeardownError(err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var retained error
		for _, part := range joined.Unwrap() {
			retained = errors.Join(retained, normalizeMelliumTeardownError(part))
		}
		return retained
	}
	if err == net.ErrClosed || err == io.ErrClosedPipe || err == io.EOF || err == syscall.EPIPE || err == syscall.ECONNRESET {
		return nil
	}
	if cause := errors.Unwrap(err); cause != nil && normalizeMelliumTeardownError(cause) == nil {
		return nil
	}
	return err
}

func (s *melliumSession) serve(session *xmpp.Session, ctx context.Context, generation uint64) {
	defer s.serveWG.Done()
	ctx = context.WithValue(ctx, melliumSessionGenerationKey{}, generation)
	err := func() (result error) {
		defer func() {
			if recover() != nil {
				result = ErrUnavailable
			}
		}()
		return session.Serve(xmpp.HandlerFunc(func(t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
			handleErr := s.handleElement(ctx, t, start)
			if handleErr != nil {
				emitRank2Evidence(rank2EvidenceRecord{
					Event:            "xmpp_element_handler_failed",
					Source:           "mellium",
					Stage:            "handle_element",
					ElementNamespace: start.Name.Space,
					ElementLocal:     start.Name.Local,
					WireGeneration:   generation,
					Current:          true,
				}, handleErr)
			}
			return handleErr
		}))
	}()
	s.mu.Lock()
	current := !s.closed && s.session == session && s.generation == generation
	s.mu.Unlock()
	emitRank2Evidence(rank2EvidenceRecord{
		Event:          "mellium_serve_return",
		Source:         "mellium",
		Stage:          "session_serve",
		WireGeneration: generation,
		Current:        current,
	}, err)
	s.retireServeGeneration(session, generation)
	select {
	case s.serveDone <- serveResult{generation: generation, err: err}:
	default:
	}
}

// retireServeGeneration fail-stops only the exact published generation whose
// parser returned. Mellium may return from Serve while the raw connection is
// still open (for example after a stanza-handler failure); leaving that socket
// alive makes ejabberd keep routing to a consumer that can no longer parse or
// acknowledge input. A stale Serve callback must never reach a resumed or
// clean-session replacement.
func (s *melliumSession) retireServeGeneration(session *xmpp.Session, generation uint64) {
	if s == nil || session == nil || generation == 0 {
		return
	}
	s.mu.Lock()
	current := !s.closed && s.session == session && s.generation == generation
	if !current {
		s.mu.Unlock()
		return
	}
	s.suspended = true
	cancel, conn := s.cancel, s.conn
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.SetDeadline(time.Now())
		_ = conn.Close()
	}
}

func (s *melliumSession) handleElement(ctx context.Context, t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
	budget := 0
	if start.Name == (xml.Name{Space: stanza.NSClient, Local: "message"}) {
		budget = s.config.StanzaBudgetBytes
	}
	if start.Name == (xml.Name{Space: stanza.NSClient, Local: "iq"}) {
		budget = MaximumSignalBytes
	}
	if budget > 0 && startElementSize(*start) > budget {
		return ErrProtocol
	}
	switch {
	case start.Name == (xml.Name{Space: stanza.NSClient, Local: "message"}):
		message, err := stanza.NewMessage(*start)
		if err != nil {
			return err
		}
		return s.handleMessage(ctx, t, *start, message)
	case start.Name == (xml.Name{Space: stanza.NSClient, Local: "iq"}):
		iq, err := stanza.NewIQ(*start)
		if err != nil {
			return err
		}
		if iq.Type == stanza.ErrorIQ && iq.From.Localpart() == "" && iq.From.Resourcepart() == "" {
			if _, issued := s.lookupIssuedJingle(iq.ID); issued {
				return s.handleJingleIQError(ctx, t, *start, iq)
			}
		}
		if (iq.Type == stanza.ResultIQ || iq.Type == stanza.ErrorIQ) && iq.From.Localpart() == "" && iq.From.Resourcepart() == "" {
			owner, ownerErr := s.classifyBareServerResult(ctx, *start, iq)
			if ownerErr != nil {
				return ownerErr
			}
			switch owner.kind {
			case bareServerResultResumeAuthority:
				return s.handleResumeAuthorityResult(ctx, t, *start, iq)
			case bareServerResultEntityTime:
				return s.handleLateServerTimeResult(ctx, t, *start, iq, owner)
			case bareServerResultResumedExternalService:
				return s.handleResumedExternalServiceResult(ctx, t, *start, iq, owner)
			default:
				return ErrAuthentication
			}
		}
		if iq.Type == stanza.GetIQ && iq.From.Localpart() == "" && iq.From.Resourcepart() == "" {
			return s.handleServerPing(ctx, t, *start, iq)
		}
		if iq.Type == stanza.ErrorIQ {
			return s.handleJingleIQError(ctx, t, *start, iq)
		}
		if iq.Type == stanza.SetIQ && iq.From.Localpart() == "" &&
			iq.From.Resourcepart() == "" && iq.To.String() == s.username+"/"+s.boundResource() {
			bound, parseErr := jid.Parse(s.username)
			if parseErr != nil || iq.From.Domainpart() != bound.Domainpart() || iq.ID == "" {
				return ErrAuthentication
			}
			decodeErr := decodeMembershipChanged(t, *start, s.config.StanzaBudgetBytes)
			if decodeErr != nil {
				_ = s.fenceAuthorityIngress(ctx)
				return decodeErr
			}
			if !s.fenceAuthorityIngress(ctx) {
				return ErrUnavailable
			}
			if emitErr := s.emit(ctx, Event{Kind: EventMembershipChanged}); emitErr != nil {
				return emitErr
			}
			if err := s.acquireWrite(ctx); err != nil {
				return err
			}
			defer s.releaseWrite()
			s.mu.Lock()
			management := s.management
			s.mu.Unlock()
			if management == nil {
				return ErrStreamManagement
			}
			ack := Stanza{Kind: StanzaAuthoritySync, From: iq.To.String(),
				To: iq.From.String(), MeshID: s.meshID, MessageID: iq.ID}
			if err := management.RecordSent(ack); err != nil {
				return err
			}
			_, err = xmlstream.Copy(t, iq.Result(nilTokenReader{}))
			if err != nil {
				return err
			}
			if err = encodeSMRequest(t); err != nil {
				return err
			}
			management.MarkHandledInbound()
			return nil
		}
		if !s.acceptPeerResource(iq.From.Resourcepart()) || iq.To.String() != s.username+"/"+s.boundResource() {
			return ErrAuthentication
		}
		s.mu.Lock()
		management := s.management
		s.mu.Unlock()
		if management == nil {
			return ErrStreamManagement
		}
		if iq.Type != stanza.SetIQ {
			return s.handleJingleIQResult(ctx, t, *start, iq)
		}
		children := &outerBoundaryTokenReader{source: t, outer: start.Name}
		counter, budgetErr := newStanzaBudget(children, *start, MaximumSignalBytes)
		if budgetErr != nil {
			return budgetErr
		}
		decoder := xml.NewTokenDecoder(counter)
		var signal Jingle
		if err = decoder.Decode(&signal); err != nil {
			return err
		}
		if token, tailErr := decoder.Token(); tailErr != io.EOF || token != nil || !children.done {
			return ErrProtocol
		}
		if signal, err = decodeJingleValue(signal); err != nil {
			return err
		}
		encoded, err := EncodeJingle(signal)
		if err != nil {
			return err
		}
		if err = s.emit(ctx, Event{Kind: EventStanza, Stanza: Stanza{Kind: StanzaSignal, From: iq.From.String(), To: iq.To.String(), MeshID: s.meshID, AttemptID: signal.SID, Data: encoded}}); err != nil {
			return err
		}
		if err := s.acquireWrite(ctx); err != nil {
			return err
		}
		defer s.releaseWrite()
		s.mu.Lock()
		suspended := s.suspended
		s.mu.Unlock()
		if suspended {
			return ErrUnavailable
		}
		resultRecord := Stanza{Kind: StanzaSignalResult, From: iq.To.String(), To: iq.From.String(), MeshID: s.meshID, AttemptID: iq.ID}
		if err = management.RecordSent(resultRecord); err != nil {
			return err
		}
		result := iq.Result(nilTokenReader{})
		_, err = xmlstream.Copy(t, result)
		if err != nil {
			return err
		}
		if err = encodeSMRequest(t); err != nil {
			return err
		}
		management.MarkHandledInbound()
		return nil
	case start.Name == (xml.Name{Space: streamManagementNamespace, Local: "a"}):
		h, err := decodeSMAck(t, *start)
		if err != nil {
			return err
		}
		s.mu.Lock()
		management := s.management
		s.mu.Unlock()
		ordinal, count, err := management.ApplyAckDetailed(h)
		if err != nil {
			return err
		}
		return s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count})
	case start.Name == (xml.Name{Space: streamManagementNamespace, Local: "r"}):
		if err := decodeSMRequest(t, *start); err != nil {
			return err
		}
		s.mu.Lock()
		management := s.management
		s.mu.Unlock()
		handled := management.HandledInbound()
		ack := xml.StartElement{Name: xml.Name{Space: streamManagementNamespace, Local: "a"}, Attr: []xml.Attr{{Name: xml.Name{Local: "h"}, Value: strconv.FormatUint(uint64(handled), 10)}}}
		if err := t.EncodeToken(ack); err != nil {
			return err
		}
		if err := t.EncodeToken(ack.End()); err != nil {
			return err
		}
		s.mu.Lock()
		generation, _ := ctx.Value(melliumSessionGenerationKey{}).(uint64)
		if generation != 0 && generation == s.resumeBarrierGeneration && generation == s.generation && s.resumeBarrier != nil && !s.resumeMarkerSeen {
			s.resumeMarkerSeen = true
		}
		s.mu.Unlock()
		return nil
	}
	return nil
}

// classifyBareServerResult assigns an unsolicited bare-server result to one
// exact current owner before any payload decoder or resume barrier can see it.
// Resume authority IDs are server-generated and belong only to the active
// resumed generation. Late XEP-0202 results belong only to an exact retained
// time-calibration ledger record; a shared string prefix is never authority.
func (s *melliumSession) classifyBareServerResult(ctx context.Context, outer xml.StartElement, iq stanza.IQ) (bareServerResultOwnership, error) {
	if s == nil || ctx == nil {
		return bareServerResultOwnership{}, ErrInvalidConfig
	}
	generation, ok := ctx.Value(melliumSessionGenerationKey{}).(uint64)
	s.mu.Lock()
	username, resource, meshID, management := s.username, s.boundResource(), s.meshID, s.management
	current := ok && generation != 0 && generation == s.generation && !s.closed && !s.suspended
	resumeOwned := current && generation == s.resumeBarrierGeneration && s.resumeBarrier != nil
	s.mu.Unlock()
	if !current || management == nil {
		return bareServerResultOwnership{}, ErrProtocol
	}
	local, err := jid.Parse(username + "/" + resource)
	if err != nil || local.Localpart() == "" || local.Resourcepart() == "" {
		return bareServerResultOwnership{}, ErrIdentityBinding
	}
	server := local.Domain()
	iqType, valid := validCorrelatedIQAttrs(outer.Attr, iq.ID, server, local)
	if outer.Name != (xml.Name{Space: stanza.NSClient, Local: "iq"}) || !valid || iqType != iq.Type {
		return bareServerResultOwnership{}, ErrAuthentication
	}
	if iq.Type == stanza.ResultIQ && resumeOwned && validResumeAuthorityID(iq.ID) {
		return bareServerResultOwnership{kind: bareServerResultResumeAuthority, generation: generation, management: management}, nil
	}
	if iq.Type == stanza.ResultIQ && validEntityTimeID(iq.ID) {
		request := Stanza{
			Kind: StanzaTimeCalibration, From: local.String(), To: server.String(),
			MeshID: meshID, MessageID: iq.ID,
		}
		sequence, retained, claimErr := management.claimPendingCorrelatedSequence(request)
		if claimErr != nil {
			return bareServerResultOwnership{}, claimErr
		}
		if !retained {
			return bareServerResultOwnership{}, ErrAuthentication
		}
		return bareServerResultOwnership{
			kind: bareServerResultEntityTime, generation: generation,
			management: management, sequence: sequence,
		}, nil
	}
	record, retained := s.claimResumedExternalService(generation, iq.ID)
	if !retained || !validExternalServiceQueryReplay(record, local.String(), server.String(), meshID) {
		clearStanzaOwned(&record)
		return bareServerResultOwnership{}, ErrAuthentication
	}
	return bareServerResultOwnership{
		kind: bareServerResultResumedExternalService, generation: generation,
		management: management, record: record,
	}, nil
}

func (s *melliumSession) claimResumedExternalService(generation uint64, id string) (Stanza, bool) {
	if s == nil || generation == 0 || id == "" {
		return Stanza{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.suspended || generation != s.generation || generation != s.resumeCorrelatedGeneration || s.resumeCorrelated == nil {
		return Stanza{}, false
	}
	record, ok := s.resumeCorrelated[id]
	if !ok {
		return Stanza{}, false
	}
	delete(s.resumeCorrelated, id)
	if len(s.resumeCorrelated) == 0 {
		s.resumeCorrelated = nil
		s.resumeCorrelatedGeneration = 0
	}
	return record, true
}

// handleResumedExternalServiceResult consumes a response whose original
// caller belonged to the disconnected Mellium session. The exact request
// descriptor is generation-owned and one-use, so an injected ID or stale
// response cannot retire stream-management state. The refreshed profile is
// intentionally discarded; a live caller may perform a fresh query later.
func (s *melliumSession) handleResumedExternalServiceResult(ctx context.Context, source xml.TokenReader, outer xml.StartElement, iq stanza.IQ, owner bareServerResultOwnership) error {
	if s == nil || ctx == nil || source == nil || owner.management == nil || owner.record.Kind != StanzaExternalServiceQuery {
		clearStanzaOwned(&owner.record)
		return ErrInvalidConfig
	}
	defer clearStanzaOwned(&owner.record)
	local, localErr := jid.Parse(owner.record.From)
	server, serverErr := jid.Parse(owner.record.To)
	if localErr != nil || serverErr != nil || iq.ID != owner.record.MessageID {
		return ErrAuthentication
	}
	reader := &prefixedTokenReader{first: outer, source: source}
	services, correlated, handled, decodeErr := decodeMelliumExternalServices(
		reader, owner.record.MessageID, server, local, owner.record.AttemptID,
		externalServiceQueryRequested(owner.record), time.Now().UTC(),
	)
	clearExternalServices(services)
	if !correlated || !handled || decodeErr != nil && !errors.Is(decodeErr, ErrUnavailable) {
		return ErrProtocol
	}
	ordinal, count, err := owner.management.ConfirmCorrelatedHandledRecord(owner.record)
	if err != nil {
		return err
	}
	owner.management.MarkHandledInbound()
	if count > 0 {
		if err = s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count}); err != nil {
			return err
		}
	}
	return nil
}

type prefixedTokenReader struct {
	first  xml.Token
	source xml.TokenReader
}

func (reader *prefixedTokenReader) Token() (xml.Token, error) {
	if reader == nil || reader.source == nil {
		return nil, io.EOF
	}
	if reader.first != nil {
		token := reader.first
		reader.first = nil
		return token, nil
	}
	return reader.source.Token()
}

// handleResumeAuthorityResult accepts only the one server-owned continuity
// result for the exact resumed generation. ejabberd's post-resume <r/> is the
// phase separator: a canonical result replayed before it belongs to an older
// reconnect and is acknowledged but ignored. Exactly one canonical result
// after it is handed to the independent control lane. The barrier itself owns
// the status so a duplicate can still turn a queued ready result into a
// fail-closed outcome before the control worker releases peer traffic.
func (s *melliumSession) handleResumeAuthorityResult(ctx context.Context, source xml.TokenReader, outer xml.StartElement, iq stanza.IQ) error {
	if s == nil || ctx == nil || source == nil {
		return ErrInvalidConfig
	}
	local, err := jid.Parse(s.username + "/" + s.boundResource())
	if err != nil || local.Localpart() == "" || local.Resourcepart() == "" ||
		!validResumeAuthorityID(iq.ID) || !validCorrelatedResultIQ(outer, iq, local.Domain(), local) {
		s.failCurrentResumeAuthority(ctx)
		return ErrAuthentication
	}
	status, err := decodeResumeAuthorityResult(source, outer, s.config.StanzaBudgetBytes)
	if err != nil {
		s.failCurrentResumeAuthority(ctx)
		return err
	}
	generation, ok := ctx.Value(melliumSessionGenerationKey{}).(uint64)
	s.mu.Lock()
	barrier := s.resumeBarrier
	current := ok && generation != 0 && generation == s.generation && generation == s.resumeBarrierGeneration && barrier != nil && !s.closed && !s.suspended
	if !current {
		s.mu.Unlock()
		return ErrProtocol
	}
	if !s.resumeMarkerSeen {
		management := s.management
		if management == nil {
			s.mu.Unlock()
			return ErrStreamManagement
		}
		management.MarkHandledInbound()
		s.mu.Unlock()
		return nil
	}
	if s.resumeResultQueued {
		barrier.ready.Store(false)
		barrier.rejected.Store(true)
		s.mu.Unlock()
		return ErrProtocol
	}
	s.resumeResultQueued = true
	barrier.ready.Store(status == "ready")
	barrier.rejected.Store(status == "not-ready")
	management := s.management
	if management == nil {
		barrier.ready.Store(false)
		barrier.rejected.Store(true)
		s.mu.Unlock()
		return ErrStreamManagement
	}
	// Commit h before publishing the barrier event. The control worker may close
	// barrier.done as soon as emit succeeds, so accounting afterward would let
	// replay escape ahead of the result's handled position.
	management.MarkHandledInbound()
	s.mu.Unlock()
	if err = s.emit(ctx, Event{Kind: EventResumeAuthorityResult, resumeBarrier: barrier}); err != nil {
		return err
	}
	return nil
}

func (s *melliumSession) failCurrentResumeAuthority(ctx context.Context) {
	generation, ok := ctx.Value(melliumSessionGenerationKey{}).(uint64)
	s.mu.Lock()
	barrier := s.resumeBarrier
	current := ok && generation != 0 && generation == s.generation && generation == s.resumeBarrierGeneration && barrier != nil && s.resumeMarkerSeen
	if current {
		barrier.ready.Store(false)
		barrier.rejected.Store(true)
		if !s.resumeResultQueued {
			s.resumeResultQueued = true
		} else {
			barrier = nil
		}
	} else {
		barrier = nil
	}
	s.mu.Unlock()
	if barrier != nil {
		_ = s.emit(ctx, Event{Kind: EventResumeAuthorityResult, resumeBarrier: barrier})
	}
}

func validResumeAuthorityID(id string) bool {
	if !strings.HasPrefix(id, resumeAuthorityIDPrefix) {
		return false
	}
	value := strings.TrimPrefix(id, resumeAuthorityIDPrefix)
	if value == "" || len(value) > 20 || value[0] == '0' {
		return false
	}
	for index := range value {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}

func decodeResumeAuthorityResult(source xml.TokenReader, outer xml.StartElement, maximum int) (string, error) {
	children := &outerBoundaryTokenReader{source: source, outer: outer.Name}
	bounded, err := newStanzaBudget(children, outer, maximum)
	if err != nil {
		return "", ErrProtocol
	}
	decoder := xml.NewTokenDecoder(bounded)
	var value struct {
		XMLName      xml.Name
		Status       string       `xml:"status,attr"`
		Text         string       `xml:",chardata"`
		Unknown      []xmlUnknown `xml:",any"`
		UnknownAttrs []xml.Attr   `xml:",any,attr"`
	}
	if err = decoder.Decode(&value); err != nil || value.XMLName != (xml.Name{Space: authorityNamespace, Local: "resume-authority"}) ||
		(value.Status != "ready" && value.Status != "not-ready") || strings.TrimSpace(value.Text) != "" || len(value.Unknown) != 0 ||
		!onlyNamespaceAttr(value.UnknownAttrs, authorityNamespace) {
		return "", ErrProtocol
	}
	if token, tailErr := decoder.Token(); tailErr != io.EOF || token != nil || !children.done {
		return "", ErrProtocol
	}
	return value.Status, nil
}

// handleJingleIQError accepts only a canonical server-originated error for an
// exact bound local resource and an ID retained in the bounded issued-Jingle
// ledger. The ledger outlives an application waiter, so a delayed legitimate
// response cannot poison the shared Rank2 stream. Unknown IDs remain fatal.
func (s *melliumSession) handleJingleIQError(ctx context.Context, source xml.TokenReader, outer xml.StartElement, iq stanza.IQ) error {
	if s == nil || ctx == nil || source == nil || iq.ID == "" {
		return ErrProtocol
	}
	local, err := jid.Parse(s.username + "/" + s.boundResource())
	if err != nil || local.Resourcepart() == "" || iq.To.String() != local.String() {
		return ErrAuthentication
	}
	server, err := jid.Parse(local.Domainpart())
	if err != nil || iq.From.String() != server.String() || iq.From.Localpart() != "" || iq.From.Resourcepart() != "" {
		return ErrAuthentication
	}
	if iqType, ok := validCorrelatedIQAttrs(outer.Attr, iq.ID, server, local); !ok || iqType != stanza.ErrorIQ {
		return ErrProtocol
	}
	if err = decodeJingleIQError(source, outer); err != nil {
		return err
	}
	issued, issuedOK := s.lookupIssuedJingle(iq.ID)
	if !issuedOK || issued.from != iq.To.String() {
		return ErrAuthentication
	}
	s.mu.Lock()
	management, generation := s.management, s.generation
	s.mu.Unlock()
	contextGeneration, ok := ctx.Value(melliumSessionGenerationKey{}).(uint64)
	if management == nil || !ok || contextGeneration == 0 || contextGeneration != generation {
		return ErrStreamManagement
	}
	firstResponse, completed := s.completeIssuedJingle(iq.ID, issued)
	if !completed {
		return ErrProtocol
	}
	if firstResponse {
		event := Event{Kind: EventJingleFailure, AttemptID: strings.Clone(iq.ID)}
		if err = s.emit(ctx, event); err != nil {
			return err
		}
	}
	management.MarkHandledInbound()
	return nil
}

func decodeJingleIQError(source xml.TokenReader, outer xml.StartElement) error {
	children := &outerBoundaryTokenReader{source: source, outer: outer.Name}
	bounded, err := newStanzaBudget(children, outer, maximumJingleIQErrorBytes)
	if err != nil {
		return ErrProtocol
	}
	reader := &privateIQTokenReader{source: bounded, remaining: maximumJingleIQErrorTokens}
	token, err := nextJingleIQErrorToken(reader)
	errorStart, ok := token.(xml.StartElement)
	if err != nil || !ok || errorStart.Name.Local != "error" || errorStart.Name.Space != "" && errorStart.Name.Space != stanza.NSClient || !validPrivateIQErrorAttrs(errorStart.Attr) {
		return ErrProtocol
	}
	token, err = nextJingleIQErrorToken(reader)
	condition, ok := token.(xml.StartElement)
	if err != nil || !ok || condition.Name.Space != "urn:ietf:params:xml:ns:xmpp-stanzas" || !validJingleIQErrorCondition(condition.Name.Local) || !onlyNamespaceAttr(condition.Attr, condition.Name.Space) {
		return ErrProtocol
	}
	token, err = nextJingleIQErrorToken(reader)
	if err != nil || token != condition.End() {
		return ErrProtocol
	}
	token, err = nextJingleIQErrorToken(reader)
	if textStart, hasText := token.(xml.StartElement); hasText {
		if textStart.Name != (xml.Name{Space: "urn:ietf:params:xml:ns:xmpp-stanzas", Local: "text"}) || !validJingleIQErrorTextAttrs(textStart.Attr) {
			return ErrProtocol
		}
		textBytes := 0
		for {
			token, err = reader.Token()
			if err != nil {
				return ErrProtocol
			}
			if end, isEnd := token.(xml.EndElement); isEnd {
				if end != textStart.End() || textBytes == 0 {
					return ErrProtocol
				}
				break
			}
			chars, isText := token.(xml.CharData)
			if !isText || len(chars) == 0 || textBytes > maximumJingleIQErrorText-len(chars) {
				return ErrProtocol
			}
			textBytes += len(chars)
		}
		token, err = nextJingleIQErrorToken(reader)
	}
	if err != nil || token != errorStart.End() {
		return ErrProtocol
	}
	if token, err = nextJingleIQErrorToken(reader); err != io.EOF || token != nil || !children.done {
		return ErrProtocol
	}
	return nil
}

func nextJingleIQErrorToken(reader xml.TokenReader) (xml.Token, error) {
	for {
		token, err := reader.Token()
		if err != nil {
			return token, err
		}
		if chars, ok := token.(xml.CharData); ok && strings.TrimSpace(string(chars)) == "" {
			continue
		}
		return token, nil
	}
}

func validJingleIQErrorTextAttrs(attrs []xml.Attr) bool {
	languageSeen := false
	namespaceSeen := false
	for _, attr := range attrs {
		if attr.Name.Space == "" && attr.Name.Local == "xmlns" && attr.Value == "urn:ietf:params:xml:ns:xmpp-stanzas" && !namespaceSeen {
			namespaceSeen = true
			continue
		}
		if attr.Name == (xml.Name{Space: "http://www.w3.org/XML/1998/namespace", Local: "lang"}) && validEntityTimeLanguage(attr.Value) && !languageSeen {
			languageSeen = true
			continue
		}
		return false
	}
	return true
}

func validJingleIQErrorCondition(condition string) bool {
	switch condition {
	case "bad-request", "conflict", "feature-not-implemented", "forbidden", "gone", "internal-server-error",
		"item-not-found", "jid-malformed", "not-acceptable", "not-allowed", "not-authorized", "policy-violation",
		"recipient-unavailable", "redirect", "registration-required", "remote-server-not-found", "remote-server-timeout",
		"resource-constraint", "service-unavailable", "subscription-required", "undefined-condition", "unexpected-request":
		return true
	default:
		return false
	}
}

func decodeSMAck(source xml.TokenReader, start xml.StartElement) (uint32, error) {
	attrs, err := decodeSMEmptyElement(source, start, "a", "h")
	if err != nil {
		return 0, err
	}
	h, ok := parseCanonicalSMUint32(attrs["h"])
	if !ok || len(attrs) != 1 {
		return 0, ErrProtocol
	}
	return h, nil
}

func decodeSMRequest(source xml.TokenReader, start xml.StartElement) error {
	attrs, err := decodeSMEmptyElement(source, start, "r")
	if err != nil || len(attrs) != 0 {
		return ErrProtocol
	}
	return nil
}

func decodeSMEnabled(source xml.TokenReader, start xml.StartElement) (string, error) {
	attrs, err := decodeSMEmptyElement(source, start, "enabled", "id", "location", "max", "resume")
	if err != nil {
		return "", err
	}
	id, hasID := attrs["id"]
	resume, hasResume := attrs["resume"]
	maximum, hasMaximum := attrs["max"]
	if !hasID || id == "" || !hasResume || resume != "true" && resume != "1" || hasMaximum && !validSMPositiveInteger(maximum) {
		return "", ErrProtocol
	}
	return id, nil
}

func decodeSMResumed(source xml.TokenReader, start xml.StartElement, expectedID string) (uint32, error) {
	attrs, err := decodeSMEmptyElement(source, start, "resumed", "h", "previd")
	if err != nil {
		return 0, err
	}
	h, ok := parseCanonicalSMUint32(attrs["h"])
	previd, hasPrevid := attrs["previd"]
	if !ok || !hasPrevid || previd == "" || previd != expectedID || len(attrs) != 2 {
		return 0, ErrProtocol
	}
	return h, nil
}

// decodeSMEmptyElement validates an entire XEP-0198 nonza before its caller
// may use any value to mutate stream-management state. Attribute names are
// unqualified in the XEP schema. Namespace declarations are XML metadata, not
// data attributes: valid default and prefix declarations are ignored after
// encoding/xml has resolved the element and attribute names.
func decodeSMEmptyElement(source xml.TokenReader, start xml.StartElement, local string, allowed ...string) (map[string]string, error) {
	if source == nil || start.Name != (xml.Name{Space: streamManagementNamespace, Local: local}) {
		return nil, ErrProtocol
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		if name == "" {
			return nil, ErrProtocol
		}
		allowedSet[name] = struct{}{}
	}
	values := make(map[string]string, len(allowed))
	namespaceSeen := false
	for index, attr := range start.Attr {
		if attr.Name.Space == "" && attr.Name.Local == "xmlns" {
			if namespaceSeen || !validSMDefaultNamespaceDeclaration(attr) {
				return nil, ErrProtocol
			}
			namespaceSeen = true
			continue
		}
		if attr.Name.Space == "xmlns" {
			if !validSMPrefixNamespaceDeclaration(attr) {
				return nil, ErrProtocol
			}
			for _, previous := range start.Attr[:index] {
				if previous.Name.Space == "xmlns" && previous.Name.Local == attr.Name.Local {
					return nil, ErrProtocol
				}
			}
			continue
		}
		if attr.Name.Space != "" {
			return nil, ErrProtocol
		}
		if _, ok := allowedSet[attr.Name.Local]; !ok {
			return nil, ErrProtocol
		}
		if _, duplicate := values[attr.Name.Local]; duplicate {
			return nil, ErrProtocol
		}
		values[attr.Name.Local] = attr.Value
	}
	token, err := source.Token()
	if err != nil {
		var syntax *xml.SyntaxError
		if errors.Is(err, io.EOF) || errors.As(err, &syntax) {
			return nil, ErrProtocol
		}
		return nil, err
	}
	end, ok := token.(xml.EndElement)
	if !ok || end.Name != start.Name {
		return nil, ErrProtocol
	}
	return values, nil
}

func validSMDefaultNamespaceDeclaration(attr xml.Attr) bool {
	return attr.Name.Space == "" && attr.Name.Local == "xmlns" &&
		attr.Value != "http://www.w3.org/XML/1998/namespace" &&
		attr.Value != "http://www.w3.org/2000/xmlns/"
}

func validSMPrefixNamespaceDeclaration(attr xml.Attr) bool {
	const (
		xmlNamespace   = "http://www.w3.org/XML/1998/namespace"
		xmlnsNamespace = "http://www.w3.org/2000/xmlns/"
	)
	if attr.Name.Space != "xmlns" || attr.Name.Local == "" || attr.Name.Local == "xmlns" || attr.Value == "" || attr.Value == xmlnsNamespace {
		return false
	}
	if attr.Name.Local == "xml" {
		return attr.Value == xmlNamespace
	}
	return attr.Value != xmlNamespace
}

func parseCanonicalSMUint32(value string) (uint32, bool) {
	if value == "" || len(value) > 10 || len(value) > 1 && value[0] == '0' {
		return 0, false
	}
	for i := range value {
		if value[i] < '0' || value[i] > '9' {
			return 0, false
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	return uint32(parsed), err == nil
}

// validSMPositiveInteger implements the lexical space of xs:positiveInteger:
// surrounding XML whitespace is collapsed, a leading plus is permitted, and
// the decimal value must contain at least one non-zero digit. It deliberately
// does not parse into a machine integer because the schema has no upper bound.
func validSMPositiveInteger(value string) bool {
	start, end := 0, len(value)
	for start < end && isSMXMLSpace(value[start]) {
		start++
	}
	for end > start && isSMXMLSpace(value[end-1]) {
		end--
	}
	if start == end {
		return false
	}
	if value[start] == '+' {
		start++
		if start == end {
			return false
		}
	}
	positive := false
	for index := start; index < end; index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
		positive = positive || value[index] != '0'
	}
	return positive
}

func isSMXMLSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}

func decodeMembershipChanged(source xml.TokenReader, outer xml.StartElement, budget int) error {
	children := &outerBoundaryTokenReader{source: source, outer: outer.Name}
	bounded, err := newStanzaBudget(children, outer, budget)
	if err != nil {
		return ErrProtocol
	}
	decoder := xml.NewTokenDecoder(bounded)
	var value struct {
		XMLName          xml.Name
		SnapshotRequired string        `xml:"snapshot-required,attr"`
		Removed          []removedPeer `xml:"removed"`
		Unknown          []xml.Attr    `xml:",any,attr"`
	}
	if err := decoder.Decode(&value); err != nil || value.XMLName != (xml.Name{Space: authorityNamespace, Local: "membership-changed"}) || value.SnapshotRequired != "true" || len(value.Unknown) != 1 || !onlyNamespaceAttr(value.Unknown, authorityNamespace) || len(value.Removed) > maximumAuthorityPeers {
		return ErrProtocol
	}
	if token, tailErr := decoder.Token(); tailErr != io.EOF || token != nil || !children.done {
		return ErrProtocol
	}
	seen := make(map[string]struct{}, len(value.Removed))
	for _, removed := range value.Removed {
		parsed, parseErr := jid.Parse(removed.JID)
		if parseErr != nil || removed.XMLName != (xml.Name{Space: authorityNamespace, Local: "removed"}) || parsed.Localpart() == "" || parsed.Resourcepart() == "" || parsed.String() != removed.JID || len(removed.Unknown) != 0 {
			return ErrProtocol
		}
		if _, duplicate := seen[removed.JID]; duplicate {
			return ErrProtocol
		}
		seen[removed.JID] = struct{}{}
	}
	return nil
}

type removedPeer struct {
	XMLName xml.Name
	JID     string     `xml:"jid,attr"`
	Unknown []xml.Attr `xml:",any,attr"`
}

func startElementSize(start xml.StartElement) int {
	size, ok := checkedXMLSize(2, len(start.Name.Local))
	if !ok {
		return int(^uint(0) >> 1)
	}
	for _, attr := range start.Attr {
		escaped, escapedOK := escapedXMLStringLength(attr.Value)
		if !escapedOK {
			return int(^uint(0) >> 1)
		}
		size, ok = checkedXMLSize(size, 4, len(attr.Name.Local), escaped)
		if !ok {
			return int(^uint(0) >> 1)
		}
	}
	return size
}

type stanzaBudgetReader struct {
	source        xml.TokenReader
	used, maximum int
}

func newStanzaBudget(source xml.TokenReader, outer xml.StartElement, maximum int) (*stanzaBudgetReader, error) {
	if source == nil || maximum <= 0 {
		return nil, ErrProtocol
	}
	used, ok := checkedXMLSize(xmlTokenSize(outer), xmlTokenSize(outer.End()))
	if !ok || used > maximum {
		return nil, ErrProtocol
	}
	return &stanzaBudgetReader{source: source, used: used, maximum: maximum}, nil
}

func (r *stanzaBudgetReader) Token() (xml.Token, error) {
	token, err := r.source.Token()
	if err != nil {
		return token, err
	}
	size := xmlTokenSize(token)
	if size < 0 || size > r.maximum || r.used > r.maximum-size {
		return nil, ErrProtocol
	}
	r.used += size
	return token, nil
}

// xmlTokenSize is the deterministic expanded-name wire accounting profile.
// It includes namespaces, all attributes, escaped text and closing tags.
func xmlTokenSize(token xml.Token) int {
	switch value := token.(type) {
	case xml.StartElement:
		size, ok := checkedXMLSize(2, len(value.Name.Local))
		if !ok {
			return -1
		}
		if value.Name.Space != "" {
			size, ok = checkedXMLSize(size, 9, len(value.Name.Space))
			if !ok {
				return -1
			}
		}
		for _, attr := range value.Attr {
			escaped, escapedOK := escapedXMLStringLength(attr.Value)
			if !escapedOK {
				return -1
			}
			size, ok = checkedXMLSize(size, 4, len(attr.Name.Local), escaped)
			if !ok {
				return -1
			}
			if attr.Name.Space != "" {
				size, ok = checkedXMLSize(size, 7, len(attr.Name.Space))
				if !ok {
					return -1
				}
			}
		}
		return size
	case xml.EndElement:
		size, ok := checkedXMLSize(3, len(value.Name.Local))
		if !ok {
			return -1
		}
		return size
	case xml.CharData:
		escaped, ok := escapedXMLBytesLength(value)
		if !ok {
			return -1
		}
		return escaped
	case xml.Comment:
		size, ok := checkedXMLSize(7, len(value))
		if !ok {
			return -1
		}
		return size
	case xml.Directive:
		size, ok := checkedXMLSize(3, len(value))
		if !ok {
			return -1
		}
		return size
	case xml.ProcInst:
		size, ok := checkedXMLSize(5, len(value.Target), len(value.Inst))
		if !ok {
			return -1
		}
		return size
	default:
		return -1
	}
}

func (s *melliumSession) handleServerPing(ctx context.Context, t xmlstream.TokenReadEncoder, outer xml.StartElement, iq stanza.IQ) error {
	if s == nil || ctx == nil || t == nil {
		return ErrInvalidConfig
	}
	bound, err := jid.Parse(s.username)
	if err != nil || iq.From.Domainpart() != bound.Domainpart() || iq.To.String() != s.username+"/"+s.boundResource() || iq.ID == "" || !validServerPingIQAttributes(outer.Attr) {
		return ErrAuthentication
	}
	if err = decodeExactServerPing(t, outer, s.config.StanzaBudgetBytes); err != nil {
		return err
	}
	if err = s.acquireWrite(ctx); err != nil {
		return err
	}
	defer s.releaseWrite()
	s.mu.Lock()
	management, suspended := s.management, s.suspended
	s.mu.Unlock()
	if suspended {
		return ErrUnavailable
	}
	if management == nil {
		return ErrStreamManagement
	}
	resultRecord := Stanza{
		Kind: StanzaSessionPingResult, From: iq.To.String(), To: iq.From.String(),
		MeshID: s.meshID, AttemptID: iq.ID,
	}
	if err = management.RecordSent(resultRecord); err != nil {
		return err
	}
	_, err = xmlstream.Copy(t, iq.Result(nilTokenReader{}))
	if err != nil {
		return err
	}
	if err = encodeSMRequest(t); err != nil {
		return err
	}
	management.MarkHandledInbound()
	return nil
}

func validServerPingIQAttributes(attributes []xml.Attr) bool {
	seen := make(map[string]struct{}, 4)
	namespaceSeen := false
	for _, attribute := range attributes {
		if attribute.Name.Space == "" && attribute.Name.Local == "xmlns" {
			if namespaceSeen || attribute.Value != stanza.NSClient {
				return false
			}
			namespaceSeen = true
			continue
		}
		if attribute.Name.Space != "" {
			return false
		}
		switch attribute.Name.Local {
		case "from", "to", "type", "id":
			if _, duplicate := seen[attribute.Name.Local]; duplicate {
				return false
			}
			seen[attribute.Name.Local] = struct{}{}
		default:
			return false
		}
	}
	return len(seen) == 4
}

func decodeExactServerPing(source xml.TokenReader, outer xml.StartElement, maximum int) error {
	children := &outerBoundaryTokenReader{source: source, outer: outer.Name}
	counter, err := newStanzaBudget(children, outer, maximum)
	if err != nil {
		return err
	}
	decoder := xml.NewTokenDecoder(counter)
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	ping, ok := token.(xml.StartElement)
	if !ok || ping.Name != (xml.Name{Space: xep0199PingNamespace, Local: "ping"}) || !validServerPingPayloadAttributes(ping.Attr) {
		return ErrProtocol
	}
	token, err = decoder.Token()
	if err != nil {
		return err
	}
	end, ok := token.(xml.EndElement)
	if !ok || end.Name != ping.Name {
		return ErrProtocol
	}
	if token, err = decoder.Token(); err != io.EOF || token != nil || !children.done {
		return ErrProtocol
	}
	return nil
}

func validServerPingPayloadAttributes(attributes []xml.Attr) bool {
	namespaceSeen := false
	for _, attribute := range attributes {
		if attribute.Name.Space == "" && attribute.Name.Local == "xmlns" {
			if namespaceSeen || attribute.Value != xep0199PingNamespace {
				return false
			}
			namespaceSeen = true
			continue
		}
		return false
	}
	return true
}

func (s *melliumSession) handleMessage(ctx context.Context, t xmlstream.TokenReadEncoder, outer xml.StartElement, message stanza.Message) error {
	if s.custodyMessageRoute(message) {
		messageID, err := decodeCustodyAcceptedBounded(t, outer, s.config.StanzaBudgetBytes)
		if err != nil || message.ID != messageID || !validReadinessTyped(messageID, "msg_", 16) {
			return ErrProtocol
		}
		s.mu.Lock()
		management := s.management
		s.mu.Unlock()
		if management == nil {
			return ErrStreamManagement
		}
		ordinal, count, err := management.EnvelopeCustodyPosition(messageID)
		if err != nil {
			return err
		}
		if err = s.emit(ctx, Event{Kind: EventCustodyAccepted, MessageID: messageID, HandledThrough: ordinal, HandledCount: count}); err != nil {
			return err
		}
		confirmedOrdinal, confirmedCount, err := management.ConfirmEnvelopeCustody(messageID)
		if err != nil || confirmedOrdinal != ordinal || confirmedCount != count {
			return ErrStreamManagement
		}
		management.MarkHandledInbound()
		return nil
	}
	if !s.acceptPeerResource(message.From.Resourcepart()) || message.To.String() != s.username+"/"+s.boundResource() {
		return ErrAuthentication
	}
	frame, err := decodeMelliumMessageFrameBounded(t, outer, s.config.StanzaBudgetBytes)
	if err != nil {
		return err
	}
	record, err := decodeStanzaFrameValue(frame, message.From.String(), message.To.String(), s.meshID, s.config.MaximumFrameBytes)
	if err != nil {
		return err
	}
	if record.Kind == StanzaEnvelope {
		codec, codecErr := protocol.NewCodec()
		if codecErr != nil {
			clearStanzaOwned(&record)
			return ErrProtocol
		}
		envelope, decodeErr := codec.Decode(record.Data)
		if decodeErr != nil {
			clearStanzaOwned(&record)
			return ErrProtocol
		}
		messageID := envelope.MessageID
		clear(envelope.Payload.Inline)
		clear(envelope.CredentialProof)
		if message.ID != messageID || !validReadinessTyped(messageID, "msg_", 16) {
			clearStanzaOwned(&record)
			return ErrProtocol
		}
		record.MessageID = messageID
	} else if message.ID != "" {
		// The Cynapsa server strips Mellium's transport-local generated ID from
		// transfer/control frames. Only application envelopes may arrive with
		// an outer XMPP correlation ID.
		clearStanzaOwned(&record)
		return ErrProtocol
	}
	if record.Kind != StanzaEnvelope {
		if err = s.emit(ctx, Event{Kind: EventStanza, Stanza: record}); err != nil {
			return err
		}
		s.mu.Lock()
		management := s.management
		s.mu.Unlock()
		management.MarkHandledInbound()
		return nil
	}
	s.mu.Lock()
	management := s.management
	generation, _ := ctx.Value(melliumSessionGenerationKey{}).(uint64)
	s.mu.Unlock()
	if management == nil {
		clearStanzaOwned(&record)
		return ErrStreamManagement
	}
	acceptance, acceptanceErr := management.DeferHandledInbound(func() {
		s.rejectInboundGeneration(management, generation)
	})
	if acceptanceErr != nil {
		clearStanzaOwned(&record)
		return acceptanceErr
	}
	record.inboundAccept = acceptance
	if err = s.emit(ctx, Event{Kind: EventStanza, Stanza: record}); err != nil {
		return err
	}
	// The parser must remain available for XEP-0198 requests, pings, and later
	// stanzas while SDK ownership is pending. acceptance resolves h later via
	// StreamManagement's ordered, payload-free ledger.
	return nil
}

func (s *melliumSession) custodyMessageRoute(message stanza.Message) bool {
	if s == nil || message.Type != stanza.HeadlineMessage || message.ID == "" ||
		message.To.String() != s.username+"/"+s.boundResource() ||
		message.From.Localpart() != "" || message.From.Resourcepart() != "" {
		return false
	}
	bound, err := jid.Parse(s.username)
	return err == nil && message.From.Domainpart() == bound.Domainpart() &&
		message.From.String() == bound.Domainpart()
}

func decodeCustodyAcceptedBounded(source xml.TokenReader, outer xml.StartElement, budget int) (string, error) {
	children := &outerBoundaryTokenReader{source: source, outer: outer.Name}
	bounded, err := newStanzaBudget(children, outer, budget)
	if err != nil {
		return "", err
	}
	decoder := xml.NewTokenDecoder(bounded)
	for {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return "", ErrProtocol
		}
		if chars, ok := token.(xml.CharData); ok && strings.TrimSpace(string(chars)) == "" {
			continue
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name != (xml.Name{Space: custodyNamespace, Local: "accepted"}) {
			return "", ErrProtocol
		}
		var accepted custodyAcceptedXML
		if err = decoder.DecodeElement(&accepted, &start); err != nil ||
			accepted.XMLName != start.Name || strings.TrimSpace(accepted.Data) != "" ||
			len(accepted.Unknown) != 0 || !onlyNamespaceAttr(accepted.UnknownAttrs, custodyNamespace) {
			return "", ErrProtocol
		}
		tail, tailErr := decoder.Token()
		if tailErr != io.EOF || tail != nil || !children.done {
			return "", ErrProtocol
		}
		return accepted.MessageID, nil
	}
}

func decodeMessageFrame(decoder *xml.Decoder) (xmlFrame, error) {
	return decodeMessageFrameBounded(decoder, 4<<20)
}

// Mellium invokes a stanza handler after consuming the authenticated outer
// start token, but xmlstream.InnerElement still yields the outer end. Mellium
// may represent that end with a different namespace form than the consumed
// start, so identify the boundary by child-stream depth and exact local name.
// Raw XML decoding keeps requiring a complete standalone document, and any
// sibling before the outer end remains visible and is rejected below.
func decodeMelliumMessageFrameBounded(source xml.TokenReader, outer xml.StartElement, budget int) (xmlFrame, error) {
	children := &outerBoundaryTokenReader{source: source, outer: outer.Name}
	bounded, err := newStanzaBudget(children, outer, budget)
	if err != nil {
		return xmlFrame{}, err
	}
	frame, err := decodeMessageFrameBounded(xml.NewTokenDecoder(bounded), budget)
	if err != nil || !children.done {
		return xmlFrame{}, ErrProtocol
	}
	return frame, nil
}

type outerBoundaryTokenReader struct {
	source xml.TokenReader
	outer  xml.Name
	depth  uint64
	done   bool
}

func (reader *outerBoundaryTokenReader) Token() (xml.Token, error) {
	if reader == nil || reader.source == nil || reader.outer.Local == "" {
		return nil, ErrProtocol
	}
	if reader.done {
		return nil, io.EOF
	}
	token, err := reader.source.Token()
	if err != nil {
		return token, err
	}
	switch value := token.(type) {
	case xml.StartElement:
		if reader.depth == ^uint64(0) {
			return nil, ErrProtocol
		}
		reader.depth++
	case xml.EndElement:
		if reader.depth == 0 {
			if value.Name.Local != reader.outer.Local {
				return nil, ErrProtocol
			}
			reader.done = true
			return nil, io.EOF
		}
		reader.depth--
	}
	return token, nil
}

func decodeMessageFrameBounded(decoder *xml.Decoder, budget int) (xmlFrame, error) {
	if decoder == nil || budget <= 0 {
		return xmlFrame{}, ErrProtocol
	}
	var found *xmlFrame
	delayFound := false
	for {
		token, err := decoder.Token()
		if decoder.InputOffset() > int64(budget) {
			return xmlFrame{}, ErrProtocol
		}
		if err == io.EOF {
			if found != nil {
				return *found, nil
			}
			return xmlFrame{}, ErrProtocol
		}
		if err != nil {
			return xmlFrame{}, err
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			if chars, space := token.(xml.CharData); space && strings.TrimSpace(string(chars)) == "" {
				continue
			}
			return xmlFrame{}, ErrProtocol
		}
		if start.Name == (xml.Name{Space: "urn:xmpp:delay", Local: "delay"}) {
			if delayFound || !validXEP0203Delay(decoder, start) {
				return xmlFrame{}, ErrProtocol
			}
			delayFound = true
			continue
		}
		if start.Name != (xml.Name{Space: AZTMNamespaceV1, Local: "frame"}) || found != nil {
			return xmlFrame{}, ErrProtocol
		}
		var frame xmlFrame
		if err = decoder.DecodeElement(&frame, &start); err != nil {
			return xmlFrame{}, err
		}
		if decoder.InputOffset() > int64(budget) {
			return xmlFrame{}, ErrProtocol
		}
		found = &frame
	}
}

// validXEP0203Delay consumes one XEP-0203 delayed-delivery element. Delay is
// transport metadata only: callers use it solely to recognize server replay
// and never surface it in the decoded Cynapsa frame or application payload.
func validXEP0203Delay(decoder *xml.Decoder, start xml.StartElement) bool {
	if decoder == nil || start.Name != (xml.Name{Space: "urn:xmpp:delay", Local: "delay"}) {
		return false
	}
	stamp, from := "", ""
	stampSeen, fromSeen, namespaceSeen := false, false, false
	for _, attribute := range start.Attr {
		if attribute.Name.Space != "" {
			return false
		}
		switch attribute.Name.Local {
		case "xmlns":
			if namespaceSeen || attribute.Value != "urn:xmpp:delay" {
				return false
			}
			namespaceSeen = true
		case "stamp":
			if stampSeen {
				return false
			}
			stamp, stampSeen = attribute.Value, true
		case "from":
			if fromSeen {
				return false
			}
			from, fromSeen = attribute.Value, true
		default:
			return false
		}
	}
	if !stampSeen || !strings.HasSuffix(stamp, "Z") {
		return false
	}
	if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
		return false
	}
	if fromSeen {
		parsed, err := jid.Parse(from)
		if err != nil || parsed.String() != from {
			return false
		}
	}
	for {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		switch value := token.(type) {
		case xml.CharData:
			// XEP-0203 permits optional human-readable delayed-delivery text.
			continue
		case xml.EndElement:
			return value.Name == start.Name
		default:
			return false
		}
	}
}

func (s *melliumSession) emit(ctx context.Context, event Event) error {
	if s == nil || ctx == nil {
		clearEventOwned(&event)
		return ErrInvalidConfig
	}
	control := isControlEvent(event.Kind)
	if event.Kind != EventStanza && !control {
		clearEventOwned(&event)
		_ = s.fenceAuthorityIngress(ctx)
		return ErrProtocol
	}
	s.mu.Lock()
	if s.events == nil {
		capacity := s.config.ReceiveCapacity
		if capacity <= 0 {
			capacity = 1
		}
		s.events = make(chan Event, capacity)
	}
	if s.controlEvents == nil {
		capacity := s.config.ReceiveCapacity
		if capacity <= 0 {
			capacity = 1
		}
		s.controlEvents = make(chan Event, capacity)
	}
	if s.inboundBudget == nil {
		capacity := cap(s.events)
		s.inboundBudget, _ = transport.NewInboundBudget(capacity, transport.MaximumControlFrameBytes)
	}
	if s.controlBudget == nil {
		s.controlBudget, _ = transport.NewInboundBudget(cap(s.controlEvents), transport.MaximumControlFrameBytes)
	}
	dedicatedControl := control && s.controlLaneActive
	budget, target := s.inboundBudget, s.events
	if dedicatedControl {
		budget, target = s.controlBudget, s.controlEvents
	}
	active := s.controlLaneActive
	s.mu.Unlock()
	size := 0
	if event.Kind == EventStanza {
		var ok bool
		size, ok = inboundStanzaBytes(event.Stanza)
		if !ok {
			clearEventOwned(&event)
			return ErrProtocol
		}
	}
	if err := ctx.Err(); err != nil {
		clearEventOwned(&event)
		return err
	}
	admission := ctx
	if active {
		probe, cancel := context.WithCancel(ctx)
		cancel()
		admission = probe
	}
	lease, err := budget.Acquire(admission, size)
	if err != nil {
		clearEventOwned(&event)
		_ = s.fenceAuthorityIngress(ctx)
		if errors.Is(err, transport.ErrProtocol) {
			return ErrProtocol
		}
		return ErrQueueFull
	}
	if event.Kind == EventStanza {
		event.Stanza.inboundLease = lease
	} else {
		event.inboundLease = lease
	}
	if generation, ok := ctx.Value(melliumSessionGenerationKey{}).(uint64); ok {
		event.sessionGeneration = generation
	}
	if active {
		select {
		case target <- event:
			return nil
		default:
			clearEventOwned(&event)
			_ = s.fenceAuthorityIngress(ctx)
			return ErrQueueFull
		}
	}
	select {
	case target <- event:
		return nil
	case <-ctx.Done():
		clearEventOwned(&event)
		return ctx.Err()
	}
}

var _ Dialer = (*MelliumDialer)(nil)
var _ Session = (*melliumSession)(nil)
