package rank2xmpp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/xml"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	externalServiceNamespace    = "urn:xmpp:extdisco:2"
	externalServiceIDPrefix     = "cynapsa-extdisco-"
	externalServiceRandomLength = 26
	maximumExternalServices     = 32
	maximumExternalServiceBytes = 64 << 10
	maximumExternalFieldBytes   = 1024
	externalServiceTimeout      = 30 * time.Second
)

// ExternalService is one ownership-safe server-authoritative STUN/TURN
// endpoint. It is private transport input and never crosses the SDK boundary.
type ExternalService struct {
	Type, Host, Transport string
	Port                  uint16
	Restricted            bool
	Username              string
	Password              []byte
	ExpiresAt             time.Time
}

// ExternalServiceProfile is valid only for the authenticated session and
// exact authenticated session against which Client returned it. ExpiresAt
// is the earliest credential expiry; zero means the accepted services did not
// carry expiring credentials.
type ExternalServiceProfile struct {
	Services  []ExternalService
	ExpiresAt time.Time
}

func (profile ExternalServiceProfile) clone() ExternalServiceProfile {
	if profile.Services != nil {
		profile.Services = append([]ExternalService{}, profile.Services...)
	}
	for i := range profile.Services {
		profile.Services[i].Type = strings.Clone(profile.Services[i].Type)
		profile.Services[i].Host = strings.Clone(profile.Services[i].Host)
		profile.Services[i].Transport = strings.Clone(profile.Services[i].Transport)
		profile.Services[i].Username = strings.Clone(profile.Services[i].Username)
		profile.Services[i].Password = append([]byte(nil), profile.Services[i].Password...)
	}
	return profile
}

func (profile *ExternalServiceProfile) clear() {
	if profile == nil {
		return
	}
	for i := range profile.Services {
		clear(profile.Services[i].Password)
		profile.Services[i].Password = nil
		profile.Services[i] = ExternalService{}
	}
	clear(profile.Services)
	*profile = ExternalServiceProfile{}
}

// Clear releases this caller-owned private profile and overwrites every TURN
// password buffer before dropping its references.
func (profile *ExternalServiceProfile) Clear() {
	profile.clear()
}

type externalServiceCache struct {
	profile            ExternalServiceProfile
	valid              bool
	token              uint64
	lifetimeGeneration uint64
	sessionEpoch       uint64
	expiry             *externalServiceExpiryOwner
}

type externalServiceExpiryOwner struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func retireExternalServiceExpiry(owner *externalServiceExpiryOwner) {
	if owner == nil {
		return
	}
	owner.cancel()
	<-owner.done
}

type externalServiceSession interface {
	QueryExternalServices(context.Context, time.Time) (ExternalServiceProfile, error)
}

// DiscoverExternalServices returns only XEP-0215 data obtained through the
// exact currently authenticated Rank2 session. A cached value is usable only
// while its exact session publication and credential lifetime
// remain exact. Discovery failure never demotes the durable session.
func (c *Client) DiscoverExternalServices(ctx context.Context) (ExternalServiceProfile, error) {
	if c == nil || ctx == nil {
		return ExternalServiceProfile{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return ExternalServiceProfile{}, err
	}
	if err := c.externalQuery.LockContext(ctx); err != nil {
		return ExternalServiceProfile{}, err
	}
	defer c.externalQuery.Unlock()

	c.mu.Lock()
	profile, session, lifetime, lifetimeGeneration, sessionEpoch, retired, err := c.externalServicesLocked()
	c.mu.Unlock()
	retireExternalServiceExpiry(retired)
	if err != nil || profile.Services != nil {
		return profile, err
	}
	source, ok := session.(externalServiceSession)
	if !ok {
		return ExternalServiceProfile{}, ErrUnavailable
	}
	operation, cancel := context.WithTimeout(ctx, externalServiceTimeout)
	stopLifetime := context.AfterFunc(lifetime, cancel)
	defer func() {
		stopLifetime()
		cancel()
	}()
	now := c.clock.Now().UTC()
	if now.IsZero() || now.Location() != time.UTC {
		return ExternalServiceProfile{}, ErrUnavailable
	}
	profile, queryErr := queryExternalServiceSession(source, operation, now)
	if queryErr != nil {
		if stateErr := c.externalServiceSessionError(lifetimeGeneration, sessionEpoch, session); stateErr != nil {
			return ExternalServiceProfile{}, stateErr
		}
		if callerErr := ctx.Err(); callerErr != nil {
			return ExternalServiceProfile{}, callerErr
		}
		if operation.Err() != nil {
			return ExternalServiceProfile{}, ErrUnavailable
		}
		return ExternalServiceProfile{}, queryErr
	}
	now = c.clock.Now().UTC()
	if err = validateExternalServiceProfile(profile, now, true); err != nil {
		profile.clear()
		return ExternalServiceProfile{}, err
	}
	c.mu.Lock()
	if err = c.externalServiceSessionErrorLocked(lifetimeGeneration, sessionEpoch, session); err != nil {
		c.mu.Unlock()
		profile.clear()
		return ExternalServiceProfile{}, err
	}
	if c.external.token == ^uint64(0) {
		c.mu.Unlock()
		profile.clear()
		return ExternalServiceProfile{}, ErrCapacity
	}
	retired = c.clearExternalServicesLocked()
	c.mu.Unlock()
	retireExternalServiceExpiry(retired)
	c.mu.Lock()
	if err = c.externalServiceSessionErrorLocked(lifetimeGeneration, sessionEpoch, session); err != nil {
		c.mu.Unlock()
		profile.clear()
		return ExternalServiceProfile{}, err
	}
	if c.external.token == ^uint64(0) {
		c.mu.Unlock()
		profile.clear()
		return ExternalServiceProfile{}, ErrCapacity
	}
	c.external.token++
	c.external.lifetimeGeneration = lifetimeGeneration
	c.external.sessionEpoch = sessionEpoch
	c.external.profile = profile.clone()
	c.external.valid = true
	c.scheduleExternalServiceExpiryLocked(c.external.token, profile.ExpiresAt)
	result := profile.clone()
	c.mu.Unlock()
	profile.clear()
	return result, nil
}

func (c *Client) externalServicesLocked() (ExternalServiceProfile, Session, context.Context, uint64, uint64, *externalServiceExpiryOwner, error) {
	if c.closed {
		return ExternalServiceProfile{}, nil, nil, 0, 0, nil, ErrClosed
	}
	if !c.started || c.state != DurableLive || !c.membershipReady || c.session == nil || c.ctx == nil {
		return ExternalServiceProfile{}, nil, nil, 0, 0, nil, ErrUnavailable
	}
	now := c.clock.Now().UTC()
	cache := &c.external
	if cache.valid && cache.lifetimeGeneration == c.generation && cache.sessionEpoch == c.sessionEpoch {
		if cache.profile.ExpiresAt.IsZero() || now.Before(cache.profile.ExpiresAt) {
			return cache.profile.clone(), c.session, c.ctx, c.generation, c.sessionEpoch, nil, nil
		}
		retired := c.clearExternalServicesLocked()
		return ExternalServiceProfile{}, c.session, c.ctx, c.generation, c.sessionEpoch, retired, nil
	}
	return ExternalServiceProfile{}, c.session, c.ctx, c.generation, c.sessionEpoch, nil, nil
}

func (c *Client) externalServiceSessionError(lifetimeGeneration, sessionEpoch uint64, session Session) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.externalServiceSessionErrorLocked(lifetimeGeneration, sessionEpoch, session)
}

func (c *Client) externalServiceSessionErrorLocked(lifetimeGeneration, sessionEpoch uint64, session Session) error {
	if c.closed || c.generation != lifetimeGeneration {
		return ErrClosed
	}
	if !c.started || c.state != DurableLive || !c.membershipReady || c.session == nil || c.session != session || c.sessionEpoch != sessionEpoch {
		return ErrUnavailable
	}
	return nil
}

func (c *Client) clearExternalServicesLocked() *externalServiceExpiryOwner {
	if c == nil {
		return nil
	}
	owner := c.external.expiry
	c.external.expiry = nil
	if owner != nil {
		owner.cancel()
	}
	c.clearExternalServiceProfileLocked()
	return owner
}

func (c *Client) clearExternalServiceProfileLocked() {
	c.external.profile.clear()
	c.external.valid = false
	c.external.lifetimeGeneration = 0
	c.external.sessionEpoch = 0
}

func (c *Client) scheduleExternalServiceExpiryLocked(token uint64, expires time.Time) {
	if expires.IsZero() || c.ctx == nil {
		return
	}
	delay := expires.Sub(c.clock.Now().UTC())
	if delay <= 0 {
		c.clearExternalServiceProfileLocked()
		return
	}
	lifetime, cancel := context.WithCancel(c.ctx)
	owner := &externalServiceExpiryOwner{cancel: cancel, done: make(chan struct{})}
	c.external.expiry = owner
	c.wg.Add(1)
	go func(owner *externalServiceExpiryOwner) {
		defer c.wg.Done()
		finished := false
		finish := func() {
			if !finished {
				close(owner.done)
				finished = true
			}
		}
		defer finish()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			c.mu.Lock()
			if c.external.expiry == owner && c.external.token == token && !c.external.profile.ExpiresAt.IsZero() && !c.clock.Now().UTC().Before(c.external.profile.ExpiresAt) {
				c.external.expiry = nil
				c.clearExternalServiceProfileLocked()
				// Detaching the owner is the expiry linearization point. Signal
				// completion before releasing c.mu so a replacement that observes
				// the nil owner cannot return ahead of the retired owner.
				finish()
			}
			c.mu.Unlock()
		case <-lifetime.Done():
		}
	}(owner)
}

// QueryExternalServices performs one bounded XEP-0215 discovery transaction
// on an already TLS/SASL/bind/XEP-0198 authenticated session. Restricted TURN
// entries without inline credentials use the standard correlated credentials
// follow-up under the same caller deadline and write ownership.
func (s *melliumSession) QueryExternalServices(ctx context.Context, now time.Time) (ExternalServiceProfile, error) {
	if s == nil || ctx == nil || now.IsZero() || now.Location() != time.UTC {
		return ExternalServiceProfile{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return ExternalServiceProfile{}, err
	}
	if err := s.acquireCorrelated(ctx); err != nil {
		return ExternalServiceProfile{}, err
	}
	defer s.releaseCorrelated()

	s.mu.Lock()
	session, management, closed, suspended := s.session, s.management, s.closed, s.suspended
	s.mu.Unlock()
	if closed || suspended || session == nil || management == nil {
		return ExternalServiceProfile{}, ErrUnavailable
	}
	local := session.LocalAddr()
	server := local.Domain()
	if local.Localpart() == "" || local.Resourcepart() == "" || server.Domainpart() == "" || local.Resourcepart() != s.boundResource() {
		return ExternalServiceProfile{}, ErrIdentityBinding
	}
	services, err := s.queryExternalServiceIQ(ctx, session, management, local, server, "services", ExternalService{}, now)
	if err != nil {
		return ExternalServiceProfile{}, err
	}
	services, err = completeExternalServiceCredentials(ctx, services, now, func(operation context.Context, requested ExternalService, queryNow time.Time) ([]ExternalService, error) {
		return s.queryExternalServiceIQ(operation, session, management, local, server, "credentials", requested, queryNow)
	})
	if err != nil {
		return ExternalServiceProfile{}, err
	}
	profile := ExternalServiceProfile{Services: services}
	for _, service := range profile.Services {
		if !service.ExpiresAt.IsZero() && (profile.ExpiresAt.IsZero() || service.ExpiresAt.Before(profile.ExpiresAt)) {
			profile.ExpiresAt = service.ExpiresAt
		}
	}
	if err := validateExternalServiceProfile(profile, now, true); err != nil {
		profile.clear()
		return ExternalServiceProfile{}, err
	}
	return profile, nil
}

type externalCredentialQuery func(context.Context, ExternalService, time.Time) ([]ExternalService, error)

func completeExternalServiceCredentials(ctx context.Context, services []ExternalService, now time.Time, query externalCredentialQuery) (result []ExternalService, err error) {
	succeeded := false
	defer func() {
		if !succeeded {
			clearExternalServices(services)
		}
	}()
	if ctx == nil || now.IsZero() || now.Location() != time.UTC || query == nil {
		return nil, ErrInvalidConfig
	}
	for i := range services {
		service := &services[i]
		if !service.Restricted || service.Type != "turn" && service.Type != "turns" || service.Username != "" && len(service.Password) != 0 {
			continue
		}
		if err = completeExternalServiceCredential(ctx, services, service, now, query); err != nil {
			return nil, err
		}
	}
	succeeded = true
	return services, nil
}

// completeExternalServiceCredential borrows services and service. The query
// callback transfers ownership only if it returns a credential slice; that
// slice is cleared on every exit. A callback panic is deliberately allowed to
// unwind so the outer owner can normalize it, while both this returned slice
// (when one was handed off) and completeExternalServiceCredentials' input are
// still cleared by their deferred owners.
func completeExternalServiceCredential(ctx context.Context, services []ExternalService, service *ExternalService, now time.Time, query externalCredentialQuery) (err error) {
	var credentials []ExternalService
	defer func() { clearExternalServices(credentials) }()
	credentials, err = query(ctx, *service, now)
	if err != nil {
		return err
	}
	if externalServiceSlicesOverlap(services, credentials) {
		return ErrProtocol
	}
	match := -1
	for candidate := range credentials {
		if sameExternalServiceIdentity(*service, credentials[candidate]) {
			if match >= 0 {
				return ErrProtocol
			}
			match = candidate
		}
	}
	if match < 0 {
		return ErrUnavailable
	}
	service.Username = credentials[match].Username
	service.Password = append([]byte(nil), credentials[match].Password...)
	service.ExpiresAt = credentials[match].ExpiresAt
	return nil
}

// queryExternalServiceSession contains the optional dependency boundary. A
// returned profile transfers ownership to this wrapper even when accompanied
// by an error; a panic hands off nothing and is normalized without touching
// dependency-private storage.
func queryExternalServiceSession(source externalServiceSession, ctx context.Context, now time.Time) (profile ExternalServiceProfile, err error) {
	defer func() {
		if recover() != nil {
			profile.clear()
			profile = ExternalServiceProfile{}
			err = ErrUnavailable
		}
	}()
	profile, err = source.QueryExternalServices(ctx, now)
	if err != nil {
		profile.clear()
		profile = ExternalServiceProfile{}
	}
	return profile, err
}

func (s *melliumSession) queryExternalServiceIQ(ctx context.Context, session *xmpp.Session, management *StreamManagement, local, server jid.JID, root string, requested ExternalService, now time.Time) ([]ExternalService, error) {
	random := rand.Text()
	if len(random) < externalServiceRandomLength {
		return nil, ErrProtocol
	}
	id := externalServiceIDPrefix + random[:externalServiceRandomLength]
	encodedQuery, err := encodeExternalServiceQuery(root, requested)
	if err != nil {
		return nil, err
	}
	defer clear(encodedQuery)
	record := Stanza{Kind: StanzaExternalServiceQuery, From: local.String(), To: server.String(), MeshID: s.meshID, AttemptID: root, MessageID: id, Data: encodedQuery}
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: id, To: server, Type: stanza.GetIQ}
	payload := xml.NewDecoder(bytes.NewReader(encodedQuery))
	response, sequence, err := s.sendTrackedIQElement(ctx, session, management, record, payload, iq)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, ErrProtocol
	}
	defer response.Close()
	services, correlated, handled, decodeErr := decodeMelliumExternalServices(response, id, server, local, root, requested, now)
	if correlated {
		ordinal, count, confirmErr := management.ConfirmCorrelatedHandled(sequence)
		if confirmErr != nil {
			clearExternalServices(services)
			return nil, confirmErr
		}
		if count > 0 {
			if emitErr := s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count}); emitErr != nil {
				clearExternalServices(services)
				return nil, emitErr
			}
		}
	}
	if handled {
		management.MarkHandledInbound()
	}
	if decodeErr != nil {
		clearExternalServices(services)
		return nil, decodeErr
	}
	return services, nil
}

// encodeExternalServiceQuery retains the exact bounded XEP-0215 request body
// needed if XEP-0198 resumes while the correlated operation is in flight.
// The descriptor contains endpoint identity only; credentials exist solely in
// the server response and are never retained here.
func encodeExternalServiceQuery(root string, requested ExternalService) ([]byte, error) {
	if root != "services" && root != "credentials" {
		return nil, ErrProtocol
	}
	var output bytes.Buffer
	encoder := xml.NewEncoder(&output)
	start := xml.StartElement{Name: xml.Name{Space: externalServiceNamespace, Local: root}}
	if err := encoder.EncodeToken(start); err != nil {
		return nil, err
	}
	if root == "credentials" {
		if requested.Type == "" || requested.Host == "" || requested.Port == 0 || requested.Transport == "" || !requested.Restricted {
			return nil, ErrProtocol
		}
		child := xml.StartElement{Name: xml.Name{Space: externalServiceNamespace, Local: "service"}, Attr: []xml.Attr{
			{Name: xml.Name{Local: "host"}, Value: requested.Host},
			{Name: xml.Name{Local: "type"}, Value: requested.Type},
			{Name: xml.Name{Local: "port"}, Value: strconv.Itoa(int(requested.Port))},
			{Name: xml.Name{Local: "transport"}, Value: requested.Transport},
		}}
		if err := encoder.EncodeToken(child); err != nil {
			return nil, err
		}
		if err := encoder.EncodeToken(child.End()); err != nil {
			return nil, err
		}
	}
	if err := encoder.EncodeToken(start.End()); err != nil {
		return nil, err
	}
	if err := encoder.Flush(); err != nil {
		return nil, err
	}
	if output.Len() == 0 || output.Len() > maximumExternalServiceBytes {
		return nil, ErrProtocol
	}
	return append([]byte(nil), output.Bytes()...), nil
}

func validExternalServiceQueryReplay(record Stanza, local, server, meshID string) bool {
	if record.Kind != StanzaExternalServiceQuery || record.From != local || record.To != server || record.MeshID != meshID ||
		(record.AttemptID != "services" && record.AttemptID != "credentials") ||
		!validExternalServiceID(record.MessageID) ||
		record.Ordinal != 0 || record.TransferID != "" || len(record.Data) == 0 || len(record.Data) > maximumExternalServiceBytes ||
		record.Evidence.TransferID != "" || record.Evidence.MessageID != "" || record.Evidence.Digest != [32]byte{} {
		return false
	}
	from, fromErr := jid.Parse(record.From)
	to, toErr := jid.Parse(record.To)
	if fromErr != nil || toErr != nil || from.Resourcepart() == "" || to.Localpart() != "" || to.Resourcepart() != "" || from.Domainpart() != to.Domainpart() {
		return false
	}
	expected, err := encodeExternalServiceQuery(record.AttemptID, externalServiceQueryRequested(record))
	if err != nil {
		return false
	}
	defer clear(expected)
	return bytes.Equal(record.Data, expected)
}

func validExternalServiceID(id string) bool {
	if !strings.HasPrefix(id, externalServiceIDPrefix) || len(id) != len(externalServiceIDPrefix)+externalServiceRandomLength {
		return false
	}
	for _, value := range id[len(externalServiceIDPrefix):] {
		if value < '2' || value > '7' && (value < 'A' || value > 'Z') {
			return false
		}
	}
	return true
}

func externalServiceQueryRequested(record Stanza) ExternalService {
	if record.AttemptID != "credentials" {
		return ExternalService{}
	}
	var wire struct {
		XMLName xml.Name
		Service struct {
			XMLName   xml.Name
			Host      string `xml:"host,attr"`
			Type      string `xml:"type,attr"`
			Port      string `xml:"port,attr"`
			Transport string `xml:"transport,attr"`
		} `xml:"service"`
	}
	if xml.Unmarshal(record.Data, &wire) != nil || wire.XMLName != (xml.Name{Space: externalServiceNamespace, Local: "credentials"}) ||
		wire.Service.XMLName != (xml.Name{Space: externalServiceNamespace, Local: "service"}) {
		return ExternalService{}
	}
	port, ok := parseCanonicalUint(wire.Service.Port, 65535, false)
	if !ok || port == 0 {
		return ExternalService{}
	}
	return ExternalService{Type: wire.Service.Type, Host: wire.Service.Host, Port: uint16(port), Transport: wire.Service.Transport, Restricted: true}
}

type externalServicesXML struct {
	XMLName      xml.Name             `xml:""`
	Services     []externalServiceXML `xml:"urn:xmpp:extdisco:2 service"`
	Text         string               `xml:",chardata"`
	Unknown      []xmlUnknown         `xml:",any"`
	UnknownAttrs []xml.Attr           `xml:",any,attr"`
}

type externalServiceXML struct {
	XMLName      xml.Name     `xml:"urn:xmpp:extdisco:2 service"`
	Type         string       `xml:"type,attr"`
	Host         string       `xml:"host,attr"`
	Port         string       `xml:"port,attr"`
	Transport    string       `xml:"transport,attr"`
	Restricted   string       `xml:"restricted,attr"`
	Username     string       `xml:"username,attr"`
	Password     string       `xml:"password,attr"`
	Expires      string       `xml:"expires,attr"`
	Name         string       `xml:"name,attr"`
	Action       string       `xml:"action,attr"`
	Text         string       `xml:",chardata"`
	Unknown      []xmlUnknown `xml:",any"`
	UnknownAttrs []xml.Attr   `xml:",any,attr"`
}

func decodeMelliumExternalServices(source xml.TokenReader, expectedID string, expectedFrom, expectedTo jid.JID, root string, requested ExternalService, now time.Time) ([]ExternalService, bool, bool, error) {
	return decodeExternalServices(source, expectedID, expectedFrom, expectedTo, root, requested, now, true)
}

func decodeCorrelatedExternalServices(source xml.TokenReader, expectedID string, expectedFrom, expectedTo jid.JID, root string, requested ExternalService, now time.Time) ([]ExternalService, bool, bool, error) {
	return decodeExternalServices(source, expectedID, expectedFrom, expectedTo, root, requested, now, false)
}

func decodeExternalServices(source xml.TokenReader, expectedID string, expectedFrom, expectedTo jid.JID, root string, requested ExternalService, now time.Time, melliumFraming bool) ([]ExternalService, bool, bool, error) {
	if source == nil || expectedID == "" || expectedFrom.String() == "" || expectedTo.String() == "" || root != "services" && root != "credentials" || now.IsZero() || now.Location() != time.UTC {
		return nil, false, false, ErrProtocol
	}
	token, err := source.Token()
	if err != nil {
		return nil, false, false, ErrProtocol
	}
	outer, ok := token.(xml.StartElement)
	if !ok || outer.Name.Local != "iq" || outer.Name.Space != "" && outer.Name.Space != stanza.NSClient {
		return nil, false, false, ErrProtocol
	}
	iqType, valid := validUploadIQAttrs(outer.Attr, expectedID, expectedFrom, expectedTo)
	if !valid {
		return nil, false, false, ErrProtocol
	}
	correlated := true
	bounded, err := newStanzaBudget(source, outer, maximumExternalServiceBytes)
	if err != nil {
		return nil, correlated, false, ErrProtocol
	}
	if iqType == string(stanza.ErrorIQ) {
		if !consumeUploadError(bounded, outer.End(), melliumFraming) {
			return nil, correlated, false, ErrProtocol
		}
		return nil, correlated, true, ErrUnavailable
	}
	children := &uploadChildReader{source: bounded, outer: outer.Name, melliumFraming: melliumFraming}
	decoder := xml.NewTokenDecoder(children)
	token, err = decoder.Token()
	if err != nil {
		return nil, correlated, false, ErrProtocol
	}
	start, ok := token.(xml.StartElement)
	if !ok || start.Name != (xml.Name{Space: externalServiceNamespace, Local: root}) {
		return nil, correlated, false, ErrProtocol
	}
	var wire externalServicesXML
	defer wire.clear()
	if err = decoder.DecodeElement(&wire, &start); err != nil {
		return nil, correlated, false, ErrProtocol
	}
	if token, err = decoder.Token(); token != nil || err != io.EOF || !children.complete {
		return nil, correlated, false, ErrProtocol
	}
	services, err := validateExternalServiceXML(wire, root, requested, now)
	if err != nil {
		return nil, correlated, true, err
	}
	return services, correlated, true, nil
}

func validateExternalServiceXML(wire externalServicesXML, root string, requested ExternalService, now time.Time) ([]ExternalService, error) {
	if wire.XMLName != (xml.Name{Space: externalServiceNamespace, Local: root}) || len(wire.Services) > maximumExternalServices || len(wire.Unknown) != 0 || !onlyNamespaceAttr(wire.UnknownAttrs, externalServiceNamespace) || strings.TrimSpace(wire.Text) != "" {
		return nil, ErrProtocol
	}
	if root == "credentials" && len(wire.Services) != 1 {
		return nil, ErrProtocol
	}
	services := make([]ExternalService, 0, len(wire.Services))
	for i := range wire.Services {
		service, supported, err := parseExternalService(&wire.Services[i], root, requested, now)
		if err != nil {
			clearExternalServices(services)
			return nil, err
		}
		if supported {
			services = append(services, service)
		}
	}
	// An authenticated empty services result is an authoritative instruction
	// to use host candidates only. Keep a non-nil empty slice so callers can
	// distinguish that successful result from a cache miss.
	sort.Slice(services, func(i, j int) bool { return externalServiceKey(services[i]) < externalServiceKey(services[j]) })
	for i := 1; i < len(services); i++ {
		if externalServiceKey(services[i-1]) == externalServiceKey(services[i]) {
			clearExternalServices(services)
			return nil, ErrProtocol
		}
	}
	if root == "credentials" && len(services) != 1 {
		clearExternalServices(services)
		return nil, ErrProtocol
	}
	return services, nil
}

func parseExternalService(value *externalServiceXML, root string, requested ExternalService, now time.Time) (ExternalService, bool, error) {
	if value == nil {
		return ExternalService{}, false, ErrProtocol
	}
	if value.XMLName != (xml.Name{Space: externalServiceNamespace, Local: "service"}) || len(value.Unknown) != 0 || !onlyNamespaceAttr(value.UnknownAttrs, externalServiceNamespace) || strings.TrimSpace(value.Text) != "" || value.Action != "" || !validExternalText(value.Name, maximumExternalFieldBytes, true) {
		return ExternalService{}, false, ErrProtocol
	}
	for _, field := range []string{value.Type, value.Host, value.Port, value.Transport, value.Restricted, value.Username, value.Expires} {
		if !validExternalText(field, maximumExternalFieldBytes, true) {
			return ExternalService{}, false, ErrProtocol
		}
	}
	if !validExternalText(value.Password, maximumExternalFieldBytes, true) {
		value.Password = ""
		return ExternalService{}, false, ErrProtocol
	}
	if value.Type != "stun" && value.Type != "stuns" && value.Type != "turn" && value.Type != "turns" {
		return ExternalService{}, false, nil
	}
	// encoding/xml necessarily materializes attribute values as immutable
	// strings. Convert the password once into owned, clearable storage and drop
	// the decoder-owned string reference before any validation can return.
	password := []byte(value.Password)
	value.Password = ""
	service := ExternalService{Type: value.Type, Host: value.Host, Username: value.Username, Password: password}
	succeeded := false
	defer func() {
		if !succeeded {
			clear(service.Password)
		}
	}()
	if !validExternalHost(service.Host) {
		return ExternalService{}, false, ErrProtocol
	}
	defaultPort, defaultTransport := uint16(3478), "udp"
	if service.Type == "stuns" || service.Type == "turns" {
		defaultPort, defaultTransport = 5349, "tcp"
	}
	service.Port = defaultPort
	if value.Port != "" {
		port, ok := parseCanonicalUint(value.Port, 65535, false)
		if !ok || port == 0 {
			return ExternalService{}, false, ErrProtocol
		}
		service.Port = uint16(port)
	}
	service.Transport = defaultTransport
	if value.Transport != "" {
		service.Transport = value.Transport
	}
	if service.Transport != "udp" && service.Transport != "tcp" || (service.Type == "stuns" || service.Type == "turns") && service.Transport != "tcp" {
		return ExternalService{}, false, ErrProtocol
	}
	switch value.Restricted {
	case "", "false":
	case "true":
		service.Restricted = true
	default:
		return ExternalService{}, false, ErrProtocol
	}
	if value.Expires != "" {
		expires, err := time.Parse(time.RFC3339Nano, value.Expires)
		if err != nil || !strings.HasSuffix(value.Expires, "Z") || expires.Location() != time.UTC || !now.Before(expires) {
			return ExternalService{}, false, ErrProtocol
		}
		service.ExpiresAt = expires
	}
	if root == "credentials" {
		if requested.Type == "" || !requested.Restricted || service.Host != requested.Host || service.Type != requested.Type || value.Restricted == "false" {
			return ExternalService{}, false, ErrProtocol
		}
		if value.Port == "" {
			service.Port = requested.Port
		}
		if value.Transport == "" {
			service.Transport = requested.Transport
		}
		service.Restricted = true
		if !sameExternalServiceIdentity(requested, service) {
			return ExternalService{}, false, ErrProtocol
		}
		if service.Username == "" || len(service.Password) == 0 || service.ExpiresAt.IsZero() {
			return ExternalService{}, false, ErrProtocol
		}
	}
	if service.Type == "stun" || service.Type == "stuns" {
		if service.Restricted || service.Username != "" || len(service.Password) != 0 || !service.ExpiresAt.IsZero() {
			return ExternalService{}, false, ErrProtocol
		}
	} else if service.Restricted {
		if (service.Username == "") != (len(service.Password) == 0) {
			return ExternalService{}, false, ErrProtocol
		}
		if service.Username != "" && service.ExpiresAt.IsZero() {
			return ExternalService{}, false, ErrProtocol
		}
	} else if service.Username != "" || len(service.Password) != 0 || !service.ExpiresAt.IsZero() {
		return ExternalService{}, false, ErrProtocol
	}
	succeeded = true
	return service, true, nil
}

func validateExternalServiceProfile(profile ExternalServiceProfile, now time.Time, requireCredentials bool) error {
	if now.IsZero() || now.Location() != time.UTC || profile.Services == nil || len(profile.Services) > maximumExternalServices {
		return ErrProtocol
	}
	var earliest time.Time
	for _, service := range profile.Services {
		if service.Type != "stun" && service.Type != "stuns" && service.Type != "turn" && service.Type != "turns" || !validExternalHost(service.Host) || service.Port == 0 || service.Transport != "udp" && service.Transport != "tcp" || (service.Type == "stuns" || service.Type == "turns") && service.Transport != "tcp" || !validExternalText(service.Username, maximumExternalFieldBytes, true) || !validExternalBytes(service.Password, maximumExternalFieldBytes, true) {
			return ErrProtocol
		}
		if service.Type == "stun" || service.Type == "stuns" {
			if service.Restricted || service.Username != "" || len(service.Password) != 0 || !service.ExpiresAt.IsZero() {
				return ErrProtocol
			}
		} else if !service.Restricted && (service.Username != "" || len(service.Password) != 0 || !service.ExpiresAt.IsZero()) {
			return ErrProtocol
		}
		if service.Restricted && (service.Type != "turn" && service.Type != "turns" || requireCredentials && (service.Username == "" || len(service.Password) == 0 || service.ExpiresAt.IsZero()) || !service.ExpiresAt.IsZero() && !now.Before(service.ExpiresAt)) {
			return ErrProtocol
		}
		if !service.ExpiresAt.IsZero() && (earliest.IsZero() || service.ExpiresAt.Before(earliest)) {
			earliest = service.ExpiresAt
		}
	}
	if !profile.ExpiresAt.Equal(earliest) || !profile.ExpiresAt.IsZero() && (!now.Before(profile.ExpiresAt) || profile.ExpiresAt.Location() != time.UTC) {
		return ErrProtocol
	}
	return nil
}

func validExternalHost(host string) bool {
	if host == "" || len(host) > 253 || strings.ToLower(host) != host || strings.ContainsAny(host, "\x00\r\n\t /[]@") || !utf8.ValidString(host) {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String() == host
	}
	if strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if char < 'a' || char > 'z' {
				if char < '0' || char > '9' {
					if char != '-' {
						return false
					}
				}
			}
		}
	}
	return true
}

func validExternalText(value string, maximum int, emptyOK bool) bool {
	if len(value) > maximum || !utf8.ValidString(value) || !emptyOK && value == "" {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func validExternalBytes(value []byte, maximum int, emptyOK bool) bool {
	if len(value) > maximum || !utf8.Valid(value) || !emptyOK && len(value) == 0 {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func externalServiceKey(service ExternalService) string {
	return service.Type + "\x00" + service.Host + "\x00" + strconv.Itoa(int(service.Port)) + "\x00" + service.Transport
}

func sameExternalServiceIdentity(left, right ExternalService) bool {
	return left.Type == right.Type && left.Host == right.Host && left.Port == right.Port && left.Transport == right.Transport
}

func externalServiceSlicesOverlap(left, right []ExternalService) bool {
	for i := range left {
		for j := range right {
			if &left[i] == &right[j] {
				return true
			}
		}
	}
	return false
}

func clearExternalServices(services []ExternalService) {
	for i := range services {
		clear(services[i].Password)
		services[i].Password = nil
		services[i] = ExternalService{}
	}
	clear(services)
}

func (wire *externalServicesXML) clear() {
	if wire == nil {
		return
	}
	for i := range wire.Services {
		wire.Services[i].Password = ""
	}
}

var _ externalServiceSession = (*melliumSession)(nil)
