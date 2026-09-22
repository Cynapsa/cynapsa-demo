package rank2xmpp

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"mellium.im/xmpp/jid"
)

func TestExternalServicesStrictInlineCredentials(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	from, _ := jid.Parse("mesh.test")
	to, _ := jid.Parse("agent@mesh.test/mesh-1")
	response := `<iq xmlns='jabber:client' id='external-id' type='result' from='mesh.test' to='agent@mesh.test/mesh-1'><services xmlns='urn:xmpp:extdisco:2'><service host='stun.test' port='3478' transport='udp' type='stun'/><service expires='2030-01-02T03:05:05Z' host='turn.test' password='secret' port='3478' restricted='true' transport='udp' type='turn' username='agent'/></services></iq>`
	services, correlated, handled, err := decodeCorrelatedExternalServices(xml.NewDecoder(strings.NewReader(response)), "external-id", from, to, "services", ExternalService{}, now)
	if err != nil || !correlated || !handled || len(services) != 2 {
		t.Fatalf("decode = (%#v,%v,%v,%v)", services, correlated, handled, err)
	}
	if services[0].Type != "stun" || services[1].Username != "agent" || !bytes.Equal(services[1].Password, []byte("secret")) || !services[1].ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("services = %#v", services)
	}
}

func TestExternalServicesAcceptAuthoritativeEmptyProfile(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	from, _ := jid.Parse("mesh.test")
	to, _ := jid.Parse("agent@mesh.test/mesh-1")
	response := `<iq xmlns='jabber:client' id='external-id' type='result' from='mesh.test' to='agent@mesh.test/mesh-1'><services xmlns='urn:xmpp:extdisco:2'/></iq>`
	services, correlated, handled, err := decodeCorrelatedExternalServices(xml.NewDecoder(strings.NewReader(response)), "external-id", from, to, "services", ExternalService{}, now)
	if err != nil || !correlated || !handled || services == nil || len(services) != 0 {
		t.Fatalf("decode = (%#v,%v,%v,%v)", services, correlated, handled, err)
	}
	if err = validateExternalServiceProfile(ExternalServiceProfile{Services: services}, now, true); err != nil {
		t.Fatalf("profile=%v", err)
	}
}

func TestExternalServicesCredentialResponseInheritsRequestedIdentity(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	from, _ := jid.Parse("mesh.test")
	to, _ := jid.Parse("agent@mesh.test/mesh-1")
	requested := ExternalService{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true}
	response := `<iq xmlns='jabber:client' id='credential-id' type='result' from='mesh.test' to='agent@mesh.test/mesh-1'><credentials xmlns='urn:xmpp:extdisco:2'><service expires='2030-01-02T03:05:05Z' host='turn.test' password='secret' type='turn' username='agent'/></credentials></iq>`
	services, correlated, handled, err := decodeCorrelatedExternalServices(xml.NewDecoder(strings.NewReader(response)), "credential-id", from, to, "credentials", requested, now)
	if err != nil || !correlated || !handled || len(services) != 1 || services[0].Port != 3478 || services[0].Transport != "udp" || !services[0].Restricted {
		t.Fatalf("decode = (%#v,%v,%v,%v)", services, correlated, handled, err)
	}
}

func TestExternalServicesCredentialResponseShapeMatrix(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	from, _ := jid.Parse("mesh.test")
	to, _ := jid.Parse("agent@mesh.test/mesh-1")
	requested := ExternalService{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true}
	tests := []struct {
		name, service string
		requested     ExternalService
		accept        bool
	}{
		{name: "omitted restricted", service: `<service expires='2030-01-02T03:05:05Z' host='turn.test' password='secret' type='turn' username='agent'/>`, requested: requested, accept: true},
		{name: "explicit restricted", service: `<service expires='2030-01-02T03:05:05Z' host='turn.test' password='secret' restricted='true' type='turn' username='agent'/>`, requested: requested, accept: true},
		{name: "explicit unrestricted credentials", service: `<service expires='2030-01-02T03:05:05Z' host='turn.test' password='secret' restricted='false' type='turn' username='agent'/>`, requested: requested},
		{name: "explicit unrestricted expiry", service: `<service expires='2030-01-02T03:05:05Z' host='turn.test' restricted='false' type='turn'/>`, requested: requested},
		{name: "missing password", service: `<service expires='2030-01-02T03:05:05Z' host='turn.test' type='turn' username='agent'/>`, requested: requested},
		{name: "missing expiry", service: `<service host='turn.test' password='secret' type='turn' username='agent'/>`, requested: requested},
		{name: "wrong host", service: `<service expires='2030-01-02T03:05:05Z' host='other.test' password='secret' type='turn' username='agent'/>`, requested: requested},
		{name: "wrong port", service: `<service expires='2030-01-02T03:05:05Z' host='turn.test' password='secret' port='3479' type='turn' username='agent'/>`, requested: requested},
		{name: "wrong transport", service: `<service expires='2030-01-02T03:05:05Z' host='turn.test' password='secret' transport='tcp' type='turn' username='agent'/>`, requested: requested},
		{name: "unrestricted request", service: `<service expires='2030-01-02T03:05:05Z' host='turn.test' password='secret' type='turn' username='agent'/>`, requested: ExternalService{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := `<iq xmlns='jabber:client' id='credential-id' type='result' from='mesh.test' to='agent@mesh.test/mesh-1'><credentials xmlns='urn:xmpp:extdisco:2'>` + test.service + `</credentials></iq>`
			services, correlated, handled, err := decodeCorrelatedExternalServices(xml.NewDecoder(strings.NewReader(response)), "credential-id", from, to, "credentials", test.requested, now)
			if !correlated || !handled {
				t.Fatalf("correlated=%t handled=%t err=%v", correlated, handled, err)
			}
			if test.accept {
				if err != nil || len(services) != 1 || !services[0].Restricted || services[0].Username != "agent" || !bytes.Equal(services[0].Password, []byte("secret")) {
					t.Fatalf("accepted shape=%#v err=%v", services, err)
				}
				return
			}
			if !errors.Is(err, ErrProtocol) || len(services) != 0 {
				t.Fatalf("invalid shape services=%#v err=%v", services, err)
			}
		})
	}
}

func TestExternalServicesPerformBoundedCredentialFollowup(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	expires := now.Add(time.Minute)
	services := []ExternalService{
		{Type: "stun", Host: "stun.test", Port: 3478, Transport: "udp"},
		{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true},
		{Type: "turn", Host: "turn.test", Port: 3478, Transport: "tcp", Restricted: true, Username: "inline", Password: []byte("inline-secret"), ExpiresAt: expires},
	}
	queries := 0
	completed, err := completeExternalServiceCredentials(context.Background(), services, now, func(_ context.Context, requested ExternalService, queryNow time.Time) ([]ExternalService, error) {
		queries++
		if requested.Type != "turn" || requested.Host != "turn.test" || requested.Port != 3478 || requested.Transport != "udp" || !queryNow.Equal(now) {
			t.Fatalf("requested=%#v now=%v", requested, queryNow)
		}
		return []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "followup", Password: []byte("followup-secret"), ExpiresAt: expires}}, nil
	})
	if err != nil || queries != 1 || completed[1].Username != "followup" || completed[2].Username != "inline" {
		t.Fatalf("completed=%#v queries=%d err=%v", completed, queries, err)
	}
}

func TestExternalServicesRejectAmbiguousCredentialFollowup(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	expires := now.Add(time.Minute)
	service := ExternalService{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true}
	credentials := ExternalService{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("secret"), ExpiresAt: expires}
	if _, err := completeExternalServiceCredentials(context.Background(), []ExternalService{service}, now, func(context.Context, ExternalService, time.Time) ([]ExternalService, error) {
		return []ExternalService{credentials, credentials}, nil
	}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("ambiguous followup=%v", err)
	}
}

func TestExternalServiceCredentialFollowupDoesNotRetainAliasedQueryStorage(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	expires := now.Add(time.Minute)
	shared := []byte("shared-query-secret")
	completed, err := completeExternalServiceCredentials(context.Background(), []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true}}, now, func(context.Context, ExternalService, time.Time) ([]ExternalService, error) {
		return []ExternalService{
			{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: shared, ExpiresAt: expires},
			{Type: "turn", Host: "other.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: shared, ExpiresAt: expires},
		}, nil
	})
	if err != nil || len(completed) != 1 || !bytes.Equal(completed[0].Password, []byte("shared-query-secret")) {
		t.Fatalf("completed=%#v err=%v", completed, err)
	}
	if !allZero(shared) {
		t.Fatalf("query-owned alias retained=%x", shared)
	}
	completed[0].Password[0] = 'S'
	if !allZero(shared) {
		t.Fatal("completed password aliases cleared query storage")
	}
	clearExternalServices(completed)
}

func TestExternalServicesRejectMalformedSecurityFields(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	from, _ := jid.Parse("mesh.test")
	to, _ := jid.Parse("agent@mesh.test/mesh-1")
	services := []string{
		`<service host='TURN.test' port='3478' transport='udp' type='turn'/>`,
		`<service host='turn.test' port='0' transport='udp' type='turn'/>`,
		`<service host='turn.test' port='3478' restricted='1' transport='udp' type='turn'/>`,
		`<service host='turn.test' password='secret' port='3478' restricted='true' transport='udp' type='turn' username='agent'/>`,
		`<service expires='2030-01-02T03:04:05Z' host='turn.test' password='secret' port='3478' restricted='true' transport='udp' type='turn' username='agent'/>`,
		`<service host='turn.test' port='5349' transport='udp' type='turns'/>`,
		`<service extra='x' host='stun.test' port='3478' transport='udp' type='stun'/>`,
	}
	for index, service := range services {
		response := `<iq xmlns='jabber:client' id='external-id' type='result' from='mesh.test' to='agent@mesh.test/mesh-1'><services xmlns='urn:xmpp:extdisco:2'>` + service + `</services></iq>`
		_, correlated, handled, err := decodeCorrelatedExternalServices(xml.NewDecoder(strings.NewReader(response)), "external-id", from, to, "services", ExternalService{}, now)
		if err == nil || !correlated || !handled {
			t.Fatalf("case %d accepted: correlated=%v handled=%v err=%v", index, correlated, handled, err)
		}
	}
}

func TestExternalServicesRejectWrongCorrelationAndOversize(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	from, _ := jid.Parse("mesh.test")
	to, _ := jid.Parse("agent@mesh.test/mesh-1")
	wrong := `<iq xmlns='jabber:client' id='wrong' type='result' from='mesh.test' to='agent@mesh.test/mesh-1'><services xmlns='urn:xmpp:extdisco:2'><service host='stun.test' type='stun'/></services></iq>`
	if _, correlated, handled, err := decodeCorrelatedExternalServices(xml.NewDecoder(strings.NewReader(wrong)), "external-id", from, to, "services", ExternalService{}, now); err == nil || correlated || handled {
		t.Fatalf("wrong correlation = %v,%v,%v", correlated, handled, err)
	}
	oversize := `<iq xmlns='jabber:client' id='external-id' type='result' from='mesh.test' to='agent@mesh.test/mesh-1'><services xmlns='urn:xmpp:extdisco:2'>` + strings.Repeat(" ", maximumExternalServiceBytes) + `<service host='stun.test' type='stun'/></services></iq>`
	if _, correlated, _, err := decodeCorrelatedExternalServices(xml.NewDecoder(strings.NewReader(oversize)), "external-id", from, to, "services", ExternalService{}, now); err == nil || !correlated {
		t.Fatalf("oversize = correlated=%v err=%v", correlated, err)
	}
}

func FuzzExternalServicesStrictDecoder(f *testing.F) {
	f.Add([]byte(`<iq xmlns='jabber:client' id='external-id' type='result' from='mesh.test' to='agent@mesh.test/mesh-1'><services xmlns='urn:xmpp:extdisco:2'><service host='stun.test' port='3478' transport='udp' type='stun'/></services></iq>`))
	f.Add([]byte(`<iq xmlns='jabber:client' id='external-id' type='error' from='mesh.test' to='agent@mesh.test/mesh-1'><error type='cancel'><service-unavailable xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error></iq>`))
	from, _ := jid.Parse("mesh.test")
	to, _ := jid.Parse("agent@mesh.test/mesh-1")
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	f.Fuzz(func(t *testing.T, document []byte) {
		if len(document) > maximumExternalServiceBytes+1024 {
			t.Skip()
		}
		services, _, handled, err := decodeCorrelatedExternalServices(xml.NewDecoder(strings.NewReader(string(document))), "external-id", from, to, "services", ExternalService{}, now)
		if err == nil {
			if !handled || services == nil || len(services) > maximumExternalServices {
				t.Fatalf("invalid successful decode: services=%#v handled=%v", services, handled)
			}
			profile := ExternalServiceProfile{Services: services}
			for _, service := range services {
				if !service.ExpiresAt.IsZero() && (profile.ExpiresAt.IsZero() || service.ExpiresAt.Before(profile.ExpiresAt)) {
					profile.ExpiresAt = service.ExpiresAt
				}
			}
			if validateErr := validateExternalServiceProfile(profile, now, false); validateErr != nil {
				t.Fatalf("successful decode failed validation: %v", validateErr)
			}
		}
	})
}

func FuzzExternalServiceCredentialResponseStrictDecoder(f *testing.F) {
	f.Add([]byte(`<iq xmlns='jabber:client' id='credential-id' type='result' from='mesh.test' to='agent@mesh.test/mesh-1'><credentials xmlns='urn:xmpp:extdisco:2'><service expires='2030-01-02T03:05:05Z' host='turn.test' password='secret' restricted='true' type='turn' username='agent'/></credentials></iq>`))
	f.Add([]byte(`<iq xmlns='jabber:client' id='credential-id' type='result' from='mesh.test' to='agent@mesh.test/mesh-1'><credentials xmlns='urn:xmpp:extdisco:2'><service expires='2030-01-02T03:05:05Z' host='turn.test' password='secret' restricted='false' type='turn' username='agent'/></credentials></iq>`))
	from, _ := jid.Parse("mesh.test")
	to, _ := jid.Parse("agent@mesh.test/mesh-1")
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	requested := ExternalService{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true}
	f.Fuzz(func(t *testing.T, document []byte) {
		if len(document) > maximumExternalServiceBytes+1024 {
			t.Skip()
		}
		services, _, handled, err := decodeCorrelatedExternalServices(xml.NewDecoder(strings.NewReader(string(document))), "credential-id", from, to, "credentials", requested, now)
		defer clearExternalServices(services)
		if err != nil {
			return
		}
		if !handled || len(services) != 1 {
			t.Fatalf("invalid successful decode: services=%#v handled=%v", services, handled)
		}
		service := services[0]
		if !sameExternalServiceIdentity(requested, service) || !service.Restricted || service.Username == "" || len(service.Password) == 0 || service.ExpiresAt.IsZero() || !now.Before(service.ExpiresAt) {
			t.Fatalf("invalid credential result accepted: %#v", service)
		}
	})
}

type externalServiceTestSession struct {
	*fakeSession
	mu               sync.Mutex
	profile          ExternalServiceProfile
	err              error
	queries          int
	returnedPassword []byte
	entered          chan struct{}
	release          chan struct{}
}

func (session *externalServiceTestSession) Authenticate(context.Context, string, []byte) (string, []byte, error) {
	return "agent@mesh.test", nil, nil
}

func (session *externalServiceTestSession) BindResource(context.Context, string) (string, error) {
	return "agent@mesh.test/mesh-1", nil
}

func (session *externalServiceTestSession) QueryExternalServices(ctx context.Context, _ time.Time) (ExternalServiceProfile, error) {
	session.mu.Lock()
	session.queries++
	profile, err := session.profile.clone(), session.err
	if len(profile.Services) != 0 {
		session.returnedPassword = profile.Services[0].Password
	}
	entered, release := session.entered, session.release
	session.mu.Unlock()
	if entered != nil {
		select {
		case <-entered:
		default:
			close(entered)
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			profile.clear()
			return ExternalServiceProfile{}, ctx.Err()
		}
	}
	return profile, err
}

func newExternalServiceClient(t *testing.T, session Session) *Client {
	t.Helper()
	pending, err := outbox.New(outbox.Config{MessageCapacity: 4, ByteCapacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(Config{Endpoint: "mesh.test:5222", Auth: Authentication{Username: "agent@mesh.test", Password: []byte("secret"), MeshID: "mesh-1"}, ReceiveCapacity: 4, TransferWorkers: 1, TransferQueue: 4, TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: 4, UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: time.Second, MailboxLimit: 4, ReconnectAttempts: 1, ReconnectInitial: time.Millisecond, ReconnectMaximum: time.Millisecond, ReconnectOperationTimeout: time.Second}, fakeDialer{session}, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = startCurrentMembershipFixture(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func TestExternalServiceCacheIsSessionEpochBound(t *testing.T) {
	expires := time.Date(2030, 1, 2, 3, 5, 5, 0, time.UTC)
	session := &externalServiceTestSession{fakeSession: &fakeSession{events: make(chan Event)}, profile: ExternalServiceProfile{Services: []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("secret"), ExpiresAt: expires}}, ExpiresAt: expires}}
	client := newExternalServiceClient(t, session)
	first, err := client.DiscoverExternalServices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first.Services[0].Password = []byte("mutated")
	second, err := client.DiscoverExternalServices(context.Background())
	if err != nil || !bytes.Equal(second.Services[0].Password, []byte("secret")) {
		t.Fatalf("owned cache = %#v, %v", second, err)
	}
	session.mu.Lock()
	queries := session.queries
	session.mu.Unlock()
	if queries != 1 {
		t.Fatalf("queries=%d", queries)
	}
	client.mu.Lock()
	client.sessionEpoch++
	client.mu.Unlock()
	if _, err = client.DiscoverExternalServices(context.Background()); err != nil {
		t.Fatalf("fresh session query=%v", err)
	}
	session.mu.Lock()
	queries = session.queries
	session.mu.Unlock()
	if queries != 2 {
		t.Fatalf("session epoch reused stale cache; queries=%d", queries)
	}
}

func TestExternalServiceProfileCloneAndClearOwnPasswordBuffers(t *testing.T) {
	originalPassword := []byte("original-secret")
	original := ExternalServiceProfile{Services: []ExternalService{{Password: originalPassword}}}
	cloned := original.clone()
	clonedPassword := cloned.Services[0].Password
	if !bytes.Equal(clonedPassword, originalPassword) || &clonedPassword[0] == &originalPassword[0] {
		t.Fatal("profile clone aliased password storage")
	}
	original.Clear()
	if !allZero(originalPassword) || !bytes.Equal(clonedPassword, []byte("original-secret")) {
		t.Fatalf("original clear=%x clone=%q", originalPassword, clonedPassword)
	}
	cloned.Clear()
	if !allZero(clonedPassword) {
		t.Fatalf("clone clear=%x", clonedPassword)
	}
}

func TestExternalServiceQueryErrorClearsReturnedPassword(t *testing.T) {
	expires := time.Date(2030, 1, 2, 3, 5, 5, 0, time.UTC)
	session := &externalServiceTestSession{fakeSession: &fakeSession{events: make(chan Event)}, profile: ExternalServiceProfile{Services: []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("error-secret"), ExpiresAt: expires}}, ExpiresAt: expires}, err: ErrUnavailable}
	client := newExternalServiceClient(t, session)
	if _, err := client.DiscoverExternalServices(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("discovery error=%v", err)
	}
	session.mu.Lock()
	returned := session.returnedPassword
	session.mu.Unlock()
	if !allZero(returned) {
		t.Fatalf("query error retained password=%x", returned)
	}
}

func TestExternalServiceCredentialFollowupErrorClearsReturnedPassword(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	services := []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true}}
	returned := []byte("followup-error-secret")
	if _, err := completeExternalServiceCredentials(context.Background(), services, now, func(context.Context, ExternalService, time.Time) ([]ExternalService, error) {
		return []ExternalService{{Password: returned}}, ErrUnavailable
	}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("followup error=%v", err)
	}
	if !allZero(returned) {
		t.Fatalf("followup error retained password=%x", returned)
	}
}

func TestExternalServiceCredentialFollowupPanicClearsOwnedInput(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	secret := []byte("panic-owned-secret")
	services := []ExternalService{
		{Type: "turn", Host: "first.test", Port: 3478, Transport: "udp", Restricted: true},
		{Type: "turn", Host: "second.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: secret, ExpiresAt: now.Add(time.Minute)},
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("credential query did not panic")
			}
		}()
		_, _ = completeExternalServiceCredentials(context.Background(), services, now, func(context.Context, ExternalService, time.Time) ([]ExternalService, error) {
			panic("injected credential dependency panic")
		})
	}()
	if !allZero(secret) {
		t.Fatalf("callback panic retained password=%x", secret)
	}
}

type panicExternalServiceSession struct{ *externalServiceTestSession }

func (*panicExternalServiceSession) QueryExternalServices(context.Context, time.Time) (ExternalServiceProfile, error) {
	panic("injected external-service dependency panic")
}

func TestExternalServiceDependencyPanicIsContained(t *testing.T) {
	session := &panicExternalServiceSession{externalServiceTestSession: &externalServiceTestSession{fakeSession: &fakeSession{events: make(chan Event)}}}
	client := newExternalServiceClient(t, session)
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("external-service dependency panic escaped: %v", recovered)
		}
	}()
	if _, err := client.DiscoverExternalServices(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("dependency panic error=%v", err)
	}
}

func TestExternalServiceEmptyProfileIsCachedAndSessionBound(t *testing.T) {
	session := &externalServiceTestSession{fakeSession: &fakeSession{events: make(chan Event)}, profile: ExternalServiceProfile{Services: []ExternalService{}}}
	client := newExternalServiceClient(t, session)
	for range 2 {
		profile, err := client.DiscoverExternalServices(context.Background())
		if err != nil || profile.Services == nil || len(profile.Services) != 0 {
			t.Fatalf("profile=%#v err=%v", profile, err)
		}
	}
	session.mu.Lock()
	queries := session.queries
	session.mu.Unlock()
	if queries != 1 {
		t.Fatalf("empty profile queries=%d", queries)
	}
}

func TestExternalServiceProfileCannotExtendCredentialLifetime(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	credentialExpiry := now.Add(time.Minute)
	profile := ExternalServiceProfile{Services: []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("secret"), ExpiresAt: credentialExpiry}}, ExpiresAt: credentialExpiry.Add(time.Minute)}
	if err := validateExternalServiceProfile(profile, now, true); !errors.Is(err, ErrProtocol) {
		t.Fatalf("extended profile lifetime=%v", err)
	}
}

func TestExternalServiceLateResultCannotCrossSessionEpoch(t *testing.T) {
	expires := time.Date(2030, 1, 2, 3, 5, 5, 0, time.UTC)
	session := &externalServiceTestSession{fakeSession: &fakeSession{events: make(chan Event)}, profile: ExternalServiceProfile{Services: []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("secret"), ExpiresAt: expires}}, ExpiresAt: expires}, entered: make(chan struct{}), release: make(chan struct{})}
	client := newExternalServiceClient(t, session)
	result := make(chan error, 1)
	go func() {
		_, err := client.DiscoverExternalServices(context.Background())
		result <- err
	}()
	<-session.entered
	client.mu.Lock()
	client.sessionEpoch++
	retired := client.clearExternalServicesLocked()
	client.mu.Unlock()
	retireExternalServiceExpiry(retired)
	close(session.release)
	if err := <-result; !errors.Is(err, ErrUnavailable) {
		t.Fatalf("late result=%v", err)
	}
}

func TestExternalServiceFailureDoesNotDemoteRank2AndPendingClearsCache(t *testing.T) {
	expires := time.Date(2030, 1, 2, 3, 5, 5, 0, time.UTC)
	session := &externalServiceTestSession{fakeSession: &fakeSession{events: make(chan Event)}, err: ErrUnavailable}
	client := newExternalServiceClient(t, session)
	if _, err := client.DiscoverExternalServices(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("discovery failure=%v", err)
	}
	if state := client.DurableState(); state != DurableLive {
		t.Fatalf("discovery demoted Rank2: %v", state)
	}
	session.mu.Lock()
	session.err = nil
	session.profile = ExternalServiceProfile{Services: []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("secret"), ExpiresAt: expires}}, ExpiresAt: expires}
	session.mu.Unlock()
	if _, err := client.DiscoverExternalServices(context.Background()); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	password := client.external.profile.Services[0].Password
	client.mu.Unlock()
	client.setState(DurablePending)
	client.mu.Lock()
	retained, valid := len(client.external.profile.Services), client.external.valid
	client.mu.Unlock()
	if retained != 0 || valid || !allZero(password) {
		t.Fatalf("pending transition retained profile: credentials=%d valid=%t", retained, valid)
	}
}

func TestExternalServiceReplacementRetiresEveryTimerOwner(t *testing.T) {
	session := &externalServiceTestSession{fakeSession: &fakeSession{events: make(chan Event)}}
	client := newExternalServiceClient(t, session)
	retired := make([]*externalServiceExpiryOwner, 0, 64)
	for cycle := range 64 {
		expires := client.clock.Now().UTC().Add(time.Hour)
		session.mu.Lock()
		session.profile = ExternalServiceProfile{Services: []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("secret-" + string(rune('a'+cycle%26))), ExpiresAt: expires}}, ExpiresAt: expires}
		session.mu.Unlock()
		client.mu.Lock()
		if cycle != 0 {
			client.sessionEpoch++
		}
		previous := client.external.expiry
		var previousPassword []byte
		if client.external.valid {
			previousPassword = client.external.profile.Services[0].Password
		}
		client.mu.Unlock()
		profile, err := client.DiscoverExternalServices(context.Background())
		if err != nil || len(profile.Services) != 1 {
			t.Fatalf("cycle %d: profile=%#v err=%v", cycle, profile, err)
		}
		profile.clear()
		if previous != nil {
			retired = append(retired, previous)
			select {
			case <-previous.done:
			default:
				t.Fatalf("cycle %d returned before superseded timer joined", cycle)
			}
		}
		if previousPassword != nil && !allZero(previousPassword) {
			t.Fatalf("cycle %d replacement retained password=%x", cycle, previousPassword)
		}
	}
	client.mu.Lock()
	last := client.external.expiry
	client.mu.Unlock()
	client.setState(DurablePending)
	retired = append(retired, last)
	for index, owner := range retired {
		if owner == nil {
			t.Fatalf("owner %d is nil", index)
		}
		select {
		case <-owner.done:
		default:
			t.Fatalf("timer owner %d survived retirement", index)
		}
	}
	client.mu.Lock()
	owner, valid := client.external.expiry, client.external.valid
	client.mu.Unlock()
	if owner != nil || valid {
		t.Fatalf("pending retained timer/profile: owner=%p valid=%t", owner, valid)
	}
}

func TestExternalServiceExpiryAndReplacementRaceCannotClearFreshProfile(t *testing.T) {
	session := &externalServiceTestSession{fakeSession: &fakeSession{events: make(chan Event)}}
	client := newExternalServiceClient(t, session)
	for cycle := range 16 {
		oldExpiry := client.clock.Now().UTC().Add(250 * time.Millisecond)
		session.mu.Lock()
		session.profile = ExternalServiceProfile{Services: []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("old"), ExpiresAt: oldExpiry}}, ExpiresAt: oldExpiry}
		session.mu.Unlock()
		client.mu.Lock()
		client.sessionEpoch++
		client.mu.Unlock()
		old, err := client.DiscoverExternalServices(context.Background())
		if err != nil || len(old.Services) != 1 {
			t.Fatalf("cycle %d old profile=%#v err=%v", cycle, old, err)
		}
		old.clear()
		client.mu.Lock()
		oldOwner := client.external.expiry
		if oldOwner == nil {
			client.mu.Unlock()
			t.Fatalf("cycle %d missing old expiry owner", cycle)
		}
		client.sessionEpoch++
		freshExpiry := client.clock.Now().UTC().Add(time.Hour)
		session.mu.Lock()
		session.profile = ExternalServiceProfile{Services: []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("fresh"), ExpiresAt: freshExpiry}}, ExpiresAt: freshExpiry}
		session.mu.Unlock()
		result := make(chan error, 1)
		go func() {
			profile, discoverErr := client.DiscoverExternalServices(context.Background())
			profile.clear()
			result <- discoverErr
		}()
		// The client owns a calibrated UTC clock whose wall value intentionally
		// differs from the host clock. Wait relative to the configured lifetime;
		// time.Until(oldExpiry) would incorrectly mix those clock domains.
		time.Sleep(275 * time.Millisecond)
		client.mu.Unlock()
		if err = <-result; err != nil {
			t.Fatalf("cycle %d replacement=%v", cycle, err)
		}
		select {
		case <-oldOwner.done:
		default:
			t.Fatalf("cycle %d replacement returned before expired owner joined", cycle)
		}
		client.mu.Lock()
		fresh := client.external.valid && len(client.external.profile.Services) == 1 && bytes.Equal(client.external.profile.Services[0].Password, []byte("fresh")) && client.external.expiry != nil && client.external.expiry != oldOwner
		client.mu.Unlock()
		if !fresh {
			t.Fatalf("cycle %d stale expiry cleared replacement", cycle)
		}
	}
}

func TestExternalServiceCloseJoinsTimerOwnerWithinBound(t *testing.T) {
	session := &externalServiceTestSession{fakeSession: &fakeSession{events: make(chan Event)}}
	client := newExternalServiceClient(t, session)
	expires := client.clock.Now().UTC().Add(time.Hour)
	session.mu.Lock()
	session.profile = ExternalServiceProfile{Services: []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("secret"), ExpiresAt: expires}}, ExpiresAt: expires}
	session.mu.Unlock()
	if _, err := client.DiscoverExternalServices(context.Background()); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	owner := client.external.expiry
	password := client.external.profile.Services[0].Password
	client.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !allZero(password) {
		t.Fatalf("Close retained password=%x", password)
	}
	select {
	case <-owner.done:
	default:
		t.Fatal("Close returned before timer owner joined")
	}
	if err := client.Close(ctx); err != nil {
		t.Fatalf("repeated Close=%v", err)
	}
}

func TestExternalServiceCredentialExpiryClearsCache(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	expires := base.Add(40 * time.Millisecond)
	session := &externalServiceTestSession{fakeSession: &fakeSession{events: make(chan Event)}, profile: ExternalServiceProfile{Services: []ExternalService{{Type: "turn", Host: "turn.test", Port: 3478, Transport: "udp", Restricted: true, Username: "agent", Password: []byte("secret"), ExpiresAt: expires}}, ExpiresAt: expires}}
	client := newExternalServiceClient(t, session)
	if _, err := client.DiscoverExternalServices(context.Background()); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	password := client.external.profile.Services[0].Password
	client.mu.Unlock()
	deadline := time.Now().Add(time.Second)
	for {
		client.mu.Lock()
		retained, valid := len(client.external.profile.Services), client.external.valid
		client.mu.Unlock()
		if retained == 0 && !valid {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired credentials retained=%d", retained)
		}
		time.Sleep(time.Millisecond)
	}
	if !allZero(password) {
		t.Fatalf("expiry retained password=%x", password)
	}
}
