package enrollment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"mellium.im/xmpp/jid"
)

const (
	productionEndpoint        = "https://enrollment.cynapsa.com/v1/enroll"
	productionRenewEndpoint   = "https://enrollment.cynapsa.com/v1/renew"
	productionV2Endpoint      = "https://enrollment.cynapsa.com/v2/enroll"
	productionV2RenewEndpoint = "https://enrollment.cynapsa.com/v2/renew"
	requestTimeout            = 10 * time.Second
	maximumResponseBytes      = 64 << 10
	maximumWrapperBytes       = 16 << 10
	maximumDisplayBytes       = 512
	maximumSessionResource    = 512
	maximumExpirySeconds      = 7 * 24 * 60 * 60
)

type FailureCode uint8

const (
	FailureCancelled FailureCode = iota + 1
	FailureDeadline
	FailureRejected
	FailureUnavailable
	FailureInvalidResponse
)

// Failure is deliberately free of dependency text and protocol values.
type Failure struct{ Code FailureCode }

func (*Failure) Error() string { return "enrollment failed" }

type Request struct {
	Token              []byte
	MeshID             string
	InstallationID     string
	InstallationSecret []byte
}

func (request *Request) Clear() {
	if request == nil {
		return
	}
	clear(request.Token)
	clear(request.InstallationSecret)
	*request = Request{}
}

// Bundle owns its sensitive byte slices. Callers must Clear it.
type Bundle struct {
	Version                       string
	OrganizationID                string
	InstallationID                string
	AgentID                       string
	AgentJID                      string
	MeshID                        string
	SessionResource               string
	MeshEndpoint                  string
	Username                      string
	Server                        string
	AccessToken                   []byte
	ExpiresIn                     int64
	Wrapper                       []byte
	Display                       string
	InstallationEpoch             int64
	AttachmentRevision            int64
	TokenID                       string
	PolicyRevision                int64
	SessionExpiryMode             string
	OfflineColdStartTargetSeconds int64
}

func (Bundle) String() string          { return "enrollment.Bundle{[REDACTED]}" }
func (bundle Bundle) GoString() string { return bundle.String() }

func (bundle *Bundle) Clear() {
	if bundle == nil {
		return
	}
	clear(bundle.AccessToken)
	clear(bundle.Wrapper)
	*bundle = Bundle{}
}

type Provider interface {
	Enroll(context.Context, Request) (Bundle, *Failure)
}

// V2Provider issues installation-and-mesh scoped credentials. The separate
// methods preserve the e1 provider surface during a controlled migration.
type V2Provider interface {
	EnrollV2(context.Context, Request) (Bundle, *Failure)
	RenewV2(context.Context, RenewRequest) (Bundle, *Failure)
}

type RenewalProvider interface {
	Renew(context.Context, RenewRequest) (Bundle, *Failure)
}

type RenewRequest struct {
	MeshID             string
	InstallationID     string
	InstallationSecret []byte
}

func (request *RenewRequest) Clear() {
	if request == nil {
		return
	}
	clear(request.InstallationSecret)
	*request = RenewRequest{}
}

type Client struct {
	endpoint        string
	renewEndpoint   string
	v2Endpoint      string
	v2RenewEndpoint string
	http            *http.Client
}

// NewProductionClient uses the fixed private production endpoint.
func NewProductionClient() *Client {
	client := newClient(productionEndpoint, http.DefaultTransport)
	client.renewEndpoint = productionRenewEndpoint
	client.v2Endpoint = productionV2Endpoint
	client.v2RenewEndpoint = productionV2RenewEndpoint
	return client
}

// NewTestClient is an internal test seam. Plain HTTP is accepted only for a
// literal loopback endpoint; production construction never calls this.
func NewTestClient(endpoint string, transport http.RoundTripper) (*Client, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || (!strings.HasSuffix(parsed.Path, "/v1/enroll") && !strings.HasSuffix(parsed.Path, "/v2/enroll")) || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return nil, errors.New("invalid enrollment endpoint")
	}
	if parsed.Scheme != "https" {
		host := parsed.Hostname()
		ip := net.ParseIP(host)
		if parsed.Scheme != "http" || (host != "localhost" && (ip == nil || !ip.IsLoopback())) {
			return nil, errors.New("invalid enrollment endpoint")
		}
	}
	return newClient(endpoint, transport), nil
}

func newClient(endpoint string, transport http.RoundTripper) *Client {
	if transport == nil {
		transport = http.DefaultTransport
	}
	versionRoot := strings.TrimSuffix(strings.TrimSuffix(endpoint, "/v1/enroll"), "/v2/enroll")
	return &Client{
		endpoint:        endpoint,
		renewEndpoint:   versionRoot + "/v1/renew",
		v2Endpoint:      versionRoot + "/v2/enroll",
		v2RenewEndpoint: versionRoot + "/v2/renew",
		http: &http.Client{
			Transport: transport,
			Timeout:   requestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

type requestWire struct {
	Token              string `json:"token"`
	MeshID             string `json:"mesh_id"`
	InstallationID     string `json:"installation_id"`
	InstallationSecret string `json:"installation_secret"`
}

type renewRequestWire struct {
	MeshID             string `json:"mesh_id"`
	InstallationID     string `json:"installation_id"`
	InstallationSecret string `json:"installation_secret"`
}

type responseWire struct {
	Version                       string `json:"version"`
	OrganizationID                string `json:"organization_id,omitempty"`
	InstallationID                string `json:"installation_id"`
	AgentID                       string `json:"agent_id"`
	AgentJID                      string `json:"agent_jid,omitempty"`
	MeshID                        string `json:"mesh_id"`
	SessionResource               string `json:"session_resource"`
	MeshEndpoint                  string `json:"mesh_endpoint"`
	Username                      string `json:"username"`
	Server                        string `json:"server"`
	AccessToken                   string `json:"access_token"`
	ExpiresIn                     int64  `json:"expires_in"`
	Wrapper                       string `json:"wrapper"`
	Display                       string `json:"display"`
	InstallationEpoch             int64  `json:"installation_epoch,omitempty"`
	AttachmentRevision            int64  `json:"attachment_revision,omitempty"`
	TokenID                       string `json:"token_id,omitempty"`
	PolicyRevision                int64  `json:"policy_revision,omitempty"`
	SessionExpiryMode             string `json:"session_expiry_mode,omitempty"`
	OfflineColdStartTargetSeconds int64  `json:"offline_cold_start_target_seconds,omitempty"`
}

func (client *Client) Enroll(ctx context.Context, request Request) (Bundle, *Failure) {
	if client == nil || client.http == nil || ctx == nil || !validRequest(request) {
		return Bundle{}, &Failure{Code: FailureRejected}
	}
	if failure := contextFailure(ctx); failure != nil {
		return Bundle{}, failure
	}
	body, err := json.Marshal(requestWire{
		Token: string(request.Token), MeshID: request.MeshID,
		InstallationID:     request.InstallationID,
		InstallationSecret: string(request.InstallationSecret),
	})
	if err != nil {
		return Bundle{}, &Failure{Code: FailureRejected}
	}
	defer clear(body)
	return client.perform(ctx, client.endpoint, body, func(wire responseWire) (Bundle, bool) {
		return validateResponse(request, wire)
	})
}

func (client *Client) Renew(ctx context.Context, request RenewRequest) (Bundle, *Failure) {
	return client.renewAt(ctx, request, client.renewEndpoint, false)
}

func (client *Client) EnrollV2(ctx context.Context, request Request) (Bundle, *Failure) {
	return client.enrollAt(ctx, request, client.v2Endpoint, true)
}

func (client *Client) RenewV2(ctx context.Context, request RenewRequest) (Bundle, *Failure) {
	return client.renewAt(ctx, request, client.v2RenewEndpoint, true)
}

func (client *Client) enrollAt(ctx context.Context, request Request, endpoint string, v2 bool) (Bundle, *Failure) {
	if client == nil || client.http == nil || ctx == nil || !validRequest(request) {
		return Bundle{}, &Failure{Code: FailureRejected}
	}
	wireToken := string(request.Token)
	if v2 {
		wireToken = publicTokenPrefix + wireToken
	}
	body, err := json.Marshal(requestWire{Token: wireToken, MeshID: request.MeshID, InstallationID: request.InstallationID, InstallationSecret: string(request.InstallationSecret)})
	if err != nil {
		return Bundle{}, &Failure{Code: FailureRejected}
	}
	defer clear(body)
	return client.perform(ctx, endpoint, body, func(wire responseWire) (Bundle, bool) {
		return validateExpectedResponseVersion(request.MeshID, request.InstallationID, wire, v2)
	})
}

func (client *Client) renewAt(ctx context.Context, request RenewRequest, endpoint string, v2 bool) (Bundle, *Failure) {
	if client == nil || client.http == nil || ctx == nil || !validRenewRequest(request) {
		return Bundle{}, &Failure{Code: FailureRejected}
	}
	if failure := contextFailure(ctx); failure != nil {
		return Bundle{}, failure
	}
	body, err := json.Marshal(renewRequestWire{
		MeshID: request.MeshID, InstallationID: request.InstallationID,
		InstallationSecret: string(request.InstallationSecret),
	})
	if err != nil {
		return Bundle{}, &Failure{Code: FailureRejected}
	}
	defer clear(body)
	return client.perform(ctx, endpoint, body, func(wire responseWire) (Bundle, bool) {
		return validateExpectedResponseVersion(request.MeshID, request.InstallationID, wire, v2)
	})
}

func (client *Client) perform(ctx context.Context, endpoint string, body []byte, validate func(responseWire) (Bundle, bool)) (Bundle, *Failure) {
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return Bundle{}, &Failure{Code: FailureRejected}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Cache-Control", "no-store")
	response, err := client.http.Do(httpRequest)
	if err != nil {
		if failure := contextFailure(ctx); failure != nil {
			return Bundle{}, failure
		}
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			return Bundle{}, &Failure{Code: FailureDeadline}
		}
		return Bundle{}, &Failure{Code: FailureUnavailable}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode >= 300 && response.StatusCode < 400 {
			return Bundle{}, &Failure{Code: FailureInvalidResponse}
		}
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return Bundle{}, &Failure{Code: FailureUnavailable}
		}
		return Bundle{}, &Failure{Code: FailureRejected}
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" || !hasNoStore(response.Header.Values("Cache-Control")) {
		return Bundle{}, &Failure{Code: FailureInvalidResponse}
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maximumResponseBytes+1))
	if err != nil {
		return Bundle{}, &Failure{Code: FailureUnavailable}
	}
	defer clear(data)
	if len(data) > maximumResponseBytes {
		return Bundle{}, &Failure{Code: FailureInvalidResponse}
	}
	var wire responseWire
	if err := strictResponse(data, &wire); err != nil {
		return Bundle{}, &Failure{Code: FailureInvalidResponse}
	}
	bundle, ok := validate(wire)
	if !ok {
		return Bundle{}, &Failure{Code: FailureInvalidResponse}
	}
	return bundle, nil
}

func validRenewRequest(request RenewRequest) bool {
	return len(request.MeshID) <= 255 && protocol.ValidateMeshID(request.MeshID) == nil && !strings.Contains(request.MeshID, "/") &&
		ValidCanonicalUUID(request.InstallationID) && validCanonicalSecret(request.InstallationSecret)
}

func validRequest(request Request) bool {
	return len(request.Token) == CanonicalTokenBodyBytes && validTokenBody(request.Token) &&
		len(request.MeshID) <= 255 && protocol.ValidateMeshID(request.MeshID) == nil && !strings.Contains(request.MeshID, "/") &&
		ValidCanonicalUUID(request.InstallationID) && validCanonicalSecret(request.InstallationSecret)
}

func validateResponse(request Request, wire responseWire) (Bundle, bool) {
	return validateExpectedResponse(request.MeshID, request.InstallationID, wire)
}

func validateRenewResponse(request RenewRequest, wire responseWire) (Bundle, bool) {
	return validateExpectedResponse(request.MeshID, request.InstallationID, wire)
}

func validateExpectedResponse(meshID, installationID string, wire responseWire) (Bundle, bool) {
	return validateExpectedResponseVersion(meshID, installationID, wire, false)
}

func validateExpectedResponseVersion(meshID, installationID string, wire responseWire, requireV2 bool) (Bundle, bool) {
	endpoint, err := model.ParseMeshEndpoint(wire.MeshEndpoint)
	if err != nil || ((!requireV2 && wire.Version != tokenVersion) || (requireV2 && wire.Version != "e2")) || wire.InstallationID != installationID ||
		wire.MeshID != meshID || !ValidCanonicalUUID(wire.AgentID) ||
		!canonicalBareUsername(wire.Username) ||
		wire.Server != endpoint.TLSName() || wire.ExpiresIn <= 0 || wire.ExpiresIn > maximumExpirySeconds ||
		!validSessionResource(wire.SessionResource) || !validOpaque(wire.AccessToken, mesh.MaxCredentialBytes) ||
		!validOpaque(wire.Wrapper, maximumWrapperBytes) || !strings.HasPrefix(wire.Wrapper, "aztm_") ||
		len(wire.Display) > maximumDisplayBytes || !utf8.ValidString(wire.Display) {
		return Bundle{}, false
	}
	if wire.Version == "e2" && (!ValidCanonicalUUID(wire.OrganizationID) || wire.AgentJID != wire.Username || wire.InstallationEpoch < 1 || wire.AttachmentRevision < 1 ||
		!validOpaque(wire.TokenID, 512) || wire.PolicyRevision < 1 ||
		(wire.SessionExpiryMode != "continue" && wire.SessionExpiryMode != "disconnect") ||
		wire.OfflineColdStartTargetSeconds < 0 || wire.OfflineColdStartTargetSeconds > 86400) {
		return Bundle{}, false
	}
	return Bundle{
		Version: wire.Version, OrganizationID: wire.OrganizationID, InstallationID: wire.InstallationID, AgentID: wire.AgentID, AgentJID: wire.AgentJID,
		MeshID: wire.MeshID, SessionResource: wire.SessionResource,
		MeshEndpoint: endpoint.DialAddress(), Username: wire.Username, Server: wire.Server,
		AccessToken: []byte(wire.AccessToken), ExpiresIn: wire.ExpiresIn,
		Wrapper: []byte(wire.Wrapper), Display: wire.Display, InstallationEpoch: wire.InstallationEpoch,
		AttachmentRevision: wire.AttachmentRevision, TokenID: wire.TokenID, PolicyRevision: wire.PolicyRevision,
		SessionExpiryMode: wire.SessionExpiryMode, OfflineColdStartTargetSeconds: wire.OfflineColdStartTargetSeconds,
	}, true
}

// ValidateBundle applies the full response contract to any injected provider.
func ValidateBundle(request Request, bundle Bundle) bool {
	if !validRequest(request) {
		return false
	}
	return validateBundleExpected(request.MeshID, request.InstallationID, bundle)
}

// ValidateRenewedBundle applies the full response contract without requiring
// the original one-time enrollment grant.
func ValidateRenewedBundle(request RenewRequest, bundle Bundle) bool {
	if !validRenewRequest(request) {
		return false
	}
	return validateBundleExpected(request.MeshID, request.InstallationID, bundle)
}

func validateBundleExpected(meshID, installationID string, bundle Bundle) bool {
	if (bundle.Version != tokenVersion && bundle.Version != "e2") || bundle.InstallationID != installationID ||
		bundle.MeshID != meshID || !ValidCanonicalUUID(bundle.AgentID) ||
		!canonicalBareUsername(bundle.Username) ||
		bundle.ExpiresIn <= 0 || bundle.ExpiresIn > maximumExpirySeconds ||
		!validSessionResource(bundle.SessionResource) ||
		!validOpaque(string(bundle.AccessToken), mesh.MaxCredentialBytes) ||
		!validOpaque(string(bundle.Wrapper), maximumWrapperBytes) ||
		!strings.HasPrefix(string(bundle.Wrapper), "aztm_") ||
		len(bundle.Display) > maximumDisplayBytes || !utf8.ValidString(bundle.Display) {
		return false
	}
	if bundle.Version == "e2" && (!ValidCanonicalUUID(bundle.OrganizationID) || bundle.AgentJID != bundle.Username || bundle.InstallationEpoch < 1 || bundle.AttachmentRevision < 1 ||
		!validOpaque(bundle.TokenID, 512) || bundle.PolicyRevision < 1 ||
		(bundle.SessionExpiryMode != "continue" && bundle.SessionExpiryMode != "disconnect") ||
		bundle.OfflineColdStartTargetSeconds < 0 || bundle.OfflineColdStartTargetSeconds > 86400) {
		return false
	}
	endpoint, err := model.ParseMeshEndpoint(bundle.MeshEndpoint)
	return err == nil && bundle.MeshEndpoint == endpoint.DialAddress() && bundle.Server == endpoint.TLSName()
}

func validOpaque(value string, limit int) bool {
	if value == "" || len(value) > limit || !utf8.ValidString(value) {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] <= 0x20 || value[index] == 0x7f {
			return false
		}
	}
	return true
}

func canonicalBareUsername(value string) bool {
	if protocol.ValidateAgentIdentity(value) != nil || strings.Contains(value, "/") {
		return false
	}
	parsed, err := jid.Parse(value)
	return err == nil && parsed.Localpart() != "" && parsed.Domainpart() != "" &&
		parsed.Resourcepart() == "" && parsed.String() == value
}

func validSessionResource(value string) bool {
	if value == "" || len(value) > maximumSessionResource || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f || character == '/' || character == '@' {
			return false
		}
	}
	return true
}

func hasNoStore(values []string) bool {
	for _, value := range values {
		for _, directive := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(strings.SplitN(directive, "=", 2)[0]), "no-store") {
				return true
			}
		}
	}
	return false
}

func strictResponse(data []byte, destination *responseWire) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return errors.New("invalid enrollment response")
	}
	seen := make(map[string]struct{}, 12)
	raw := make(map[string]json.RawMessage, 12)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return errors.New("invalid enrollment response")
		}
		if _, exists := seen[key]; exists || !knownResponseField(key) {
			return errors.New("invalid enrollment response")
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return errors.New("invalid enrollment response")
		}
		raw[key] = value
	}
	if closing, err := decoder.Token(); err != nil || closing != json.Delim('}') || (len(seen) != 12 && len(seen) != 20) {
		return errors.New("invalid enrollment response")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("invalid enrollment response")
	}
	canonical, err := json.Marshal(raw)
	if err != nil {
		return errors.New("invalid enrollment response")
	}
	strict := json.NewDecoder(bytes.NewReader(canonical))
	strict.DisallowUnknownFields()
	if err := strict.Decode(destination); err != nil {
		return errors.New("invalid enrollment response")
	}
	return nil
}

func knownResponseField(key string) bool {
	switch key {
	case "version", "organization_id", "installation_id", "agent_id", "agent_jid", "mesh_id", "session_resource", "mesh_endpoint", "username", "server", "access_token", "expires_in", "wrapper", "display", "installation_epoch", "attachment_revision", "token_id", "policy_revision", "session_expiry_mode", "offline_cold_start_target_seconds":
		return true
	default:
		return false
	}
}

func contextFailure(ctx context.Context) *Failure {
	switch ctx.Err() {
	case nil:
		return nil
	case context.DeadlineExceeded:
		return &Failure{Code: FailureDeadline}
	default:
		return &Failure{Code: FailureCancelled}
	}
}
