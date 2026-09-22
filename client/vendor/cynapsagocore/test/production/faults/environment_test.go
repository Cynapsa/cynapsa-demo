package faults_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
	"github.com/Cynapsa/cynapsagocore/internal/xep0363"
	"github.com/Cynapsa/cynapsagocore/test/internal/productionlock"
	"mellium.im/sasl"
)

const pinnedProxyImage = "ghcr.io/shopify/toxiproxy@sha256:9378ed52a28bc50edc1350f936f518f31fa95f0d15917d6eb40b8e376d1a214e"

type profileFile struct {
	Version  int                `json:"version"`
	Profiles map[string][]toxic `json:"profiles"`
}

type toxic struct {
	Proxy      string           `json:"proxy"`
	Name       string           `json:"name"`
	Stream     string           `json:"stream"`
	Type       string           `json:"type"`
	Toxicity   float64          `json:"toxicity"`
	Attributes map[string]int64 `json:"attributes"`
}

type proxyAPI struct {
	base   string
	client *http.Client
}

type staticSlots struct{ slot xep0363.Slot }

func (slots staticSlots) RequestSlot(context.Context, int64, string) (xep0363.Slot, error) {
	return slots.slot, nil
}

func TestPinnedTCPAndHTTPFaultProfiles(t *testing.T) {
	lockContext, cancelLock := context.WithTimeout(context.Background(), 10*time.Minute)
	release, err := productionlock.Acquire(lockContext)
	cancelLock()
	if err != nil {
		t.Fatalf("acquire production environment: %v", err)
	}
	t.Cleanup(release)

	repository := repositoryRoot(t)
	harness := filepath.Join(repository, "test", "production", "faults", "run.sh")
	state := runCommand(t, repository, 2*time.Minute, harness, "start")
	if state == "" || !filepath.IsAbs(state) {
		t.Fatalf("invalid environment state %q", state)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = runCommandAllowFailure(repository, 45*time.Second, harness, "stop", state)
		}
	})

	if image := readFirst(t, filepath.Join(state, "proxy-image.txt")); image != pinnedProxyImage {
		t.Fatalf("proxy image=%q, want immutable %q", image, pinnedProxyImage)
	}
	profiles := readProfiles(t, filepath.Join(repository, "test", "production", "faults", "profiles-v1.json"))
	api := &proxyAPI{
		base:   "http://127.0.0.1:" + readFirst(t, filepath.Join(state, "api-port")),
		client: &http.Client{Timeout: 3 * time.Second},
	}
	api.ensureProxies(t, state)

	child := readFirst(t, filepath.Join(state, "ejabberd-state"))
	ejabberdHarness := filepath.Join(repository, "test", "e2e", "ejabberd", "run.sh")
	for _, command := range [][]string{{"create", "fault-mesh"}, {"add", "agent-a", "fault-mesh"}} {
		arguments := append([]string{"admin", child}, command...)
		runCommand(t, repository, 30*time.Second, ejabberdHarness, arguments...)
	}
	credentials := readEnvironment(t, filepath.Join(state, "credentials.env"))
	endpoint := "127.0.0.1:" + readFirst(t, filepath.Join(state, "xmpp-port"))
	roots := readRoots(t, filepath.Join(child, "ca.pem"))

	t.Run("clean XMPP production establishment", func(t *testing.T) {
		api.apply(t, profiles, "clean")
		session, err := establishXMPP(t, endpoint, credentials, roots, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		closeSession(t, session)
	})

	t.Run("XMPP latency reaches real establishment", func(t *testing.T) {
		api.apply(t, profiles, "xmpp-latency")
		started := time.Now()
		session, err := establishXMPP(t, endpoint, credentials, roots, 8*time.Second)
		elapsed := time.Since(started)
		if err != nil {
			t.Fatal(err)
		}
		closeSession(t, session)
		if elapsed < 240*time.Millisecond || elapsed > 8*time.Second {
			t.Fatalf("latency profile elapsed=%s, want injected delay and bounded success", elapsed)
		}
	})

	t.Run("XMPP half-open honors caller deadline", func(t *testing.T) {
		api.apply(t, profiles, "xmpp-half-open")
		started := time.Now()
		_, err := establishXMPP(t, endpoint, credentials, roots, 700*time.Millisecond)
		elapsed := time.Since(started)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("half-open error=%v, want deadline", err)
		}
		if elapsed < 500*time.Millisecond || elapsed > 2*time.Second {
			t.Fatalf("half-open elapsed=%s, want bounded deadline", elapsed)
		}
	})

	t.Run("XMPP peer reset fails boundedly", func(t *testing.T) {
		api.apply(t, profiles, "xmpp-reset")
		started := time.Now()
		_, err := establishXMPP(t, endpoint, credentials, roots, 2*time.Second)
		if err == nil {
			t.Fatal("peer reset unexpectedly established XMPP")
		}
		if elapsed := time.Since(started); elapsed > 2500*time.Millisecond {
			t.Fatalf("peer reset took %s", elapsed)
		}
	})

	objectPort, err := strconv.Atoi(readFirst(t, filepath.Join(state, "object-port")))
	if err != nil || objectPort < 1 || objectPort > 65535 {
		t.Fatalf("invalid object port: %v", err)
	}
	objectURL := fmt.Sprintf("http://127.0.0.1:%d/object", objectPort)
	payload := bytes.Repeat([]byte("fault-profile-payload-"), 3200)

	t.Run("clean HTTP upload and download", func(t *testing.T) {
		api.apply(t, profiles, "clean")
		client := newObjectClient(t, objectPort, objectURL, 5*time.Second)
		defer client.Close()
		reference, err := client.Upload(context.Background(), payload)
		if err != nil || reference != objectURL {
			t.Fatalf("upload reference=%q error=%v", reference, err)
		}
		value, err := client.Download(context.Background(), reference)
		if err != nil || !bytes.Equal(value, payload) {
			t.Fatalf("download bytes=%d error=%v", len(value), err)
		}
	})

	t.Run("HTTP bandwidth restriction applies backpressure", func(t *testing.T) {
		api.apply(t, profiles, "http-bandwidth")
		client := newObjectClient(t, objectPort, objectURL, 6*time.Second)
		defer client.Close()
		started := time.Now()
		if _, err := client.Upload(context.Background(), payload); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(started)
		if elapsed < 1300*time.Millisecond || elapsed > 6*time.Second {
			t.Fatalf("bandwidth profile elapsed=%s, want real bounded backpressure", elapsed)
		}
	})

	t.Run("HTTP half-open normalizes timeout", func(t *testing.T) {
		api.apply(t, profiles, "http-half-open")
		client := newObjectClient(t, objectPort, objectURL, 700*time.Millisecond)
		defer client.Close()
		started := time.Now()
		_, err := client.Upload(context.Background(), payload)
		if !errors.Is(err, xep0363.ErrTimeout) {
			t.Fatalf("half-open upload error=%v, want ErrTimeout", err)
		}
		if elapsed := time.Since(started); elapsed < 500*time.Millisecond || elapsed > 2*time.Second {
			t.Fatalf("half-open upload elapsed=%s", elapsed)
		}
	})

	t.Run("HTTP reset normalizes transfer failure", func(t *testing.T) {
		api.apply(t, profiles, "http-reset")
		client := newObjectClient(t, objectPort, objectURL, 2*time.Second)
		defer client.Close()
		_, err := client.Upload(context.Background(), payload)
		if !errors.Is(err, xep0363.ErrTransfer) {
			t.Fatalf("reset upload error=%v, want ErrTransfer", err)
		}
	})

	t.Run("abrupt proxy restart invalidates old session and permits fresh establishment", func(t *testing.T) {
		api.apply(t, profiles, "clean")
		old, err := establishXMPP(t, endpoint, credentials, roots, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer closeSession(t, old)
		runCommand(t, repository, 30*time.Second, harness, "restart-proxy", state)
		query, ok := old.(interface {
			QueryServerTime(context.Context) (time.Time, error)
		})
		if !ok {
			t.Fatal("production session omitted server-time query")
		}
		queryContext, cancel := context.WithTimeout(context.Background(), time.Second)
		_, oldErr := query.QueryServerTime(queryContext)
		cancel()
		if oldErr == nil {
			t.Fatal("old XMPP session survived abrupt proxy restart")
		}
		api.ensureProxies(t, state)
		fresh, err := establishXMPP(t, endpoint, credentials, roots, 5*time.Second)
		if err != nil {
			t.Fatalf("fresh establishment after proxy restart: %v", err)
		}
		closeSession(t, fresh)
	})

	proxyID := readFirst(t, filepath.Join(state, "proxy-container-id"))
	objectID := readFirst(t, filepath.Join(state, "object-container-id"))
	ejabberdID := readFirst(t, filepath.Join(child, "container-id"))
	network := readFirst(t, filepath.Join(child, "network"))
	volume := readFirst(t, filepath.Join(child, "volume"))
	runCommand(t, repository, 45*time.Second, harness, "stop", state)
	stopped = true
	for _, identifier := range []string{proxyID, objectID, ejabberdID} {
		if result := runCommandAllowFailure(repository, 5*time.Second, "docker", "inspect", identifier); result.exitCode == 0 {
			t.Errorf("container remained after teardown: %s", identifier)
		}
	}
	if result := runCommandAllowFailure(repository, 5*time.Second, "docker", "network", "inspect", network); result.exitCode == 0 {
		t.Errorf("network remained after teardown: %s", network)
	}
	if result := runCommandAllowFailure(repository, 5*time.Second, "docker", "volume", "inspect", volume); result.exitCode == 0 {
		t.Errorf("volume remained after teardown: %s", volume)
	}
	for _, secret := range []string{"credentials.env", "objectserver"} {
		if _, err := os.Stat(filepath.Join(state, secret)); !os.IsNotExist(err) {
			t.Errorf("private artifact remained after teardown: %s", secret)
		}
	}
}

func establishXMPP(t *testing.T, endpoint string, credentials map[string]string, roots *x509.CertPool, timeout time.Duration) (rank2xmpp.Session, error) {
	t.Helper()
	dialer, err := rank2xmpp.NewMelliumDialer(endpoint, rank2xmpp.MelliumConfig{
		TLSConfig:       &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13},
		SASLMechanisms:  []sasl.Mechanism{sasl.ScramSha256Plus, sasl.ScramSha256},
		ReceiveCapacity: 4, StreamManagementCapacity: 4,
		StreamManagementByteCapacity: 2 << 20,
		MaximumFrameBytes:            transport.MaximumControlFrameBytes, StanzaBudgetBytes: 512 << 10,
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	session, err := dialer.Dial(ctx)
	if err != nil {
		return nil, err
	}
	fail := func(value error) (rank2xmpp.Session, error) {
		closeContext, closeCancel := context.WithTimeout(context.Background(), time.Second)
		_ = session.Close(closeContext)
		closeCancel()
		return nil, value
	}
	if err = session.ConnectTLS(ctx, endpoint); err != nil {
		return fail(err)
	}
	user := credentials["CYNAPSA_EJABBERD_AGENT_A"]
	bare, _, err := session.Authenticate(ctx, user, []byte(credentials["CYNAPSA_EJABBERD_PASSWORD_A"]))
	if err != nil {
		return fail(err)
	}
	if bare != user {
		return fail(fmt.Errorf("authenticated bare identity=%q", bare))
	}
	full, err := session.BindResource(ctx, "fault-mesh")
	if err != nil {
		return fail(err)
	}
	if full != user+"/fault-mesh" {
		return fail(fmt.Errorf("bound identity=%q", full))
	}
	if err = session.EnableStreamManagement(ctx, true); err != nil {
		return fail(err)
	}
	return session, nil
}

func closeSession(t *testing.T, session rank2xmpp.Session) {
	t.Helper()
	if session == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.Close(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, rank2xmpp.ErrClosed) {
		t.Errorf("close XMPP session: %v", err)
	}
}

func newObjectClient(t *testing.T, port int, objectURL string, timeout time.Duration) *xep0363.Client {
	t.Helper()
	client, err := xep0363.NewClient(xep0363.Config{
		Policy: xep0363.Policy{
			MaximumBytes: 1 << 20, MaximumURLBytes: 2048, MaximumResponseHeaderBytes: 16 << 10,
			RequestTimeout: timeout, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second,
			ResponseHeaderTimeout: timeout, IdleConnTimeout: time.Second, MaximumRedirects: 0,
			AllowedHosts: map[string]bool{"127.0.0.1": true}, AllowedPorts: map[uint16]bool{uint16(port): true},
			AllowPrivateHosts: map[string]bool{"127.0.0.1": true}, AllowHTTPHosts: map[string]bool{"127.0.0.1": true},
		},
		Slots: staticSlots{slot: xep0363.Slot{PutURL: objectURL, GetURL: objectURL}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func readProfiles(t *testing.T, path string) profileFile {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 64<<10))
	decoder.DisallowUnknownFields()
	var profiles profileFile
	if err = decoder.Decode(&profiles); err != nil || profiles.Version != 1 {
		t.Fatalf("decode profiles: version=%d error=%v", profiles.Version, err)
	}
	want := []string{"clean", "http-bandwidth", "http-half-open", "http-reset", "xmpp-half-open", "xmpp-latency", "xmpp-reset"}
	got := make([]string, 0, len(profiles.Profiles))
	for name, toxics := range profiles.Profiles {
		got = append(got, name)
		for _, value := range toxics {
			if (value.Proxy != "xmpp" && value.Proxy != "object") || (value.Stream != "upstream" && value.Stream != "downstream") || value.Name == "" || value.Toxicity != 1 {
				t.Fatalf("invalid toxic in profile %q: %#v", name, value)
			}
			switch value.Type {
			case "latency", "timeout", "reset_peer", "bandwidth":
			default:
				t.Fatalf("unapproved toxic type %q", value.Type)
			}
		}
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("profile inventory=%v, want %v", got, want)
	}
	return profiles
}

func (api *proxyAPI) ensureProxies(t *testing.T, state string) {
	t.Helper()
	for _, value := range []struct{ name, listen, upstream string }{
		{"xmpp", "0.0.0.0:8666", readFirst(t, filepath.Join(state, "xmpp-upstream"))},
		{"object", "0.0.0.0:8667", readFirst(t, filepath.Join(state, "object-upstream"))},
	} {
		body := map[string]any{"name": value.name, "listen": value.listen, "upstream": value.upstream, "enabled": true}
		status := api.request(t, http.MethodPost, "/proxies", body, http.StatusCreated, http.StatusConflict)
		if status == http.StatusConflict {
			api.request(t, http.MethodPost, "/proxies/"+value.name, body, http.StatusOK)
		}
	}
}

func (api *proxyAPI) apply(t *testing.T, profiles profileFile, name string) {
	t.Helper()
	api.clear(t)
	values, ok := profiles.Profiles[name]
	if !ok {
		t.Fatalf("unknown fault profile %q", name)
	}
	for _, value := range values {
		api.request(t, http.MethodPost, "/proxies/"+value.Proxy+"/toxics", value, http.StatusOK)
	}
	api.assertApplied(t, values)
}

func (api *proxyAPI) assertApplied(t *testing.T, expected []toxic) {
	t.Helper()
	actual := make([]toxic, 0, len(expected))
	for _, proxy := range []string{"xmpp", "object"} {
		request, err := http.NewRequest(http.MethodGet, api.base+"/proxies/"+proxy+"/toxics", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := api.client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var values []toxic
		decodeErr := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&values)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || decodeErr != nil {
			t.Fatalf("read back toxics for %s: status=%d error=%v", proxy, response.StatusCode, decodeErr)
		}
		for index := range values {
			values[index].Proxy = proxy
		}
		actual = append(actual, values...)
	}
	sortToxics := func(values []toxic) {
		sort.Slice(values, func(i, j int) bool {
			if values[i].Proxy == values[j].Proxy {
				return values[i].Name < values[j].Name
			}
			return values[i].Proxy < values[j].Proxy
		})
	}
	want := append(make([]toxic, 0, len(expected)), expected...)
	sortToxics(want)
	sortToxics(actual)
	if len(actual) != len(want) {
		t.Fatalf("installed toxics do not match requested profile:\nactual=%#v\nwant=%#v", actual, want)
	}
	for index := range want {
		if actual[index].Proxy != want[index].Proxy || actual[index].Name != want[index].Name || actual[index].Stream != want[index].Stream || actual[index].Type != want[index].Type || actual[index].Toxicity != want[index].Toxicity || !equalAttributes(actual[index].Attributes, want[index].Attributes) {
			t.Fatalf("installed toxics do not match requested profile:\nactual=%#v\nwant=%#v", actual, want)
		}
	}
}

func equalAttributes(left, right map[string]int64) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func (api *proxyAPI) clear(t *testing.T) {
	t.Helper()
	for _, proxy := range []string{"xmpp", "object"} {
		request, err := http.NewRequest(http.MethodGet, api.base+"/proxies/"+proxy+"/toxics", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := api.client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var values []struct {
			Name string `json:"name"`
		}
		decodeErr := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&values)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || decodeErr != nil {
			t.Fatalf("list toxics for %s: status=%d error=%v", proxy, response.StatusCode, decodeErr)
		}
		for _, value := range values {
			api.request(t, http.MethodDelete, "/proxies/"+proxy+"/toxics/"+value.Name, nil, http.StatusNoContent)
		}
	}
}

func (api *proxyAPI) request(t *testing.T, method, path string, value any, expected ...int) int {
	t.Helper()
	var body io.Reader
	if value != nil {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, api.base+path, body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := api.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	for _, status := range expected {
		if response.StatusCode == status {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			return response.StatusCode
		}
	}
	detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	t.Fatalf("%s %s status=%d body=%q, want %v", method, path, response.StatusCode, detail, expected)
	return response.StatusCode
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	command := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(output))
}

type commandResult struct {
	output   string
	exitCode int
}

func runCommand(t *testing.T, directory string, timeout time.Duration, name string, arguments ...string) string {
	t.Helper()
	result := runCommandWithContext(directory, timeout, name, arguments...)
	if result.exitCode != 0 {
		t.Fatalf("command failed (%d): %s %s\n%s", result.exitCode, name, strings.Join(arguments, " "), result.output)
	}
	return strings.TrimSpace(result.output)
}

func runCommandAllowFailure(directory string, timeout time.Duration, name string, arguments ...string) commandResult {
	return runCommandWithContext(directory, timeout, name, arguments...)
}

func runCommandWithContext(directory string, timeout time.Duration, name string, arguments ...string) commandResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	command := exec.CommandContext(ctx, name, arguments...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GOTOOLCHAIN=go1.26.6")
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		return commandResult{output: string(output) + "\ncontext: " + ctx.Err().Error(), exitCode: -1}
	}
	if err == nil {
		return commandResult{output: string(output), exitCode: 0}
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return commandResult{output: string(output), exitCode: exit.ExitCode()}
	}
	return commandResult{output: string(output) + "\n" + err.Error(), exitCode: -1}
}

func readFirst(t *testing.T, path string) string {
	t.Helper()
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	line, _, _ := strings.Cut(string(value), "\n")
	if line == "" {
		t.Fatalf("empty file: %s", path)
	}
	return line
}

func readEnvironment(t *testing.T, path string) map[string]string {
	t.Helper()
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]string)
	for _, line := range strings.Split(string(value), "\n") {
		if line == "" {
			continue
		}
		key, item, ok := strings.Cut(line, "=")
		if !ok || key == "" {
			t.Fatalf("invalid environment line in %s", path)
		}
		result[key] = item
	}
	return result
}

func readRoots(t *testing.T, path string) *x509.CertPool {
	t.Helper()
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(value) {
		t.Fatalf("invalid CA: %s", path)
	}
	return roots
}
