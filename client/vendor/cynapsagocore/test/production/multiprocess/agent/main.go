// Command agent is a black-box public-API process used only by production
// qualification. Each invocation owns exactly one Core and one identity.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	core "github.com/Cynapsa/cynapsagocore"
	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

type report struct {
	Event              string `json:"event"`
	Role               string `json:"role,omitempty"`
	Path               string `json:"path,omitempty"`
	From               string `json:"from,omitempty"`
	Bytes              int    `json:"bytes,omitempty"`
	Available          bool   `json:"available,omitempty"`
	Connectivity       string `json:"connectivity,omitempty"`
	Reachable          *bool  `json:"reachable,omitempty"`
	RecoveryInProgress *bool  `json:"recovery_in_progress,omitempty"`
	ElapsedMillis      int64  `json:"elapsed_millis,omitempty"`
	At                 int64  `json:"at_unix_milli"`
}

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "sender" && os.Args[1] != "receiver") {
		fmt.Fprintln(os.Stderr, "usage: agent <sender|receiver>")
		os.Exit(2)
	}
	// The process lifetime covers one initial Rank1 attempt, one bounded retry,
	// and the remaining request/fallback assertions. It is a test watchdog, not
	// a Core transport timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	if err := run(ctx, os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, role string) error {
	endpoint := required("CYNAPSA_EJABBERD_ENDPOINT")
	meshID := required("CYNAPSA_MESH_ID")
	localAgent := required("CYNAPSA_AGENT_ID")
	localPassword := required("CYNAPSA_AGENT_PASSWORD")
	peer := required("CYNAPSA_PEER_ID")
	runtime, err := core.New(core.Config{QueueLimit: 32, PayloadLimit: 1 << 20})
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	if err = runtime.Start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	defer func() {
		operation, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = shutdownAndDrain(operation, runtime)
		cancel()
		_ = runtime.Destroy()
	}()
	if _, err = execute(ctx, runtime, v1.AuthConnectCommand{CommandBase: commandBase(role + "-auth"), Auth: v1.AuthInput{MeshEndpoint: endpoint, Username: v1.AgentID(localAgent), Password: localPassword, MeshID: v1.MeshID(meshID), AgentInstanceID: role + "-process"}}); err != nil {
		return fmt.Errorf("authenticate: %w", err)
	}
	if _, err = execute(ctx, runtime, v1.PolicySetCommand{CommandBase: commandBase(role + "-policy"), Rules: []v1.PolicyRule{{Action: v1.PolicyActionAllow, AgentID: v1.AgentID(peer)}}}); err != nil {
		return fmt.Errorf("policy: %w", err)
	}
	emit(report{Event: "ready", Role: role})
	expectLive := os.Getenv("CYNAPSA_EXPECT_LIVE") != "false"
	recovery := os.Getenv("CYNAPSA_RECOVERY") == "true"
	if os.Getenv("CYNAPSA_REMOTE_SMOKE") == "true" {
		nonce := required("CYNAPSA_SMOKE_NONCE")
		if role == "receiver" {
			return runSmokeReceiver(ctx, runtime, endpoint, localAgent, peer, nonce)
		}
		return runSmokeSender(ctx, runtime, endpoint, peer, nonce)
	}
	if role == "receiver" {
		return runReceiver(ctx, runtime, localAgent, peer, expectLive, recovery)
	}
	return runSender(ctx, runtime, peer, expectLive, recovery)
}

const (
	// Cover Core's initial bounded establishment, one backoff retry, and the
	// stable-health observation. Keeping the test deadline equal to one Core
	// attempt made a successful retry impossible to observe.
	smokeEstablishmentTimeout = 70 * time.Second
	smokePhaseTimeout         = 10 * time.Second
)

func runSmokeReceiver(ctx context.Context, runtime *core.Core, endpoint, localAgent, peer, nonce string) error {
	var err error
	if err = waitForControl(ctx, "/control/establish-released"); err != nil {
		return err
	}
	if err = runSmokeReceiverEstablishment(ctx, runtime, peer, nonce); err != nil {
		return fmt.Errorf("receiver initial peer: %w", err)
	}
	emit(report{Event: "smoke-barrier-ready", Role: "receiver", Available: true})
	if err = waitForControl(ctx, "/control/smoke-released"); err != nil {
		return err
	}
	if err = proveXMPPBlackhole(endpoint); err != nil {
		return err
	}
	if err = runSmokeHealthPhase(ctx, runtime, peer); err != nil {
		return fmt.Errorf("receiver pre-exchange peer: %w", err)
	}
	emit(report{Event: "peer-before", Role: "receiver", Available: true})
	if err = waitForControl(ctx, "/control/request-released"); err != nil {
		return err
	}
	if err = runSmokeReceiverRequestPhase(ctx, runtime, peer, nonce); err != nil {
		return err
	}
	emit(report{Event: "request-phase-complete", Role: "receiver"})
	if err = waitForControl(ctx, "/control/reverse-released"); err != nil {
		return err
	}
	if err = runSmokeReceiverReversePhase(ctx, runtime, peer, nonce); err != nil {
		return err
	}
	emit(report{Event: "reverse-phase-complete", Role: "receiver"})
	if err = waitForControl(ctx, "/control/post-health-released"); err != nil {
		return err
	}
	if err = runSmokeHealthPhase(ctx, runtime, peer); err != nil {
		return fmt.Errorf("receiver post-exchange peer: %w", err)
	}
	emit(report{Event: "peer-after", Role: "receiver", Available: true})
	emit(report{Event: "terminal-ready", Role: "receiver", From: localAgent})
	if err = waitForControl(ctx, "/control/finish-released"); err != nil {
		return err
	}
	emit(report{Event: "complete", Role: "receiver", From: localAgent})
	return nil
}

func runSmokeSender(ctx context.Context, runtime *core.Core, endpoint, peer, nonce string) error {
	var err error
	if err = waitForControl(ctx, "/control/establish-released"); err != nil {
		return err
	}
	if err = runSmokeSenderEstablishment(ctx, runtime, peer, nonce); err != nil {
		return err
	}
	emit(report{Event: "smoke-barrier-ready", Role: "sender", Available: true})
	if err = waitForControl(ctx, "/control/smoke-released"); err != nil {
		return err
	}
	if err = proveXMPPBlackhole(endpoint); err != nil {
		return err
	}
	if err = runSmokeHealthPhase(ctx, runtime, peer); err != nil {
		return fmt.Errorf("sender pre-exchange peer: %w", err)
	}
	emit(report{Event: "peer-before", Role: "sender", Available: true})
	if err = waitForControl(ctx, "/control/request-released"); err != nil {
		return err
	}
	if err = runSmokeSenderRequestPhase(ctx, runtime, peer, nonce); err != nil {
		return err
	}
	emit(report{Event: "request-phase-complete", Role: "sender"})
	if err = waitForControl(ctx, "/control/reverse-released"); err != nil {
		return err
	}
	if err = runSmokeSenderReversePhase(ctx, runtime, peer, nonce); err != nil {
		return err
	}
	emit(report{Event: "reverse-phase-complete", Role: "sender"})
	if err = waitForControl(ctx, "/control/post-health-released"); err != nil {
		return err
	}
	if err = runSmokeHealthPhase(ctx, runtime, peer); err != nil {
		return fmt.Errorf("sender post-exchange peer: %w", err)
	}
	emit(report{Event: "peer-after", Role: "sender", Available: true})
	emit(report{Event: "terminal-ready", Role: "sender"})
	if err = waitForControl(ctx, "/control/finish-released"); err != nil {
		return err
	}
	emit(report{Event: "complete", Role: "sender"})
	return nil
}

func runSmokeReceiverEstablishment(ctx context.Context, runtime *core.Core, peer, nonce string) error {
	phase, cancel := context.WithTimeout(ctx, smokeEstablishmentTimeout)
	defer cancel()
	return establishSmokeReceiver(
		phase,
		peer,
		nonce,
		func(ctx context.Context) (v1.Event, v1.MessageReceivedEvent, error) {
			return nextMessage(ctx, runtime)
		},
		func(ctx context.Context, eventID v1.EventID) error {
			_, err := execute(ctx, runtime, v1.DeliveryAcceptCommand{CommandBase: commandBase("smoke-establish-accept"), EventID: eventID})
			return err
		},
		func(ctx context.Context) error {
			_, err := waitPeerFor(ctx, runtime, peer, true, smokeEstablishmentTimeout)
			return err
		},
	)
}

func runSmokeSenderEstablishment(ctx context.Context, runtime *core.Core, peer, nonce string) error {
	phase, cancel := context.WithTimeout(ctx, smokeEstablishmentTimeout)
	defer cancel()
	return establishSmokeSender(
		phase,
		func(ctx context.Context) error {
			result, err := execute(ctx, runtime, v1.MessageSendCommand{CommandBase: commandBase("smoke-establish"), To: v1.AgentID(peer), Payload: nativePayload("/remote-smoke/establish/"+nonce, "rank1-bootstrap")})
			if err != nil {
				return fmt.Errorf("smoke establish: %w", err)
			}
			if sent, ok := result.(v1.SendResult); !ok || !sent.Accepted {
				return fmt.Errorf("smoke establish result: %#v", result)
			}
			return nil
		},
		func(ctx context.Context) error {
			if _, err := waitPeerFor(ctx, runtime, peer, true, smokeEstablishmentTimeout); err != nil {
				return fmt.Errorf("sender initial peer after bootstrap: %w", err)
			}
			return nil
		},
	)
}

type smokeMessageReader func(context.Context) (v1.Event, v1.MessageReceivedEvent, error)
type smokeEventAcceptor func(context.Context, v1.EventID) error
type smokeBootstrapSender func(context.Context) error
type smokeHealthWaiter func(context.Context) error

func establishSmokeSender(ctx context.Context, send smokeBootstrapSender, waitHealthy smokeHealthWaiter) error {
	if err := send(ctx); err != nil {
		return err
	}
	return waitHealthy(ctx)
}

func establishSmokeReceiver(ctx context.Context, peer, nonce string, read smokeMessageReader, accept smokeEventAcceptor, waitHealthy smokeHealthWaiter) error {
	event, message, err := read(ctx)
	if err != nil {
		return fmt.Errorf("receive smoke bootstrap: %w", err)
	}
	if err = validateSmokeBootstrap(peer, nonce, message); err != nil {
		return err
	}
	if err = accept(ctx, event.ID); err != nil {
		return fmt.Errorf("accept smoke bootstrap: %w", err)
	}
	return waitHealthy(ctx)
}

func validateSmokeBootstrap(peer, nonce string, message v1.MessageReceivedEvent) error {
	native, ok := message.Payload.Value.(v1.NativePayload)
	if !ok || message.FromAgentID != v1.AgentID(peer) || message.Mode != v1.MessageModeOneWay || native.Path != "/remote-smoke/establish/"+nonce || string(native.Body) != "rank1-bootstrap" || message.RequestHandle != "" {
		return fmt.Errorf("smoke bootstrap mismatch: %#v", message)
	}
	return nil
}

func runSmokeHealthPhase(ctx context.Context, runtime *core.Core, peer string) error {
	phase, cancel := context.WithTimeout(ctx, smokePhaseTimeout)
	defer cancel()
	_, err := waitPeer(phase, runtime, peer, true)
	return err
}

func runSmokeReceiverRequestPhase(ctx context.Context, runtime *core.Core, peer, nonce string) error {
	phase, cancel := context.WithTimeout(ctx, smokePhaseTimeout)
	defer cancel()
	event, message, err := awaitSmokeRequest(phase, peer, nonce, func(ctx context.Context) (v1.Event, v1.MessageReceivedEvent, error) {
		return nextMessage(ctx, runtime)
	})
	if err != nil {
		return err
	}
	native, ok := message.Payload.Value.(v1.NativePayload)
	// awaitSmokeRequest validated the exact type and payload before returning.
	if !ok {
		return fmt.Errorf("smoke request payload changed after validation: %#v", message)
	}
	if _, err = execute(phase, runtime, v1.DeliveryAcceptCommand{CommandBase: commandBase("smoke-request-accept"), EventID: event.ID}); err != nil {
		return fmt.Errorf("accept smoke request: %w", err)
	}
	emit(report{Event: "message", Role: "receiver", Path: native.Path, From: string(message.FromAgentID), Bytes: len(native.Body)})
	emit(report{Event: "accepted", Role: "receiver", Path: native.Path, From: string(message.FromAgentID), Bytes: len(native.Body)})
	result, err := execute(phase, runtime, v1.MessageReplyCommand{CommandBase: commandBase("smoke-reply"), RequestHandle: message.RequestHandle, Payload: nativePayload("/remote-smoke/reply/"+nonce+"-b", "reply-b-to-a")})
	if err != nil {
		return fmt.Errorf("smoke reply: %w", err)
	}
	if sent, valid := result.(v1.SendResult); !valid || !sent.Accepted {
		return fmt.Errorf("smoke reply result: %#v", result)
	}
	return nil
}

func awaitSmokeRequest(ctx context.Context, peer, nonce string, read smokeMessageReader) (v1.Event, v1.MessageReceivedEvent, error) {
	event, message, err := read(ctx)
	if err != nil {
		return v1.Event{}, v1.MessageReceivedEvent{}, fmt.Errorf("receive smoke request: %w", err)
	}
	native, ok := message.Payload.Value.(v1.NativePayload)
	if !ok || message.FromAgentID != v1.AgentID(peer) || message.Mode != v1.MessageModeRequest || native.Path != "/remote-smoke/request/"+nonce+"-a" || string(native.Body) != "request-a-to-b" || message.RequestHandle == "" {
		return v1.Event{}, v1.MessageReceivedEvent{}, fmt.Errorf("smoke request mismatch: %#v", message)
	}
	return event, message, nil
}

func runSmokeReceiverReversePhase(ctx context.Context, runtime *core.Core, peer, nonce string) error {
	phase, cancel := context.WithTimeout(ctx, smokePhaseTimeout)
	defer cancel()
	result, err := execute(phase, runtime, v1.MessageSendCommand{CommandBase: commandBase("smoke-reverse"), To: v1.AgentID(peer), Payload: nativePayload("/remote-smoke/message/"+nonce+"-b", "message-b-to-a")})
	if err != nil {
		return fmt.Errorf("smoke reverse message: %w", err)
	}
	if sent, valid := result.(v1.SendResult); !valid || !sent.Accepted {
		return fmt.Errorf("smoke reverse result: %#v", result)
	}
	return nil
}

func runSmokeSenderRequestPhase(ctx context.Context, runtime *core.Core, peer, nonce string) error {
	phase, cancel := context.WithTimeout(ctx, smokePhaseTimeout)
	defer cancel()
	response, err := request(phase, runtime, "smoke-request", peer, "/remote-smoke/request/"+nonce+"-a", "request-a-to-b")
	if err != nil {
		return fmt.Errorf("smoke request: %w", err)
	}
	native, ok := response.Payload.Value.(v1.NativePayload)
	if !ok || response.FromAgentID != v1.AgentID(peer) || native.Path != "/remote-smoke/reply/"+nonce+"-b" || string(native.Body) != "reply-b-to-a" {
		return fmt.Errorf("smoke response mismatch: %#v", response)
	}
	emit(report{Event: "response", Role: "sender", Path: native.Path, From: string(response.FromAgentID), Bytes: len(native.Body)})
	return nil
}

func runSmokeSenderReversePhase(ctx context.Context, runtime *core.Core, peer, nonce string) error {
	phase, cancel := context.WithTimeout(ctx, smokePhaseTimeout)
	defer cancel()
	event, message, err := nextMessage(phase, runtime)
	if err != nil {
		return fmt.Errorf("smoke reverse receive: %w", err)
	}
	native, ok := message.Payload.Value.(v1.NativePayload)
	if !ok || message.FromAgentID != v1.AgentID(peer) || message.Mode != v1.MessageModeOneWay || native.Path != "/remote-smoke/message/"+nonce+"-b" || string(native.Body) != "message-b-to-a" {
		return fmt.Errorf("smoke reverse mismatch: %#v", message)
	}
	emit(report{Event: "message", Role: "sender", Path: native.Path, From: string(message.FromAgentID), Bytes: len(native.Body)})
	if _, err = execute(phase, runtime, v1.DeliveryAcceptCommand{CommandBase: commandBase("smoke-reverse-accept"), EventID: event.ID}); err != nil {
		return fmt.Errorf("accept smoke reverse: %w", err)
	}
	emit(report{Event: "accepted", Role: "sender", Path: native.Path, From: string(message.FromAgentID), Bytes: len(native.Body)})
	return nil
}

func proveXMPPBlackhole(endpoint string) error {
	return proveXMPPBlackholeWithDial(endpoint, func(network, address string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout(network, address, timeout)
	})
}

func proveXMPPBlackholeWithDial(endpoint string, dial func(string, string, time.Duration) (net.Conn, error)) error {
	connection, err := dial("tcp", endpoint, 250*time.Millisecond)
	if err == nil {
		_ = connection.Close()
		return errors.New("XMPP remained reachable after the smoke barrier")
	}
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		return fmt.Errorf("XMPP blackhole did not produce a bounded timeout: %w", err)
	}
	return nil
}

func runReceiver(ctx context.Context, runtime *core.Core, localAgent, peer string, expectLive, recovery bool) error {
	want := []struct {
		mode v1.MessageMode
		path string
	}{
		{mode: v1.MessageModeOneWay, path: "/p2p/establish"},
		{mode: v1.MessageModeRequest, path: "/p2p/request"},
		{mode: v1.MessageModeRequest, path: "/p2p/done"},
	}
	if !expectLive {
		want = want[:2]
	} else if recovery {
		want = []struct {
			mode v1.MessageMode
			path string
		}{
			{mode: v1.MessageModeOneWay, path: "/p2p/establish"},
			{mode: v1.MessageModeRequest, path: "/p2p/before-fault"},
			{mode: v1.MessageModeRequest, path: "/p2p/during-fault"},
			{mode: v1.MessageModeOneWay, path: "/p2p/recover-trigger"},
			{mode: v1.MessageModeRequest, path: "/p2p/after-recovery"},
		}
	}
	want = append(want, struct {
		mode v1.MessageMode
		path string
	}{mode: v1.MessageModeRequest, path: "/p2p/complete"})
	for index, expected := range want {
		event, message, err := nextMessage(ctx, runtime)
		if err != nil {
			return err
		}
		native, ok := message.Payload.Value.(v1.NativePayload)
		if !ok || message.FromAgentID != v1.AgentID(peer) || message.Mode != expected.mode || native.Path != expected.path {
			return fmt.Errorf("message %d mismatch: %#v", index, message)
		}
		emit(report{Event: "message", Role: "receiver", Path: native.Path, From: string(message.FromAgentID), Bytes: len(native.Body)})
		if _, err = execute(ctx, runtime, v1.DeliveryAcceptCommand{CommandBase: commandBase(fmt.Sprintf("accept-%d", index)), EventID: event.ID}); err != nil {
			return fmt.Errorf("accept %d: %w", index, err)
		}
		if index == 0 && !expectLive {
			result, sendErr := execute(ctx, runtime, v1.MessageSendCommand{CommandBase: commandBase("establish-back"), To: v1.AgentID(peer), Payload: nativePayload("/p2p/establish-back", "durable-bootstrap-back")})
			if sendErr != nil {
				return fmt.Errorf("reverse establish send: %w", sendErr)
			}
			if sent, ok := result.(v1.SendResult); !ok || !sent.Accepted {
				return fmt.Errorf("reverse establish result: %#v", result)
			}
		}
		if expected.mode == v1.MessageModeRequest {
			if message.RequestHandle == "" {
				return errors.New("request handle missing")
			}
			replyPath, replyBody := "/p2p/reply", "relay-response"
			replyID := "reply"
			if expected.path == "/p2p/done" {
				replyPath, replyBody = "/p2p/done-ack", "done-ack"
				replyID = "done-reply"
			} else if expected.path == "/p2p/complete" {
				replyPath, replyBody = "/p2p/complete-ack", "complete-ack"
				replyID = "complete-reply"
			} else if recovery {
				replyPath = expected.path + "-ack"
				replyBody = strings.TrimPrefix(expected.path, "/p2p/") + "-ack"
				replyID = fmt.Sprintf("recovery-reply-%d", index)
			}
			result, replyErr := execute(ctx, runtime, v1.MessageReplyCommand{CommandBase: commandBase(replyID), RequestHandle: message.RequestHandle, Payload: nativePayload(replyPath, replyBody)})
			if replyErr != nil {
				return fmt.Errorf("reply: %w", replyErr)
			}
			if sent, ok := result.(v1.SendResult); !ok || !sent.Accepted {
				return fmt.Errorf("reply result: %#v", result)
			}
		}
	}
	emit(report{Event: "terminal-ready", Role: "receiver", From: localAgent})
	if err := waitForControl(ctx, "/control/finish-released"); err != nil {
		return err
	}
	emit(report{Event: "complete", Role: "receiver", From: localAgent})
	return nil
}

func runSender(ctx context.Context, runtime *core.Core, peer string, expectLive, recovery bool) error {
	result, err := execute(ctx, runtime, v1.MessageSendCommand{CommandBase: commandBase("establish"), To: v1.AgentID(peer), Payload: nativePayload("/p2p/establish", "durable-bootstrap")})
	if err != nil {
		return fmt.Errorf("establish send: %w", err)
	}
	if sent, ok := result.(v1.SendResult); !ok || !sent.Accepted {
		return fmt.Errorf("establish result: %#v", result)
	}
	if !expectLive {
		event, message, receiveErr := nextMessage(ctx, runtime)
		if receiveErr != nil {
			return fmt.Errorf("reverse establish receive: %w", receiveErr)
		}
		native, ok := message.Payload.Value.(v1.NativePayload)
		if !ok || message.FromAgentID != v1.AgentID(peer) || message.Mode != v1.MessageModeOneWay || native.Path != "/p2p/establish-back" || string(native.Body) != "durable-bootstrap-back" {
			return fmt.Errorf("reverse establish mismatch: %#v", message)
		}
		if _, receiveErr = execute(ctx, runtime, v1.DeliveryAcceptCommand{CommandBase: commandBase("establish-back-accept"), EventID: event.ID}); receiveErr != nil {
			return fmt.Errorf("reverse establish accept: %w", receiveErr)
		}
		emit(report{Event: "message", Role: "sender", Path: native.Path, From: string(message.FromAgentID), Bytes: len(native.Body)})
	}
	available, err := waitPeer(ctx, runtime, peer, expectLive)
	if err != nil {
		return err
	}
	emit(report{Event: "peer", Role: "sender", Available: available})
	if recovery {
		return runRecoverySender(ctx, runtime, peer)
	}

	response, err := request(ctx, runtime, "request", peer, "/p2p/request", "relay-request")
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	if response.FromAgentID != v1.AgentID(peer) {
		return fmt.Errorf("response: %#v", response)
	}
	native, ok := response.Payload.Value.(v1.NativePayload)
	if !ok || native.Path != "/p2p/reply" || string(native.Body) != "relay-response" {
		return fmt.Errorf("response payload: %#v", response.Payload)
	}
	emit(report{Event: "response", Role: "sender", Path: native.Path, From: string(response.FromAgentID), Bytes: len(native.Body)})

	if expectLive {
		response, requestErr := request(ctx, runtime, "done", peer, "/p2p/done", "done")
		if requestErr != nil {
			return fmt.Errorf("done request: %w", requestErr)
		}
		native, ok := response.Payload.Value.(v1.NativePayload)
		if !ok || response.FromAgentID != v1.AgentID(peer) || native.Path != "/p2p/done-ack" || string(native.Body) != "done-ack" {
			return fmt.Errorf("done response: %#v", response)
		}
	}
	if err = sendCompletionAck(ctx, runtime, peer); err != nil {
		return err
	}
	emit(report{Event: "terminal-ready", Role: "sender"})
	if err = waitForControl(ctx, "/control/finish-released"); err != nil {
		return err
	}
	emit(report{Event: "complete", Role: "sender"})
	return nil
}

func runRecoverySender(ctx context.Context, runtime *core.Core, peer string) error {
	before, err := request(ctx, runtime, "before-fault", peer, "/p2p/before-fault", "before-fault")
	if err != nil {
		return fmt.Errorf("before fault: %w", err)
	}
	if err = expectResponse(before, peer, "/p2p/before-fault-ack", "before-fault-ack"); err != nil {
		return err
	}
	emit(report{Event: "response", Role: "sender", Path: "/p2p/before-fault-ack", From: string(before.FromAgentID), Bytes: len("before-fault-ack")})
	emit(report{Event: "fault-ready", Role: "sender"})
	if err = waitForControl(ctx, "/control/drop-released"); err != nil {
		return err
	}
	during, err := request(ctx, runtime, "during-fault", peer, "/p2p/during-fault", "during-fault")
	if err != nil {
		return fmt.Errorf("during fault fallback: %w", err)
	}
	if err = expectResponse(during, peer, "/p2p/during-fault-ack", "during-fault-ack"); err != nil {
		return err
	}
	emit(report{Event: "response", Role: "sender", Path: "/p2p/during-fault-ack", From: string(during.FromAgentID), Bytes: len("during-fault-ack")})
	emit(report{Event: "fallback", Role: "sender"})
	if err = waitForControl(ctx, "/control/restore-released"); err != nil {
		return err
	}
	result, err := execute(ctx, runtime, v1.MessageSendCommand{CommandBase: commandBase("recover-trigger"), To: v1.AgentID(peer), Payload: nativePayload("/p2p/recover-trigger", "recover-trigger")})
	if err != nil {
		return fmt.Errorf("recovery trigger: %w", err)
	}
	if sent, ok := result.(v1.SendResult); !ok || !sent.Accepted {
		return fmt.Errorf("recovery trigger result: %#v", result)
	}
	// Require bounded path recovery after the controller restores UDP. This is
	// a functional recovery assertion; elapsed time alone does not distinguish
	// an in-place ICE restart from replacement of a missing link.
	if _, err = waitPeerFor(ctx, runtime, peer, true, 25*time.Second); err != nil {
		return fmt.Errorf("refreshed link: %w", err)
	}
	after, err := request(ctx, runtime, "after-recovery", peer, "/p2p/after-recovery", "after-recovery")
	if err != nil {
		return fmt.Errorf("after recovery: %w", err)
	}
	if err = expectResponse(after, peer, "/p2p/after-recovery-ack", "after-recovery-ack"); err != nil {
		return err
	}
	emit(report{Event: "response", Role: "sender", Path: "/p2p/after-recovery-ack", From: string(after.FromAgentID), Bytes: len("after-recovery-ack")})
	if err = sendCompletionAck(ctx, runtime, peer); err != nil {
		return err
	}
	emit(report{Event: "terminal-ready", Role: "sender"})
	if err = waitForControl(ctx, "/control/finish-released"); err != nil {
		return err
	}
	emit(report{Event: "recovered", Role: "sender", Available: true})
	emit(report{Event: "complete", Role: "sender"})
	return nil
}

func sendCompletionAck(ctx context.Context, runtime *core.Core, peer string) error {
	response, err := request(ctx, runtime, "complete-ack", peer, "/p2p/complete", "complete")
	if err != nil {
		return fmt.Errorf("completion acknowledgement: %w", err)
	}
	if err = expectResponse(response, peer, "/p2p/complete-ack", "complete-ack"); err != nil {
		return fmt.Errorf("completion acknowledgement result: %w", err)
	}
	return nil
}

func expectResponse(response v1.ResponseResult, peer, path, body string) error {
	native, ok := response.Payload.Value.(v1.NativePayload)
	if !ok || response.FromAgentID != v1.AgentID(peer) || native.Path != path || string(native.Body) != body {
		return fmt.Errorf("response mismatch: %#v", response)
	}
	return nil
}

func waitForControl(ctx context.Context, path string) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func request(ctx context.Context, runtime *core.Core, id, peer, path, body string) (v1.ResponseResult, error) {
	admission, err := runtime.Submit(ctx, v1.MessageRequestCommand{CommandBase: commandBase(id), To: v1.AgentID(peer), Payload: nativePayload(path, body), TTL: 30 * time.Second})
	if err != nil || !admission.Accepted {
		return v1.ResponseResult{}, fmt.Errorf("admission: %#v, %w", admission, err)
	}
	completion, err := runtime.NextCompletion(ctx)
	if err != nil {
		return v1.ResponseResult{}, fmt.Errorf("completion read: %w", err)
	}
	if !completion.OK {
		if completion.Error != nil {
			diagnostic, diagnosticErr := execute(ctx, runtime, v1.DiagnosticsConnectivityCommand{CommandBase: commandBase(fmt.Sprintf("request-failure-%d", time.Now().UnixNano()))})
			peerDiagnostic, peerDiagnosticErr := execute(ctx, runtime, v1.DiagnosticsPeerCommand{CommandBase: commandBase(fmt.Sprintf("request-peer-failure-%d", time.Now().UnixNano())), Peer: v1.AgentID(peer)})
			return v1.ResponseResult{}, fmt.Errorf("completion %s: code=%s stage=%s retryable=%t connectivity=%#v connectivity_error=%v peer=%#v peer_error=%v message=%w", completion.CommandID, completion.Error.Code, completion.Error.Stage, completion.Error.Retryable, diagnostic, diagnosticErr, peerDiagnostic, peerDiagnosticErr, completion.Error)
		}
		return v1.ResponseResult{}, fmt.Errorf("completion %s failed without error", completion.CommandID)
	}
	response, ok := completion.Result.(v1.ResponseResult)
	if !ok {
		return v1.ResponseResult{}, fmt.Errorf("result: %#v", completion.Result)
	}
	return response, nil
}

func execute(ctx context.Context, runtime *core.Core, command v1.Command) (v1.Result, error) {
	admission, err := runtime.Submit(ctx, command)
	if err != nil {
		return nil, err
	}
	if !admission.Accepted {
		if admission.Error != nil {
			return nil, admission.Error
		}
		return nil, errors.New("command not accepted")
	}
	completion, err := runtime.NextCompletion(ctx)
	if err != nil {
		return nil, err
	}
	if completion.CommandID != admission.CommandID || !completion.OK {
		if completion.Error != nil {
			return nil, completion.Error
		}
		return nil, errors.New("command did not complete successfully")
	}
	return completion.Result, nil
}

func nextMessage(ctx context.Context, runtime *core.Core) (v1.Event, v1.MessageReceivedEvent, error) {
	for {
		event, err := runtime.NextEvent(ctx)
		if err != nil {
			return v1.Event{}, v1.MessageReceivedEvent{}, err
		}
		if event.Name != v1.EventMessageReceived {
			continue
		}
		message, ok := event.Payload.(v1.MessageReceivedEvent)
		if !ok {
			return v1.Event{}, v1.MessageReceivedEvent{}, errors.New("message event payload mismatch")
		}
		return event, message, nil
	}
}

func waitPeer(ctx context.Context, runtime *core.Core, peer string, want bool) (bool, error) {
	// A positive row permits the same initial attempt plus one bounded retry as
	// production Core. The extra margin covers cooldown and the 300 ms stable
	// health requirement without changing Core's own attempt timeout.
	timeout := 70 * time.Second
	if !want {
		// A negative row must also cover the automatic retry and observe it leave
		// RecoveryInProgress before exercising the durable path. Starting a
		// request sooner makes its TTL race the independent establishment budget.
		timeout = 70 * time.Second
	}
	return waitPeerFor(ctx, runtime, peer, want, timeout)
}

func waitPeerFor(ctx context.Context, runtime *core.Core, peer string, want bool, timeout time.Duration) (bool, error) {
	started := time.Now()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	var last v1.PeerStatus
	var lastObserved v1.PeerStatus
	haveObservation := false
	var availableSince time.Time
	sawRecovery := false
	for {
		result, err := execute(ctx, runtime, v1.DiagnosticsPeerCommand{CommandBase: commandBase(fmt.Sprintf("peer-%d", time.Now().UnixNano())), Peer: v1.AgentID(peer)})
		if err != nil {
			return false, err
		}
		status, ok := result.(v1.PeerStatus)
		if !ok {
			return false, fmt.Errorf("peer result: %#v", result)
		}
		last = status
		if !haveObservation || status != lastObserved {
			reachable, recovering := status.Reachable, status.RecoveryInProgress
			emit(report{
				Event: "peer-observation", Connectivity: string(status.Connectivity),
				Reachable: &reachable, RecoveryInProgress: &recovering,
				ElapsedMillis: time.Since(started).Milliseconds(),
			})
			lastObserved = status
			haveObservation = true
		}
		sawRecovery = sawRecovery || status.RecoveryInProgress
		available := status.Reachable && !status.RecoveryInProgress && string(status.Connectivity) == "available"
		if want {
			if available {
				if availableSince.IsZero() {
					availableSince = time.Now()
				} else if time.Since(availableSince) >= 300*time.Millisecond {
					return true, nil
				}
			} else {
				availableSince = time.Time{}
			}
		}
		if !want && available {
			return true, errors.New("peer unexpectedly became available")
		}
		if !want && sawRecovery && !status.RecoveryInProgress && time.Since(started) >= 5*time.Second {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-deadline.C:
			if !want {
				return false, nil
			}
			return false, peerTimeoutError(ctx, runtime, peer, last)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func peerTimeoutError(ctx context.Context, runtime *core.Core, peer string, last v1.PeerStatus) error {
	diagnosticContext, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	connectivity, connectivityErr := execute(diagnosticContext, runtime, v1.DiagnosticsConnectivityCommand{CommandBase: commandBase(fmt.Sprintf("peer-timeout-connectivity-%d", time.Now().UnixNano()))})
	snapshot, snapshotErr := execute(diagnosticContext, runtime, v1.DiagnosticsSnapshotCommand{CommandBase: commandBase(fmt.Sprintf("peer-timeout-snapshot-%d", time.Now().UnixNano()))})
	return fmt.Errorf("%w: peer=%#v connectivity=%#v connectivity_error=%v snapshot=%#v snapshot_error=%v",
		errPeerUnavailable, last, connectivity, connectivityErr, snapshot, snapshotErr)
}

var errPeerUnavailable = errors.New("peer did not become available")

func shutdownAndDrain(ctx context.Context, runtime *core.Core) error {
	result := make(chan error, 1)
	go func() { result <- runtime.Shutdown(ctx) }()
	for {
		select {
		case err := <-result:
			return err
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		wait, cancel := context.WithTimeout(ctx, 2*time.Millisecond)
		_, completionErr := runtime.NextCompletion(wait)
		cancel()
		if !expectedDrainError(completionErr) {
			return completionErr
		}
		wait, cancel = context.WithTimeout(ctx, 2*time.Millisecond)
		_, eventErr := runtime.NextEvent(wait)
		cancel()
		if !expectedDrainError(eventErr) {
			return eventErr
		}
	}
}

func expectedDrainError(err error) bool {
	if err == nil {
		return true
	}
	var public *v1.Error
	return errors.As(err, &public) && (public.Code == v1.ErrorCodeDeliveryTimeout || public.Code == v1.ErrorCodeShutdownInProgress)
}

func nativePayload(path, body string) v1.Payload {
	return v1.Payload{Value: v1.NativePayload{ContentType: "application/octet-stream", Path: path, Body: []byte(body)}}
}

func commandBase(id string) v1.CommandBase {
	id = strings.ReplaceAll(id, "_", "-")
	return v1.CommandBase{CommandID: v1.CommandID("production-" + id), SDKSessionID: "production-session"}
}

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		fmt.Fprintf(os.Stderr, "required environment missing: %s\n", name)
		os.Exit(2)
	}
	return value
}

func emit(value report) {
	value.At = time.Now().UnixMilli()
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))
}
