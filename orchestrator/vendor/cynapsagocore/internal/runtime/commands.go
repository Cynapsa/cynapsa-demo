package runtime

import (
	"sort"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

var allowedCommands = map[string]struct{}{
	"core.init": {}, "core.capabilities": {}, "core.status": {}, "core.shutdown": {},
	"session.config.get": {}, "session.config.update": {},
	"command.channel.register": {}, "command.channel.clear": {}, "command.cancel": {},
	"event.sink.register": {}, "event.sink.clear": {}, "event.sink.bind": {},
	"auth.login": {}, "auth.connect": {}, "auth.logout": {}, "auth.agent_id": {},
	"auth.token_login": {}, "auth.token_connect": {},
	"auth.installation_login": {}, "auth.installation_connect": {},
	"mesh.list": {}, "mesh.membership.refresh": {},
	"address.map.put": {}, "address.map.remove": {}, "address.map.list": {}, "address.resolve": {},
	"message.send": {}, "message.request": {}, "message.reply": {},
	"delivery.next": {}, "delivery.accept": {}, "delivery.queue.status": {}, "delivery.retry": {},
	"delivery.pause": {}, "delivery.resume": {}, "delivery.drop": {},
	"handler.register": {}, "handler.unregister": {},
	"payload.open": {}, "payload.write_chunk": {}, "payload.finish": {}, "payload.cancel": {},
	"payload.read": {}, "payload.close": {}, "payload.retain": {}, "payload.release": {},
	"conversation.list": {}, "conversation.status": {}, "conversation.close": {},
	"policy.set": {}, "policy.get": {}, "policy.test": {},
	"diagnostics.peer_status": {}, "diagnostics.connectivity_status": {},
	"diagnostics.snapshot": {}, "diagnostics.logs.subscribe": {},
}

var sortedCommandCatalog = func() []string {
	catalog := make([]string, 0, len(allowedCommands))
	for name := range allowedCommands {
		catalog = append(catalog, name)
	}
	sort.Strings(catalog)
	return catalog
}()

// CommandCatalog returns the complete private runtime command allowlist in
// stable lexical order. The returned slice is owned by the caller.
func CommandCatalog() []string {
	return append([]string(nil), sortedCommandCatalog...)
}

func knownCommand(name string) bool {
	_, ok := allowedCommands[name]
	return ok
}

func validCommandArgs(name string, args model.CommandArgs) bool {
	switch name {
	case "core.init", "core.capabilities", "core.status", "core.shutdown",
		"session.config.get", "auth.logout", "auth.agent_id", "mesh.list",
		"mesh.membership.refresh", "address.map.list", "delivery.next",
		"delivery.queue.status", "delivery.pause", "delivery.resume", "payload.open",
		"conversation.list", "policy.get", "diagnostics.connectivity_status",
		"diagnostics.snapshot":
		return isArgs[model.EmptyArgs](args)
	case "session.config.update":
		return isArgs[model.ConfigUpdateArgs](args)
	case "command.channel.register":
		return isArgs[model.ChannelRegisterArgs](args)
	case "command.channel.clear":
		return isArgs[model.ChannelIDArgs](args)
	case "command.cancel":
		return isArgs[model.CommandCancelArgs](args)
	case "event.sink.register":
		return isArgs[model.EventSinkRegisterArgs](args)
	case "event.sink.clear", "event.sink.bind":
		return isArgs[model.EventSinkIDArgs](args)
	case "auth.login", "auth.connect":
		return isArgs[model.AuthArgs](args)
	case "auth.token_login", "auth.token_connect":
		return isArgs[model.TokenAuthArgs](args)
	case "auth.installation_login", "auth.installation_connect":
		return isArgs[model.InstallationAuthArgs](args)
	case "address.map.put":
		return isArgs[model.AddressPutArgs](args)
	case "address.map.remove":
		return isArgs[model.AddressRemoveArgs](args)
	case "address.resolve":
		return isArgs[model.AddressResolveArgs](args)
	case "message.send":
		return isArgs[model.MessageSendArgs](args)
	case "message.request":
		return isArgs[model.MessageRequestArgs](args)
	case "message.reply":
		return isArgs[model.MessageReplyArgs](args)
	case "delivery.accept":
		return isArgs[model.DeliveryAcceptArgs](args)
	case "delivery.retry", "delivery.drop":
		return isArgs[model.MessageIDArgs](args)
	case "handler.register", "handler.unregister":
		return isArgs[model.HandlerPathArgs](args)
	case "payload.write_chunk":
		return isArgs[model.PayloadWriteArgs](args)
	case "payload.finish", "payload.cancel", "payload.close", "payload.retain", "payload.release":
		return isArgs[model.PayloadHandleArgs](args)
	case "payload.read":
		return isArgs[model.PayloadReadArgs](args)
	case "conversation.status", "conversation.close":
		return isArgs[model.ConversationIDArgs](args)
	case "policy.set":
		return isArgs[model.PolicySetArgs](args)
	case "policy.test":
		return isArgs[model.PolicyTestArgs](args)
	case "diagnostics.peer_status":
		return isArgs[model.DiagnosticsPeerArgs](args)
	case "diagnostics.logs.subscribe":
		return isArgs[model.DiagnosticsLogsArgs](args)
	default:
		return false
	}
}

func isArgs[T any](args model.CommandArgs) bool {
	switch typed := any(args).(type) {
	case T:
		return true
	case *T:
		return typed != nil
	default:
		return false
	}
}
