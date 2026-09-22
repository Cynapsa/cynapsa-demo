package v1

const (
	CommandAddressPut     CommandName = "address.map.put"
	CommandAddressRemove  CommandName = "address.map.remove"
	CommandAddressList    CommandName = "address.map.list"
	CommandAddressResolve CommandName = "address.resolve"
)

// AddressMapping maps a virtual origin to a recipient identity in the current login-session mesh.
type AddressMapping struct {
	VirtualOrigin string
	Recipient     AgentID
}

// AddressMapPutCommand adds or replaces an address mapping.
type AddressMapPutCommand struct {
	CommandBase
	Mapping AddressMapping
}

// Name returns the allowlisted command name.
func (AddressMapPutCommand) Name() CommandName {
	return CommandAddressPut
}
func (AddressMapPutCommand) commandType() {}

// AddressMapRemoveCommand removes one virtual origin mapping.
type AddressMapRemoveCommand struct {
	CommandBase
	VirtualOrigin string
}

// Name returns the allowlisted command name.
func (AddressMapRemoveCommand) Name() CommandName {
	return CommandAddressRemove
}
func (AddressMapRemoveCommand) commandType() {}

// AddressMapListCommand requests all public mappings.
type AddressMapListCommand struct{ CommandBase }

// Name returns the allowlisted command name.
func (AddressMapListCommand) Name() CommandName {
	return CommandAddressList
}
func (AddressMapListCommand) commandType() {}

// AddressResolveCommand resolves one URL to public routing intent.
type AddressResolveCommand struct {
	CommandBase
	URL string
}

// Name returns the allowlisted command name.
func (AddressResolveCommand) Name() CommandName {
	return CommandAddressResolve
}
func (AddressResolveCommand) commandType() {}

// AddressMappingsResult returns mappings for the authenticated Core.
type AddressMappingsResult struct {
	Mappings []AddressMapping
}

func (AddressMappingsResult) resultType() {}

// AddressResolution is the normalized result of resolving one virtual URL.
type AddressResolution struct {
	Recipient AgentID
	// Path is a nonempty absolute application path of at most 2,048 bytes;
	// query and fragment delimiters are forbidden.
	Path  string
	Query string
}

func (AddressResolution) resultType() {}
