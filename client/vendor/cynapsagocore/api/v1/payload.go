package v1

const (
	CommandPayloadOpen    CommandName = "payload.open"
	CommandPayloadWrite   CommandName = "payload.write_chunk"
	CommandPayloadFinish  CommandName = "payload.finish"
	CommandPayloadCancel  CommandName = "payload.cancel"
	CommandPayloadRead    CommandName = "payload.read"
	CommandPayloadClose   CommandName = "payload.close"
	CommandPayloadRetain  CommandName = "payload.retain"
	CommandPayloadRelease CommandName = "payload.release"
)

type PayloadOpenCommand struct{ CommandBase }

func (PayloadOpenCommand) Name() CommandName { return CommandPayloadOpen }
func (PayloadOpenCommand) commandType()      {}

type PayloadWriteCommand struct {
	CommandBase
	Handle PayloadHandle
	Chunk  []byte
}

func (PayloadWriteCommand) Name() CommandName { return CommandPayloadWrite }
func (PayloadWriteCommand) commandType()      {}

type PayloadFinishCommand struct {
	CommandBase
	Handle PayloadHandle
}

func (PayloadFinishCommand) Name() CommandName { return CommandPayloadFinish }
func (PayloadFinishCommand) commandType()      {}

type PayloadCancelCommand struct {
	CommandBase
	Handle PayloadHandle
}

func (PayloadCancelCommand) Name() CommandName { return CommandPayloadCancel }
func (PayloadCancelCommand) commandType()      {}

type PayloadReadCommand struct {
	CommandBase
	Handle PayloadHandle
	Offset uint64
	Limit  uint32
}

func (PayloadReadCommand) Name() CommandName { return CommandPayloadRead }
func (PayloadReadCommand) commandType()      {}

type PayloadCloseCommand struct {
	CommandBase
	Handle PayloadHandle
}

func (PayloadCloseCommand) Name() CommandName { return CommandPayloadClose }
func (PayloadCloseCommand) commandType()      {}

type PayloadRetainCommand struct {
	CommandBase
	Handle PayloadHandle
}

func (PayloadRetainCommand) Name() CommandName { return CommandPayloadRetain }
func (PayloadRetainCommand) commandType()      {}

type PayloadReleaseCommand struct {
	CommandBase
	Handle PayloadHandle
}

func (PayloadReleaseCommand) Name() CommandName { return CommandPayloadRelease }
func (PayloadReleaseCommand) commandType()      {}

// PayloadHandleResult returns opaque local payload data only.
type PayloadHandleResult struct {
	Handle PayloadHandle
	Size   uint64
	EOF    bool
	Chunk  []byte
}

func (PayloadHandleResult) resultType() {}
