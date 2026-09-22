package v1

const (
	CommandMeshList    CommandName = "mesh.list"
	CommandMeshRefresh CommandName = "mesh.membership.refresh"
)

// MeshListCommand requests known public mesh scopes.
type MeshListCommand struct{ CommandBase }

// Name returns the allowlisted command name.
func (MeshListCommand) Name() CommandName {
	return CommandMeshList
}
func (MeshListCommand) commandType() {}

// MeshMembershipRefreshCommand requests semantic membership refresh.
type MeshMembershipRefreshCommand struct{ CommandBase }

// Name returns the allowlisted command name.
func (MeshMembershipRefreshCommand) Name() CommandName {
	return CommandMeshRefresh
}
func (MeshMembershipRefreshCommand) commandType() {}

// MeshSummary is a bounded public view of the authenticated mesh scope.
type MeshSummary struct {
	MeshID MeshID
	Active bool
}

// MeshListResult contains public mesh summaries without private membership data.
type MeshListResult struct {
	Meshes []MeshSummary
}

func (MeshListResult) resultType() {}
