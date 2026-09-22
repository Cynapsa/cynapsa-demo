package rank2xmpp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestAuditObjectPublicationSuccessOwnership(t *testing.T) {
	tests := []struct {
		file string
		name string
	}{
		{file: "object_transfer.go", name: "failObjectCompletion"},
		{file: "client.go", name: "replayMeshAllowed"},
		{file: "client.go", name: "transferLoop"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := parser.ParseFile(token.NewFileSet(), test.file, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			var target *ast.FuncDecl
			for _, declaration := range parsed.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if ok && function.Name.Name == test.name {
					target = function
					break
				}
			}
			if target == nil {
				t.Fatalf("production function %s not found", test.name)
			}
			decodeCalls, clearCalls := 0, 0
			ast.Inspect(target.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				identifier, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				switch identifier.Name {
				case "decodeObjectPublication":
					decodeCalls++
				case "clearObjectPublicationOwned":
					clearCalls++
				}
				return true
			})
			if decodeCalls != 1 {
				t.Fatalf("%s has %d successful publication decode sites, want exactly one", test.name, decodeCalls)
			}
			if clearCalls != decodeCalls {
				t.Fatalf("%s transfers %d decoded publication owner(s) but retires %d; successful decode leaves manifest digest buffers to GC", test.name, decodeCalls, clearCalls)
			}
		})
	}
}
