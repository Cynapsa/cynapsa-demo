package main

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestABITimeoutsUseSignedIntegerMilliseconds(t *testing.T) {
	for _, function := range []struct {
		name  string
		value any
	}{
		{name: "CoreNextCompletion", value: CoreNextCompletion},
		{name: "CoreNextEvent", value: CoreNextEvent},
		{name: "CoreShutdown", value: CoreShutdown},
	} {
		typeOf := reflect.TypeOf(function.value)
		if got := typeOf.In(1).Kind(); got != reflect.Int64 {
			t.Errorf("%s timeout kind=%s want int64", function.name, got)
		}
	}
}

func TestSerializedABIOperationsReturnPointerFreeBufferDescriptors(t *testing.T) {
	for _, function := range []struct {
		name  string
		value any
		index int
	}{
		{name: "CoreCreate", value: CoreCreate, index: 1},
		{name: "CoreSubmit", value: CoreSubmit},
		{name: "CoreNextCompletion", value: CoreNextCompletion},
		{name: "CoreNextEvent", value: CoreNextEvent},
		{name: "CoreStatus", value: CoreStatus},
		{name: "PayloadOpen", value: PayloadOpen},
		{name: "PayloadFinish", value: PayloadFinish},
		{name: "PayloadRead", value: PayloadRead},
	} {
		output := reflect.TypeOf(function.value).Out(function.index)
		if output != reflect.TypeOf(BufferDescriptor{}) {
			t.Errorf("%s output=%s want BufferDescriptor", function.name, output)
		}
		for field := 0; field < output.NumField(); field++ {
			if output.Field(field).Type.Kind() != reflect.Uint64 {
				t.Errorf("%s descriptor field %s is not uint64", function.name, output.Field(field).Name)
			}
		}
	}
	if got := reflect.TypeOf(BufferRead).Out(0).Kind(); got != reflect.Uint64 {
		t.Fatalf("BufferRead byte count kind=%s", got)
	}
}

func TestSharedWrapperImportsOnlyPublicCorePackages(t *testing.T) {
	const module = "github.com/Cynapsa/cynapsagocore"

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list shared-wrapper sources: %v", err)
	}

	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}

		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("decode import in %s: %v", file, err)
			}
			if path == module || path == module+"/api/v1" || !strings.HasPrefix(path, module+"/") {
				continue
			}
			t.Errorf("shared wrapper %s imports non-public core package %q", file, path)
		}
	}
}
