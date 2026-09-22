package main

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNativeSharedLibrarySymbolsAndCClientSmoke(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native smoke harness is enabled on POSIX builders")
	}
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("cgo disabled")
	}
	compiler, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("C compiler unavailable")
	}
	directory := t.TempDir()
	extension := ".so"
	libraryEnvironment := "LD_LIBRARY_PATH"
	if runtime.GOOS == "darwin" {
		extension = ".dylib"
		libraryEnvironment = "DYLD_LIBRARY_PATH"
	}
	library := filepath.Join(directory, "libcynapsacore"+extension)
	root := filepath.Clean(filepath.Join("..", ".."))
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	linkerControl := filepath.Join(absoluteRoot, "cmd", "cynapsacore-shared", "exports_linux.map")
	extLinkFlag := "-Wl,--version-script," + linkerControl
	if runtime.GOOS == "darwin" {
		linkerControl = filepath.Join(absoluteRoot, "cmd", "cynapsacore-shared", "exports_darwin.txt")
		extLinkFlag = "-Wl,-exported_symbols_list," + linkerControl
	}
	build := exec.Command("go", "build", "-buildmode=c-shared", "-ldflags=-extldflags="+extLinkFlag, "-o", library, "./cmd/cynapsacore-shared")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build shared library: %v\n%s", err, output)
	}

	wanted := map[string]struct{}{
		"cynapsa_v1_abi_version": {}, "cynapsa_v1_core_create": {}, "cynapsa_v1_core_start": {},
		"cynapsa_v1_core_submit": {}, "cynapsa_v1_core_cancel": {}, "cynapsa_v1_core_next_completion": {},
		"cynapsa_v1_core_next_event": {}, "cynapsa_v1_core_status": {}, "cynapsa_v1_core_shutdown": {},
		"cynapsa_v1_core_destroy": {}, "cynapsa_v1_payload_open": {}, "cynapsa_v1_payload_write": {},
		"cynapsa_v1_payload_finish": {}, "cynapsa_v1_payload_read": {}, "cynapsa_v1_payload_cancel": {},
		"cynapsa_v1_payload_retain": {}, "cynapsa_v1_payload_release": {}, "cynapsa_v1_callbacks_register": {},
		"cynapsa_v1_callbacks_clear": {}, "cynapsa_v1_buffer_read": {}, "cynapsa_v1_buffer_free": {},
	}
	nmArgs := []string{"-D", "--defined-only", library}
	if runtime.GOOS == "darwin" {
		nmArgs = []string{"-gU", library}
	}
	nm := exec.Command("nm", nmArgs...)
	output, err := nm.Output()
	if err != nil {
		t.Fatalf("read symbols: %v", err)
	}
	found := make(map[string]struct{}, len(wanted))
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		name := strings.TrimPrefix(fields[len(fields)-1], "_")
		found[name] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for name := range wanted {
		if _, exists := found[name]; !exists {
			t.Errorf("shared library is missing %s", name)
		}
	}
	for name := range found {
		if _, approved := wanted[name]; !approved {
			t.Errorf("shared library exports unapproved symbol %s", name)
		}
	}

	executable := filepath.Join(directory, "native_smoke")
	compile := exec.Command(compiler, "-std=c11", "-Wall", "-Wextra", "-Werror", "-I", filepath.Join(root, "cmd", "cynapsacore-shared"), filepath.Join(root, "cmd", "cynapsacore-shared", "testdata", "native_smoke.c"), "-L", directory, "-lcynapsacore", "-o", executable)
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile C client: %v\n%s", err, output)
	}
	run := exec.Command(executable)
	run.Env = append(os.Environ(), libraryEnvironment+"="+directory)
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("run C client: %v\n%s", err, output)
	}
}
