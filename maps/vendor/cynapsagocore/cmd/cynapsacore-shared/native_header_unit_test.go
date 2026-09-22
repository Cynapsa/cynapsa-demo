package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestAuthoritativeC99HeaderCompilesAndDeclaresFrozenExports(t *testing.T) {
	compiler := "cc"
	if runtime.GOOS == "windows" {
		compiler = "clang"
	}
	path, err := exec.LookPath(compiler)
	if err != nil {
		t.Skip("C compiler unavailable")
	}
	directory := t.TempDir()
	source := filepath.Join(directory, "header_check.c")
	program := `#include "cynapsacore_v1.h"
static cynapsa_callback_v1 callback_value;
int main(void) {
    cynapsa_buffer_desc_v1 value = {0, 0};
    uint32_t version = 0;
    callback_value = 0;
    return (int)cynapsa_v1_abi_version(&version, &value);
}`
	if err := os.WriteFile(source, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(path, "-std=c11", "-Wall", "-Wextra", "-Werror", "-I.", "-c", source, "-o", filepath.Join(directory, "header_check.o"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("compile authoritative header: %v\n%s", err, output)
	}

	header, err := os.ReadFile("cynapsacore_v1.h")
	if err != nil {
		t.Fatal(err)
	}
	text := string(header)
	wanted := []string{
		"abi_version", "core_create", "core_start", "core_submit", "core_cancel",
		"core_next_completion", "core_next_event", "core_status", "core_shutdown", "core_destroy",
		"payload_open", "payload_write", "payload_finish", "payload_read", "payload_cancel",
		"payload_retain", "payload_release", "callbacks_register", "callbacks_clear", "buffer_read", "buffer_free",
	}
	for _, suffix := range wanted {
		if strings.Count(text, "cynapsa_v1_"+suffix+"(") != 1 {
			t.Errorf("header declaration count for %s is not one", suffix)
		}
	}
}
