package main

import "testing"

func TestWithClearedOwnedInputClearsOnReturnAndPreservesPanic(t *testing.T) {
	t.Run("return", func(t *testing.T) {
		owned := []byte("secret")
		retained := owned
		got := withClearedOwnedInput(owned, func(input []byte) int {
			if string(input) != "secret" {
				t.Fatalf("input = %q", input)
			}
			return len(input)
		})
		if got != len(retained) {
			t.Fatalf("result = %d", got)
		}
		assertZeroBytes(t, retained)
	})

	t.Run("panic", func(t *testing.T) {
		owned := []byte("secret")
		retained := owned
		panicValue := &struct{ name string }{name: "first panic"}
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			withClearedOwnedInput(owned, func(input []byte) struct{} {
				if string(input) != "secret" {
					t.Fatalf("input = %q", input)
				}
				panic(panicValue)
			})
		}()
		if recovered != panicValue {
			t.Fatalf("recovered = %#v, want original panic %#v", recovered, panicValue)
		}
		assertZeroBytes(t, retained)
	})
}

func TestWithClearedInputCopyLeavesCallerUnchanged(t *testing.T) {
	caller := []byte("caller-owned")
	var retained []byte
	withClearedInputCopy(caller, func(input []byte) struct{} {
		retained = input
		input[0] = 'X'
		return struct{}{}
	})
	if string(caller) != "caller-owned" {
		t.Fatalf("caller input mutated: %q", caller)
	}
	assertZeroBytes(t, retained)
}

func assertZeroBytes(t *testing.T, values []byte) {
	t.Helper()
	for index, value := range values {
		if value != 0 {
			t.Fatalf("byte %d = %d, want zero", index, value)
		}
	}
}
