package xep0363

import (
	"context"
	"strings"
)

type Header struct{ Name, Value string }

// Slot contains private endpoints and narrowly allowlisted upload headers.
type Slot struct {
	PutURL     string
	GetURL     string
	PutHeaders []Header
}

// SlotRequester is implemented by the private mesh control plane. Slot
// negotiation remains separate from HTTP object transfer.
type SlotRequester interface {
	RequestSlot(context.Context, int64, string) (Slot, error)
}

func (c *Client) RequestSlot(ctx context.Context, size int64, contentType string) (Slot, error) {
	if c == nil || c.slots == nil || ctx == nil || size <= 0 || size > c.policy.MaximumBytes || contentType == "" || len(contentType) > 256 || containsUnsafe(contentType) {
		return Slot{}, ErrInvalidSlot
	}
	if err := ctx.Err(); err != nil {
		return Slot{}, err
	}
	provided, err := c.slots.RequestSlot(ctx, size, contentType)
	if err != nil {
		if contextErr := dependencyContextError(ctx, err); contextErr != nil {
			return Slot{}, contextErr
		}
		return Slot{}, ErrSlotUnavailable
	}
	slot := cloneSlot(provided)
	valid := false
	defer func() {
		if !valid {
			clearSlot(&slot)
		}
	}()
	if err := c.validateURL(slot.PutURL); err != nil {
		return Slot{}, err
	}
	if err := c.validateURL(slot.GetURL); err != nil {
		return Slot{}, err
	}
	if err := validateHeaders(slot.PutHeaders); err != nil {
		return Slot{}, err
	}
	valid = true
	return slot, nil
}

func validateHeaders(headers []Header) error {
	if len(headers) > 16 {
		return ErrInvalidSlot
	}
	for _, h := range headers {
		name := canonicalHeader(h.Name)
		if name != strings.ToLower(h.Name) || !headerToken(h.Name) || name != "authorization" && name != "cookie" && name != "expires" {
			return ErrInvalidSlot
		}
		if len(h.Value) > 4096 || containsUnsafe(h.Value) {
			return ErrInvalidSlot
		}
	}
	return nil
}

func headerToken(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c <= 0x20 || c >= 0x7f || strings.ContainsRune("()<>@,;:\\\"/[]?={} ", rune(c)) {
			return false
		}
	}
	return true
}

func cloneSlot(slot Slot) Slot {
	owned := Slot{PutURL: strings.Clone(slot.PutURL), GetURL: strings.Clone(slot.GetURL), PutHeaders: make([]Header, len(slot.PutHeaders))}
	for index := range slot.PutHeaders {
		owned.PutHeaders[index] = Header{Name: strings.Clone(slot.PutHeaders[index].Name), Value: strings.Clone(slot.PutHeaders[index].Value)}
	}
	return owned
}

func clearSlot(slot *Slot) {
	if slot == nil {
		return
	}
	for index := range slot.PutHeaders {
		slot.PutHeaders[index].Name = ""
		slot.PutHeaders[index].Value = ""
	}
	clear(slot.PutHeaders)
	slot.PutHeaders = nil
	slot.PutURL = ""
	slot.GetURL = ""
}
