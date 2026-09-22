package payload

import "unicode/utf8"

// Validate verifies one V1 canonical payload without materializing any field.
// Finish uses this zero-copy path so its exact canonical buffer is the only
// temporary payload-sized allocation retained during validation.
func (s *Serializer) Validate(encoded []byte) error {
	if s == nil {
		return ErrInvalidLimits
	}
	if len(encoded) == 0 {
		return ErrMalformedCanonical
	}
	if int64(len(encoded)) > s.maximum {
		return ErrPayloadTooLarge
	}
	reader := canonicalReader{data: encoded}
	pairs, err := reader.length(5)
	if err != nil {
		return err
	}
	if err := reader.key(0); err != nil {
		return err
	}
	version, err := reader.unsigned()
	if err != nil {
		return err
	}
	if version != uint64(CanonicalVersion1) {
		return ErrUnsupportedVersion
	}
	if err := reader.key(1); err != nil {
		return err
	}
	kind, err := reader.unsigned()
	if err != nil {
		return err
	}
	switch kind {
	case uint64(CanonicalKindNative):
		if pairs != 5 {
			return ErrMalformedCanonical
		}
		err = reader.native()
	case uint64(CanonicalKindHTTPRequest):
		if pairs != 7 {
			return ErrMalformedCanonical
		}
		err = reader.request()
	case uint64(CanonicalKindHTTPResponse):
		if pairs != 6 && pairs != 7 {
			return ErrMalformedCanonical
		}
		err = reader.response(pairs == 7)
	default:
		return ErrUnsupportedVariant
	}
	if err != nil {
		return err
	}
	if reader.offset != len(reader.data) {
		return ErrMalformedCanonical
	}
	return nil
}

type canonicalReader struct {
	data   []byte
	offset int
}

func (r *canonicalReader) native() error {
	if err := r.key(2); err != nil {
		return err
	}
	contentType, err := r.text(maximumContentTypeBytes)
	if err != nil || containsCRLF(contentType) {
		return ErrMalformedCanonical
	}
	if err := r.key(3); err != nil {
		return err
	}
	path, err := r.text(maximumPathBytes)
	if err != nil || !validPathBytes(path) {
		return ErrMalformedCanonical
	}
	if err := r.key(4); err != nil {
		return err
	}
	return r.byteString()
}

func (r *canonicalReader) request() error {
	if err := r.key(2); err != nil {
		return err
	}
	method, err := r.text(maximumMethodBytes)
	if err != nil || len(method) == 0 || !httpTokenBytes(method) {
		return ErrMalformedCanonical
	}
	if err := r.key(3); err != nil {
		return err
	}
	path, err := r.text(maximumPathBytes)
	if err != nil || !validPathBytes(path) {
		return ErrMalformedCanonical
	}
	if err := r.key(4); err != nil {
		return err
	}
	if _, err := r.text(maximumQueryBytes); err != nil {
		return err
	}
	if err := r.key(5); err != nil {
		return err
	}
	if err := r.headers(); err != nil {
		return err
	}
	if err := r.key(6); err != nil {
		return err
	}
	return r.byteString()
}

func (r *canonicalReader) response(hasError bool) error {
	if err := r.key(2); err != nil {
		return err
	}
	status, err := r.unsigned()
	if err != nil || status < 100 || status > 599 {
		return ErrMalformedCanonical
	}
	if err := r.key(3); err != nil {
		return err
	}
	reason, err := r.text(maximumReasonBytes)
	if err != nil || containsCRLF(reason) {
		return ErrMalformedCanonical
	}
	if err := r.key(4); err != nil {
		return err
	}
	if err := r.headers(); err != nil {
		return err
	}
	if err := r.key(5); err != nil {
		return err
	}
	if err := r.byteString(); err != nil {
		return err
	}
	if !hasError {
		return nil
	}
	if status < 400 {
		return ErrMalformedCanonical
	}
	if err := r.key(6); err != nil {
		return err
	}
	return r.applicationError()
}

func (r *canonicalReader) applicationError() error {
	fields, err := r.length(5)
	if err != nil || fields != 3 {
		return ErrMalformedCanonical
	}
	if err := r.key(0); err != nil {
		return err
	}
	code, err := r.text(maximumHeaderNameBytes)
	if err != nil || len(code) == 0 {
		return ErrMalformedCanonical
	}
	if err := r.key(1); err != nil {
		return err
	}
	detail, err := r.text(maximumHeaderValueBytes)
	if err != nil || len(detail) == 0 {
		return ErrMalformedCanonical
	}
	if err := r.key(2); err != nil {
		return err
	}
	details, err := r.text(maximumHeaderValueBytes)
	if err != nil || !validApplicationErrorUnicodeEscapes(details) || !validApplicationErrorJSONObject(string(details)) {
		return ErrMalformedCanonical
	}
	return nil
}

func (r *canonicalReader) headers() error {
	count, err := r.length(4)
	if err != nil {
		return err
	}
	if count > MaximumHeaderCount {
		return ErrPayloadTooLarge
	}
	for index := uint64(0); index < count; index++ {
		fields, err := r.length(4)
		if err != nil || fields != 2 {
			return ErrMalformedCanonical
		}
		name, err := r.text(maximumHeaderNameBytes)
		if err != nil || len(name) == 0 || !httpTokenBytes(name) {
			return ErrMalformedCanonical
		}
		value, err := r.text(maximumHeaderValueBytes)
		if err != nil || containsCRLF(value) {
			return ErrMalformedCanonical
		}
	}
	return nil
}

func (r *canonicalReader) key(expected uint64) error {
	value, err := r.unsigned()
	if err != nil || value != expected {
		return ErrMalformedCanonical
	}
	return nil
}

func (r *canonicalReader) unsigned() (uint64, error) {
	return r.head(0)
}

func (r *canonicalReader) length(major byte) (uint64, error) {
	return r.head(major)
}

func (r *canonicalReader) text(maximum int) ([]byte, error) {
	length, err := r.length(3)
	if err != nil || length > uint64(maximum) || length > uint64(len(r.data)-r.offset) {
		return nil, ErrMalformedCanonical
	}
	start := r.offset
	r.offset += int(length)
	value := r.data[start:r.offset]
	if !utf8.Valid(value) {
		return nil, ErrMalformedCanonical
	}
	return value, nil
}

func (r *canonicalReader) byteString() error {
	if r.offset < len(r.data) && r.data[r.offset] == 0xf6 {
		r.offset++
		return nil
	}
	length, err := r.length(2)
	if err != nil || length == 0 || length > uint64(len(r.data)-r.offset) {
		return ErrMalformedCanonical
	}
	r.offset += int(length)
	return nil
}

func (r *canonicalReader) head(expectedMajor byte) (uint64, error) {
	if r.offset >= len(r.data) {
		return 0, ErrMalformedCanonical
	}
	initial := r.data[r.offset]
	r.offset++
	if initial>>5 != expectedMajor {
		return 0, ErrMalformedCanonical
	}
	additional := initial & 0x1f
	switch {
	case additional < 24:
		return uint64(additional), nil
	case additional == 24:
		value, ok := r.readUint(1)
		if !ok || value < 24 {
			return 0, ErrMalformedCanonical
		}
		return value, nil
	case additional == 25:
		value, ok := r.readUint(2)
		if !ok || value <= 0xff {
			return 0, ErrMalformedCanonical
		}
		return value, nil
	case additional == 26:
		value, ok := r.readUint(4)
		if !ok || value <= 0xffff {
			return 0, ErrMalformedCanonical
		}
		return value, nil
	case additional == 27:
		value, ok := r.readUint(8)
		if !ok || value <= 0xffffffff {
			return 0, ErrMalformedCanonical
		}
		return value, nil
	default:
		return 0, ErrMalformedCanonical
	}
}

func (r *canonicalReader) readUint(bytes int) (uint64, bool) {
	if bytes < 0 || bytes > len(r.data)-r.offset {
		return 0, false
	}
	value := uint64(0)
	for end := r.offset + bytes; r.offset < end; r.offset++ {
		value = value<<8 | uint64(r.data[r.offset])
	}
	return value, true
}

func validPathBytes(value []byte) bool {
	if len(value) == 0 || value[0] != '/' {
		return false
	}
	for _, current := range value {
		if current == '?' || current == '#' {
			return false
		}
	}
	return true
}

func containsCRLF(value []byte) bool {
	for _, current := range value {
		if current == '\r' || current == '\n' {
			return true
		}
	}
	return false
}

func httpTokenBytes(value []byte) bool {
	for _, current := range value {
		if current <= 0x20 || current >= 0x7f {
			return false
		}
		switch current {
		case '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', '{', '}':
			return false
		}
	}
	return true
}
