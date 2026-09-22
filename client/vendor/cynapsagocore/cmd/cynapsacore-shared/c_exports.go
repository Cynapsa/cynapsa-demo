package main

/*
#include <stdint.h>

typedef struct cynapsa_buffer_desc_v1 {
    uint64_t buffer_handle;
    uint64_t byte_length;
} cynapsa_buffer_desc_v1;

#if defined(_MSC_VER)
__declspec(thread) static uint64_t cynapsa_callback_core_v1 = 0;
#else
static _Thread_local uint64_t cynapsa_callback_core_v1 = 0;
#endif

typedef void (*cynapsa_callback_v1)(uint64_t, uint32_t, cynapsa_buffer_desc_v1);

static void cynapsa_invoke_callback_v1(void *callback, uint64_t core, uint64_t token, uint32_t kind, uint64_t buffer, uint64_t length) {
    cynapsa_callback_v1 fn = (cynapsa_callback_v1)callback;
    cynapsa_buffer_desc_v1 descriptor = {buffer, length};
    uint64_t previous = cynapsa_callback_core_v1;
    cynapsa_callback_core_v1 = core;
    fn(token, kind, descriptor);
    cynapsa_callback_core_v1 = previous;
}

static uint64_t cynapsa_current_callback_core_v1(void) {
    return cynapsa_callback_core_v1;
}
*/
import "C"

import (
	"errors"
	"unsafe"

	core "github.com/Cynapsa/cynapsagocore"
	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

const (
	statusOK            C.int32_t = 0
	statusError         C.int32_t = 1
	statusWaitTimeout   C.int32_t = 2
	maxNativeInputBytes           = 2 << 20
)

var errCallbackReentrant = errors.New("native ABI: callback teardown reentrancy")

func invokeNativeCallback(callback unsafe.Pointer, coreHandle, token uint64, kind uint32, descriptor BufferDescriptor) {
	C.cynapsa_invoke_callback_v1(callback, C.uint64_t(coreHandle), C.uint64_t(token), C.uint32_t(kind), C.uint64_t(descriptor.BufferHandle), C.uint64_t(descriptor.ByteLength))
}

func inNativeCallback(coreHandle uint64) bool {
	return uint64(C.cynapsa_current_callback_core_v1()) == coreHandle && coreHandle != 0
}

func clearCDescriptor(value *C.cynapsa_buffer_desc_v1) {
	if value != nil {
		value.buffer_handle = 0
		value.byte_length = 0
	}
}

func setCDescriptor(output *C.cynapsa_buffer_desc_v1, value BufferDescriptor) bool {
	if output == nil {
		return false
	}
	output.buffer_handle = C.uint64_t(value.BufferHandle)
	output.byte_length = C.uint64_t(value.ByteLength)
	return true
}

func copiedNativeInput(input *C.uint8_t, length C.uint64_t) ([]byte, error) {
	size := uint64(length)
	if size == 0 || size > maxNativeInputBytes || input == nil {
		return nil, abiPublicError(v1.ErrorCodeMalformedInput)
	}
	view := unsafe.Slice((*byte)(unsafe.Pointer(input)), int(size))
	return append([]byte(nil), view...), nil
}

func abiPublicError(code v1.ErrorCode) error {
	value := &v1.Error{Code: code, Stage: v1.ErrorStageSDK, Location: v1.ErrorLocationLocal}
	switch code {
	case v1.ErrorCodeMalformedInput:
		value.Message = "The command input is invalid"
	case v1.ErrorCodeInvalidHandle:
		value.Message = "The local handle is invalid"
	case v1.ErrorCodeShutdownInProgress:
		value.Message = "Shutdown is in progress"
		value.Stage = v1.ErrorStageShutdown
	case v1.ErrorCodeQueueFull:
		value.Message = "The local queue is full"
	default:
		value.Code = v1.ErrorCodeCore
		value.Message = "The AZTM core could not complete the operation"
		value.Stage = v1.ErrorStageCommand
	}
	return value
}

func normalizeABIError(err error) error {
	if err == nil {
		return nil
	}
	var public *v1.Error
	if errors.As(err, &public) {
		return public
	}
	switch {
	case errors.Is(err, errInvalidABIHandle):
		return abiPublicError(v1.ErrorCodeInvalidHandle)
	case errors.Is(err, errInvalidABIInput):
		return abiPublicError(v1.ErrorCodeMalformedInput)
	case errors.Is(err, errABIClosing):
		return abiPublicError(v1.ErrorCodeShutdownInProgress)
	case errors.Is(err, errCallbackReentrant):
		return abiPublicError(v1.ErrorCodeShutdownInProgress)
	case errors.Is(err, errHandleAllocation):
		return abiPublicError(v1.ErrorCodeQueueFull)
	default:
		return abiPublicError(v1.ErrorCodeCore)
	}
}

func encodeCError(output *C.cynapsa_buffer_desc_v1, err error) C.int32_t {
	clearCDescriptor(output)
	if output == nil {
		return statusError
	}
	encoded, encodeErr := core.EncodeABIError(normalizeABIError(err))
	if encodeErr != nil || len(encoded) == 0 {
		return statusError
	}
	descriptor := allocateEncodedCError(processBuffers, encoded)
	clear(encoded)
	if !setCDescriptor(output, descriptor) {
		return statusError
	}
	return statusError
}

// allocateEncodedCError is the sole path allowed to use the process-owned
// emergency descriptor. Ordinary results continue to fail normal slot/byte
// admission and cannot bypass the ABI buffer quotas.
func allocateEncodedCError(store *bufferStore, encoded []byte) BufferDescriptor {
	descriptor, err := store.allocate(encoded)
	if err != nil {
		return emergencyQueueFullDescriptor()
	}
	return descriptor
}

func waitStatus(output *C.cynapsa_buffer_desc_v1, err error) C.int32_t {
	var public *v1.Error
	if errors.As(err, &public) && public.Code == v1.ErrorCodeDeliveryTimeout {
		clearCDescriptor(output)
		return statusWaitTimeout
	}
	return encodeCError(output, err)
}

func recoverCABI(output *C.cynapsa_buffer_desc_v1, status *C.int32_t) {
	if recover() != nil {
		*status = encodeCError(output, abiPublicError(v1.ErrorCodeCore))
	}
}

func recoverCABIResult(result, output *C.cynapsa_buffer_desc_v1, status *C.int32_t) {
	if recover() == nil {
		return
	}
	if result != nil && result.buffer_handle != 0 {
		_ = FreeBuffer(uint64(result.buffer_handle))
		clearCDescriptor(result)
	}
	*status = encodeCError(output, abiPublicError(v1.ErrorCodeCore))
}

//export cynapsa_v1_abi_version
func cynapsa_v1_abi_version(outVersion *C.uint32_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	if outVersion != nil {
		*outVersion = 0
	}
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	if outVersion == nil || outError == nil {
		return encodeCError(outError, abiPublicError(v1.ErrorCodeMalformedInput))
	}
	*outVersion = C.uint32_t(ABIVersion())
	return statusOK
}

//export cynapsa_v1_core_create
func cynapsa_v1_core_create(input *C.uint8_t, length C.uint64_t, outCore *C.uint64_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	if outCore != nil {
		*outCore = 0
	}
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	if outCore == nil || outError == nil {
		return encodeCError(outError, abiPublicError(v1.ErrorCodeMalformedInput))
	}
	data, err := copiedNativeInput(input, length)
	if err != nil {
		return encodeCError(outError, err)
	}
	created := withClearedOwnedInput(data, func(owned []byte) struct {
		handle uint64
		err    error
	} {
		handle, _, createErr := CoreCreate(owned)
		return struct {
			handle uint64
			err    error
		}{handle: handle, err: createErr}
	})
	handle, err := created.handle, created.err
	if err != nil {
		return encodeCError(outError, err)
	}
	*outCore = C.uint64_t(handle)
	return statusOK
}

//export cynapsa_v1_core_start
func cynapsa_v1_core_start(handle C.uint64_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	if outError == nil {
		return statusError
	}
	if err := CoreStart(uint64(handle)); err != nil {
		return encodeCError(outError, err)
	}
	return statusOK
}

func nativeResultCall(handle C.uint64_t, input *C.uint8_t, length C.uint64_t, outResult, outError *C.cynapsa_buffer_desc_v1, call func(uint64, []byte) (BufferDescriptor, error)) C.int32_t {
	clearCDescriptor(outResult)
	clearCDescriptor(outError)
	if outResult == nil || outError == nil {
		return encodeCError(outError, abiPublicError(v1.ErrorCodeMalformedInput))
	}
	data, err := copiedNativeInput(input, length)
	if err != nil {
		return encodeCError(outError, err)
	}
	result := withClearedOwnedInput(data, func(owned []byte) struct {
		descriptor BufferDescriptor
		err        error
	} {
		descriptor, callErr := call(uint64(handle), owned)
		return struct {
			descriptor BufferDescriptor
			err        error
		}{descriptor: descriptor, err: callErr}
	})
	descriptor, err := result.descriptor, result.err
	if err != nil {
		return encodeCError(outError, err)
	}
	setCDescriptor(outResult, descriptor)
	return statusOK
}

//export cynapsa_v1_core_submit
func cynapsa_v1_core_submit(handle C.uint64_t, input *C.uint8_t, length C.uint64_t, outResult, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outResult)
	clearCDescriptor(outError)
	defer recoverCABIResult(outResult, outError, &status)
	return nativeResultCall(handle, input, length, outResult, outError, CoreSubmit)
}

func nativeMutationCall(handle C.uint64_t, input *C.uint8_t, length C.uint64_t, outError *C.cynapsa_buffer_desc_v1, call func(uint64, []byte) error) C.int32_t {
	clearCDescriptor(outError)
	if outError == nil {
		return statusError
	}
	data, err := copiedNativeInput(input, length)
	if err != nil {
		return encodeCError(outError, err)
	}
	err = withClearedOwnedInput(data, func(owned []byte) error { return call(uint64(handle), owned) })
	if err != nil {
		return encodeCError(outError, err)
	}
	return statusOK
}

//export cynapsa_v1_core_cancel
func cynapsa_v1_core_cancel(handle C.uint64_t, input *C.uint8_t, length C.uint64_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	return nativeMutationCall(handle, input, length, outError, CoreCancel)
}

func nativeWaitCall(handle C.uint64_t, timeout C.int64_t, outResult, outError *C.cynapsa_buffer_desc_v1, call func(uint64, int64) (BufferDescriptor, error)) C.int32_t {
	clearCDescriptor(outResult)
	clearCDescriptor(outError)
	if outResult == nil || outError == nil {
		return encodeCError(outError, abiPublicError(v1.ErrorCodeMalformedInput))
	}
	descriptor, err := call(uint64(handle), int64(timeout))
	if err != nil {
		return waitStatus(outError, err)
	}
	setCDescriptor(outResult, descriptor)
	return statusOK
}

//export cynapsa_v1_core_next_completion
func cynapsa_v1_core_next_completion(handle C.uint64_t, timeout C.int64_t, outResult, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outResult)
	clearCDescriptor(outError)
	defer recoverCABIResult(outResult, outError, &status)
	return nativeWaitCall(handle, timeout, outResult, outError, CoreNextCompletion)
}

//export cynapsa_v1_core_next_event
func cynapsa_v1_core_next_event(handle C.uint64_t, timeout C.int64_t, outResult, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outResult)
	clearCDescriptor(outError)
	defer recoverCABIResult(outResult, outError, &status)
	return nativeWaitCall(handle, timeout, outResult, outError, CoreNextEvent)
}

//export cynapsa_v1_core_status
func cynapsa_v1_core_status(handle C.uint64_t, outResult, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outResult)
	clearCDescriptor(outError)
	defer recoverCABIResult(outResult, outError, &status)
	if outResult == nil || outError == nil {
		return encodeCError(outError, abiPublicError(v1.ErrorCodeMalformedInput))
	}
	descriptor, err := CoreStatus(uint64(handle))
	if err != nil {
		return encodeCError(outError, err)
	}
	setCDescriptor(outResult, descriptor)
	return statusOK
}

//export cynapsa_v1_core_shutdown
func cynapsa_v1_core_shutdown(handle C.uint64_t, timeout C.int64_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	if outError == nil {
		return statusError
	}
	if err := CoreShutdown(uint64(handle), int64(timeout)); err != nil {
		return encodeCError(outError, err)
	}
	return statusOK
}

//export cynapsa_v1_core_destroy
func cynapsa_v1_core_destroy(handle C.uint64_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	if outError == nil {
		return statusError
	}
	if err := CoreDestroy(uint64(handle)); err != nil {
		return encodeCError(outError, err)
	}
	return statusOK
}

//export cynapsa_v1_payload_open
func cynapsa_v1_payload_open(handle C.uint64_t, outResult, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outResult)
	clearCDescriptor(outError)
	defer recoverCABIResult(outResult, outError, &status)
	if outResult == nil || outError == nil {
		return encodeCError(outError, abiPublicError(v1.ErrorCodeMalformedInput))
	}
	descriptor, err := PayloadOpen(uint64(handle))
	if err != nil {
		return encodeCError(outError, err)
	}
	setCDescriptor(outResult, descriptor)
	return statusOK
}

//export cynapsa_v1_payload_write
func cynapsa_v1_payload_write(handle C.uint64_t, input *C.uint8_t, length C.uint64_t, outResult, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outResult)
	clearCDescriptor(outError)
	defer recoverCABIResult(outResult, outError, &status)
	return nativeResultCall(handle, input, length, outResult, outError, PayloadWrite)
}

//export cynapsa_v1_payload_finish
func cynapsa_v1_payload_finish(handle C.uint64_t, input *C.uint8_t, length C.uint64_t, outResult, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outResult)
	clearCDescriptor(outError)
	defer recoverCABIResult(outResult, outError, &status)
	return nativeResultCall(handle, input, length, outResult, outError, PayloadFinish)
}

//export cynapsa_v1_payload_read
func cynapsa_v1_payload_read(handle C.uint64_t, input *C.uint8_t, length C.uint64_t, outResult, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outResult)
	clearCDescriptor(outError)
	defer recoverCABIResult(outResult, outError, &status)
	return nativeResultCall(handle, input, length, outResult, outError, PayloadRead)
}

//export cynapsa_v1_payload_cancel
func cynapsa_v1_payload_cancel(handle C.uint64_t, input *C.uint8_t, length C.uint64_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	return nativeMutationCall(handle, input, length, outError, PayloadCancel)
}

//export cynapsa_v1_payload_retain
func cynapsa_v1_payload_retain(handle C.uint64_t, input *C.uint8_t, length C.uint64_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	return nativeMutationCall(handle, input, length, outError, PayloadRetain)
}

//export cynapsa_v1_payload_release
func cynapsa_v1_payload_release(handle C.uint64_t, input *C.uint8_t, length C.uint64_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	return nativeMutationCall(handle, input, length, outError, PayloadRelease)
}

//export cynapsa_v1_callbacks_register
func cynapsa_v1_callbacks_register(handle C.uint64_t, callback unsafe.Pointer, completion, event, diagnostic C.uint64_t, capacity C.uint32_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	if outError == nil || callback == nil {
		return encodeCError(outError, abiPublicError(v1.ErrorCodeMalformedInput))
	}
	err := RegisterCallbacks(uint64(handle), CallbackSet{Callback: callback, CompletionToken: uint64(completion), EventToken: uint64(event), LogToken: uint64(diagnostic), Capacity: uint32(capacity)})
	if err != nil {
		return encodeCError(outError, err)
	}
	return statusOK
}

//export cynapsa_v1_callbacks_clear
func cynapsa_v1_callbacks_clear(handle C.uint64_t, timeout C.int64_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	if outError == nil {
		return statusError
	}
	if inNativeCallback(uint64(handle)) {
		return encodeCError(outError, errCallbackReentrant)
	}
	record, value, err := borrowCore(uint64(handle))
	if err != nil {
		return encodeCError(outError, err)
	}
	configured := value.DefaultTimeout()
	record.release()
	ctx, cancel, err := nativeWaitContext(configured, int64(timeout))
	if err != nil {
		return encodeCError(outError, err)
	}
	defer cancel()
	if err := ClearCallbacks(uint64(handle), ctx); err != nil {
		return encodeCError(outError, err)
	}
	return statusOK
}

//export cynapsa_v1_buffer_read
func cynapsa_v1_buffer_read(handle, offset C.uint64_t, destination *C.uint8_t, destinationLength C.uint64_t, outCopied *C.uint64_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	if outCopied != nil {
		*outCopied = 0
	}
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	if outCopied == nil || outError == nil || uint64(destinationLength) > maxABIBufferBytes || destinationLength != 0 && destination == nil {
		return encodeCError(outError, abiPublicError(v1.ErrorCodeMalformedInput))
	}
	var target []byte
	if destinationLength != 0 {
		target = unsafe.Slice((*byte)(unsafe.Pointer(destination)), int(destinationLength))
	}
	copied, err := BufferRead(uint64(handle), uint64(offset), target)
	if err != nil {
		return encodeCError(outError, err)
	}
	*outCopied = C.uint64_t(copied)
	return statusOK
}

//export cynapsa_v1_buffer_free
func cynapsa_v1_buffer_free(handle C.uint64_t, outError *C.cynapsa_buffer_desc_v1) (status C.int32_t) {
	clearCDescriptor(outError)
	defer recoverCABI(outError, &status)
	if outError == nil {
		return statusError
	}
	if err := FreeBuffer(uint64(handle)); err != nil {
		return encodeCError(outError, err)
	}
	return statusOK
}
