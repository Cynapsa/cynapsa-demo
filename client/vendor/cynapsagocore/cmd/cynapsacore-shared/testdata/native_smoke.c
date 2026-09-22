#define _POSIX_C_SOURCE 200809L
#include "cynapsacore_v1.h"

#include <stdint.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static _Atomic int completion_seen = 0;
static _Atomic int callback_failed = 0;

#define DESCRIPTOR_SENTINEL UINT64_C(0xa5a5a5a5a5a5a5a5)
#define COPIED_SENTINEL UINT64_C(0x5a5a5a5a5a5a5a5a)
#define MAX_ABI_BUFFER_BYTES (UINT64_C(64) << 20)
#define MAX_LIVE_ABI_BUFFERS 4096

static void callback(uint64_t token, uint32_t kind, cynapsa_buffer_desc_v1 value) {
    (void)token;
    cynapsa_buffer_desc_v1 error = {0, 0};
    if (value.buffer_handle == 0 || cynapsa_v1_buffer_free(value.buffer_handle, &error) != CYNAPSA_STATUS_V1_OK) {
        atomic_store(&callback_failed, 1);
    }
    if (kind == CYNAPSA_CALLBACK_V1_COMPLETION) {
        atomic_store(&completion_seen, 1);
    }
}

static int read_and_free_error(cynapsa_buffer_desc_v1 error, const char *code) {
    if (error.buffer_handle == 0 || error.byte_length == 0 || error.byte_length > 4096) {
        return 20;
    }
    uint8_t *bytes = (uint8_t *)calloc((size_t)error.byte_length + 1, 1);
    if (bytes == NULL) {
        return 21;
    }
    uint64_t copied = 0;
    cynapsa_buffer_desc_v1 nested = {0, 0};
    if (cynapsa_v1_buffer_read(error.buffer_handle, 0, bytes, error.byte_length, &copied, &nested) != CYNAPSA_STATUS_V1_OK || copied != error.byte_length) {
        free(bytes);
        return 22;
    }
    if (strstr((const char *)bytes, code) == NULL) {
        fprintf(stderr, "unexpected normalized error: %s\n", bytes);
        free(bytes);
        return 23;
    }
    free(bytes);
    if (cynapsa_v1_buffer_free(error.buffer_handle, &nested) != CYNAPSA_STATUS_V1_OK) {
        return 24;
    }
    return 0;
}

static int descriptor_is_zero(cynapsa_buffer_desc_v1 value) {
    return value.buffer_handle == 0 && value.byte_length == 0;
}

static int test_export_output_initialization(void) {
    static const uint8_t malformed[] = "{}";
    uint32_t version = UINT32_C(0xa5a5a5a5);
    if (cynapsa_v1_abi_version(&version, NULL) != CYNAPSA_STATUS_V1_ERROR || version != 0) {
        return 60;
    }

    cynapsa_core_t core = DESCRIPTOR_SENTINEL;
    if (cynapsa_v1_core_create(malformed, sizeof(malformed) - 1, &core, NULL) != CYNAPSA_STATUS_V1_ERROR || core != 0) {
        return 61;
    }

    cynapsa_buffer_desc_v1 result = {DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_core_submit(0, malformed, sizeof(malformed) - 1, &result, NULL) != CYNAPSA_STATUS_V1_ERROR || !descriptor_is_zero(result)) {
        return 62;
    }
    result = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_core_next_completion(0, 0, &result, NULL) != CYNAPSA_STATUS_V1_ERROR || !descriptor_is_zero(result)) {
        return 63;
    }
    result = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_core_next_event(0, 0, &result, NULL) != CYNAPSA_STATUS_V1_ERROR || !descriptor_is_zero(result)) {
        return 64;
    }
    result = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_core_status(0, &result, NULL) != CYNAPSA_STATUS_V1_ERROR || !descriptor_is_zero(result)) {
        return 65;
    }
    result = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_payload_open(0, &result, NULL) != CYNAPSA_STATUS_V1_ERROR || !descriptor_is_zero(result)) {
        return 66;
    }
    result = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_payload_write(0, malformed, sizeof(malformed) - 1, &result, NULL) != CYNAPSA_STATUS_V1_ERROR || !descriptor_is_zero(result)) {
        return 67;
    }
    result = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_payload_finish(0, malformed, sizeof(malformed) - 1, &result, NULL) != CYNAPSA_STATUS_V1_ERROR || !descriptor_is_zero(result)) {
        return 68;
    }
    result = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_payload_read(0, malformed, sizeof(malformed) - 1, &result, NULL) != CYNAPSA_STATUS_V1_ERROR || !descriptor_is_zero(result)) {
        return 69;
    }
    return 0;
}

static int test_buffer_read_output_initialization(void) {
    cynapsa_buffer_desc_v1 source = {DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_free(0, &source) != CYNAPSA_STATUS_V1_ERROR ||
        source.buffer_handle == 0 || source.byte_length == 0) {
        return 30;
    }

    uint64_t copied = COPIED_SENTINEL;
    cynapsa_buffer_desc_v1 error = {DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_read(source.buffer_handle, 0, NULL, 0, &copied, &error) != CYNAPSA_STATUS_V1_OK ||
        copied != 0 || !descriptor_is_zero(error)) {
        return 31;
    }

    uint8_t short_destination[8];
    memset(short_destination, 0xa5, sizeof(short_destination));
    copied = COPIED_SENTINEL;
    error = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_read(source.buffer_handle, 0, short_destination, 7, &copied, &error) != CYNAPSA_STATUS_V1_OK ||
        copied != 7 || !descriptor_is_zero(error) || short_destination[7] != 0xa5) {
        return 32;
    }

    copied = COPIED_SENTINEL;
    error = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_read(source.buffer_handle, 0, NULL, 1, &copied, &error) != CYNAPSA_STATUS_V1_ERROR || copied != 0) {
        return 33;
    }
    int error_status = read_and_free_error(error, "malformed_input");
    if (error_status != 0) {
        return 34;
    }

    uint8_t one = 0;
    copied = COPIED_SENTINEL;
    error = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_read(source.buffer_handle, 0, &one, MAX_ABI_BUFFER_BYTES + 1, &copied, &error) != CYNAPSA_STATUS_V1_ERROR || copied != 0) {
        return 35;
    }
    error_status = read_and_free_error(error, "malformed_input");
    if (error_status != 0) {
        return 36;
    }

    copied = COPIED_SENTINEL;
    error = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_read(source.buffer_handle, source.byte_length + 1, &one, 1, &copied, &error) != CYNAPSA_STATUS_V1_ERROR || copied != 0) {
        return 37;
    }
    error_status = read_and_free_error(error, "invalid_handle");
    if (error_status != 0) {
        return 38;
    }

    error = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_read(source.buffer_handle, 0, &one, 1, NULL, &error) != CYNAPSA_STATUS_V1_ERROR) {
        return 39;
    }
    error_status = read_and_free_error(error, "malformed_input");
    if (error_status != 0) {
        return 40;
    }

    error = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_free(source.buffer_handle, &error) != CYNAPSA_STATUS_V1_OK || !descriptor_is_zero(error)) {
        return 41;
    }
    return 0;
}

static int test_emergency_capacity_descriptor(void) {
    cynapsa_buffer_desc_v1 *held = (cynapsa_buffer_desc_v1 *)calloc(MAX_LIVE_ABI_BUFFERS, sizeof(*held));
    if (held == NULL) {
        return 50;
    }
    for (size_t index = 0; index < MAX_LIVE_ABI_BUFFERS; index++) {
        held[index] = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
        if (cynapsa_v1_buffer_free(0, &held[index]) != CYNAPSA_STATUS_V1_ERROR ||
            held[index].buffer_handle == 0 || held[index].byte_length == 0) {
            free(held);
            return 51;
        }
    }

    cynapsa_buffer_desc_v1 first = {DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    cynapsa_buffer_desc_v1 second = {DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_free(0, &first) != CYNAPSA_STATUS_V1_ERROR ||
        cynapsa_v1_buffer_free(0, &second) != CYNAPSA_STATUS_V1_ERROR ||
        first.buffer_handle == 0 || first.byte_length == 0 ||
        first.buffer_handle != second.buffer_handle || first.byte_length != second.byte_length) {
        free(held);
        return 52;
    }
    int error_status = read_and_free_error(first, "\"code\":\"queue_full\"");
    if (error_status != 0) {
        free(held);
        return 53;
    }

    cynapsa_buffer_desc_v1 nested = {DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_free(first.buffer_handle, &nested) != CYNAPSA_STATUS_V1_OK || !descriptor_is_zero(nested)) {
        free(held);
        return 54;
    }
    nested = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_free(second.buffer_handle, &nested) != CYNAPSA_STATUS_V1_OK || !descriptor_is_zero(nested)) {
        free(held);
        return 55;
    }

    for (size_t index = 0; index < MAX_LIVE_ABI_BUFFERS; index++) {
        nested = (cynapsa_buffer_desc_v1){DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
        if (cynapsa_v1_buffer_free(held[index].buffer_handle, &nested) != CYNAPSA_STATUS_V1_OK || !descriptor_is_zero(nested)) {
            free(held);
            return 56;
        }
    }
    free(held);

    cynapsa_buffer_desc_v1 ordinary = {DESCRIPTOR_SENTINEL, DESCRIPTOR_SENTINEL};
    if (cynapsa_v1_buffer_free(0, &ordinary) != CYNAPSA_STATUS_V1_ERROR ||
        ordinary.buffer_handle == first.buffer_handle) {
        return 57;
    }
    error_status = read_and_free_error(ordinary, "\"code\":\"invalid_handle\"");
    if (error_status != 0) {
        return 58;
    }
    return 0;
}

int main(void) {
    uint32_t version = 0;
    cynapsa_buffer_desc_v1 error = {0, 0};
    if (cynapsa_v1_abi_version(&version, &error) != CYNAPSA_STATUS_V1_OK || version != 1 || error.buffer_handle != 0) {
        return 1;
    }

    int export_status = test_export_output_initialization();
    if (export_status != 0) {
        return export_status;
    }

    int output_status = test_buffer_read_output_initialization();
    if (output_status != 0) {
        return output_status;
    }

    static const uint8_t malformed[] = "{}";
    cynapsa_core_t core = 0;
    if (cynapsa_v1_core_create(malformed, sizeof(malformed) - 1, &core, &error) != CYNAPSA_STATUS_V1_ERROR || core != 0) {
        return 2;
    }
    int error_status = read_and_free_error(error, "unsupported_version");
    if (error_status != 0) {
        return error_status;
    }

    static const uint8_t config[] = "{\"abi_version\":1,\"command_timeout_ms\":0,\"rpc_timeout_ms\":0,\"queue_limit\":4,\"payload_limit\":1048576}";
    if (cynapsa_v1_core_create(config, sizeof(config) - 1, &core, &error) != CYNAPSA_STATUS_V1_OK || core == 0) {
        return 3;
    }
    if (cynapsa_v1_core_start(core, &error) != CYNAPSA_STATUS_V1_OK) {
        return 4;
    }
    if (cynapsa_v1_callbacks_register(core, callback, 11, 12, 13, 4, &error) != CYNAPSA_STATUS_V1_OK) {
        return 5;
    }
    static const uint8_t command[] = "{\"abi_version\":1,\"command_id\":\"native-smoke\",\"command_name\":\"core.init\",\"sdk_session_id\":\"session-1\",\"args\":{}}";
    cynapsa_buffer_desc_v1 admission = {0, 0};
    if (cynapsa_v1_core_submit(core, command, sizeof(command) - 1, &admission, &error) != CYNAPSA_STATUS_V1_OK || admission.buffer_handle == 0) {
        return 6;
    }
    if (cynapsa_v1_buffer_free(admission.buffer_handle, &error) != CYNAPSA_STATUS_V1_OK) {
        return 7;
    }
    struct timespec interval = {0, 1000000};
    for (int attempt = 0; attempt < 5000 && atomic_load(&completion_seen) == 0; attempt++) {
        nanosleep(&interval, NULL);
    }
    if (atomic_load(&completion_seen) == 0 || atomic_load(&callback_failed) != 0) {
        return 8;
    }
    if (cynapsa_v1_core_shutdown(core, 5000, &error) != CYNAPSA_STATUS_V1_OK) {
        return 9;
    }
    if (cynapsa_v1_core_destroy(core, &error) != CYNAPSA_STATUS_V1_OK) {
        return 10;
    }
    if (cynapsa_v1_core_destroy(core, &error) != CYNAPSA_STATUS_V1_OK) {
        return 11;
    }
    int capacity_status = test_emergency_capacity_descriptor();
    if (capacity_status != 0) {
        return capacity_status;
    }
    return 0;
}
