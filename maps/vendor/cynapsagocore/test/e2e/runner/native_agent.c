#define _POSIX_C_SOURCE 200809L
#include "cynapsacore_v1.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static _Atomic int completion_seen = 0;
static _Atomic int callback_failed = 0;
static _Atomic int attempt_reentrant_clear = 0;
static cynapsa_core_t active_core = 0;
static int32_t reentrant_clear_status = CYNAPSA_STATUS_V1_OK;
static cynapsa_buffer_desc_v1 reentrant_clear_error = {0, 0};

static void callback(uint64_t token, uint32_t kind, cynapsa_buffer_desc_v1 value) {
    (void)token;
    cynapsa_buffer_desc_v1 error = {0, 0};
    if (kind == CYNAPSA_CALLBACK_V1_COMPLETION &&
        atomic_exchange(&attempt_reentrant_clear, 0) == 1) {
        reentrant_clear_status = cynapsa_v1_callbacks_clear(active_core, 1, &reentrant_clear_error);
    }
    if (value.buffer_handle == 0 ||
        cynapsa_v1_buffer_free(value.buffer_handle, &error) != CYNAPSA_STATUS_V1_OK) {
        atomic_store(&callback_failed, 1);
    }
    if (kind == CYNAPSA_CALLBACK_V1_COMPLETION) {
        atomic_store(&completion_seen, 1);
    }
}

static char *read_buffer(cynapsa_buffer_desc_v1 value) {
    if (value.buffer_handle == 0 || value.byte_length == 0 || value.byte_length > (1U << 20)) {
        return NULL;
    }
    char *bytes = (char *)calloc((size_t)value.byte_length + 1, 1);
    if (bytes == NULL) {
        return NULL;
    }
    uint64_t copied = 0;
    cynapsa_buffer_desc_v1 nested = {0, 0};
    if (cynapsa_v1_buffer_read(value.buffer_handle, 0, (uint8_t *)bytes, value.byte_length,
                               &copied, &nested) != CYNAPSA_STATUS_V1_OK ||
        copied != value.byte_length) {
        free(bytes);
        return NULL;
    }
    return bytes;
}

static int emit_and_free_document(const char *scenario, cynapsa_buffer_desc_v1 value) {
    char *document = read_buffer(value);
    if (document == NULL) {
        return 1;
    }
    printf("{\"scenario\":\"%s\",\"document\":%s}\n", scenario, document);
    free(document);
    cynapsa_buffer_desc_v1 error = {0, 0};
    return cynapsa_v1_buffer_free(value.buffer_handle, &error) == CYNAPSA_STATUS_V1_OK ? 0 : 1;
}

static int wait_for_completion(void) {
    const struct timespec interval = {0, 1000000};
    for (int attempt = 0; attempt < 5000 && atomic_load(&completion_seen) == 0; attempt++) {
        nanosleep(&interval, NULL);
    }
    return atomic_load(&completion_seen) != 0 && atomic_load(&callback_failed) == 0 ? 0 : 1;
}

static int extract_payload_handle(const char *document, char *handle, size_t capacity) {
    static const char prefix[] = "\"payload_handle\":\"";
    const char *start = strstr(document, prefix);
    if (start == NULL) {
        return 1;
    }
    start += sizeof(prefix) - 1;
    const char *end = strchr(start, '"');
    if (end == NULL || end == start || (size_t)(end - start) >= capacity) {
        return 1;
    }
    memcpy(handle, start, (size_t)(end - start));
    handle[end - start] = '\0';
    return 0;
}

int main(void) {
    uint32_t version = 0;
    cynapsa_buffer_desc_v1 error = {0, 0};
    if (cynapsa_v1_abi_version(&version, &error) != CYNAPSA_STATUS_V1_OK || version != 1) {
        return 1;
    }
    printf("{\"scenario\":\"abi_version_positive\",\"version\":%u}\n", version);

    cynapsa_buffer_desc_v1 result = {0, 0};
    if (cynapsa_v1_core_status(0, &result, &error) != CYNAPSA_STATUS_V1_ERROR ||
        result.buffer_handle != 0 ||
        emit_and_free_document("abi_invalid_core_handle_negative", error) != 0) {
        return 2;
    }

    static const uint8_t unsupported[] =
        "{\"abi_version\":2,\"command_timeout_ms\":0,\"rpc_timeout_ms\":0,"
        "\"queue_limit\":4,\"payload_limit\":1048576}";
    cynapsa_core_t core = 0;
    if (cynapsa_v1_core_create(unsupported, sizeof(unsupported) - 1, &core, &error) !=
            CYNAPSA_STATUS_V1_ERROR ||
        core != 0 || emit_and_free_document("abi_version_mismatch_negative", error) != 0) {
        return 3;
    }

    static const uint8_t config[] =
        "{\"abi_version\":1,\"command_timeout_ms\":0,\"rpc_timeout_ms\":0,"
        "\"queue_limit\":4,\"payload_limit\":1048576}";
    if (cynapsa_v1_core_create(config, sizeof(config) - 1, &core, &error) !=
            CYNAPSA_STATUS_V1_OK ||
        core == 0) {
        return 4;
    }
    active_core = core;
    if (cynapsa_v1_core_start(core, &error) != CYNAPSA_STATUS_V1_OK) {
        return 5;
    }
    printf("{\"scenario\":\"abi_core_lifecycle_positive\",\"started\":true}\n");

    if (cynapsa_v1_core_next_completion(core, 1, &result, &error) !=
            CYNAPSA_STATUS_V1_WAIT_TIMEOUT ||
        result.buffer_handle != 0 || error.buffer_handle != 0) {
        return 6;
    }
    printf("{\"scenario\":\"abi_poll_timeout_positive\",\"wait_timeout\":true}\n");

    static const uint8_t unknown[] =
        "{\"abi_version\":1,\"command_id\":\"native-unknown\","
        "\"command_name\":\"unknown.command\",\"sdk_session_id\":\"session-1\",\"args\":{}}";
    if (cynapsa_v1_core_submit(core, unknown, sizeof(unknown) - 1, &result, &error) !=
            CYNAPSA_STATUS_V1_ERROR ||
        result.buffer_handle != 0 ||
        emit_and_free_document("capabilities_unknown_command_negative", error) != 0) {
        return 7;
    }

    static const uint8_t duplicate[] =
        "{\"abi_version\":1,\"abi_version\":1,\"command_id\":\"native-duplicate\","
        "\"command_name\":\"core.init\",\"sdk_session_id\":\"session-1\",\"args\":{}}";
    if (cynapsa_v1_core_submit(core, duplicate, sizeof(duplicate) - 1, &result, &error) !=
            CYNAPSA_STATUS_V1_ERROR ||
        result.buffer_handle != 0 ||
        emit_and_free_document("abi_duplicate_field_negative", error) != 0) {
        return 8;
    }

    static const uint8_t command[] =
        "{\"abi_version\":1,\"command_id\":\"native-smoke\","
        "\"command_name\":\"core.init\",\"sdk_session_id\":\"session-1\",\"args\":{}}";
    if (cynapsa_v1_core_submit(core, command, sizeof(command) - 1, &result, &error) !=
            CYNAPSA_STATUS_V1_OK ||
        result.buffer_handle == 0) {
        return 9;
    }
    cynapsa_buffer_t retired = result.buffer_handle;
    if (emit_and_free_document("abi_submit_positive", result) != 0) {
        return 10;
    }
    if (cynapsa_v1_buffer_free(retired, &error) != CYNAPSA_STATUS_V1_ERROR ||
        emit_and_free_document("abi_buffer_double_free_negative", error) != 0) {
        return 11;
    }

    if (cynapsa_v1_callbacks_register(core, callback, 11, 12, 13, 4, &error) !=
        CYNAPSA_STATUS_V1_OK) {
        return 12;
    }
    atomic_store(&attempt_reentrant_clear, 1);
    static const uint8_t callback_command[] =
        "{\"abi_version\":1,\"command_id\":\"native-callback\","
        "\"command_name\":\"core.init\",\"sdk_session_id\":\"session-1\",\"args\":{}}";
    if (cynapsa_v1_core_submit(core, callback_command, sizeof(callback_command) - 1, &result,
                               &error) != CYNAPSA_STATUS_V1_OK ||
        cynapsa_v1_buffer_free(result.buffer_handle, &error) != CYNAPSA_STATUS_V1_OK ||
        wait_for_completion() != 0) {
        return 13;
    }
    printf("{\"scenario\":\"abi_callback_positive\",\"completion_seen\":true}\n");
    if (reentrant_clear_status != CYNAPSA_STATUS_V1_ERROR ||
        emit_and_free_document("abi_callback_reentrant_clear_negative", reentrant_clear_error) != 0) {
        return 14;
    }
    if (cynapsa_v1_callbacks_clear(core, 5000, &error) != CYNAPSA_STATUS_V1_OK) {
        return 15;
    }
    printf("{\"scenario\":\"abi_callback_clear_positive\",\"cleared\":true}\n");

    if (cynapsa_v1_payload_open(core, &result, &error) != CYNAPSA_STATUS_V1_OK) {
        return 16;
    }
    char *open_document = read_buffer(result);
    char payload_handle[128] = {0};
    if (open_document == NULL ||
        extract_payload_handle(open_document, payload_handle, sizeof(payload_handle)) != 0) {
        free(open_document);
        return 17;
    }
    printf("{\"scenario\":\"abi_payload_open_positive\",\"document\":%s}\n", open_document);
    free(open_document);
    if (cynapsa_v1_buffer_free(result.buffer_handle, &error) != CYNAPSA_STATUS_V1_OK) {
        return 18;
    }

    char payload_input[512] = {0};
    int input_length = snprintf(payload_input, sizeof(payload_input),
                                "{\"abi_version\":1,\"handle\":\"%s\","
                                "\"chunk\":\"pQABAQACYANhLwRBeA==\"}",
                                payload_handle);
    if (input_length <= 0 || (size_t)input_length >= sizeof(payload_input) ||
        cynapsa_v1_payload_write(core, (const uint8_t *)payload_input, (uint64_t)input_length,
                                 &result, &error) != CYNAPSA_STATUS_V1_OK ||
        emit_and_free_document("abi_payload_write_positive", result) != 0) {
        return 19;
    }
    input_length = snprintf(payload_input, sizeof(payload_input),
                            "{\"abi_version\":1,\"handle\":\"%s\"}", payload_handle);
    if (input_length <= 0 || (size_t)input_length >= sizeof(payload_input) ||
        cynapsa_v1_payload_finish(core, (const uint8_t *)payload_input, (uint64_t)input_length,
                                  &result, &error) != CYNAPSA_STATUS_V1_OK ||
        emit_and_free_document("abi_payload_finish_positive", result) != 0) {
        return 20;
    }
    if (cynapsa_v1_payload_retain(core, (const uint8_t *)payload_input, (uint64_t)input_length,
                                  &error) != CYNAPSA_STATUS_V1_OK ||
        cynapsa_v1_payload_release(core, (const uint8_t *)payload_input, (uint64_t)input_length,
                                   &error) != CYNAPSA_STATUS_V1_OK) {
        return 21;
    }
    input_length = snprintf(payload_input, sizeof(payload_input),
                            "{\"abi_version\":1,\"handle\":\"%s\",\"offset\":0,\"limit\":13}",
                            payload_handle);
    if (input_length <= 0 || (size_t)input_length >= sizeof(payload_input) ||
        cynapsa_v1_payload_read(core, (const uint8_t *)payload_input, (uint64_t)input_length,
                                &result, &error) != CYNAPSA_STATUS_V1_OK ||
        emit_and_free_document("abi_payload_read_positive", result) != 0) {
        return 22;
    }
    input_length = snprintf(payload_input, sizeof(payload_input),
                            "{\"abi_version\":1,\"handle\":\"%s\"}", payload_handle);
    if (cynapsa_v1_payload_release(core, (const uint8_t *)payload_input, (uint64_t)input_length,
                                   &error) != CYNAPSA_STATUS_V1_OK ||
        cynapsa_v1_payload_release(core, (const uint8_t *)payload_input, (uint64_t)input_length,
                                   &error) != CYNAPSA_STATUS_V1_ERROR ||
        emit_and_free_document("abi_payload_double_release_negative", error) != 0) {
        return 23;
    }

    if (cynapsa_v1_payload_open(core, &result, &error) != CYNAPSA_STATUS_V1_OK) {
        return 24;
    }
    open_document = read_buffer(result);
    if (open_document == NULL ||
        extract_payload_handle(open_document, payload_handle, sizeof(payload_handle)) != 0) {
        free(open_document);
        return 25;
    }
    free(open_document);
    if (cynapsa_v1_buffer_free(result.buffer_handle, &error) != CYNAPSA_STATUS_V1_OK) {
        return 26;
    }
    input_length = snprintf(payload_input, sizeof(payload_input),
                            "{\"abi_version\":1,\"handle\":\"%s\"}", payload_handle);
    if (cynapsa_v1_payload_cancel(core, (const uint8_t *)payload_input, (uint64_t)input_length,
                                  &error) != CYNAPSA_STATUS_V1_OK ||
        cynapsa_v1_payload_cancel(core, (const uint8_t *)payload_input, (uint64_t)input_length,
                                  &error) != CYNAPSA_STATUS_V1_OK) {
        return 27;
    }
    printf("{\"scenario\":\"abi_payload_cancel_idempotent_positive\",\"cancelled\":true}\n");
    if (cynapsa_v1_payload_release(core, (const uint8_t *)payload_input, (uint64_t)input_length,
                                   &error) != CYNAPSA_STATUS_V1_OK) {
        return 28;
    }

    if (cynapsa_v1_callbacks_register(core, callback, 21, 22, 23, 4, &error) !=
        CYNAPSA_STATUS_V1_OK) {
        return 29;
    }
    if (cynapsa_v1_core_shutdown(core, 5000, &error) != CYNAPSA_STATUS_V1_OK) {
        return 30;
    }
    if (cynapsa_v1_core_destroy(core, &error) != CYNAPSA_STATUS_V1_OK ||
        cynapsa_v1_core_destroy(core, &error) != CYNAPSA_STATUS_V1_OK) {
        return 31;
    }
    printf("{\"scenario\":\"abi_shutdown_destroy_positive\",\"destroyed\":true}\n");
    return 0;
}
