#ifndef CYNAPSACORE_V1_H
#define CYNAPSACORE_V1_H

#include <stdint.h>

#if defined(_WIN32) && defined(CYNAPSA_BUILD)
#define CYNAPSA_API __declspec(dllexport)
#elif defined(_WIN32)
#define CYNAPSA_API __declspec(dllimport)
#else
#define CYNAPSA_API __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t cynapsa_core_t;
typedef uint64_t cynapsa_buffer_t;

typedef struct cynapsa_buffer_desc_v1 {
    uint64_t buffer_handle;
    uint64_t byte_length;
} cynapsa_buffer_desc_v1;

typedef enum cynapsa_status_v1 {
    CYNAPSA_STATUS_V1_OK = 0,
    CYNAPSA_STATUS_V1_ERROR = 1,
    CYNAPSA_STATUS_V1_WAIT_TIMEOUT = 2
} cynapsa_status_v1;

typedef enum cynapsa_callback_kind_v1 {
    CYNAPSA_CALLBACK_V1_COMPLETION = 1,
    CYNAPSA_CALLBACK_V1_EVENT = 2,
    CYNAPSA_CALLBACK_V1_DIAGNOSTIC = 3
} cynapsa_callback_kind_v1;

typedef void (*cynapsa_callback_v1)(
    uint64_t token,
    uint32_t kind,
    cynapsa_buffer_desc_v1 value);

/* Input bytes are borrowed for one call and copied by the library. Every
 * nonzero result/error descriptor is immutable library-owned data: copy it
 * with buffer_read, then pass it to buffer_free. Ordinary descriptors retire
 * exactly once. The process-owned capacity-error descriptor is the sole
 * exception; buffer_free accepts it as an idempotent no-op. */

/* Every function resets each non-NULL scalar or descriptor output before it
 * validates any argument (including another required output) or enters a
 * recoverable panic path. A failed call never leaves a stale caller value. */

CYNAPSA_API int32_t cynapsa_v1_abi_version(uint32_t *out_version, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_core_create(const uint8_t *input, uint64_t input_length, cynapsa_core_t *out_core, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_core_start(cynapsa_core_t core, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_core_submit(cynapsa_core_t core, const uint8_t *input, uint64_t input_length, cynapsa_buffer_desc_v1 *out_result, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_core_cancel(cynapsa_core_t core, const uint8_t *input, uint64_t input_length, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_core_next_completion(cynapsa_core_t core, int64_t timeout_ms, cynapsa_buffer_desc_v1 *out_result, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_core_next_event(cynapsa_core_t core, int64_t timeout_ms, cynapsa_buffer_desc_v1 *out_result, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_core_status(cynapsa_core_t core, cynapsa_buffer_desc_v1 *out_result, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_core_shutdown(cynapsa_core_t core, int64_t timeout_ms, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_core_destroy(cynapsa_core_t core, cynapsa_buffer_desc_v1 *out_error);

CYNAPSA_API int32_t cynapsa_v1_payload_open(cynapsa_core_t core, cynapsa_buffer_desc_v1 *out_result, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_payload_write(cynapsa_core_t core, const uint8_t *input, uint64_t input_length, cynapsa_buffer_desc_v1 *out_result, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_payload_finish(cynapsa_core_t core, const uint8_t *input, uint64_t input_length, cynapsa_buffer_desc_v1 *out_result, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_payload_read(cynapsa_core_t core, const uint8_t *input, uint64_t input_length, cynapsa_buffer_desc_v1 *out_result, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_payload_cancel(cynapsa_core_t core, const uint8_t *input, uint64_t input_length, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_payload_retain(cynapsa_core_t core, const uint8_t *input, uint64_t input_length, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_payload_release(cynapsa_core_t core, const uint8_t *input, uint64_t input_length, cynapsa_buffer_desc_v1 *out_error);

/* Callback and polling consumers compete for the same normalized queues.
 * The callback owns each delivered descriptor until buffer_free. A successful
 * callbacks_clear guarantees that no callback for that registration remains
 * in flight. */
CYNAPSA_API int32_t cynapsa_v1_callbacks_register(cynapsa_core_t core, cynapsa_callback_v1 callback, uint64_t completion_token, uint64_t event_token, uint64_t diagnostic_token, uint32_t capacity, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_callbacks_clear(cynapsa_core_t core, int64_t timeout_ms, cynapsa_buffer_desc_v1 *out_error);

CYNAPSA_API int32_t cynapsa_v1_buffer_read(cynapsa_buffer_t buffer, uint64_t offset, uint8_t *destination, uint64_t destination_length, uint64_t *out_copied, cynapsa_buffer_desc_v1 *out_error);
CYNAPSA_API int32_t cynapsa_v1_buffer_free(cynapsa_buffer_t buffer, cynapsa_buffer_desc_v1 *out_error);

#ifdef __cplusplus
}
#endif

#endif
