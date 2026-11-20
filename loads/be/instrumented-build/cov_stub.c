/* Weak no-op sanitizer-coverage hooks. Linked into every executable of the
 * instrumented StarRocks BE build so CMake's feature-test programs (which are
 * compiled with -fsanitize-coverage but don't include antithesis_instrumentation.h)
 * still link. In starrocks_be, the strong definitions from
 * antithesis_instrumentation.h take precedence. Compiled WITHOUT coverage. */
#include <stdint.h>
__attribute__((weak)) void __sanitizer_cov_trace_pc_guard_init(uint32_t *start, uint32_t *stop) { (void)start; (void)stop; }
__attribute__((weak)) void __sanitizer_cov_trace_pc_guard(uint32_t *guard) { (void)guard; }
__attribute__((weak)) void __sanitizer_cov_pcs_init(const uintptr_t *beg, const uintptr_t *end) { (void)beg; (void)end; }
