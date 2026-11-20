#!/usr/bin/env bash
# Build an Antithesis-instrumented StarRocks 4.1.7 BE (starrocks_be) with Clang 18,
# inside sr-dev-clang:4.1.7 (Dockerfile.dev-clang). SRC: a 4.1.7 checkout with
# starrocks-4.1.7.patch applied (default $HOME/sr-417; the patch adds the instrumentation include to starrocks_main.cpp and drops -Werror).
# SDK: an antithesis-sdk-cpp clone. STUB: a directory holding cov_stub.o, compiled
# from cov_stub.c. stub/cov_stub.o (weak no-op coverage hooks) is linked into every
# executable so CMake's feature tests link; starrocks_be gets the strong hooks
# from antithesis_instrumentation.h.
# Output: $SRC/output/be/lib/starrocks_be (the script fails if missing).
set -euo pipefail
JOBS=${JOBS:-16}
SRC=${SRC:-$HOME/sr-417}; SDK=${SDK:-$HOME/antithesis-sdk}; STUB=${STUB:-$HOME/antithesis-build/stub}
[ "${CLEAN:-1}" = 1 ] && docker run --rm -v "$SRC":/src alpine rm -rf /src/be/build_Release /src/output/be
docker run --rm --name sr-be-build \
  -v "$SRC":/root/starrocks \
  -v "$SDK":/sdk:ro \
  -v "$STUB":/stub:ro \
  -w /root/starrocks --entrypoint bash sr-dev-clang:4.1.7 -c "
    set -e
    export CC=/usr/lib/llvm-18/bin/clang CXX=/usr/lib/llvm-18/bin/clang++
    export STARROCKS_CXX_COMMON_FLAGS='--gcc-toolchain=/opt/gcc-toolset-14 -I/sdk -fsanitize-coverage=trace-pc-guard,pc-table -fno-sanitize-link-runtime -g -Wno-unused-command-line-argument'
    export STARROCKS_CXX_LINKER_FLAGS='--gcc-toolchain=/opt/gcc-toolset-14 -fno-sanitize-link-runtime -Wl,--build-id /stub/cov_stub.o'
    ./build.sh --be --without-java-ext -j $JOBS
  "
B=$SRC/output/be/lib/starrocks_be
[ -f "$B" ] || { echo "BUILD FAILED: $B not produced"; exit 1; }
echo "BUILD OK: $B"
