# Base images come from a pull-through mirror by default: Docker Hub rate-limits
# anonymous pulls (HTTP 429), which silently leaves a stale driver image behind.
ARG RUST_IMAGE=mirror.gcr.io/library/rust:1-bookworm
ARG RUNTIME_IMAGE=mirror.gcr.io/library/debian:bookworm-slim
FROM ${RUST_IMAGE} AS build
WORKDIR /src
COPY driver/Cargo.toml driver/Cargo.lock* ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src
COPY driver/src ./src
RUN touch src/main.rs && cargo build --release

FROM ${RUNTIME_IMAGE}
RUN apt-get update -y && apt-get install -y --no-install-recommends ca-certificates curl jq default-mysql-client \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/driver /usr/local/bin/driver
COPY driver/first_start_workload /opt/antithesis/test/v1/main/first_start_workload
RUN chmod 755 /opt/antithesis/test/v1/main/first_start_workload
CMD ["/usr/local/bin/driver"]
