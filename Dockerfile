# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
# Carried in the binary by build.rs and served at /docs/mcp. .dockerignore leaves out docs/records.
COPY build.rs ./
COPY docs ./docs
# The commit the image was built from, which /docs/mcp reports.
ARG VERSION=unknown
ENV VERSION=${VERSION}
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo clean --release -p agent-ti -p agent-ti-config \
      -p agent-ti-core -p agent-ti-plugin-sdk -p agent-ti-store \
    && cargo build --release --locked && cp target/release/agent-ti /agent-ti

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 agent-ti && useradd --uid 10001 --gid 10001 --no-create-home agent-ti \
    && mkdir -p /data && chown 10001:10001 /data
COPY --from=build /agent-ti /usr/local/bin/agent-ti
# The host file every deployment starts from; it takes its settings from the environment
# and loads each agent folder in /etc/agent-ti/agents, which the image leaves empty.
COPY deploy/k8s/base/config/agent-ti.json /etc/agent-ti/host/agent-ti.json
USER 10001:10001
WORKDIR /opt/agent-ti
EXPOSE 18120
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s CMD curl -fsS http://127.0.0.1:18120/readyz || exit 1
ENTRYPOINT ["agent-ti"]
CMD ["serve", "/etc/agent-ti/host/agent-ti.json"]
