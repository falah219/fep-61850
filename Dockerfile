# syntax=docker/dockerfile:1
# Image produksi FEP IEC 61850 (pola sama dengan Dockerfile FEP IEC 104).

FROM rust:slim-bookworm AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        git ca-certificates build-essential cmake clang libclang-dev pkg-config libssl-dev libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

# libIEC61850 di-pin ke tag rilis (setara pin commit lib60870 di FEP IEC 104), link statis.
ARG LIBIEC61850_TAG=v1.6.2.1
RUN git clone --depth 1 -b ${LIBIEC61850_TAG} https://github.com/mz-automation/libiec61850.git /tmp/libiec61850 \
    && cmake -S /tmp/libiec61850 -B /tmp/libiec61850/build -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=/usr/local \
    && cmake --build /tmp/libiec61850/build -j"$(nproc)" \
    && cmake --install /tmp/libiec61850/build \
    && rm -rf /tmp/libiec61850

WORKDIR /srv/fep-iec61850
COPY Cargo.toml Cargo.lock build.rs wrapper.h ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/srv/fep-iec61850/target \
    cargo build --release --bin fep-iec61850 \
    && cp target/release/fep-iec61850 /usr/local/bin/fep-iec61850 \
    && strip /usr/local/bin/fep-iec61850

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libsasl2-2 libssl3 libcap2-bin \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r appuser && useradd -r -g appuser -d /app appuser

WORKDIR /app
COPY --from=builder /usr/local/bin/fep-iec61850 /app/

# Ping ICMP (status per channel) butuh raw socket.
RUN setcap cap_net_raw+ep /app/fep-iec61850 && chown -R appuser:appuser /app

USER appuser
CMD ["./fep-iec61850"]
