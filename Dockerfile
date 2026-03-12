FROM rust:latest AS builder

RUN apt-get update && apt-get install -y protobuf-compiler libprotobuf-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cargo build --release --bin cove-server --bin cove-worker

FROM debian:trixie-slim

RUN apt-get update && \
    apt-get install -y ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash cove

COPY --from=builder /build/target/release/cove-server /usr/local/bin/cove-server
COPY --from=builder /build/target/release/cove-worker /usr/local/bin/cove-worker
COPY migrations /opt/cove/migrations

RUN mkdir -p /opt/cove/data/media && chown -R cove:cove /opt/cove

USER cove
WORKDIR /opt/cove

EXPOSE 50051
EXPOSE 9090

CMD ["cove-server"]
