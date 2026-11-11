FROM clux/muslrust:stable@sha256:12e02bbe193e3bd9255f9b5a9312f0039a0c79b9e015cf5e81eeab83a1eb8e0f AS chef
USER root
RUN cargo install cargo-chef --version '=0.1.78' --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG CARGO_FEATURES="default,metrics"
ARG TARGETARCH
RUN case "$TARGETARCH" in \
      amd64) echo x86_64-unknown-linux-musl > /rust-target ;; \
      arm64) echo aarch64-unknown-linux-musl > /rust-target ;; \
      *) echo "Unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --target "$(cat /rust-target)" --recipe-path recipe.json --features "$CARGO_FEATURES"
COPY . .
RUN cargo build --release --locked --target "$(cat /rust-target)" --features "$CARGO_FEATURES" \
    && mkdir /out \
    && cp "target/$(cat /rust-target)/release/python-index-proxy" /out/python-index-proxy

FROM scratch
ENV PIPR__BIND=0.0.0.0:3000
COPY docker/passwd /etc/
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 65534
WORKDIR /var/lib/python-index-proxy
COPY --from=builder /out/python-index-proxy /bin/python-index-proxy
ENTRYPOINT [ "/bin/python-index-proxy" ]
