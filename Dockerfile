FROM clux/muslrust:stable AS chef
USER root
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG CARGO_FEATURES="default,metrics"
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json --features "$CARGO_FEATURES"
COPY . .
RUN cargo build --release --locked --target x86_64-unknown-linux-musl --features "$CARGO_FEATURES"

FROM scratch
ENV PIPR__BIND=0.0.0.0:3000
COPY docker/passwd /etc/
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 65534
WORKDIR /var/lib/python-index-proxy
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/python-index-proxy /bin/python-index-proxy
ENTRYPOINT [ "/bin/python-index-proxy" ]
