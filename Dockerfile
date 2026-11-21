# Shopping Sessions: Bare-metal AVX-512 Rust pipeline with PGO
FROM rust:1.94-bookworm AS builder

RUN rustup component add llvm-tools-preview

WORKDIR /build
COPY Cargo.toml ./
COPY src ./src
COPY generator ./generator

# 1. Build dataset generator and create a 2M-event PGO training dataset
RUN cargo build --release --manifest-path generator/Cargo.toml && \
    ./generator/target/release/generate --seed 42 --events 2000000 --clicks 500000 --output /tmp/pgo_data

# 2. Build instrumented binary, collect profile on host microarchitecture, merge profile
RUN RUSTFLAGS="-C target-cpu=native -C profile-generate=/tmp/pgo" cargo build --release && \
    PGO_TRAIN=1 NCPU=4 ./target/release/shopping-sessions /tmp/pgo_data /tmp/pgo_out && \
    LLVM_PROFDATA="$(find "$(rustc --print sysroot)" -name llvm-profdata -type f | head -n 1)" && \
    "$LLVM_PROFDATA" merge -o /tmp/pgo/merged.profdata /tmp/pgo/*.profraw && \
    rm -rf /tmp/pgo_data /tmp/pgo_out

# 3. Recompile final binary with PGO + native target tuning
RUN RUSTFLAGS="-C target-cpu=native -C profile-use=/tmp/pgo/merged.profdata -C llvm-args=-pgo-warn-missing-function" cargo build --release && \
    rm -rf /tmp/pgo

FROM debian:bookworm-slim

RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY --from=builder /build/target/release/shopping-sessions /app/shopping-sessions
RUN chmod 0755 /app/shopping-sessions

WORKDIR /app
CMD ["/app/shopping-sessions"]
