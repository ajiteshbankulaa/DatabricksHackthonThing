# Shopping Sessions: Bare-metal AVX-512 Rust pipeline with PGO
FROM rust:1.94-bookworm AS builder

RUN rustup component add llvm-tools-preview

WORKDIR /build
COPY Cargo.toml ./
COPY src ./src
COPY generator ./generator

# 1. Build dataset generator & instrumented binary, collect NCPU=8 PGO profiles across 3 seeds
RUN cargo build --release --manifest-path generator/Cargo.toml && \
    RUSTFLAGS="-C target-cpu=native -C profile-generate=/tmp/pgo" cargo build --release && \
    for seed in 1 42 99; do \
        ./generator/target/release/generate --seed "$seed" --events 2000000 --clicks 500000 --output /tmp/pgo_data && \
        PGO_TRAIN=1 NCPU=8 ./target/release/shopping-sessions /tmp/pgo_data /tmp/pgo_out && \
        rm -rf /tmp/pgo_data /tmp/pgo_out || exit 1; \
    done && \
    LLVM_PROFDATA="$(find "$(rustc --print sysroot)" -name llvm-profdata -type f | head -n 1)" && \
    "$LLVM_PROFDATA" merge -o /tmp/pgo/merged.profdata /tmp/pgo/*.profraw && \
    rm -f /tmp/pgo/*.profraw

# 2. Recompile with merged PGO + native target tuning, then apply guarded instrumented LLVM BOLT
RUN RUSTFLAGS="-C target-cpu=native -C profile-use=/tmp/pgo/merged.profdata -C llvm-args=-pgo-warn-missing-function -C link-arg=-Wl,--emit-relocs" cargo build --release && \
    rm -rf /tmp/pgo && \
    LLVM_BOLT="$(find "$(rustc --print sysroot)" /usr -name llvm-bolt -type f 2>/dev/null | head -n 1)" && \
    if [ -n "$LLVM_BOLT" ]; then \
        ./generator/target/release/generate --seed 42 --events 2000000 --clicks 500000 --output /tmp/bolt_data && \
        "$LLVM_BOLT" /build/target/release/shopping-sessions -instrument -instrumentation-file=/tmp/prof.fdata -o /build/target/release/shopping-sessions.inst && \
        PGO_TRAIN=1 NCPU=8 /build/target/release/shopping-sessions.inst /tmp/bolt_data /tmp/bolt_out && \
        "$LLVM_BOLT" /build/target/release/shopping-sessions -o /build/target/release/shopping-sessions.bolt -data=/tmp/prof.fdata \
            -reorder-blocks=ext-tsp -reorder-functions=hfsort -split-functions -split-all-cold && \
        mv /build/target/release/shopping-sessions.bolt /build/target/release/shopping-sessions || true; \
        rm -rf /tmp/bolt_data /tmp/bolt_out /tmp/prof.fdata /build/target/release/shopping-sessions.inst /build/target/release/shopping-sessions.bolt; \
    fi && \
    strip /build/target/release/shopping-sessions

FROM debian:bookworm-slim

RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY --from=builder /build/target/release/shopping-sessions /app/shopping-sessions
RUN chmod 0755 /app/shopping-sessions

WORKDIR /app
CMD ["/app/shopping-sessions"]
