#!/usr/bin/env bash
# scripts/flamegraph.sh — generate a CPU flamegraph of the kvengine server under load.
#
# Prerequisites:
#   go install github.com/google/pprof@latest
#   (pprof is also bundled with Go: go tool pprof)
#
# Usage:
#   chmod +x scripts/flamegraph.sh
#   ./scripts/flamegraph.sh
#
# Output: profiles/cpu.svg   (open in a browser)
#         profiles/cpu.pb.gz (raw pprof data, use go tool pprof to inspect)

set -euo pipefail

BINARY="./kvengine"
WAL="/tmp/flamegraph-bench.wal"
KV_ADDR="127.0.0.1:6399"
PPROF_ADDR="127.0.0.1:6060"
PROFILE_SECS=15
OUTDIR="profiles"

mkdir -p "$OUTDIR"
rm -f "$WAL"

echo "==> Building kvengine..."
go build -o "$BINARY" ./cmd/kvengine

echo "==> Starting server (KV=$KV_ADDR, pprof=$PPROF_ADDR)..."
"$BINARY" "$WAL" serve "$KV_ADDR" &
SERVER_PID=$!
trap "kill $SERVER_PID 2>/dev/null; rm -f $WAL $BINARY" EXIT
sleep 0.5

echo "==> Generating load for ${PROFILE_SECS}s while capturing CPU profile..."

# Start CPU profile capture in background.
go tool pprof -seconds="$PROFILE_SECS" \
    -proto \
    -output "$OUTDIR/cpu.pb.gz" \
    "http://$PPROF_ADDR/debug/pprof/profile" &
PPROF_PID=$!

# Generate load: concurrent writers hitting the server.
for i in $(seq 1 8); do
    (
        while true; do
            printf 'PUT bench%d 5\r\nhello\r\n' "$i" | nc -q0 "${KV_ADDR%:*}" "${KV_ADDR#*:}" 2>/dev/null || true
        done
    ) &
done
LOAD_PIDS=$(jobs -p | tail -8)

echo "==> Load running (8 concurrent writers)... waiting ${PROFILE_SECS}s"
wait $PPROF_PID || true

# Kill load generators.
for pid in $LOAD_PIDS; do
    kill "$pid" 2>/dev/null || true
done

echo ""
echo "==> Profile captured: $OUTDIR/cpu.pb.gz"
echo ""
echo "==> Generating SVG flamegraph..."
go tool pprof -svg "$OUTDIR/cpu.pb.gz" > "$OUTDIR/cpu.svg" 2>/dev/null || \
    go tool pprof -svg -output "$OUTDIR/cpu.svg" "$OUTDIR/cpu.pb.gz" 2>/dev/null || \
    echo "Note: SVG generation failed — open $OUTDIR/cpu.pb.gz with: go tool pprof $OUTDIR/cpu.pb.gz"

echo ""
echo "==> Done. Results:"
echo "    Flamegraph SVG : $OUTDIR/cpu.svg    (open in browser)"
echo "    Raw profile    : $OUTDIR/cpu.pb.gz"
echo ""
echo "==> Interactive analysis:"
echo "    go tool pprof $OUTDIR/cpu.pb.gz"
echo "    Then type: web   (opens flamegraph in browser)"
echo "               top   (shows hottest functions)"
echo "               list <funcname>  (annotated source)"
