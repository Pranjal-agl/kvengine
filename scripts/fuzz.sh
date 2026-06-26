#!/usr/bin/env bash
# scripts/fuzz.sh — run all fuzz targets for a given duration.
#
# Usage:
#   chmod +x scripts/fuzz.sh
#   ./scripts/fuzz.sh           # runs each fuzzer for 30 seconds (default)
#   ./scripts/fuzz.sh 120       # runs each fuzzer for 2 minutes
#   ./scripts/fuzz.sh 0         # just run the seed corpus (no fuzzing, fast CI check)
#
# Any crash found by the fuzzer is saved to testdata/fuzz/<FuzzTarget>/<hash>
# and the next `go test` run will replay it automatically.

set -euo pipefail

DURATION="${1:-30}"
FUZZ_CMD="go test -fuzz"

echo "========================================"
echo "  kvengine fuzz suite (${DURATION}s each)"
echo "========================================"
echo ""

run_fuzzer() {
    local pkg="$1"
    local target="$2"
    echo "--- $target ($pkg) ---"
    if [ "$DURATION" -eq 0 ]; then
        # Just run seed corpus — no fuzzing, just crash-check.
        go test -run="$target" "./$pkg/..."
    else
        go test -fuzz="$target" -fuzztime="${DURATION}s" "./$pkg/..."
    fi
    echo ""
}

run_fuzzer "internal/wal"    "FuzzReadRecord"
run_fuzzer "internal/wal"    "FuzzWALAppendReplay"
run_fuzzer "internal/server" "FuzzServerProtocol"
run_fuzzer "internal/raft"   "FuzzHandleRequestVote"
run_fuzzer "internal/raft"   "FuzzHandleAppendEntries"

echo "========================================"
echo "  All fuzz targets completed"
echo ""
echo "  Corpus entries are in testdata/fuzz/"
echo "  Re-run failing cases: go test -run=FuzzXxx ./internal/xxx/..."
echo "========================================"
