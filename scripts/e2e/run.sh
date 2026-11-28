#!/usr/bin/env bash
# Builds kvengine (and the v1.0 release, for the migration test) and runs the
# end-to-end scenarios against real processes.
#
#   bash scripts/e2e/run.sh                 # everything
#   bash scripts/e2e/run.sh failover reads  # some scenarios
#
# KVENGINE_OLD_REF picks the old release (default: v1.0, the last commit on
# main before the correctness rewrite). Set it to "none" to skip migrate.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
old_ref=${KVENGINE_OLD_REF:-47f1503}
work=$(mktemp -d)
trap 'git -C "$root" worktree remove --force "$work/old-src" 2>/dev/null || true; rm -rf "$work"' EXIT

echo "building kvengine"
(cd "$root" && go build -o "$work/kvengine" ./cmd/kvengine)

old_args=()
if [ "$old_ref" != "none" ]; then
  if git -C "$root" cat-file -e "$old_ref^{commit}" 2>/dev/null; then
    echo "building old release $old_ref"
    git -C "$root" worktree add --quiet --detach "$work/old-src" "$old_ref"
    (cd "$work/old-src" && go build -o "$work/kvengine-old" ./cmd/kvengine)
    old_args=(--old-bin "$work/kvengine-old")
  else
    echo "old release $old_ref not in this clone (shallow?); migrate will be skipped"
  fi
fi

python3 "$root/scripts/e2e/run.py" --bin "$work/kvengine" "${old_args[@]}" "$@"
