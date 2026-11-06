#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/check_docs.py
if [[ -f Cargo.toml ]]; then
  command -v cargo >/dev/null || { echo 'Cargo.toml exists but cargo missing'; exit 1; }
  cargo fmt --all --check
  cargo check --workspace
else
  echo 'Rust workspace not bootstrapped: documentation-only preflight PASS; no Rust tests claimed'
fi
