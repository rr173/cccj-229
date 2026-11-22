#!/usr/bin/env bash
# End-to-end, self-checking demo of slice mirror delivery (full + delta files,
# hand-computed diff reconciliation, snapshot consistency, failure recovery).
set -euo pipefail
cd "$(dirname "$0")/.."
python3 demo/run_mirror_demo.py
