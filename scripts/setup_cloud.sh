#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python -m pip install -e '.[test]'
python -m pytest -q
python scripts/fetch_data.py --groups "${BONE_DATA_GROUPS:-all}"
if [ "${BONE_DATA_GROUPS:-all}" = all ]; then
  python scripts/validate_bundle.py --require-all
else
  python scripts/validate_bundle.py
fi
python scripts/run_baseline.py
