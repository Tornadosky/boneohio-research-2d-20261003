"""Extract side-effect-free functions from frozen Q99 sources; no original import."""
from pathlib import Path
import ast
import hashlib
import json

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
SOURCE = HERE / "replay.py"
NAMES = {"_pm", "token_bids", "above_asof", "_cancel", "sim_engine", "sim_fp3", "sim_dm"}
text = SOURCE.read_text(encoding="utf-8")
tree = ast.parse(text)
parts = [ast.get_source_segment(text, n) for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in NAMES]
assert len(parts) == len(NAMES)
header = '''"""Exact function extraction from Q99 decision replay (2026-10-03).

See vendor/maker/SOURCE_MANIFEST.json and docs/MAKER_REFERENCE.md.
No source runner is imported: its original hard-coded output mkdir is avoided.
"""
import numpy as np
import pandas as pd
MIC = 1_000_000
FEED = 13.0
'''
(ROOT / "src/bonebundle/maker/_reference.py").write_text(header + "\n\n" + "\n\n\n".join(parts) + "\n", encoding="utf-8")
sources = {
    "replay.py": "q99_20261002_DEPLOYGAP/code/replay/replay.py",
    "fillrep.py": "q99_20261002_DEPLOYGAP/code/fill/fillrep.py",
    "queue99_backtester.py": "q99_20261002_DEPLOYGAP/lat/pkg_v9fp/queue99_backtester.py",
    "canonical_bone.py": "boneohio_20261002_FINAL/maker/canonical_bone.py",
    "REPLAY.md": "q99_20261002_DEPLOYGAP/notes/REPLAY.md",
    "RUNBOOK.md": "boneohio_20261002_FINAL/maker/RUNBOOK.md",
}
old = json.loads((HERE / "SOURCE_MANIFEST.json").read_text(encoding="utf-8")) if (HERE / "SOURCE_MANIFEST.json").exists() else {}
original = {r["file"]: r for r in old.get("files", [])}
manifest = {
    "snapshot_date_utc": "2026-10-03",
    "canonical_q99_engine": "q99_20261002_DEPLOYGAP/lat/pkg_v9fp",
    "newer_recommendation": "notes/REPLAY.md eng_ftfix (footprint own removal + bounded trade-through)",
    "original_files_are_reference_only": True,
    "public_redaction": "Source paths, host names and private account aliases anonymized; extracted fill functions unchanged.",
    "extracted_functions": sorted(NAMES),
    "files": [{"file": f, "source": p,
               "source_bytes": original.get(f, {}).get("source_bytes", original.get(f, {}).get("bytes", (HERE/f).stat().st_size)),
               "source_sha256": original.get(f, {}).get("source_sha256", original.get(f, {}).get("sha256", hashlib.sha256((HERE/f).read_bytes()).hexdigest())),
               "distributed_bytes": (HERE/f).stat().st_size,
               "distributed_sha256": hashlib.sha256((HERE/f).read_bytes()).hexdigest()} for f, p in sources.items()],
    "extracted_sha256": hashlib.sha256((ROOT / "src/bonebundle/maker/_reference.py").read_bytes()).hexdigest(),
}
(HERE / "SOURCE_MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
print(json.dumps(manifest, indent=2))
