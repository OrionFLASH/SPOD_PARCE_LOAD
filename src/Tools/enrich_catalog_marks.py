# -*- coding: utf-8
"""Метки ``marks`` в каталоге: порядок ключей (после allow_empty), дефолт только если ключа нет."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MARKS: List[str] = ["ПКАП", "ФАБРИКА"]

FIELD_KEY_ORDER = [
    "n", "key", "status", "label", "description", "kind",
    "variants", "variant_labels", "default", "allow_empty",
    "marks", "json_required", "json_target", "note",
]


def normalize_marks(raw: Any, *, default_if_missing: bool) -> List[str]:
    if raw is None or not isinstance(raw, list):
        return DEFAULT_MARKS[:] if default_if_missing else []
    out: List[str] = []
    for item in raw:
        t = str(item or "").strip()
        if not t:
            continue
        upper = t.upper()
        if upper == "PKAP":
            t = "ПКАП"
        elif upper in ("FABRIKA", "FABRIC"):
            t = "ФАБРИКА"
        if t in ("ПКАП", "ФАБРИКА") and t not in out:
            out.append(t)
    return out


def order_field(field: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in FIELD_KEY_ORDER:
        if key in field:
            out[key] = field[key]
    for key, val in field.items():
        if key not in out:
            out[key] = val
    return out


def enrich_catalog(data: Dict[str, Any]) -> int:
    changed = 0
    all_marks: set[str] = set()
    for sec in data.get("sections") or []:
        new_fields: List[Dict[str, Any]] = []
        for field in sec.get("fields") or []:
            if not isinstance(field, dict):
                new_fields.append(field)
                continue
            had_key = "marks" in field
            before = field.get("marks")
            tags = normalize_marks(before, default_if_missing=not had_key)
            ordered = order_field({**field, "marks": tags})
            if before != tags or list(field.keys()) != list(ordered.keys()):
                changed += 1
            all_marks.update(tags)
            new_fields.append(ordered)
        sec["fields"] = new_fields
    data["marksManifest"] = [m for m in DEFAULT_MARKS if m in all_marks] or DEFAULT_MARKS[:]
    return changed


def main(argv: Optional[List[str]] = None) -> int:
    args = argv if argv is not None else sys.argv
    paths = [Path(p) for p in args[1:]] if len(args) > 1 else [
        ROOT / "common" / "web-edit" / "game_edit_catalog.json",
        ROOT / "common" / "web-edit-full" / "game_edit_catalog.json",
        ROOT / "common" / "web-fill" / "catalog.json",
        ROOT / "common" / "web-fill-full" / "catalog.json",
        ROOT / "common" / "param_catalog_review" / "catalog.json",
    ]
    for path in paths:
        if not path.is_file():
            print(f"skip: {path}", file=sys.stderr)
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        n = enrich_catalog(payload)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        fields = sum(len(s.get("fields") or []) for s in payload.get("sections") or [])
        print(f"OK: {path.relative_to(ROOT)} — полей {fields}, обновлено {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
