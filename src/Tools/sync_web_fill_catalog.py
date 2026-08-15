# -*- coding: utf-8 -*-
"""Синхронизация catalog из web-edit → web-fill и встройка в index.html."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "common" / "web-edit" / "catalog.json"
DST_DIR = ROOT / "common" / "web-fill"
INDEX = DST_DIR / "index.html"


def embed_into_index(data: dict) -> None:
    """Обновить блок EMBEDDED_CATALOG в однофайловом fill."""
    if not INDEX.exists():
        print(f"Пропуск встройки: нет {INDEX}", file=sys.stderr)
        return
    html = INDEX.read_text(encoding="utf-8")
    payload = json.dumps(data, ensure_ascii=False)
    block = (
        "/* EMBEDDED_CATALOG_START */\n"
        f"const EMBEDDED_CATALOG = {payload};\n"
        "/* EMBEDDED_CATALOG_END */"
    )
    new_html, n = re.subn(
        r"/\* EMBEDDED_CATALOG_START \*/.*?/\* EMBEDDED_CATALOG_END \*/",
        block,
        html,
        count=1,
        flags=re.S,
    )
    if n != 1:
        print("Ошибка: маркеры EMBEDDED_CATALOG_* не найдены в index.html", file=sys.stderr)
        raise SystemExit(2)
    INDEX.write_text(new_html, encoding="utf-8")


def main() -> int:
    if not SRC.exists():
        print(f"Нет файла: {SRC}", file=sys.stderr)
        return 1
    DST_DIR.mkdir(parents=True, exist_ok=True)
    data = json.loads(SRC.read_text(encoding="utf-8"))
    text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    (DST_DIR / "catalog.json").write_text(text, encoding="utf-8")
    (DST_DIR / "catalog.js").write_text(
        "/* зеркало catalog.json — sync_web_fill_catalog.py */\n"
        f"window.PARAM_REVIEW_CATALOG = {text.rstrip()};\n",
        encoding="utf-8",
    )
    embed_into_index(data)
    n = sum(len(s.get("fields") or []) for s in data.get("sections") or [])
    print(f"OK: web-fill/catalog.json + catalog.js + index.html EMBEDDED ({n} полей)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
