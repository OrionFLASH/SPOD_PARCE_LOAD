# -*- coding: utf-8 -*-
"""Синхронизация catalog из web-edit → web-fill и встройка EMBEDDED_CATALOG."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC_FULL = ROOT / "common" / "web-edit-full" / "game_edit_catalog.json"
SRC_LEGACY = ROOT / "common" / "web-edit" / "game_edit_catalog.json"
SRC = SRC_FULL if SRC_FULL.exists() else SRC_LEGACY
DST_DIR = ROOT / "common" / "web-fill"
# Однофайловые fill-страницы с маркерами EMBEDDED_CATALOG
FILL_PAGES = (
    DST_DIR / "game_fill_settings.html",
)


def embed_into_html(path: Path, data: dict) -> bool:
    """Обновить блок EMBEDDED_CATALOG в HTML. True если файл обновлён."""
    if not path.exists():
        return False
    html = path.read_text(encoding="utf-8")
    payload = json.dumps(data, ensure_ascii=False)
    block = (
        "/* EMBEDDED_CATALOG_START */\n"
        f"const EMBEDDED_CATALOG = {payload};\n"
        "/* EMBEDDED_CATALOG_END */"
    )
    new_html, n = re.subn(
        r"/\* EMBEDDED_CATALOG_START \*/.*?/\* EMBEDDED_CATALOG_END \*/",
        # lambda: иначе re.sub разворачивает \n/\uXXXX из JSON в реальные символы
        lambda _m: block,
        html,
        count=1,
        flags=re.S,
    )
    if n != 1:
        print(
            f"Ошибка: маркеры EMBEDDED_CATALOG_* не найдены в {path.name}",
            file=sys.stderr,
        )
        raise SystemExit(2)
    path.write_text(new_html, encoding="utf-8")
    return True


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
    full_dir = ROOT / "common" / "web-fill-full"
    if full_dir.is_dir():
        (full_dir / "catalog.json").write_text(text, encoding="utf-8")
        (full_dir / "catalog.js").write_text(
            "/* зеркало catalog.json — sync_web_fill_catalog.py */\n"
            f"window.PARAM_REVIEW_CATALOG = {text.rstrip()};\n",
            encoding="utf-8",
        )
    review = ROOT / "common" / "param_catalog_review"
    if review.is_dir():
        (review / "catalog.js").write_text(
            "/* зеркало catalog.json */\n"
            f"window.PARAM_REVIEW_CATALOG = {text.rstrip()};\n"
            "window.SPOD_PARAM_CATALOG = window.PARAM_REVIEW_CATALOG;\n",
            encoding="utf-8",
        )
    embedded: list[str] = []
    for page in FILL_PAGES:
        if embed_into_html(page, data):
            embedded.append(page.name)
    n = sum(len(s.get("fields") or []) for s in data.get("sections") or [])
    emb = ", ".join(embedded) if embedded else "(нет HTML)"
    extra = " + web-fill-full catalog" if full_dir.is_dir() else ""
    print(f"OK: web-fill/catalog.json + catalog.js + EMBEDDED → {emb} ({n} полей){extra}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
