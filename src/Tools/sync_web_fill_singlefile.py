# -*- coding: utf-8 -*-
"""Собрать однофайловый web-fill из fill-full (CSS + разметка + JS).

Каталог EMBEDDED_CATALOG берётся из текущего HTML (его обновляет
sync_web_fill_catalog.py). Ключ localStorage остаётся spod_web_fill_project_v2.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FULL_DIR = ROOT / "common" / "web-fill-full"
FILL_HTML = ROOT / "common" / "web-fill" / "game_fill_settings.html"
JS_PARTS = (
    "game_fill_core.js",
    "game_fill_model.js",
    "game_fill_filters.js",
    "game_fill_ui.js",
    "game_fill_io.js",
    "game_fill_boot.js",
)
LS_FULL = 'var LS_PROJECT = "spod_web_fill_full_project_v2";'
LS_SINGLE = 'var LS_PROJECT = "spod_web_fill_project_v2";'
TITLE = "Заполнение SPOD — Герои продаж (Геймификация)"


def extract_embedded_catalog(html: str) -> str:
    m = re.search(
        r"/\* EMBEDDED_CATALOG_START \*/.*?/\* EMBEDDED_CATALOG_END \*/",
        html,
        flags=re.S,
    )
    if not m:
        raise SystemExit("Нет маркеров EMBEDDED_CATALOG в однофайловом fill")
    return m.group(0)


def extract_full_body_markup(html: str) -> str:
    m = re.search(r"<body>\s*(.*?)\s*<script\s", html, flags=re.S | re.I)
    if not m:
        raise SystemExit("Не удалось вырезать разметку из fill-full HTML")
    return m.group(1).rstrip() + "\n"


def js_for_singlefile(js: str, embedded: str) -> str:
    idx = js.find("var BLOCK")
    if idx < 0:
        idx = js.find("const BLOCK")
    if idx < 0:
        raise SystemExit("В fill-full JS нет BLOCK")
    js = js[idx:].replace(LS_FULL, LS_SINGLE, 1)
    js = js.replace(
        'const LS_PROJECT = "spod_web_fill_full_project_v2";',
        LS_SINGLE,
        1,
    )
    if LS_SINGLE not in js:
        raise SystemExit("Не удалось выставить LS_PROJECT однофайлового fill")
    return (
        "\n/* === Встроенный каталог (править здесь или через dual-edit HTML) === */\n"
        f"{embedded}\n\n"
        f"{js}"
    )


def build_html(css: str, body: str, script: str) -> str:
    return (
        "<!DOCTYPE html>\n"
        '<html lang="ru">\n'
        "<head>\n"
        '<meta charset="UTF-8" />\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1" />\n'
        f"<title>{TITLE}</title>\n"
        "<style>\n"
        f"{css.rstrip()}\n"
        "</style>\n"
        "</head>\n"
        "<body>\n"
        f"{body.rstrip()}\n"
        "<script>\n"
        f"{script.rstrip()}\n"
        "</script>\n"
        "</body>\n"
        "</html>\n"
    )


def main() -> int:
    css = (FULL_DIR / "game_fill_styles.css").read_text(encoding="utf-8")
    full_html = (FULL_DIR / "game_fill_settings.html").read_text(encoding="utf-8")
    parts: list[str] = []
    for name in JS_PARTS:
        path = FULL_DIR / name
        if not path.is_file():
            raise SystemExit(f"Нет {path.relative_to(ROOT)}")
        parts.append(path.read_text(encoding="utf-8"))
    js = "\n".join(parts)
    old = FILL_HTML.read_text(encoding="utf-8")
    embedded = extract_embedded_catalog(old)
    body = extract_full_body_markup(full_html)
    script = js_for_singlefile(js, embedded)
    FILL_HTML.write_text(build_html(css, body, script), encoding="utf-8")
    print(f"Собран {FILL_HTML.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
