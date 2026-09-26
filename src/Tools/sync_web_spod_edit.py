#!/usr/bin/env python3
"""
Синхронизация объединённой страницы common/web-SPOD-Edit с исходными программами.

Программы переносятся в web-SPOD-Edit «как есть»:
  common/web-report/      → common/web-SPOD-Edit/report/
  common/web-fill-full/   → common/web-SPOD-Edit/fill/
после копирования в HTML заново подключается общий трейс-лог (../spod_trace.js),
а в fill — ещё хуки fill_trace_hooks.js. Логика и интерфейс программ не меняются.

Запуск из корня репозитория:
    python3 src/Tools/sync_web_spod_edit.py          # скопировать и подключить лог
    python3 src/Tools/sync_web_spod_edit.py --check  # только проверить, что копии совпадают
"""

from __future__ import annotations

import argparse
import filecmp
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMMON = ROOT / "common"
TARGET = COMMON / "web-SPOD-Edit"

REPORT_SRC = COMMON / "web-report"
FILL_SRC = COMMON / "web-fill-full"

REPORT_FILES = [
    "report_app.html",
    "report_app.js",
    "report_core.js",
    "report_io.js",
    "report_styles.css",
    "xlsx.full.min.js",
]
FILL_FILES = [
    "game_fill_settings.html",
    "game_fill_styles.css",
    "game_fill_core.js",
    "game_fill_model.js",
    "game_fill_filters.js",
    "game_fill_ui.js",
    "game_fill_io.js",
    "game_fill_boot.js",
    "catalog.js",
    "catalog.json",
]

REPORT_TRACE_OLD = '<script src="report_trace.js"></script>'
REPORT_TRACE_NEW = '<script src="../spod_trace.js" data-area="report"></script>'
FILL_CATALOG_TAG = '<script src="catalog.js"></script>'
FILL_TRACE_TAG = '<script src="../spod_trace.js" data-area="fill"></script>'
FILL_BOOT_TAG = '<script src="game_fill_boot.js"></script>'
FILL_HOOKS_TAG = '<script src="fill_trace_hooks.js"></script>'


def patch_report_html(text: str) -> str:
    """web-report: собственный report_trace.js → общий ../spod_trace.js (область report)."""
    if REPORT_TRACE_NEW in text:
        return text
    if REPORT_TRACE_OLD not in text:
        raise SystemExit("report_app.html: не найден тег report_trace.js — проверьте разметку")
    return text.replace(REPORT_TRACE_OLD, REPORT_TRACE_NEW, 1)


def patch_fill_html(text: str) -> str:
    """web-fill-full: общий трейс-лог первым скриптом и хуки перед game_fill_boot.js."""
    if FILL_TRACE_TAG not in text:
        if FILL_CATALOG_TAG not in text:
            raise SystemExit("game_fill_settings.html: не найден тег catalog.js")
        text = text.replace(FILL_CATALOG_TAG, FILL_TRACE_TAG + "\n" + FILL_CATALOG_TAG, 1)
    if FILL_HOOKS_TAG not in text:
        if FILL_BOOT_TAG not in text:
            raise SystemExit("game_fill_settings.html: не найден тег game_fill_boot.js")
        text = text.replace(FILL_BOOT_TAG, FILL_HOOKS_TAG + "\n" + FILL_BOOT_TAG, 1)
    return text


def expected_text(src: Path, name: str) -> str | None:
    """Ожидаемое содержимое HTML в web-SPOD-Edit (после подключения лога); None — копия байт в байт."""
    if name == "report_app.html":
        return patch_report_html(src.read_text(encoding="utf-8"))
    if name == "game_fill_settings.html":
        return patch_fill_html(src.read_text(encoding="utf-8"))
    return None


def sync_group(src_dir: Path, dst_dir: Path, names: list[str], check: bool) -> list[str]:
    diffs: list[str] = []
    dst_dir.mkdir(parents=True, exist_ok=True)
    for name in names:
        src = src_dir / name
        dst = dst_dir / name
        if not src.exists():
            raise SystemExit(f"нет исходного файла: {src}")
        text = expected_text(src, name)
        if check:
            if not dst.exists():
                diffs.append(f"нет файла: {dst.relative_to(ROOT)}")
            elif text is not None:
                if dst.read_text(encoding="utf-8") != text:
                    diffs.append(f"отличается: {dst.relative_to(ROOT)}")
            elif not filecmp.cmp(src, dst, shallow=False):
                diffs.append(f"отличается: {dst.relative_to(ROOT)}")
            continue
        if text is not None:
            dst.write_text(text, encoding="utf-8")
        else:
            shutil.copy2(src, dst)
    return diffs


def sync_examples(check: bool) -> list[str]:
    diffs: list[str] = []
    src_dir = REPORT_SRC / "examples"
    dst_dir = TARGET / "report" / "examples"
    dst_dir.mkdir(parents=True, exist_ok=True)
    for src in sorted(src_dir.iterdir()):
        if not src.is_file():
            continue
        dst = dst_dir / src.name
        if check:
            if not dst.exists() or not filecmp.cmp(src, dst, shallow=False):
                diffs.append(f"отличается: {dst.relative_to(ROOT)}")
            continue
        shutil.copy2(src, dst)
    return diffs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="только проверить совпадение копий, ничего не писать")
    args = parser.parse_args()

    diffs = []
    diffs += sync_group(REPORT_SRC, TARGET / "report", REPORT_FILES, args.check)
    diffs += sync_examples(args.check)
    diffs += sync_group(FILL_SRC, TARGET / "fill", FILL_FILES, args.check)

    if args.check:
        if diffs:
            print("web-SPOD-Edit отстаёт от исходных программ:")
            for d in diffs:
                print("  -", d)
            print("Запустите: python3 src/Tools/sync_web_spod_edit.py")
            return 1
        print("web-SPOD-Edit совпадает с web-report и web-fill-full")
        return 0
    print("web-SPOD-Edit обновлён из web-report и web-fill-full (трейс-лог подключён)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
