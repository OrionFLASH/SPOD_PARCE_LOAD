# -*- coding: utf-8 -*-
"""
Сборка POST/<имя_проекта>/ — снимок WEB (edit/fill), примеров JSON и утилит экспорта.

  python src/Tools/sync_post_web_bundle.py

Каталог POST/SPOD_PROM/ перед сборкой полностью удаляется и создаётся заново.
Имена файлов и папок не меняются (без .txt).
"""

from __future__ import annotations

import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

ROOT = Path(__file__).resolve().parents[2]
POST = ROOT / "POST"
PROJECT_NAME = ROOT.name  # SPOD_PROM

COMMON_DIRS = (
    "web-edit",
    "web-edit-full",
    "web-fill",
    "web-fill-full",
    "param_catalog_review",
    "examples",
)

DOCS_PATHS = (
    "PLAN_WEB_FILL.md",
    "TODO_WEB_FILL.md",
    "PLAN_WEB_FILL_FULL.md",
    "TODO_WEB_FILL_FULL.md",
    "CONTEST_BADGE_FORM_PARAM_REVIEW.md",
    "CONTEST_BADGE_FORM_FILLING.md",
    "JSON/README.md",
    "param_review_editor/README.md",
)

TOOLS_WEB = (
    "export_web_fill_examples_from_spod.py",
    "sync_web_fill_catalog.py",
    "sync_web_fill_singlefile.py",
    "build_web_edit_full_catalog.py",
    "build_param_review_editor.py",
    "patch_web_fill_catalog_lists.py",
    "enrich_catalog_marks.py",
    "sync_post_web_bundle.py",
)

TESTS_WEB = (
    "test_web_fill_catalog.py",
    "test_web_fill_code_ending.py",
    "test_web_fill_csv_json.py",
)

CONFIG_FILES = (
    "CONFIG_RUN_INPUT.json",
)

# Не копировать: сохранения UI fill (не примеры)
WEB_FILL_SKIP_GLOBS = (
    "spod_fill_2*.json",
)


def _skip_path(path: Path) -> bool:
    name = path.name
    if name.startswith(".") or name == "__pycache__":
        return True
    if path.suffix == ".pyc":
        return True
    for pat in WEB_FILL_SKIP_GLOBS:
        if path.match(pat):
            return True
    return False


def _copy_tree(src: Path, dst: Path) -> int:
    """Рекурсивное копирование; возвращает число файлов."""
    n = 0
    if not src.is_dir():
        return 0
    for item in sorted(src.rglob("*")):
        if not item.is_file() or _skip_path(item):
            continue
        rel = item.relative_to(src)
        if any(p.startswith(".") for p in rel.parts):
            continue
        target = dst / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target)
        n += 1
    return n


def _copy_files(src_dir: Path, names: Iterable[str], dst_dir: Path) -> int:
    n = 0
    dst_dir.mkdir(parents=True, exist_ok=True)
    for name in names:
        src = src_dir / name
        if not src.is_file():
            print(f"  skip (нет): {src.relative_to(ROOT)}", file=sys.stderr)
            continue
        shutil.copy2(src, dst_dir / name)
        n += 1
    return n


def write_compose_txt(bundle: Path, stats: dict[str, int]) -> None:
    lines = [
        f"WEB-снимок проекта {PROJECT_NAME}",
        f"Собран: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"Скрипт: src/Tools/sync_post_web_bundle.py",
        "",
        "Состав:",
        "  common/web-edit, web-edit-full — редактор описания параметров",
        "  common/web-fill, web-fill-full — заполнение SPOD (HTML + catalog)",
        "  common/param_catalog_review — каталог для blank/review",
        "  common/examples/web-fill — примеры JSON для импорта",
        "  Docs/ — документация WEB (PLAN_WEB_FILL, PLAN_WEB_FILL_FULL, TODO, param_review)",
        "  src/Tools/ — экспорт и синхронизация JSON/каталогов",
        "  src/Tests/ — тесты web-fill",
        "  config/CONFIG_RUN_INPUT.json — вход для export_web_fill_examples_from_spod.py",
        "",
        "Запуск fill-full локально:",
        "  cd common/web-fill-full && python3 -m http.server 8766",
        "",
        "Пересборка примеров JSON:",
        "  python3 src/Tools/export_web_fill_examples_from_spod.py",
        "",
        "Синхронизация каталога fill из edit:",
        "  python3 src/Tools/sync_web_fill_catalog.py",
        "",
        f"Файлов скопировано: {sum(stats.values())} (common={stats.get('common', 0)}, "
        f"Docs={stats.get('docs', 0)}, Tools={stats.get('tools', 0)}, "
        f"Tests={stats.get('tests', 0)}, config={stats.get('config', 0)})",
        "",
    ]
    (bundle / "СОСТАВ_ПАКЕТА.txt").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    bundle = POST / PROJECT_NAME
    if bundle.exists():
        shutil.rmtree(bundle)
    bundle.mkdir(parents=True, exist_ok=True)
    POST.mkdir(parents=True, exist_ok=True)

    stats: dict[str, int] = {}

    common_dst = bundle / "common"
    for sub in COMMON_DIRS:
        src = ROOT / "common" / sub
        if not src.is_dir():
            print(f"skip common/{sub} (нет каталога)", file=sys.stderr)
            continue
        n = _copy_tree(src, common_dst / sub)
        stats["common"] = stats.get("common", 0) + n

    docs_dst = bundle / "Docs"
    stats["docs"] = 0
    for rel in DOCS_PATHS:
        src = ROOT / "Docs" / rel
        if not src.is_file():
            print(f"  skip Docs/{rel}", file=sys.stderr)
            continue
        dst = docs_dst / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        stats["docs"] += 1

    stats["tools"] = _copy_files(ROOT / "src" / "Tools", TOOLS_WEB, bundle / "src" / "Tools")
    stats["tests"] = _copy_files(ROOT / "src" / "Tests", TESTS_WEB, bundle / "src" / "Tests")
    stats["config"] = _copy_files(ROOT / "config", CONFIG_FILES, bundle / "config")

    write_compose_txt(bundle, stats)

    readme = POST / "README_POST.txt"
    readme.write_text(
        f"WEB-снимок: POST/{PROJECT_NAME}/\n"
        f"Состав — POST/{PROJECT_NAME}/СОСТАВ_ПАКЕТА.txt\n"
        f"Обновление: python3 src/Tools/sync_post_web_bundle.py\n",
        encoding="utf-8",
    )

    total = sum(stats.values())
    print(f"OK: POST/{PROJECT_NAME}/ — {total} файлов")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
