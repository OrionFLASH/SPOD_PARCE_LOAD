# -*- coding: utf-8 -*-
"""
Сборка каталога POST/: копии файлов программы с суффиксом .txt в имени (для переноса без Git).

Копируются только:
  - корень: main.py, requirements.txt, config.json, README.md (в POST — с суффиксом .txt);
  - модули основной программы: src/*.py и src/**/*.py, кроме каталогов src/Tools/ и src/Tests/.

Каталог Docs/ в POST не входит. Перед копированием из POST удаляется всё,
кроме служебных файлов: КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt, restore_names_from_txt.bat,
restore_names_from_txt.bat.txt.

Запуск из корня проекта: python src/Tools/sync_post_txt.py
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Iterable, List, Set

ROOT = Path(__file__).resolve().parents[2]
POST = ROOT / "POST"

# Файлы в корне POST, которые не трогаем при очистке (инструкции и скрипт Windows).
KEEP_IN_POST_ROOT: Set[str] = {
    "КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt",
    "restore_names_from_txt.bat",
    "restore_names_from_txt.bat.txt",
}

ROOT_FILES: List[str] = ["main.py", "requirements.txt", "config.json", "README.md"]


def iter_py_files(src: Path) -> Iterable[Path]:
    """Все .py под src/, кроме __pycache__, src/Tools/ и src/Tests/ (утилиты и тесты не для переноса)."""
    skip_dirs = frozenset({"Tools", "Tests"})
    for p in src.rglob("*.py"):
        if "__pycache__" in p.parts:
            continue
        rel_parts = p.relative_to(src).parts
        if rel_parts and rel_parts[0] in skip_dirs:
            continue
        yield p


def dest_with_txt(rel: Path) -> Path:
    """Путь внутри POST: к имени файла добавлен суффикс .txt."""
    return rel.parent / f"{rel.name}.txt"


def copy_one(src: Path, rel: Path) -> None:
    """Копирует файл в POST с именем rel + .txt."""
    out = POST / dest_with_txt(rel)
    out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, out)


def prune_post_keep_helpers() -> None:
    """Удаляет из POST старые копии (в т.ч. Docs/), оставляя только служебные файлы."""
    if not POST.is_dir():
        return
    for item in list(POST.iterdir()):
        if item.name in KEEP_IN_POST_ROOT:
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            try:
                item.unlink()
            except OSError as e:
                print(f"Не удалось удалить {item}: {e}", file=sys.stderr)


def main() -> None:
    POST.mkdir(parents=True, exist_ok=True)
    prune_post_keep_helpers()

    n = 0
    for name in ROOT_FILES:
        p = ROOT / name
        if not p.is_file():
            print(f"Пропуск (нет файла): {p}", file=sys.stderr)
            continue
        copy_one(p, Path(name))
        n += 1

    src_root = ROOT / "src"
    for p in iter_py_files(src_root):
        rel = p.relative_to(ROOT)
        copy_one(p, rel)
        n += 1

    for garbage in POST.rglob(".DS_Store"):
        try:
            garbage.unlink()
        except OSError:
            pass

    print(f"Готово. Скопировано файлов: {n}. Каталог: {POST}")


if __name__ == "__main__":
    main()
