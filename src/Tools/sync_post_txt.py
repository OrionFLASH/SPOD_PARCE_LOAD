# -*- coding: utf-8 -*-
"""
Сборка каталога POST/: снимок кода для переноса без Git.

Полностью очищает POST/, затем копирует:
  - main.py, config.json — в корень POST с именами main.py.txt, config.json.txt;
  - все src/**/*.py, кроме каталогов src/Tools/ и src/Tests/ — с суффиксом .txt к имени файла
    (например src/main_impl.py → POST/src/main_impl.py.txt).

Служебные файлы без двойного .txt копируются из Docs/POST_SNAPSHOT/:
  КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt, restore_names_from_txt.bat

README.md, requirements.txt и каталог Docs/ в POST не попадают.

Каталог POST/ указан в .gitignore и не должен коммититься в репозиторий.

Запуск из корня проекта: python src/Tools/sync_post_txt.py
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[2]
POST = ROOT / "POST"
# Шаблоны инструкции и bat для копирования в POST как есть (отслеживаются в Git под Docs/)
HELPERS_SRC = ROOT / "Docs" / "POST_SNAPSHOT"
_SKIP_SRC_SUBDIRS = frozenset({"Tools", "Tests"})


def iter_py_files(src: Path) -> Iterable[Path]:
    """Все .py под src/, кроме __pycache__, src/Tools/ и src/Tests/."""
    for p in src.rglob("*.py"):
        if "__pycache__" in p.parts:
            continue
        rel_parts = p.relative_to(src).parts
        if rel_parts and rel_parts[0] in _SKIP_SRC_SUBDIRS:
            continue
        yield p


def dest_with_txt(rel: Path) -> Path:
    """Путь внутри POST: к имени файла добавлен суффикс .txt."""
    return rel.parent / f"{rel.name}.txt"


def copy_one_txt_suffix(src: Path, rel: Path) -> None:
    """Копирует файл в POST с именем «как в проекте + .txt»."""
    out = POST / dest_with_txt(rel)
    out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, out)


def main() -> None:
    if POST.is_dir():
        shutil.rmtree(POST)
    POST.mkdir(parents=True, exist_ok=True)

    if HELPERS_SRC.is_dir():
        for h in sorted(HELPERS_SRC.iterdir()):
            if h.is_file() and not h.name.startswith("."):
                shutil.copy2(h, POST / h.name)
    else:
        print(f"Предупреждение: нет каталога шаблонов {HELPERS_SRC}", file=sys.stderr)

    n = 0
    for name in ("main.py", "config.json"):
        p = ROOT / name
        if not p.is_file():
            print(f"Пропуск (нет файла): {p}", file=sys.stderr)
            continue
        copy_one_txt_suffix(p, Path(name))
        n += 1

    src_root = ROOT / "src"
    for p in iter_py_files(src_root):
        rel = p.relative_to(ROOT)
        copy_one_txt_suffix(p, rel)
        n += 1

    for garbage in POST.rglob(".DS_Store"):
        try:
            garbage.unlink()
        except OSError:
            pass

    print(f"Готово. Скопировано файлов кода/конфига с суффиксом .txt: {n}. Каталог: {POST}")


if __name__ == "__main__":
    main()
