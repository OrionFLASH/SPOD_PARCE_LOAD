# -*- coding: utf-8 -*-
"""
Сборка каталога POST/: копии файлов проекта с суффиксом .txt в имени (для переноса без Git).

Копируются:
  - корень: main.py, requirements.txt, config.json, README.md;
  - все src/**/*.py (включая Tools, Tests);
  - вся документация каталога Docs/ (включая Docs/JSON и примеры JSON).

Запуск из корня проекта: python src/Tools/sync_post_txt.py
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Iterable, List

ROOT = Path(__file__).resolve().parents[2]
POST = ROOT / "POST"

ROOT_FILES: List[str] = ["main.py", "requirements.txt", "config.json", "README.md"]


def iter_py_files(src: Path) -> Iterable[Path]:
    """Все .py под src/, кроме __pycache__."""
    for p in src.rglob("*.py"):
        if "__pycache__" in p.parts:
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


def main() -> None:
    POST.mkdir(parents=True, exist_ok=True)
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

    docs = ROOT / "Docs"
    if docs.is_dir():
        for p in docs.rglob("*"):
            if p.is_dir():
                continue
            rel = p.relative_to(ROOT)
            copy_one(p, rel)
            n += 1
    else:
        print("Предупреждение: каталог Docs/ не найден.", file=sys.stderr)

    # Устаревшие дубликаты: раньше часть документации клали в корень POST — теперь только POST/Docs/.
    dup_analysis = POST / "АНАЛИЗ_ПРОВЕРОК_КОНСИСТЕНТНОСТИ.md.txt"
    if dup_analysis.is_file() and (POST / "Docs" / "АНАЛИЗ_ПРОВЕРОК_КОНСИСТЕНТНОСТИ.md.txt").is_file():
        dup_analysis.unlink()

    for garbage in POST.rglob(".DS_Store"):
        try:
            garbage.unlink()
        except OSError:
            pass

    print(f"Готово. Скопировано файлов: {n}. Каталог: {POST}")


if __name__ == "__main__":
    main()
