# -*- coding: utf-8 -*-
"""
Сборка каталога POST/: снимок кода и документации для переноса без Git.

Полностью очищает POST/, затем копирует:
  - все *.py из корня проекта + config.json, README.md, requirements.txt — в корень POST
    с суффиксом .txt к имени файла;
  - все src/**/*.py (включая Tools и Tests) — POST/src/.../модуль.py.txt;
  - всё дерево Docs/**, кроме подкаталога Docs/POST_SNAPSHOT/ (шаблоны дублируются в корень POST отдельно),
    с сохранением структуры и суффиксом .txt (например Docs/CONSISTENCY.md → POST/Docs/CONSISTENCY.md.txt).

Служебные файлы без доп. суффикса копируются из Docs/POST_SNAPSHOT/ в корень POST:
  КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt, restore_names_from_txt.bat

Каталог POST/ в .gitignore — не коммитится.

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
# Все модули под src/ копируются в POST (включая Tools и Tests) для полного переноса.
# Шаблоны из Docs/POST_SNAPSHOT кладутся в корень POST как есть; дерево Docs копируем без этого подкаталога
_DOCS_ROOT = ROOT / "Docs"
_POST_SNAPSHOT_UNDER_DOCS = _DOCS_ROOT / "POST_SNAPSHOT"


def iter_py_files(src: Path) -> Iterable[Path]:
    """Все .py под src/, кроме __pycache__."""
    for p in src.rglob("*.py"):
        if "__pycache__" in p.parts:
            continue
        yield p


def iter_root_py_files(root: Path) -> Iterable[Path]:
    """Все .py в корне проекта (без скрытых файлов)."""
    for p in sorted(root.glob("*.py")):
        if p.name.startswith("."):
            continue
        yield p


def iter_docs_files(docs: Path) -> Iterable[Path]:
    """Файлы под Docs/, кроме __pycache__, скрытых и всего под Docs/POST_SNAPSHOT/."""
    if not docs.is_dir():
        return
    for p in docs.rglob("*"):
        if not p.is_file():
            continue
        if "__pycache__" in p.parts or p.name.startswith("."):
            continue
        try:
            p.relative_to(_POST_SNAPSHOT_UNDER_DOCS)
            continue
        except ValueError:
            pass
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

    n_code = 0
    for p in iter_root_py_files(ROOT):
        rel = p.relative_to(ROOT)
        copy_one_txt_suffix(p, rel)
        n_code += 1

    for name in ("config.json", "README.md", "requirements.txt"):
        p = ROOT / name
        if not p.is_file():
            print(f"Пропуск (нет файла): {p}", file=sys.stderr)
            continue
        copy_one_txt_suffix(p, Path(name))
        n_code += 1

    src_root = ROOT / "src"
    for p in iter_py_files(src_root):
        rel = p.relative_to(ROOT)
        copy_one_txt_suffix(p, rel)
        n_code += 1

    n_docs = 0
    for p in iter_docs_files(_DOCS_ROOT):
        rel = p.relative_to(ROOT)
        copy_one_txt_suffix(p, rel)
        n_docs += 1

    for garbage in POST.rglob(".DS_Store"):
        try:
            garbage.unlink()
        except OSError:
            pass

    print(
        f"Готово. Код и конфиг (.txt): {n_code}; документация Docs/ (.txt): {n_docs}. "
        f"Каталог: {POST}",
        file=sys.stdout,
    )


if __name__ == "__main__":
    main()
