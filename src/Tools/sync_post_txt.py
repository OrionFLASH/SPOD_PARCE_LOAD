# -*- coding: utf-8 -*-
"""
Сборка каталога POST/: снимок кода и (опционально) документации для переноса без Git.

Режимы:
  python src/Tools/sync_post_txt.py
      — полный снимок: корень + src (включая Tools, Tests) + Docs + README + requirements.
  python src/Tools/sync_post_txt.py --main-only
      — только основная программа: корневые *.py, config.json, src/**/*.py
        без каталогов src/Tests/ и src/Tools/; без Docs/README/requirements.
        В корень POST пишется КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt с построчной картой размещения.

Служебные файлы без .txt копируются из Docs/POST_SNAPSHOT/ в корень POST:
  restore_names_from_txt.bat

Каталог POST/ в .gitignore — не коммитится.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import date
from pathlib import Path
from typing import Iterable, List, Tuple

ROOT = Path(__file__).resolve().parents[2]
POST = ROOT / "POST"
HELPERS_SRC = ROOT / "Docs" / "POST_SNAPSHOT"
_DOCS_ROOT = ROOT / "Docs"
_POST_SNAPSHOT_UNDER_DOCS = _DOCS_ROOT / "POST_SNAPSHOT"
# Подкаталоги src/, не входящие в снимок «основной программы»
_SRC_SKIP_DIRS = frozenset({"Tests", "Tools"})


def iter_py_files(src: Path, *, main_only: bool) -> Iterable[Path]:
    """Все .py под src/, кроме __pycache__; в main_only — без Tests и Tools."""
    for p in src.rglob("*.py"):
        if "__pycache__" in p.parts:
            continue
        if main_only:
            rel = p.relative_to(src)
            if rel.parts and rel.parts[0] in _SRC_SKIP_DIRS:
                continue
        yield p


def iter_root_py_files(root: Path) -> Iterable[Path]:
    """Все .py в корне проекта (без скрытых файлов)."""
    for p in sorted(root.glob("*.py")):
        if p.name.startswith("."):
            continue
        yield p


def iter_docs_files(docs: Path) -> Iterable[Path]:
    """Файлы под Docs/, кроме __pycache__, скрытых и Docs/POST_SNAPSHOT/."""
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


def write_placement_map_main_only(copied: List[Tuple[Path, Path]]) -> None:
    """
    Текстовый файл: для каждого файла в POST — целевой путь в дереве проекта.
    copied: (rel_in_project, rel_in_post_with_txt)
    """
    lines: List[str] = [
        "=" * 78,
        "  КУДА ПОЛОЖИТЬ КАЖДЫЙ ФАЙЛ (основная программа, без тестов и Tools)",
        "=" * 78,
        "",
        "Снимок: только Python-модули пайплайна и config.json.",
        "Исключены: src/Tests/, src/Tools/, документация Docs/, README, requirements.",
        "",
        "На целевом ПК создайте корень проекта (например SPOD_PROM/) и для каждой",
        "строки ниже: снимите суффикс .txt с имени файла в POST и скопируйте",
        "в указанный каталог. Либо запустите restore_names_from_txt.bat в POST,",
        "затем перенесите дерево целиком в SPOD_PROM/.",
        "",
        "Формат:  файл в POST  →  каталог назначения в проекте",
        "-" * 78,
        "",
    ]
    copied_sorted = sorted(copied, key=lambda x: (str(x[0]).count("/"), str(x[0])))
    for rel_proj, rel_post in copied_sorted:
        target_dir = rel_proj.parent if str(rel_proj.parent) != "." else Path(".")
        if str(target_dir) == ".":
            dest = f"корень проекта/  ({rel_proj.name})"
        else:
            dest = f"{target_dir}/  ({rel_proj.name})"
        lines.append(f"  POST/{rel_post.as_posix()}")
        lines.append(f"      →  {dest}")
        lines.append("")

    lines.extend(
        [
            "-" * 78,
            "Структура каталогов на целевом ПК после переноса:",
            "",
            "  SPOD_PROM/",
            "  ├── main.py          (и прочие *.py из корня POST, если есть)",
            "  ├── config.json",
            "  └── src/",
            "      ├── __init__.py",
            "      ├── main_impl.py",
            "      └── … (остальные модули из POST/src/, кроме Tests и Tools)",
            "",
            "Дополнительно: IN/, OUT/, LOGS/ — по README.md; pip install -r requirements.txt",
            "(requirements.txt в этот снимок не входит — возьмите из репозитория при необходимости).",
            "",
            f"Дата формирования: {date.today().isoformat()}",
            "=" * 78,
        ]
    )
    out_path = POST / "КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt"
    out_path.write_text("\n".join(lines), encoding="utf-8")


def build_post(*, main_only: bool) -> None:
    if POST.is_dir():
        shutil.rmtree(POST)
    POST.mkdir(parents=True, exist_ok=True)

    if HELPERS_SRC.is_dir():
        for h in sorted(HELPERS_SRC.iterdir()):
            if h.is_file() and not h.name.startswith("."):
                if main_only and h.name == "КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt":
                    continue
                shutil.copy2(h, POST / h.name)
    elif not main_only:
        print(f"Предупреждение: нет каталога шаблонов {HELPERS_SRC}", file=sys.stderr)

    copied: List[Tuple[Path, Path]] = []
    n_code = 0

    for p in iter_root_py_files(ROOT):
        rel = p.relative_to(ROOT)
        copy_one_txt_suffix(p, rel)
        copied.append((rel, dest_with_txt(rel)))
        n_code += 1

    root_extras = ("config.json",) if main_only else ("config.json", "README.md", "requirements.txt")
    for name in root_extras:
        p = ROOT / name
        if not p.is_file():
            print(f"Пропуск (нет файла): {p}", file=sys.stderr)
            continue
        rel = Path(name)
        copy_one_txt_suffix(p, rel)
        copied.append((rel, dest_with_txt(rel)))
        n_code += 1

    src_root = ROOT / "src"
    for p in iter_py_files(src_root, main_only=main_only):
        rel = p.relative_to(ROOT)
        copy_one_txt_suffix(p, rel)
        copied.append((rel, dest_with_txt(rel)))
        n_code += 1

    n_docs = 0
    if not main_only:
        for p in iter_docs_files(_DOCS_ROOT):
            rel = p.relative_to(ROOT)
            copy_one_txt_suffix(p, rel)
            n_docs += 1

    if main_only:
        write_placement_map_main_only(copied)
    elif HELPERS_SRC.is_dir():
        helper = HELPERS_SRC / "КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt"
        if helper.is_file():
            shutil.copy2(helper, POST / helper.name)

    for garbage in POST.rglob(".DS_Store"):
        try:
            garbage.unlink()
        except OSError:
            pass

    mode = "main-only (без Tests/Tools/Docs)" if main_only else "полный"
    print(
        f"Готово [{mode}]. Файлов с .txt: {n_code}"
        + (f"; Docs: {n_docs}" if not main_only else "")
        + f". Каталог: {POST}",
        file=sys.stdout,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Сборка каталога POST/ для переноса без Git.")
    parser.add_argument(
        "--main-only",
        action="store_true",
        help="Только основная программа: .py + config.json, без Tests/Tools/Docs",
    )
    args = parser.parse_args()
    build_post(main_only=args.main_only)


if __name__ == "__main__":
    main()
