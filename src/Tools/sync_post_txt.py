# -*- coding: utf-8 -*-
"""
Сборка каталога POST/: снимок кода и (опционально) документации для переноса без Git.

Режимы:
  python src/Tools/sync_post_txt.py
      — полный снимок: корень + src (включая Tools, Tests) + Docs + README + requirements.
  python src/Tools/sync_post_txt.py --program-only
      — все *.py (корень + src, включая Tests и Tools) и config.json;
        в корень POST — КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt с поимённой картой каталогов.
  python src/Tools/sync_post_txt.py --main-only --changed-only
      — в POST копируются только файлы, изменившиеся с прошлой синхронизации
        (сравнение SHA-256; манифест POST/.sync_manifest.json). POST не очищается.
        Дополнительно: ОБНОВЛЁННЫЕ_ФАЙЛЫ.txt — карта только для обновлённых файлов.

Служебные файлы без .txt копируются из Docs/POST_SNAPSHOT/ в корень POST:
  restore_names_from_txt.bat

Каталог POST/ в .gitignore — не коммитится.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
POST = ROOT / "POST"
MANIFEST_PATH = POST / ".sync_manifest.json"
HELPERS_SRC = ROOT / "Docs" / "POST_SNAPSHOT"
_DOCS_ROOT = ROOT / "Docs"
_POST_SNAPSHOT_UNDER_DOCS = _DOCS_ROOT / "POST_SNAPSHOT"
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


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def copy_one_txt_suffix(src: Path, rel: Path) -> Path:
    """Копирует файл в POST с именем «как в проекте + .txt»; возвращает путь назначения."""
    out = POST / dest_with_txt(rel)
    out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, out)
    return out


def iter_main_only_sources() -> Iterable[Tuple[Path, Path]]:
    """(абсолютный_путь, rel от ROOT) для снимка main-only."""
    for p in iter_root_py_files(ROOT):
        yield p, p.relative_to(ROOT)
    for name in ("config.json",):
        p = ROOT / name
        if p.is_file():
            yield p, Path(name)
    src_root = ROOT / "src"
    for p in iter_py_files(src_root, main_only=True):
        yield p, p.relative_to(ROOT)


def load_manifest() -> Dict[str, Any]:
    if not MANIFEST_PATH.is_file():
        return {}
    try:
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_manifest(files: Dict[str, str], *, mode: str) -> None:
    POST.mkdir(parents=True, exist_ok=True)
    data = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "files": files,
    }
    MANIFEST_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_placement_map(
    copied: List[Tuple[Path, Path]],
    *,
    out_name: str,
    title: str,
    intro: List[str],
) -> None:
    """Текстовая карта размещения для списка скопированных файлов."""
    lines: List[str] = [
        "=" * 78,
        f"  {title}",
        "=" * 78,
        "",
        *intro,
        "",
        "Формат:  файл в POST  →  каталог назначения в проекте",
        "-" * 78,
        "",
    ]
    if not copied:
        lines.append("  (нет изменённых файлов с прошлой синхронизации)")
        lines.append("")
    for rel_proj, rel_post in sorted(copied, key=lambda x: (str(x[0]).count("/"), str(x[0]))):
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
            f"Дата: {date.today().isoformat()}",
            "=" * 78,
        ]
    )
    (POST / out_name).write_text("\n".join(lines), encoding="utf-8")


def write_placement_map_main_only(copied: List[Tuple[Path, Path]]) -> None:
    write_placement_map(
        copied,
        out_name="КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt",
        title="КУДА ПОЛОЖИТЬ КАЖДЫЙ ФАЙЛ (основная программа, без тестов и Tools)",
        intro=[
            "Снимок: только Python-модули пайплайна и config.json.",
            "Исключены: src/Tests/, src/Tools/, документация Docs/, README, requirements.",
            "",
            "На целевом ПК: снимите суффикс .txt или запустите restore_names_from_txt.bat.",
        ],
    )


def write_placement_map_changed_only(copied: List[Tuple[Path, Path]]) -> None:
    write_placement_map(
        copied,
        out_name="ОБНОВЛЁННЫЕ_ФАЙЛЫ.txt",
        title="ОБНОВЛЁННЫЕ ФАЙЛЫ (только изменения с прошлой синхронизации POST)",
        intro=[
            "Скопированы только файлы, у которых изменилось содержимое (SHA-256).",
            "Остальные файлы в POST не трогались.",
            "",
            "Перенесите на целевой ПК только перечисленные ниже пути.",
        ],
    )


def copy_helpers(*, main_only: bool, force_helpers: bool) -> None:
    if not HELPERS_SRC.is_dir():
        if not main_only:
            print(f"Предупреждение: нет каталога шаблонов {HELPERS_SRC}", file=sys.stderr)
        return
    for h in sorted(HELPERS_SRC.iterdir()):
        if not h.is_file() or h.name.startswith("."):
            continue
        if main_only and h.name == "КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt":
            continue
        dest = POST / h.name
        if force_helpers or not dest.exists() or file_sha256(h) != file_sha256(dest):
            shutil.copy2(h, dest)


def iter_program_sources() -> Iterable[Tuple[Path, Path]]:
    """Все .py проекта + config.json (корень и src, включая Tests/Tools)."""
    for p in iter_root_py_files(ROOT):
        yield p, p.relative_to(ROOT)
    cfg = ROOT / "config.json"
    if cfg.is_file():
        yield cfg, Path("config.json")
    src_root = ROOT / "src"
    for p in iter_py_files(src_root, main_only=False):
        yield p, p.relative_to(ROOT)


def write_placement_map_program(copied: List[Tuple[Path, Path]]) -> None:
    write_placement_map(
        copied,
        out_name="КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt",
        title="КУДА ПОЛОЖИТЬ КАЖДЫЙ ФАЙЛ (все .py и config.json)",
        intro=[
            "Снимок: все Python-файлы (корень, src/, включая Tests и Tools) и config.json.",
            "Без документации Docs/, README и requirements.",
            "",
            "На целевом ПК: снимите суффикс .txt с каждого файла или запустите restore_names_from_txt.bat.",
        ],
    )


def prune_obsolete_post_files(prev_keys: Dict[str, str], new_keys: set[str]) -> int:
    """Удалить из POST файлы, которых больше нет в текущем снимке (по ключам манифеста)."""
    removed = 0
    for key in set(prev_keys) - new_keys:
        target = POST / dest_with_txt(Path(key))
        if target.is_file():
            target.unlink()
            removed += 1
            parent = target.parent
            while parent != POST and parent.is_dir() and not any(parent.iterdir()):
                parent.rmdir()
                parent = parent.parent
    return removed


def build_post_program_only() -> None:
    """Снимок: все .py + config.json и карта размещения (без полного удаления POST/)."""
    POST.mkdir(parents=True, exist_ok=True)
    prev_manifest = load_manifest()
    prev_hashes: Dict[str, str] = prev_manifest.get("files") or {}

    copy_helpers(main_only=True, force_helpers=False)

    copied: List[Tuple[Path, Path]] = []
    all_hashes: Dict[str, str] = {}
    new_keys: set[str] = set()

    for src_path, rel in iter_program_sources():
        rel_key = rel.as_posix()
        new_keys.add(rel_key)
        copy_one_txt_suffix(src_path, rel)
        copied.append((rel, dest_with_txt(rel)))
        all_hashes[rel_key] = file_sha256(src_path)

    n_pruned = prune_obsolete_post_files(prev_hashes, new_keys)
    write_placement_map_program(copied)
    save_manifest(all_hashes, mode="program-only")

    for garbage in POST.rglob(".DS_Store"):
        try:
            garbage.unlink()
        except OSError:
            pass

    print(
        f"Готово [program-only]. Файлов с .txt: {len(copied)}. "
        f"Удалено устаревших: {n_pruned}. "
        f"Карта: POST/КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt. Каталог: {POST}",
        file=sys.stdout,
    )


def build_post_main_only(*, changed_only: bool) -> None:
    """Снимок main-only: полный или только изменённые файлы."""
    POST.mkdir(parents=True, exist_ok=True)

    copy_helpers(main_only=True, force_helpers=not changed_only)

    prev_manifest = load_manifest()
    prev_hashes: Dict[str, str] = prev_manifest.get("files") or {}
    new_hashes: Dict[str, str] = dict(prev_hashes)
    copied: List[Tuple[Path, Path]] = []
    skipped = 0

    for src_path, rel in iter_main_only_sources():
        rel_key = rel.as_posix()
        try:
            digest = file_sha256(src_path)
        except OSError as e:
            print(f"Пропуск (ошибка чтения): {src_path}: {e}", file=sys.stderr)
            continue
        new_hashes[rel_key] = digest
        if changed_only:
            if prev_hashes.get(rel_key) == digest:
                skipped += 1
                continue
            dest_post = POST / dest_with_txt(rel)
            if dest_post.is_file() and file_sha256(dest_post) == digest:
                skipped += 1
                continue
        copy_one_txt_suffix(src_path, rel)
        copied.append((rel, dest_with_txt(rel)))

    if changed_only:
        write_placement_map_changed_only(copied)
        save_manifest(new_hashes, mode="main-only-changed")
        print(
            f"Готово [main-only, только изменения]. Обновлено: {len(copied)}, "
            f"без изменений: {skipped}. Список: POST/ОБНОВЛЁННЫЕ_ФАЙЛЫ.txt. Каталог: {POST}",
            file=sys.stdout,
        )
    else:
        n_pruned = prune_obsolete_post_files(prev_hashes, set(new_hashes))
        write_placement_map_main_only(copied)
        save_manifest(new_hashes, mode="main-only-full")
        print(
            f"Готово [main-only, полный]. Файлов с .txt: {len(copied)}. "
            f"Удалено устаревших: {n_pruned}. Каталог: {POST}",
            file=sys.stdout,
        )

    for garbage in POST.rglob(".DS_Store"):
        try:
            garbage.unlink()
        except OSError:
            pass


def build_post_full() -> None:
    """Полный снимок (без удаления всего POST/)."""
    POST.mkdir(parents=True, exist_ok=True)
    prev_manifest = load_manifest()
    prev_hashes: Dict[str, str] = prev_manifest.get("files") or {}

    copy_helpers(main_only=False, force_helpers=False)

    copied: List[Tuple[Path, Path]] = []
    n_code = 0
    all_hashes: Dict[str, str] = {}
    new_keys: set[str] = set()

    for p in iter_root_py_files(ROOT):
        rel = p.relative_to(ROOT)
        rel_key = rel.as_posix()
        new_keys.add(rel_key)
        copy_one_txt_suffix(p, rel)
        copied.append((rel, dest_with_txt(rel)))
        all_hashes[rel_key] = file_sha256(p)
        n_code += 1

    for name in ("config.json", "README.md", "requirements.txt"):
        p = ROOT / name
        if not p.is_file():
            print(f"Пропуск (нет файла): {p}", file=sys.stderr)
            continue
        rel = Path(name)
        rel_key = rel.as_posix()
        new_keys.add(rel_key)
        copy_one_txt_suffix(p, rel)
        all_hashes[rel_key] = file_sha256(p)
        n_code += 1

    src_root = ROOT / "src"
    for p in iter_py_files(src_root, main_only=False):
        rel = p.relative_to(ROOT)
        rel_key = rel.as_posix()
        new_keys.add(rel_key)
        copy_one_txt_suffix(p, rel)
        all_hashes[rel_key] = file_sha256(p)
        n_code += 1

    n_docs = 0
    for p in iter_docs_files(_DOCS_ROOT):
        rel = p.relative_to(ROOT)
        rel_key = rel.as_posix()
        new_keys.add(rel_key)
        copy_one_txt_suffix(p, rel)
        all_hashes[rel_key] = file_sha256(p)
        n_docs += 1

    n_pruned = prune_obsolete_post_files(prev_hashes, new_keys)

    if HELPERS_SRC.is_dir():
        helper = HELPERS_SRC / "КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt"
        if helper.is_file():
            shutil.copy2(helper, POST / helper.name)

    save_manifest(all_hashes, mode="full")

    for garbage in POST.rglob(".DS_Store"):
        try:
            garbage.unlink()
        except OSError:
            pass

    print(
        f"Готово [полный]. Файлов с .txt: {n_code}; Docs: {n_docs}. "
        f"Удалено устаревших: {n_pruned}. Каталог: {POST}",
        file=sys.stdout,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Сборка каталога POST/ для переноса без Git.")
    parser.add_argument(
        "--program-only",
        action="store_true",
        help="Все .py + config.json (включая Tests/Tools), карта КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt",
    )
    parser.add_argument(
        "--main-only",
        action="store_true",
        help="Только основная программа: .py + config.json, без Tests/Tools/Docs",
    )
    parser.add_argument(
        "--changed-only",
        action="store_true",
        help="Копировать только изменённые файлы (требует --main-only; не очищает POST)",
    )
    args = parser.parse_args()

    if args.changed_only and not args.main_only:
        print("Ошибка: --changed-only работает только вместе с --main-only", file=sys.stderr)
        sys.exit(1)

    if args.program_only and args.main_only:
        print("Ошибка: укажите только один из --program-only или --main-only", file=sys.stderr)
        sys.exit(1)

    if args.program_only:
        if args.changed_only:
            print("Ошибка: --changed-only не поддерживается с --program-only", file=sys.stderr)
            sys.exit(1)
        build_post_program_only()
    elif args.main_only:
        build_post_main_only(changed_only=args.changed_only)
    else:
        if args.changed_only:
            print("Ошибка: --changed-only без --main-only не поддерживается", file=sys.stderr)
            sys.exit(1)
        build_post_full()


if __name__ == "__main__":
    main()
