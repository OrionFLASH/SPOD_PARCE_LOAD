# -*- coding: utf-8 -*-
"""
Упаковка программы в POST/: все .py, config.json, README.md — шифрование + .txt.

Запуск из корня проекта:
  python src/Tools/pack_post_encrypted_program.py

Результат: каталог POST/ (очищается и создаётся заново):
  - все зашифрованные .txt в корне POST/ (без подкаталогов);
  - bundle_manifest.txt (зашифрованный манифест);
  - pack_post_encrypted_program.py.txt, decrypt_post_program.py.txt (копии утилит);
  - КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt — карта «файл в POST → путь в OUT/POST».
"""

from __future__ import annotations

import shutil
import sys
from datetime import date
from pathlib import Path
from typing import List, Tuple

ROOT = Path(__file__).resolve().parents[2]
POST = ROOT / "POST"
PACK_SCRIPT = Path(__file__)
DECRYPT_SCRIPT = ROOT / "decrypt_post_program.py"

# Утилиты пересылки — только открытые .txt в POST, не в зашифрованном bundle
_SKIP_BUNDLE_TARGETS = {
    "decrypt_post_program.py",
    "src/Tools/pack_post_encrypted_program.py",
}

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.Tools.post_transfer_crypto import (  # noqa: E402
    PACK_PASSWORD,
    build_manifest,
    encrypt_bytes,
    encrypt_manifest,
    iter_program_source_files,
    manifest_storage_name,
    storage_flat_name_for_target,
)


def _write_placement_map(entries: List[Tuple[str, str]]) -> None:
    lines = [
        "=" * 78,
        "  КУДА ПОЛОЖИТЬ ФАЙЛЫ ПОСЛЕ РАСШИФРОВКИ (OUT/POST)",
        "=" * 78,
        "",
        "На отправителе: python src/Tools/pack_post_encrypted_program.py",
        "Перешлите каталог POST/ по почте.",
        "На получателе: положите все .txt из POST в IN/POST/ (тоже без подкаталогов).",
        "Скопируйте decrypt_post_program.py.txt в корень (уберите .txt)",
        "или используйте уже имеющийся decrypt_post_program.py.",
        "Из корня проекта:  python decrypt_post_program.py",
        "Результат: OUT/POST/ с восстановленной структурой подкаталогов.",
        "",
        "Формат:  файл в POST (плоский список)  →  OUT/POST/<путь с подпапками>",
        "-" * 78,
        "",
    ]
    for storage, target in sorted(entries, key=lambda x: x[1]):
        lines.append(f"  POST/{storage}")
        lines.append(f"      →  OUT/POST/{target}")
        lines.append("")
    lines.extend(
        [
            "-" * 78,
            f"Дата: {date.today().isoformat()}",
            f"Пароль (идентификатор): {PACK_PASSWORD}",
            "=" * 78,
        ]
    )
    (POST / "КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt").write_text("\n".join(lines), encoding="utf-8")


def _copy_utility_scripts() -> None:
    """Копии pack/decrypt в POST с суффиксом .txt (для пересылки)."""
    shutil.copy2(PACK_SCRIPT, POST / "pack_post_encrypted_program.py.txt")
    if DECRYPT_SCRIPT.is_file():
        shutil.copy2(DECRYPT_SCRIPT, POST / "decrypt_post_program.py.txt")
    else:
        print(f"Предупреждение: нет {DECRYPT_SCRIPT}", file=sys.stderr)


def main() -> int:
    if POST.is_dir():
        shutil.rmtree(POST)
    POST.mkdir(parents=True, exist_ok=True)

    manifest_files: List[dict[str, str]] = []
    placement: List[Tuple[str, str]] = []
    n_packed = 0

    for src_path, target_rel in iter_program_source_files(ROOT):
        target_key = target_rel.as_posix()
        if target_key in _SKIP_BUNDLE_TARGETS:
            continue
        storage_key = storage_flat_name_for_target(target_rel)

        plaintext = src_path.read_bytes()
        encrypted = encrypt_bytes(plaintext)
        out_path = POST / storage_key
        out_path.write_text(encrypted + "\n", encoding="utf-8")

        manifest_files.append({"storage": storage_key, "target": target_key})
        placement.append((storage_key, target_key))
        n_packed += 1

    manifest = build_manifest(manifest_files)
    manifest_enc = encrypt_manifest(manifest)
    manifest_name = manifest_storage_name()
    (POST / manifest_name).write_text(manifest_enc + "\n", encoding="utf-8")
    placement.append((manifest_name, "(манифест — не копировать вручную)"))

    _copy_utility_scripts()
    _write_placement_map(placement)

    print(
        f"Готово: POST/ — {n_packed} зашифрованных файлов + манифест + утилиты (.txt). "
        f"Каталог: {POST}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
