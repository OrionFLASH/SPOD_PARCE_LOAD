# -*- coding: utf-8 -*-
"""
Расшифровка снимка из IN/POST → OUT/POST (восстановление структуры проекта).

На получателе: положите пересланные .txt из POST в каталог IN/POST/
и запустите из корня проекта:

  python decrypt_post_program.py

Опции:
  --input DIR   каталог с зашифрованными .txt (по умолчанию: IN/POST)
  --output DIR  каталог результата (по умолчанию: OUT/POST)
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT = ROOT / "IN" / "POST"
DEFAULT_OUTPUT = ROOT / "OUT" / "POST"

# Импорт из src при запуске из корня проекта
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.Tools.post_transfer_crypto import (  # noqa: E402
    PACK_PASSWORD,
    decrypt_bytes,
    decrypt_manifest,
    manifest_storage_name,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Расшифровка POST-снимка программы в OUT/POST"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Каталог с .txt (по умолчанию: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Куда восстановить файлы (по умолчанию: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--password",
        type=str,
        default=PACK_PASSWORD,
        help="Пароль (по умолчанию — встроенный идентификатор bundle)",
    )
    args = parser.parse_args()

    in_dir = args.input.resolve()
    out_dir = args.output.resolve()
    manifest_path = in_dir / manifest_storage_name()

    if not manifest_path.is_file():
        print(f"Не найден манифест: {manifest_path}", file=sys.stderr)
        return 1

    try:
        manifest = decrypt_manifest(manifest_path.read_text(encoding="utf-8"), args.password)
    except Exception as exc:
        print(f"Ошибка расшифровки манифеста: {exc}", file=sys.stderr)
        return 1

    files = manifest.get("files") or []
    if not files:
        print("Манифест пуст", file=sys.stderr)
        return 1

    if out_dir.exists():
        for child in out_dir.iterdir():
            if child.is_file():
                child.unlink()
            elif child.is_dir():
                shutil.rmtree(child)
    out_dir.mkdir(parents=True, exist_ok=True)

    restored = 0
    for entry in files:
        storage = entry.get("storage")
        target = entry.get("target")
        if not storage or not target:
            continue
        src_file = in_dir / storage
        if not src_file.is_file():
            print(f"Пропуск (нет файла): {src_file}", file=sys.stderr)
            continue
        try:
            plaintext = decrypt_bytes(src_file.read_text(encoding="utf-8"), args.password)
        except Exception as exc:
            print(f"Ошибка расшифровки {storage}: {exc}", file=sys.stderr)
            return 1
        dest = out_dir / target
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(plaintext)
        restored += 1

    print(f"Восстановлено {restored} файлов в {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
