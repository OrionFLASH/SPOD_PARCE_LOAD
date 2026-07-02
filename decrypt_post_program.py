# -*- coding: utf-8 -*-
"""
Расшифровка снимка из IN/POST → OUT/POST (восстановление структуры проекта).

На получателе: положите все пересланные .txt в IN/POST/ (плоский список,
без подкаталогов) и запустите из корня проекта:

  python decrypt_post_program.py

Опции:
  --input DIR   каталог с зашифрованными .txt (по умолчанию: IN/POST)
  --output DIR  каталог результата (по умолчанию: OUT/POST)
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT = ROOT / "IN" / "POST"
DEFAULT_OUTPUT = ROOT / "OUT" / "POST"

# Константы оставлены совместимыми со старыми пакетами.
BUNDLE_PASSWORD_ID: str = "SPOD_post_program_bundle_v1"
_MANIFEST_FILE_NAME: str = "bundle_manifest.txt"
_FRAME_TAG = b"SPODENC1"
_SALT_BYTES = 16
_KDF_ROUNDS = 200_000
_KDF_LEN = 32
_BUNDLE_FORMAT = "SPOD_POST_BUNDLE_V1"


def _derive_secret(password: str, salt: bytes) -> bytes:
    """Построить ключ потока из пароля и соли."""
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        _KDF_ROUNDS,
        dklen=_KDF_LEN,
    )


def _build_stream(length: int, key: bytes) -> bytes:
    """Построить детерминированный поток байтов нужной длины."""
    out = bytearray()
    counter = 0
    while len(out) < length:
        block = hashlib.sha256(key + counter.to_bytes(4, "big")).digest()
        out.extend(block)
        counter += 1
    return bytes(out[:length])


def _decode_payload(payload_b64: str, password: str) -> bytes:
    """Раскодировать payload старого формата SPODENC1."""
    raw = base64.b64decode("".join(payload_b64.split()))
    if not raw.startswith(_FRAME_TAG):
        raise ValueError("Неверный формат файла (ожидается SPODENC1)")
    salt_start = len(_FRAME_TAG)
    salt = raw[salt_start : salt_start + _SALT_BYTES]
    ciphertext = raw[salt_start + _SALT_BYTES :]
    key = _derive_secret(password, salt)
    stream = _build_stream(len(ciphertext), key)
    return bytes(left ^ right for left, right in zip(ciphertext, stream))


def _decode_manifest(payload_b64: str, password: str) -> Dict[str, Any]:
    """Раскодировать манифест и проверить сигнатуру формата."""
    raw = _decode_payload(payload_b64, password)
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict) or data.get("format") != _BUNDLE_FORMAT:
        raise ValueError("Неверный формат манифеста")
    return data


def _resolve_safe_destination(base_dir: Path, target_rel: str) -> Path:
    """Собрать безопасный путь назначения без выхода за пределы OUT/POST."""
    normalized = target_rel.replace("\\", "/").strip("/")
    if not normalized:
        raise ValueError("Пустой путь назначения")
    parts = [part for part in normalized.split("/") if part]
    if any(part in {".", ".."} for part in parts):
        raise ValueError(f"Небезопасный путь назначения: {target_rel}")
    destination = (base_dir / Path(*parts)).resolve()
    base_resolved = base_dir.resolve()
    if destination != base_resolved and base_resolved not in destination.parents:
        raise ValueError(f"Выход за каталог назначения запрещён: {target_rel}")
    return destination


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
        default=BUNDLE_PASSWORD_ID,
        help="Пароль (по умолчанию — встроенный идентификатор bundle)",
    )
    args = parser.parse_args()

    in_dir = args.input.resolve()
    out_dir = args.output.resolve()
    manifest_path = in_dir / _MANIFEST_FILE_NAME

    if not manifest_path.is_file():
        print(f"Не найден манифест: {manifest_path}", file=sys.stderr)
        return 1

    try:
        manifest = _decode_manifest(manifest_path.read_text(encoding="utf-8"), args.password)
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
            plaintext = _decode_payload(src_file.read_text(encoding="utf-8"), args.password)
        except Exception as exc:
            print(f"Ошибка расшифровки {storage}: {exc}", file=sys.stderr)
            return 1
        try:
            dest = _resolve_safe_destination(out_dir, target)
        except Exception as exc:
            print(f"Ошибка пути назначения для {storage}: {exc}", file=sys.stderr)
            return 1
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(plaintext)
        restored += 1

    print(f"Восстановлено {restored} файлов в {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
