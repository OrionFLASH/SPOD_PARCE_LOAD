# -*- coding: utf-8 -*-
"""
Расшифровка post/leaders_for_admin_auto_js.py.txt → src/leaders_for_admin_auto_js.py

Скопируйте каталог post/ на рабочий ПК и запустите из корня проекта:
  python post/decrypt_leaders_for_admin_auto_js.py

Опции:
  --output PATH   куда записать .py (по умолчанию: src/leaders_for_admin_auto_js.py)
  --input PATH    зашифрованный .txt (по умолчанию: рядом с этим скриптом)
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import sys
from pathlib import Path

# Пароль должен совпадать с src/Tools/pack_post_encrypted_leaders.py
PACK_PASSWORD = "SPOD_post_leaders_for_admin_v1"

_MAGIC = b"SPODENC1"
_SALT_LEN = 16
_PBKDF2_ITERATIONS = 200_000

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = SCRIPT_DIR / "leaders_for_admin_auto_js.py.txt"
DEFAULT_OUTPUT = SCRIPT_DIR.parent / "src" / "leaders_for_admin_auto_js.py"


def _derive_key(password: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        _PBKDF2_ITERATIONS,
        dklen=32,
    )


def _xor_keystream(data_len: int, key: bytes) -> bytes:
    """Потоковый XOR-ключ из SHA-256 (только stdlib)."""
    out = bytearray()
    counter = 0
    while len(out) < data_len:
        block = hashlib.sha256(key + counter.to_bytes(4, "big")).digest()
        out.extend(block)
        counter += 1
    return bytes(out[:data_len])


def decrypt_bytes(blob_b64: str, password: str) -> bytes:
    """Расшифровывает base64-текст из .txt."""
    raw = base64.b64decode("".join(blob_b64.split()))
    if not raw.startswith(_MAGIC):
        raise ValueError("Неверный формат файла (ожидается SPODENC1)")
    offset = len(_MAGIC)
    salt = raw[offset : offset + _SALT_LEN]
    ciphertext = raw[offset + _SALT_LEN :]
    key = _derive_key(password, salt)
    stream = _xor_keystream(len(ciphertext), key)
    return bytes(a ^ b for a, b in zip(ciphertext, stream))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Расшифровка leaders_for_admin_auto_js.py из post/*.txt"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Зашифрованный файл (по умолчанию: {DEFAULT_INPUT.name})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Путь для восстановленного .py",
    )
    args = parser.parse_args()

    if not args.input.is_file():
        print(f"Не найден входной файл: {args.input}", file=sys.stderr)
        return 1

    try:
        plaintext = decrypt_bytes(args.input.read_text(encoding="utf-8"), PACK_PASSWORD)
    except Exception as exc:
        print(f"Ошибка расшифровки: {exc}", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(plaintext)
    print(f"Восстановлено: {args.output} ({len(plaintext)} байт)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
