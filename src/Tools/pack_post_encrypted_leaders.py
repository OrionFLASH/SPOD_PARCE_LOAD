# -*- coding: utf-8 -*-
"""
Упаковка src/leaders_for_admin_auto_js.py в post/leaders_for_admin_auto_js.py.txt (шифрование).

Запуск из корня проекта:
  python src/Tools/pack_post_encrypted_leaders.py
"""
from __future__ import annotations

import base64
import hashlib
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC_FILE = ROOT / "src" / "leaders_for_admin_auto_js.py"
OUT_FILE = ROOT / "post" / "leaders_for_admin_auto_js.py.txt"

# Пароль должен совпадать с post/decrypt_leaders_for_admin_auto_js.py
PACK_PASSWORD = "SPOD_post_leaders_for_admin_v1"

_MAGIC = b"SPODENC1"
_SALT_LEN = 16
_PBKDF2_ITERATIONS = 200_000


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


def encrypt_bytes(plaintext: bytes, password: str) -> str:
    """Шифрует байты и возвращает base64-текст для .txt."""
    salt = os.urandom(_SALT_LEN)
    key = _derive_key(password, salt)
    stream = _xor_keystream(len(plaintext), key)
    ciphertext = bytes(a ^ b for a, b in zip(plaintext, stream))
    payload = _MAGIC + salt + ciphertext
    return base64.b64encode(payload).decode("ascii")


def main() -> int:
    if not SRC_FILE.is_file():
        print(f"Не найден исходник: {SRC_FILE}", file=sys.stderr)
        return 1
    plaintext = SRC_FILE.read_bytes()
    encrypted = encrypt_bytes(plaintext, PACK_PASSWORD)
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(encrypted + "\n", encoding="utf-8")
    print(f"Записано: {OUT_FILE} ({len(encrypted)} символов base64)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
