# -*- coding: utf-8 -*-
"""
Шифрование/расшифровка снимка программы для пересылки в POST (только stdlib).

Формат: base64(SPODENC1 + salt16 + XOR ciphertext).
Пароль: SPOD_post_program_bundle_v1 (должен совпадать в pack и decrypt).
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

PACK_PASSWORD: str = "SPOD_post_program_bundle_v1"
_MANIFEST_STORAGE_NAME: str = "bundle_manifest.txt"

_MAGIC = b"SPODENC1"
_SALT_LEN = 16
_PBKDF2_ITERATIONS = 200_000

# Убрать из имён намёки на скрипты (JS, auto_js и т.п.) в каталоге POST
_SCRIPT_HINT_PATTERNS: Tuple[str, ...] = (
    "_auto_js",
    "auto_js",
    "_autojs",
    "_AutoJS",
)


def _derive_key(password: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        _PBKDF2_ITERATIONS,
        dklen=32,
    )


def _xor_keystream(data_len: int, key: bytes) -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < data_len:
        block = hashlib.sha256(key + counter.to_bytes(4, "big")).digest()
        out.extend(block)
        counter += 1
    return bytes(out[:data_len])


def encrypt_bytes(plaintext: bytes, password: str = PACK_PASSWORD) -> str:
    """Шифрует байты и возвращает base64-текст для .txt."""
    salt = os.urandom(_SALT_LEN)
    key = _derive_key(password, salt)
    stream = _xor_keystream(len(plaintext), key)
    ciphertext = bytes(a ^ b for a, b in zip(plaintext, stream))
    payload = _MAGIC + salt + ciphertext
    return base64.b64encode(payload).decode("ascii")


def decrypt_bytes(blob_b64: str, password: str = PACK_PASSWORD) -> bytes:
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


def sanitize_name_part(name: str) -> str:
    """Убрать из сегмента имени намёки на JS/скрипты (не трогать _json и т.п.)."""
    result = name
    for hint in ("_auto_js", "auto_js", "_autojs", "_AutoJS"):
        result = re.sub(re.escape(hint), "", result, flags=re.IGNORECASE)
    # _js только как отдельный суффикс (…_auto_js уже убран выше)
    result = re.sub(r"(?i)_js(?=_|$)", "", result)
    result = re.sub(r"_+", "_", result).strip("_")
    return result or "data"


def sanitize_project_relpath(rel: Path) -> Path:
    """Санитизировать относительный путь для хранения в POST."""
    parts: List[str] = []
    for part in rel.parts:
        p = Path(part)
        if p.suffix:
            stem = sanitize_name_part(p.stem)
            parts.append(f"{stem}{p.suffix}")
        else:
            parts.append(sanitize_name_part(part))
    return Path(*parts)


def storage_flat_name_for_target(target_rel: Path) -> str:
    """Имя файла в корне POST (без подкаталогов): санитизированный путь через __ + .txt."""
    sanitized = sanitize_project_relpath(target_rel)
    if sanitized.parent == Path("."):
        base = sanitized.name
    else:
        base = "__".join(sanitized.parts)
    return f"{base}.txt"


def manifest_storage_name() -> str:
    return _MANIFEST_STORAGE_NAME


def build_manifest(files: List[Dict[str, str]], *, password: str = PACK_PASSWORD) -> Dict[str, Any]:
    return {
        "format": "SPOD_POST_BUNDLE_V1",
        "password_id": password,
        "files": files,
    }


def encrypt_manifest(manifest: Dict[str, Any], password: str = PACK_PASSWORD) -> str:
    raw = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
    return encrypt_bytes(raw, password)


def decrypt_manifest(blob_b64: str, password: str = PACK_PASSWORD) -> Dict[str, Any]:
    raw = decrypt_bytes(blob_b64, password)
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict) or data.get("format") != "SPOD_POST_BUNDLE_V1":
        raise ValueError("Неверный формат манифеста")
    return data


def iter_program_source_files(root: Path) -> Iterable[Tuple[Path, Path]]:
    """
    Все .py (корень + src), каталог config/*.json, README.md.
    Возвращает (абсолютный путь, rel от root).
    """
    for p in sorted(root.glob("*.py")):
        if not p.name.startswith("."):
            yield p, Path(p.name)
    cfg_dir = root / "config"
    if cfg_dir.is_dir():
        for p in sorted(cfg_dir.rglob("*.json")):
            if p.is_file():
                yield p, p.relative_to(root)
    readme = root / "README.md"
    if readme.is_file():
        yield readme, Path("README.md")
    src_root = root / "src"
    if src_root.is_dir():
        for p in sorted(src_root.rglob("*.py")):
            if "__pycache__" in p.parts:
                continue
            yield p, p.relative_to(root)
