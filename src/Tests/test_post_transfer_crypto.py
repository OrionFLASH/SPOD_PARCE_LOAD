# -*- coding: utf-8 -*-
"""Тесты шифрования POST-снимка и санитизации имён."""

from __future__ import annotations

from pathlib import Path

from src.Tools.post_transfer_crypto import (
    decrypt_bytes,
    encrypt_bytes,
    sanitize_name_part,
    sanitize_project_relpath,
    storage_flat_name_for_target,
)


def test_sanitize_removes_auto_js() -> None:
    assert sanitize_name_part("leaders_for_admin_auto_js") == "leaders_for_admin"
    assert sanitize_name_part("profile_gp_auto_js") == "profile_gp"


def test_storage_flat_name_txt_suffix() -> None:
    assert storage_flat_name_for_target(Path("main.py")) == "main.py.txt"
    assert (
        storage_flat_name_for_target(Path("src/leaders_for_admin_auto_js.py"))
        == "src__leaders_for_admin.py.txt"
    )
    assert storage_flat_name_for_target(Path("config.json")) == "config.json.txt"


def test_encrypt_decrypt_roundtrip() -> None:
    data = b"print('hello')\n"
    enc = encrypt_bytes(data)
    assert decrypt_bytes(enc) == data


def test_sanitize_keeps_json_in_name() -> None:
    assert "json" in sanitize_name_part("export_spod_json_examples")
    assert sanitize_project_relpath(Path("config.json")) == Path("config.json")
    assert storage_flat_name_for_target(Path("README.md")) == "README.md.txt"
