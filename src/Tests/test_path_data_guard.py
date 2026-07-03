# -*- coding: utf-8 -*-
"""Тесты защиты каталогов IN/ и OUT/."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.path_data_guard import (
    ProtectedDataPathError,
    assert_safe_mutable_tree,
    is_under_protected_data,
    post_decrypt_test_dirs,
)


def test_protected_in_and_out() -> None:
    root = Path("/proj")
    assert is_under_protected_data(root / "IN" / "SPOD", root)
    assert is_under_protected_data(root / "OUT" / "2026" / "02-07", root)
    assert not is_under_protected_data(root / ".work" / "x", root)
    assert not is_under_protected_data(root / "POST", root)


def test_assert_safe_blocks_in_out() -> None:
    root = Path("/proj")
    with pytest.raises(ProtectedDataPathError):
        assert_safe_mutable_tree(root / "OUT" / "POST", root)
    assert_safe_mutable_tree(root / ".work" / "t", root)  # не бросает


def test_post_decrypt_test_dirs_outside_in_out() -> None:
    root = Path("/proj")
    in_p, out_p = post_decrypt_test_dirs(root)
    assert in_p.name == "IN_POST"
    assert out_p.name == "OUT_POST"
    assert ".work" in in_p.parts
    assert not is_under_protected_data(in_p, root)
