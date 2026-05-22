# -*- coding: utf-8 -*-
"""Тесты канонизации и хешей построчного архива."""

from __future__ import annotations

import pandas as pd

from src.input_archive_row_hash import (
    compute_row_hash,
    compute_row_key,
    compute_row_hashes_from_series,
)
from src.input_archive_row_parallel import dedupe_by_key_last_wins
from src.input_archive_row_parallel import RowHashRecord


def test_row_key_stable() -> None:
    fields = {"CONTEST_CODE": "C1", "GROUP_CODE": "G1", "GROUP_VALUE": "V1"}
    h1, j1, _ = compute_row_key(fields, ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"])
    h2, j2, _ = compute_row_key(fields, ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"])
    assert h1 == h2
    assert "CONTEST_CODE" in j1


def test_row_hash_changes_on_value() -> None:
    a = {"A": "1", "B": "2"}
    b = {"A": "1", "B": "3"}
    assert compute_row_hash(a) != compute_row_hash(b)


def test_series_roundtrip() -> None:
    row = pd.Series({"CONTEST_CODE": " X ", "N": 1})
    kh, _, rh, _ = compute_row_hashes_from_series(row, ["CONTEST_CODE"])
    assert kh
    assert rh


def test_dedupe_last_wins() -> None:
    r1 = RowHashRecord(0, "aaa", "{}", "h1", {"k": "1"})
    r2 = RowHashRecord(1, "aaa", "{}", "h2", {"k": "2"})
    out, dups = dedupe_by_key_last_wins([r1, r2])
    assert dups == 1
    assert len(out) == 1
    assert out[0].row_hash == "h2"
