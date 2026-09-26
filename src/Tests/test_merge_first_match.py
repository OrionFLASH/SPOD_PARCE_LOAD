# -*- coding: utf-8 -*-
"""Merge mode=value: при дублях ключа в источнике берётся ПЕРВОЕ значение + WARNING (BUG-02, решение Q1)."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import main_impl  # noqa: E402


def _merge(df_ref: pd.DataFrame, columns=("STATUS",)) -> pd.DataFrame:
    base = pd.DataFrame({"CODE": ["A", "B", "C"]})
    return main_impl.add_fields_to_sheet(base, df_ref, ["CODE"], ["CODE"], list(columns), "DST", "SRC")


def test_first_value_taken_and_conflict_reported(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    main_impl._merge_key_conflicts.clear()
    file_only = []
    monkeypatch.setattr(main_impl, "_log_file_only", lambda level, msg: file_only.append((level, msg)))
    ref = pd.DataFrame({"CODE": ["A", "A", "B", "B"], "STATUS": ["first", "second", "same", "same"]})
    out = _merge(ref)
    assert out["SRC=>STATUS"].tolist() == ["first", "same", "-"]
    assert main_impl._merge_key_conflicts == [{"src": "SRC", "dst": "DST", "column": "STATUS", "keys": 1}]
    assert file_only == [(
        logging.WARNING,
        "[MERGE] SRC→DST, поле STATUS: 1 ключ(ей) с разными значениями в источнике, взято первое (примеры ключей: A)",
    )]
    with caplog.at_level(logging.WARNING):
        main_impl._report_merge_key_conflicts_summary()
    assert "полей 1" in caplog.records[-1].getMessage()
    assert main_impl._merge_key_conflicts == []


def test_no_warning_without_conflicts(caplog: pytest.LogCaptureFixture) -> None:
    main_impl._merge_key_conflicts.clear()
    ref = pd.DataFrame({"CODE": ["A", "B", "B"], "STATUS": ["x", "y", "y"]})
    with caplog.at_level(logging.WARNING):
        out = _merge(ref)
    assert out["SRC=>STATUS"].tolist() == ["x", "y", "-"]
    assert main_impl._merge_key_conflicts == []
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
