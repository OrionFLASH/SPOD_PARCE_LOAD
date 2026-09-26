# -*- coding: utf-8 -*-
"""Потоковая запись (PERF-07): та же книга, что у pandas.to_excel + _format_sheet, в особых случаях."""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import main_impl  # noqa: E402
from src.Tools.pipeline_fingerprint import compare_fingerprints, fingerprint_workbook  # noqa: E402


def _sheets() -> dict:
    return {
        "SUMMARY": (pd.DataFrame({"K": ["a", "b"], "N": [1.0, 2.5]}), {"sheet": "SUMMARY", "freeze": "B2"}),
        "TYPES": (pd.DataFrame({
            "int": pd.Series([1, None, 3], dtype="Int64"),
            "float": [1.0, np.nan, np.inf],
            "dt": [pd.Timestamp("2025-01-02 03:04:05"), pd.NaT, pd.Timestamp("2026-01-01")],
            "date": [dt.date(2024, 5, 6), None, dt.date(2024, 1, 1)],
            "bool": [True, False, True],
            "text": ["", "текст", "=1+1"],
            "td": [pd.Timedelta(hours=6), pd.Timedelta(0), pd.Timedelta(days=1)],
        }), {"sheet": "TYPES", "col_width_mode": "AUTO", "max_col_width": 40}),
        "EMPTY_ROWS": (pd.DataFrame(columns=["A", "B"]), {"sheet": "EMPTY_ROWS"}),
        "NO_COLS": (pd.DataFrame(), {"sheet": "NO_COLS"}),
    }


@pytest.mark.parametrize("use_color_scheme", [True, False])
def test_write_only_equals_default(tmp_path: Path, monkeypatch, use_color_scheme: bool) -> None:
    default_path = tmp_path / "default.xlsx"
    wo_path = tmp_path / "write_only.xlsx"
    monkeypatch.setattr(main_impl, "EXCEL_WRITER", "openpyxl")
    main_impl.write_to_excel(_sheets(), str(default_path), use_color_scheme=use_color_scheme)
    monkeypatch.setattr(main_impl, "EXCEL_WRITER", "write_only")
    main_impl.write_to_excel(_sheets(), str(wo_path), use_color_scheme=use_color_scheme)
    diffs, _ = compare_fingerprints(
        fingerprint_workbook(str(default_path)), fingerprint_workbook(str(wo_path)), strict_order=True
    )
    assert diffs == []


def test_excel_writer_config_values() -> None:
    from src.config_loader import parse_excel_writer

    assert parse_excel_writer({}) == "openpyxl"
    assert parse_excel_writer({"performance": {"excel_writer": "WRITE_ONLY"}}) == "write_only"
    with pytest.raises(ValueError):
        parse_excel_writer({"performance": {"excel_writer": "xlsxwriter"}})
