# -*- coding: utf-8 -*-
"""STATISTICS: number-формат не должен затирать текстовые колонки ORG_UNIT после merge."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src import main_impl as m
from src.main_impl import (
    KEY_COMPARE_AS_TEXT,
    _column_matches_format_rule,
    add_fields_to_sheet,
    apply_column_format_conversion,
)


def _load_formats() -> None:
    fmt = json.loads(Path("config/CONFIG_FORMATS.json").read_text(encoding="utf-8"))
    m.COLUMN_FORMATS = fmt["column_formats"]


def test_except_matches_merge_leaf_suffix() -> None:
    rule = {
        "sheet": "STATISTICS",
        "except_columns": ["TB_SHORT_NAME", "GOSB_NAME"],
        "data_type": "number",
    }
    # Колонка под except по суффиксу после => — правило number НЕ применяется
    assert _column_matches_format_rule("ORG_UNIT_V20=>TB_SHORT_NAME", rule) is False
    assert _column_matches_format_rule("ORG_UNIT_V20=>GOSB_NAME", rule) is False
    # Числовая колонка без except — правило применяется
    assert _column_matches_format_rule("ТБ", rule) is True


def test_number_conversion_preserves_org_unit_text_names() -> None:
    _load_formats()
    df_stat = pd.DataFrame(
        {
            "ТБ": ["18"],
            "ГОСБ": ["1802"],
            "Табельный номер": ["1"],
            "Фамилия": ["Иванов"],
        }
    )
    df_org = pd.DataFrame(
        {
            "TB_CODE": ["18"],
            "GOSB_CODE": ["1802"],
            "TB_SHORT_NAME": ["ББ"],
            "TB_FULL_NAME": ["Байкальский банк"],
            "GOSB_NAME": ["Иркутское ГОСБ"],
        }
    )
    out = add_fields_to_sheet(
        df_stat,
        df_org,
        src_keys=["TB_CODE", "GOSB_CODE"],
        dst_keys=["ТБ", "ГОСБ"],
        columns=["TB_SHORT_NAME", "TB_FULL_NAME", "GOSB_NAME"],
        sheet_name="STATISTICS",
        ref_sheet_name="ORG_UNIT_V20",
        mode="value",
        key_compare=KEY_COMPARE_AS_TEXT,
    )
    apply_column_format_conversion(out, "STATISTICS")
    assert out.loc[0, "ORG_UNIT_V20=>TB_SHORT_NAME"] == "ББ"
    assert out.loc[0, "ORG_UNIT_V20=>TB_FULL_NAME"] == "Байкальский банк"
    assert out.loc[0, "ORG_UNIT_V20=>GOSB_NAME"] == "Иркутское ГОСБ"
    # Коды по-прежнему числа
    assert int(out.loc[0, "ТБ"]) == 18
    assert int(out.loc[0, "ГОСБ"]) == 1802


def test_number_conversion_keeps_mixed_text_if_not_excepted() -> None:
    """Защита: даже без except нечисловой текст не превращается в NA."""
    m.COLUMN_FORMATS = [
        {
            "sheet": "STATISTICS",
            "except_columns": ["Табельный номер"],
            "data_type": "number",
            "decimal_places": 0,
        }
    ]
    df = pd.DataFrame(
        {
            "Табельный номер": ["1"],
            "ORG_UNIT_V20=>TB_SHORT_NAME": ["ББ"],
            "ТБ": ["18"],
        }
    )
    apply_column_format_conversion(df, "STATISTICS")
    assert df.loc[0, "ORG_UNIT_V20=>TB_SHORT_NAME"] == "ББ"
    assert int(df.loc[0, "ТБ"]) == 18
