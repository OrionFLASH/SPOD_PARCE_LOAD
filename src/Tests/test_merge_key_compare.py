# -*- coding: utf-8 -*-
"""Тесты key_compare в merge: exact vs number_as_text."""

from __future__ import annotations

import pandas as pd

from src.main_impl import (
    KEY_COMPARE_EXACT,
    KEY_COMPARE_NUMBER_AS_TEXT,
    _normalize_merge_key_value,
    add_fields_to_sheet,
)


def test_normalize_number_as_text_equates_int_and_str() -> None:
    assert _normalize_merge_key_value(18, KEY_COMPARE_NUMBER_AS_TEXT) == "18"
    assert _normalize_merge_key_value("18", KEY_COMPARE_NUMBER_AS_TEXT) == "18"
    assert _normalize_merge_key_value("18.0", KEY_COMPARE_NUMBER_AS_TEXT) == "18"
    assert _normalize_merge_key_value("«18»", KEY_COMPARE_NUMBER_AS_TEXT) == "18"


def test_normalize_exact_keeps_raw_types() -> None:
    assert _normalize_merge_key_value(18, KEY_COMPARE_EXACT) == 18
    assert _normalize_merge_key_value("18", KEY_COMPARE_EXACT) == "18"


def test_add_fields_exact_does_not_match_int_vs_str() -> None:
    df_base = pd.DataFrame({"ТБ": [18], "ГОСБ": [0]})
    df_ref = pd.DataFrame(
        {
            "TB_CODE": ["18"],
            "GOSB_CODE": ["0"],
            "TB_SHORT_NAME": ["ББ"],
        }
    )
    out = add_fields_to_sheet(
        df_base,
        df_ref,
        src_keys=["TB_CODE", "GOSB_CODE"],
        dst_keys=["ТБ", "ГОСБ"],
        columns=["TB_SHORT_NAME"],
        sheet_name="STATISTICS",
        ref_sheet_name="ORG_UNIT_V20",
        mode="value",
        key_compare=KEY_COMPARE_EXACT,
    )
    assert list(out["ORG_UNIT_V20=>TB_SHORT_NAME"]) == ["-"]


def test_add_fields_number_as_text_matches_int_vs_str() -> None:
    df_base = pd.DataFrame({"ТБ": [18], "ГОСБ": [0]})
    df_ref = pd.DataFrame(
        {
            "TB_CODE": ["18"],
            "GOSB_CODE": ["0"],
            "TB_SHORT_NAME": ["ББ"],
            "TB_FULL_NAME": ["Байкальский банк"],
            "GOSB_NAME": ["Аппарат"],
        }
    )
    out = add_fields_to_sheet(
        df_base,
        df_ref,
        src_keys=["TB_CODE", "GOSB_CODE"],
        dst_keys=["ТБ", "ГОСБ"],
        columns=["TB_SHORT_NAME", "TB_FULL_NAME", "GOSB_NAME"],
        sheet_name="STATISTICS",
        ref_sheet_name="ORG_UNIT_V20",
        mode="value",
        key_compare=KEY_COMPARE_NUMBER_AS_TEXT,
    )
    assert list(out["ORG_UNIT_V20=>TB_SHORT_NAME"]) == ["ББ"]
    assert list(out["ORG_UNIT_V20=>TB_FULL_NAME"]) == ["Байкальский банк"]
    assert list(out["ORG_UNIT_V20=>GOSB_NAME"]) == ["Аппарат"]
