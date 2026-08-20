# -*- coding: utf-8 -*-
"""Тесты json_spod_format: внешняя обёртка \" и array_value_keys (helpCodeList / seasonItem)."""

from __future__ import annotations

from src.json_spod_format_check import validate_spod_json_cell


def test_outer_single_quotes_rejected() -> None:
    inner = '[{"""period_code""": 0}]'
    raw = "'" + inner + "'"
    ok, msg = validate_spod_json_cell(raw, json_required=False)
    assert ok is False
    assert "двойными" in msg or "одинарн" in msg


def test_outer_double_quotes_ok_for_period_array() -> None:
    inner = (
        '[{"""period_code""": 0, """criterion_mark_type""": """>""", '
        '"""criterion_mark_value""": 0}]'
    )
    raw = '"' + inner + '"'
    ok, msg = validate_spod_json_cell(
        raw,
        json_required=False,
        numeric_value_keys=["period_code", "criterion_mark_value"],
    )
    assert ok is True, msg


def test_bare_brackets_empty_ok() -> None:
    ok, msg = validate_spod_json_cell("[]", json_required=False)
    assert ok is True, msg


def test_season_item_scalar_rejected() -> None:
    raw = '{"""seasonItem""": """SEASON_2026_1""", """nftFlg""": """N"""}'
    ok, msg = validate_spod_json_cell(
        raw,
        json_required=True,
        array_value_keys=["seasonItem", "helpCodeList"],
    )
    assert ok is False
    assert "seasonItem" in msg
    assert "массив" in msg


def test_season_item_array_ok() -> None:
    raw = '{"""seasonItem""": ["""SEASON_2026_1"""], """helpCodeList""": ["""NFT_1"""]}'
    ok, msg = validate_spod_json_cell(
        raw,
        json_required=True,
        array_value_keys=["seasonItem", "helpCodeList"],
    )
    assert ok is True, msg


def test_help_code_list_empty_array_ok() -> None:
    raw = '{"""helpCodeList""": []}'
    ok, msg = validate_spod_json_cell(
        raw,
        json_required=True,
        array_value_keys=["helpCodeList"],
    )
    assert ok is True, msg


def test_missing_array_key_ok() -> None:
    raw = '{"""nftFlg""": """N"""}'
    ok, msg = validate_spod_json_cell(
        raw,
        json_required=True,
        array_value_keys=["seasonItem", "helpCodeList"],
    )
    assert ok is True, msg
