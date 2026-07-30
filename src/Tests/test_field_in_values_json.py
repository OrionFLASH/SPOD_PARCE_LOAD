# -*- coding: utf-8 -*-
"""Тесты field_in_values: скаляр, массив JSON и SPOD-строка [\"\"\"…\"\"\"]."""

from __future__ import annotations

import pandas as pd

from src.consistency_checks import (
    _field_in_values_coerce_to_items,
    _run_field_in_values_check,
    _validate_field_in_values_scalar,
)


_ALLOWED = {"CSM", "KMKKSB", "MNS", "сбросить"}


def test_coerce_scalar_and_list() -> None:
    items, err = _field_in_values_coerce_to_items("KMKKSB")
    assert err is None and items == ["KMKKSB"]
    items, err = _field_in_values_coerce_to_items(["KMKKSB", "MNS"])
    assert err is None and items == ["KMKKSB", "MNS"]
    items, err = _field_in_values_coerce_to_items([])
    assert err is None and items == []


def test_coerce_spod_array_string() -> None:
    items, err = _field_in_values_coerce_to_items('["""KMKKSB"""]')
    assert err is None and items == ["KMKKSB"]
    items, err = _field_in_values_coerce_to_items("[]")
    assert err is None and items == []


def test_validate_array_ok_and_bad() -> None:
    assert _validate_field_in_values_scalar(["KMKKSB"], _ALLOWED, True) == "OK"
    assert _validate_field_in_values_scalar('["""MNS"""]', _ALLOWED, True) == "OK"
    msg = _validate_field_in_values_scalar(["KMKKSB", "BAD"], _ALLOWED, True)
    assert msg.startswith("не в списке:") and "BAD" in msg


def test_run_json_business_block_in() -> None:
    df = pd.DataFrame(
        {
            "REWARD_ADD_DATA": [
                '{"""businessBlock""": ["""KMKKSB"""]}',
                '{"""businessBlock""": ["""ZZZ"""]}',
                "",
            ]
        }
    )
    sheets = {"REWARD": (df, {})}
    rule = {
        "id": "t_bb",
        "sheet": "REWARD",
        "source": "json",
        "json_column": "REWARD_ADD_DATA",
        "json_key": "businessBlock",
        "allowed_values": list(_ALLOWED),
        "allow_empty": True,
        "output": {"column_on_sheet": "ПРОВЕРКА: bb"},
    }
    _run_field_in_values_check(sheets, rule)
    out = sheets["REWARD"][0]["ПРОВЕРКА: bb"].tolist()
    assert out[0] == "OK"
    assert out[1].startswith("не в списке:")
    assert out[2] == "OK"


def test_run_column_spod_array_in() -> None:
    df = pd.DataFrame({"BUSINESS_BLOCK": ['["""KMKKSB"""]', '["""NOPE"""]', "[]"]})
    sheets = {"CONTEST-DATA": (df, {})}
    rule = {
        "id": "t_col",
        "sheet": "CONTEST-DATA",
        "source": "column",
        "field": "BUSINESS_BLOCK",
        "allowed_values": list(_ALLOWED),
        "allow_empty": True,
        "output": {"column_on_sheet": "ПРОВЕРКА: BUSINESS_BLOCK IN"},
    }
    _run_field_in_values_check(sheets, rule)
    out = sheets["CONTEST-DATA"][0]["ПРОВЕРКА: BUSINESS_BLOCK IN"].tolist()
    assert out == ["OK", "не в списке: NOPE", "OK"]
