# -*- coding: utf-8 -*-
"""safe_json_loads (BUG-03): ступенчатая починка JSON без порчи апострофов и типографских кавычек."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.json_utils import safe_json_loads  # noqa: E402
from src.Tests.fixtures.pipeline_prom_fixture import rows_for_sheet  # noqa: E402


def _legacy(s):
    """Прежнее поведение (до BUG-03) — для сравнения там, где оно давало результат."""
    if not isinstance(s, str):
        return s
    s = s.strip()
    if not s or s in {"-", "None", "null"}:
        return None
    try:
        return json.loads(s)
    except Exception:
        try:
            fixed = s.replace('"""', '"').replace("'", '"')
            fixed = re.sub(r'"{2,}([^"\s]+)"{2,}', r'"\1"', fixed)
            fixed = re.sub(r'"{2,}([^"\s]+)"{2,}\s*:', r'"\1":', fixed)
            fixed = re.sub(r':\s*"{2,}([^"\s]+)"{2,}', r':"\1"', fixed)
            fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
            fixed = re.sub(r'(\"[^"]+\")\s+(\")', r'\1: \2', fixed)
            fixed = re.sub(r'(\"[^"]+\")\s*:\s*', r'\1:', fixed)
            return json.loads(fixed)
        except Exception:
            return None


@pytest.mark.parametrize(
    "raw, expected",
    [
        ('{"""a""": """O\'Neil"""}', {"a": "O'Neil"}),                       # апостроф внутри значения
        ('{"""t""": """Конкурс “Лучший”"""}', {"t": "Конкурс “Лучший”"}),    # типографские кавычки — текст
        ('{“a”: “b”}', {"a": "b"}),                                          # типографские кавычки — синтаксис
        ("{‘a’: ‘b’}", {"a": "b"}),
        ("{'a': 'b', 'n': None, 'f': True}", {"a": "b", "n": None, "f": True}),  # литерал Python
        ('{"a": 1,}', {"a": 1}),
        ('[1, 2,]', [1, 2]),
        ('{"""k""" """v"""}', {"k": "v"}),
        ("текст", None),
        ("-", None),
        ("", None),
    ],
)
def test_repairs(raw, expected) -> None:
    assert safe_json_loads(raw) == expected


def test_same_as_legacy_where_legacy_worked_on_fixture_json() -> None:
    """На всех JSON синтетики, которые разбирались прежде, результат не изменился."""
    checked = 0
    for sheet, col in (("CONTEST-DATA", "CONTEST_FEATURE"), ("REWARD", "REWARD_ADD_DATA"),
                       ("TOURNAMENT-SCHEDULE", "TARGET_TYPE"), ("CONTEST-DATA", "BUSINESS_BLOCK")):
        header, rows, _ = rows_for_sheet(sheet)
        idx = header.index(col)
        for row in rows:
            old = _legacy(row[idx])
            if old is not None:
                assert safe_json_loads(row[idx]) == old
                checked += 1
    assert checked >= 10


def test_apostrophe_json_parsed_now_but_not_before() -> None:
    raw = '{"""feature""": ["""Клиент O\'Neil"""], """vid""": """ТЕСТ"""}'
    assert _legacy(raw) is None
    assert safe_json_loads(raw) == {"feature": ["Клиент O'Neil"], "vid": "ТЕСТ"}
