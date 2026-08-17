"""Списки каталога fill: INDICATOR_CODE — dropdown, нужные коды методов."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "common" / "web-edit" / "game_edit_catalog.json"


def _field(section_id: str, key: str) -> dict:
    data = json.loads(CATALOG.read_text(encoding="utf-8"))
    for sec in data["sections"]:
        if sec["id"] == section_id:
            for f in sec["fields"]:
                if f["key"] == key:
                    return f
    raise AssertionError(f"Нет поля {section_id}::{key}")


def test_indicator_code_is_dropdown_with_wait() -> None:
    f = _field("TABLE:INDICATOR", "INDICATOR_CODE")
    assert f["kind"] == "dropdown"
    assert f["default"] == "WAIT"
    variants = f["variants"]
    assert "WAIT" in variants
    assert "WD" in variants
    assert len(variants) >= 16


def test_plan_and_factor_lists() -> None:
    plan = _field("CONTEST", "PLAN_METHOD_CODE")
    assert plan["variants"] == ["NOT_USED", "PRESET_VALUE", "DEPENDS_PREVIOUS_PERIOD"]
    mod = _field("CONTEST", "PLAN_MOD_METOD")
    assert "APPEND" in mod["variants"]
    assert "MULTIPLIER" in mod["variants"]
    fact = _field("CONTEST", "CONTEST_FACTOR_METHOD")
    assert "FACT" in fact["variants"]
    assert "RUN_RATE" in fact["variants"]
    post = _field("CONTEST", "FACT_POST_PROCESSING")
    assert len(post["variants"]) == 6
    agg = _field("TABLE:INDICATOR", "INDICATOR_AGG_FUNCTION")
    for code in ("MIN", "AVG", "COUNT", "LAST_VALUE"):
        assert code in agg["variants"]


def test_reward_type_list() -> None:
    f = _field("REWARD", "REWARD_TYPE")
    assert f["kind"] == "dropdown"
    assert f["variants"] == ["BADGE", "LABEL", "ITEM", "CRYSTAL"]
    assert "CRISTAL" not in f["variants"]


def test_marks_after_allow_empty() -> None:
    data = json.loads(CATALOG.read_text(encoding="utf-8"))
    total = 0
    for sec in data["sections"]:
        for f in sec["fields"]:
            total += 1
            keys = list(f.keys())
            assert "marks" in f, f"{sec['id']}::{f['key']}"
            assert keys.index("marks") == keys.index("allow_empty") + 1, (
                f"{sec['id']}::{f['key']}"
            )
    assert total >= 100
    assert data.get("marksManifest") == ["ПКАП", "ФАБРИКА"]
