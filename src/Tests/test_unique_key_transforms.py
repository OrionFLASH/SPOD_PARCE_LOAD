# -*- coding: utf-8 -*-
"""Тесты unique с key_transforms (left) и правила LIST-REWARDS для PROM."""

from __future__ import annotations

import pandas as pd

from src.consistency_checks import run_all_consistency_checks


def _sheets_list_rewards(rows: list[dict]) -> dict:
    df = pd.DataFrame(rows)
    return {"LIST-REWARDS": (df, {"sheet": "LIST-REWARDS"})}


def test_unique_left_transform_detects_dup_by_date_prefix() -> None:
    """Две строки с разным временем, но одной датой YYYY-MM-DD — дубль по left:10."""
    sheets = _sheets_list_rewards(
        [
            {
                "Код турнира": "t_1",
                "Код награды": "r_1",
                "Табельный номер сотрудника": "0001",
                "Дата создания": "2026-03-18T10:00:00",
            },
            {
                "Код турнира": "t_1",
                "Код награды": "r_1",
                "Табельный номер сотрудника": "0001",
                "Дата создания": "2026-03-18T22:15:00",
            },
            {
                "Код турнира": "t_1",
                "Код награды": "r_1",
                "Табельный номер сотрудника": "0001",
                "Дата создания": "2026-03-19T09:00:00",
            },
        ]
    )
    rule = {
        "id": "unique_list_rewards_tournament_reward_person_created10",
        "name": "LIST-REWARDS unique with date prefix",
        "type": "unique",
        "enabled": True,
        "blocks": ["PROM"],
        "sheet": "LIST-REWARDS",
        "key_columns": [
            "Код турнира",
            "Код награды",
            "Табельный номер сотрудника",
            "Дата создания",
        ],
        "key_transforms": {"Дата создания": {"type": "left", "length": 10}},
        "unique_scope_mode": "all",
        "unique_scope_conditions": [],
        "unique_scope_column": "",
        "unique_scope_value": "",
        "unique_require_non_empty": [],
        "output": {
            "column_on_sheet": "ДУБЛЬ: test",
            "include_in_summary": True,
        },
    }
    results = run_all_consistency_checks(
        sheets, {"rules": [rule]}, current_block="PROM", max_workers=1
    )
    assert len(results) == 1
    assert results[0]["violations"] == 2
    df = sheets["LIST-REWARDS"][0]
    assert list(df["ДУБЛЬ: test"]) == ["x2", "x2", ""]


def test_unique_list_rewards_rule_only_on_prom() -> None:
    """Правило с blocks PROM не считает нарушения на IFT."""
    sheets = _sheets_list_rewards(
        [
            {
                "Код турнира": "t_1",
                "Код награды": "r_1",
                "Табельный номер сотрудника": "0001",
                "Дата создания": "2026-03-18T10:00:00",
            },
            {
                "Код турнира": "t_1",
                "Код награды": "r_1",
                "Табельный номер сотрудника": "0001",
                "Дата создания": "2026-03-18T11:00:00",
            },
        ]
    )
    rule = {
        "id": "unique_list_rewards_tournament_reward_person_created10",
        "type": "unique",
        "enabled": True,
        "blocks": ["PROM"],
        "sheet": "LIST-REWARDS",
        "key_columns": [
            "Код турнира",
            "Код награды",
            "Табельный номер сотрудника",
            "Дата создания",
        ],
        "key_transforms": {"Дата создания": {"type": "left", "length": 10}},
        "unique_scope_mode": "all",
        "unique_scope_conditions": [],
        "unique_scope_column": "",
        "unique_scope_value": "",
        "unique_require_non_empty": [],
        "output": {"column_on_sheet": "ДУБЛЬ: test", "include_in_summary": True},
    }
    out_ift = run_all_consistency_checks(
        sheets, {"rules": [rule]}, current_block="IFT", max_workers=1
    )
    assert out_ift[0]["violations"] == 0
    assert "пропущено для блока IFT" in "; ".join(out_ift[0]["sample"])
    # колонка на листе не создаётся при пропуске по блоку
    assert "ДУБЛЬ: test" not in sheets["LIST-REWARDS"][0].columns

    out_prom = run_all_consistency_checks(
        sheets, {"rules": [rule]}, current_block="PROM", max_workers=1
    )
    assert out_prom[0]["violations"] == 2


def test_config_contains_list_rewards_unique_prom() -> None:
    """В CONFIG_CHECKS есть включённое правило LIST-REWARDS для PROM."""
    from src.config_loader import Config

    cfg = Config()
    rules = (cfg.consistency_checks or {}).get("rules") or []
    hit = [
        r
        for r in rules
        if r.get("id") == "unique_list_rewards_tournament_reward_person_created10"
    ]
    assert len(hit) == 1
    rule = hit[0]
    assert rule.get("enabled") is True
    assert rule.get("blocks") == ["PROM"]
    assert rule.get("sheet") == "LIST-REWARDS"
    assert rule.get("key_columns") == [
        "Код турнира",
        "Код награды",
        "Табельный номер сотрудника",
        "Дата создания",
    ]
    assert (rule.get("key_transforms") or {}).get("Дата создания", {}).get("length") == 10
