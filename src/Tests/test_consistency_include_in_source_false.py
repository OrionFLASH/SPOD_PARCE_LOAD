# -*- coding: utf-8 -*-
"""Тесты: листы с include_in_source=false участвуют в consistency."""

from __future__ import annotations

import pandas as pd

from src.consistency_checks import run_all_consistency_checks
from src.main_impl import (
    build_raw_sheets_data_for_consistency,
    copy_consistency_results_from_raw_to_processed,
)


def test_build_raw_includes_sheets_missing_from_source() -> None:
    """LIST-REWARDS нет в raw_sheets (source), но есть в sheets_data — попадает в checks."""
    raw_sheets = {
        "REPORT": (pd.DataFrame({"A": [1]}), {"sheet": "REPORT", "include_in_source": True}),
    }
    sheets_data = {
        "REPORT": (pd.DataFrame({"A": [1]}), {"sheet": "REPORT", "include_in_source": True}),
        "LIST-REWARDS": (
            pd.DataFrame(
                {
                    "Код турнира": ["t1"],
                    "Код награды": ["r1"],
                    "Табельный номер сотрудника": ["1"],
                    "Дата создания": ["2026-03-18T10:00:00"],
                }
            ),
            {"sheet": "LIST-REWARDS", "include_in_source": False},
        ),
    }
    out = build_raw_sheets_data_for_consistency(raw_sheets, sheets_data)
    assert "REPORT" in out
    assert "LIST-REWARDS" in out
    assert list(out["LIST-REWARDS"][0]["Код турнира"]) == ["t1"]


def test_list_rewards_unique_column_copied_to_processed() -> None:
    """Правило unique на LIST-REWARDS создаёт колонку и копирует её на обработанный лист."""
    lr_df = pd.DataFrame(
        {
            "Код турнира": ["t1", "t1"],
            "Код награды": ["r1", "r1"],
            "Табельный номер сотрудника": ["1", "1"],
            "Дата создания": ["2026-03-18T10:00:00", "2026-03-18T22:00:00"],
        }
    )
    conf = {"sheet": "LIST-REWARDS", "include_in_source": False}
    raw_sheets: dict = {}
    sheets_data = {"LIST-REWARDS": (lr_df.copy(), conf)}
    raw_sheets_data = build_raw_sheets_data_for_consistency(raw_sheets, sheets_data)

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
        "output": {
            "column_on_sheet": "ДУБЛЬ: Код турнира_Код награды_Табельный_Дата создания[:10]",
            "include_in_summary": True,
        },
    }
    results = run_all_consistency_checks(
        raw_sheets_data, {"rules": [rule]}, current_block="PROM", max_workers=1
    )
    assert results[0]["violations"] == 2
    col = "ДУБЛЬ: Код турнира_Код награды_Табельный_Дата создания[:10]"
    assert col in raw_sheets_data["LIST-REWARDS"][0].columns

    copy_consistency_results_from_raw_to_processed(
        raw_sheets_data, sheets_data, "CONSISTENCY"
    )
    assert col in sheets_data["LIST-REWARDS"][0].columns
    assert list(sheets_data["LIST-REWARDS"][0][col]) == ["x2", "x2"]
