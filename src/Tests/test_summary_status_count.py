# -*- coding: utf-8 -*-
"""SUMMARY: count по статусам турнира (status_filters + count_label)."""

from __future__ import annotations

import pandas as pd

import src.main_impl as main_impl


def _status_count_rules() -> list[dict]:
    """Пять правил count по TOURNAMENT_STATUS → SUMMARY."""
    statuses = [
        ("ACTIVE", "АКТИВНЫЙ"),
        ("COMPLETED", "ЗАВЕРШЕН"),
        ("CANCELLED", "ОТМЕНЕН"),
        ("SUMMING_UP", "ПОДВЕДЕНИЕ ИТОГОВ"),
        ("DELETED", "УДАЛЕН"),
    ]
    rules: list[dict] = []
    for label, status in statuses:
        rules.append(
            {
                "sheet_src": "TOURNAMENT-SCHEDULE",
                "sheet_dst": "SUMMARY",
                "src_key": ["CONTEST_CODE"],
                "dst_key": ["CONTEST_CODE"],
                "column": ["TOURNAMENT_CODE"],
                "mode": "count",
                "multiply_rows": False,
                "count_aggregation": "nunique",
                "count_label": label,
                "status_filters": {"TOURNAMENT_STATUS": [status]},
                "custom_conditions": None,
                "group_by": None,
                "aggregate": None,
            }
        )
    return rules


def test_build_summary_sheet_applies_status_count_labels() -> None:
    """На SUMMARY появляются 5 колонок COUNT_nunique_* по статусам."""
    main_impl.SUMMARY_KEY_COLUMNS = [
        "CONTEST_CODE",
        "TOURNAMENT_CODE",
        "REWARD_CODE",
        "GROUP_CODE",
        "GROUP_VALUE",
        "INDICATOR_CODE",
        "INDICATOR_ADD_CALC_TYPE",
    ]
    main_impl.SUMMARY_KEY_DEFS = [
        {"sheet": "CONTEST-DATA", "cols": ["CONTEST_CODE"]},
        {"sheet": "TOURNAMENT-SCHEDULE", "cols": ["TOURNAMENT_CODE", "CONTEST_CODE"]},
    ]

    contests = pd.DataFrame({"CONTEST_CODE": ["C1"]})
    schedule = pd.DataFrame(
        {
            "CONTEST_CODE": ["C1", "C1", "C1", "C1", "C1", "C1"],
            "TOURNAMENT_CODE": ["T_ACT1", "T_ACT2", "T_DONE", "T_CAN", "T_SUM", "T_DEL"],
            "TOURNAMENT_STATUS": [
                "АКТИВНЫЙ",
                "АКТИВНЫЙ",
                "ЗАВЕРШЕН",
                "ОТМЕНЕН",
                "ПОДВЕДЕНИЕ ИТОГОВ",
                "УДАЛЕН",
            ],
        }
    )
    # Пустые связанные листы — каркас SUMMARY всё равно соберётся по contest/schedule
    empty = pd.DataFrame(
        columns=["CONTEST_CODE", "REWARD_CODE", "GROUP_CODE", "GROUP_VALUE", "INDICATOR_CODE", "INDICATOR_ADD_CALC_TYPE"]
    )
    dfs = {
        "CONTEST-DATA": contests,
        "TOURNAMENT-SCHEDULE": schedule,
        "REWARD-LINK": empty.copy(),
        "GROUP": empty.copy(),
        "INDICATOR": empty.copy(),
        "REWARD": pd.DataFrame(columns=["REWARD_CODE"]),
    }

    out = main_impl.build_summary_sheet(
        dfs,
        params_summary={"sheet": "SUMMARY"},
        merge_fields=_status_count_rules(),
    )

    expected = {
        "TOURNAMENT-SCHEDULE=>COUNT_nunique_ACTIVE": 2,
        "TOURNAMENT-SCHEDULE=>COUNT_nunique_COMPLETED": 1,
        "TOURNAMENT-SCHEDULE=>COUNT_nunique_CANCELLED": 1,
        "TOURNAMENT-SCHEDULE=>COUNT_nunique_SUMMING_UP": 1,
        "TOURNAMENT-SCHEDULE=>COUNT_nunique_DELETED": 1,
    }
    for col, want in expected.items():
        assert col in out.columns, f"нет колонки {col}; есть: {list(out.columns)}"
        # Одно значение на все строки конкурса C1
        assert int(out.loc[out["CONTEST_CODE"] == "C1", col].iloc[0]) == want
