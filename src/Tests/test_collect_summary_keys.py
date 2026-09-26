# -*- coding: utf-8 -*-
"""
collect_summary_keys (PERF-02, BUG-13): тот же набор строк, что у прежней реализации,
и детерминированный (отсортированный) порядок.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.main_impl import SUMMARY_KEY_COLUMNS, collect_summary_keys  # noqa: E402
from src.Tests.fixtures.pipeline_prom_fixture import rows_for_sheet  # noqa: E402
from src.Tests.legacy_collect_summary_keys import legacy_collect_summary_keys  # noqa: E402

_SHEETS = ["REWARD-LINK", "TOURNAMENT-SCHEDULE", "GROUP", "REWARD", "CONTEST-DATA", "INDICATOR"]


def _fixture_dfs() -> Dict[str, pd.DataFrame]:
    dfs = {}
    for sheet in _SHEETS:
        header, rows, _bom = rows_for_sheet(sheet)
        n = len(header)
        rows = [r[: n - 1] + [";".join(r[n - 1:])] if len(r) > n else r for r in rows]
        dfs[sheet] = pd.DataFrame(rows, columns=header)
    return dfs


def _with_edge_cases(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Пропуски, пробелы по краям, «-» и сироты во всех листах-источниках ключей."""
    def add(sheet: str, row: dict) -> None:
        df = dfs[sheet]
        dfs[sheet] = pd.concat([df, pd.DataFrame([{c: row.get(c, "") for c in df.columns}])], ignore_index=True)

    add("TOURNAMENT-SCHEDULE", {"TOURNAMENT_CODE": "t_orphan", "CONTEST_CODE": np.nan})
    add("TOURNAMENT-SCHEDULE", {"TOURNAMENT_CODE": "t_dash", "CONTEST_CODE": "-"})
    add("TOURNAMENT-SCHEDULE", {"TOURNAMENT_CODE": np.nan, "CONTEST_CODE": "CONTEST_T01"})
    add("TOURNAMENT-SCHEDULE", {"TOURNAMENT_CODE": "t_T01_1", "CONTEST_CODE": "CONTEST_T04"})
    add("REWARD-LINK", {"CONTEST_CODE": np.nan, "GROUP_CODE": "BANK", "REWARD_CODE": "r_nolink"})
    add("REWARD-LINK", {"CONTEST_CODE": "-", "GROUP_CODE": "BANK", "REWARD_CODE": "r_dash"})
    add("GROUP", {"CONTEST_CODE": "CONTEST_T01", "GROUP_CODE": "TB", "GROUP_VALUE": np.nan})
    add("GROUP", {"CONTEST_CODE": np.nan, "GROUP_CODE": "ORPHAN_G", "GROUP_VALUE": "1"})
    add("GROUP", {"CONTEST_CODE": "-", "GROUP_CODE": "DASH_G", "GROUP_VALUE": "2"})
    add("INDICATOR", {"CONTEST_CODE": " CONTEST_T01 ", "INDICATOR_ADD_CALC_TYPE": " ADD1", "INDICATOR_CODE": "IND_SP"})
    add("INDICATOR", {"CONTEST_CODE": "CONTEST_T01", "INDICATOR_ADD_CALC_TYPE": np.nan, "INDICATOR_CODE": np.nan})
    add("INDICATOR", {"CONTEST_CODE": np.nan, "INDICATOR_ADD_CALC_TYPE": "X", "INDICATOR_CODE": "IND_NOC"})
    add("REWARD", {"REWARD_CODE": "r_only_in_reward"})
    add("CONTEST-DATA", {"CONTEST_CODE": "CONTEST_ONLY_DATA"})
    return dfs


def _rows(df: pd.DataFrame) -> list:
    return [tuple(r) for r in df[SUMMARY_KEY_COLUMNS].astype(str).itertuples(index=False)]


def test_same_rows_as_legacy_on_fixture() -> None:
    dfs = _fixture_dfs()
    assert sorted(set(_rows(collect_summary_keys(dfs)))) == sorted(set(_rows(legacy_collect_summary_keys(dfs))))


def test_same_rows_as_legacy_with_edge_cases() -> None:
    dfs = _with_edge_cases(_fixture_dfs())
    new = _rows(collect_summary_keys(dfs))
    old = _rows(legacy_collect_summary_keys(dfs))
    assert len(new) == len(set(new)) == len(old)
    assert set(new) == set(old)


def test_rows_sorted_and_deterministic() -> None:
    """Порядок — сортировка по ключам (BUG-13). Набор строк от порядка входа зависит только через
    «первый INDICATOR_CODE» для (CONTEST_CODE, INDICATOR_ADD_CALC_TYPE) — так задумано (правило Q1)."""
    dfs = _with_edge_cases(_fixture_dfs())
    rows = _rows(collect_summary_keys(dfs))
    assert rows == sorted(rows)
    assert _rows(collect_summary_keys(dfs)) == rows
    others = {k: (v if k == "INDICATOR" else v.sample(frac=1.0, random_state=7).reset_index(drop=True))
              for k, v in dfs.items()}
    assert _rows(collect_summary_keys(others)) == rows


def test_empty_input() -> None:
    out = collect_summary_keys({})
    assert list(out.columns) == SUMMARY_KEY_COLUMNS and out.empty
