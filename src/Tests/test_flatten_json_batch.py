# -*- coding: utf-8 -*-
"""Пакетное добавление колонок в flatten_json_column_recursive без фрагментации."""

from __future__ import annotations

import json
import warnings

import pandas as pd

from src.main_impl import flatten_json_column_recursive


def test_flatten_json_many_columns_no_performance_warning() -> None:
    """Много плоских ключей — без PerformanceWarning (пакетный pd.concat)."""
    payload = {f"key_{i}": f"val_{i}" for i in range(60)}
    df = pd.DataFrame({"REWARD_ADD_DATA": [json.dumps(payload, ensure_ascii=False)] * 10})
    with warnings.catch_warnings():
        warnings.simplefilter("error", pd.errors.PerformanceWarning)
        out = flatten_json_column_recursive(df.copy(), "REWARD_ADD_DATA", prefix="ADD_DATA", sheet="REWARD")
    new_cols = [c for c in out.columns if c not in df.columns]
    assert len(new_cols) == 61
    assert out.loc[0, "ADD_DATA => key_0"] == "val_0"
