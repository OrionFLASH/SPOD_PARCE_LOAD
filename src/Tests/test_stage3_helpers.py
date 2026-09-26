# -*- coding: utf-8 -*-
"""Этап 3 рефакторинга: вспомогательные функции main_impl (STR-03, BUG-10 и др.)."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import main_impl  # noqa: E402


def test_select_consistency_sheets_tolerates_empty_and_text_violations() -> None:
    summary = pd.DataFrame({
        "sheet": ["A", "B", "C", "D", None],
        "violations": [3, "", "-", np.nan, 5],
    })
    data = {"CONSISTENCY": (summary, {}), "A": (pd.DataFrame(), {}), "B": (pd.DataFrame(), {}),
            "C": (pd.DataFrame(), {}), "D": None, "E": (pd.DataFrame(), {})}
    out = main_impl.select_consistency_sheets(data, "CONSISTENCY")
    assert set(out) == {"CONSISTENCY", "A"}


def test_select_consistency_sheets_without_summary() -> None:
    data = {"A": (pd.DataFrame(), {})}
    assert main_impl.select_consistency_sheets(data, "CONSISTENCY") == {}


def test_setup_logger_adds_file_handler_despite_foreign_handlers(tmp_path: Path, monkeypatch) -> None:
    """BUG-05: при чужих обработчиках файл лога всё равно создаётся; повторный вызов не дублирует."""
    import logging

    monkeypatch.setattr(main_impl, "DIR_LOGS", str(tmp_path / "LOGS"))
    root = logging.getLogger()
    foreign = logging.StreamHandler()
    root.addHandler(foreign)
    try:
        log1 = main_impl.setup_logger()
        logging.warning("проверка BUG-05")
        log2 = main_impl.setup_logger(block_suffix="IFT")
        ours = [h for h in root.handlers if getattr(h, main_impl._SPOD_HANDLER_ATTR, False)]
        assert len(ours) == 1 and isinstance(ours[0], logging.FileHandler)
        assert log2.endswith("_IFT.log")
        assert "проверка BUG-05" in Path(log1).read_text(encoding="utf-8")
        assert foreign.level == logging.WARNING
    finally:
        for h in list(root.handlers):
            if getattr(h, main_impl._SPOD_HANDLER_ATTR, False) or h is foreign:
                root.removeHandler(h)
                h.close()


def test_cleanup_old_logs(tmp_path: Path) -> None:
    """LOG-04: удаляются только старые логи программы; пустые каталоги дат — тоже; 0 — ничего."""
    import os
    import time as _t
    from datetime import datetime

    old_dir = tmp_path / "2025" / "01-01"
    old_dir.mkdir(parents=True)
    old_log = old_dir / "LOGS_INFO_20250101_10_00_00.log"
    old_log.write_text("x")
    foreign = tmp_path / "2025" / "keep.txt"
    foreign.write_text("x")
    new_dir = tmp_path / "2026" / "26-09"
    new_dir.mkdir(parents=True)
    new_log = new_dir / "LOGS_INFO_20260926_10_00_00.log"
    new_log.write_text("x")
    long_ago = _t.time() - 40 * 86400
    os.utime(old_log, (long_ago, long_ago))
    assert main_impl.cleanup_old_logs(str(tmp_path), "LOGS", 0) == 0 and old_log.exists()
    assert main_impl.cleanup_old_logs(str(tmp_path), "LOGS", 30, now=datetime.now()) == 1
    assert not old_log.exists() and not old_dir.exists()
    assert foreign.exists() and new_log.exists()


def test_parse_log_retention_days() -> None:
    import pytest
    from src.config_loader import parse_log_retention_days

    assert parse_log_retention_days({}) == 0
    assert parse_log_retention_days({"logging": {"retention_days": "14"}}) == 14
    with pytest.raises(ValueError):
        parse_log_retention_days({"logging": {"retention_days": -1}})


def test_warn_if_long_path(caplog) -> None:
    import logging

    with caplog.at_level(logging.WARNING):
        main_impl._warn_if_long_path("/tmp/" + "x" * 300 + ".xlsx")
        main_impl._warn_if_long_path("/tmp/short.xlsx")
    msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(msgs) == 1 and "MAX_PATH" in msgs[0]


def _fixture_sheets() -> dict:
    from src.Tests.fixtures.pipeline_prom_fixture import rows_for_sheet
    from src.config_loader import load_config_dict, default_config_path

    cfg = load_config_dict(default_config_path(str(ROOT)))
    data = {}
    for entry in cfg["input_files"]["PROM"]:
        header, rows, _ = rows_for_sheet(entry["sheet"])
        n = len(header)
        rows = [r[: n - 1] + [";".join(r[n - 1:])] if len(r) > n else r for r in rows]
        data[entry["sheet"]] = (pd.DataFrame(rows, columns=header, dtype=object), dict(entry))
    return data, cfg


def test_grouped_merge_equals_sequential(monkeypatch) -> None:
    """BUG-07: параллельные группы дают тот же результат, что и выполнение правил строго по одному."""
    import copy

    base, cfg = _fixture_sheets()
    rules = [r for r in cfg["merge_fields_advanced"] if r.get("sheet_dst") != "SUMMARY"]

    grouped = copy.deepcopy(base)
    main_impl.merge_fields_across_sheets(grouped, rules, count_column_prefix="COUNT", merge_name="T")

    sequential = copy.deepcopy(base)
    monkeypatch.setattr(main_impl, "_group_independent_rules", lambda rs: [[r] for r in rs])
    main_impl.merge_fields_across_sheets(sequential, rules, count_column_prefix="COUNT", merge_name="T")

    assert grouped.keys() == sequential.keys()
    for name in grouped:
        pd.testing.assert_frame_equal(grouped[name][0], sequential[name][0], check_like=False)


def test_grouping_rules() -> None:
    r = lambda src, dst, mult=False: {"sheet_src": src, "sheet_dst": dst, "multiply_rows": mult}  # noqa: E731
    groups = main_impl._group_independent_rules([
        r("A", "B"), r("C", "D"),   # независимы
        r("B", "E"),                # читает B, который пишет группа → новая группа
        r("F", "G", mult=True),     # multiply_rows → отдельная группа
        r("H", "I"),
    ])
    assert [[x["sheet_dst"] for x in g] for g in groups] == [["B", "D"], ["E"], ["G"], ["I"]]


def _legacy_multiply(df_base, df_ref, src_keys, dst_keys, columns, ref_sheet_name):
    """Прежняя реализация multiply_rows (iterrows) — только для сравнения (ключи exact)."""
    rows = []
    ref_keys = df_ref[src_keys].apply(tuple, axis=1)
    for _, base_row in df_base.iterrows():
        key = tuple(base_row[k] for k in dst_keys)
        match = df_ref[ref_keys == key]
        if match.empty:
            nr = base_row.copy()
            for c in columns:
                nr[f"{ref_sheet_name}=>{c}"] = "-"
            rows.append(nr)
        else:
            for _, ref_row in match.iterrows():
                nr = base_row.copy()
                for c in columns:
                    nr[f"{ref_sheet_name}=>{c}"] = ref_row[c]
                rows.append(nr)
    return pd.DataFrame(rows).reset_index(drop=True)


def _multiply(df_base, df_ref, key_compare="exact"):
    return main_impl.add_fields_to_sheet(
        df_base.copy(), df_ref, ["CODE"], ["CODE"], ["VAL", "EXTRA"], "DST", "SRC",
        multiply_rows=True, key_compare=key_compare,
    )


def test_multiply_rows_same_as_legacy() -> None:
    """BUG-08: 1→N, 0 совпадений, порядок строк приёмника и совпадений источника."""
    base = pd.DataFrame({"CODE": ["B", "A", "Z", "B"], "N": [1, 2, 3, 4]})
    ref = pd.DataFrame({"CODE": ["A", "B", "B", "C"], "VAL": ["a1", "b1", "b2", "c"], "EXTRA": [1, 2, 3, 4]})
    new = _multiply(base, ref)
    old = _legacy_multiply(base, ref, ["CODE"], ["CODE"], ["VAL", "EXTRA"], "SRC")
    pd.testing.assert_frame_equal(new, old, check_dtype=False)
    assert new["SRC=>VAL"].tolist() == ["b1", "b2", "a1", "-", "b1", "b2"]


def test_multiply_rows_as_text_keys_and_empty_base() -> None:
    base = pd.DataFrame({"CODE": [18, "7"]})
    ref = pd.DataFrame({"CODE": ["18", "18.0", 7], "VAL": ["x", "y", "z"], "EXTRA": [0, 0, 0]})
    out = _multiply(base, ref, key_compare="as_text")
    assert out["SRC=>VAL"].tolist() == ["x", "y", "z"]
    empty = _multiply(pd.DataFrame({"CODE": pd.Series([], dtype=object)}), ref)
    assert list(empty.columns) == ["CODE", "SRC=>VAL", "SRC=>EXTRA"] and empty.empty
