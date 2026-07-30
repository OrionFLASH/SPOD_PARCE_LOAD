# -*- coding: utf-8 -*-
"""Тесты: регистр колонок, пустой фильтр merge, тихий парсинг дат."""

from __future__ import annotations

import logging
import warnings

import pandas as pd

from src.csv_headers import align_dataframe_columns, resolve_columns_in_dataframe
from src.main_impl import (
    _QuietExpectedMergeConsoleFilter,
    _parse_date_column_to_date,
    add_fields_to_sheet,
    calculate_tournament_status,
)


def test_resolve_columns_case_insensitive() -> None:
    df = pd.DataFrame({"calc_type": [1], "TOURNAMENT_CODE": ["T1"]})
    resolved, missing = resolve_columns_in_dataframe(df, ["CALC_TYPE", "TOURNAMENT_CODE"])
    assert missing == []
    assert resolved[0] == "calc_type"


def test_resolve_columns_merge_suffix_case() -> None:
    """SUMMARY ищет CALC_TYPE, на schedule есть CONTEST-DATA=>calc_type."""
    df = pd.DataFrame({"TOURNAMENT_CODE": ["T1"], "CONTEST-DATA=>calc_type": [1]})
    resolved, missing = resolve_columns_in_dataframe(df, ["CALC_TYPE"])
    assert missing == []
    assert resolved == ["CONTEST-DATA=>calc_type"]


def test_align_dataframe_columns_renames_case() -> None:
    df = pd.DataFrame({"calc_type": [0, 1], "contest_code": ["C1", "C2"]})
    aligned, missing, renames = align_dataframe_columns(df, ["CALC_TYPE", "CONTEST_CODE"])
    assert missing == []
    assert "CALC_TYPE" in aligned.columns
    assert "CONTEST_CODE" in aligned.columns
    assert ("calc_type", "CALC_TYPE") in renames
    assert ("contest_code", "CONTEST_CODE") in renames


def test_align_copies_prefixed_calc_type() -> None:
    df = pd.DataFrame({"TOURNAMENT_CODE": ["T1"], "INDICATOR=>calc_type": [2]})
    aligned, missing, renames = align_dataframe_columns(df, ["CALC_TYPE"])
    assert missing == []
    assert "CALC_TYPE" in aligned.columns
    assert list(aligned["CALC_TYPE"]) == [2]
    assert ("INDICATOR=>calc_type", "CALC_TYPE") in renames


def test_add_fields_finds_lowercase_calc_type(caplog) -> None:
    """SUMMARY тянет CALC_TYPE; во входе колонка calc_type — не должно быть WARNING."""
    df_base = pd.DataFrame({"TOURNAMENT_CODE": ["T1", "T2"]})
    df_ref = pd.DataFrame(
        {
            "TOURNAMENT_CODE": ["T1", "T2"],
            "calc_type": [0, 1],
            "START_DT": ["2026-01-01", "2026-02-01"],
        }
    )
    with caplog.at_level(logging.WARNING):
        out = add_fields_to_sheet(
            df_base,
            df_ref,
            src_keys=["TOURNAMENT_CODE"],
            dst_keys=["TOURNAMENT_CODE"],
            columns=["CALC_TYPE", "START_DT"],
            sheet_name="SUMMARY",
            ref_sheet_name="TOURNAMENT-SCHEDULE",
            mode="value",
        )
    warn_msgs = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert not any("CALC_TYPE не найдена" in m for m in warn_msgs)
    assert "TOURNAMENT-SCHEDULE=>CALC_TYPE" in out.columns
    assert list(out["TOURNAMENT-SCHEDULE=>CALC_TYPE"]) == [0, 1]


def test_add_fields_finds_prefixed_lowercase_calc_type(caplog) -> None:
    df_base = pd.DataFrame({"TOURNAMENT_CODE": ["T1"]})
    df_ref = pd.DataFrame(
        {
            "TOURNAMENT_CODE": ["T1"],
            "CONTEST-DATA=>calc_type": [7],
        }
    )
    with caplog.at_level(logging.WARNING):
        out = add_fields_to_sheet(
            df_base,
            df_ref,
            src_keys=["TOURNAMENT_CODE"],
            dst_keys=["TOURNAMENT_CODE"],
            columns=["CALC_TYPE"],
            sheet_name="SUMMARY",
            ref_sheet_name="TOURNAMENT-SCHEDULE",
            mode="value",
        )
    warn_msgs = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert not any("CALC_TYPE не найдена" in m for m in warn_msgs)
    assert list(out["TOURNAMENT-SCHEDULE=>CALC_TYPE"]) == [7]


def test_add_fields_empty_after_filter_is_info_not_warning(caplog) -> None:
    """После фильтра 0 строк — не WARNING «лист пустой»."""
    df_base = pd.DataFrame({"CONTEST_CODE": ["C1"]})
    df_ref = pd.DataFrame()  # пустой после фильтра
    with caplog.at_level(logging.DEBUG):
        out = add_fields_to_sheet(
            df_base,
            df_ref,
            src_keys=["CONTEST_CODE"],
            dst_keys=["CONTEST_CODE"],
            columns=["TOURNAMENT_CODE"],
            sheet_name="CONTEST-DATA",
            ref_sheet_name="TOURNAMENT-SCHEDULE",
            mode="count",
            count_aggregation="nunique",
            count_label="ACTIVE",
            source_rows_before_filter=292,
            applied_filters={"TOURNAMENT_STATUS": ["АКТИВНЫЙ"]},
        )
    msgs = [r.getMessage() for r in caplog.records]
    warn_msgs = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert not any("пустой или None" in m for m in warn_msgs)
    assert not any("Лист TOURNAMENT-SCHEDULE пустой" in m for m in msgs)
    col = "TOURNAMENT-SCHEDULE=>COUNT_nunique_ACTIVE"
    assert col in out.columns
    assert int(out[col].iloc[0]) == 0


def test_quiet_console_filter_skips_expected_merge_info() -> None:
    filt = _QuietExpectedMergeConsoleFilter()
    rec_ok = logging.LogRecord("n", logging.INFO, __file__, 1, "обычное сообщение", (), None)
    rec_skip = logging.LogRecord(
        "n",
        logging.INFO,
        __file__,
        1,
        "[add_fields_to_sheet] Не ошибка: после фильтрации листа-источника «TOURNAMENT-SCHEDULE»",
        (),
        None,
    )
    rec_warn = logging.LogRecord("n", logging.WARNING, __file__, 1, "реальная проблема", (), None)
    assert filt.filter(rec_ok) is True
    assert filt.filter(rec_skip) is False
    assert filt.filter(rec_warn) is True


def test_parse_date_column_no_userwarning() -> None:
    s = pd.Series(["2025-09-01", "4000-01-01", "", "2026-03-31"])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        parsed = _parse_date_column_to_date(s, "END_DT")
    user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
    assert user_warnings == []
    assert str(parsed.iloc[0]) == "2025-09-01"
    assert str(parsed.iloc[1]) == "4000-01-01"
    assert pd.isna(parsed.iloc[2])


def test_calculate_tournament_status_no_userwarning() -> None:
    df = pd.DataFrame(
        {
            "TOURNAMENT_CODE": ["T1"],
            "START_DT": ["2026-01-01"],
            "END_DT": ["2026-12-31"],
            "RESULT_DT": [""],
        }
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = calculate_tournament_status(df)
    user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
    assert user_warnings == []
    assert "CALC_TOURNAMENT_STATUS" in out.columns
