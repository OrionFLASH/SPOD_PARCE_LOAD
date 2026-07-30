# -*- coding: utf-8 -*-
"""
Расчёт статуса турнира по датам (START_DT, END_DT, RESULT_DT) и отчётам (CONTEST_DATE).
"""

import logging
import time
import warnings
from typing import Optional

import pandas as pd

from src.config_loader import Config


def _parse_date_column_to_date(series: pd.Series, col_label: str = "") -> pd.Series:
    """
    Парсит колонку дат в ``datetime.date`` без шумного pandas UserWarning в консоли.
    """
    from datetime import date as date_cls
    from datetime import datetime as dt_cls
    from typing import Any

    label = col_label or str(getattr(series, "name", "") or "date")
    empty_markers = {"", "-", "None", "null", "nan", "NaT", "<NA>"}

    def _one(val: Any):
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(val, dt_cls):
            return val.date()
        if isinstance(val, date_cls):
            return val
        if isinstance(val, pd.Timestamp):
            return None if pd.isna(val) else val.date()
        s = str(val).strip()
        if s in empty_markers or s.lower() == "nat":
            return None
        try:
            return dt_cls.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return False

    primary = series.map(_one)
    need_fallback = primary.apply(lambda x: x is False)
    n_fallback = int(need_fallback.sum())
    out = primary.map(lambda x: None if x is False else x)

    if n_fallback > 0:
        logging.debug(
            f"[DATE] {label}: {n_fallback} значений не в формате YYYY-MM-DD — "
            f"повторный разбор (без UserWarning в консоли)"
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            fb = pd.to_datetime(series[need_fallback], errors="coerce")

        def _ts_to_date(v: Any):
            try:
                if v is None or pd.isna(v):
                    return None
            except (TypeError, ValueError):
                return None
            if isinstance(v, pd.Timestamp):
                return v.date()
            return None

        out.loc[need_fallback] = fb.map(_ts_to_date).values
    return out


def calculate_tournament_status(
    config: Config,
    df_tournament: pd.DataFrame,
    df_report: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Вычисляет статус турнира на основе текущей даты и дат турнира.
    Добавляет колонку CALC_TOURNAMENT_STATUS.
    """
    func_start = time.time()
    params = "(TOURNAMENT-SCHEDULE status calculation)"
    logging.info(f"[START] calculate_tournament_status {params}")

    today = pd.Timestamp.now().date()
    df = df_tournament.copy()

    df["START_DT_parsed"] = _parse_date_column_to_date(df["START_DT"], "START_DT")
    df["END_DT_parsed"] = _parse_date_column_to_date(df["END_DT"], "END_DT")
    df["RESULT_DT_parsed"] = _parse_date_column_to_date(df["RESULT_DT"], "RESULT_DT")

    max_contest_dates = {}
    if df_report is not None and "CONTEST_DATE" in df_report.columns and "TOURNAMENT_CODE" in df_report.columns:
        df_report_dates = df_report.copy()
        df_report_dates["CONTEST_DATE_parsed"] = _parse_date_column_to_date(
            df_report_dates["CONTEST_DATE"], "CONTEST_DATE"
        )
        df_report_dates = df_report_dates.dropna(
            subset=["CONTEST_DATE_parsed", "TOURNAMENT_CODE"]
        )
        if not df_report_dates.empty:
            max_contest_dates = (
                df_report_dates.groupby("TOURNAMENT_CODE")["CONTEST_DATE_parsed"]
                .max()
                .to_dict()
            )

    if max_contest_dates:
        df["MAX_CONTEST_DATE"] = df["TOURNAMENT_CODE"].map(max_contest_dates)
    else:
        df["MAX_CONTEST_DATE"] = None

    choices_list = config.tournament_status_choices
    conditions = [
        pd.isna(df["START_DT_parsed"]) | pd.isna(df["END_DT_parsed"]),
        (df["START_DT_parsed"] <= today) & (today <= df["END_DT_parsed"]),
        today < df["START_DT_parsed"],
        (today > df["END_DT_parsed"])
        & (pd.isna(df["RESULT_DT_parsed"]) | (today < df["RESULT_DT_parsed"])),
        (today > df["END_DT_parsed"])
        & (~pd.isna(df["RESULT_DT_parsed"]))
        & (today >= df["RESULT_DT_parsed"])
        & pd.isna(df["MAX_CONTEST_DATE"]),
        (today > df["END_DT_parsed"])
        & (~pd.isna(df["RESULT_DT_parsed"]))
        & (today >= df["RESULT_DT_parsed"])
        & (~pd.isna(df["MAX_CONTEST_DATE"]))
        & (df["MAX_CONTEST_DATE"] < df["RESULT_DT_parsed"]),
        (today > df["END_DT_parsed"])
        & (~pd.isna(df["RESULT_DT_parsed"]))
        & (today >= df["RESULT_DT_parsed"])
        & (~pd.isna(df["MAX_CONTEST_DATE"]))
        & (df["MAX_CONTEST_DATE"] >= df["RESULT_DT_parsed"]),
    ]
    choices = (
        choices_list
        if len(choices_list) >= len(conditions)
        else (choices_list + ["НЕОПРЕДЕЛЕН"] * (len(conditions) - len(choices_list)))
    )[: len(conditions)]
    default_label = choices_list[0] if choices_list else "НЕОПРЕДЕЛЕН"

    try:
        import numpy as np

        df["CALC_TOURNAMENT_STATUS"] = np.select(conditions, choices, default=default_label)
    except ImportError:
        df["CALC_TOURNAMENT_STATUS"] = pd.Series(default_label, index=df.index)
        for i, (cond, choice) in enumerate(zip(conditions, choices)):
            df.loc[cond, "CALC_TOURNAMENT_STATUS"] = choice

    df = df.drop(
        columns=["START_DT_parsed", "END_DT_parsed", "RESULT_DT_parsed", "MAX_CONTEST_DATE"]
    )

    status_counts = df["CALC_TOURNAMENT_STATUS"].value_counts()
    logging.info(f"[TOURNAMENT STATUS] Статистика: {status_counts.to_dict()}")

    func_time = time.time() - func_start
    logging.info(f"[END] calculate_tournament_status {params} (время: {func_time:.3f}s)")
    return df
