# -*- coding: utf-8 -*-
"""
Расчёт статуса турнира по датам (START_DT, END_DT, RESULT_DT) и отчётам (CONTEST_DATE).
"""

import logging
import time
from typing import Optional

import pandas as pd

from src.config_loader import Config


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

    df["START_DT_parsed"] = pd.to_datetime(df["START_DT"], errors="coerce").dt.date
    df["END_DT_parsed"] = pd.to_datetime(df["END_DT"], errors="coerce").dt.date
    df["RESULT_DT_parsed"] = pd.to_datetime(df["RESULT_DT"], errors="coerce").dt.date

    max_contest_dates = {}
    if df_report is not None and "CONTEST_DATE" in df_report.columns and "TOURNAMENT_CODE" in df_report.columns:
        df_report_dates = df_report.copy()
        df_report_dates["CONTEST_DATE_parsed"] = (
            pd.to_datetime(df_report_dates["CONTEST_DATE"], errors="coerce").dt.date
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
