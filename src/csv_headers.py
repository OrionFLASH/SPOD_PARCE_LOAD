# -*- coding: utf-8 -*-
"""
Нормализация имён колонок CSV: BOM, Unicode-пробелы, регистр для сопоставления.
"""

from __future__ import annotations

import unicodedata
from typing import Any, List, Optional, Sequence

import pandas as pd


def normalize_csv_column_header(name: Any) -> str:
    """
    Имя заголовка после чтения CSV: снять BOM (U+FEFF), NFKC, схлопнуть пробелы.
    """
    s = ("" if name is None else str(name)).strip()
    if s.startswith("\ufeff"):
        s = s.lstrip("\ufeff").strip()
    s = unicodedata.normalize("NFKC", s)
    return " ".join(s.split())


def normalize_dataframe_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Переименовать столбцы DataFrame по normalize_csv_column_header."""
    if df is None or df.empty and len(df.columns) == 0:
        return df
    mapping = {c: normalize_csv_column_header(c) for c in df.columns}
    return df.rename(columns=mapping)


def resolve_columns_in_dataframe(
    df: pd.DataFrame,
    logical_names: Sequence[str],
) -> tuple[List[str], List[str]]:
    """
    Сопоставить имена из config с фактическими столбцами DataFrame.
    Возвращает (реальные_имена_в_df, логические_имена_без_пары).
    """
    index: dict[str, str] = {}
    for col in df.columns:
        key = normalize_csv_column_header(col).casefold()
        if key not in index:
            index[key] = str(col)
    resolved: List[str] = []
    missing: List[str] = []
    for want in logical_names:
        wn = normalize_csv_column_header(want).casefold()
        if wn in index:
            resolved.append(index[wn])
        elif want in df.columns:
            resolved.append(str(want))
        else:
            missing.append(str(want))
    return resolved, missing


def align_dataframe_columns(
    df: pd.DataFrame,
    logical_names: Sequence[str],
) -> tuple[pd.DataFrame, List[str], List[tuple[str, str]]]:
    """
    Переименовывает столбцы DataFrame к именам из config при совпадении без учёта регистра
    (например ``calc_type`` → ``CALC_TYPE``).

    Returns:
        (df, missing, renames): missing — логические имена без пары;
        renames — список (старое_имя, новое_имя).
    """
    if df is None or not isinstance(df, pd.DataFrame):
        return df, [str(x) for x in logical_names], []
    resolved, missing = resolve_columns_in_dataframe(df, logical_names)
    rename_map: dict[str, str] = {}
    renames: List[tuple[str, str]] = []
    # resolved соответствует logical_names без missing — восстанавливаем пары
    want_ok = [w for w in logical_names if str(w) not in missing]
    for want, actual in zip(want_ok, resolved):
        want_s = str(want)
        actual_s = str(actual)
        if actual_s != want_s and actual_s not in rename_map:
            # не перезаписываем уже существующую целевую колонку
            if want_s in df.columns and actual_s != want_s:
                continue
            rename_map[actual_s] = want_s
            renames.append((actual_s, want_s))
    if rename_map:
        df = df.rename(columns=rename_map)
    return df, missing, renames
