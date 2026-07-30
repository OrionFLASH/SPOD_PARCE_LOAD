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

    Учитывает:
    - регистр (``calc_type`` ↔ ``CALC_TYPE``);
    - BOM/пробелы через ``normalize_csv_column_header``;
    - колонки после merge вида ``CONTEST-DATA=>CALC_TYPE`` / ``INDICATOR=>calc_type``
      (сопоставление по суффиксу после ``=>``).

    Возвращает (реальные_имена_в_df, логические_имена_без_пары).
    """
    index: dict[str, str] = {}
    for col in df.columns:
        key = normalize_csv_column_header(col).casefold()
        if key not in index:
            index[key] = str(col)

    def _leaf_name(col_name: str) -> str:
        s = str(col_name)
        if "=>" in s:
            return s.split("=>")[-1].strip()
        return s

    resolved: List[str] = []
    missing: List[str] = []
    for want in logical_names:
        wn = normalize_csv_column_header(want).casefold()
        if wn in index:
            resolved.append(index[wn])
            continue
        if want in df.columns:
            resolved.append(str(want))
            continue
        # Суффикс после «=>» (merge-колонки), без учёта регистра
        candidates: List[str] = []
        for col in df.columns:
            leaf = normalize_csv_column_header(_leaf_name(str(col))).casefold()
            if leaf == wn:
                candidates.append(str(col))
        if candidates:
            bare = [c for c in candidates if "=>" not in c]
            resolved.append(bare[0] if bare else candidates[0])
        else:
            missing.append(str(want))
    return resolved, missing


def align_dataframe_columns(
    df: pd.DataFrame,
    logical_names: Sequence[str],
) -> tuple[pd.DataFrame, List[str], List[tuple[str, str]]]:
    """
    Приводит столбцы DataFrame к именам из config:

    - ``calc_type`` → переименование в ``CALC_TYPE``;
    - ``CONTEST-DATA=>calc_type`` → копия в ``CALC_TYPE`` (исходная колонка сохраняется).

    Returns:
        (df, missing, renames): missing — логические имена без пары;
        renames — список (фактическое_имя, логическое_имя).
    """
    if df is None or not isinstance(df, pd.DataFrame):
        return df, [str(x) for x in logical_names], []
    resolved, missing = resolve_columns_in_dataframe(df, logical_names)
    rename_map: dict[str, str] = {}
    renames: List[tuple[str, str]] = []
    want_ok = [w for w in logical_names if str(w) not in missing]
    for want, actual in zip(want_ok, resolved):
        want_s = str(want)
        actual_s = str(actual)
        if actual_s == want_s:
            continue
        if want_s in df.columns:
            # Уже есть целевое имя — не трогаем
            continue
        if "=>" in actual_s:
            # Merge-колонка: копируем под ожидаемым именем из config
            df = df.copy()
            df[want_s] = df[actual_s]
            renames.append((actual_s, want_s))
        elif actual_s not in rename_map:
            rename_map[actual_s] = want_s
            renames.append((actual_s, want_s))
    if rename_map:
        df = df.rename(columns=rename_map)
    return df, missing, renames
