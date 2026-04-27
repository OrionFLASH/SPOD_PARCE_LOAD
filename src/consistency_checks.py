# -*- coding: utf-8 -*-
"""
Модуль проверок консистентности данных.
Выполняет правила из конфига consistency_checks: создаёт колонки unique (ДУБЛЬ: …, опционально область unique_scope_*
и unique_require_non_empty), field_length, field_format, json-проверки и json_spod_format на листах, затем referential/referential_composite,
собирает результаты в свод CONSISTENCY.
Результаты выводятся в колонки на листах, сводный лист CONSISTENCY, консоль и лог.
Тип json_priority_unique_per_contest_link: уникальность ключа JSON (например priority) среди REWARD_CODE
с одним CONTEST_CODE по REWARD-LINK; парсинг ADD_DATA через _parse_add_data_cell_with_normalized.
"""

import json
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from src.debug_timing import debug_timed


def _get_sheet_df(sheets_data: Dict[str, Any], sheet_name: str) -> Optional[pd.DataFrame]:
    """Возвращает DataFrame листа или None."""
    if sheet_name not in sheets_data or sheets_data[sheet_name] is None:
        return None
    item = sheets_data[sheet_name]
    if not isinstance(item, (list, tuple)) or len(item) < 1:
        return None
    df = item[0]
    return df if isinstance(df, pd.DataFrame) else None


def _get_sheet_item(sheets_data: Dict[str, Any], sheet_name: str) -> Optional[Tuple[pd.DataFrame, Any]]:
    """Возвращает (df, conf) для листа или None."""
    if sheet_name not in sheets_data or sheets_data[sheet_name] is None:
        return None
    item = sheets_data[sheet_name]
    if not isinstance(item, (list, tuple)) or len(item) < 2:
        return None
    df, conf = item[0], item[1]
    if not isinstance(df, pd.DataFrame):
        return None
    return (df, conf)


def _excel_row(idx: int) -> int:
    """Номер строки в Excel: строка 1 — заголовок, первая строка данных = 2."""
    return int(idx) + 2


# Максимум записей в sample для свода CONSISTENCY (унифицированный формат)
_MAX_SAMPLE = 20


def _unique_cell_is_empty(val: Any) -> bool:
    """
    True, если ячейку для правила unique_require_non_empty считаем пустой
    (строка не участвует в проверке уникальности).
    Согласовано с field_length: пусто, прочерк, None/null как строка.
    """
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    s = str(val).strip()
    if s == "" or s in ("-", "None", "null"):
        return True
    return False


def _unique_cell_compare_str(val: Any) -> str:
    """Строка для сравнения в условиях области unique_scope (после нормализации)."""
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    return str(val).strip()


def _normalize_unique_scope_conditions(rule: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Собирает список пар (колонка, ожидаемое значение) для ограничения области проверки unique.
    Сначала unique_scope_conditions (массив объектов с ключами column/value),
    иначе устаревшая пара unique_scope_column + unique_scope_value.
    """
    out: List[Tuple[str, str]] = []
    raw = rule.get("unique_scope_conditions")
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            col = item.get("column") or item.get("col") or ""
            val = item.get("value")
            col = str(col).strip()
            if not col:
                continue
            out.append((col, "" if val is None else str(val)))
    if not out:
        leg_col = rule.get("unique_scope_column")
        if leg_col is not None and str(leg_col).strip():
            leg_val = rule.get("unique_scope_value")
            out.append(
                (str(leg_col).strip(), "" if leg_val is None else str(leg_val))
            )
    return out


def _unique_scope_mode(rule: Dict[str, Any]) -> str:
    """Режим объединения условий области: all (И) или any (ИЛИ)."""
    m = str(rule.get("unique_scope_mode", "all")).strip().lower()
    if m in ("any", "or", "или"):
        return "any"
    return "all"


def _unique_scope_mask(
    df: pd.DataFrame,
    conditions: List[Tuple[str, str]],
    mode: str,
    sheet_hint: str = "",
) -> pd.Series:
    """
    Маска строк, попадающих в область по условиям column==value.
    Пустой список conditions — все строки (True).
    """
    if not conditions:
        return pd.Series(True, index=df.index)
    masks: List[pd.Series] = []
    for col, expected in conditions:
        exp = str(expected).strip() if expected is not None else ""
        if col not in df.columns:
            logging.warning(
                f"[consistency] unique: колонка области «{col}» отсутствует на листе {sheet_hint or '?'}"
            )
            masks.append(pd.Series(False, index=df.index))
            continue
        eq = df[col].map(_unique_cell_compare_str) == exp
        masks.append(eq)
    if not masks:
        return pd.Series(True, index=df.index)
    if mode == "any":
        combined = masks[0].copy()
        for m in masks[1:]:
            combined = combined | m
        return combined
    combined = masks[0].copy()
    for m in masks[1:]:
        combined = combined & m
    return combined


def _unique_require_non_empty_mask(df: pd.DataFrame, rule: Dict[str, Any], sheet_hint: str = "") -> pd.Series:
    """
    Маска строк, у которых все колонки из unique_require_non_empty непустые.
    Пустой список в правиле — все строки участвуют (True).
    """
    cols = rule.get("unique_require_non_empty") or []
    if not cols:
        return pd.Series(True, index=df.index)
    combined = pd.Series(True, index=df.index)
    for col in cols:
        if col not in df.columns:
            logging.warning(
                f"[consistency] unique: unique_require_non_empty, колонка «{col}» отсутствует на {sheet_hint or '?'}"
            )
            combined = pd.Series(False, index=df.index)
            break
        combined = combined & ~df[col].map(_unique_cell_is_empty)
    return combined


def _unique_active_row_mask(df: pd.DataFrame, rule: Dict[str, Any]) -> pd.Series:
    """
    Строки листа, для которых выполняется проверка уникальности:
    область unique_scope (И/ИЛИ) и непустота колонок unique_require_non_empty.
    """
    sheet_hint = str(rule.get("sheet", ""))
    conds = _normalize_unique_scope_conditions(rule)
    mode = _unique_scope_mode(rule)
    scope_m = _unique_scope_mask(df, conds, mode, sheet_hint=sheet_hint)
    nonempty_m = _unique_require_non_empty_mask(df, rule, sheet_hint=sheet_hint)
    return scope_m & nonempty_m


def _referential_row_conditions_mask(
    df: pd.DataFrame,
    conditions: Optional[List[Dict[str, Any]]],
    sheet_hint: str = "",
) -> pd.Series:
    """
    Маска строк листа по списку условий (логическое И).
    Элемент: column, op (=, ==, eq, <>, !=, ne), value (строковое сравнение после strip).
    Пустой список или None — все строки True.
    """
    if not conditions:
        return pd.Series(True, index=df.index)
    m = pd.Series(True, index=df.index)
    for c in conditions:
        if not isinstance(c, dict):
            continue
        col = str(c.get("column", "")).strip()
        op = str(c.get("op", "=")).strip().lower()
        val = c.get("value")
        expected = "" if val is None else str(val)
        if not col:
            continue
        if col not in df.columns:
            logging.warning(
                f"[consistency] referential: колонка фильтра «{col}» отсутствует на листе {sheet_hint or '?'}"
            )
            return pd.Series(False, index=df.index)

        def _cell_str(x: Any) -> str:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return ""
            return str(x).strip()

        colvals = df[col].map(_cell_str)
        if op in ("=", "==", "eq"):
            m &= colvals == expected
        elif op in ("<>", "!=", "ne"):
            m &= colvals != expected
        else:
            logging.warning(
                f"[consistency] referential: неизвестный op «{op}» для колонки «{col}», условие пропущено"
            )
    return m


def run_referential(
    sheets_data: Dict[str, Any],
    rule: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Проверка типа referential: значения column_src на sheet_src должны быть в sheet_ref.column_ref.
    Записывает колонку результата на sheet_src. Возвращает запись для сводки.
    """
    sheet_src = rule.get("sheet_src")
    column_src = rule.get("column_src")
    sheet_ref = rule.get("sheet_ref")
    column_ref = rule.get("column_ref")
    output = rule.get("output") or {}
    col_out = output.get("column_on_sheet") or "ПРОВЕРКА"
    check_id = rule.get("id", "")

    df_src = _get_sheet_df(sheets_data, sheet_src)
    df_ref = _get_sheet_df(sheets_data, sheet_ref)
    if df_src is None:
        logging.debug(f"[consistency] referential {check_id}: лист {sheet_src} отсутствует, пропуск")
        return {"check_id": check_id, "sheet": sheet_src, "name": rule.get("name", ""), "column_on_sheet": col_out, "type": "referential", "total_rows": 0, "violations": 0, "sample": [], "include_in_summary": True}
    if df_ref is None:
        logging.debug(f"[consistency] referential {check_id}: справочник {sheet_ref} отсутствует, помечаем все как нарушение")
    if column_src not in df_src.columns:
        logging.warning(f"[consistency] referential {check_id}: колонка {column_src} не найдена на {sheet_src}")
        return {"check_id": check_id, "sheet": sheet_src, "name": rule.get("name", ""), "column_on_sheet": col_out, "type": "referential", "total_rows": len(df_src), "violations": len(df_src), "sample": [], "include_in_summary": True}

    src_conds = rule.get("src_row_conditions") or rule.get("sheet_src_row_conditions")
    ref_conds = rule.get("ref_row_conditions") or rule.get("sheet_ref_row_conditions")
    src_mask = _referential_row_conditions_mask(df_src, src_conds, str(sheet_src or ""))

    ref_set = set()
    if df_ref is not None and column_ref in df_ref.columns:
        ref_mask = _referential_row_conditions_mask(df_ref, ref_conds, str(sheet_ref or ""))
        ref_sub = df_ref.loc[ref_mask]
        ref_set = set(ref_sub[column_ref].astype(str).str.strip())

    def _status(val: Any) -> str:
        s = str(val).strip() if pd.notna(val) else ""
        if s == "":
            return "OK"
        return "OK" if s in ref_set else f"НЕТ в {sheet_ref}"

    checked = df_src[column_src].map(_status)
    results = checked.where(src_mask, other="—")
    total = len(df_src)
    violations_mask = src_mask & (checked != "OK")
    n_violations = int(violations_mask.sum())
    sample: List[str] = []
    if n_violations > 0:
        vio_idx = df_src.index[violations_mask].tolist()[: _MAX_SAMPLE]
        for idx in vio_idx:
            val = df_src.loc[idx, column_src]
            v = "" if pd.isna(val) else str(val).strip()[:50]
            sample.append(f"[{_excel_row(idx)}] {v}")
    # Записываем колонку на лист (модифицируем sheets_data)
    item = _get_sheet_item(sheets_data, sheet_src)
    if item is not None:
        df, conf = item
        df = df.copy()
        df[col_out] = results.values
        sheets_data[sheet_src] = (df, conf)
        logging.debug(f"[consistency] referential {check_id}: записана колонка {col_out} на {sheet_src}")

    return {
        "check_id": check_id,
        "sheet": sheet_src,
        "name": rule.get("name", ""),
        "column_on_sheet": col_out,
        "type": "referential",
        "total_rows": total,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


def run_referential_composite(
    sheets_data: Dict[str, Any],
    rule: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Проверка типа referential_composite: комбинация columns_src на sheet_src должна встречаться в sheet_ref (columns_ref).
    Записывает колонку результата на sheet_src.
    """
    sheet_src = rule.get("sheet_src")
    columns_src = rule.get("columns_src") or []
    sheet_ref = rule.get("sheet_ref")
    columns_ref = rule.get("columns_ref") or []
    output = rule.get("output") or {}
    col_out = output.get("column_on_sheet") or "ПРОВЕРКА"
    check_id = rule.get("id", "")

    df_src = _get_sheet_df(sheets_data, sheet_src)
    df_ref = _get_sheet_df(sheets_data, sheet_ref)
    if df_src is None:
        return {"check_id": check_id, "sheet": sheet_src, "name": rule.get("name", ""), "column_on_sheet": col_out, "type": "referential_composite", "total_rows": 0, "violations": 0, "sample": [], "include_in_summary": True}
    missing_src = [c for c in columns_src if c not in df_src.columns]
    if missing_src:
        logging.warning(f"[consistency] referential_composite {check_id}: колонки {missing_src} не найдены на {sheet_src}")
        return {"check_id": check_id, "sheet": sheet_src, "name": rule.get("name", ""), "column_on_sheet": col_out, "type": "referential_composite", "total_rows": len(df_src), "violations": 0, "sample": [], "include_in_summary": True}

    src_conds = rule.get("src_row_conditions") or rule.get("sheet_src_row_conditions")
    ref_conds = rule.get("ref_row_conditions") or rule.get("sheet_ref_row_conditions")
    src_mask = _referential_row_conditions_mask(df_src, src_conds, str(sheet_src or ""))

    ref_set = set()
    if df_ref is not None:
        missing_ref = [c for c in columns_ref if c not in df_ref.columns]
        if not missing_ref:
            ref_mask = _referential_row_conditions_mask(df_ref, ref_conds, str(sheet_ref or ""))
            ref_sub = df_ref.loc[ref_mask]
            for _, row in ref_sub[columns_ref].iterrows():
                t = tuple(str(row[c]).strip() if pd.notna(row[c]) else "" for c in columns_ref)
                ref_set.add(t)

    def _row_status(row: pd.Series) -> str:
        t = tuple(str(row[c]).strip() if pd.notna(row[c]) else "" for c in columns_src)
        return "OK" if t in ref_set else f"НЕТ в {sheet_ref}"

    checked = df_src[columns_src].apply(_row_status, axis=1)
    results = checked.where(src_mask, other="—")
    total = len(df_src)
    violations_mask = src_mask & (checked != "OK")
    n_violations = int(violations_mask.sum())
    sample: List[str] = []
    if n_violations > 0:
        vio_idx = df_src.index[violations_mask].tolist()[: _MAX_SAMPLE]
        for idx in vio_idx:
            row = df_src.loc[idx, columns_src]
            parts = [str(row[c]).strip()[:30] for c in columns_src]
            sample.append(f"[{_excel_row(idx)}] {','.join(parts)}")

    item = _get_sheet_item(sheets_data, sheet_src)
    if item is not None:
        df, conf = item
        df = df.copy()
        df[col_out] = results.values
        sheets_data[sheet_src] = (df, conf)

    return {
        "check_id": check_id,
        "sheet": sheet_src,
        "name": rule.get("name", ""),
        "column_on_sheet": col_out,
        "type": "referential_composite",
        "total_rows": total,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


def _parse_cfg_date(value: Any, date_format: str) -> Optional[datetime]:
    """Парсит дату по формату из конфига (в т.ч. YYYY-MM-DD)."""
    s = str(value).strip() if value is not None and pd.notna(value) else ""
    if s == "":
        return None
    fmt = date_format
    if fmt.upper() == "YYYY-MM-DD":
        fmt = "%Y-%m-%d"
    try:
        return datetime.strptime(s, fmt)
    except (TypeError, ValueError):
        return None


def run_cross_sheet_date_lte_today(
    sheets_data: Dict[str, Any],
    rule: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Проверка: для каждого кода из sheet_src.column_src дата sheet_ref.column_date_ref
    по ключу sheet_ref.column_ref должна быть <= текущей системной дате.
    """
    sheet_src = rule.get("sheet_src")
    column_src = rule.get("column_src")
    sheet_ref = rule.get("sheet_ref")
    column_ref = rule.get("column_ref")
    column_date_ref = rule.get("column_date_ref")
    date_format = str(rule.get("date_format", "YYYY-MM-DD"))
    output = rule.get("output") or {}
    col_out = output.get("column_on_sheet") or f"ПРОВЕРКА: {column_date_ref} <= today"
    check_id = rule.get("id", "")

    df_src = _get_sheet_df(sheets_data, sheet_src)
    df_ref = _get_sheet_df(sheets_data, sheet_ref)
    if df_src is None:
        return {
            "check_id": check_id,
            "sheet": sheet_src,
            "name": rule.get("name", ""),
            "column_on_sheet": col_out,
            "type": "cross_sheet_date_lte_today",
            "total_rows": 0,
            "violations": 0,
            "sample": [],
            "include_in_summary": output.get("include_in_summary", True),
        }

    if column_src not in df_src.columns:
        logging.warning(f"[consistency] cross_sheet_date_lte_today {check_id}: колонка {column_src} не найдена на {sheet_src}")
        return {
            "check_id": check_id,
            "sheet": sheet_src,
            "name": rule.get("name", ""),
            "column_on_sheet": col_out,
            "type": "cross_sheet_date_lte_today",
            "total_rows": len(df_src),
            "violations": len(df_src),
            "sample": [],
            "include_in_summary": output.get("include_in_summary", True),
        }

    src_conds = rule.get("src_row_conditions") or rule.get("sheet_src_row_conditions")
    ref_conds = rule.get("ref_row_conditions") or rule.get("sheet_ref_row_conditions")
    src_mask = _referential_row_conditions_mask(df_src, src_conds, str(sheet_src or ""))

    ref_dates_by_code: Dict[str, List[datetime]] = defaultdict(list)
    ref_parse_error_codes: Set[str] = set()
    ref_empty_date_codes: Set[str] = set()
    if df_ref is not None:
        required_ref = [column_ref, column_date_ref]
        missing_ref = [c for c in required_ref if c not in df_ref.columns]
        if missing_ref:
            logging.warning(f"[consistency] cross_sheet_date_lte_today {check_id}: колонки {missing_ref} не найдены на {sheet_ref}")
        else:
            ref_mask = _referential_row_conditions_mask(df_ref, ref_conds, str(sheet_ref or ""))
            ref_sub = df_ref.loc[ref_mask]
            for _, row in ref_sub[[column_ref, column_date_ref]].iterrows():
                code = str(row[column_ref]).strip() if pd.notna(row[column_ref]) else ""
                if not code:
                    continue
                parsed_dt = _parse_cfg_date(row[column_date_ref], date_format)
                if parsed_dt is None:
                    raw = str(row[column_date_ref]).strip() if pd.notna(row[column_date_ref]) else ""
                    if raw == "":
                        ref_empty_date_codes.add(code)
                    else:
                        ref_parse_error_codes.add(code)
                    continue
                ref_dates_by_code[code].append(parsed_dt)

    today = datetime.now().date()

    def _status(code_val: Any) -> str:
        code = str(code_val).strip() if pd.notna(code_val) else ""
        if code == "":
            return "OK"
        if code in ref_parse_error_codes:
            return f"Некорректная дата {column_date_ref} в {sheet_ref}"
        if code in ref_empty_date_codes and not ref_dates_by_code.get(code):
            return f"Пустая дата {column_date_ref} в {sheet_ref}"
        ref_vals = ref_dates_by_code.get(code) or []
        if not ref_vals:
            return f"НЕТ в {sheet_ref}"
        max_dt = max(ref_vals).date()
        if max_dt <= today:
            return "OK"
        return f"{column_date_ref}:{max_dt.strftime('%Y-%m-%d')}>{today.strftime('%Y-%m-%d')}"

    checked = df_src[column_src].map(_status)
    results = checked.where(src_mask, other="—")
    violations_mask = src_mask & (checked != "OK")
    n_violations = int(violations_mask.sum())
    total = len(df_src)
    sample: List[str] = []
    if n_violations > 0:
        vio_idx = df_src.index[violations_mask].tolist()[: _MAX_SAMPLE]
        for idx in vio_idx:
            code = str(df_src.loc[idx, column_src]).strip() if pd.notna(df_src.loc[idx, column_src]) else ""
            msg = str(checked.loc[idx]).strip()
            sample.append(f"[{_excel_row(idx)}] {code} | {msg}")

    item = _get_sheet_item(sheets_data, sheet_src)
    if item is not None:
        df, conf = item
        df = df.copy()
        df[col_out] = results.values
        sheets_data[sheet_src] = (df, conf)

    return {
        "check_id": check_id,
        "sheet": sheet_src,
        "name": rule.get("name", ""),
        "column_on_sheet": col_out,
        "type": "cross_sheet_date_lte_today",
        "total_rows": total,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


def _run_unique_check(sheets_data: Dict[str, Any], rule: Dict[str, Any]) -> None:
    """
    Создаёт на листе колонку с пометкой дублей по key_columns (значение «xN» или пусто).
    Опционально: unique_scope_conditions + unique_scope_mode (all/any) — только строки, где выполнены
    условия column==value (И или ИЛИ); unique_require_non_empty — строки с пустыми указанными колонками
    в проверку не входят. Устаревшие unique_scope_column / unique_scope_value — одна пара, режим И.
    Обновляет sheets_data на месте. Вызывается до collect_unique_result, чтобы колонка существовала.
    """
    sheet_name = rule.get("sheet")
    key_columns = rule.get("key_columns") or []
    output = rule.get("output") or {}
    col_name = output.get("column_on_sheet") or ("ДУБЛЬ: " + "_".join(key_columns))

    item = _get_sheet_item(sheets_data, sheet_name)
    if item is None:
        return
    df, conf = item
    missing = [c for c in key_columns if c not in df.columns]
    if missing:
        logging.warning(f"[consistency] unique: лист {sheet_name}, отсутствуют колонки {missing}, пропуск")
        return
    try:
        active = _unique_active_row_mask(df, rule)
        result_col = pd.Series("", index=df.index, dtype=object)
        if active.any():
            sub = df.loc[active]
            dup_counts = sub.groupby(key_columns, dropna=False)[key_columns[0]].transform("count")

            def _dup_label(n: Any) -> str:
                k = int(n) if not pd.isna(n) else 0
                return f"x{k}" if k > 1 else ""

            result_col.loc[active] = dup_counts.map(_dup_label).values
        df = df.copy()
        df[col_name] = result_col.values
        sheets_data[sheet_name] = (df, conf)
        logging.debug(
            f"[consistency] unique: лист {sheet_name}, колонка {col_name}, активных строк: {int(active.sum())}"
        )
    except Exception as e:
        logging.error(f"[consistency] Ошибка при создании колонки дублей {sheet_name} по {key_columns}: {e}")


def _run_field_length_check(sheets_data: Dict[str, Any], rule: Dict[str, Any]) -> None:
    """
    Создаёт на листе колонку результата проверки длины полей (FIELD_LENGTH_CHECK и т.д.).
    Правило должно содержать sheet, result_column, fields (имя_поля -> {limit, operator}).
    Обновляет sheets_data на месте. Вызывается до collect_field_length_result.
    """
    sheet_name = rule.get("sheet")
    result_column = rule.get("result_column") or "FIELD_LENGTH_CHECK"
    fields_config = rule.get("fields") or {}

    item = _get_sheet_item(sheets_data, sheet_name)
    if item is None:
        return
    df, conf = item
    missing = [f for f in fields_config if f not in df.columns]
    if missing:
        logging.warning(
            f"[consistency] field_length: лист {sheet_name}, отсутствуют поля {missing}, пропуск"
        )
        return
    if not fields_config:
        return

    violations_dict: Dict[str, Any] = {}
    for field_name, field_cfg in fields_config.items():
        if field_name not in df.columns:
            continue
        limit = field_cfg.get("limit", 0)
        operator = field_cfg.get("operator", "<=")
        lengths = df[field_name].astype(str).str.len()
        empty_mask = (
            df[field_name].isin(["", "-", "None", "null"]) | df[field_name].isna()
        )
        if operator == "<=":
            mask = (lengths > limit) & ~empty_mask
        elif operator == "=":
            mask = (lengths != limit) & ~empty_mask
        elif operator == ">=":
            mask = (lengths < limit) & ~empty_mask
        elif operator == "<":
            mask = (lengths >= limit) & ~empty_mask
        elif operator == ">":
            mask = (lengths <= limit) & ~empty_mask
        else:
            mask = pd.Series(False, index=df.index)
        if mask.any():
            violations_dict[field_name] = pd.Series("", index=df.index, dtype=str)
            violations_dict[field_name].loc[mask] = df.loc[mask, field_name].apply(
                lambda val: f"{field_name} = {len(str(val))} {operator} {limit}"
            )

    df = df.copy()
    if violations_dict:
        violations_df = pd.DataFrame(violations_dict)
        violations_series = violations_df.apply(
            lambda row: "; ".join([str(v) for v in row if v and str(v).strip()]),
            axis=1,
        )
        df[result_column] = violations_series.replace("", "-")
    else:
        df[result_column] = "-"
    sheets_data[sheet_name] = (df, conf)
    logging.debug(
        f"[consistency] field_length: записана колонка {result_column} на {sheet_name}"
    )


def _validate_field_format(value: Any, format_spec: Dict[str, Any]) -> str:
    """
    Проверяет значение на соответствие формату. Возвращает "OK" или строку с описанием ошибки.
    format_spec.type: "date" | "decimal" | "fixed_length_digits".
    - date: date_format ("YYYY-MM-DD" → %Y-%m-%d), allow_empty, special_values (список допустимых строк).
    - decimal: decimal_places (число знаков после точки), allow_empty.
    - fixed_length_digits: length (длина строки из цифр), allow_empty.
    """
    if format_spec is None:
        return "OK"
    fmt_type = format_spec.get("type", "")
    allow_empty = format_spec.get("allow_empty", False)
    s = str(value).strip() if value is not None and pd.notna(value) else ""
    if s == "" or (isinstance(value, float) and pd.isna(value)):
        return "OK" if allow_empty else "Пустое значение"
    if fmt_type == "date":
        special = format_spec.get("special_values") or []
        if s in special:
            return "OK"
        date_fmt = format_spec.get("date_format", "YYYY-MM-DD")
        if date_fmt.upper() == "YYYY-MM-DD":
            date_fmt = "%Y-%m-%d"
        try:
            datetime.strptime(s, date_fmt)
            return "OK"
        except (ValueError, TypeError):
            return f"Не дата формата {date_fmt}"
    if fmt_type == "decimal":
        places = int(format_spec.get("decimal_places", 5))
        pattern = re.compile(r"^-?\d+\.\d{" + str(places) + r"}$")
        if pattern.match(s):
            return "OK"
        try:
            num = float(s)
            if not pd.isna(num):
                formatted = f"{num:.{places}f}"
                if re.match(r"^-?\d+\.\d+$", formatted) and len(formatted.split(".")[1]) == places:
                    return "OK"
            return f"Ожидается формат 0.{'0' * places} (дробная часть {places} знаков)"
        except (ValueError, TypeError):
            return "Не число"
    if fmt_type == "fixed_length_digits":
        # Ровно length цифр (только 0-9), пусто не допускается (если не allow_empty).
        length = int(format_spec.get("length", 20))
        if not s.isdigit():
            return "Ожидаются только цифры"
        if len(s) == length:
            return "OK"
        # Проверяем и меньше, и больше заданной длины (не принимаем короткие с лидирующими нулями как OK)
        if len(s) < length:
            return f"{len(s)} < {length}"
        return f"{len(s)} > {length}"
    return "OK"


def _format_error_to_code(msg: str) -> str:
    """Сокращённый код ошибки формата для единого стиля sample."""
    if not msg or msg == "OK":
        return "OK"
    s = msg.strip()
    if s == "Пустое значение":
        return "пусто"
    if s.startswith("Не дата формата"):
        return "не_дата"
    if "Ожидается формат 0." in s or "дробная часть" in s:
        return "decimal_N"
    if s == "Не число":
        return "не_число"
    if s == "Ожидаются только цифры":
        return "не_цифры"
    if "Ожидается " in s and " цифр, получено " in s:
        return "длина≠L"
    return "формат"


def _run_field_format_check(sheets_data: Dict[str, Any], rule: Dict[str, Any]) -> None:
    """
    Создаёт на листе колонку результата проверки формата поля (field_format).
    Правило: sheet, field, format (type, ...), output.column_on_sheet.
    Обновляет sheets_data на месте.
    """
    sheet_name = rule.get("sheet")
    field_name = rule.get("field")
    format_spec = rule.get("format") or {}
    output = rule.get("output") or {}
    col_out = output.get("column_on_sheet") or f"ПРОВЕРКА ФОРМАТ: {field_name}"
    check_id = rule.get("id", "")

    item = _get_sheet_item(sheets_data, sheet_name)
    if item is None:
        logging.debug(f"[consistency] field_format {check_id}: лист {sheet_name} отсутствует")
        return
    df, conf = item
    if field_name not in df.columns:
        logging.warning(f"[consistency] field_format {check_id}: поле {field_name} не найдено на {sheet_name}")
        return

    results = df[field_name].apply(lambda val: _validate_field_format(val, format_spec))
    df = df.copy()
    df[col_out] = results.values
    sheets_data[sheet_name] = (df, conf)
    logging.debug(f"[consistency] field_format {check_id}: записана колонка {col_out} на {sheet_name}")


def collect_field_format_result(
    sheets_data: Dict[str, Any],
    rule: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Собирает результат проверки field_format по уже заполненной колонке.
    """
    sheet_name = rule.get("sheet")
    output = rule.get("output") or {}
    field_name = rule.get("field", "")
    col_out = output.get("column_on_sheet") or f"ПРОВЕРКА ФОРМАТ: {field_name}"
    check_id = rule.get("id", "")

    df = _get_sheet_df(sheets_data, sheet_name)
    if df is None or col_out not in df.columns:
        return {
            "check_id": check_id,
            "sheet": sheet_name,
            "name": rule.get("name", ""),
            "column_on_sheet": col_out,
            "type": "field_format",
            "total_rows": 0,
            "violations": 0,
            "sample": [],
            "include_in_summary": output.get("include_in_summary", True),
        }

    col_series = df[col_out].astype(str).str.strip()
    violations_mask = col_series != "OK"
    n_violations = int(violations_mask.sum())
    total = len(df)
    sample: List[str] = []
    field_name = rule.get("field", "")
    if n_violations > 0 and field_name and field_name in df.columns:
        vio_idx = df.index[violations_mask].tolist()[: _MAX_SAMPLE]
        for idx in vio_idx:
            raw_val = df.loc[idx, field_name]
            raw_short = "" if pd.isna(raw_val) else str(raw_val).strip()
            msg = col_series.loc[idx]
            # Для формата «N < L» / «N > L» (fixed_length_digits): [N] значение = факт < ожид
            if re.match(r"^\d+\s*[<>]\s*\d+$", msg.strip()):
                val_display = raw_short[:50] if len(raw_short) > 50 else raw_short
                sample.append(f"[{_excel_row(idx)}] {val_display} = {msg.strip()}")
            else:
                code = _format_error_to_code(msg)
                sample.append(f"[{_excel_row(idx)}] {field_name}={raw_short[:25]} | {code}")
    elif n_violations > 0:
        vio_idx = df.index[violations_mask].tolist()[: _MAX_SAMPLE]
        for idx in vio_idx:
            msg = col_series.loc[idx]
            code = _format_error_to_code(msg)
            sample.append(f"[{_excel_row(idx)}] | {code}")

    return {
        "check_id": check_id,
        "sheet": sheet_name,
        "name": rule.get("name", ""),
        "column_on_sheet": col_out,
        "type": "field_format",
        "total_rows": total,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


def collect_unique_result(
    sheets_data: Dict[str, Any],
    rule: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Собирает результат проверки unique по колонке (ДУБЛЬ: ...).
    Колонка создаётся ранее в _run_unique_check.
    """
    sheet_name = rule.get("sheet")
    key_columns = rule.get("key_columns") or []
    output = rule.get("output") or {}
    col_name = output.get("column_on_sheet") or ("ДУБЛЬ: " + "_".join(key_columns))
    check_id = rule.get("id", "")

    df = _get_sheet_df(sheets_data, sheet_name)
    if df is None or col_name not in df.columns:
        return {
            "check_id": check_id,
            "sheet": sheet_name,
            "name": rule.get("name", ""),
            "column_on_sheet": col_name,
            "type": "unique",
            "total_rows": 0,
            "violations": 0,
            "sample": [],
            "include_in_summary": output.get("include_in_summary", True),
        }

    col_series = df[col_name].astype(str).str.strip()
    violations_mask = col_series != ""
    n_violations = int(violations_mask.sum())
    # Число строк, для которых правило реально применялось (область + непустые обязательные колонки)
    active = _unique_active_row_mask(df, rule)
    total = int(active.sum())
    sample = []
    if n_violations > 0 and key_columns and all(c in df.columns for c in key_columns):
        dup_df = df.loc[violations_mask, key_columns + [col_name]].copy()
        dup_df["_row"] = dup_df.index
        grouped = dup_df.groupby(key_columns, dropna=False)
        for key_vals, grp in grouped:
            if len(grp) < 2:
                continue
            vals = key_vals if isinstance(key_vals, tuple) else (key_vals,)
            key_parts = [str(v)[:20] for v in vals]
            key_str = ",".join(key_parts)
            excel_rows = sorted(grp["_row"].astype(int).tolist())
            row_str = ", ".join(str(_excel_row(r)) for r in excel_rows)
            sample.append(f"[{row_str}] {{{key_str}}} ×{len(grp)}")
            if len(sample) >= _MAX_SAMPLE:
                break
    elif n_violations > 0:
        dup_idx = df.index[violations_mask].tolist()[: _MAX_SAMPLE]
        sample = [f"[{_excel_row(i)}]" for i in dup_idx]

    return {
        "check_id": check_id,
        "sheet": sheet_name,
        "name": rule.get("name", ""),
        "column_on_sheet": col_name,
        "type": "unique",
        "total_rows": total,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


def _compact_field_length_cell(cell_text: str) -> str:
    """Сокращает текст ячейки field_length: 'FIELD = 25 <= 20' -> 'FIELD:25>20'."""
    if not cell_text or cell_text.strip() in ("", "-"):
        return ""
    parts = []
    for part in str(cell_text).split(";"):
        part = part.strip()
        if " = " not in part:
            continue
        field, rest = part.split(" = ", 1)
        field = field.strip()
        rest = rest.strip()
        # "25 <= 20" -> "25>20", "3 = 5" -> "3≠5", "1 >= 2" -> "1<2"
        for op, sym in [(" <= ", ">"), (" >= ", "<"), (" = ", "≠"), (" < ", "<"), (" > ", ">")]:
            if op in rest:
                a, b = rest.split(op, 1)
                parts.append(f"{field}:{a.strip()}{sym}{b.strip()}")
                break
        else:
            parts.append(f"{field}:{rest}")
    return "; ".join(parts)


def collect_field_length_result(
    sheets_data: Dict[str, Any],
    rule: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Собирает результат проверки field_length по уже заполненной колонке (FIELD_LENGTH_CHECK и т.д.).
    Текущую реализацию field_length_validations не трогаем — только читаем колонку.
    """
    sheet_name = rule.get("sheet")
    result_column = rule.get("result_column") or "FIELD_LENGTH_CHECK"
    output = rule.get("output") or {}
    col_out = output.get("column_on_sheet") or result_column
    check_id = rule.get("id", "")

    df = _get_sheet_df(sheets_data, sheet_name)
    if df is None or result_column not in df.columns:
        return {
            "check_id": check_id,
            "sheet": sheet_name,
            "name": rule.get("name", ""),
            "column_on_sheet": col_out,
            "type": "field_length",
            "total_rows": 0,
            "violations": 0,
            "sample": [],
            "include_in_summary": output.get("include_in_summary", True),
        }

    col_series = df[result_column].astype(str).str.strip()
    violations_mask = (col_series != "") & (col_series != "-")
    n_violations = int(violations_mask.sum())
    total = len(df)
    sample: List[str] = []
    if n_violations > 0:
        vio_idx = df.index[violations_mask].tolist()[: _MAX_SAMPLE]
        for idx in vio_idx:
            cell = col_series.loc[idx]
            compact = _compact_field_length_cell(cell)
            if compact:
                sample.append(f"[{_excel_row(idx)}] | {compact}")

    return {
        "check_id": check_id,
        "sheet": sheet_name,
        "name": rule.get("name", ""),
        "column_on_sheet": col_out,
        "type": "field_length",
        "total_rows": total,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


def _parse_add_data_cell(val: Any) -> Optional[Dict[str, Any]]:
    """
    Разбирает значение ADD_DATA: тройные кавычки заменяются на одинарные, затем JSON.
    Возвращает dict или None при ошибке разбора.
    """
    parsed, _, _ = _parse_add_data_cell_with_normalized(val)
    return parsed


def _parse_add_data_cell_with_normalized(val: Any) -> Tuple[Optional[Dict[str, Any]], str, str]:
    """
    Разбирает значение ADD_DATA; возвращает (dict или None, исходная строка, строка после замены \"\"\" -> \").
    Нужно для DEBUG-логирования при ошибках проверок json_field_equals_column и json_field_in_column.
    """
    raw_str = "" if pd.isna(val) or val is None else str(val).strip()
    if not raw_str:
        return None, raw_str, raw_str
    normalized = raw_str.replace('"""', '"')
    try:
        return json.loads(normalized), raw_str, normalized
    except (json.JSONDecodeError, TypeError):
        return None, raw_str, normalized


def _run_json_field_equals_column_check(sheets_data: Dict[str, Any], rule: Dict[str, Any]) -> None:
    """
    Проверка: значение ключа json_key в JSON-поле json_column должно равняться значению колонки column_compare.
    Опционально: только для строк, где filter_column == filter_value и/или в JSON есть json_filter_key == json_filter_value.
    ADD_DATA разбирается с заменой тройных кавычек на одинарные.
    Записывает колонку результата на лист (OK / сообщение об ошибке / пусто для неприменимых строк).
    """
    sheet_name = rule.get("sheet")
    json_column = rule.get("json_column")
    json_key = rule.get("json_key")
    column_compare = rule.get("column_compare")
    filter_column = rule.get("filter_column")
    filter_value = rule.get("filter_value")
    json_filter_key = rule.get("json_filter_key")
    json_filter_value = rule.get("json_filter_value")
    must_not_equal = rule.get("must_not_equal", False)  # true: требовать parentRewardCode != REWARD_CODE
    output = rule.get("output") or {}
    col_out = output.get("column_on_sheet") or f"ПРОВЕРКА: {json_key} в {json_column} = {column_compare}"
    check_id = rule.get("id", "")

    item = _get_sheet_item(sheets_data, sheet_name)
    if item is None:
        logging.debug(f"[consistency] json_field_equals_column {check_id}: лист {sheet_name} отсутствует")
        return
    df, conf = item
    for c in [json_column, column_compare]:
        if c not in df.columns:
            logging.warning(f"[consistency] json_field_equals_column {check_id}: колонка {c} не найдена на {sheet_name}")
            return
    if filter_column and filter_column not in df.columns:
        logging.warning(f"[consistency] json_field_equals_column {check_id}: filter_column {filter_column} не найдена")
        return

    def _check_one(row: pd.Series) -> str:
        if filter_column is not None and filter_value is not None:
            if str(row.get(filter_column, "")).strip() != str(filter_value).strip():
                return ""
        raw_val = row.get(json_column)
        add_data, raw_str, normalized_str = _parse_add_data_cell_with_normalized(raw_val)
        excel_row = (int(row.name) + 2) if row.name is not None else "?"
        if add_data is None:
            logging.debug(f"[consistency] json_field_equals_column {check_id} строка {excel_row}: ошибка разбора ADD_DATA")
            logging.debug(f"  Исходное значение колонки (целиком): {raw_str!r}")
            logging.debug(f"  После преобразований (\"\"\"->\"): {normalized_str!r}")
            return "Ошибка разбора ADD_DATA"
        # Правило применяется только если в JSON значение json_filter_key равно json_filter_value (например masterBadge == "Y")
        if json_filter_key is not None and json_filter_value is not None:
            actual_filter = str(add_data.get(json_filter_key, "")).strip()
            required_filter = str(json_filter_value).strip()
            if actual_filter != required_filter:
                return ""  # не применяется (например, для BADGE с masterBadge="N" — пустая ячейка)
        from_json = str(add_data.get(json_key, "")).strip()
        expected = str(row.get(column_compare, "")).strip()
        if must_not_equal:
            if from_json == expected:
                logging.debug(f"[consistency] json_field_equals_column {check_id} строка {excel_row}: не должно совпадать")
                logging.debug(f"  Исходное значение колонки (целиком): {raw_str!r}")
                logging.debug(f"  После преобразований: {normalized_str!r}")
                logging.debug(f"  JSON (дерево структуры):\n{json.dumps(add_data, ensure_ascii=False, indent=2)}")
                logging.debug(f"  Поле из JSON с ошибкой: {json_key!r} = {from_json!r} | значение для сравнения ({column_compare}): {expected!r}")
                return f"не должно совпадать с REWARD_CODE: {expected}"
            return "OK"
        if from_json == expected:
            return "OK"
        logging.debug(f"[consistency] json_field_equals_column {check_id} строка {excel_row}: несовпадение")
        logging.debug(f"  Исходное значение колонки (целиком): {raw_str!r}")
        logging.debug(f"  После преобразований: {normalized_str!r}")
        logging.debug(f"  JSON (дерево структуры):\n{json.dumps(add_data, ensure_ascii=False, indent=2)}")
        logging.debug(f"  Поле из JSON с ошибкой: {json_key!r} = {from_json!r} | значение, с которым сравниваем ({column_compare}): {expected!r}")
        return f"ожидалось {expected}, в ADD_DATA: {from_json}"

    results = df.apply(_check_one, axis=1)
    df = df.copy()
    df[col_out] = results.values
    sheets_data[sheet_name] = (df, conf)
    logging.debug(f"[consistency] json_field_equals_column {check_id}: записана колонка {col_out} на {sheet_name}")


def collect_json_field_equals_column_result(
    sheets_data: Dict[str, Any],
    rule: Dict[str, Any],
) -> Dict[str, Any]:
    """Собирает результат проверки json_field_equals_column по уже заполненной колонке."""
    sheet_name = rule.get("sheet")
    output = rule.get("output") or {}
    json_key = rule.get("json_key", "")
    json_column = rule.get("json_column", "")
    column_compare = rule.get("column_compare", "")
    col_out = output.get("column_on_sheet") or f"ПРОВЕРКА: {json_key} в {json_column} = {column_compare}"
    check_id = rule.get("id", "")

    df = _get_sheet_df(sheets_data, sheet_name)
    if df is None or col_out not in df.columns:
        return {
            "check_id": check_id,
            "sheet": sheet_name,
            "name": rule.get("name", ""),
            "column_on_sheet": col_out,
            "type": "json_field_equals_column",
            "total_rows": 0,
            "violations": 0,
            "sample": [],
            "include_in_summary": output.get("include_in_summary", True),
        }
    col_series = df[col_out].astype(str).str.strip()
    violations_mask = col_series.ne("") & col_series.ne("OK")
    n_violations = int(violations_mask.sum())
    total_applicable = int((col_series.ne("")).sum())
    total = len(df)
    sample = []
    if n_violations > 0:
        vio_idx = df.index[violations_mask].tolist()[: _MAX_SAMPLE]
        for idx in vio_idx:
            msg = col_series.loc[idx]
            if "Ошибка разбора" in msg:
                sample.append(f"[{_excel_row(idx)}] | json_бит")
            elif "не должно совпадать" in msg:
                sample.append(f"[{_excel_row(idx)}] | =запрещено")
            elif "ожидалось " in msg and " в ADD_DATA: " in msg:
                try:
                    _, rest = msg.split("ожидалось ", 1)
                    exp, in_add = rest.split(", в ADD_DATA: ", 1)
                    sample.append(f"[{_excel_row(idx)}] {in_add.strip()[:30]} ≠ {exp.strip()[:20]}")
                except ValueError:
                    sample.append(f"[{_excel_row(idx)}] | {msg[:50]}")
            else:
                sample.append(f"[{_excel_row(idx)}] | {msg[:50]}")
    return {
        "check_id": check_id,
        "sheet": sheet_name,
        "name": rule.get("name", ""),
        "column_on_sheet": col_out,
        "type": "json_field_equals_column",
        "total_rows": total_applicable,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


def _run_json_field_in_column_check(sheets_data: Dict[str, Any], rule: Dict[str, Any]) -> None:
    """
    Проверка: все уникальные значения ключа json_key в JSON-поле json_column должны присутствовать
    в колонке column_in_sheet того же листа (например parentRewardCode из ADD_DATA — в REWARD_CODE).
    Для каждой строки: извлечь json_key из JSON; если значение не пусто — проверить, что оно есть в колонке.
    """
    sheet_name = rule.get("sheet")
    json_column = rule.get("json_column")
    json_key = rule.get("json_key")
    column_in_sheet = rule.get("column_in_sheet")
    output = rule.get("output") or {}
    col_out = output.get("column_on_sheet") or f"ПРОВЕРКА: {json_key} из {json_column} в {column_in_sheet}"
    check_id = rule.get("id", "")

    item = _get_sheet_item(sheets_data, sheet_name)
    if item is None:
        logging.debug(f"[consistency] json_field_in_column {check_id}: лист {sheet_name} отсутствует")
        return
    df, conf = item
    for c in [json_column, column_in_sheet]:
        if c not in df.columns:
            logging.warning(f"[consistency] json_field_in_column {check_id}: колонка {c} не найдена на {sheet_name}")
            return

    allowed_set = set(df[column_in_sheet].astype(str).str.strip())

    def _check_one(row: pd.Series) -> str:
        raw_val = row.get(json_column)
        add_data, raw_str, normalized_str = _parse_add_data_cell_with_normalized(raw_val)
        excel_row = (int(row.name) + 2) if row.name is not None else "?"
        if add_data is None:
            logging.debug(f"[consistency] json_field_in_column {check_id} строка {excel_row}: ошибка разбора ADD_DATA")
            logging.debug(f"  Исходное значение колонки (целиком): {raw_str!r}")
            logging.debug(f"  После преобразований (\"\"\"->\"): {normalized_str!r}")
            return "Ошибка разбора ADD_DATA"
        from_json = str(add_data.get(json_key, "")).strip()
        if not from_json:
            return ""
        if from_json in allowed_set:
            return "OK"
        # Ошибка: значение из JSON не найдено в колонке листа
        logging.debug(f"[consistency] json_field_in_column {check_id} строка {excel_row}: значение не в колонке")
        logging.debug(f"  Исходное значение колонки (целиком): {raw_str!r}")
        logging.debug(f"  После преобразований: {normalized_str!r}")
        logging.debug(f"  JSON (дерево структуры):\n{json.dumps(add_data, ensure_ascii=False, indent=2)}")
        logging.debug(f"  Поле из JSON с ошибкой: {json_key!r} = {from_json!r} (ожидается наличие в колонке {column_in_sheet!r})")
        return f"НЕТ в {column_in_sheet}"

    results = df.apply(_check_one, axis=1)
    df = df.copy()
    df[col_out] = results.values
    sheets_data[sheet_name] = (df, conf)
    logging.debug(f"[consistency] json_field_in_column {check_id}: записана колонка {col_out} на {sheet_name}")


def _run_json_priority_unique_per_contest_link_check(
    sheets_data: Dict[str, Any],
    rule: Dict[str, Any],
) -> None:
    """
    Проверка: для каждого CONTEST_CODE на REWARD-LINK берутся уникальные REWARD_CODE (GROUP_CODE не учитывается).
    У всех соответствующих строк на листе REWARD в json_column читается ключ json_key из JSON
    (разбор через _parse_add_data_cell_with_normalized — как в json_field_equals_column).

    Логика по группе одного CONTEST_CODE:
    - у всех привязанных наград нет значения json_key (пусто) — не нарушение, ячейки результата не трогаем для этих строк;
    - у всех значение задано — они должны быть попарно различны; иначе сообщение о дубле;
    - смешанный случай (часть с полем, часть без) — нарушение для всех строк группы с единым текстом.

    Строки REWARD, не попавшие ни в одну группу по REWARD-LINK, получают пустой результат.
    Ошибка разбора JSON — только для соответствующей строки.
    """
    sheet_name = rule.get("sheet", "REWARD")
    json_column = rule.get("json_column", "REWARD_ADD_DATA")
    json_key = rule.get("json_key", "priority")
    reward_code_column = rule.get("reward_code_column", "REWARD_CODE")
    link_sheet = rule.get("link_sheet", "REWARD-LINK")
    link_contest_column = rule.get("link_contest_column", "CONTEST_CODE")
    link_reward_column = rule.get("link_reward_column", "REWARD_CODE")
    output = rule.get("output") or {}
    col_out = output.get("column_on_sheet") or f"ПРОВЕРКА: {json_key} уникален по CONTEST (REWARD-LINK)"
    check_id = rule.get("id", "")

    item_reward = _get_sheet_item(sheets_data, sheet_name)
    if item_reward is None:
        logging.debug(f"[consistency] json_priority_unique_per_contest_link {check_id}: лист {sheet_name} отсутствует")
        return
    df_reward, conf = item_reward
    for c in (reward_code_column, json_column):
        if c not in df_reward.columns:
            logging.warning(
                f"[consistency] json_priority_unique_per_contest_link {check_id}: колонка {c} не найдена на {sheet_name}"
            )
            return

    df_link = _get_sheet_df(sheets_data, link_sheet)
    if df_link is None:
        logging.debug(f"[consistency] json_priority_unique_per_contest_link {check_id}: лист {link_sheet} отсутствует")
        return
    for c in (link_contest_column, link_reward_column):
        if c not in df_link.columns:
            logging.warning(
                f"[consistency] json_priority_unique_per_contest_link {check_id}: колонка {c} не найдена на {link_sheet}"
            )
            return

    # Первая строка на REWARD для каждого REWARD_CODE (при дубликатах кодов — первая по индексу)
    idx_by_code: Dict[str, Any] = {}
    for idx in df_reward.index:
        raw_code = df_reward.loc[idx, reward_code_column]
        code = str(raw_code).strip() if pd.notna(raw_code) else ""
        if code and code not in idx_by_code:
            idx_by_code[code] = idx

    violations_by_idx: Dict[Any, List[str]] = defaultdict(list)
    ok_idx: Set[Any] = set()

    for contest_val, group in df_link.groupby(link_contest_column, dropna=False):
        cstr = str(contest_val).strip() if pd.notna(contest_val) else ""
        if not cstr:
            continue
        codes_series = group[link_reward_column].dropna().astype(str).str.strip()
        codes = [c for c in codes_series.unique().tolist() if c]

        entries: List[Dict[str, Any]] = []
        for rc in codes:
            if rc not in idx_by_code:
                logging.warning(
                    f"[consistency] json_priority_unique_per_contest_link {check_id}: "
                    f"REWARD_CODE={rc!r} из {link_sheet} (CONTEST_CODE={cstr!r}) нет на {sheet_name}, пропуск кода"
                )
                continue
            idx = idx_by_code[rc]
            raw_val = df_reward.loc[idx, json_column]
            add_data, raw_str, normalized_str = _parse_add_data_cell_with_normalized(raw_val)
            excel_row = _excel_row(idx) if idx is not None else "?"
            if add_data is None:
                logging.debug(
                    f"[consistency] json_priority_unique_per_contest_link {check_id} строка {excel_row}: "
                    f"ошибка разбора ADD_DATA (CONTEST_CODE={cstr!r}, REWARD_CODE={rc!r})"
                )
                logging.debug(f"  Исходное значение колонки (целиком): {raw_str!r}")
                logging.debug(f"  После преобразований (\"\"\"->\"): {normalized_str!r}")
                entries.append({"idx": idx, "code": rc, "parse_ok": False})
            else:
                pv = str(add_data.get(json_key, "")).strip()
                entries.append(
                    {
                        "idx": idx,
                        "code": rc,
                        "parse_ok": True,
                        "priority_present": bool(pv),
                        "priority": pv,
                    }
                )

        if not entries:
            continue

        for e in entries:
            if not e["parse_ok"]:
                violations_by_idx[e["idx"]].append("Ошибка разбора ADD_DATA")

        ok_entries = [e for e in entries if e["parse_ok"]]
        if not ok_entries:
            continue

        with_priority = [e for e in ok_entries if e["priority_present"]]
        if len(with_priority) == 0:
            # Все без json_key — не ошибка
            continue
        if len(with_priority) < len(ok_entries):
            msg = (
                f"В CONTEST_CODE={cstr!r} поле {json_key} должно быть задано у всех "
                f"привязанных REWARD_CODE или ни у одного ({link_sheet})"
            )
            for e in ok_entries:
                violations_by_idx[e["idx"]].append(msg)
            continue

        by_pri: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for e in ok_entries:
            by_pri[e["priority"]].append(e)

        dup_found = False
        for pri_val, es in by_pri.items():
            if len(es) > 1:
                dup_found = True
                codes_dup = ", ".join(sorted(x["code"] for x in es))
                msg = (
                    f"Неуникальный {json_key}={pri_val!r} в CONTEST_CODE={cstr!r} "
                    f"(REWARD_CODE: {codes_dup})"
                )
                for e in es:
                    violations_by_idx[e["idx"]].append(msg)

        if not dup_found:
            for e in ok_entries:
                ok_idx.add(e["idx"])

    result_series = pd.Series("", index=df_reward.index, dtype=object)
    for idx in df_reward.index:
        if idx in violations_by_idx:
            uniq_msgs: List[str] = []
            for m in violations_by_idx[idx]:
                if m not in uniq_msgs:
                    uniq_msgs.append(m)
            result_series.loc[idx] = " | ".join(uniq_msgs)
        elif idx in ok_idx:
            result_series.loc[idx] = "OK"

    df_out = df_reward.copy()
    df_out[col_out] = result_series.values
    sheets_data[sheet_name] = (df_out, conf)
    logging.debug(
        f"[consistency] json_priority_unique_per_contest_link {check_id}: записана колонка {col_out} на {sheet_name}"
    )


def collect_json_priority_unique_per_contest_link_result(
    sheets_data: Dict[str, Any],
    rule: Dict[str, Any],
) -> Dict[str, Any]:
    """Собирает результат проверки json_priority_unique_per_contest_link по уже заполненной колонке."""
    sheet_name = rule.get("sheet", "REWARD")
    output = rule.get("output") or {}
    json_key = rule.get("json_key", "priority")
    col_out = output.get("column_on_sheet") or f"ПРОВЕРКА: {json_key} уникален по CONTEST (REWARD-LINK)"
    check_id = rule.get("id", "")

    df = _get_sheet_df(sheets_data, sheet_name)
    if df is None or col_out not in df.columns:
        return {
            "check_id": check_id,
            "sheet": sheet_name,
            "name": rule.get("name", ""),
            "column_on_sheet": col_out,
            "type": "json_priority_unique_per_contest_link",
            "total_rows": 0,
            "violations": 0,
            "sample": [],
            "include_in_summary": output.get("include_in_summary", True),
        }
    col_series = df[col_out].astype(str).str.strip()
    violations_mask = col_series.ne("") & col_series.ne("OK")
    n_violations = int(violations_mask.sum())
    total_applicable = int((col_series.ne("")).sum())
    sample: List[str] = []
    if n_violations > 0:
        vio_idx = df.index[violations_mask].tolist()[:_MAX_SAMPLE]
        for idx in vio_idx:
            msg = col_series.loc[idx]
            if "Ошибка разбора" in msg:
                sample.append(f"[{_excel_row(idx)}] | json_бит")
            else:
                sample.append(f"[{_excel_row(idx)}] | {msg[:80]}")
    return {
        "check_id": check_id,
        "sheet": sheet_name,
        "name": rule.get("name", ""),
        "column_on_sheet": col_out,
        "type": "json_priority_unique_per_contest_link",
        "total_rows": total_applicable,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


def collect_json_field_in_column_result(
    sheets_data: Dict[str, Any],
    rule: Dict[str, Any],
) -> Dict[str, Any]:
    """Собирает результат проверки json_field_in_column по уже заполненной колонке."""
    sheet_name = rule.get("sheet")
    output = rule.get("output") or {}
    json_key = rule.get("json_key", "")
    json_column = rule.get("json_column", "")
    column_in_sheet = rule.get("column_in_sheet", "")
    col_out = output.get("column_on_sheet") or f"ПРОВЕРКА: {json_key} из {json_column} в {column_in_sheet}"
    check_id = rule.get("id", "")

    df = _get_sheet_df(sheets_data, sheet_name)
    if df is None or col_out not in df.columns:
        return {
            "check_id": check_id,
            "sheet": sheet_name,
            "name": rule.get("name", ""),
            "column_on_sheet": col_out,
            "type": "json_field_in_column",
            "total_rows": 0,
            "violations": 0,
            "sample": [],
            "include_in_summary": output.get("include_in_summary", True),
        }
    col_series = df[col_out].astype(str).str.strip()
    violations_mask = col_series.ne("") & col_series.ne("OK")
    n_violations = int(violations_mask.sum())
    total_applicable = int((col_series.ne("")).sum())
    sample = []
    if n_violations > 0:
        vio_idx = df.index[violations_mask].tolist()[: _MAX_SAMPLE]
        for idx in vio_idx:
            msg = col_series.loc[idx]
            if "Ошибка разбора" in msg:
                sample.append(f"[{_excel_row(idx)}] | json_бит")
            else:
                add_data = _parse_add_data_cell(df.loc[idx, json_column])
                val = ""
                if add_data is not None:
                    val = str(add_data.get(json_key, "")).strip()[:30]
                sample.append(f"[{_excel_row(idx)}] {val} ∉{column_in_sheet}")
    return {
        "check_id": check_id,
        "sheet": sheet_name,
        "name": rule.get("name", ""),
        "column_on_sheet": col_out,
        "type": "json_field_in_column",
        "total_rows": total_applicable,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


def _sheet_written_by_rule(rule: Dict[str, Any]) -> Optional[str]:
    """Лист, в который правило пишет результат (для блокировки)."""
    t = rule.get("type", "")
    if t in (
        "unique",
        "field_length",
        "field_format",
        "json_field_equals_column",
        "json_field_in_column",
        "json_priority_unique_per_contest_link",
        "json_spod_format",
    ):
        return rule.get("sheet")
    if t in ("referential", "referential_composite", "cross_sheet_date_lte_today"):
        return rule.get("sheet_src")
    return None


def _disabled_rule_summary(
    rule: Dict[str, Any],
    sheets_data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Строка свода CONSISTENCY для правила с enabled: false: total_rows по целевому листу,
    в sample — пометка без подсчёта нарушений.
    """
    t = str(rule.get("type", ""))
    if t in ("referential", "referential_composite"):
        sheet = rule.get("sheet_src") or ""
    else:
        sheet = rule.get("sheet") or rule.get("sheet_src") or ""
    out = rule.get("output") or {}
    col = out.get("column_on_sheet", "")
    total = 0
    if sheet:
        df = _get_sheet_df(sheets_data, str(sheet))
        if df is not None:
            total = len(df)
    return {
        "check_id": rule.get("id", ""),
        "sheet": sheet,
        "name": rule.get("name", ""),
        "column_on_sheet": col,
        "type": t,
        "total_rows": total,
        "violations": 0,
        "sample": ["правило отключено (enabled: false), проверка не выполнялась"],
        "include_in_summary": out.get("include_in_summary", True),
        "disabled": True,
    }


def run_all_consistency_checks(
    sheets_data: Dict[str, Any],
    config: Dict[str, Any],
    max_workers: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Выполняет все включённые правила консистентности (с параллелизацией по правилам).
    Сначала создаёт колонки unique (ДУБЛЬ: …), field_length, json_* на листах; затем referential/referential_composite
    и сбор результатов. Правила, пишущие в разные листы, выполняются параллельно;
    запись в один лист защищена блокировкой по листу.
    Возвращает список записей для сводного листа в порядке правил (включая выключенные — синтетическая строка свода).
    """
    rules = config.get("rules") or []
    if not rules:
        return []

    enabled_pairs: List[Tuple[int, Dict[str, Any]]] = [
        (i, r) for i, r in enumerate(rules) if r.get("enabled", True)
    ]

    n_workers = max_workers if max_workers is not None and max_workers > 0 else min(8, (os.cpu_count() or 8), max(1, len(enabled_pairs)))
    n_workers = max(1, n_workers)
    logging.debug(
        f"[consistency] Параллельный запуск проверок: потоков={n_workers}, "
        f"включённых правил={len(enabled_pairs)} из {len(rules)}"
    )

    # Листы, в которые что-то пишется — по одному Lock на лист (только для включённых правил)
    sheets_written: set = set()
    for _, r in enabled_pairs:
        sh = _sheet_written_by_rule(r)
        if sh:
            sheets_written.add(sh)
    lock_by_sheet: Dict[str, threading.Lock] = {s: threading.Lock() for s in sheets_written}

    def _phase1_task(idx: int, rule: Dict[str, Any]) -> None:
        sheet = _sheet_written_by_rule(rule)
        if not sheet or sheet not in lock_by_sheet:
            return
        with lock_by_sheet[sheet]:
            if rule.get("type") == "unique":
                _run_unique_check(sheets_data, rule)
            elif rule.get("type") == "field_length":
                _run_field_length_check(sheets_data, rule)
            elif rule.get("type") == "field_format":
                _run_field_format_check(sheets_data, rule)
            elif rule.get("type") == "json_field_equals_column":
                _run_json_field_equals_column_check(sheets_data, rule)
            elif rule.get("type") == "json_field_in_column":
                _run_json_field_in_column_check(sheets_data, rule)
            elif rule.get("type") == "json_priority_unique_per_contest_link":
                _run_json_priority_unique_per_contest_link_check(sheets_data, rule)

    # Фаза 1: создаём колонки unique, field_length, field_format, json_* на листах
    phase1_rules = [
        (gi, r)
        for gi, r in enabled_pairs
        if r.get("type")
        in (
            "unique",
            "field_length",
            "field_format",
            "json_field_equals_column",
            "json_field_in_column",
            "json_priority_unique_per_contest_link",
        )
    ]
    if phase1_rules:
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures_ph1 = [executor.submit(_phase1_task, idx, rule) for idx, rule in phase1_rules]
            for f in as_completed(futures_ph1):
                f.result()

    # Фаза 2: referential/referential_composite/collect/json_spod_format — (глобальный индекс правила, результат)
    def _phase2_task(idx: int, rule: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        rule_type = rule.get("type", "")
        check_id = rule.get("id", "")
        try:
            if rule_type == "referential":
                sheet_src = rule.get("sheet_src")
                if sheet_src and sheet_src in lock_by_sheet:
                    with lock_by_sheet[sheet_src]:
                        res = run_referential(sheets_data, rule)
                else:
                    res = run_referential(sheets_data, rule)
            elif rule_type == "referential_composite":
                sheet_src = rule.get("sheet_src")
                if sheet_src and sheet_src in lock_by_sheet:
                    with lock_by_sheet[sheet_src]:
                        res = run_referential_composite(sheets_data, rule)
                else:
                    res = run_referential_composite(sheets_data, rule)
            elif rule_type == "cross_sheet_date_lte_today":
                sheet_src = rule.get("sheet_src")
                if sheet_src and sheet_src in lock_by_sheet:
                    with lock_by_sheet[sheet_src]:
                        res = run_cross_sheet_date_lte_today(sheets_data, rule)
                else:
                    res = run_cross_sheet_date_lte_today(sheets_data, rule)
            elif rule_type == "unique":
                res = collect_unique_result(sheets_data, rule)
            elif rule_type == "field_length":
                res = collect_field_length_result(sheets_data, rule)
            elif rule_type == "field_format":
                res = collect_field_format_result(sheets_data, rule)
            elif rule_type == "json_field_equals_column":
                res = collect_json_field_equals_column_result(sheets_data, rule)
            elif rule_type == "json_field_in_column":
                res = collect_json_field_in_column_result(sheets_data, rule)
            elif rule_type == "json_priority_unique_per_contest_link":
                res = collect_json_priority_unique_per_contest_link_result(sheets_data, rule)
            elif rule_type == "json_spod_format":
                from src.json_spod_format_check import run_json_spod_format_check

                sh_json = rule.get("sheet")
                if sh_json and sh_json in lock_by_sheet:
                    with lock_by_sheet[sh_json]:
                        res = run_json_spod_format_check(sheets_data, rule)
                else:
                    res = run_json_spod_format_check(sheets_data, rule)
            else:
                logging.debug(f"[consistency] Неизвестный тип правила: {rule_type}, id={check_id}")
                _out = rule.get("output") or {}
                return (idx, {
                    "check_id": check_id,
                    "sheet": rule.get("sheet_src") or rule.get("sheet", ""),
                    "name": rule.get("name", ""),
                    "column_on_sheet": _out.get("column_on_sheet", ""),
                    "type": rule_type,
                    "total_rows": 0,
                    "violations": 0,
                    "sample": [],
                    "include_in_summary": True,
                })
            return (idx, res)
        except Exception as e:
            logging.error(f"[consistency] Ошибка при выполнении правила {check_id} ({rule_type}): {e}")
            _out = rule.get("output") or {}
            return (idx, {
                "check_id": check_id,
                "sheet": rule.get("sheet_src") or rule.get("sheet", ""),
                "name": rule.get("name", ""),
                "column_on_sheet": _out.get("column_on_sheet", ""),
                "type": rule_type,
                "total_rows": 0,
                "violations": 0,
                "sample": [],
                "include_in_summary": True,
                "error": str(e),
            })

    slot: List[Optional[Dict[str, Any]]] = [None] * len(rules)
    if enabled_pairs:
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures_ph2 = [executor.submit(_phase2_task, gi, rule) for gi, rule in enabled_pairs]
            for future in as_completed(futures_ph2):
                gi, res = future.result()
                slot[gi] = res
    out: List[Dict[str, Any]] = []
    for i, rule in enumerate(rules):
        if slot[i] is not None:
            out.append(slot[i])  # type: ignore[arg-type]
        else:
            out.append(_disabled_rule_summary(rule, sheets_data))
    return out


def _rule_to_description_columns(rule: Dict[str, Any]) -> Dict[str, str]:
    """
    По правилу из конфига формирует колонки как в таблице проверок (Проверки-Tаблица 1.csv):
    ТИП ПРОВЕРКИ, Описание, таблица источник, поле источник, таблица где проверяем, поле для проверки, параметр сравнения, комментарий.
    """
    t = rule.get("type", "")
    name = rule.get("name", "")
    type_ru = {
        "referential": "внешний ключ в одну колонку",
        "referential_composite": "внешний ключ из нескольких колонок",
        "cross_sheet_date_lte_today": "дата из справочника не позже текущей даты",
        "unique": "уникальность, отсутствие дублей по ключу",
        "field_length": "длина полей",
        "field_format": "формат поля",
        "json_field_equals_column": "поле в JSON равно колонке",
        "json_field_in_column": "поле в JSON должно быть в колонке листа",
        "json_priority_unique_per_contest_link": "уникальность поля JSON по CONTEST_CODE (REWARD-LINK)",
        "json_spod_format": "JSON в формате SPOD (тройные кавычки, числа без кавычек)",
    }.get(t, t)
    table_src = ""
    field_src = ""
    table_ref = ""
    field_ref = ""
    param = ""
    comment = ""
    if t == "referential":
        table_src = rule.get("sheet_src", "")
        field_src = rule.get("column_src", "")
        table_ref = rule.get("sheet_ref", "")
        field_ref = rule.get("column_ref", "")
        param = "все из источника существуют во второй таблице"
        sc = rule.get("src_row_conditions") or rule.get("sheet_src_row_conditions")
        rc = rule.get("ref_row_conditions") or rule.get("sheet_ref_row_conditions")
        if sc or rc:
            comment = f"фильтр src: {sc!r}; фильтр ref: {rc!r}".strip()
    elif t == "referential_composite":
        table_src = rule.get("sheet_src", "")
        field_src = ", ".join(rule.get("columns_src") or [])
        table_ref = rule.get("sheet_ref", "")
        field_ref = ", ".join(rule.get("columns_ref") or [])
        param = "все из источника существуют во второй таблице"
        sc = rule.get("src_row_conditions") or rule.get("sheet_src_row_conditions")
        rc = rule.get("ref_row_conditions") or rule.get("sheet_ref_row_conditions")
        if sc or rc:
            comment = f"фильтр src: {sc!r}; фильтр ref: {rc!r}".strip()
    elif t == "cross_sheet_date_lte_today":
        table_src = rule.get("sheet_src", "")
        field_src = rule.get("column_src", "")
        table_ref = rule.get("sheet_ref", "")
        field_ref = f"{rule.get('column_ref', '')} -> {rule.get('column_date_ref', '')}"
        param = f"{rule.get('column_date_ref', '')} <= сегодня ({rule.get('date_format', 'YYYY-MM-DD')})"
        sc = rule.get("src_row_conditions") or rule.get("sheet_src_row_conditions")
        rc = rule.get("ref_row_conditions") or rule.get("sheet_ref_row_conditions")
        if sc or rc:
            comment = f"фильтр src: {sc!r}; фильтр ref: {rc!r}".strip()
    elif t == "unique":
        table_src = rule.get("sheet", "")
        field_src = ", ".join(rule.get("key_columns") or [])
        param = "нет дублей"
        conds = _normalize_unique_scope_conditions(rule)
        if conds:
            mode_ru = "И" if _unique_scope_mode(rule) == "all" else "ИЛИ"
            pairs = "; ".join(f"{c}={v!r}" for c, v in conds)
            comment = f"область ({mode_ru}): {pairs}"
        req = rule.get("unique_require_non_empty") or []
        if req:
            extra = f"только при непустых: {', '.join(req)}"
            comment = f"{comment}; {extra}" if comment else extra
    elif t == "field_length":
        table_src = rule.get("sheet", "")
        fields_cfg = rule.get("fields") or {}
        parts = []
        for fname, fcfg in fields_cfg.items():
            op = fcfg.get("operator", "")
            lim = fcfg.get("limit", "")
            parts.append(f"{fname} {op}{lim}")
        field_src = "; ".join(parts)
        param = "длина в заданных границах"
    elif t == "field_format":
        table_src = rule.get("sheet", "")
        field_src = rule.get("field", "")
        fmt = rule.get("format") or {}
        fmt_type = fmt.get("type", "")
        if fmt_type == "date":
            param = fmt.get("date_format", "YYYY-MM-DD")
            if fmt.get("special_values"):
                comment = "учесть вариант " + ", ".join(fmt["special_values"])
            if fmt.get("allow_empty"):
                comment = (comment + "; может быть пустым").lstrip("; ")
        elif fmt_type == "decimal":
            places = fmt.get("decimal_places", 5)
            param = "0." + "0" * places
        elif fmt_type == "fixed_length_digits":
            param = f"{fmt.get('length', 20)} цифр с лидирующими нулями"
        else:
            param = str(fmt_type)
    elif t == "json_field_equals_column":
        table_src = rule.get("sheet", "")
        field_src = rule.get("json_column", "")
        field_ref = rule.get("column_compare", "")
        param = f"{rule.get('json_key', '')} из JSON = {field_ref}" if not rule.get("must_not_equal") else f"{rule.get('json_key', '')} из JSON ≠ {field_ref}"
        comment = ""
        if rule.get("must_not_equal"):
            comment = "значения должны отличаться"
        if rule.get("filter_column") and rule.get("filter_value") is not None:
            comment = (comment + f"; только где {rule['filter_column']}={rule['filter_value']}").lstrip("; ")
        if rule.get("json_filter_key") and rule.get("json_filter_value") is not None:
            comment = (comment + f"; в JSON {rule['json_filter_key']}={rule['json_filter_value']}").lstrip("; ")
    elif t == "json_field_in_column":
        table_src = rule.get("sheet", "")
        field_src = rule.get("json_column", "")
        field_ref = rule.get("column_in_sheet", "")
        param = f"все значения {rule.get('json_key', '')} из JSON должны быть в колонке {field_ref}"
    elif t == "json_priority_unique_per_contest_link":
        table_src = rule.get("link_sheet", "REWARD-LINK")
        field_src = f"{rule.get('link_contest_column', 'CONTEST_CODE')}, {rule.get('link_reward_column', 'REWARD_CODE')}"
        table_ref = rule.get("sheet", "REWARD")
        field_ref = f"{rule.get('json_column', 'REWARD_ADD_DATA')} → ключ {rule.get('json_key', 'priority')}"
        param = (
            "по каждому CONTEST_CODE уникальные значения json_key среди привязанных REWARD_CODE; "
            "либо поле отсутствует у всех, либо задано у всех с разными значениями"
        )
        comment = "GROUP_CODE не учитывается; парсинг ADD_DATA как в json_field_equals_column"
    elif t == "json_spod_format":
        table_src = rule.get("sheet", "")
        field_src = rule.get("json_column", "")
        req = rule.get("json_required", True)
        param = "обязательный JSON в каждой строке" if req else "пустые ячейки допустимы"
        nv = rule.get("numeric_value_keys") or []
        if nv:
            comment = "числовые значения без кавычек у ключей: " + ", ".join(str(x) for x in nv)
    return {
        "ТИП ПРОВЕРКИ": type_ru,
        "Описание": name,
        "таблица источник": table_src,
        "поле источник": field_src,
        "таблица где проверяем": table_ref,
        "поле для проверки": field_ref,
        "параметр сравнения": param,
        "комментарий": comment,
    }


def _sample_rows_prefix(sample: List[Any]) -> str:
    """
    Из записей sample извлекает все номера строк (из ведущих [N] или [N, M, K])
    и возвращает строку вида «[37], [47], [57]» для префикса при обрезке.
    """
    seen: Set[int] = set()
    for item in sample:
        s = str(item).strip()
        m = re.match(r"\[([^\]]+)\]", s)
        if m:
            for part in m.group(1).split(","):
                try:
                    n = int(part.strip())
                    seen.add(n)
                except (ValueError, TypeError):
                    pass
    if not seen:
        return ""
    return ", ".join(f"[{n}]" for n in sorted(seen))


def build_consistency_summary_df(
    results: List[Dict[str, Any]],
    rules: Optional[List[Dict[str, Any]]] = None,
) -> pd.DataFrame:
    """
    Формирует DataFrame для сводного листа CONSISTENCY.
    Если передан rules (список правил из конфига), добавляются колонки по образцу таблицы проверок:
    ТИП ПРОВЕРКИ, Описание, таблица источник, поле источник, таблица где проверяем, поле для проверки, параметр сравнения, комментарий.
    При нескольких записях в sample в начало выводится список всех строк с ошибками, затем « => » и детали (чтобы при обрезке номера строк были видны).
    """
    base_columns = [
        "check_id", "sheet", "name", "имя_колонки", "type", "total_rows", "violations", "sample"
    ]
    desc_columns = [
        "ТИП ПРОВЕРКИ", "Описание", "таблица источник", "поле источник",
        "таблица где проверяем", "поле для проверки", "параметр сравнения", "комментарий"
    ]
    if not results:
        return pd.DataFrame(columns=desc_columns + base_columns if rules else base_columns)

    rule_by_id: Dict[str, Dict[str, Any]] = {}
    if rules:
        for rule in rules:
            rid = rule.get("id", "")
            if rid:
                rule_by_id[rid] = rule

    rows = []
    for r in results:
        if not r.get("include_in_summary", True):
            continue
        sample = r.get("sample") or []
        detail_parts = [str(x)[:80] for x in sample[:5]]
        sample_str = "; ".join(detail_parts)
        if len(sample) > 5:
            sample_str += " ..."
        # При нескольких записях: префикс «[37], [47], ... => » чтобы при обрезке были видны все строки с ошибками
        if len(sample) > 1:
            prefix = _sample_rows_prefix(sample)
            if prefix:
                sample_str = f"{prefix} => {sample_str}"
        row = {
            "check_id": r.get("check_id", ""),
            "sheet": r.get("sheet", ""),
            "name": r.get("name", ""),
            "имя_колонки": r.get("column_on_sheet", ""),
            "type": r.get("type", ""),
            "total_rows": r.get("total_rows", 0),
            "violations": r.get("violations", 0),
            "sample": sample_str,
        }
        if rules:
            desc = _rule_to_description_columns(rule_by_id.get(r.get("check_id", ""), {}))
            row = {**desc, **row}
        rows.append(row)
    cols = (desc_columns + base_columns) if rules else base_columns
    return pd.DataFrame(rows, columns=cols)


def log_and_console_consistency_report(results: List[Dict[str, Any]]) -> None:
    """
    Пишет итог проверок консистентности в лог (файл).
    Краткая сводка для пользователя — в консоли через console_ui (консольный handler обычно WARNING+).
    """
    with_violations = [r for r in results if r.get("violations", 0) > 0]
    if not with_violations:
        logging.info("Проверки консистентности: нарушений не найдено.")
        return

    # Кратко в лог-файл INFO (на консоль не попадёт при уровне WARNING)
    parts = [
        f"{r.get('check_id', '')} ({r.get('sheet', '')}) — {r.get('violations', 0)}"
        for r in with_violations
    ]
    msg = "Проверки консистентности: найдены нарушения: " + "; ".join(parts)
    if len(msg) > 500:
        msg = msg[:497] + "..."
    logging.info(msg)

    # DEBUG: подробно
    for r in with_violations:
        logging.debug(
            f"[consistency] {r.get('check_id')} | {r.get('sheet')} | {r.get('type')} | "
            f"всего строк: {r.get('total_rows')}, нарушений: {r.get('violations')}"
        )
        sample = r.get("sample") or []
        for i, s in enumerate(sample[:10], 1):
            logging.debug(f"[consistency]   пример {i}: {s}")
        if len(sample) > 10:
            logging.debug(f"[consistency]   ... и ещё {len(sample) - 10}")


@debug_timed()
def run_consistency_checks_and_attach_summary(
    sheets_data: Dict[str, Any],
    config: Dict[str, Any],
    max_workers: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Полный цикл: выполнить все проверки (с параллелизацией), добавить сводный лист в sheets_data,
    записать отчёт в лог-файл. Возвращает список результатов правил (для краткой сводки в консоли).
    """
    summary_sheet_name = config.get("summary_sheet_name", "CONSISTENCY")
    results = run_all_consistency_checks(sheets_data, config, max_workers=max_workers)
    # config здесь — секция consistency_checks (summary_sheet_name + rules), а не весь config.json
    rules = config.get("rules") or []
    df_summary = build_consistency_summary_df(results, rules=rules)
    params = {"sheet": summary_sheet_name, "max_col_width": 80, "col_width_mode": "AUTO", "min_col_width": 10}
    sheets_data[summary_sheet_name] = (df_summary, params)
    log_and_console_consistency_report(results)
    return results
