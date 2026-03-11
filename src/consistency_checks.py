# -*- coding: utf-8 -*-
"""
Модуль проверок консистентности данных.
Выполняет правила из конфига consistency_checks: referential, referential_composite,
а также собирает результаты unique и field_length (реализации не меняются — только сбор в свод).
Результаты выводятся в колонки на листах, сводный лист CONSISTENCY, консоль и лог.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


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
        return {"check_id": check_id, "sheet": sheet_src, "type": "referential", "total_rows": 0, "violations": 0, "sample": []}
    if df_ref is None:
        logging.debug(f"[consistency] referential {check_id}: справочник {sheet_ref} отсутствует, помечаем все как нарушение")
    if column_src not in df_src.columns:
        logging.warning(f"[consistency] referential {check_id}: колонка {column_src} не найдена на {sheet_src}")
        return {"check_id": check_id, "sheet": sheet_src, "type": "referential", "total_rows": len(df_src), "violations": len(df_src), "sample": []}

    ref_set = set()
    if df_ref is not None and column_ref in df_ref.columns:
        ref_set = set(df_ref[column_ref].astype(str).str.strip())

    def _status(val: Any) -> str:
        s = str(val).strip() if pd.notna(val) else ""
        if s == "":
            return "OK"
        return "OK" if s in ref_set else f"НЕТ в {sheet_ref}"

    results = df_src[column_src].map(_status)
    total = len(df_src)
    violations_mask = results != "OK"
    n_violations = int(violations_mask.sum())
    sample = []
    if n_violations > 0:
        sample = df_src.loc[violations_mask, column_src].drop_duplicates().head(20).astype(str).tolist()

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
        return {"check_id": check_id, "sheet": sheet_src, "type": "referential_composite", "total_rows": 0, "violations": 0, "sample": []}
    missing_src = [c for c in columns_src if c not in df_src.columns]
    if missing_src:
        logging.warning(f"[consistency] referential_composite {check_id}: колонки {missing_src} не найдены на {sheet_src}")
        return {"check_id": check_id, "sheet": sheet_src, "type": "referential_composite", "total_rows": len(df_src), "violations": 0, "sample": []}

    ref_set = set()
    if df_ref is not None:
        missing_ref = [c for c in columns_ref if c not in df_ref.columns]
        if not missing_ref:
            for _, row in df_ref[columns_ref].iterrows():
                t = tuple(str(row[c]).strip() if pd.notna(row[c]) else "" for c in columns_ref)
                ref_set.add(t)

    def _row_status(row: pd.Series) -> str:
        t = tuple(str(row[c]).strip() if pd.notna(row[c]) else "" for c in columns_src)
        return "OK" if t in ref_set else f"НЕТ в {sheet_ref}"

    results = df_src[columns_src].apply(_row_status, axis=1)
    total = len(df_src)
    violations_mask = results != "OK"
    n_violations = int(violations_mask.sum())
    sample = []
    if n_violations > 0:
        sample_df = df_src.loc[violations_mask, columns_src].drop_duplicates().head(10)
        sample = [sample_df.iloc[i].to_dict() for i in range(len(sample_df))]

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
        "type": "referential_composite",
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
    Собирает результат проверки unique по уже заполненной колонке (ДУБЛЬ: ...).
    Текущую реализацию check_duplicates не трогаем — только читаем колонку.
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
            "type": "unique",
            "total_rows": 0,
            "violations": 0,
            "sample": [],
            "include_in_summary": output.get("include_in_summary", True),
        }

    col_series = df[col_name].astype(str).str.strip()
    violations_mask = col_series != ""
    n_violations = int(violations_mask.sum())
    total = len(df)
    sample = []
    if n_violations > 0:
        sample_vals = df.loc[violations_mask, col_name].drop_duplicates().head(20).tolist()
        sample = [str(v) for v in sample_vals]

    return {
        "check_id": check_id,
        "sheet": sheet_name,
        "name": rule.get("name", ""),
        "type": "unique",
        "total_rows": total,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


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
    check_id = rule.get("id", "")

    df = _get_sheet_df(sheets_data, sheet_name)
    if df is None or result_column not in df.columns:
        return {
            "check_id": check_id,
            "sheet": sheet_name,
            "name": rule.get("name", ""),
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
    sample = []
    if n_violations > 0:
        sample = col_series.loc[violations_mask].drop_duplicates().head(20).tolist()

    return {
        "check_id": check_id,
        "sheet": sheet_name,
        "name": rule.get("name", ""),
        "type": "field_length",
        "total_rows": total,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


def run_all_consistency_checks(
    sheets_data: Dict[str, Any],
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Выполняет все включённые правила консистентности.
    Для referential и referential_composite — выполняет проверку и пишет колонки.
    Для unique и field_length — только собирает данные из уже заполненных колонок.
    Возвращает список записей для сводного листа (каждая запись — результат одной проверки).
    """
    rules = config.get("rules") or []
    enabled = [r for r in rules if r.get("enabled", True)]
    results: List[Dict[str, Any]] = []

    for rule in enabled:
        rule_type = rule.get("type", "")
        check_id = rule.get("id", "")
        try:
            if rule_type == "referential":
                res = run_referential(sheets_data, rule)
            elif rule_type == "referential_composite":
                res = run_referential_composite(sheets_data, rule)
            elif rule_type == "unique":
                res = collect_unique_result(sheets_data, rule)
            elif rule_type == "field_length":
                res = collect_field_length_result(sheets_data, rule)
            else:
                logging.debug(f"[consistency] Неизвестный тип правила: {rule_type}, id={check_id}")
                continue
            results.append(res)
        except Exception as e:
            logging.error(f"[consistency] Ошибка при выполнении правила {check_id} ({rule_type}): {e}")
            results.append({
                "check_id": check_id,
                "sheet": rule.get("sheet_src") or rule.get("sheet", ""),
                "name": rule.get("name", ""),
                "type": rule_type,
                "total_rows": 0,
                "violations": 0,
                "sample": [],
                "include_in_summary": True,
                "error": str(e),
            })

    return results


def build_consistency_summary_df(results: List[Dict[str, Any]]) -> pd.DataFrame:
    """Формирует DataFrame для сводного листа CONSISTENCY."""
    if not results:
        return pd.DataFrame(columns=[
            "check_id", "sheet", "name", "type", "total_rows", "violations", "sample"
        ])

    rows = []
    for r in results:
        if not r.get("include_in_summary", True):
            continue
        sample = r.get("sample") or []
        sample_str = "; ".join(str(x)[:80] for x in sample[:5])
        if len(sample) > 5:
            sample_str += " ..."
        rows.append({
            "check_id": r.get("check_id", ""),
            "sheet": r.get("sheet", ""),
            "name": r.get("name", ""),
            "type": r.get("type", ""),
            "total_rows": r.get("total_rows", 0),
            "violations": r.get("violations", 0),
            "sample": sample_str,
        })
    return pd.DataFrame(rows)


def log_and_console_consistency_report(results: List[Dict[str, Any]]) -> None:
    """
    Выводит итог проверок консистентности: в INFO — найдено/не найдено и какие;
    в DEBUG — подробности по каждой проверке.
    """
    with_violations = [r for r in results if r.get("violations", 0) > 0]
    if not with_violations:
        logging.info("Проверки консистентности: нарушений не найдено.")
        print("Проверки консистентности: нарушений не найдено.")
        return

    # INFO: кратко — найдено и по каким проверкам
    parts = [f"{r.get('check_id', '')} ({r.get('sheet', '')}) — {r.get('violations', 0)}" for r in with_violations]
    msg = "Проверки консистентности: найдено нарушений: " + "; ".join(parts)
    logging.info(msg)
    print(msg)

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


def run_consistency_checks_and_attach_summary(
    sheets_data: Dict[str, Any],
    config: Dict[str, Any],
) -> None:
    """
    Полный цикл: выполнить все проверки, добавить сводный лист в sheets_data,
    вывести отчёт в лог и консоль.
    """
    summary_sheet_name = config.get("summary_sheet_name", "CONSISTENCY")
    results = run_all_consistency_checks(sheets_data, config)
    df_summary = build_consistency_summary_df(results)
    params = {"sheet": summary_sheet_name, "max_col_width": 80, "col_width_mode": "AUTO", "min_col_width": 10}
    sheets_data[summary_sheet_name] = (df_summary, params)
    log_and_console_consistency_report(results)
