# -*- coding: utf-8 -*-
"""
Модуль проверок консистентности данных.
Выполняет правила из конфига consistency_checks: создаёт колонки unique (ДУБЛЬ: …) и field_length
на листах, затем referential/referential_composite, собирает результаты в свод CONSISTENCY.
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
        return {"check_id": check_id, "sheet": sheet_src, "name": rule.get("name", ""), "column_on_sheet": col_out, "type": "referential", "total_rows": 0, "violations": 0, "sample": [], "include_in_summary": True}
    if df_ref is None:
        logging.debug(f"[consistency] referential {check_id}: справочник {sheet_ref} отсутствует, помечаем все как нарушение")
    if column_src not in df_src.columns:
        logging.warning(f"[consistency] referential {check_id}: колонка {column_src} не найдена на {sheet_src}")
        return {"check_id": check_id, "sheet": sheet_src, "name": rule.get("name", ""), "column_on_sheet": col_out, "type": "referential", "total_rows": len(df_src), "violations": len(df_src), "sample": [], "include_in_summary": True}

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
        "column_on_sheet": col_out,
        "type": "referential_composite",
        "total_rows": total,
        "violations": n_violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }


def _run_unique_check(sheets_data: Dict[str, Any], rule: Dict[str, Any]) -> None:
    """
    Создаёт на листе колонку с пометкой дублей по key_columns (значение «xN» или пусто).
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
        dup_counts = df.groupby(key_columns)[key_columns[0]].transform("count")
        df = df.copy()
        df[col_name] = dup_counts.map(lambda x: f"x{x}" if x > 1 else "")
        sheets_data[sheet_name] = (df, conf)
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
    total = len(df)
    sample = []
    if n_violations > 0 and key_columns and all(c in df.columns for c in key_columns):
        # Группируем строки с дублями по ключу, в sample — значения ключа и номера строк (как в Excel: строка 1 = заголовок)
        dup_df = df.loc[violations_mask, key_columns + [col_name]].copy()
        dup_df["_row"] = dup_df.index
        grouped = dup_df.groupby(key_columns, dropna=False)
        for key_vals, grp in grouped:
            if len(grp) < 2:
                continue
            vals = key_vals if isinstance(key_vals, tuple) else (key_vals,)
            key_parts = [f"{k}={v}" for k, v in zip(key_columns, vals)]
            key_str = ", ".join(str(p) for p in key_parts)
            excel_rows = sorted(grp["_row"].astype(int).tolist())
            row_str = ", ".join(str(r + 2) for r in excel_rows)  # +2: Excel: строка 1 — заголовок
            sample.append(f"({key_str}) — строки: {row_str} (дублей: {len(grp)})")
            if len(sample) >= 20:
                break
    elif n_violations > 0:
        # Запасной вариант: без ключей показываем хотя бы номера строк
        dup_idx = df.index[violations_mask].tolist()[:20]
        sample = [f"строка {i + 2}" for i in dup_idx]

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
    sample = []
    if n_violations > 0:
        sample = col_series.loc[violations_mask].drop_duplicates().head(20).tolist()

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


def run_all_consistency_checks(
    sheets_data: Dict[str, Any],
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Выполняет все включённые правила консистентности.
    Сначала создаёт колонки unique (ДУБЛЬ: …) на листах, затем referential/referential_composite,
    затем собирает результаты unique и field_length для сводки.
    Возвращает список записей для сводного листа (каждая запись — результат одной проверки).
    """
    rules = config.get("rules") or []
    enabled = [r for r in rules if r.get("enabled", True)]
    # Фаза 1: создаём колонки unique и field_length на листах (замена check_duplicates и field_length_validations)
    for rule in enabled:
        if rule.get("type") == "unique":
            _run_unique_check(sheets_data, rule)
        elif rule.get("type") == "field_length":
            _run_field_length_check(sheets_data, rule)
    # Фаза 2: выполняем проверки и собираем результаты
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
            _out = rule.get("output") or {}
            results.append({
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

    return results


def build_consistency_summary_df(results: List[Dict[str, Any]]) -> pd.DataFrame:
    """Формирует DataFrame для сводного листа CONSISTENCY."""
    if not results:
        return pd.DataFrame(columns=[
            "check_id", "sheet", "name", "имя_колонки", "type", "total_rows", "violations", "sample"
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
            "имя_колонки": r.get("column_on_sheet", ""),
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
