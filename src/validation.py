# -*- coding: utf-8 -*-
"""
Валидация длины полей и проверка дубликатов по конфигурации.
"""

import logging
import threading
import time
import time as tmod
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from src.config_loader import Config


def validate_field_lengths(config: Config, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """
    Проверяет длину полей по config.field_length_validations.
    Добавляет колонку с результатом: "-" или строка нарушений.
    """
    func_start = time.time()
    if sheet_name not in config.field_length_validations:
        return df

    cfg = config.field_length_validations[sheet_name]
    result_column = cfg["result_column"]
    fields_config = cfg["fields"]

    missing_fields = [f for f in fields_config if f not in df.columns]
    if missing_fields:
        logging.warning(f"[FIELD LENGTH] Пропущены поля {missing_fields} в листе {sheet_name}")
        df[result_column] = "-"
        return df

    total_rows = len(df)
    logging.info(f"[FIELD LENGTH] Проверка длины полей для листа {sheet_name}, строк: {total_rows}")

    def check_field_length(value: Any, limit: int, operator: str) -> bool:
        if pd.isna(value) or value in ["", "-", "None", "null"]:
            return True
        length = len(str(value))
        if operator == "<=":
            return length <= limit
        if operator == "=":
            return length == limit
        if operator == ">=":
            return length >= limit
        if operator == "<":
            return length < limit
        if operator == ">":
            return length > limit
        return True

    def check_row(row: pd.Series, row_idx: int) -> str:
        violations = []
        for field_name, field_config in fields_config.items():
            limit = field_config["limit"]
            operator = field_config["operator"]
            value = row.get(field_name, "")
            if not check_field_length(value, limit, operator):
                length = len(str(value)) if not pd.isna(value) else 0
                violations.append(f"{field_name} = {length} {operator} {limit}")
                logging.debug(
                    f"[DEBUG] Строка {row_idx}: поле '{field_name}' = {length} {operator} {limit} (нарушение)"
                )
        return "; ".join(violations) if violations else "-"

    results = []
    correct_count = 0
    error_count = 0
    step = config.gender_progress_step
    for idx, row in df.iterrows():
        result = check_row(row, idx)
        results.append(result)
        if result == "-":
            correct_count += 1
        else:
            error_count += 1
        if (idx + 1) % step == 0:
            percent = ((idx + 1) / total_rows) * 100
            logging.info(f"[FIELD LENGTH] Обработано {idx + 1} из {total_rows} строк ({percent:.1f}%)")

    df[result_column] = results
    func_time = time.time() - func_start
    logging.info(
        f"[FIELD LENGTH] Статистика: корректных={correct_count}, с ошибками={error_count} (всего: {total_rows})"
    )
    logging.info(f"[FIELD LENGTH] Завершено за {func_time:.3f}s для листа {sheet_name}")
    return df


def validate_field_lengths_vectorized(config: Config, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """Векторизованная проверка длины полей."""
    func_start = time.time()
    if sheet_name not in config.field_length_validations:
        return df

    cfg = config.field_length_validations[sheet_name]
    result_column = cfg["result_column"]
    fields_config = cfg["fields"]

    missing_fields = [f for f in fields_config if f not in df.columns]
    if missing_fields:
        logging.warning(
            f"[FIELD LENGTH VECTORIZED] Пропущены поля {missing_fields} в листе {sheet_name}"
        )
        df[result_column] = "-"
        return df

    total_rows = len(df)
    logging.info(
        f"[FIELD LENGTH VECTORIZED] Проверка длины полей для листа {sheet_name}, строк: {total_rows}"
    )

    violations_dict: Dict[str, pd.Series] = {}
    for field_name, field_config in fields_config.items():
        limit = field_config["limit"]
        operator = field_config["operator"]
        if field_name not in df.columns:
            continue
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
            for idx in df.index[mask]:
                logging.debug(
                    f"[DEBUG] Строка {idx}: поле '{field_name}' = {len(str(df.loc[idx, field_name]))} {operator} {limit} (нарушение)"
                )

    if violations_dict:
        violations_df = pd.DataFrame(violations_dict)
        violations_series = violations_df.apply(
            lambda row: "; ".join([str(v) for v in row if v and str(v).strip()]),
            axis=1,
        )
        df[result_column] = violations_series.replace("", "-")
    else:
        df[result_column] = "-"

    correct_count = (df[result_column] == "-").sum()
    error_count = total_rows - correct_count
    func_time = time.time() - func_start
    logging.info(
        f"[FIELD LENGTH VECTORIZED] Статистика: корректных={correct_count}, с ошибками={error_count} (всего: {total_rows})"
    )
    logging.info(
        f"[FIELD LENGTH VECTORIZED] Завершено за {func_time:.3f}s для листа {sheet_name}"
    )
    return df


def compare_validate_results(
    df_old: pd.DataFrame, df_new: pd.DataFrame, result_column: str
) -> Dict[str, Any]:
    """Сравнивает результаты двух версий валидации."""
    if result_column not in df_old.columns or result_column not in df_new.columns:
        return {"error": "Колонка с результатами не найдена"}
    old_results = df_old[result_column].fillna("-")
    new_results = df_new[result_column].fillna("-")
    differences = (old_results != new_results).sum()
    total = len(df_old)
    matches = total - differences
    diff_examples = []
    if differences > 0:
        diff_mask = old_results != new_results
        diff_indices = df_old.index[diff_mask][:5]
        for idx in diff_indices:
            diff_examples.append(
                {"index": idx, "old": old_results.loc[idx], "new": new_results.loc[idx]}
            )
    return {
        "total": total,
        "matches": matches,
        "differences": differences,
        "match_percent": (matches / total * 100) if total > 0 else 0,
        "diff_examples": diff_examples,
        "identical": differences == 0,
    }


def mark_duplicates(
    df: Optional[pd.DataFrame],
    key_cols: List[str],
    sheet_name: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """
    Добавляет колонку с пометкой о дублях по key_cols.
    Если строк по ключу больше одной — пишем xN, иначе пусто.
    """
    if df is None:
        logging.warning(
            f"[mark_duplicates] DataFrame для листа {sheet_name} равен None, пропускаем проверку дублей"
        )
        return df
    func_start = tmod.time()
    col_name = "ДУБЛЬ: " + "_".join(key_cols)
    logging.info(f"[START] Проверка дублей: {sheet_name}, ключ: {key_cols}")
    try:
        dup_counts = df.groupby(key_cols)[key_cols[0]].transform("count")
        df[col_name] = dup_counts.map(lambda x: f"x{x}" if x > 1 else "")
        n_duplicates = (df[col_name] != "").sum()
        func_time = tmod.time() - func_start
        logging.info(
            f"[INFO] Дублей найдено: {n_duplicates} на листе {sheet_name} по ключу {key_cols}"
        )
        logging.info(f"[END] Проверка дублей: {sheet_name}, время: {func_time:.3f}s")
    except Exception as ex:
        func_time = tmod.time() - func_start
        logging.error(
            f"[ERROR] Ошибка при поиске дублей: {sheet_name}, ключ: {key_cols}: {ex}"
        )
        logging.info(f"[END] Проверка дублей: {sheet_name}, время: {func_time:.3f}s")
    return df


def validate_single_sheet(
    config: Config,
    sheet_name: str,
    sheets_data_item: Optional[Tuple[pd.DataFrame, Any]],
) -> Tuple[str, Optional[Tuple[pd.DataFrame, Any]]]:
    """Проверяет длину полей для одного листа (для параллельного вызова)."""
    if sheets_data_item is None:
        logging.warning(
            f"[validate_single_sheet] Данные для листа {sheet_name} равны None, пропускаем"
        )
        return sheet_name, sheets_data_item
    try:
        df, conf = sheets_data_item
        if df is None or conf is None:
            logging.warning(
                f"[validate_single_sheet] DataFrame или конфигурация для листа {sheet_name} равны None, пропускаем"
            )
            return sheet_name, sheets_data_item
        df_old = df.copy()
        df_validated = validate_field_lengths_vectorized(config, df, sheet_name)
        if sheet_name in config.field_length_validations:
            result_column = config.field_length_validations[sheet_name]["result_column"]
            comparison = compare_validate_results(df_old, df_validated, result_column)
            if not comparison.get("identical", False):
                logging.warning(
                    f"[VALIDATE COMPARISON] {sheet_name}: различия найдены - {comparison.get('differences', 0)} из {comparison.get('total', 0)}"
                )
                df_validated = validate_field_lengths(config, df_old, sheet_name)
                logging.warning(f"[VALIDATE FALLBACK] {sheet_name}: использована оригинальная версия")
            else:
                logging.info(
                    f"[VALIDATE COMPARISON] {sheet_name}: результаты идентичны ({comparison.get('match_percent', 0)}%)"
                )
        else:
            df_validated = df
        logging.debug(
            f"Проверка длины полей завершена: {sheet_name} [поток: {threading.current_thread().name}]"
        )
        return sheet_name, (df_validated, conf)
    except Exception as e:
        logging.error(
            f"Ошибка проверки длины полей для {sheet_name}: {e} [поток: {threading.current_thread().name}]"
        )
        return sheet_name, sheets_data_item


def check_duplicates_single_sheet(
    config: Config,
    sheet_name: str,
    sheets_data_item: Optional[Tuple[Any, Any]],
) -> Tuple[str, Optional[Tuple[Any, Any]]]:
    """Проверяет дубликаты для одного листа (для параллельного вызова)."""
    if sheets_data_item is None:
        logging.warning(
            f"[check_duplicates_single_sheet] Данные для листа {sheet_name} равны None, пропускаем"
        )
        return sheet_name, sheets_data_item
    try:
        df, conf = sheets_data_item
        if df is None or conf is None:
            logging.warning(
                f"[check_duplicates_single_sheet] DataFrame или конфигурация для листа {sheet_name} равны None, пропускаем"
            )
            return sheet_name, sheets_data_item
        check_configs = [x for x in config.check_duplicates if x["sheet"] == sheet_name]
        for check_cfg in check_configs:
            df = mark_duplicates(df, check_cfg["key"], sheet_name=sheet_name)
        if check_configs:
            logging.debug(
                f"Проверка дубликатов завершена: {sheet_name} [поток: {threading.current_thread().name}]"
            )
        return sheet_name, (df, conf)
    except Exception as e:
        logging.error(
            f"Ошибка проверки дубликатов для {sheet_name}: {e} [поток: {threading.current_thread().name}]"
        )
        return sheet_name, sheets_data_item
