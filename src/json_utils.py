# -*- coding: utf-8 -*-
"""
Утилиты для разбора и разворота JSON-полей в DataFrame.
"""

import json
import logging
import re
import time as tmod
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import pandas as pd


def safe_json_loads(s: str) -> Any:
    """
    Преобразует строку в объект JSON. Возвращает dict/list или None при ошибке.
    Толерантен к разным кавычкам и пустым строкам; исправляет типичные ошибки JSON.
    """
    if not isinstance(s, str):
        return s
    s = s.strip()
    if not s or s in {"-", "None", "null"}:
        return None
    try:
        return json.loads(s)
    except Exception as ex:
        try:
            fixed = s
            fixed = fixed.replace('"""', '"')
            fixed = fixed.replace("'", '"').replace('"', '"').replace('"', '"')
            fixed = fixed.replace(''', '"').replace(''', '"')
            fixed = re.sub(r'"{2,}([^"\s]+)"{2,}', r'"\1"', fixed)
            fixed = re.sub(r'"{2,}([^"\s]+)"{2,}\s*:', r'"\1":', fixed)
            fixed = re.sub(r':\s*"{2,}([^"\s]+)"{2,}', r':"\1"', fixed)
            fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
            fixed = re.sub(r'(\"[^"]+\")\s+(\")', r'\1: \2', fixed)
            fixed = re.sub(r'(\"[^"]+\")\s*:\s*', r'\1:', fixed)
            return json.loads(fixed)
        except Exception as ex2:
            logging.debug(
                f"[safe_json_loads] Ошибка: первый парсинг {ex}, после исправления {ex2} | Исходная строка: {repr(s)}"
            )
            return None


def safe_json_loads_preserve_triple_quotes(s: str) -> Any:
    """
    Преобразует строку в объект JSON, сохраняя тройные кавычки как есть.
    Используется для обработки JSON из CSV с тройными кавычками.
    """
    if not isinstance(s, str):
        return s
    s = s.strip()
    if not s or s in {"-", "None", "null"}:
        return None
    try:
        return json.loads(s)
    except Exception as ex:
        logging.debug(
            f"[safe_json_loads_preserve_triple_quotes] Сохраняем исходную строку с тройными кавычками: {repr(s)}"
        )
        return s


def flatten_json_column_recursive(
    df: pd.DataFrame,
    column: str,
    prefix: Optional[str] = None,
    sheet: Optional[str] = None,
    sep: str = "; ",
    max_workers_io: int = 4,
) -> pd.DataFrame:
    """
    Рекурсивно разворачивает JSON-колонку в несколько колонок.
    Для больших данных (>5000 строк) использует параллельный парсинг.
    """
    func_start = tmod.time()
    n_rows = len(df)
    n_errors = 0
    prefix = prefix if prefix is not None else column
    logging.info(f"[START] flatten_json_column_recursive (лист: {sheet}, колонка: {column})")

    original_column_data = None
    temp_column = None
    if column == "CONTEST_FEATURE" and column in df.columns:
        original_column_data = df[column].copy()
        temp_column = f"{column}_TEMP_PARSED"
        df[temp_column] = df[column].apply(
            lambda x: x.replace('"""', '"') if isinstance(x, str) else x
        )
        column_to_parse = temp_column
    else:
        column_to_parse = column

    def extract(obj: Any, current_prefix: str) -> Dict[str, Any]:
        """Рекурсивно разворачивает объект; сохраняет поле и разворачивает вложенный JSON."""
        fields: Dict[str, Any] = {}
        if isinstance(obj, str):
            nested = safe_json_loads(obj)
            if isinstance(nested, (dict, list)):
                fields[current_prefix] = obj
                fields.update(extract(nested, current_prefix))
                return fields
            fields[current_prefix] = obj
            return fields
        if isinstance(obj, dict):
            fields[current_prefix] = json.dumps(obj, ensure_ascii=False)
            for k, v in obj.items():
                new_prefix = f"{current_prefix} => {k}"
                fields.update(extract(v, new_prefix))
        elif isinstance(obj, list):
            if all(isinstance(x, (str, int, float, bool, type(None))) for x in obj):
                fields[current_prefix] = sep.join(str(x) for x in obj)
            else:
                fields[current_prefix] = json.dumps(obj, ensure_ascii=False)
                for idx, x in enumerate(obj):
                    item_prefix = f"{current_prefix} => [{idx}]"
                    fields.update(extract(x, item_prefix))
        else:
            if isinstance(obj, float) and pd.isna(obj):
                fields[current_prefix] = None
            else:
                fields[current_prefix] = obj
        return fields

    new_cols: Dict[str, List[Any]] = {}
    PARALLEL_JSON_THRESHOLD = 5000

    if n_rows > PARALLEL_JSON_THRESHOLD:

        def parse_json_chunk(chunk_data: tuple) -> tuple:
            chunk_results: Dict[str, Dict[int, Any]] = {}
            chunk_errors = 0
            chunk_idx, chunk_values = chunk_data
            for local_idx, val in enumerate(chunk_values):
                global_idx = chunk_idx + local_idx
                try:
                    parsed = None
                    if isinstance(val, str):
                        val = val.strip()
                        if val in {"", "-", "None", "null"}:
                            parsed = {}
                        else:
                            parsed = safe_json_loads(val)
                    elif isinstance(val, (dict, list)):
                        parsed = val
                    else:
                        parsed = {}
                    flat = extract(parsed, prefix)
                except Exception as ex:
                    logging.debug(f"Ошибка разбора JSON (строка {global_idx}): {ex}")
                    chunk_errors += 1
                    flat = {}
                for k, v in flat.items():
                    if k not in chunk_results:
                        chunk_results[k] = {}
                    chunk_results[k][global_idx] = v
            return chunk_results, chunk_errors

        chunk_size = max(2000, n_rows // max_workers_io)
        chunks = [
            (
                i * chunk_size,
                df[column_to_parse].iloc[i * chunk_size : (i + 1) * chunk_size].tolist(),
            )
            for i in range((n_rows + chunk_size - 1) // chunk_size)
        ]
        if len(chunks) > 1:
            with ThreadPoolExecutor(max_workers=min(max_workers_io, len(chunks))) as executor:
                chunk_data_list = list(executor.map(parse_json_chunk, chunks))
                chunk_results_list = [data[0] for data in chunk_data_list]
                n_errors += sum(data[1] for data in chunk_data_list)
            for chunk_results in chunk_results_list:
                for k, v_dict in chunk_results.items():
                    if k not in new_cols:
                        new_cols[k] = [None] * n_rows
                    for idx, val in v_dict.items():
                        new_cols[k][idx] = val
        else:
            chunk_results, chunk_errors = parse_json_chunk(chunks[0])
            n_errors += chunk_errors
            for k, v_dict in chunk_results.items():
                if k not in new_cols:
                    new_cols[k] = [None] * n_rows
                for idx, val in v_dict.items():
                    new_cols[k][idx] = val
    else:
        for idx, val in enumerate(df[column_to_parse]):
            try:
                parsed = None
                if isinstance(val, str):
                    val = val.strip()
                    if val in {"", "-", "None", "null"}:
                        parsed = {}
                    else:
                        parsed = safe_json_loads(val)
                elif isinstance(val, (dict, list)):
                    parsed = val
                else:
                    parsed = {}
                flat = extract(parsed, prefix)
            except Exception as ex:
                logging.debug(f"Ошибка разбора JSON (строка {idx}): {ex}")
                n_errors += 1
                flat = {}
            for k, v in flat.items():
                if k not in new_cols:
                    new_cols[k] = [None] * n_rows
                new_cols[k][idx] = v

    for col_name, values in new_cols.items():
        if any(x is not None for x in values):
            df[col_name] = values

    if original_column_data is not None:
        df[column] = original_column_data
        if temp_column and temp_column in df.columns:
            df.drop(columns=[temp_column], inplace=True)
        logging.info("[CONTEST_FEATURE] Исходная колонка восстановлена с тройными кавычками")

    logging.info(f"[INFO] {column} → новых колонок: {len(new_cols)}")
    logging.info(f"[INFO] Все новые колонки: {list(new_cols.keys())}")
    return df
