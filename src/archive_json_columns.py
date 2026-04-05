# -*- coding: utf-8 -*-
"""
Разворот JSON из колонок CONTEST_FEATURE / REWARD_ADD_DATA для архива SQLite (листы CONTEST-DATA, REWARD).

Нормализация строки по каталогу Docs/JSON/SPOD_INPUT_DATA_CATALOG.md (тройные кавычки, внешние кавычки),
затем json.loads через safe_json_loads. Имена колонок в БД: JSON_<имя_исходной_колонки>_<путь_ключей>
только для листьев (скаляры или «сплющенные» массивы примитивов). Вложенные объекты и массивы объектов
разворачиваются по пути; при совпадении имён листьев путь задаёт префикс автоматически.

Исходные колонки CSV в таблице arch_* не удаляются — добавляются только дополнительные JSON_* в конец.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from src.json_utils import safe_json_loads

# Лист → колонка CSV с JSON (как в SPOD_INPUT_DATA_CATALOG.md)
ARCHIVE_JSON_SOURCE_COLUMN: Dict[str, str] = {
    "CONTEST-DATA": "CONTEST_FEATURE",
    "REWARD": "REWARD_ADD_DATA",
}


def _sanitize_sql_name(name: str, max_len: int = 200) -> str:
    """Допустимое имя колонки SQLite (как в input_archive_sqlite._sanitize_column)."""
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", str(name)).strip("_")
    if not s:
        s = "col"
    if s.upper().startswith("ARCH__"):
        s = "d_" + s
    return s[:max_len]


def _is_scalar_leaf(x: Any) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    return isinstance(x, (str, int, float, bool))


def _scalar_to_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def normalize_spod_json_cell(raw: Any) -> str:
    """
    Приведение ячейки CSV к строке для json.loads: тройные кавычки → обычные ",
    снятие внешних кавычек, если вся ячейка — JSON в кавычках.
    """
    if raw is None:
        return ""
    if isinstance(raw, float) and pd.isna(raw):
        return ""
    s = str(raw).strip()
    if not s or s in {"-", "None", "null"}:
        return ""
    # Типичный формат выгрузки SPOD (см. каталог)
    s = s.replace('"""', '"')
    # Пока строка целиком обёрнута в кавычки и внутри — объект/массив JSON
    while len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        inner = s[1:-1].strip()
        if inner.startswith("{") or inner.startswith("["):
            s = inner
        else:
            break
    return s.strip()


def parse_archive_json_cell(raw: Any) -> Any:
    """Нормализация + разбор JSON; при ошибке — None."""
    norm = normalize_spod_json_cell(raw)
    if not norm:
        return None
    return safe_json_loads(norm)


def _flatten_to_path_values(value: Any) -> Dict[Tuple[str, ...], str]:
    """
    Рекурсивно собирает только «листья»: ключ — кортеж сегментов пути от корня объекта
    (пустой кортеж — корневой скаляр или корневой массив целиком).
    """
    out: Dict[Tuple[str, ...], str] = {}

    def walk(val: Any, path: Tuple[str, ...]) -> None:
        if isinstance(val, dict):
            if not val:
                out[path] = "{}"
                return
            for k, v in val.items():
                walk(v, path + (str(k),))
            return
        if isinstance(val, list):
            if not val:
                out[path] = ""
                return
            if all(_is_scalar_leaf(x) for x in val):
                out[path] = "; ".join(_scalar_to_text(x) for x in val)
                return
            for i, x in enumerate(val):
                walk(x, path + (str(i),))
            return
        out[path] = _scalar_to_text(val)

    if value is None:
        return out
    if isinstance(value, dict):
        walk(value, ())
        return out
    if isinstance(value, list):
        walk(value, ())
        return out
    out[()] = _scalar_to_text(value)
    return out


def _allocate_sql_names_for_paths(
    root_csv_column: str,
    all_paths: Set[Tuple[str, ...]],
    reserved_lower: Set[str],
) -> Dict[Tuple[str, ...], str]:
    """
    Стабильные имена колонок: JSON_<колонка>_<ключ1>_<ключ2>...
    При коллизии с уже занятыми (из CSV) — суффикс _j2, _j3, ...
    """
    taken = set(reserved_lower)
    result: Dict[Tuple[str, ...], str] = {}
    for path in sorted(all_paths, key=lambda p: (len(p), p)):
        parts = ["JSON", root_csv_column] + [p.replace(" ", "_") for p in path]
        base = _sanitize_sql_name("_".join(parts))
        candidate = base
        n = 2
        while candidate.lower() in taken:
            candidate = _sanitize_sql_name(f"{base}_j{n}")
            n += 1
        taken.add(candidate.lower())
        result[path] = candidate
    return result


def plan_archive_json_flat_columns(
    sheet_name: str,
    df: pd.DataFrame,
    reserved_sql_names_lower: Set[str],
) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Для CONTEST-DATA / REWARD: собирает объединение путей по всем строкам DataFrame,
    выдаёт отсортированный список имён колонок SQLite и по строке — значения {sql_col: text}.

    Исходная колонка CONTEST_FEATURE / REWARD_ADD_DATA в CSV не трогается; в архив она уже попадает
    как обычное поле, сюда только дополнительные JSON_*.
    """
    src = ARCHIVE_JSON_SOURCE_COLUMN.get(sheet_name)
    if not src or src not in df.columns:
        return [], []

    per_row_paths: List[Dict[Tuple[str, ...], str]] = []
    union: Set[Tuple[str, ...]] = set()

    for _, row in df.iterrows():
        parsed = parse_archive_json_cell(row.get(src))
        pv = _flatten_to_path_values(parsed)
        per_row_paths.append(pv)
        union.update(pv.keys())

    if not union:
        return [], [{} for _ in range(len(per_row_paths))]

    path_to_sql = _allocate_sql_names_for_paths(src, union, reserved_sql_names_lower)
    sql_columns = sorted(path_to_sql.values())

    row_values: List[Dict[str, str]] = []
    for pv in per_row_paths:
        d: Dict[str, str] = {}
        for path, text in pv.items():
            sql_n = path_to_sql.get(path)
            if sql_n is not None:
                d[sql_n] = text
        row_values.append(d)

    return sql_columns, row_values


def ensure_extra_text_columns(
    cur: Any,
    table: str,
    column_names: List[str],
    quote_ident: Any,
    existing_columns_lower: Set[str],
) -> None:
    """ALTER TABLE ADD COLUMN для отсутствующих имён (в конец таблицы в SQLite)."""
    for name in column_names:
        if name.lower() in existing_columns_lower:
            continue
        cur.execute(f"ALTER TABLE {quote_ident(table)} ADD COLUMN {quote_ident(name)} TEXT")
        existing_columns_lower.add(name.lower())


def update_json_flat_for_snapshot_rows(
    cur: Any,
    table: str,
    snapshot_id_col: str,
    row_ix_col: str,
    snapshot_id: int,
    json_flat_cols: List[str],
    json_row_maps: List[Dict[str, str]],
    quote_ident: Any,
) -> None:
    """
    Заполняет колонки JSON_* у существующих строк снимка (когда новый INSERT не выполнялся).
    По одному UPDATE на строку: WHERE snapshot_id_col = id AND row_ix_col = индекс строки.
    """
    if not json_flat_cols:
        return
    q_snap = quote_ident(snapshot_id_col)
    q_row = quote_ident(row_ix_col)
    q_tbl = quote_ident(table)
    set_clause = ", ".join(f"{quote_ident(c)} = ?" for c in json_flat_cols)
    sql = f"UPDATE {q_tbl} SET {set_clause} WHERE {q_snap} = ? AND {q_row} = ?"
    for ix, jmap in enumerate(json_row_maps):
        vals: List[Any] = [jmap.get(c, "") for c in json_flat_cols] + [snapshot_id, ix]
        cur.execute(sql, vals)
