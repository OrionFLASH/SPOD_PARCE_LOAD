# -*- coding: utf-8 -*-
"""
Канонизация полей строки CSV и SHA-256 для построчного архива SQLite (v2).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

ROW_STATUS_ACTIVE = "active"
ROW_STATUS_INACTIVE = "inactive"
ROW_STATUS_SUPERSEDED = "superseded"


def _norm_cell(value: Any) -> str:
    """Нормализация ячейки для хеша (как в плане: strip, пусто/NaN → '')."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if s in ("-", "None", "null"):
        return ""
    return s


def series_to_field_dict(row: pd.Series) -> Dict[str, str]:
    """Строка DataFrame → словарь имя_колонки → строка."""
    return {str(c): _norm_cell(row[c]) for c in row.index}


def dict_to_series(fields: Dict[str, str], columns: Sequence[str]) -> pd.Series:
    """Восстановление Series по списку колонок листа."""
    return pd.Series({c: fields.get(c, "") for c in columns}, dtype=object)


def canonical_json_object(fields: Mapping[str, str], keys: Sequence[str]) -> str:
    """Канонический JSON для ключа или тела: сортировка ключей, значения после strip."""
    ordered = {k: _norm_cell(fields[k]) for k in sorted(keys) if k in fields}
    return json.dumps(ordered, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def compute_row_key(
    fields: Mapping[str, str],
    key_columns: Sequence[str],
) -> Tuple[str, str, str]:
    """
    Возвращает (row_key_hash, row_key_json, error_message).
    error_message непустой при отсутствующих колонках ключа.
    """
    missing = [c for c in key_columns if c not in fields]
    if missing:
        return "", "", f"отсутствуют колонки ключа: {missing[:5]}"
    key_json = canonical_json_object(fields, key_columns)
    return sha256_hex(key_json), key_json, ""


def compute_row_hash(
    fields: Mapping[str, str],
    hash_columns: Optional[Sequence[str]] = None,
) -> str:
    """Хеш содержимого строки: все колонки или явный список hash_columns."""
    if hash_columns is None:
        cols = sorted(fields.keys())
    else:
        cols = list(hash_columns)
    body_json = canonical_json_object(fields, cols)
    return sha256_hex(body_json)


def compute_row_hashes_from_series(
    row: pd.Series,
    key_columns: Sequence[str],
    hash_columns: Optional[Sequence[str]] = None,
) -> Tuple[str, str, str, Dict[str, str]]:
    """
    Полный расчёт для одной строки: key_hash, key_json, row_hash, fields dict.
    Пустой key_hash если ключ невалиден.
    """
    fields = series_to_field_dict(row)
    key_hash, key_json, err = compute_row_key(fields, key_columns)
    if err:
        return "", key_json, "", fields
    row_hash = compute_row_hash(fields, hash_columns)
    return key_hash, key_json, row_hash, fields
