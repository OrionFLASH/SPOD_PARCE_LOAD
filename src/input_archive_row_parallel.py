# -*- coding: utf-8 -*-
"""
Параллельный расчёт хешей строк и классификация new / unchanged / changed (архив v2).
"""

from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from src.input_archive_row_hash import (
    compute_row_hash,
    compute_row_key,
    series_to_field_dict,
)

# Типы для классификации
CLASS_NEW = "new"
CLASS_UNCHANGED = "unchanged"
CLASS_CHANGED = "changed"
CLASS_DUPLICATE_KEY = "duplicate_key"


@dataclass
class RowHashRecord:
    """Результат расчёта хешей для одной строки файла."""

    row_index: int
    row_key_hash: str
    row_key_json: str
    row_hash: str
    fields: Dict[str, str]
    error: str = ""


@dataclass
class ClassifiedRow:
    """Строка после сравнения с БД."""

    record: RowHashRecord
    kind: str
    existing_payload_id: Optional[int] = None
    existing_row_hash: Optional[str] = None


def _resolve_workers(cfg: Mapping[str, Any]) -> int:
    raw = int(cfg.get("max_workers") or 0)
    if raw > 0:
        return raw
    cpu = os.cpu_count() or 2
    return max(1, min(8, cpu - 1))


def merge_parallel_config(raw: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Слияние parallel_row_processing с дефолтами."""
    base: Dict[str, Any] = {
        "enabled": True,
        "max_workers": 0,
        "chunk_size": 2000,
        "min_rows_for_parallel": 500,
    }
    if isinstance(raw, dict):
        base.update(raw)
    return base


def _chunk_indices(n: int, chunk_size: int) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []
    i = 0
    while i < n:
        out.append((i, min(i + chunk_size, n)))
        i += chunk_size
    return out


def _compute_chunk_worker(args: Tuple) -> List[RowHashRecord]:
    """
    Воркер: (rows_as_dicts, key_columns, hash_columns, start_index).
    rows_as_dicts — список dict полей строки.
    """
    rows_as_dicts, key_columns, hash_columns, start_index = args
    key_cols = list(key_columns)
    hash_cols = list(hash_columns) if hash_columns else None
    out: List[RowHashRecord] = []
    for offset, fields in enumerate(rows_as_dicts):
        idx = start_index + offset
        key_hash, key_json, err = compute_row_key(fields, key_cols)
        if err:
            out.append(
                RowHashRecord(
                    row_index=idx,
                    row_key_hash="",
                    row_key_json=key_json,
                    row_hash="",
                    fields=dict(fields),
                    error=err,
                )
            )
            continue
        row_hash = compute_row_hash(fields, hash_cols)
        out.append(
            RowHashRecord(
                row_index=idx,
                row_key_hash=key_hash,
                row_key_json=key_json,
                row_hash=row_hash,
                fields=dict(fields),
            )
        )
    return out


def _classify_chunk_worker(args: Tuple) -> List[ClassifiedRow]:
    """Воркер классификации: (records_dicts, existing_map)."""
    records_data, existing_map = args
    classified: List[ClassifiedRow] = []
    for rd in records_data:
        rec = RowHashRecord(**rd)
        if not rec.row_key_hash or rec.error:
            classified.append(ClassifiedRow(record=rec, kind=CLASS_NEW))
            continue
        prev = existing_map.get(rec.row_key_hash)
        if prev is None:
            classified.append(ClassifiedRow(record=rec, kind=CLASS_NEW))
            continue
        prev_hash, prev_pid = prev
        if prev_hash == rec.row_hash:
            classified.append(
                ClassifiedRow(
                    record=rec,
                    kind=CLASS_UNCHANGED,
                    existing_payload_id=prev_pid,
                    existing_row_hash=prev_hash,
                )
            )
        else:
            classified.append(
                ClassifiedRow(
                    record=rec,
                    kind=CLASS_CHANGED,
                    existing_payload_id=prev_pid,
                    existing_row_hash=prev_hash,
                )
            )
    return classified


def dataframe_to_row_dicts(df) -> List[Dict[str, str]]:
    """DataFrame → список словарей полей (для picklable воркеров)."""
    rows: List[Dict[str, str]] = []
    for i in range(len(df)):
        rows.append(series_to_field_dict(df.iloc[i]))
    return rows


def compute_row_hashes_parallel(
    row_dicts: List[Dict[str, str]],
    key_columns: Sequence[str],
    hash_columns: Optional[Sequence[str]],
    parallel_cfg: Mapping[str, Any],
) -> List[RowHashRecord]:
    """Расчёт хешей по всем строкам; при малом объёме — в одном процессе."""
    cfg = merge_parallel_config(parallel_cfg)
    n = len(row_dicts)
    if n == 0:
        return []
    chunk_size = max(1, int(cfg.get("chunk_size") or 2000))
    min_parallel = int(cfg.get("min_rows_for_parallel") or 500)
    use_pool = bool(cfg.get("enabled", True)) and n >= min_parallel
    chunks = _chunk_indices(n, chunk_size)
    tasks = [
        (row_dicts[a:b], list(key_columns), list(hash_columns) if hash_columns else None, a)
        for a, b in chunks
    ]
    if not use_pool:
        merged: List[RowHashRecord] = []
        for t in tasks:
            merged.extend(_compute_chunk_worker(t))
        return merged
    workers = _resolve_workers(cfg)
    merged = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for part in ex.map(_compute_chunk_worker, tasks, chunksize=1):
            merged.extend(part)
    merged.sort(key=lambda r: r.row_index)
    return merged


def dedupe_by_key_last_wins(records: List[RowHashRecord]) -> Tuple[List[RowHashRecord], int]:
    """
    Дубликаты row_key_hash в одном файле: последняя строка побеждает.
    Возвращает (уникальные записи, число предупреждений о дубликатах).
    """
    by_key: Dict[str, RowHashRecord] = {}
    dup_count = 0
    for rec in sorted(records, key=lambda r: r.row_index):
        if not rec.row_key_hash:
            continue
        if rec.row_key_hash in by_key:
            dup_count += 1
        by_key[rec.row_key_hash] = rec
    return list(by_key.values()), dup_count


def classify_rows_parallel(
    records: List[RowHashRecord],
    existing_map: Dict[str, Tuple[str, Optional[int]]],
    parallel_cfg: Mapping[str, Any],
) -> List[ClassifiedRow]:
    """
    Сравнение с картой из БД: key_hash → (row_hash, payload_id).
    existing_map picklable (plain dict).
    """
    cfg = merge_parallel_config(parallel_cfg)
    n = len(records)
    if n == 0:
        return []
    chunk_size = max(1, int(cfg.get("chunk_size") or 2000))
    min_parallel = int(cfg.get("min_rows_for_parallel") or 500)
    use_pool = bool(cfg.get("enabled", True)) and n >= min_parallel
    chunks = _chunk_indices(n, chunk_size)

    def _rec_to_dict(r: RowHashRecord) -> Dict[str, Any]:
        return {
            "row_index": r.row_index,
            "row_key_hash": r.row_key_hash,
            "row_key_json": r.row_key_json,
            "row_hash": r.row_hash,
            "fields": r.fields,
            "error": r.error,
        }

    tasks = [
        ([_rec_to_dict(records[i]) for i in range(a, b)], existing_map)
        for a, b in chunks
    ]
    if not use_pool:
        merged: List[ClassifiedRow] = []
        for t in tasks:
            merged.extend(_classify_chunk_worker(t))
        return merged
    workers = _resolve_workers(cfg)
    merged = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for part in ex.map(_classify_chunk_worker, tasks, chunksize=1):
            merged.extend(part)
    return merged


def count_by_kind(classified: List[ClassifiedRow]) -> Dict[str, int]:
    """Счётчики new / unchanged / changed / errors."""
    stats = {"new": 0, "unchanged": 0, "changed": 0, "key_errors": 0, "inactive_marked": 0}
    for c in classified:
        if c.record.error or not c.record.row_key_hash:
            stats["key_errors"] += 1
            continue
        if c.kind == CLASS_NEW:
            stats["new"] += 1
        elif c.kind == CLASS_UNCHANGED:
            stats["unchanged"] += 1
        elif c.kind == CLASS_CHANGED:
            stats["changed"] += 1
    return stats
