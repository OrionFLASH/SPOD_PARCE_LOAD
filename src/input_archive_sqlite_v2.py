# -*- coding: utf-8 -*-
"""
Построчный архив входных CSV в SQLite (schema v2).

Отдельный файл БД от v1 (снимки целого файла). Включение: input_archive_sqlite.row_level_archive.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd

from src import console_ui
from src.input_archive_row_hash import ROW_STATUS_ACTIVE, ROW_STATUS_INACTIVE
from src.input_archive_row_parallel import (
    CLASS_CHANGED,
    CLASS_NEW,
    CLASS_UNCHANGED,
    ClassifiedRow,
    classify_rows_parallel,
    compute_row_hashes_parallel,
    count_by_kind,
    dataframe_to_row_dicts,
    dedupe_by_key_last_wins,
    merge_parallel_config,
)
from src.input_archive_sqlite import (
    _archive_reporting_modes,
    _hash_file,
    _log_archive_event,
    _log_archive_summary_line,
    merge_archive_config,
)

TABLE_CURRENT = "archive_row_current"
TABLE_PAYLOAD = "archive_row_payload"
TABLE_INGEST_RUN = "archive_ingest_run"
TABLE_FILE_INVENTORY = "archive_file_row_inventory"
SCHEMA_VERSION = 2


def _defaults_row_level() -> Dict[str, Any]:
    return {
        "row_level_archive": False,
        "schema_version": SCHEMA_VERSION,
        "db_path": "OUT/DB/spod_input_archive_v2.sqlite",
        "legacy_db_path": "OUT/DB/spod_input_archive.sqlite",
        "row_hash_columns": None,
        "parallel_row_processing": {
            "enabled": True,
            "max_workers": 0,
            "chunk_size": 2000,
            "min_rows_for_parallel": 500,
        },
        "skip_ingest_if_file_unchanged": True,
        "default_row_key_by_sheet": {},
    }


def merge_archive_v2_config(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Конфиг v1 + поля построчного архива."""
    cfg = merge_archive_config(raw)
    row_defaults = _defaults_row_level()
    for k, v in row_defaults.items():
        if k not in cfg:
            cfg[k] = v
    if isinstance(raw, dict):
        if "parallel_row_processing" in raw and isinstance(raw["parallel_row_processing"], dict):
            cfg["parallel_row_processing"] = merge_parallel_config(
                {**row_defaults["parallel_row_processing"], **raw["parallel_row_processing"]}
            )
        if "default_row_key_by_sheet" in raw and isinstance(raw["default_row_key_by_sheet"], dict):
            cfg["default_row_key_by_sheet"] = dict(raw["default_row_key_by_sheet"])
        for k in ("row_level_archive", "schema_version", "legacy_db_path", "row_hash_columns", "skip_ingest_if_file_unchanged"):
            if k in raw:
                cfg[k] = raw[k]
        if raw.get("row_level_archive") and "db_path" in raw:
            cfg["db_path"] = raw["db_path"]
    if cfg.get("row_level_archive") and str(cfg.get("db_path", "")).endswith("spod_input_archive.sqlite"):
        cfg["db_path"] = row_defaults["db_path"]
    return cfg


def resolve_row_key_columns(
    sheet_name: str,
    file_conf: Mapping[str, Any],
    cfg: Mapping[str, Any],
) -> Optional[List[str]]:
    """Ключ строки: из entry, иначе default_row_key_by_sheet, иначе шаблоны RATING_/ORDER_."""
    explicit = file_conf.get("row_key_columns")
    if isinstance(explicit, list) and explicit:
        return [str(c) for c in explicit]
    by_sheet = cfg.get("default_row_key_by_sheet") or {}
    if sheet_name in by_sheet:
        v = by_sheet[sheet_name]
        return [str(c) for c in v] if isinstance(v, list) else None
    if sheet_name.startswith("RATING"):
        v = by_sheet.get("RATING_*") or by_sheet.get("RATING")
        if isinstance(v, list):
            return [str(c) for c in v]
    if sheet_name.startswith("ORDER"):
        v = by_sheet.get("ORDER_*") or by_sheet.get("ORDER_ALL")
        if isinstance(v, list):
            return [str(c) for c in v]
    return None


def _ensure_v2_schema(cur: sqlite3.Cursor) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CURRENT} (
            sheet_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            subdir TEXT NOT NULL DEFAULT '',
            row_key_hash TEXT NOT NULL,
            row_key_json TEXT NOT NULL,
            row_hash TEXT NOT NULL,
            row_status TEXT NOT NULL DEFAULT 'active',
            source_file TEXT,
            source_path TEXT,
            first_seen_at TEXT NOT NULL,
            last_loaded_at TEXT NOT NULL,
            inactive_since TEXT,
            payload_id INTEGER,
            PRIMARY KEY (sheet_name, file_name, subdir, row_key_hash)
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PAYLOAD} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            subdir TEXT NOT NULL DEFAULT '',
            row_key_hash TEXT NOT NULL,
            row_hash TEXT NOT NULL,
            loaded_at TEXT NOT NULL,
            source_file TEXT,
            payload_json TEXT NOT NULL
        )
        """
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PAYLOAD}_sheet_key "
        f"ON {TABLE_PAYLOAD}(sheet_name, file_name, subdir, row_key_hash, row_hash)"
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_INGEST_RUN} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingest_run_id TEXT NOT NULL,
            sheet_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            subdir TEXT NOT NULL DEFAULT '',
            started_at TEXT NOT NULL,
            finished_at TEXT,
            file_sha256 TEXT,
            count_new INTEGER DEFAULT 0,
            count_changed INTEGER DEFAULT 0,
            count_unchanged INTEGER DEFAULT 0,
            count_inactive INTEGER DEFAULT 0,
            count_key_errors INTEGER DEFAULT 0,
            hash_phase_sec REAL,
            compare_phase_sec REAL,
            db_write_sec REAL
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_FILE_INVENTORY} (
            sheet_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            subdir TEXT NOT NULL DEFAULT '',
            last_content_sha256 TEXT,
            last_source_row_count INTEGER NOT NULL DEFAULT 0,
            last_checked_at TEXT,
            PRIMARY KEY (sheet_name, file_name, subdir)
        )
        """
    )


def _load_existing_map(
    cur: sqlite3.Cursor,
    sheet_name: str,
    file_name: str,
    subdir: str,
) -> Dict[str, Tuple[str, Optional[int]]]:
    """Активные и неактивные строки листа+файла: key_hash → (row_hash, payload_id)."""
    cur.execute(
        f"""
        SELECT row_key_hash, row_hash, payload_id
        FROM {TABLE_CURRENT}
        WHERE sheet_name = ? AND file_name = ? AND subdir = ?
          AND row_status IN ('active', 'inactive')
        """,
        (sheet_name, file_name, subdir),
    )
    out: Dict[str, Tuple[str, Optional[int]]] = {}
    for row in cur.fetchall():
        kh = str(row[0])
        out[kh] = (str(row[1]), row[2] if row[2] is not None else None)
    return out


def _get_file_inventory_sha(
    cur: sqlite3.Cursor,
    sheet_name: str,
    file_name: str,
    subdir: str,
) -> Optional[str]:
    cur.execute(
        f"""
        SELECT last_content_sha256 FROM {TABLE_FILE_INVENTORY}
        WHERE sheet_name = ? AND file_name = ? AND subdir = ?
        """,
        (sheet_name, file_name, subdir),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0]).strip().lower() or None


def _upsert_file_inventory(
    cur: sqlite3.Cursor,
    sheet_name: str,
    file_name: str,
    subdir: str,
    content_sha: str,
    row_count: int,
    checked_at: str,
) -> None:
    cur.execute(
        f"""
        INSERT INTO {TABLE_FILE_INVENTORY} (
            sheet_name, file_name, subdir, last_content_sha256,
            last_source_row_count, last_checked_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(sheet_name, file_name, subdir) DO UPDATE SET
            last_content_sha256 = excluded.last_content_sha256,
            last_source_row_count = excluded.last_source_row_count,
            last_checked_at = excluded.last_checked_at
        """,
        (sheet_name, file_name, subdir, content_sha, row_count, checked_at),
    )


def _insert_payload(
    cur: sqlite3.Cursor,
    sheet_name: str,
    file_name: str,
    subdir: str,
    row_key_hash: str,
    row_hash: str,
    loaded_at: str,
    source_file: str,
    payload_json: str,
) -> int:
    cur.execute(
        f"""
        INSERT INTO {TABLE_PAYLOAD} (
            sheet_name, file_name, subdir, row_key_hash, row_hash,
            loaded_at, source_file, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sheet_name,
            file_name,
            subdir,
            row_key_hash,
            row_hash,
            loaded_at,
            source_file,
            payload_json,
        ),
    )
    return int(cur.lastrowid)


def _fields_to_json(fields: Dict[str, str]) -> str:
    import json

    return json.dumps(fields, ensure_ascii=False, sort_keys=True)


def _ingest_one_file(
    cur: sqlite3.Cursor,
    cfg: Dict[str, Any],
    ingest_run_id: str,
    sheet_name: str,
    file_name: str,
    subdir: str,
    file_path: str,
    source_file_label: str,
    df_raw: pd.DataFrame,
    row_key_columns: Sequence[str],
    now_utc: str,
    log_mode: str,
) -> Dict[str, Any]:
    """Построчный ingest одного CSV; возвращает статистику и тайминги для отчёта."""
    hash_cols = cfg.get("row_hash_columns")
    hash_cols_list: Optional[List[str]] = None
    if isinstance(hash_cols, list) and hash_cols:
        hash_cols_list = [str(c) for c in hash_cols]

    parallel_cfg = cfg.get("parallel_row_processing") or {}
    skip_if_same = bool(cfg.get("skip_ingest_if_file_unchanged", True))

    t0 = time.perf_counter()
    content_sha = _hash_file(file_path)
    row_count = len(df_raw)

    if skip_if_same:
        prev_sha = _get_file_inventory_sha(cur, sheet_name, file_name, subdir)
        if prev_sha and prev_sha == content_sha.lower():
            _upsert_file_inventory(cur, sheet_name, file_name, subdir, content_sha, row_count, now_utc)
            _log_archive_event(
                log_mode,
                f"[archive_v2] «{sheet_name}» / {file_name}: файл без изменений (SHA), построчный ingest пропущен",
                f"sha={content_sha[:16]}…",
            )
            return {
                "kind": "file_unchanged",
                "new": 0,
                "changed": 0,
                "unchanged": row_count,
                "inactive": 0,
                "key_errors": 0,
                "dup_warnings": 0,
                "hash_sec": 0.0,
                "compare_sec": 0.0,
                "db_sec": 0.0,
            }

    row_dicts = dataframe_to_row_dicts(df_raw)
    t_hash_start = time.perf_counter()
    records = compute_row_hashes_parallel(
        row_dicts, row_key_columns, hash_cols_list, parallel_cfg
    )
    hash_sec = time.perf_counter() - t_hash_start

    records, dup_warnings = dedupe_by_key_last_wins(records)
    if dup_warnings:
        logging.warning(
            "[archive_v2] «%s» / %s: дубликаты ключа строки (%s), политика last-wins",
            sheet_name,
            file_name,
            dup_warnings,
        )

    key_set: Set[str] = {r.row_key_hash for r in records if r.row_key_hash}

    existing_map = _load_existing_map(cur, sheet_name, file_name, subdir)
    t_cmp_start = time.perf_counter()
    classified = classify_rows_parallel(records, existing_map, parallel_cfg)
    compare_sec = time.perf_counter() - t_cmp_start

    t_db_start = time.perf_counter()
    counts = count_by_kind(classified)
    new_rows: List[ClassifiedRow] = []
    changed_rows: List[ClassifiedRow] = []
    unchanged_rows: List[ClassifiedRow] = []
    for c in classified:
        if c.record.error or not c.record.row_key_hash:
            continue
        if c.kind == CLASS_NEW:
            new_rows.append(c)
        elif c.kind == CLASS_CHANGED:
            changed_rows.append(c)
        elif c.kind == CLASS_UNCHANGED:
            unchanged_rows.append(c)

    for c in new_rows + changed_rows:
        rec = c.record
        pid = _insert_payload(
            cur,
            sheet_name,
            file_name,
            subdir,
            rec.row_key_hash,
            rec.row_hash,
            now_utc,
            source_file_label,
            _fields_to_json(rec.fields),
        )
        cur.execute(
            f"""
            INSERT INTO {TABLE_CURRENT} (
                sheet_name, file_name, subdir, row_key_hash, row_key_json, row_hash,
                row_status, source_file, source_path, first_seen_at, last_loaded_at,
                inactive_since, payload_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
            ON CONFLICT(sheet_name, file_name, subdir, row_key_hash) DO UPDATE SET
                row_key_json = excluded.row_key_json,
                row_hash = excluded.row_hash,
                row_status = 'active',
                source_file = excluded.source_file,
                source_path = excluded.source_path,
                last_loaded_at = excluded.last_loaded_at,
                inactive_since = NULL,
                payload_id = excluded.payload_id
            """,
            (
                sheet_name,
                file_name,
                subdir,
                rec.row_key_hash,
                rec.row_key_json,
                rec.row_hash,
                ROW_STATUS_ACTIVE,
                source_file_label,
                file_path,
                now_utc,
                now_utc,
                pid,
            ),
        )

    for c in unchanged_rows:
        rec = c.record
        cur.execute(
            f"""
            UPDATE {TABLE_CURRENT}
            SET last_loaded_at = ?,
                source_file = ?,
                source_path = ?,
                row_status = 'active',
                inactive_since = NULL
            WHERE sheet_name = ? AND file_name = ? AND subdir = ?
              AND row_key_hash = ?
            """,
            (
                now_utc,
                source_file_label,
                file_path,
                sheet_name,
                file_name,
                subdir,
                rec.row_key_hash,
            ),
        )

    inactive_count = 0
    if key_set:
        placeholders = ",".join("?" * len(key_set))
        cur.execute(
            f"""
            SELECT row_key_hash FROM {TABLE_CURRENT}
            WHERE sheet_name = ? AND file_name = ? AND subdir = ?
              AND row_status = 'active'
              AND row_key_hash NOT IN ({placeholders})
            """,
            [sheet_name, file_name, subdir, *key_set],
        )
        to_inactivate = [str(r[0]) for r in cur.fetchall()]
        inactive_count = len(to_inactivate)
        if to_inactivate:
            ph2 = ",".join("?" * len(to_inactivate))
            cur.execute(
                f"""
                UPDATE {TABLE_CURRENT}
                SET row_status = ?, inactive_since = ?, last_loaded_at = ?
                WHERE sheet_name = ? AND file_name = ? AND subdir = ?
                  AND row_key_hash IN ({ph2})
                """,
                [
                    ROW_STATUS_INACTIVE,
                    now_utc,
                    now_utc,
                    sheet_name,
                    file_name,
                    subdir,
                    *to_inactivate,
                ],
            )

    _upsert_file_inventory(cur, sheet_name, file_name, subdir, content_sha, row_count, now_utc)
    db_sec = time.perf_counter() - t_db_start
    total_sec = time.perf_counter() - t0

    cur.execute(
        f"""
        INSERT INTO {TABLE_INGEST_RUN} (
            ingest_run_id, sheet_name, file_name, subdir, started_at, finished_at,
            file_sha256, count_new, count_changed, count_unchanged, count_inactive,
            count_key_errors, hash_phase_sec, compare_phase_sec, db_write_sec
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ingest_run_id,
            sheet_name,
            file_name,
            subdir,
            now_utc,
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            content_sha,
            len(new_rows),
            len(changed_rows),
            len(unchanged_rows),
            inactive_count,
            counts.get("key_errors", 0),
            round(hash_sec, 4),
            round(compare_sec, 4),
            round(db_sec, 4),
        ),
    )

    logging.debug(
        "[archive_v2] «%s» hash=%.3fs compare=%.3fs db=%.3fs total=%.3fs workers_cfg=%s rows=%s",
        sheet_name,
        hash_sec,
        compare_sec,
        db_sec,
        total_sec,
        parallel_cfg,
        row_count,
    )

    return {
        "kind": "ingested",
        "new": len(new_rows),
        "changed": len(changed_rows),
        "unchanged": len(unchanged_rows),
        "inactive": inactive_count,
        "key_errors": counts.get("key_errors", 0),
        "dup_warnings": dup_warnings,
        "hash_sec": hash_sec,
        "compare_sec": compare_sec,
        "db_sec": db_sec,
    }


def run_input_archive_sqlite_v2(
    project_base_dir: str,
    archive_cfg: Dict[str, Any],
    payloads: Dict[str, Dict[str, Any]],
) -> None:
    """
    Построчная запись в SQLite v2. Вызывается из main при row_level_archive=true.
    """
    cfg = merge_archive_v2_config(archive_cfg)
    if not cfg.get("enabled") or not cfg.get("row_level_archive"):
        return
    if not payloads:
        return

    default_rel = str(cfg.get("db_path") or "OUT/DB/spod_input_archive_v2.sqlite")
    by_db: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for sn, pack in payloads.items():
        fc = pack.get("file_conf") or {}
        rel = (fc.get("archive_db_path") or "").strip() or default_rel
        by_db[rel][sn] = pack
    if len(by_db) > 1:
        logging.info("[archive_v2] Запись в %s отдельных БД", len(by_db))
        for rel, sub in by_db.items():
            sub_cfg = {**archive_cfg, "db_path": rel}
            run_input_archive_sqlite_v2(project_base_dir, sub_cfg, sub)
        return
    if len(by_db) == 1:
        only_rel = next(iter(by_db.keys()))
        payloads = by_db[only_rel]
        archive_cfg = {**archive_cfg, "db_path": only_rel}
        cfg = merge_archive_v2_config(archive_cfg)

    db_rel = str(cfg.get("db_path") or "OUT/DB/spod_input_archive_v2.sqlite")
    db_path = db_rel if os.path.isabs(db_rel) else os.path.join(project_base_dir, db_rel)
    db_display = db_rel if not os.path.isabs(db_rel) else os.path.relpath(db_path, project_base_dir)
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    default_on = bool(cfg.get("default_archive_to_db"))
    console_mode, log_mode = _archive_reporting_modes(cfg)
    ingest_run_id = str(uuid.uuid4())
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    events: List[Dict[str, Any]] = []
    stats: Dict[str, int] = {
        "new": 0,
        "changed": 0,
        "unchanged": 0,
        "inactive": 0,
        "file_unchanged": 0,
        "key_errors": 0,
        "errors": 0,
        "not_requested": 0,
        "no_payload": 0,
        "no_row_key": 0,
    }

    conn = sqlite3.connect(db_path, timeout=120.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        cur = conn.cursor()
        _ensure_v2_schema(cur)
        conn.commit()

        _log_archive_event(
            log_mode,
            f"[archive_v2] Старт построчного архива, БД: {db_display}, run_id={ingest_run_id[:8]}…",
            f"schema_version={SCHEMA_VERSION}",
        )

        for sheet_name, pack in payloads.items():
            file_conf = pack.get("file_conf") or {}
            df_raw: Optional[pd.DataFrame] = pack.get("df_raw")
            file_path: Optional[str] = pack.get("file_path")
            subdir = (file_conf.get("subdir") or "").strip()
            fn = str(file_conf.get("file", ""))

            if df_raw is None or file_path is None:
                stats["no_payload"] += 1
                events.append(
                    {
                        "sheet": sheet_name,
                        "file": fn,
                        "kind": "no_payload",
                        "label": "нет данных",
                        "rows": None,
                        "extra": "",
                    }
                )
                continue

            want = bool(file_conf.get("archive_to_db", default_on))
            if not want:
                stats["not_requested"] += 1
                events.append(
                    {
                        "sheet": sheet_name,
                        "file": fn,
                        "kind": "not_requested",
                        "label": "вне архива",
                        "rows": len(df_raw),
                        "extra": "",
                    }
                )
                continue

            key_cols = resolve_row_key_columns(sheet_name, file_conf, cfg)
            if not key_cols:
                stats["no_row_key"] += 1
                logging.error(
                    "[archive_v2] «%s»: не задан row_key_columns и нет в default_row_key_by_sheet",
                    sheet_name,
                )
                events.append(
                    {
                        "sheet": sheet_name,
                        "file": fn,
                        "kind": "no_row_key",
                        "label": "нет ключа строки в config",
                        "rows": len(df_raw),
                        "extra": "",
                    }
                )
                continue

            missing_cols = [c for c in key_cols if c not in df_raw.columns]
            if missing_cols:
                stats["errors"] += 1
                logging.error(
                    "[archive_v2] «%s»: в CSV нет колонок ключа: %s",
                    sheet_name,
                    missing_cols,
                )
                events.append(
                    {
                        "sheet": sheet_name,
                        "file": fn,
                        "kind": "error",
                        "label": f"нет колонок: {missing_cols[:3]}",
                        "rows": len(df_raw),
                        "extra": "",
                    }
                )
                continue

            try:
                result = _ingest_one_file(
                    cur,
                    cfg,
                    ingest_run_id,
                    sheet_name,
                    fn,
                    subdir,
                    file_path,
                    fn,
                    df_raw,
                    key_cols,
                    now_utc,
                    log_mode,
                )
            except Exception:
                stats["errors"] += 1
                logging.exception("[archive_v2] Ошибка ingest «%s» / %s", sheet_name, fn)
                events.append(
                    {
                        "sheet": sheet_name,
                        "file": fn,
                        "kind": "error",
                        "label": "ошибка записи",
                        "rows": len(df_raw),
                        "extra": "",
                    }
                )
                continue

            if result.get("kind") == "file_unchanged":
                stats["file_unchanged"] += 1
                stats["unchanged"] += int(result.get("unchanged", 0))
                label = "файл без изменений (SHA)"
            else:
                stats["new"] += int(result.get("new", 0))
                stats["changed"] += int(result.get("changed", 0))
                stats["unchanged"] += int(result.get("unchanged", 0))
                stats["inactive"] += int(result.get("inactive", 0))
                stats["key_errors"] += int(result.get("key_errors", 0))
                label = (
                    f"new={result.get('new')} chg={result.get('changed')} "
                    f"same={result.get('unchanged')} off={result.get('inactive')}"
                )

            extra = ""
            if result.get("hash_sec") is not None:
                extra = (
                    f"hash={result.get('hash_sec', 0):.2f}s "
                    f"cmp={result.get('compare_sec', 0):.2f}s "
                    f"db={result.get('db_sec', 0):.2f}s"
                )
            events.append(
                {
                    "sheet": sheet_name,
                    "file": fn,
                    "kind": result.get("kind", "ingested"),
                    "label": label,
                    "rows": len(df_raw),
                    "extra": extra,
                }
            )
            conn.commit()

        _log_archive_summary_line(
            f"[archive_v2] Итог: new={stats['new']} changed={stats['changed']} "
            f"unchanged={stats['unchanged']} inactive={stats['inactive']} "
            f"file_skip={stats['file_unchanged']} errors={stats['errors']}"
        )
        console_ui.print_input_archive_row_report(console_mode, db_display, stats, events)
    finally:
        conn.close()
