# -*- coding: utf-8 -*-
"""
Архив входных CSV в SQLite: версии файлов без перезаписи истории.

Включается секцией config.json → input_archive_sqlite.enabled.
Сводная таблица archive_file_inventory хранит последнюю известную сигнатуру файла
(размер, число строк/колонок, SHA-256 содержимого) — по ней решается, писать ли новый снимок.
Повторные запуски с теми же файлами не раздувают БД: строки данных не дублируются.

Связи между таблицами листов (FOREIGN KEY) намеренно не создаются.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

# Мета-таблица: каждый снимок (версия файла в архиве)
META_TABLE = "archive_file_snapshot"
# Сводная таблица: одна строка на логический вход (sheet + file + subdir) — для решения «нужен ли ingest»
INVENTORY_TABLE = "archive_file_inventory"


def _defaults_cfg() -> Dict[str, Any]:
    """Значения по умолчанию для input_archive_sqlite (слияние с config)."""
    return {
        "enabled": False,
        "db_path": "OUT/DB/spod_input_archive.sqlite",
        # Сравнение «тот же файл»: по SHA-256 байтов на диске (устойчиво к смене mtime без смены содержимого)
        "use_sha256_for_identity": True,
        # Устаревший алиас; если в JSON только compute_sha256 — подхватывается в merge_archive_config
        "compute_sha256": True,
        "default_archive_to_db": False,
        "append_on_content_change": True,
        "system_columns": {
            "snapshot_id": "__snapshot_id",
            "row_index": "__row_ix",
            "loaded_at": "__loaded_at",
        },
    }


def merge_archive_config(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Объединяет конфиг из JSON с дефолтами."""
    base = _defaults_cfg()
    if isinstance(raw, dict):
        for k, v in raw.items():
            if k == "system_columns" and isinstance(v, dict):
                base["system_columns"] = {**base["system_columns"], **v}
            else:
                base[k] = v
        # Обратная совместимость: только старое имя compute_sha256
        if "use_sha256_for_identity" not in raw and "compute_sha256" in raw:
            base["use_sha256_for_identity"] = bool(raw["compute_sha256"])
    return base


def sheet_to_table_name(sheet_name: str) -> str:
    """Имя таблицы данных для листа: arch_<безопасное_имя>."""
    safe = re.sub(r"[^0-9a-zA-Z_]+", "_", sheet_name).strip("_")
    if not safe:
        safe = "sheet"
    return f"arch_{safe}"[:63]


def _quote_ident(name: str) -> str:
    """Экранирование идентификатора SQLite."""
    return '"' + name.replace('"', '""') + '"'


def _sanitize_column(name: str) -> str:
    """Допустимое имя колонки для SQLite; коллизии с системными префиксами уводим."""
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", str(name)).strip("_")
    if not s:
        s = "col"
    if s.upper().startswith("ARCH__"):
        s = "d_" + s
    return s[:200]


def _table_columns(cur: sqlite3.Cursor, table: str) -> List[str]:
    cur.execute(f"PRAGMA table_info({_quote_ident(table)})")
    return [str(r[1]) for r in cur.fetchall()]


def _migrate_snapshot_table(cur: sqlite3.Cursor) -> None:
    """Добавляет в archive_file_snapshot недостающие служебные колонки (старые БД)."""
    cols = {c.lower() for c in _table_columns(cur, META_TABLE)}
    if not cols:
        return
    if "source_col_count" not in cols:
        cur.execute(f"ALTER TABLE {META_TABLE} ADD COLUMN source_col_count INTEGER")


def _ensure_meta_table(cur: sqlite3.Cursor) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {META_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            subdir TEXT NOT NULL DEFAULT '',
            resolved_path TEXT,
            source_mtime REAL NOT NULL,
            source_size INTEGER NOT NULL,
            source_row_count INTEGER NOT NULL,
            source_col_count INTEGER,
            source_sha256 TEXT,
            loaded_at TEXT NOT NULL,
            actuality_checked_at TEXT,
            row_status TEXT NOT NULL,
            superseded_by_id INTEGER
        )
        """
    )
    _migrate_snapshot_table(cur)
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{META_TABLE}_sheet_file_status "
        f"ON {META_TABLE}(sheet_name, file_name, subdir, row_status)"
    )


def _ensure_inventory_table(cur: sqlite3.Cursor) -> None:
    """Одна строка на логический файл: последняя сигнатура и счётчики прогонов."""
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {INVENTORY_TABLE} (
            sheet_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            subdir TEXT NOT NULL DEFAULT '',
            latest_snapshot_id INTEGER,
            resolved_path_last TEXT,
            last_source_mtime REAL NOT NULL DEFAULT 0,
            last_source_size INTEGER NOT NULL DEFAULT 0,
            last_source_row_count INTEGER NOT NULL DEFAULT 0,
            last_source_col_count INTEGER NOT NULL DEFAULT 0,
            last_content_sha256 TEXT,
            last_ingest_at TEXT,
            last_checked_at TEXT,
            total_ingests INTEGER NOT NULL DEFAULT 0,
            total_skips_same_content INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (sheet_name, file_name, subdir)
        )
        """
    )


def _backfill_inventory_from_snapshots(cur: sqlite3.Cursor) -> None:
    """Заполняет inventory из существующих latest-снимков, если строки в inventory ещё нет."""
    cur.execute(
        f"""
        INSERT INTO {INVENTORY_TABLE} (
            sheet_name, file_name, subdir, latest_snapshot_id, resolved_path_last,
            last_source_mtime, last_source_size, last_source_row_count, last_source_col_count,
            last_content_sha256, last_ingest_at, last_checked_at, total_ingests, total_skips_same_content
        )
        SELECT
            s.sheet_name, s.file_name, s.subdir, s.id, s.resolved_path,
            s.source_mtime, s.source_size, s.source_row_count,
            COALESCE(s.source_col_count, 0),
            s.source_sha256, s.loaded_at, s.actuality_checked_at, 1, 0
        FROM {META_TABLE} s
        WHERE s.row_status = 'latest'
          AND NOT EXISTS (
            SELECT 1 FROM {INVENTORY_TABLE} i
            WHERE i.sheet_name = s.sheet_name AND i.file_name = s.file_name AND i.subdir = s.subdir
          )
        """
    )


def _hash_file(path: str) -> str:
    """SHA-256 содержимого файла на диске (сырые байты CSV)."""
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def _file_stat(path: str) -> Tuple[float, int]:
    """mtime (float), size_bytes."""
    st = os.stat(path)
    return float(st.st_mtime), int(st.st_size)


def _existing_columns(cur: sqlite3.Cursor, table: str) -> List[str]:
    cur.execute(f"PRAGMA table_info({_quote_ident(table)})")
    return [str(r[1]) for r in cur.fetchall()]


def _ensure_data_table(
    cur: sqlite3.Cursor,
    table: str,
    data_column_names: Sequence[str],
    snap_col: str,
    row_col: str,
    loaded_col: str,
) -> None:
    """Создаёт таблицу данных или добавляет недостающие TEXT-колонки."""
    sys_cols = {snap_col, row_col, loaded_col}
    sanitized_map: List[Tuple[str, str]] = []
    seen: set = set()
    for orig in data_column_names:
        sc = _sanitize_column(orig)
        base = sc
        n = 2
        while sc.upper() in seen or sc in sys_cols:
            sc = f"{base}_{n}"
            n += 1
        seen.add(sc.upper())
        sanitized_map.append((orig, sc))

    col_defs = [
        f"{_quote_ident(snap_col)} INTEGER NOT NULL",
        f"{_quote_ident(row_col)} INTEGER NOT NULL",
        f"{_quote_ident(loaded_col)} TEXT NOT NULL",
    ]
    first_cols_sql = col_defs + [f'{_quote_ident(sc)} TEXT' for _, sc in sanitized_map]

    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {_quote_ident(table)} ({', '.join(first_cols_sql)})"
    )
    existing = {c.lower() for c in _existing_columns(cur, table)}
    for _, sc in sanitized_map:
        if sc.lower() not in existing:
            cur.execute(
                f"ALTER TABLE {_quote_ident(table)} ADD COLUMN {_quote_ident(sc)} TEXT"
            )


def _get_latest_snapshot(
    cur: sqlite3.Cursor,
    sheet_name: str,
    file_name: str,
    subdir: str,
) -> Optional[sqlite3.Row]:
    cur.execute(
        f"""
        SELECT * FROM {META_TABLE}
        WHERE sheet_name = ? AND file_name = ? AND subdir = ? AND row_status = 'latest'
        ORDER BY id DESC LIMIT 1
        """,
        (sheet_name, file_name, subdir),
    )
    return cur.fetchone()


def _get_inventory_row(
    cur: sqlite3.Cursor,
    sheet_name: str,
    file_name: str,
    subdir: str,
) -> Optional[sqlite3.Row]:
    cur.execute(
        f"""
        SELECT * FROM {INVENTORY_TABLE}
        WHERE sheet_name = ? AND file_name = ? AND subdir = ?
        """,
        (sheet_name, file_name, subdir),
    )
    return cur.fetchone()


def _metadata_only_dedupe_skip(
    inv: sqlite3.Row,
    mtime: float,
    size: int,
    row_count: int,
    col_count: int,
) -> bool:
    """Режим без SHA-256: пропуск только если совпали размер, строки, колонки и mtime."""
    if int(inv["last_source_size"]) != size:
        return False
    if int(inv["last_source_row_count"]) != row_count:
        return False
    if int(inv["last_source_col_count"] or 0) != col_count:
        return False
    if abs(float(inv["last_source_mtime"]) - mtime) > 1e-6:
        return False
    return True


def run_input_archive_sqlite(
    project_base_dir: str,
    archive_cfg: Dict[str, Any],
    payloads: Dict[str, Dict[str, Any]],
) -> None:
    """
    Записывает в SQLite сырые копии листов согласно конфигу.

    Решение о новом снимке:
    - при use_sha256_for_identity=true: если размер, число строк и колонок как в inventory —
      сравнивается SHA-256 файла; совпадение → без новых строк в arch_*;
    - при false: как раньше — размер, строки, колонки и mtime.
    """
    cfg = merge_archive_config(archive_cfg)
    if not cfg.get("enabled"):
        return

    use_sha256 = bool(cfg.get("use_sha256_for_identity", cfg.get("compute_sha256", True)))

    db_rel = str(cfg.get("db_path") or "OUT/DB/spod_input_archive.sqlite")
    db_path = db_rel if os.path.isabs(db_rel) else os.path.join(project_base_dir, db_rel)
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    sys_cols = cfg["system_columns"]
    snap_c = str(sys_cols.get("snapshot_id", "__snapshot_id"))
    row_c = str(sys_cols.get("row_index", "__row_ix"))
    loaded_c = str(sys_cols.get("loaded_at", "__loaded_at"))

    default_on = bool(cfg.get("default_archive_to_db"))
    append_on_change = bool(cfg.get("append_on_content_change", True))

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    conn = sqlite3.connect(db_path, timeout=60.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        cur = conn.cursor()
        _ensure_meta_table(cur)
        _ensure_inventory_table(cur)
        _backfill_inventory_from_snapshots(cur)
        conn.commit()

        for sheet_name, pack in payloads.items():
            file_conf = pack.get("file_conf") or {}
            df_raw: Optional[pd.DataFrame] = pack.get("df_raw")
            file_path: Optional[str] = pack.get("file_path")

            if df_raw is None or file_path is None:
                continue

            want = bool(file_conf.get("archive_to_db", default_on))
            if not want:
                continue

            subdir = (file_conf.get("subdir") or "").strip()
            fn = str(file_conf.get("file", ""))
            row_count = len(df_raw)
            col_count = int(len(df_raw.columns))

            if not append_on_change:
                cur.execute(
                    f"SELECT 1 FROM {META_TABLE} WHERE sheet_name = ? AND file_name = ? AND subdir = ? LIMIT 1",
                    (sheet_name, fn, subdir),
                )
                if cur.fetchone():
                    logging.info(
                        f"[archive_sqlite] Пропуск {sheet_name}: append_on_content_change=false, снимок уже есть"
                    )
                    continue

            try:
                mtime, size = _file_stat(file_path)
            except OSError as e:
                logging.warning(f"[archive_sqlite] Нет доступа к файлу {file_path}: {e}")
                continue

            checked_at = now_utc
            inv = _get_inventory_row(cur, sheet_name, fn, subdir)

            # Быстрый признак «точно новое содержимое» — размер или число строк/колонок
            quick_changed = True
            if inv is not None:
                quick_changed = (
                    int(inv["last_source_size"]) != size
                    or int(inv["last_source_row_count"]) != row_count
                    or int(inv["last_source_col_count"] or 0) != col_count
                )

            # Решение: нужен ли новый снимок и строки в arch_*.
            # При use_sha256: хеш считаем только если размер/строки/колонки совпали с inventory (экономия I/O).
            skip_ingest = False
            content_hash: Optional[str] = None

            if inv is None:
                skip_ingest = False
            elif not use_sha256:
                skip_ingest = _metadata_only_dedupe_skip(inv, mtime, size, row_count, col_count)
            elif quick_changed:
                skip_ingest = False
            else:
                content_hash = _hash_file(file_path)
                prev_h = inv["last_content_sha256"]
                if prev_h is not None and str(prev_h) == content_hash:
                    skip_ingest = True
                elif prev_h is None:
                    # Старая БД: в inventory/snapshot не было SHA — записываем хеш без дублирования строк
                    cur.execute(
                        f"""
                        UPDATE {INVENTORY_TABLE}
                        SET last_content_sha256 = ?, last_checked_at = ?,
                            resolved_path_last = ?, last_source_mtime = ?,
                            last_source_size = ?, last_source_row_count = ?, last_source_col_count = ?
                        WHERE sheet_name = ? AND file_name = ? AND subdir = ?
                        """,
                        (
                            content_hash,
                            checked_at,
                            file_path,
                            mtime,
                            size,
                            row_count,
                            col_count,
                            sheet_name,
                            fn,
                            subdir,
                        ),
                    )
                    sid = inv["latest_snapshot_id"]
                    if sid is not None:
                        cur.execute(
                            f"""
                            UPDATE {META_TABLE}
                            SET source_sha256 = ?, actuality_checked_at = ?,
                                source_col_count = ?, resolved_path = ?,
                                source_mtime = ?, source_size = ?, source_row_count = ?
                            WHERE id = ?
                            """,
                            (
                                content_hash,
                                checked_at,
                                col_count,
                                file_path,
                                mtime,
                                size,
                                row_count,
                                int(sid),
                            ),
                        )
                    logging.info(
                        f"[archive_sqlite] Зафиксирован SHA-256 без нового снимка: {sheet_name} / {fn}"
                    )
                    conn.commit()
                    continue

            if skip_ingest and inv is not None:
                cur.execute(
                    f"""
                    UPDATE {INVENTORY_TABLE}
                    SET last_checked_at = ?,
                        total_skips_same_content = total_skips_same_content + 1,
                        resolved_path_last = ?,
                        last_source_mtime = ?, last_source_size = ?,
                        last_source_row_count = ?, last_source_col_count = ?
                    WHERE sheet_name = ? AND file_name = ? AND subdir = ?
                    """,
                    (
                        checked_at,
                        file_path,
                        mtime,
                        size,
                        row_count,
                        col_count,
                        sheet_name,
                        fn,
                        subdir,
                    ),
                )
                sid = inv["latest_snapshot_id"]
                if sid is not None:
                    cur.execute(
                        f"""
                        UPDATE {META_TABLE}
                        SET actuality_checked_at = ?, resolved_path = ?,
                            source_mtime = ?, source_size = ?, source_row_count = ?,
                            source_col_count = ?
                        WHERE id = ?
                        """,
                        (
                            checked_at,
                            file_path,
                            mtime,
                            size,
                            row_count,
                            col_count,
                            int(sid),
                        ),
                    )
                logging.info(
                    f"[archive_sqlite] Тот же файл, новый снимок не создаётся: {sheet_name} / {fn} "
                    f"(пропусков по контенту: {int(inv['total_skips_same_content']) + 1})"
                )
                conn.commit()
                continue

            # Новый снимок
            if use_sha256 and content_hash is None:
                content_hash = _hash_file(file_path)

            prev = _get_latest_snapshot(cur, sheet_name, fn, subdir)
            prev_id: Optional[int] = int(prev["id"]) if prev is not None else None

            table = sheet_to_table_name(sheet_name)
            data_cols = [str(c) for c in df_raw.columns.tolist()]
            _ensure_data_table(cur, table, data_cols, snap_c, row_c, loaded_c)

            sanitized_map: List[Tuple[str, str]] = []
            seen: set = set()
            sys_reserved = {snap_c.lower(), row_c.lower(), loaded_c.lower()}
            for orig in data_cols:
                sc = _sanitize_column(orig)
                base = sc
                n = 2
                while sc.lower() in seen or sc.lower() in sys_reserved:
                    sc = f"{base}_{n}"
                    n += 1
                seen.add(sc.lower())
                sanitized_map.append((orig, sc))

            conn.execute("BEGIN IMMEDIATE")
            try:
                if prev_id is not None:
                    cur.execute(
                        f"UPDATE {META_TABLE} SET row_status = 'historical' WHERE id = ?",
                        (prev_id,),
                    )
                cur.execute(
                    f"""
                    INSERT INTO {META_TABLE} (
                        sheet_name, file_name, subdir, resolved_path,
                        source_mtime, source_size, source_row_count, source_col_count, source_sha256,
                        loaded_at, actuality_checked_at, row_status, superseded_by_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'latest', NULL)
                    """,
                    (
                        sheet_name,
                        fn,
                        subdir,
                        file_path,
                        mtime,
                        size,
                        row_count,
                        col_count,
                        content_hash,
                        now_utc,
                        checked_at,
                    ),
                )
                new_id = int(cur.execute("SELECT last_insert_rowid()").fetchone()[0])
                if prev_id is not None:
                    cur.execute(
                        f"UPDATE {META_TABLE} SET superseded_by_id = ? WHERE id = ?",
                        (new_id, prev_id),
                    )

                cur.execute(
                    f"""
                    INSERT INTO {INVENTORY_TABLE} (
                        sheet_name, file_name, subdir, latest_snapshot_id, resolved_path_last,
                        last_source_mtime, last_source_size, last_source_row_count, last_source_col_count,
                        last_content_sha256, last_ingest_at, last_checked_at,
                        total_ingests, total_skips_same_content
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)
                    ON CONFLICT(sheet_name, file_name, subdir) DO UPDATE SET
                        latest_snapshot_id = excluded.latest_snapshot_id,
                        resolved_path_last = excluded.resolved_path_last,
                        last_source_mtime = excluded.last_source_mtime,
                        last_source_size = excluded.last_source_size,
                        last_source_row_count = excluded.last_source_row_count,
                        last_source_col_count = excluded.last_source_col_count,
                        last_content_sha256 = excluded.last_content_sha256,
                        last_ingest_at = excluded.last_ingest_at,
                        last_checked_at = excluded.last_checked_at,
                        total_ingests = {INVENTORY_TABLE}.total_ingests + 1
                    """,
                    (
                        sheet_name,
                        fn,
                        subdir,
                        new_id,
                        file_path,
                        mtime,
                        size,
                        row_count,
                        col_count,
                        content_hash,
                        now_utc,
                        checked_at,
                    ),
                )

                insert_cols = [snap_c, row_c, loaded_c] + [sc for _, sc in sanitized_map]
                placeholders = ", ".join(["?"] * len(insert_cols))
                qcols = ", ".join(_quote_ident(c) for c in insert_cols)
                insert_sql = f"INSERT INTO {_quote_ident(table)} ({qcols}) VALUES ({placeholders})"

                rows_sql: List[Tuple[Any, ...]] = []
                for ix, (_, row) in enumerate(df_raw.iterrows()):
                    vals: List[Any] = [new_id, ix, now_utc]
                    for orig, sc in sanitized_map:
                        v = row.get(orig, "")
                        if pd.isna(v):
                            vals.append("")
                        else:
                            vals.append(str(v))
                    rows_sql.append(tuple(vals))

                if rows_sql:
                    cur.executemany(insert_sql, rows_sql)

                conn.commit()
            except Exception:
                conn.rollback()
                raise

            logging.info(
                f"[archive_sqlite] Новый снимок #{new_id}: {sheet_name} ({fn}), строк={row_count}, "
                f"таблица={table}"
                + (f", sha256={content_hash[:16]}…" if content_hash else "")
            )

        conn.commit()
    finally:
        conn.close()
