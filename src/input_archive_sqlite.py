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
from collections import defaultdict
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd

from src import console_ui
from src.archive_json_columns import (
    ensure_extra_text_columns,
    plan_archive_json_flat_columns,
    update_json_flat_for_snapshot_rows,
)

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
        # Если на диске снова тот же контент, что уже был в historical-снимке — не создавать новый arch_*,
        # а вернуть тот снимок в latest (см. _find_snapshot_id_by_content_sha256 / _reactivate_snapshot_as_latest).
        "reuse_matching_historical_snapshot": True,
        "system_columns": {
            "snapshot_id": "__snapshot_id",
            "row_index": "__row_ix",
            "loaded_at": "__loaded_at",
        },
        # Подробность вывода: консоль (stdout) и лог независимы — см. _normalize_console_verbosity / _normalize_log_verbosity
        "reporting": {
            "console": "normal",
            "log": "normal",
        },
    }


def merge_archive_config(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Объединяет конфиг из JSON с дефолтами."""
    base = _defaults_cfg()
    if isinstance(raw, dict):
        for k, v in raw.items():
            if k == "system_columns" and isinstance(v, dict):
                base["system_columns"] = {**base["system_columns"], **v}
            elif k == "reporting" and isinstance(v, dict):
                base["reporting"] = {**(base.get("reporting") or {}), **v}
            else:
                base[k] = v
        # Обратная совместимость: только старое имя compute_sha256
        if "use_sha256_for_identity" not in raw and "compute_sha256" in raw:
            base["use_sha256_for_identity"] = bool(raw["compute_sha256"])
    return base


def _normalize_console_verbosity(raw: Optional[str]) -> str:
    """
    Режим вывода в консоль (stdout), независимо от уровня логов.
    off — молча; summary — только итоговые счётчики; normal — итог + строка на файл;
    verbose — как normal, плюс размеры/хеш/причины в консоли.
    """
    s = str(raw or "normal").lower().strip()
    if s in ("0", "off", "none", "no", "false"):
        return "off"
    if s in ("1", "summary", "brief", "short"):
        return "summary"
    if s in ("2", "normal", "info", "default"):
        return "normal"
    if s in ("3", "verbose", "debug", "detail", "full"):
        return "verbose"
    return "normal"


def _normalize_log_verbosity(raw: Optional[str]) -> str:
    """
    Режим сообщений в лог-файл.
    minimal — построчно только DEBUG (итог один раз INFO); normal — INFO по событию, детали DEBUG;
    verbose — те же INFO, расширенные технические детали в DEBUG.
    """
    s = str(raw or "normal").lower().strip()
    if s in ("0", "minimal", "quiet", "errors_only"):
        return "minimal"
    if s in ("1", "normal", "info", "default"):
        return "normal"
    if s in ("2", "verbose", "debug", "detail", "full"):
        return "verbose"
    return "normal"


def _archive_reporting_modes(cfg: Dict[str, Any]) -> Tuple[str, str]:
    """Возвращает (console_mode, log_mode) из input_archive_sqlite.reporting."""
    rep = cfg.get("reporting")
    if not isinstance(rep, dict):
        rep = {}
    c_raw = rep.get("console")
    l_raw = rep.get("log")
    c_s = c_raw if isinstance(c_raw, str) else None
    l_s = l_raw if isinstance(l_raw, str) else None
    return (_normalize_console_verbosity(c_s), _normalize_log_verbosity(l_s))


def _log_archive_event(
    log_mode: str,
    info_line: str,
    debug_detail: Optional[str] = None,
    verbose_debug: Optional[str] = None,
) -> None:
    """
    Запись в лог согласно log_mode.
    minimal: только DEBUG (кроме явных warning/error и финальной сводки INFO).
    normal: INFO основная строка, debug_detail — DEBUG.
    verbose: INFO + DEBUG с debug_detail и при необходимости вторым блоком verbose_debug.
    """
    if log_mode == "minimal":
        logging.debug(info_line)
        if debug_detail:
            logging.debug(debug_detail)
        if verbose_debug:
            logging.debug(verbose_debug)
        return
    if log_mode == "normal":
        logging.info(info_line)
        if debug_detail:
            logging.debug(debug_detail)
        if verbose_debug:
            logging.debug(verbose_debug)
        return
    logging.info(info_line)
    parts = [p for p in (debug_detail, verbose_debug) if p]
    if parts:
        logging.debug(" | ".join(parts))


def _log_archive_summary_line(text: str) -> None:
    """Итог архива — всегда INFO (видно при стандартном logging.level=INFO)."""
    logging.info(text)


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
    # Ускорение поиска «тот же файл по SHA» среди всех снимков (включая historical)
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{META_TABLE}_sheet_file_sha "
        f"ON {META_TABLE}(sheet_name, file_name, subdir, source_sha256)"
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


def _find_snapshot_id_by_content_sha256(
    cur: sqlite3.Cursor,
    sheet_name: str,
    file_name: str,
    subdir: str,
    content_sha256: str,
) -> Optional[int]:
    """
    Любой снимок с тем же SHA-256 байтов файла (latest или historical).
    Берём наибольший id — последний когда-либо записанный снимок с таким содержимым.
    """
    cur.execute(
        f"""
        SELECT id FROM {META_TABLE}
        WHERE sheet_name = ? AND file_name = ? AND subdir = ?
          AND source_sha256 IS NOT NULL AND source_sha256 = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (sheet_name, file_name, subdir, content_sha256),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def _sync_arch_json_flat_columns(
    cur: sqlite3.Cursor,
    table: str,
    snap_c: str,
    row_c: str,
    snapshot_id: int,
    json_flat_cols: List[str],
    json_row_maps: List[Dict[str, str]],
) -> None:
    """Обновляет JSON_* в arch_* для уже существующих строк снимка (без нового INSERT)."""
    update_json_flat_for_snapshot_rows(
        cur,
        table,
        snap_c,
        row_c,
        snapshot_id,
        json_flat_cols,
        json_row_maps,
        _quote_ident,
    )


def _reactivate_snapshot_as_latest(
    cur: sqlite3.Cursor,
    *,
    reuse_snapshot_id: int,
    old_latest_id: Optional[int],
    file_path: str,
    mtime: float,
    size: int,
    row_count: int,
    col_count: int,
    content_hash: Optional[str],
    checked_at: str,
    sheet_name: str,
    file_name: str,
    subdir: str,
) -> None:
    """
    Делает снимок reuse_snapshot_id снова latest без INSERT в arch_*:
    старый latest (если есть и это другой id) переводится в historical и ссылается на «возвращённый» снимок.
    Исходное время loaded_at у переиспользуемого снимка не меняем — обновляем путь, mtime/size и actuality_checked_at.
    """
    if old_latest_id is not None and old_latest_id != reuse_snapshot_id:
        cur.execute(
            f"""
            UPDATE {META_TABLE}
            SET row_status = 'historical', superseded_by_id = ?
            WHERE id = ?
            """,
            (reuse_snapshot_id, old_latest_id),
        )
    cur.execute(
        f"""
        UPDATE {META_TABLE}
        SET row_status = 'latest',
            superseded_by_id = NULL,
            resolved_path = ?,
            source_mtime = ?,
            source_size = ?,
            source_row_count = ?,
            source_col_count = ?,
            source_sha256 = ?,
            actuality_checked_at = ?
        WHERE id = ?
        """,
        (
            file_path,
            mtime,
            size,
            row_count,
            col_count,
            content_hash,
            checked_at,
            reuse_snapshot_id,
        ),
    )
    cur.execute(
        f"""
        UPDATE {INVENTORY_TABLE}
        SET latest_snapshot_id = ?,
            resolved_path_last = ?,
            last_source_mtime = ?,
            last_source_size = ?,
            last_source_row_count = ?,
            last_source_col_count = ?,
            last_content_sha256 = ?,
            last_checked_at = ?
        WHERE sheet_name = ? AND file_name = ? AND subdir = ?
        """,
        (
            reuse_snapshot_id,
            file_path,
            mtime,
            size,
            row_count,
            col_count,
            content_hash,
            checked_at,
            sheet_name,
            file_name,
            subdir,
        ),
    )


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
    - при reuse_matching_historical_snapshot=true и режиме SHA: перед INSERT ищется **любой** снимок с тем же
      хешем; если это не текущий latest — снимок **реактивируется** (без дублирования строк в arch_*).

    Сводка в консоль и лог — по полям reporting.console и reporting.log (см. _normalize_*).
    """
    cfg = merge_archive_config(archive_cfg)
    if not cfg.get("enabled"):
        return
    if not payloads:
        return

    # Несколько файлов SQLite: у каждой записи input_files может быть свой archive_db_path (иначе db_path из конфига)
    default_rel = str(cfg.get("db_path") or "OUT/DB/spod_input_archive.sqlite")
    by_db: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for sn, pack in payloads.items():
        fc = pack.get("file_conf") or {}
        rel = (fc.get("archive_db_path") or "").strip() or default_rel
        by_db[rel][sn] = pack
    if len(by_db) > 1:
        logging.info(
            "[archive_sqlite] Запись в %s отдельных файлов БД (поле archive_db_path в input_files)",
            len(by_db),
        )
        for rel, sub in by_db.items():
            sub_cfg = dict(archive_cfg)
            sub_cfg["db_path"] = rel
            run_input_archive_sqlite(project_base_dir, sub_cfg, sub)
        return
    if len(by_db) == 1:
        only_rel = next(iter(by_db.keys()))
        payloads = by_db[only_rel]
        archive_cfg = {**archive_cfg, "db_path": only_rel}
        cfg = merge_archive_config(archive_cfg)

    use_sha256 = bool(cfg.get("use_sha256_for_identity", cfg.get("compute_sha256", True)))

    db_rel = str(cfg.get("db_path") or "OUT/DB/spod_input_archive.sqlite")
    db_path = db_rel if os.path.isabs(db_rel) else os.path.join(project_base_dir, db_rel)
    # Для консоли и лога — относительный путь от корня проекта (полное имя файла, без усечения в UI)
    db_display = db_rel if not os.path.isabs(db_rel) else os.path.relpath(db_path, project_base_dir)
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    sys_cols = cfg["system_columns"]
    snap_c = str(sys_cols.get("snapshot_id", "__snapshot_id"))
    row_c = str(sys_cols.get("row_index", "__row_ix"))
    loaded_c = str(sys_cols.get("loaded_at", "__loaded_at"))

    default_on = bool(cfg.get("default_archive_to_db"))
    append_on_change = bool(cfg.get("append_on_content_change", True))

    console_mode, log_mode = _archive_reporting_modes(cfg)
    events: List[Dict[str, Any]] = []
    stats: Dict[str, int] = {
        "ingested": 0,
        "unchanged": 0,
        "reactivated": 0,
        "sha_backfill": 0,
        "skipped_first": 0,
        "errors": 0,
        "not_requested": 0,
        "no_payload": 0,
    }

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

        _log_archive_event(
            log_mode,
            f"[archive_sqlite] Старт архивации, БД: {db_display}",
            f"режимы: console={console_mode}, log={log_mode}; use_sha256={use_sha256}; "
            f"append_on_content_change={append_on_change}; default_archive_to_db={default_on}",
            f"листов в пакете: {len(payloads)}",
        )

        for sheet_name, pack in payloads.items():
            file_conf = pack.get("file_conf") or {}
            df_raw: Optional[pd.DataFrame] = pack.get("df_raw")
            file_path: Optional[str] = pack.get("file_path")
            subdir_early = (file_conf.get("subdir") or "").strip()
            fn_early = str(file_conf.get("file", ""))

            if df_raw is None or file_path is None:
                stats["no_payload"] += 1
                _log_archive_event(
                    log_mode,
                    f"[archive_sqlite] Пропуск «{sheet_name}»: нет DataFrame или пути к файлу",
                    f"file={fn_early!r}, subdir={subdir_early!r}",
                )
                events.append(
                    {
                        "sheet": sheet_name,
                        "file": fn_early,
                        "subdir": subdir_early,
                        "kind": "no_payload",
                        "label": "нет данных",
                        "rows": None,
                        "size": None,
                        "snapshot_id": None,
                        "sha16": None,
                        "extra": "",
                    }
                )
                continue

            want = bool(file_conf.get("archive_to_db", default_on))
            if not want:
                stats["not_requested"] += 1
                _log_archive_event(
                    log_mode,
                    f"[archive_sqlite] Не архивируется «{sheet_name}» / {fn_early} (archive_to_db=false и дефолт выкл.)",
                    None,
                )
                events.append(
                    {
                        "sheet": sheet_name,
                        "file": fn_early,
                        "subdir": subdir_early,
                        "kind": "not_requested",
                        "label": "вне архива по конфигу",
                        "rows": len(df_raw),
                        "size": None,
                        "snapshot_id": None,
                        "sha16": None,
                        "extra": "",
                    }
                )
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
                    stats["skipped_first"] += 1
                    _log_archive_event(
                        log_mode,
                        f"[archive_sqlite] Пропуск «{sheet_name}»: append_on_content_change=false, снимок уже есть",
                        f"file={fn}, subdir={subdir}",
                    )
                    events.append(
                        {
                            "sheet": sheet_name,
                            "file": fn,
                            "subdir": subdir,
                            "kind": "skipped_first",
                            "label": "только первый снимок",
                            "rows": row_count,
                            "size": None,
                            "snapshot_id": None,
                            "sha16": None,
                            "extra": "",
                        }
                    )
                    continue

            try:
                mtime, size = _file_stat(file_path)
            except OSError as e:
                stats["errors"] += 1
                logging.warning(f"[archive_sqlite] Нет доступа к файлу {file_path}: {e}")
                events.append(
                    {
                        "sheet": sheet_name,
                        "file": fn,
                        "subdir": subdir,
                        "kind": "io_error",
                        "label": "ошибка доступа к файлу",
                        "rows": row_count,
                        "size": None,
                        "snapshot_id": None,
                        "sha16": None,
                        "extra": str(e),
                    }
                )
                continue

            checked_at = now_utc
            inv = _get_inventory_row(cur, sheet_name, fn, subdir)

            # Таблица arch_* и JSON_*: подготовка на каждом прогоне (не только при новом INSERT).
            # Раньше колонки добавлялись только при новом снимке — при «без изменений» база не менялась.
            table = sheet_to_table_name(sheet_name)
            data_cols = [str(c) for c in df_raw.columns.tolist()]
            _ensure_data_table(cur, table, data_cols, snap_c, row_c, loaded_c)
            sanitized_map: List[Tuple[str, str]] = []
            seen_names: set = set()
            sys_reserved = {snap_c.lower(), row_c.lower(), loaded_c.lower()}
            for orig in data_cols:
                sc = _sanitize_column(orig)
                base = sc
                n = 2
                while sc.lower() in seen_names or sc.lower() in sys_reserved:
                    sc = f"{base}_{n}"
                    n += 1
                seen_names.add(sc.lower())
                sanitized_map.append((orig, sc))
            reserved_for_json: Set[str] = set(sys_reserved)
            for _, sc in sanitized_map:
                reserved_for_json.add(sc.lower())
            json_flat_cols, json_row_maps = plan_archive_json_flat_columns(
                sheet_name, df_raw, reserved_for_json
            )
            existing_tbl_lower = {c.lower() for c in _existing_columns(cur, table)}
            if json_flat_cols:
                ensure_extra_text_columns(
                    cur, table, json_flat_cols, _quote_ident, existing_tbl_lower
                )
                _log_archive_event(
                    log_mode,
                    f"[archive_sqlite] JSON_* для «{sheet_name}» ({table}): {len(json_flat_cols)} колонок "
                    f"(ALTER при необходимости; значения — UPDATE при пропуске ingest или в INSERT при новом снимке)",
                    None,
                )

            quick_changed = True
            if inv is not None:
                quick_changed = (
                    int(inv["last_source_size"]) != size
                    or int(inv["last_source_row_count"]) != row_count
                    or int(inv["last_source_col_count"] or 0) != col_count
                )

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
                    stats["sha_backfill"] += 1
                    _log_archive_event(
                        log_mode,
                        f"[archive_sqlite] Дозаписан SHA-256 без нового снимка: {sheet_name} / {fn}",
                        f"size={size}, rows={row_count}, cols={col_count}, mtime={mtime}",
                        f"sha256={content_hash}" if content_hash else None,
                    )
                    events.append(
                        {
                            "sheet": sheet_name,
                            "file": fn,
                            "subdir": subdir,
                            "kind": "sha_backfill",
                            "label": "обновлён хеш, строки не дублировались",
                            "rows": row_count,
                            "size": size,
                            "snapshot_id": int(sid) if sid is not None else None,
                            "sha16": (content_hash or "")[:16] if content_hash else None,
                            "extra": "",
                        }
                    )
                    if sid is not None and json_flat_cols:
                        _sync_arch_json_flat_columns(
                            cur,
                            table,
                            snap_c,
                            row_c,
                            int(sid),
                            json_flat_cols,
                            json_row_maps,
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
                skips = int(inv["total_skips_same_content"]) + 1
                stats["unchanged"] += 1
                same_by = "SHA-256" if use_sha256 and content_hash is not None else "метаданные (размер/строки/колонки/mtime)"
                _log_archive_event(
                    log_mode,
                    f"[archive_sqlite] Без изменений, снимок не создаётся: {sheet_name} / {fn} "
                    f"(учёт пропусков: {skips}; признак: {same_by})",
                    f"size={size}, rows={row_count}, cols={col_count}",
                    f"path={file_path}" + (f", sha256={content_hash}" if content_hash else ""),
                )
                events.append(
                    {
                        "sheet": sheet_name,
                        "file": fn,
                        "subdir": subdir,
                        "kind": "unchanged",
                        "label": f"без изменений ({same_by})",
                        "rows": row_count,
                        "size": size,
                        "snapshot_id": int(sid) if sid is not None else None,
                        "sha16": (content_hash or "")[:16] if content_hash else None,
                        "extra": f"пропусков_всего={skips}",
                    }
                )
                if sid is not None and json_flat_cols:
                    _sync_arch_json_flat_columns(
                        cur,
                        table,
                        snap_c,
                        row_c,
                        int(sid),
                        json_flat_cols,
                        json_row_maps,
                    )
                conn.commit()
                continue

            if use_sha256 and content_hash is None:
                content_hash = _hash_file(file_path)

            prev = _get_latest_snapshot(cur, sheet_name, fn, subdir)
            prev_id: Optional[int] = int(prev["id"]) if prev is not None else None

            # Повторное появление **того же содержимого**, что уже было в historical: не плодим строки в arch_*
            reuse_hist = bool(cfg.get("reuse_matching_historical_snapshot", True))
            reuse_id: Optional[int] = None
            if reuse_hist and use_sha256 and content_hash:
                reuse_id = _find_snapshot_id_by_content_sha256(
                    cur, sheet_name, fn, subdir, content_hash
                )

            if reuse_id is not None:
                if prev_id is not None and reuse_id == prev_id:
                    cur.execute(
                        f"""
                        UPDATE {INVENTORY_TABLE}
                        SET last_checked_at = ?,
                            total_skips_same_content = total_skips_same_content + 1,
                            resolved_path_last = ?,
                            last_source_mtime = ?, last_source_size = ?,
                            last_source_row_count = ?, last_source_col_count = ?,
                            last_content_sha256 = ?
                        WHERE sheet_name = ? AND file_name = ? AND subdir = ?
                        """,
                        (
                            checked_at,
                            file_path,
                            mtime,
                            size,
                            row_count,
                            col_count,
                            content_hash,
                            sheet_name,
                            fn,
                            subdir,
                        ),
                    )
                    cur.execute(
                        f"""
                        UPDATE {META_TABLE}
                        SET actuality_checked_at = ?, resolved_path = ?,
                            source_mtime = ?, source_size = ?, source_row_count = ?,
                            source_col_count = ?, source_sha256 = ?
                        WHERE id = ?
                        """,
                        (
                            checked_at,
                            file_path,
                            mtime,
                            size,
                            row_count,
                            col_count,
                            content_hash,
                            reuse_id,
                        ),
                    )
                    stats["unchanged"] += 1
                    skips_q = 0
                    if inv is not None:
                        skips_q = int(inv["total_skips_same_content"]) + 1
                    _log_archive_event(
                        log_mode,
                        f"[archive_sqlite] Синхронизация latest с диском (тот же SHA): {sheet_name} / {fn}",
                        f"size={size}, rows={row_count}; пропусков по контенту (счётчик): {skips_q}",
                        f"path={file_path}",
                    )
                    events.append(
                        {
                            "sheet": sheet_name,
                            "file": fn,
                            "subdir": subdir,
                            "kind": "unchanged",
                            "label": "тот же контент у текущего latest (синхр. метаданных)",
                            "rows": row_count,
                            "size": size,
                            "snapshot_id": reuse_id,
                            "sha16": (content_hash or "")[:16] if content_hash else None,
                            "extra": f"пропусков_всего={skips_q}",
                        }
                    )
                    if json_flat_cols:
                        _sync_arch_json_flat_columns(
                            cur,
                            table,
                            snap_c,
                            row_c,
                            int(reuse_id),
                            json_flat_cols,
                            json_row_maps,
                        )
                    conn.commit()
                    continue

                conn.execute("BEGIN IMMEDIATE")
                try:
                    _reactivate_snapshot_as_latest(
                        cur,
                        reuse_snapshot_id=reuse_id,
                        old_latest_id=prev_id,
                        file_path=file_path,
                        mtime=mtime,
                        size=size,
                        row_count=row_count,
                        col_count=col_count,
                        content_hash=content_hash,
                        checked_at=checked_at,
                        sheet_name=sheet_name,
                        file_name=fn,
                        subdir=subdir,
                    )
                    if json_flat_cols:
                        _sync_arch_json_flat_columns(
                            cur,
                            table,
                            snap_c,
                            row_c,
                            int(reuse_id),
                            json_flat_cols,
                            json_row_maps,
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

                stats["reactivated"] += 1
                sha16 = (content_hash or "")[:16] if content_hash else None
                _log_archive_event(
                    log_mode,
                    f"[archive_sqlite] Реактивация снимка id={reuse_id} (тот же SHA, без новых строк в arch_*): "
                    f"{sheet_name} / {fn}"
                    + (f"; прежний latest был id={prev_id}" if prev_id is not None else ""),
                    f"size={size}, rows={row_count}, cols={col_count}",
                    f"path={file_path}" + (f", sha256={content_hash}" if content_hash else ""),
                )
                events.append(
                    {
                        "sheet": sheet_name,
                        "file": fn,
                        "subdir": subdir,
                        "kind": "reactivated",
                        "label": "возвращён ранний снимок с тем же хешем",
                        "rows": row_count,
                        "size": size,
                        "snapshot_id": reuse_id,
                        "sha16": sha16,
                        "extra": f"был_latest_id={prev_id}",
                    }
                )
                continue

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

                insert_cols = (
                    [snap_c, row_c, loaded_c]
                    + [sc for _, sc in sanitized_map]
                    + list(json_flat_cols)
                )
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
                    jmap = json_row_maps[ix] if ix < len(json_row_maps) else {}
                    for jc in json_flat_cols:
                        vals.append(jmap.get(jc, ""))
                    rows_sql.append(tuple(vals))

                if rows_sql:
                    cur.executemany(insert_sql, rows_sql)

                conn.commit()
            except Exception:
                conn.rollback()
                raise

            stats["ingested"] += 1
            sha16 = (content_hash or "")[:16] if content_hash else None
            _log_archive_event(
                log_mode,
                f"[archive_sqlite] Новый снимок id={new_id}: {sheet_name} ({fn}), строк={row_count}, таблица={table}"
                + (f", sha256[0:16]={sha16}…" if sha16 else ""),
                f"size={size} байт, cols={col_count}, предыдущий снимок id={prev_id}",
                f"path={file_path}" + (f", sha256_full={content_hash}" if content_hash else ""),
            )
            events.append(
                {
                    "sheet": sheet_name,
                    "file": fn,
                    "subdir": subdir,
                    "kind": "ingested",
                    "label": "записан новый снимок",
                    "rows": row_count,
                    "size": size,
                    "snapshot_id": new_id,
                    "sha16": sha16,
                    "extra": f"таблица={table}, prev_id={prev_id}",
                }
            )

        summary = (
            f"[archive_sqlite] Итог: новых снимков={stats['ingested']}, без изменений={stats['unchanged']}, "
            f"реактивация historical={stats['reactivated']}, дозапись SHA={stats['sha_backfill']}, "
            f"только первый снимок (пропуск)={stats['skipped_first']}, "
            f"ошибок ввода-вывода={stats['errors']}, вне архива по конфигу={stats['not_requested']}, "
            f"нет данных={stats['no_payload']}"
        )
        _log_archive_summary_line(summary)
        console_ui.print_input_archive_sqlite_report(console_mode, db_display, stats, events)

        conn.commit()
    finally:
        conn.close()
