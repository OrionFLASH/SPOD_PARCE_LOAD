# -*- coding: utf-8 -*-
"""
Разбор JSON профилей (Profile_GP_LOAD) и дозаполнение enrich_columns.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from src.manager_stats import _cell_str, is_enrich_value_missing, normalize_tab_number

DEFAULT_PROFILE_JSON_FIELD_MAP: Dict[str, str] = {
    "Фамилия": "lastName",
    "Имя": "firstName",
    "ТБ": "tbCode",
    "ГОСБ": "gosbCode",
    "Код роли": "roleCode",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def resolve_profile_gp_json_paths(
    pg_cfg: Mapping[str, Any],
    *,
    paths_cfg: Optional[Mapping[str, Any]] = None,
) -> List[Path]:
    """Пути к JSON профилей в IN/<subdir>/ (один файл или список json_files)."""
    if pg_cfg.get("json_enabled") is False:
        return []

    input_rel = str((paths_cfg or {}).get("input") or "IN").strip() or "IN"
    base = Path(input_rel)
    if not base.is_absolute():
        base = _project_root() / base

    subdir = str(pg_cfg.get("json_subdir") or "JS").strip()
    js_dir = base / subdir if subdir else base

    names: List[str] = []
    raw_files = pg_cfg.get("json_files")
    if isinstance(raw_files, list) and raw_files:
        names = [str(x).strip() for x in raw_files if str(x).strip()]
    else:
        single = str(pg_cfg.get("json_file") or "").strip()
        if single:
            names = [single]

    if not names:
        return []

    paths: List[Path] = []
    for name in names:
        path = js_dir / name
        if path.is_file():
            paths.append(path)
        else:
            logging.warning("[manager_stats] profile GP JSON: файл не найден: %s", path)
    return paths


def _tab_from_profile_record(record: Mapping[str, Any], *, pad_width: int) -> str:
    """Нормализованный табельный из записи JSON."""
    candidates: List[Any] = []
    tn = record.get("tn")
    if tn is not None:
        candidates.append(tn)
    req = record.get("requestBody")
    if isinstance(req, dict):
        candidates.append(req.get("employeeNumber"))
    processed = record.get("processed")
    if isinstance(processed, dict):
        body = processed.get("body")
        if isinstance(body, dict):
            candidates.append(body.get("employeeNumber"))
    for raw in candidates:
        tab = normalize_tab_number(raw, pad_width)
        if tab:
            return tab
    return ""


def _body_from_profile_record(record: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """Тело профиля из успешного ответа или None."""
    if record.get("error"):
        return None
    processed = record.get("processed")
    if not isinstance(processed, dict):
        return None
    if processed.get("success") is False:
        return None
    body = processed.get("body")
    if not isinstance(body, dict):
        return None
    return body


def load_profile_bodies_index(
    json_paths: Sequence[Path],
    *,
    pad_width: int = 20,
) -> Dict[str, Dict[str, Any]]:
    """
    Индекс tab_normalized → body профиля.
    Несколько файлов: поздний перезаписывает табельный при дубликате.
    """
    index: Dict[str, Dict[str, Any]] = {}
    total_records = 0
    for path in json_paths:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logging.warning("[manager_stats] profile GP JSON: не прочитан %s: %s", path, exc)
            continue
        if not isinstance(raw, list):
            logging.warning("[manager_stats] profile GP JSON: ожидался массив в %s", path)
            continue
        for item in raw:
            if not isinstance(item, dict):
                continue
            total_records += 1
            tab = _tab_from_profile_record(item, pad_width=pad_width)
            body = _body_from_profile_record(item)
            if not tab or not body:
                continue
            index[tab] = body
    logging.info(
        "[manager_stats] profile GP JSON: %s файлов, %s записей, %s табельных с телом",
        len(json_paths),
        total_records,
        len(index),
    )
    return index


def profile_json_field_map_from_config(pg_cfg: Mapping[str, Any]) -> Dict[str, str]:
    """Маппинг output_column → ключ body JSON."""
    raw = pg_cfg.get("json_field_map")
    if isinstance(raw, dict) and raw:
        return {
            str(out_col).strip(): str(json_key).strip()
            for out_col, json_key in raw.items()
            if str(out_col).strip() and str(json_key).strip()
        }
    return dict(DEFAULT_PROFILE_JSON_FIELD_MAP)


def apply_profile_gp_json_enrich(
    df_tabs: pd.DataFrame,
    mcfg: Mapping[str, Any],
    *,
    paths_cfg: Optional[Mapping[str, Any]] = None,
) -> pd.DataFrame:
    """
    Дозаполняет пустые enrich-колонки из JSON профилей (по employeeNumber).
    Вызывается после CSV-enrich и до lookup ORG_UNIT по ТБ+ГОСБ.
    """
    if df_tabs is None or df_tabs.empty:
        return df_tabs

    pg_cfg = dict(mcfg.get("profile_gp_load") or {})
    json_paths = resolve_profile_gp_json_paths(pg_cfg, paths_cfg=paths_cfg)
    if not json_paths:
        return df_tabs

    pad_width = int(mcfg.get("normalize_pad_width") or 20)
    default_val = str(mcfg.get("enrich_default") or "-").strip()
    field_map = profile_json_field_map_from_config(pg_cfg)
    index = load_profile_bodies_index(json_paths, pad_width=pad_width)
    if not index:
        return df_tabs

    out = df_tabs.copy()
    tab_col = "Табельный номер"
    if tab_col not in out.columns:
        logging.warning("[manager_stats] profile GP JSON: нет колонки %r", tab_col)
        return df_tabs

    filled_cells = 0
    touched_tabs = 0

    for idx, row in out.iterrows():
        tab = normalize_tab_number(row.get(tab_col), pad_width)
        if not tab:
            continue
        body = index.get(tab)
        if not body:
            continue
        row_filled = False
        for out_col, json_key in field_map.items():
            if out_col not in out.columns:
                continue
            if not is_enrich_value_missing(row.get(out_col), default_val):
                continue
            val = _cell_str(body.get(json_key))
            if val:
                out.at[idx, out_col] = val
                filled_cells += 1
                row_filled = True
        if row_filled:
            touched_tabs += 1

    logging.info(
        "[manager_stats] profile GP JSON enrich: %s ячеек, %s табельных",
        filled_cells,
        touched_tabs,
    )
    return out
