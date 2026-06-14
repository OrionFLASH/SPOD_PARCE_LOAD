# -*- coding: utf-8 -*-
"""
Статистика по менеджерам: сбор табельных и обогащение колонками по config.json.

manager_stats.sources — уникальные табельные;
manager_stats.enrich_columns — поля с приоритетным забором с листов (фильтры, режимы value/sum/count).
"""

from __future__ import annotations

import fnmatch
import logging
import os
import re
import time
import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd

from src.json_utils import safe_json_loads
from src.rating_item_matrix import _resolve_column

_DEFAULT_TAB_FALLBACKS: List[str] = [
    "Табельный номер",
    "Табельный номер сотрудника",
    "PERSON_NUMBER",
    "PERSON_NUMBER_ADD",
    "MANAGER_PERSON_NUMBER",
    "personNumber",
    "EMPLOYEE_NUMBER",
]

_DEFAULT_SOURCES: List[Dict[str, Any]] = [
    {"id": "employee_person", "sheet": "EMPLOYEE", "tab_column": "PERSON_NUMBER"},
    {"id": "employee_person_add", "sheet": "EMPLOYEE", "tab_column": "PERSON_NUMBER_ADD"},
    {"id": "report_manager", "sheet": "REPORT", "tab_column": "MANAGER_PERSON_NUMBER"},
    {"id": "statistics_tab", "sheet": "STATISTICS", "tab_column": "Табельный номер"},
    {"id": "list_rewards_tab", "sheet": "LIST-REWARDS", "tab_column": "Табельный номер сотрудника"},
    {"id": "rating_agg", "sheet": "RATING", "tab_column": "Табельный номер"},
    {"id": "rating_sheets", "sheet_pattern": "RATING_*", "tab_column": "Табельный номер"},
    {"id": "order_agg", "sheet": "ORDER", "tab_column": "Табельный номер"},
    {"id": "order_sheets", "sheet_pattern": "ORDER_*", "tab_column": "Табельный номер"},
]

_VALID_MODES = frozenset({"value", "sum", "count", "exists"})
_VALID_MULTI_ROW = frozenset({"first", "join"})
_COMPOSITE_KEY_SEP = "\x1f"

_DEFAULT_RATING_NUMERIC_PREFIXES: List[str] = [
    "Количество кристаллов |",
    "Место в рейтинге по стране |",
    "Место в рейтинге ТБ |",
    "Место в рейтинге ГОСБ |",
]

_DEFAULT_COLUMN_FORMATS: List[Dict[str, Any]] = [
    {
        "column_prefixes": list(_DEFAULT_RATING_NUMERIC_PREFIXES),
        "data_type": "number",
        "decimal_places": 0,
        "decimal_separator": ",",
        "thousands_separator": True,
        "horizontal": "center",
        "vertical": "center",
        "wrap_text": False,
    },
    {
        "columns": ["ТБ", "ГОСБ"],
        "data_type": "number",
        "decimal_places": 0,
        "decimal_separator": ",",
        "thousands_separator": False,
        "horizontal": "center",
        "vertical": "center",
        "wrap_text": False,
    },
]

_DEFAULT_COLUMN_WIDTHS: Dict[str, Dict[str, Any]] = {
    "Табельный номер": {"width_mode": 24},
    "Источники": {"width_mode": "AUTO", "min_width": 50, "max_width": 80},
}


def merge_manager_stats_config(raw: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Слияние секции manager_stats из config.json с дефолтами."""
    defaults: Dict[str, Any] = {
        "output_sheet": "TAB_NUMBERS",
        "normalize_pad_width": 20,
        "summary_sheet": "MANAGER_STATS_SUMMARY",
        "sources": list(_DEFAULT_SOURCES),
        "enrich_columns": [],
        "enrich_default": "-",
        "freeze": "E2",
        "column_widths": dict(_DEFAULT_COLUMN_WIDTHS),
        "column_formats": list(_DEFAULT_COLUMN_FORMATS),
        "enrich_parallel": {
            "enabled": True,
            "max_workers": 0,
            "min_tabs_for_parallel": 50,
            "min_fields_for_parallel": 3,
            "chunk_size": 500,
        },
        "prom_tournament_catalog": {
            "enabled": True,
            "sheet_name": "PROM_TOURNAMENTS",
            "schedule_sheet": "TOURNAMENT-SCHEDULE",
            "reward_link_sheet": "REWARD-LINK",
            "contest_sheet": "CONTEST-DATA",
            "reward_sheet": "REWARD",
            "list_rewards_sheet": "LIST-REWARDS",
            "rewards_received_column": "получено наград",
            "active_statuses": ["АКТИВНЫЙ", "ПОДВЕДЕНИЕ ИТОГОВ"],
            "date_year": "2026",
            "contest_vid": "ПРОМ",
            "tab_columns_enabled": True,
            "tab_columns_default": 0,
            "tab_columns_width": 7,
            "tab_columns_total_nagrada": "НАГРАДА всего",
            "tab_columns_total_tournament": "ТУРНИР всего",
            "leaders_for_admin_column": "запрос leadersForAdmin",
            "leaders_for_admin_value_yes": "ДА",
            "leaders_for_admin_contest_type": "ТУРНИРНЫЙ",
            "leaders_for_admin_js_enabled": True,
            "leaders_for_admin_js_file": "Tournament_LeadersForAdmin_AutoRun.js",
            "leaders_for_admin_json_enabled": True,
            "leaders_for_admin_json_subdir": "JS",
            "leaders_for_admin_json_file": "",
            "leaders_for_admin_pretender_categories": [
                "Серебро",
                "Бронза",
                "Вы в лидерах",
            ],
            "tab_columns_pretender_prefix": "ТУРНИР (претендент)",
            "tab_columns_total_pretender": "ТУРНИР (претендент) всего",
            "column_formats": [
                {
                    "columns": ["START_DT", "END_DT"],
                    "data_type": "date",
                    "date_format": "YYYY-MM-DD",
                    "horizontal": "center",
                    "vertical": "center",
                    "wrap_text": False,
                }
            ],
        },
        "profile_gp_load": {
            "js_enabled": True,
            "js_file": "Profile_GP_LOAD_AutoRun.js",
            "js_template": "Profile_GP_LOAD_file.js",
            "js_template_subdir": "JS",
            "json_subdir": "JS",
            "json_file": "",
            "json_files": [],
            "json_enabled": True,
            "json_field_map": {
                "Фамилия": "lastName",
                "Имя": "firstName",
                "ТБ": "tbCode",
                "ГОСБ": "gosbCode",
                "Код роли": "roleCode",
            },
            "js_missing_columns": [
                "Фамилия",
                "Имя",
                "ТБ",
                "ГОСБ",
            ],
            "missing_columns": [
                "Фамилия",
                "Имя",
                "ТБ",
                "ГОСБ",
                "Код роли",
                "Наименование Роли",
                "Email Sigma",
                "Email Alpha",
            ],
            "request_delay_ms": 2,
            "enable_retry": True,
            "max_retries": 1,
            "retry_delay_on_error_ms": 1500,
            "output_base_name": "profiles",
            "batch_size": 12000,
            "enable_photo_download": False,
            "enable_photo_strip": True,
        },
    }
    if not raw:
        return defaults
    out = {**defaults}
    for k in (
        "output_sheet",
        "normalize_pad_width",
        "summary_sheet",
        "sources",
        "enrich_columns",
        "enrich_default",
        "enrich_parallel",
        "freeze",
        "column_widths",
        "column_formats",
        "prom_tournament_catalog",
        "profile_gp_load",
    ):
        if k in raw:
            out[k] = raw[k]
    if not out.get("sources"):
        out["sources"] = list(_DEFAULT_SOURCES)
    ep_raw = out.get("enrich_parallel")
    if isinstance(ep_raw, dict):
        out["enrich_parallel"] = {**defaults["enrich_parallel"], **ep_raw}
    else:
        out["enrich_parallel"] = dict(defaults["enrich_parallel"])
    ptc_raw = out.get("prom_tournament_catalog")
    if isinstance(ptc_raw, dict):
        out["prom_tournament_catalog"] = {**defaults["prom_tournament_catalog"], **ptc_raw}
    else:
        out["prom_tournament_catalog"] = dict(defaults["prom_tournament_catalog"])
    pgl_raw = out.get("profile_gp_load")
    if isinstance(pgl_raw, dict):
        out["profile_gp_load"] = {**defaults["profile_gp_load"], **pgl_raw}
    else:
        out["profile_gp_load"] = dict(defaults["profile_gp_load"])
    cw_raw = out.get("column_widths")
    if isinstance(cw_raw, dict):
        merged_cw = {**_DEFAULT_COLUMN_WIDTHS}
        for col, rule in cw_raw.items():
            cname = str(col).strip()
            if not cname:
                continue
            norm = _normalize_column_width_rule(rule)
            if norm:
                merged_cw[cname] = {**merged_cw.get(cname, {}), **norm}
        out["column_widths"] = merged_cw
    else:
        out["column_widths"] = dict(_DEFAULT_COLUMN_WIDTHS)
    cf_raw = out.get("column_formats")
    if isinstance(cf_raw, list) and cf_raw:
        out["column_formats"] = [r for r in cf_raw if isinstance(r, dict)]
    else:
        out["column_formats"] = list(_DEFAULT_COLUMN_FORMATS)
    return out


def _column_format_rules_from_config(mcfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Правила числового/датового формата для листа TAB_NUMBERS (передаются в write_to_excel)."""
    raw = mcfg.get("column_formats")
    if isinstance(raw, list):
        return [dict(r) for r in raw if isinstance(r, dict)]
    return list(_DEFAULT_COLUMN_FORMATS)


def _normalize_column_width_rule(raw: Any) -> Dict[str, Any]:
    """Правило ширины колонки для added_columns_width (write_to_excel)."""
    if isinstance(raw, (int, float)):
        return {"width_mode": int(raw)}
    if isinstance(raw, str):
        s = raw.strip()
        if s.upper() == "AUTO":
            return {"width_mode": "AUTO"}
        if s.isdigit():
            return {"width_mode": int(s)}
    if isinstance(raw, dict):
        out: Dict[str, Any] = {}
        if "width_mode" in raw:
            out["width_mode"] = raw["width_mode"]
        if "min_width" in raw:
            out["min_width"] = raw["min_width"]
        if "max_width" in raw:
            out["max_width"] = raw["max_width"]
        return out
    return {}


def _added_columns_width_from_config(mcfg: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Словарь ширин колонок листа TAB_NUMBERS для main_impl.calculate_column_width."""
    raw = mcfg.get("column_widths")
    if not isinstance(raw, dict):
        return dict(_DEFAULT_COLUMN_WIDTHS)
    return dict(raw)


def normalize_tab_number(raw: Any, pad_width: int = 20) -> str:
    """Нормализация табельного: trim; числовые — с ведущими нулями до pad_width."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = str(raw).strip()
    if not s or s.lower() in ("-", "none", "null", "nan"):
        return ""
    if re.fullmatch(r"\d+", s):
        if pad_width > 0 and len(s) <= pad_width:
            return s.zfill(pad_width)
        return s
    return s


def _cell_str(x: Any) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).strip()


def is_enrich_value_missing(val: Any, default: str = "-") -> bool:
    """Отсутствие enrich-значения: пусто, NULL/NaN или «-» (и enrich_default)."""
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except (TypeError, ValueError):
        pass
    s = _cell_str(val)
    if not s:
        return True
    low = s.lower()
    if low in ("null", "none", "nan"):
        return True
    default_s = str(default or "-").strip()
    return s == default_s or s == "-"


def _parse_catalog_date_value(x: Any) -> pd.Timestamp:
    """Преобразование ячейки расписания в дату; пустое и «-» → NaT."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return pd.NaT
    if isinstance(x, pd.Timestamp):
        return x
    if isinstance(x, (datetime.datetime, datetime.date)):
        return pd.Timestamp(x)
    if isinstance(x, (int, float)) and not pd.isna(x):
        try:
            return pd.Timestamp("1899-12-30") + pd.Timedelta(days=float(x))
        except (OverflowError, ValueError):
            return pd.NaT
    s = _cell_str(x)
    if not s or s == "-":
        return pd.NaT
    parsed = pd.to_datetime(s, errors="coerce", dayfirst=False)
    return parsed if not pd.isna(parsed) else pd.NaT


def _normalize_bool_token(x: Any) -> str:
    """Приведение bool/строки к true|false для сравнения в фильтрах where_in/where_not_in."""
    if isinstance(x, bool):
        return "true" if x else "false"
    s = _cell_str(x)
    if not s:
        return ""
    low = s.lower()
    if low in ("true", "1", "да", "yes", "y", "истина"):
        return "true"
    if low in ("false", "0", "нет", "no", "n", "ложь"):
        return "false"
    return s


def _normalize_value_list(values: Any) -> List[str]:
    if values is None:
        return []
    if isinstance(values, str):
        s = values.strip()
        return [_normalize_bool_token(s)] if s else []
    if isinstance(values, (list, tuple)):
        return [_normalize_bool_token(v) for v in values]
    return [_normalize_bool_token(values)]


def _normalize_code_key_part(x: Any) -> str:
    """Нормализация части составного ключа (ТБ/ГОСБ): trim; целые числа без .0."""
    s = _cell_str(x)
    if not s or s == "-":
        return ""
    num = _try_parse_number(s)
    if num is not None and abs(num - round(num)) < 1e-9:
        return str(int(round(num)))
    return s


def _composite_key_from_parts(parts: Sequence[str]) -> str:
    return _COMPOSITE_KEY_SEP.join(parts)


def _normalize_key_columns(raw: Any) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        return [s] if s else []
    if isinstance(raw, (list, tuple)):
        return [str(k).strip() for k in raw if str(k).strip()]
    return []


def _row_lookup_key(row: Mapping[str, Any], lookup_row_key: Sequence[str]) -> str:
    """Составной ключ из уже заполненных колонок строки TAB_NUMBERS."""
    parts: List[str] = []
    for col in lookup_row_key:
        parts.append(_normalize_code_key_part(row.get(col)))
    if any(not p for p in parts):
        return ""
    return _composite_key_from_parts(parts)


def _normalize_filter_map(raw: Any) -> Dict[str, List[str]]:
    if not raw or not isinstance(raw, dict):
        return {}
    out: Dict[str, List[str]] = {}
    for col, vals in raw.items():
        c = str(col).strip()
        if c:
            out[c] = _normalize_value_list(vals)
    return out


def _resolve_df_column(
    df: pd.DataFrame,
    column_name: str,
    fallbacks: Optional[Sequence[str]] = None,
) -> Optional[str]:
    name = str(column_name).strip()
    if not name:
        return None
    fb = list(fallbacks) if fallbacks else list(_DEFAULT_TAB_FALLBACKS)
    return _resolve_column(df, name, fb)


def _build_filter_mask(
    df: pd.DataFrame,
    where_in: Mapping[str, Sequence[str]],
    where_not_in: Mapping[str, Sequence[str]],
    *,
    sheet_hint: str = "",
    rule_id: str = "",
) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    hint = f"{sheet_hint} [{rule_id}]".strip()

    for col, allowed in where_in.items():
        resolved = _resolve_df_column(df, col)
        if not resolved:
            logging.warning(
                "[manager_stats] where_in: колонка «%s» не найдена на листе %s — правило пропущено",
                col,
                hint,
            )
            return pd.Series(False, index=df.index)
        colvals = df[resolved].map(_normalize_bool_token)
        mask &= colvals.isin(set(allowed))

    for col, excluded in where_not_in.items():
        resolved = _resolve_df_column(df, col)
        if not resolved:
            logging.warning(
                "[manager_stats] where_not_in: колонка «%s» не найдена на листе %s — условие игнорируется",
                col,
                hint,
            )
            continue
        colvals = df[resolved].map(_normalize_bool_token)
        mask &= ~colvals.isin(set(excluded))

    return mask


def _sheet_locator_from_raw(raw: Mapping[str, Any]) -> Dict[str, str]:
    return {
        "sheet": str(raw.get("sheet") or "").strip(),
        "sheet_pattern": str(raw.get("sheet_pattern") or "").strip(),
    }


def _sheets_for_locator(
    locator: Mapping[str, str],
    available_sheets: Sequence[str],
) -> List[str]:
    exact = str(locator.get("sheet") or "").strip()
    pattern = str(locator.get("sheet_pattern") or "").strip()
    names = sorted(set(available_sheets))
    if exact:
        return [exact] if exact in names else []
    if pattern:
        return [n for n in names if fnmatch.fnmatch(n, pattern)]
    return []


def _get_sheet_df(sheets_data: Mapping[str, Any], sheet_name: str) -> Optional[pd.DataFrame]:
    item = sheets_data.get(sheet_name)
    if not item or not isinstance(item, (list, tuple)) or len(item) < 1:
        return None
    df = item[0]
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None
    return df


def _merge_where_not_in_maps(
    base: Mapping[str, Sequence[str]],
    extra: Mapping[str, Sequence[str]],
) -> Dict[str, List[str]]:
    """Объединение where_not_in по колонкам без дублей значений."""
    out: Dict[str, List[str]] = {}
    for src in (base, extra):
        for col, vals in src.items():
            bucket = out.setdefault(str(col), [])
            for v in _normalize_value_list(vals):
                if v not in bucket:
                    bucket.append(v)
    return out


def _employee_placeholder_tabs_to_exclude(
    sheets_data: Mapping[str, Any],
    rules: Sequence[Mapping[str, Any]],
    *,
    pad_width: int,
) -> Set[str]:
    """
    Табельные из строк EMPLOYEE, отфильтрованных where_not_in правил sources (заглушки SURNAME).

    Убирает из итогового списка номера, которые могли попасть с других листов (REPORT, RATING, …).
    """
    emp_rules = [
        r
        for r in rules
        if str(r.get("sheet") or "").strip() == "EMPLOYEE" and r.get("where_not_in")
    ]
    if not emp_rules:
        return set()

    df = _get_sheet_df(sheets_data, "EMPLOYEE")
    if df is None:
        return set()

    merged_wni = _merge_where_not_in_maps({}, {})
    for rule in emp_rules:
        merged_wni = _merge_where_not_in_maps(merged_wni, rule["where_not_in"])
    if not merged_wni:
        return set()

    keep_mask = _build_filter_mask(
        df,
        {},
        merged_wni,
        sheet_hint="EMPLOYEE",
        rule_id="employee_placeholder_exclusion",
    )
    placeholder_mask = ~keep_mask
    if not placeholder_mask.any():
        return set()

    excluded: Set[str] = set()
    for rule in emp_rules:
        tab_col = _resolve_df_column(df, rule["tab_column"])
        if not tab_col:
            continue
        excluded |= _extract_tabs_from_filtered_rows(
            df, tab_col, placeholder_mask, pad_width=pad_width
        )
    return excluded


def _normalize_source_rule(raw: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    if raw.get("enabled") is False:
        return None
    loc = _sheet_locator_from_raw(raw)
    tab_column = str(raw.get("tab_column") or "").strip()
    if not tab_column:
        logging.warning("[manager_stats] Правило без tab_column пропущено: %s", raw.get("id", "?"))
        return None
    if not loc["sheet"] and not loc["sheet_pattern"]:
        logging.warning(
            "[manager_stats] Правило без sheet/sheet_pattern пропущено: %s",
            raw.get("id", "?"),
        )
        return None
    rid = str(raw.get("id") or "").strip() or f"{loc['sheet'] or loc['sheet_pattern']}:{tab_column}"
    return {
        "id": rid,
        **loc,
        "tab_column": tab_column,
        "where_in": _normalize_filter_map(raw.get("where_in")),
        "where_not_in": _normalize_filter_map(raw.get("where_not_in")),
    }


def _normalize_enrich_source(raw: Mapping[str, Any], index: int) -> Optional[Dict[str, Any]]:
    if raw.get("enabled") is False:
        return None
    loc = _sheet_locator_from_raw(raw)
    tab_column = str(raw.get("tab_column") or "").strip()
    key_columns = _normalize_key_columns(raw.get("key_columns"))
    value_column = str(raw.get("value_column") or "").strip()
    if not tab_column and not key_columns:
        return None
    if not loc["sheet"] and not loc["sheet_pattern"]:
        return None
    try:
        priority = int(raw.get("priority", index + 1))
    except (TypeError, ValueError):
        priority = index + 1
    src_present = raw.get("present_value")
    return {
        **loc,
        "priority": priority,
        "tab_column": tab_column,
        "key_columns": key_columns,
        "value_column": value_column,
        "where_in": _normalize_filter_map(raw.get("where_in")),
        "where_not_in": _normalize_filter_map(raw.get("where_not_in")),
        "present_value": _cell_str(src_present) if src_present is not None else "",
    }


def _normalize_enrich_field(raw: Mapping[str, Any], global_default: str) -> Optional[Dict[str, Any]]:
    if raw.get("enabled") is False:
        return None
    output_column = str(raw.get("output_column") or "").strip()
    if not output_column:
        logging.warning("[manager_stats] enrich_columns: нет output_column, пропуск %s", raw.get("id"))
        return None
    mode = str(raw.get("mode") or "value").strip().lower()
    if mode not in _VALID_MODES:
        logging.warning(
            "[manager_stats] enrich «%s»: неизвестный mode=%s, используем value",
            raw.get("id"),
            mode,
        )
        mode = "value"
    multi_row = str(raw.get("multi_row") or "first").strip().lower()
    if multi_row not in _VALID_MULTI_ROW:
        multi_row = "first"
    raw_sources = raw.get("sources") or []
    sources: List[Dict[str, Any]] = []
    for i, s in enumerate(raw_sources):
        if isinstance(s, dict):
            norm = _normalize_enrich_source(s, i)
            if norm:
                sources.append(norm)
    if not sources:
        logging.warning("[manager_stats] enrich «%s»: нет sources, пропуск", raw.get("id"))
        return None
    sources.sort(key=lambda x: (x["priority"], x.get("sheet") or x.get("sheet_pattern") or ""))
    default_val = raw.get("default")
    if default_val is None:
        default_val = global_default
    lookup_row_key = _normalize_key_columns(raw.get("lookup_row_key"))
    present_value = _cell_str(raw.get("present_value") or "ДА") if mode == "exists" else ""
    return {
        "id": str(raw.get("id") or output_column).strip(),
        "output_column": output_column,
        "default": _cell_str(default_val) if default_val is not None else global_default,
        "mode": mode,
        "multi_row": multi_row,
        "join_separator": str(raw.get("join_separator") or ";"),
        "lookup_row_key": lookup_row_key,
        "present_value": present_value,
        "sources": sources,
    }


def _source_label(sheet_name: str, tab_col: str, rule_id: str) -> str:
    if rule_id:
        return f"{sheet_name} / {tab_col} [{rule_id}]"
    return f"{sheet_name} / {tab_col}"


def _extract_tabs_from_filtered_rows(
    df: pd.DataFrame,
    tab_col: str,
    row_mask: pd.Series,
    *,
    pad_width: int,
) -> Set[str]:
    out: Set[str] = set()
    if tab_col not in df.columns:
        return out
    for val in df.loc[row_mask, tab_col].tolist():
        tab = normalize_tab_number(val, pad_width)
        if tab:
            out.add(tab)
    return out


def _rows_for_tab(
    df: pd.DataFrame,
    tab_col: str,
    target_tab: str,
    where_in: Mapping[str, Sequence[str]],
    where_not_in: Mapping[str, Sequence[str]],
    *,
    pad_width: int,
    sheet_hint: str,
    rule_id: str,
) -> pd.DataFrame:
    if tab_col not in df.columns:
        return df.iloc[0:0]
    tabs_norm = df[tab_col].map(lambda x: normalize_tab_number(x, pad_width))
    mask = tabs_norm == target_tab
    mask &= _build_filter_mask(df, where_in, where_not_in, sheet_hint=sheet_hint, rule_id=rule_id)
    return df.loc[mask]


def _try_parse_number(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s.replace(",", ".").replace(" ", ""))
    except ValueError:
        return None


def _aggregate_field_value(
    sub: pd.DataFrame,
    value_col: Optional[str],
    *,
    mode: str,
    multi_row: str,
    join_separator: str,
) -> Optional[str]:
    """Агрегация значений по отфильтрованным строкам; None — нет данных."""
    if sub.empty:
        return None
    if mode == "count":
        return str(len(sub))

    if not value_col or value_col not in sub.columns:
        return None

    raw_vals = [_cell_str(v) for v in sub[value_col].tolist()]
    non_empty = [v for v in raw_vals if v]
    if not non_empty:
        return None

    if mode == "sum":
        total = 0.0
        found = False
        for v in non_empty:
            num = _try_parse_number(v)
            if num is not None:
                total += num
                found = True
        if not found:
            return None
        if abs(total - round(total)) < 1e-9:
            return str(int(round(total)))
        return str(total)

    # mode == value
    if multi_row == "join":
        unique: List[str] = []
        for v in non_empty:
            if v not in unique:
                unique.append(v)
        return join_separator.join(unique)
    return non_empty[0]


@dataclass
class _SourceIndexEntry:
    """Предрасчитанный индекс tab → значение для одного источника enrich."""

    priority: int
    first_map: Dict[str, str] = field(default_factory=dict)
    join_map: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class _EnrichFieldContext:
    """Контекст обогащения одной выходной колонки."""

    field: Dict[str, Any]
    sources: List[_SourceIndexEntry]


def _merge_parallel_config(raw: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    defaults = {
        "enabled": True,
        "max_workers": 0,
        "min_tabs_for_parallel": 50,
        "chunk_size": 500,
    }
    if not raw:
        return defaults
    return {**defaults, **dict(raw)}


def _parallel_workers(parallel_cfg: Mapping[str, Any], n_tasks: int) -> int:
    """Число потоков для enrich; 1 — без параллелизма."""
    cfg = _merge_parallel_config(parallel_cfg)
    if not cfg.get("enabled", True):
        return 1
    min_tabs = int(cfg.get("min_tabs_for_parallel") or 50)
    if n_tasks < min_tabs:
        return 1
    raw = int(cfg.get("max_workers") or 0)
    if raw <= 0:
        raw = min(8, (os.cpu_count() or 4))
    return max(1, min(raw, n_tasks))


def _parallel_field_workers(parallel_cfg: Mapping[str, Any], n_fields: int) -> int:
    """Потоки для параллельного enrich нескольких колонок (только tab-ключ)."""
    cfg = _merge_parallel_config(parallel_cfg)
    if not cfg.get("enabled", True):
        return 1
    min_fields = int(cfg.get("min_fields_for_parallel") or 3)
    if n_fields < min_fields:
        return 1
    raw = int(cfg.get("max_workers") or 0)
    if raw <= 0:
        raw = min(8, (os.cpu_count() or 4))
    return max(1, min(raw, n_fields))


def _build_source_maps(
    df: pd.DataFrame,
    *,
    tab_col: str,
    value_col: Optional[str],
    where_in: Mapping[str, Sequence[str]],
    where_not_in: Mapping[str, Sequence[str]],
    pad_width: int,
    mode: str,
    multi_row: str,
    join_separator: str,
    present_value: str = "ДА",
) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """Один проход по листу: tab_norm → first/sum/count/exists и списки для join."""
    first_map: Dict[str, str] = {}
    join_map: Dict[str, List[str]] = {}
    if tab_col not in df.columns:
        return first_map, join_map

    mask = _build_filter_mask(df, where_in, where_not_in)
    sub = df.loc[mask]
    if sub.empty:
        return first_map, join_map

    tabs = sub[tab_col].map(lambda x: normalize_tab_number(x, pad_width))
    valid = tabs.astype(str).str.len() > 0
    sub = sub.loc[valid]
    tabs = tabs.loc[valid]
    if sub.empty:
        return first_map, join_map

    if mode == "count":
        for tab, cnt in sub.groupby(tabs, sort=False).size().items():
            first_map[str(tab)] = str(int(cnt))
        return first_map, join_map

    if mode == "exists":
        for tab, cnt in sub.groupby(tabs, sort=False).size().items():
            if int(cnt) > 0:
                first_map[str(tab)] = present_value
        return first_map, join_map

    if not value_col or value_col not in sub.columns:
        return first_map, join_map

    vals = sub[value_col].map(_cell_str)
    work = pd.DataFrame({"tab": tabs.values, "val": vals.values})

    if mode == "sum":
        for tab, grp in work.groupby("tab", sort=False):
            total = 0.0
            found = False
            for v in grp["val"]:
                if not v:
                    continue
                num = _try_parse_number(v)
                if num is not None:
                    total += num
                    found = True
            if found:
                if abs(total - round(total)) < 1e-9:
                    first_map[str(tab)] = str(int(round(total)))
                else:
                    first_map[str(tab)] = str(total)
        return first_map, join_map

    for tab, grp in work.groupby("tab", sort=False):
        non_empty = [v for v in grp["val"].tolist() if v]
        if not non_empty:
            continue
        tab_s = str(tab)
        if multi_row == "join":
            unique: List[str] = []
            for v in non_empty:
                if v not in unique:
                    unique.append(v)
            join_map[tab_s] = unique
            first_map[tab_s] = join_separator.join(unique)
        else:
            first_map[tab_s] = non_empty[0]
    return first_map, join_map


def _resolve_sheet_key_columns(df: pd.DataFrame, key_columns: Sequence[str]) -> List[str]:
    resolved: List[str] = []
    for col in key_columns:
        name = _resolve_df_column(df, col, [col])
        if not name:
            return []
        resolved.append(name)
    return resolved


def _composite_keys_series(sub: pd.DataFrame, key_cols: Sequence[str]) -> pd.Series:
    parts_df = pd.DataFrame({c: sub[c].map(_normalize_code_key_part) for c in key_cols})

    def _row_key(row: pd.Series) -> str:
        vals = row.tolist()
        if any(not v for v in vals):
            return ""
        return _composite_key_from_parts(vals)

    return parts_df.apply(_row_key, axis=1)


def _row_matches_filter_map(
    row: Mapping[str, Any],
    df: pd.DataFrame,
    where_in: Mapping[str, Sequence[str]],
    where_not_in: Mapping[str, Sequence[str]],
) -> bool:
    """Проверка одной строки листа на соответствие where_in / where_not_in."""
    for col, allowed in where_in.items():
        resolved = _resolve_df_column(df, col)
        if not resolved:
            return False
        val = _normalize_bool_token(row.get(resolved))
        if val not in set(allowed):
            return False
    for col, excluded in where_not_in.items():
        resolved = _resolve_df_column(df, col)
        if not resolved:
            continue
        val = _normalize_bool_token(row.get(resolved))
        if val in set(excluded):
            return False
    return True


def _build_exists_join_combined_entry(
    sheets_data: Mapping[str, Any],
    sheet_name: str,
    sources: Sequence[Mapping[str, Any]],
    field: Mapping[str, Any],
    *,
    pad_width: int,
) -> Optional[_SourceIndexEntry]:
    """
    exists + join: один проход по листу — для каждой строки собираем коды всех подходящих sources.
    """
    df = _get_sheet_df(sheets_data, sheet_name)
    if df is None or df.empty or not sources:
        return None
    tab_col = _resolve_df_column(df, sources[0]["tab_column"])
    if not tab_col:
        return None

    join_sep = str(field.get("join_separator") or ";")
    field_present = str(field.get("present_value") or "ДА")
    sources_sorted = sorted(sources, key=lambda s: int(s["priority"]))

    join_map: Dict[str, List[str]] = {}
    first_map: Dict[str, str] = {}
    min_priority = min(int(s["priority"]) for s in sources_sorted)

    for _, row in df.iterrows():
        tab = normalize_tab_number(row.get(tab_col), pad_width)
        if not tab:
            continue
        for src in sources_sorted:
            pv = str(src.get("present_value") or field_present or "ДА")
            if not pv:
                continue
            if _row_matches_filter_map(
                row,
                df,
                src.get("where_in") or {},
                src.get("where_not_in") or {},
            ):
                codes = join_map.setdefault(tab, [])
                if pv not in codes:
                    codes.append(pv)

    for tab, codes in join_map.items():
        first_map[tab] = join_sep.join(codes)

    if not first_map:
        return None
    return _SourceIndexEntry(priority=min_priority, first_map=first_map, join_map=join_map)


def _build_composite_key_maps(
    df: pd.DataFrame,
    *,
    key_cols: Sequence[str],
    value_col: Optional[str],
    where_in: Mapping[str, Sequence[str]],
    where_not_in: Mapping[str, Sequence[str]],
    mode: str,
    multi_row: str,
    join_separator: str,
    present_value: str = "ДА",
) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """Индекс составной ключ (TB_CODE+GOSB_CODE) → значение."""
    first_map: Dict[str, str] = {}
    join_map: Dict[str, List[str]] = {}
    if not key_cols:
        return first_map, join_map

    mask = _build_filter_mask(df, where_in, where_not_in)
    sub = df.loc[mask]
    if sub.empty:
        return first_map, join_map

    keys = _composite_keys_series(sub, key_cols)
    valid = keys.astype(str).str.len() > 0
    sub = sub.loc[valid]
    keys = keys.loc[valid]
    if sub.empty:
        return first_map, join_map

    if mode == "count":
        for key, cnt in sub.groupby(keys, sort=False).size().items():
            first_map[str(key)] = str(int(cnt))
        return first_map, join_map

    if mode == "exists":
        for key, cnt in sub.groupby(keys, sort=False).size().items():
            if int(cnt) > 0:
                first_map[str(key)] = present_value
        return first_map, join_map

    if not value_col or value_col not in sub.columns:
        return first_map, join_map

    vals = sub[value_col].map(_cell_str)
    work = pd.DataFrame({"key": keys.values, "val": vals.values})

    if mode == "sum":
        for key, grp in work.groupby("key", sort=False):
            total = 0.0
            found = False
            for v in grp["val"]:
                if not v:
                    continue
                num = _try_parse_number(v)
                if num is not None:
                    total += num
                    found = True
            if found:
                if abs(total - round(total)) < 1e-9:
                    first_map[str(key)] = str(int(round(total)))
                else:
                    first_map[str(key)] = str(total)
        return first_map, join_map

    for key, grp in work.groupby("key", sort=False):
        non_empty = [v for v in grp["val"].tolist() if v]
        if not non_empty:
            continue
        key_s = str(key)
        if multi_row == "join":
            unique: List[str] = []
            for v in non_empty:
                if v not in unique:
                    unique.append(v)
            join_map[key_s] = unique
            first_map[key_s] = join_separator.join(unique)
        else:
            first_map[key_s] = non_empty[0]
    return first_map, join_map


def _build_source_index_entry(
    sheets_data: Mapping[str, Any],
    sheet_name: str,
    src: Mapping[str, Any],
    field: Mapping[str, Any],
    *,
    pad_width: int,
) -> Optional[_SourceIndexEntry]:
    """Индекс для одного листа и правила source."""
    df = _get_sheet_df(sheets_data, sheet_name)
    if df is None:
        return None
    value_col_cfg = str(src.get("value_column") or "").strip()
    value_col = _resolve_df_column(df, value_col_cfg, [value_col_cfg]) if value_col_cfg else None
    mode = str(field.get("mode") or "value")
    if mode not in ("count", "exists") and not value_col:
        return None
    present_value = str(src.get("present_value") or field.get("present_value") or "ДА")

    key_columns = list(src.get("key_columns") or [])
    if key_columns:
        key_cols = _resolve_sheet_key_columns(df, key_columns)
        if not key_cols:
            return None
        first_map, join_map = _build_composite_key_maps(
            df,
            key_cols=key_cols,
            value_col=value_col,
            where_in=src["where_in"],
            where_not_in=src["where_not_in"],
            mode=mode,
            multi_row=field["multi_row"],
            join_separator=field["join_separator"],
            present_value=present_value,
        )
    else:
        tab_col = _resolve_df_column(df, src["tab_column"])
        if not tab_col:
            return None
        first_map, join_map = _build_source_maps(
            df,
            tab_col=tab_col,
            value_col=value_col,
            where_in=src["where_in"],
            where_not_in=src["where_not_in"],
            pad_width=pad_width,
            mode=mode,
            multi_row=field["multi_row"],
            join_separator=field["join_separator"],
            present_value=present_value,
        )
    if not first_map and not join_map:
        return None
    return _SourceIndexEntry(
        priority=int(src["priority"]),
        first_map=first_map,
        join_map=join_map,
    )


def _build_enrich_field_context(
    field: Mapping[str, Any],
    sheets_data: Mapping[str, Any],
    available: Sequence[str],
    *,
    pad_width: int,
) -> _EnrichFieldContext:
    """Собирает индексы всех sources для одной enrich-колонки."""
    entries: List[_SourceIndexEntry] = []
    use_combined_exists = field["mode"] == "exists" and field["multi_row"] == "join"

    if use_combined_exists:
        combined_groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        for src in field["sources"]:
            if src.get("key_columns"):
                for sheet_name in _sheets_for_locator(src, available):
                    entry = _build_source_index_entry(
                        sheets_data,
                        sheet_name,
                        src,
                        field,
                        pad_width=pad_width,
                    )
                    if entry is not None:
                        entries.append(entry)
                continue
            tab_column = str(src.get("tab_column") or "").strip()
            for sheet_name in _sheets_for_locator(src, available):
                combined_groups[(sheet_name, tab_column)].append(src)

        for (sheet_name, _), group_sources in combined_groups.items():
            entry = _build_exists_join_combined_entry(
                sheets_data,
                sheet_name,
                group_sources,
                field,
                pad_width=pad_width,
            )
            if entry is not None:
                entries.append(entry)
    else:
        for src in field["sources"]:
            for sheet_name in _sheets_for_locator(src, available):
                entry = _build_source_index_entry(
                    sheets_data,
                    sheet_name,
                    src,
                    field,
                    pad_width=pad_width,
                )
                if entry is not None:
                    entries.append(entry)

    entries.sort(key=lambda e: e.priority)
    return _EnrichFieldContext(field=dict(field), sources=entries)


def _lookup_enrich_value_cached(lookup_key: str, ctx: _EnrichFieldContext) -> str:
    """Быстрый lookup по предрасчитанным индексам (табельный или составной ключ)."""
    field = ctx.field
    default = str(field.get("default") or "-")
    if not lookup_key:
        return default
    mode = field["mode"]
    multi_row = field["multi_row"]
    join_sep = field["join_separator"]

    if mode == "value" and multi_row == "join":
        unique: List[str] = []
        for entry in ctx.sources:
            for val in entry.join_map.get(lookup_key, []):
                if val not in unique:
                    unique.append(val)
        return join_sep.join(unique) if unique else default

    if mode == "exists" and multi_row == "join":
        unique: List[str] = []
        for entry in ctx.sources:
            codes = entry.join_map.get(lookup_key)
            if codes:
                for val in codes:
                    if val and val not in unique:
                        unique.append(val)
                continue
            val = entry.first_map.get(lookup_key)
            if val is not None and val != "" and val not in unique:
                unique.append(val)
        return join_sep.join(unique) if unique else default

    for entry in ctx.sources:
        val = entry.first_map.get(lookup_key)
        if val is not None and val != "":
            return val
    return default


def _lookup_keys_for_field(
    lookup_keys: Sequence[str],
    ctx: _EnrichFieldContext,
    parallel_cfg: Mapping[str, Any],
) -> List[str]:
    """Подстановка значений по ключам (табельный или составной), опционально в потоках."""
    n = len(lookup_keys)
    workers = _parallel_workers(parallel_cfg, n)
    if workers <= 1:
        return [_lookup_enrich_value_cached(key, ctx) for key in lookup_keys]

    chunk_size = max(1, int(parallel_cfg.get("chunk_size") or 500))
    chunks: List[Sequence[str]] = [
        lookup_keys[i : i + chunk_size] for i in range(0, n, chunk_size)
    ]

    def _process_chunk(chunk: Sequence[str]) -> List[str]:
        return [_lookup_enrich_value_cached(key, ctx) for key in chunk]

    ordered: List[Optional[List[str]]] = [None] * len(chunks)
    with ThreadPoolExecutor(max_workers=min(workers, len(chunks))) as executor:
        futures = {executor.submit(_process_chunk, chunk): idx for idx, chunk in enumerate(chunks)}
        for fut, idx in futures.items():
            ordered[idx] = fut.result()

    out: List[str] = []
    for part in ordered:
        if part:
            out.extend(part)
    return out


def _build_lookup_keys_for_field(
    df_out: pd.DataFrame,
    field: Mapping[str, Any],
    tabs: Sequence[str],
) -> List[str]:
    """Ключи для lookup: табельный или составной из lookup_row_key."""
    lookup_row_key = list(field.get("lookup_row_key") or [])
    if not lookup_row_key:
        return list(tabs)
    keys: List[str] = []
    for _, row in df_out.iterrows():
        keys.append(_row_lookup_key(row, lookup_row_key))
    return keys


def _try_source_enrich(
    target_tab: str,
    src: Mapping[str, Any],
    sheet_name: str,
    sheets_data: Mapping[str, Any],
    field: Mapping[str, Any],
    *,
    pad_width: int,
    fid: str,
) -> Optional[str]:
    """Одна попытка забора с листа; None — нет данных."""
    df = _get_sheet_df(sheets_data, sheet_name)
    if df is None:
        return None
    tab_col = _resolve_df_column(df, src["tab_column"])
    if not tab_col:
        return None
    value_col_cfg = str(src.get("value_column") or "").strip()
    value_col = _resolve_df_column(df, value_col_cfg, [value_col_cfg]) if value_col_cfg else None
    mode = field["mode"]
    if mode not in ("count", "exists") and not value_col:
        return None
    if mode == "exists":
        sub = _rows_for_tab(
            df,
            tab_col,
            target_tab,
            src["where_in"],
            src["where_not_in"],
            pad_width=pad_width,
            sheet_hint=sheet_name,
            rule_id=fid,
        )
        if sub.empty:
            return None
        return str(src.get("present_value") or field.get("present_value") or "ДА")
    sub = _rows_for_tab(
        df,
        tab_col,
        target_tab,
        src["where_in"],
        src["where_not_in"],
        pad_width=pad_width,
        sheet_hint=sheet_name,
        rule_id=fid,
    )
    return _aggregate_field_value(
        sub,
        value_col,
        mode=field["mode"],
        multi_row=field["multi_row"],
        join_separator=field["join_separator"],
    )


def _collect_values_from_source(
    target_tab: str,
    src: Mapping[str, Any],
    sheet_name: str,
    sheets_data: Mapping[str, Any],
    field: Mapping[str, Any],
    *,
    pad_width: int,
    fid: str,
) -> List[str]:
    """Непустые значения value_column с листа (порядок строк сохраняется)."""
    df = _get_sheet_df(sheets_data, sheet_name)
    if df is None:
        return []
    tab_col = _resolve_df_column(df, src["tab_column"])
    if not tab_col:
        return []
    value_col_cfg = str(src.get("value_column") or "").strip()
    value_col = _resolve_df_column(df, value_col_cfg, [value_col_cfg]) if value_col_cfg else None
    if not value_col:
        return []
    sub = _rows_for_tab(
        df,
        tab_col,
        target_tab,
        src["where_in"],
        src["where_not_in"],
        pad_width=pad_width,
        sheet_hint=sheet_name,
        rule_id=fid,
    )
    if sub.empty or value_col not in sub.columns:
        return []
    return [_cell_str(v) for v in sub[value_col].tolist() if _cell_str(v)]


def _lookup_enrich_value(
    target_tab: str,
    field: Mapping[str, Any],
    sheets_data: Mapping[str, Any],
    available: Sequence[str],
    *,
    pad_width: int,
) -> str:
    """
    Значение поля для табельного.

    value + first / sum / count — первый источник с данными, дальше не ищем.
    value + join — уникальные значения со всех источников через join_separator.
    """
    fid = str(field.get("id") or "")
    default = str(field.get("default") or "-")
    mode = field["mode"]
    multi_row = field["multi_row"]
    join_sep = field["join_separator"]

    if mode == "value" and multi_row == "join":
        unique: List[str] = []
        for src in field["sources"]:
            for sheet_name in _sheets_for_locator(src, available):
                for val in _collect_values_from_source(
                    target_tab,
                    src,
                    sheet_name,
                    sheets_data,
                    field,
                    pad_width=pad_width,
                    fid=fid,
                ):
                    if val not in unique:
                        unique.append(val)
        return join_sep.join(unique) if unique else default

    if mode == "exists" and multi_row == "join":
        unique: List[str] = []
        field_present = str(field.get("present_value") or "ДА")
        for src in sorted(field["sources"], key=lambda s: int(s["priority"])):
            for sheet_name in _sheets_for_locator(src, available):
                df = _get_sheet_df(sheets_data, sheet_name)
                if df is None:
                    continue
                tab_col = _resolve_df_column(df, src["tab_column"])
                if not tab_col:
                    continue
                sub = _rows_for_tab(
                    df,
                    tab_col,
                    target_tab,
                    src["where_in"],
                    src["where_not_in"],
                    pad_width=pad_width,
                    sheet_hint=sheet_name,
                    rule_id=fid,
                )
                if sub.empty:
                    continue
                pv = str(src.get("present_value") or field_present or "ДА")
                if pv and pv not in unique:
                    unique.append(pv)
        return join_sep.join(unique) if unique else default

    for src in field["sources"]:
        for sheet_name in _sheets_for_locator(src, available):
            result = _try_source_enrich(
                target_tab,
                src,
                sheet_name,
                sheets_data,
                field,
                pad_width=pad_width,
                fid=fid,
            )
            if result is not None and result != "":
                return result
    return default


def enrich_tab_dataframe(
    df_tabs: pd.DataFrame,
    sheets_data: Mapping[str, Any],
    cfg: Optional[Mapping[str, Any]] = None,
    *,
    df_catalog: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Добавляет в df_tabs колонки из manager_stats.enrich_columns и PROM_TOURNAMENTS."""
    if df_tabs is None or df_tabs.empty:
        return df_tabs
    mcfg = merge_manager_stats_config(cfg)
    if isinstance(cfg, Mapping) and cfg.get("_paths"):
        mcfg = {**mcfg, "_paths": dict(cfg["_paths"])}
    pad_width = int(mcfg.get("normalize_pad_width") or 20)
    parallel_cfg = _merge_parallel_config(mcfg.get("enrich_parallel"))
    fields = _normalized_enrich_fields_from_config(mcfg)
    tabs = [
        normalize_tab_number(v, pad_width)
        for v in df_tabs["Табельный номер"].tolist()
    ]
    n_tabs = len(tabs)
    out = df_tabs.copy()

    if fields:
        available = [k for k in sheets_data.keys() if sheets_data.get(k) is not None]

        t0 = time.perf_counter()
        contexts: List[_EnrichFieldContext] = []
        index_workers = _parallel_workers(parallel_cfg, len(fields))
        if index_workers <= 1 or len(fields) < 2:
            for fld in fields:
                contexts.append(
                    _build_enrich_field_context(fld, sheets_data, available, pad_width=pad_width)
                )
        else:
            with ThreadPoolExecutor(max_workers=min(index_workers, len(fields))) as executor:
                contexts = list(
                    executor.map(
                        lambda fld: _build_enrich_field_context(
                            fld, sheets_data, available, pad_width=pad_width
                        ),
                        fields,
                    )
                )
        t_index = time.perf_counter() - t0
        total_index_entries = sum(len(c.sources) for c in contexts)

        lookup_workers = _parallel_workers(parallel_cfg, n_tabs)
        t1 = time.perf_counter()

        tab_only_pairs: List[Tuple[Dict[str, Any], _EnrichFieldContext]] = []
        composite_pairs: List[Tuple[Dict[str, Any], _EnrichFieldContext]] = []
        for fld, ctx in zip(fields, contexts):
            if fld.get("lookup_row_key"):
                composite_pairs.append((fld, ctx))
            else:
                tab_only_pairs.append((fld, ctx))

        field_workers = _parallel_field_workers(parallel_cfg, len(tab_only_pairs))

        def _lookup_field_column(
            pair: Tuple[Dict[str, Any], _EnrichFieldContext],
        ) -> Tuple[str, List[str]]:
            fld, ctx = pair
            lookup_keys = _build_lookup_keys_for_field(out, fld, tabs)
            return fld["output_column"], _lookup_keys_for_field(lookup_keys, ctx, parallel_cfg)

        if tab_only_pairs and field_workers > 1:
            with ThreadPoolExecutor(max_workers=min(field_workers, len(tab_only_pairs))) as executor:
                tab_only_results = list(executor.map(_lookup_field_column, tab_only_pairs))
            for col_name, values in tab_only_results:
                out[col_name] = values
            logging.info(
                "[manager_stats] enrich: %s tab-колонок параллельно (workers=%s, строк=%s)",
                len(tab_only_pairs),
                field_workers,
                n_tabs,
            )
        else:
            for fld, ctx in tab_only_pairs:
                col_name = fld["output_column"]
                lookup_keys = _build_lookup_keys_for_field(out, fld, tabs)
                out[col_name] = _lookup_keys_for_field(lookup_keys, ctx, parallel_cfg)
                logging.info(
                    "[manager_stats] enrich «%s» → «%s» (mode=%s, ключ=табельный, индексов=%s, строк=%s)",
                    fld["id"],
                    col_name,
                    fld["mode"],
                    len(ctx.sources),
                    n_tabs,
                )

        t_lookup_tab = time.perf_counter() - t1

        from src.profile_gp_json import apply_profile_gp_json_enrich

        paths_cfg = mcfg.get("_paths")
        out = apply_profile_gp_json_enrich(out, mcfg, paths_cfg=paths_cfg)
        t_json = time.perf_counter()

        t2 = time.perf_counter()
        for fld, ctx in composite_pairs:
            col_name = fld["output_column"]
            lookup_keys = _build_lookup_keys_for_field(out, fld, tabs)
            out[col_name] = _lookup_keys_for_field(lookup_keys, ctx, parallel_cfg)
            logging.info(
                "[manager_stats] enrich «%s» → «%s» (mode=%s, ключ=составной, индексов=%s, строк=%s)",
                fld["id"],
                col_name,
                fld["mode"],
                len(ctx.sources),
                n_tabs,
            )
        t_lookup_composite = time.perf_counter() - t2

        logging.info(
            "[manager_stats] enrich: индексы %s за %.2f с; lookup tab %s за %.2f с; "
            "profile JSON за %.2f с; lookup составной за %.2f с (workers=%s)",
            total_index_entries,
            t_index,
            n_tabs,
            t_lookup_tab - t1,
            t_json - t_lookup_tab,
            t_lookup_composite - t2,
            lookup_workers,
        )

    out, prom_cols, pretender_cols = _append_prom_tournament_tab_columns(
        out,
        sheets_data,
        mcfg,
        tabs,
        df_catalog=df_catalog,
    )

    tail = ["Источники", "Число источников"]
    enrich_names = [f["output_column"] for f in fields]
    head = ["№", "Табельный номер"]
    ordered = head + enrich_names + prom_cols + pretender_cols + [c for c in tail if c in out.columns]
    extra = [c for c in out.columns if c not in ordered]
    return out[ordered + extra]


def collect_tab_numbers_from_sheets(
    sheets_data: Mapping[str, Any],
    input_files: Optional[Sequence[Mapping[str, Any]]] = None,
    cfg: Optional[Mapping[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Собирает уникальные табельные по manager_stats.sources."""
    del input_files
    mcfg = merge_manager_stats_config(cfg)
    pad_width = int(mcfg.get("normalize_pad_width") or 20)
    rules: List[Dict[str, Any]] = []
    for raw in mcfg.get("sources") or []:
        if isinstance(raw, dict):
            norm = _normalize_source_rule(raw)
            if norm:
                rules.append(norm)

    tab_to_sources: Dict[str, Set[str]] = defaultdict(set)
    per_rule_rows: List[Dict[str, Any]] = []
    available = [k for k in sheets_data.keys() if sheets_data.get(k) is not None]

    for rule in rules:
        rid = rule["id"]
        matched_sheets = _sheets_for_locator(rule, available)
        if not matched_sheets:
            continue

        for sheet_name in matched_sheets:
            df = _get_sheet_df(sheets_data, sheet_name)
            if df is None:
                continue

            tab_col = _resolve_df_column(df, rule["tab_column"])
            if not tab_col:
                logging.warning(
                    "[manager_stats] Правило «%s»: колонка «%s» не найдена на «%s»",
                    rid,
                    rule["tab_column"],
                    sheet_name,
                )
                continue

            row_mask = _build_filter_mask(
                df,
                rule["where_in"],
                rule["where_not_in"],
                sheet_hint=sheet_name,
                rule_id=rid,
            )
            n_active = int(row_mask.sum())
            tabs = _extract_tabs_from_filtered_rows(df, tab_col, row_mask, pad_width=pad_width)
            if not tabs:
                continue

            label = _source_label(sheet_name, tab_col, rid)
            for t in tabs:
                tab_to_sources[t].add(label)

            filter_note = _format_filter_note(rule["where_in"], rule["where_not_in"])
            per_rule_rows.append(
                {
                    "Правило": rid,
                    "Лист": sheet_name,
                    "Колонка табельного": tab_col,
                    "Строк после фильтра": n_active,
                    "Уникальных табельных": len(tabs),
                    "Фильтры": filter_note,
                }
            )

    excluded_placeholder = _employee_placeholder_tabs_to_exclude(
        sheets_data, rules, pad_width=pad_width
    )
    if excluded_placeholder:
        removed = 0
        for tab in list(tab_to_sources.keys()):
            if tab in excluded_placeholder:
                del tab_to_sources[tab]
                removed += 1
        logging.info(
            "[manager_stats] Исключено табельных по заглушкам EMPLOYEE (SURNAME и др.): %s",
            removed,
        )
        per_rule_rows.append(
            {
                "Правило": "employee_placeholder_exclusion",
                "Лист": "EMPLOYEE",
                "Колонка табельного": "PERSON_NUMBER; PERSON_NUMBER_ADD",
                "Строк после фильтра": len(excluded_placeholder),
                "Уникальных табельных": removed,
                "Фильтры": "итоговое исключение табельных из строк-заглушек EMPLOYEE",
            }
        )

    rows: List[Dict[str, Any]] = []
    for i, tab in enumerate(sorted(tab_to_sources.keys()), start=1):
        sources = sorted(tab_to_sources[tab])
        rows.append(
            {
                "№": i,
                "Табельный номер": tab,
                "Источники": "; ".join(sources),
                "Число источников": len(sources),
            }
        )

    df_tabs = pd.DataFrame(
        rows,
        columns=["№", "Табельный номер", "Источники", "Число источников"],
    )
    df_rule_summary = pd.DataFrame(
        per_rule_rows,
        columns=[
            "Правило",
            "Лист",
            "Колонка табельного",
            "Строк после фильтра",
            "Уникальных табельных",
            "Фильтры",
        ],
    )
    if not df_rule_summary.empty:
        df_rule_summary = df_rule_summary.sort_values(["Лист", "Правило"]).reset_index(drop=True)

    logging.info(
        "[manager_stats] Всего уникальных табельных: %s; правил с данными: %s",
        len(df_tabs),
        len(df_rule_summary),
    )
    return df_tabs, df_rule_summary


def _format_filter_note(
    where_in: Mapping[str, Sequence[str]],
    where_not_in: Mapping[str, Sequence[str]],
) -> str:
    if not where_in and not where_not_in:
        return "—"
    parts: List[str] = []
    if where_in:
        parts.append("where_in: " + "; ".join(f"{k}∈[{', '.join(v)}]" for k, v in where_in.items()))
    if where_not_in:
        parts.append(
            "where_not_in: " + "; ".join(f"{k}∉[{', '.join(v)}]" for k, v in where_not_in.items())
        )
    return " | ".join(parts)


_SUMMARY_COLUMNS: List[str] = [
    "Раздел",
    "Колонка TAB_NUMBERS",
    "ID",
    "Приоритет",
    "Лист",
    "Сопоставление",
    "Колонка значения",
    "Режим",
    "Логика",
    "Фильтры",
    "Примечание",
]


def _sheet_label_from_locator(loc: Mapping[str, str]) -> str:
    """Имя листа или glob-паттерн для сводки."""
    sheet = str(loc.get("sheet") or "").strip()
    pattern = str(loc.get("sheet_pattern") or "").strip()
    if sheet:
        return sheet
    if pattern:
        return f"pattern:{pattern}"
    return "—"


def _format_enrich_lookup(src: Mapping[str, Any], field: Mapping[str, Any]) -> str:
    """Описание ключа сопоставления enrich-источника."""
    lookup_row_key = list(field.get("lookup_row_key") or [])
    key_columns = list(src.get("key_columns") or [])
    tab_column = str(src.get("tab_column") or "").strip()
    if lookup_row_key:
        lrk = "+".join(lookup_row_key)
        if key_columns:
            return f"строка TAB_NUMBERS:[{lrk}] → лист:[{'+'.join(key_columns)}]"
        return f"строка TAB_NUMBERS:[{lrk}]"
    if key_columns:
        return f"ключ листа:[{'+'.join(key_columns)}]"
    if tab_column:
        return f"табельный:[{tab_column}]"
    return "—"


def _format_enrich_field_logic(field: Mapping[str, Any]) -> str:
    """Человекочитаемое описание режима enrich-колонки."""
    mode = str(field.get("mode") or "value")
    multi_row = str(field.get("multi_row") or "first")
    if mode == "value" and multi_row == "join":
        sep = str(field.get("join_separator") or ";")
        return f"value+join: уникальные значения со всех источников через «{sep}»"
    if mode == "value":
        return "value+first: первый источник с данными (меньший приоритет = раньше)"
    if mode == "sum":
        return "sum: сумма value_column по первому источнику с подходящими строками"
    if mode == "count":
        return "count: число подходящих строк по первому источнику"
    if mode == "exists":
        present = str(field.get("present_value") or "ДА")
        if multi_row == "join":
            sep = str(field.get("join_separator") or ";")
            return f"exists+join: коды present_value со всех подходящих источников через «{sep}»"
        return f"exists: «{present}» если есть хотя бы одна подходящая строка (первый источник)"
    return mode


def _normalized_enrich_fields_from_config(mcfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Нормализованные enrich_columns для сводки и enrich."""
    global_default = _cell_str(mcfg.get("enrich_default")) or "-"
    fields: List[Dict[str, Any]] = []
    for raw in mcfg.get("enrich_columns") or []:
        if isinstance(raw, dict):
            norm = _normalize_enrich_field(raw, global_default)
            if norm:
                fields.append(norm)
    return fields


def _sources_summary_rows(sources_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Строки сводки по правилам sources."""
    rows: List[Dict[str, Any]] = []
    if sources_df is None or sources_df.empty:
        return rows
    for rec in sources_df.to_dict(orient="records"):
        rows.append(
            {
                "Раздел": "Сбор табельных",
                "Колонка TAB_NUMBERS": "Табельный номер",
                "ID": rec.get("Правило", ""),
                "Приоритет": "—",
                "Лист": rec.get("Лист", ""),
                "Сопоставление": rec.get("Колонка табельного", ""),
                "Колонка значения": "—",
                "Режим": "табельный номер",
                "Логика": "уникальные табельные в общий список (объединение всех sources)",
                "Фильтры": rec.get("Фильтры", "—"),
                "Примечание": (
                    f"строк после фильтра: {rec.get('Строк после фильтра', '')}; "
                    f"уникальных таб.: {rec.get('Уникальных табельных', '')}"
                ),
            }
        )
    return rows


def _enrich_summary_rows(mcfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Строки сводки по enrich_columns (что добавляется на лист TAB_NUMBERS)."""
    rows: List[Dict[str, Any]] = []
    for field in _normalized_enrich_fields_from_config(mcfg):
        logic = _format_enrich_field_logic(field)
        default = str(field.get("default") or "-")
        lookup_note = ""
        if field.get("lookup_row_key"):
            lookup_note = f"lookup_row_key: {'+'.join(field['lookup_row_key'])}; "
        note_prefix = f"{lookup_note}default: «{default}»"
        sources = sorted(field.get("sources") or [], key=lambda s: int(s.get("priority") or 0))
        for idx, src in enumerate(sources):
            value_col = str(src.get("value_column") or "").strip()
            mode = str(field.get("mode") or "value")
            if mode in ("count", "exists") and not value_col:
                value_col = "—"
            rows.append(
                {
                    "Раздел": "Обогащение",
                    "Колонка TAB_NUMBERS": field["output_column"],
                    "ID": field["id"],
                    "Приоритет": str(src.get("priority", "")),
                    "Лист": _sheet_label_from_locator(src),
                    "Сопоставление": _format_enrich_lookup(src, field),
                    "Колонка значения": value_col or "—",
                    "Режим": mode,
                    "Логика": logic if idx == 0 else "запасной источник (если выше пусто)",
                    "Фильтры": _format_filter_note(src.get("where_in") or {}, src.get("where_not_in") or {}),
                    "Примечание": (
                        (f"present_value: «{src.get('present_value')}»; " if src.get("present_value") else "")
                        + (note_prefix if idx == 0 else "")
                    ).strip("; "),
                }
            )
    return rows


def _column_formats_summary_rows(mcfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Строки сводки по числовому формату колонок TAB_NUMBERS."""
    rows: List[Dict[str, Any]] = []
    for rule in _column_format_rules_from_config(mcfg):
        prefixes = list(rule.get("column_prefixes") or [])
        columns = list(rule.get("columns") or [])
        targets = prefixes or columns
        if not targets:
            continue
        dtype = str(rule.get("data_type") or "general")
        dec = int(rule.get("decimal_places", 0))
        thousands = bool(rule.get("thousands_separator"))
        rows.append(
            {
                "Раздел": "Формат Excel",
                "Колонка TAB_NUMBERS": "; ".join(str(t) for t in targets[:6])
                + ("; …" if len(targets) > 6 else ""),
                "ID": "column_formats",
                "Приоритет": "—",
                "Лист": str(mcfg.get("output_sheet") or "TAB_NUMBERS"),
                "Сопоставление": "column_prefixes" if prefixes else "columns",
                "Колонка значения": "—",
                "Режим": dtype,
                "Логика": (
                    f"числовой формат при записи Excel "
                    f"(decimal_places={dec}, thousands_separator={thousands})"
                ),
                "Фильтры": "—",
                "Примечание": f"всего префиксов/колонок: {len(targets)}",
            }
        )
    return rows


def build_manager_stats_summary_dataframe(
    sources_summary: pd.DataFrame,
    mcfg: Mapping[str, Any],
    sheets_data: Optional[Mapping[str, Any]] = None,
    *,
    df_catalog: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Полная сводка MANAGER_STATS_SUMMARY: sources, enrich, PROM колонки, форматы."""
    rows: List[Dict[str, Any]] = []
    rows.extend(_sources_summary_rows(sources_summary))
    if rows:
        rows.append({col: "" for col in _SUMMARY_COLUMNS})
    enrich_rows = _enrich_summary_rows(mcfg)
    if enrich_rows:
        rows.extend(enrich_rows)
        rows.append({col: "" for col in _SUMMARY_COLUMNS})
    if sheets_data is not None:
        catalog_cfg = dict(mcfg.get("prom_tournament_catalog") or {})
        if catalog_cfg.get("tab_columns_enabled") is not False:
            if df_catalog is None and sheets_data is not None:
                df_catalog = build_prom_tournament_catalog_dataframe(sheets_data, mcfg)
            if df_catalog is not None and not df_catalog.empty:
                prom_rows = _prom_tab_columns_summary_rows(
                    _build_prom_tab_column_specs(df_catalog),
                    mcfg,
                )
                if prom_rows:
                    rows.extend(prom_rows)
                    rows.append({col: "" for col in _SUMMARY_COLUMNS})
    rows.extend(_column_formats_summary_rows(mcfg))
    if not rows:
        return pd.DataFrame(columns=_SUMMARY_COLUMNS)
    while rows and all(not str(rows[-1].get(c) or "").strip() for c in _SUMMARY_COLUMNS):
        rows.pop()
    return pd.DataFrame(rows, columns=_SUMMARY_COLUMNS)


def _contest_vid_from_cell(raw: Any) -> str:
    """Извлекает vid из ячейки CONTEST_FEATURE (нормализация тройных кавычек в JSON)."""
    s = _cell_str(raw)
    if not s:
        return ""
    parsed = safe_json_loads(s.replace('"""', '"'))
    if isinstance(parsed, dict):
        return _cell_str(parsed.get("vid"))
    return ""


def _map_contest_type_label(raw: Any) -> str:
    """CONTEST_TYPE из CONTEST-DATA → метка для листа PROM_TOURNAMENTS."""
    s = _cell_str(raw)
    if s == "ТУРНИРНЫЙ":
        return "ТУРНИР"
    if s in ("ИНДИВИДУАЛЬНЫЙ", "ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ"):
        return "НАГРАДА"
    return "-"


def _build_contest_prom_index(
    df_contest: pd.DataFrame,
    *,
    contest_vid: str,
) -> Tuple[Set[str], Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str]]:
    """CONTEST_CODE с vid=ПРОМ; словари FULL_NAME, CONTEST_TYPE, PRODUCT_GROUP, PRODUCT."""
    code_col = _resolve_df_column(df_contest, "CONTEST_CODE")
    name_col = _resolve_df_column(df_contest, "FULL_NAME")
    type_col = _resolve_df_column(df_contest, "CONTEST_TYPE")
    pg_col = _resolve_df_column(df_contest, "PRODUCT_GROUP")
    prod_col = _resolve_df_column(df_contest, "PRODUCT")
    if not code_col:
        return set(), {}, {}, {}, {}
    vid_col = _resolve_df_column(df_contest, "CONTEST_FEATURE => vid", ["CONTEST_FEATURE => vid"])
    feat_col = _resolve_df_column(df_contest, "CONTEST_FEATURE")
    prom_codes: Set[str] = set()
    names: Dict[str, str] = {}
    types: Dict[str, str] = {}
    product_groups: Dict[str, str] = {}
    products: Dict[str, str] = {}
    target_vid = _cell_str(contest_vid)
    for _, row in df_contest.iterrows():
        code = _cell_str(row.get(code_col))
        if not code:
            continue
        vid = ""
        if vid_col:
            vid = _cell_str(row.get(vid_col))
        elif feat_col:
            vid = _contest_vid_from_cell(row.get(feat_col))
        if vid != target_vid:
            continue
        prom_codes.add(code)
        if name_col:
            names[code] = _cell_str(row.get(name_col)) or "-"
        if type_col:
            types[code] = _map_contest_type_label(row.get(type_col))
        if pg_col:
            product_groups[code] = _cell_str(row.get(pg_col)) or "-"
        if prod_col:
            products[code] = _cell_str(row.get(prod_col)) or "-"
    return prom_codes, names, types, product_groups, products


def collect_leaders_for_admin_tournament_codes(
    df_schedule: pd.DataFrame,
    df_contest: pd.DataFrame,
    *,
    active_statuses: Sequence[str],
    contest_vid: str = "ПРОМ",
    contest_type_raw: str = "ТУРНИРНЫЙ",
) -> List[str]:
    """
    Уникальные TOURNAMENT_CODE для скрипта leadersForAdmin.

    Условия: TOURNAMENT_STATUS из active_statuses и CONTEST_CODE с vid=ПРОМ,
    у которого CONTEST_TYPE = contest_type_raw (по умолчанию ТУРНИРНЫЙ).
    """
    if df_schedule is None or df_schedule.empty or df_contest is None or df_contest.empty:
        return []

    prom_codes, _, contest_types, _, _ = _build_contest_prom_index(
        df_contest,
        contest_vid=contest_vid,
    )
    target_label = _map_contest_type_label(contest_type_raw)
    tournament_contest_codes = {c for c in prom_codes if contest_types.get(c) == target_label}
    if not tournament_contest_codes:
        return []

    status_col = _resolve_df_column(df_schedule, "TOURNAMENT_STATUS")
    t_col = _resolve_df_column(df_schedule, "TOURNAMENT_CODE")
    c_col = _resolve_df_column(df_schedule, "CONTEST_CODE")
    if not status_col or not t_col or not c_col:
        return []

    active_set = {_cell_str(s) for s in active_statuses if _cell_str(s)}
    codes: List[str] = []
    seen: Set[str] = set()
    for _, row in df_schedule.iterrows():
        if _cell_str(row.get(status_col)) not in active_set:
            continue
        contest_code = _cell_str(row.get(c_col))
        if contest_code not in tournament_contest_codes:
            continue
        tournament_code = _cell_str(row.get(t_col))
        if tournament_code and tournament_code not in seen:
            seen.add(tournament_code)
            codes.append(tournament_code)
    codes.sort()
    return codes


def _schedule_selection_mask(
    df_schedule: pd.DataFrame,
    *,
    active_statuses: Sequence[str],
    date_year: str,
) -> pd.Series:
    """Маска TOURNAMENT-SCHEDULE: активные/подведение итогов или START_DT/END_DT с годом date_year."""
    status_col = _resolve_df_column(df_schedule, "TOURNAMENT_STATUS")
    start_col = _resolve_df_column(df_schedule, "START_DT")
    end_col = _resolve_df_column(df_schedule, "END_DT")
    active_set = {_cell_str(s) for s in active_statuses if _cell_str(s)}
    year = _cell_str(date_year)
    status_ok = (
        df_schedule[status_col].map(_cell_str).isin(active_set)
        if status_col
        else pd.Series(False, index=df_schedule.index)
    )
    start_s = df_schedule[start_col].map(_cell_str) if start_col else pd.Series("", index=df_schedule.index)
    end_s = df_schedule[end_col].map(_cell_str) if end_col else pd.Series("", index=df_schedule.index)
    date_ok = start_s.str.contains(year, na=False) | end_s.str.contains(year, na=False)
    return status_ok | date_ok


def _extract_schedule_pairs(
    df_schedule: pd.DataFrame,
    *,
    mask: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """Уникальные пары TOURNAMENT_CODE+CONTEST_CODE с полями расписания."""
    t_col = _resolve_df_column(df_schedule, "TOURNAMENT_CODE")
    c_col = _resolve_df_column(df_schedule, "CONTEST_CODE")
    period_col = _resolve_df_column(df_schedule, "PERIOD_TYPE")
    start_col = _resolve_df_column(df_schedule, "START_DT")
    end_col = _resolve_df_column(df_schedule, "END_DT")
    status_col = _resolve_df_column(df_schedule, "TOURNAMENT_STATUS")
    if not t_col or not c_col:
        return df_schedule.iloc[0:0]

    base = df_schedule.loc[mask] if mask is not None else df_schedule
    pick_cols: List[str] = [t_col, c_col]
    rename_map: Dict[str, str] = {t_col: "TOURNAMENT_CODE", c_col: "CONTEST_CODE"}
    for src_col, out_name in (
        (period_col, "PERIOD_TYPE"),
        (start_col, "START_DT"),
        (end_col, "END_DT"),
        (status_col, "TOURNAMENT_STATUS"),
    ):
        if src_col:
            pick_cols.append(src_col)
            rename_map[src_col] = out_name

    sub = base.loc[:, pick_cols].copy()
    sub = sub.rename(columns=rename_map)
    for col in ("TOURNAMENT_CODE", "CONTEST_CODE"):
        sub[col] = sub[col].map(_cell_str)
    for col in ("PERIOD_TYPE", "TOURNAMENT_STATUS"):
        if col in sub.columns:
            sub[col] = sub[col].map(lambda x: _cell_str(x) if _cell_str(x) else "-")
    for col in ("START_DT", "END_DT"):
        if col in sub.columns:
            sub[col] = sub[col].map(_parse_catalog_date_value)
    sub = sub[(sub["TOURNAMENT_CODE"] != "") & (sub["CONTEST_CODE"] != "")]
    return sub.drop_duplicates(subset=["TOURNAMENT_CODE", "CONTEST_CODE"])


def _rows_from_schedule_reward_link(
    pairs: pd.DataFrame,
    df_reward_link: Optional[pd.DataFrame],
    reward_link_sheet: str,
) -> pd.DataFrame:
    """Пары расписания + REWARD_CODE из REWARD-LINK по CONTEST_CODE."""
    if pairs.empty:
        return pairs
    rl_contest_col = _resolve_df_column(df_reward_link, "CONTEST_CODE") if df_reward_link is not None else None
    rl_reward_col = _resolve_df_column(df_reward_link, "REWARD_CODE") if df_reward_link is not None else None
    if df_reward_link is not None and rl_contest_col and rl_reward_col:
        links = df_reward_link[[rl_contest_col, rl_reward_col]].copy()
        links.columns = ["CONTEST_CODE", "REWARD_CODE"]
        links["CONTEST_CODE"] = links["CONTEST_CODE"].map(_cell_str)
        links["REWARD_CODE"] = links["REWARD_CODE"].map(_cell_str)
        links = links[(links["CONTEST_CODE"] != "") & (links["REWARD_CODE"] != "")]
        links = links.drop_duplicates(subset=["CONTEST_CODE", "REWARD_CODE"])
        return pairs.merge(links, on="CONTEST_CODE", how="left")
    logging.warning(
        "[manager_stats] PROM_TOURNAMENTS: лист «%s» недоступен — REWARD_CODE из REWARD-LINK не заполняется",
        reward_link_sheet,
    )
    out = pairs.copy()
    out["REWARD_CODE"] = ""
    return out


def _rows_from_list_rewards(
    df_list_rewards: pd.DataFrame,
    schedule_all: pd.DataFrame,
    *,
    prom_codes: Set[str],
    date_year: str,
) -> pd.DataFrame:
    """LIST-REWARDS: Код турнира + Код награды при Дата создания с годом date_year."""
    t_col = _resolve_df_column(df_list_rewards, "Код турнира")
    r_col = _resolve_df_column(df_list_rewards, "Код награды")
    created_col = _resolve_df_column(df_list_rewards, "Дата создания")
    if not t_col or not r_col or not created_col:
        return df_list_rewards.iloc[0:0]
    if schedule_all.empty:
        return df_list_rewards.iloc[0:0]

    year = _cell_str(date_year)
    created_s = df_list_rewards[created_col].map(_cell_str)
    mask = created_s.str.contains(year, na=False)
    lr = df_list_rewards.loc[mask, [t_col, r_col]].copy()
    lr.columns = ["TOURNAMENT_CODE", "REWARD_CODE"]
    lr["TOURNAMENT_CODE"] = lr["TOURNAMENT_CODE"].map(_cell_str)
    lr["REWARD_CODE"] = lr["REWARD_CODE"].map(_cell_str)
    lr = lr[(lr["TOURNAMENT_CODE"] != "") & (lr["REWARD_CODE"] != "")]
    lr = lr.drop_duplicates(subset=["TOURNAMENT_CODE", "REWARD_CODE"])
    if lr.empty:
        return lr

    merged = lr.merge(schedule_all, on="TOURNAMENT_CODE", how="inner")
    merged = merged[merged["CONTEST_CODE"].isin(prom_codes)]
    return merged.drop_duplicates(subset=["TOURNAMENT_CODE", "CONTEST_CODE", "REWARD_CODE"])


def _build_list_rewards_received_counts(
    df_list_rewards: Optional[pd.DataFrame],
) -> Dict[Tuple[str, str], int]:
    """Число строк LIST-REWARDS по паре Код турнира + Код награды."""
    if df_list_rewards is None or df_list_rewards.empty:
        return {}
    t_col = _resolve_df_column(df_list_rewards, "Код турнира")
    r_col = _resolve_df_column(df_list_rewards, "Код награды")
    if not t_col or not r_col:
        return {}
    sub = df_list_rewards[[t_col, r_col]].copy()
    sub.columns = ["TOURNAMENT_CODE", "REWARD_CODE"]
    sub["TOURNAMENT_CODE"] = sub["TOURNAMENT_CODE"].map(_cell_str)
    sub["REWARD_CODE"] = sub["REWARD_CODE"].map(_cell_str)
    sub = sub[(sub["TOURNAMENT_CODE"] != "") & (sub["REWARD_CODE"] != "")]
    if sub.empty:
        return {}
    grouped = sub.groupby(["TOURNAMENT_CODE", "REWARD_CODE"], sort=False).size()
    return {(str(t), str(r)): int(n) for (t, r), n in grouped.items()}


def _reward_names_map(df_reward: Optional[pd.DataFrame]) -> Dict[str, str]:
    """Словарь REWARD_CODE → FULL_NAME."""
    reward_names: Dict[str, str] = {}
    if df_reward is None:
        return reward_names
    rw_code_col = _resolve_df_column(df_reward, "REWARD_CODE")
    rw_name_col = _resolve_df_column(df_reward, "FULL_NAME")
    if not rw_code_col or not rw_name_col:
        return reward_names
    for _, row in df_reward.iterrows():
        code = _cell_str(row.get(rw_code_col))
        if code:
            reward_names[code] = _cell_str(row.get(rw_name_col)) or "-"
    return reward_names


def _apply_prom_catalog_enrichment(
    out: pd.DataFrame,
    *,
    contest_names: Mapping[str, str],
    contest_types: Mapping[str, str],
    product_groups: Mapping[str, str],
    products: Mapping[str, str],
    reward_names: Mapping[str, str],
    received_counts: Optional[Mapping[Tuple[str, str], int]] = None,
    rewards_received_column: str = "получено наград",
) -> pd.DataFrame:
    """Подстановка полей конкурса, награды и количества выдач."""
    df = out.copy()
    df["FULL_NAME"] = df["CONTEST_CODE"].map(lambda c: contest_names.get(c, "-"))
    df["CONTEST_TYPE"] = df["CONTEST_CODE"].map(lambda c: contest_types.get(c, "-"))
    df["PRODUCT_GROUP"] = df["CONTEST_CODE"].map(lambda c: product_groups.get(c, "-"))
    df["PRODUCT"] = df["CONTEST_CODE"].map(lambda c: products.get(c, "-"))
    df["REWARD_CODE"] = df["REWARD_CODE"].map(lambda c: _cell_str(c) if _cell_str(c) else "-")
    df["REWARD_FULL_NAME"] = df["REWARD_CODE"].map(
        lambda c: reward_names.get(c, "-") if c != "-" else "-"
    )
    counts = received_counts or {}
    col = str(rewards_received_column or "получено наград").strip() or "получено наград"
    df[col] = [
        counts.get((t, r), 0) if r != "-" else 0
        for t, r in zip(
            df["TOURNAMENT_CODE"].map(_cell_str),
            df["REWARD_CODE"].map(_cell_str),
        )
    ]
    return df


def _prom_catalog_column_format_rules(catalog_cfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Правила формата колонок листа PROM_TOURNAMENTS (даты START_DT/END_DT)."""
    default: List[Dict[str, Any]] = [
        {
            "columns": ["START_DT", "END_DT"],
            "data_type": "date",
            "date_format": "YYYY-MM-DD",
            "horizontal": "center",
            "vertical": "center",
            "wrap_text": False,
        }
    ]
    raw = catalog_cfg.get("column_formats")
    if isinstance(raw, list) and raw:
        return [dict(r) for r in raw if isinstance(r, dict)]
    return default


_PROM_CATALOG_SORT_PRIORITY: Tuple[str, ...] = (
    "START_DT",
    "REWARD_CODE",
    "PRODUCT",
    "PRODUCT_GROUP",
    "CONTEST_TYPE",
)


def _prom_catalog_sort_text_value(x: Any) -> str:
    """Текстовый ключ сортировки: trim; пустое и «-» → пустая строка."""
    s = _cell_str(x)
    return "" if not s or s == "-" else s


def _sort_prom_catalog_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Многоуровневая сортировка как диалог Excel (уровень 1 — главный ключ).

    Уровни: START_DT → REWARD_CODE → PRODUCT → PRODUCT_GROUP → CONTEST_TYPE.
    """
    if df.empty:
        return df
    keys = df.copy()
    for col in ("START_DT", "END_DT"):
        if col in keys.columns:
            keys[col] = keys[col].map(_parse_catalog_date_value)
    for col in ("PRODUCT", "PRODUCT_GROUP", "REWARD_CODE", "CONTEST_TYPE"):
        if col in keys.columns:
            keys[col] = keys[col].map(_prom_catalog_sort_text_value)
    sort_cols = [c for c in _PROM_CATALOG_SORT_PRIORITY if c in keys.columns]
    if not sort_cols:
        return df
    order = keys.sort_values(by=sort_cols, kind="mergesort", na_position="last").index
    return df.loc[order]


def _finalize_prom_catalog_dataframe(
    out: pd.DataFrame,
    *,
    rewards_received_column: str = "получено наград",
    leaders_for_admin_column: Optional[str] = None,
) -> pd.DataFrame:
    """Сортировка, нумерация и порядок колонок листа PROM_TOURNAMENTS."""
    received_col = str(rewards_received_column or "получено наград").strip() or "получено наград"
    leaders_col = str(leaders_for_admin_column or "").strip()
    work = out.copy()
    sorted_df = _sort_prom_catalog_dataframe(work).reset_index(drop=True)
    for col in ("START_DT", "END_DT"):
        if col in sorted_df.columns:
            sorted_df[col] = sorted_df[col].map(_parse_catalog_date_value)
    sorted_df.insert(0, "№", range(1, len(sorted_df) + 1))
    ordered = [
        "№",
        "TOURNAMENT_CODE",
        "PERIOD_TYPE",
        "START_DT",
        "END_DT",
        "TOURNAMENT_STATUS",
        "CONTEST_CODE",
        "CONTEST_TYPE",
    ]
    if leaders_col and leaders_col in sorted_df.columns:
        ordered.append(leaders_col)
    ordered.extend(
        [
            "PRODUCT",
            "PRODUCT_GROUP",
            "REWARD_CODE",
            "FULL_NAME",
            "REWARD_FULL_NAME",
            received_col,
        ]
    )
    present = [c for c in ordered if c in sorted_df.columns]
    extra = [c for c in sorted_df.columns if c not in present]
    return sorted_df[present + extra]


@dataclass
class _PromTabColumnSpec:
    """Колонка TAB_NUMBERS из каталога PROM_TOURNAMENTS."""

    column_name: str
    contest_type: str
    product_group: str
    product: str
    contest_code: str
    sort_start_dt: pd.Timestamp
    pairs: List[Tuple[str, str]] = field(default_factory=list)
    tournament_code: str = ""


def _format_prom_tab_column_start_dt(value: Any) -> str:
    """Дата START_DT для заголовка колонки TAB_NUMBERS."""
    ts = _parse_catalog_date_value(value)
    if pd.isna(ts):
        return "-"
    return ts.strftime("%Y-%m-%d")


def _prom_tab_column_sort_key(spec: _PromTabColumnSpec) -> Tuple[Any, ...]:
    """Порядок колонок: НАГРАДА → ТУРНИР → PRODUCT_GROUP → PRODUCT → CONTEST_CODE → START_DT."""
    type_rank = 0 if spec.contest_type == "НАГРАДА" else 1
    start = spec.sort_start_dt if not pd.isna(spec.sort_start_dt) else pd.Timestamp.max
    return (
        type_rank,
        _prom_catalog_sort_text_value(spec.product_group),
        _prom_catalog_sort_text_value(spec.product),
        _prom_catalog_sort_text_value(spec.contest_code),
        start,
        spec.column_name,
    )


def _build_prom_tab_column_specs(
    df_catalog: pd.DataFrame,
    *,
    exclude_tournament_codes: Optional[Set[str]] = None,
) -> List[_PromTabColumnSpec]:
    """
    Колонки TAB_NUMBERS по каталогу PROM_TOURNAMENTS.

    НАГРАДА: НАГРАДА REWARD_FULL_NAME (START_DT) [PRODUCT]
    ТУРНИР: ТУРНИР FULL_NAME (START_DT) [PRODUCT]
    Турниры из exclude_tournament_codes (leadersForAdmin JSON) не включаются.
    """
    if df_catalog is None or df_catalog.empty:
        return []
    exclude = exclude_tournament_codes or set()
    buckets: Dict[str, _PromTabColumnSpec] = {}
    for _, row in df_catalog.iterrows():
        contest_type = _cell_str(row.get("CONTEST_TYPE"))
        if contest_type not in ("НАГРАДА", "ТУРНИР"):
            continue
        product_group = _cell_str(row.get("PRODUCT_GROUP")) or "-"
        product = _cell_str(row.get("PRODUCT")) or "-"
        contest_code = _cell_str(row.get("CONTEST_CODE"))
        tournament_code = _cell_str(row.get("TOURNAMENT_CODE"))
        if contest_type == "ТУРНИР" and tournament_code in exclude:
            continue
        reward_code = _cell_str(row.get("REWARD_CODE"))
        if reward_code == "-":
            reward_code = ""
        contest_full_name = _cell_str(row.get("FULL_NAME")) or tournament_code
        reward_full_name = _cell_str(row.get("REWARD_FULL_NAME")) or reward_code
        if contest_full_name == "-":
            contest_full_name = tournament_code
        if reward_full_name == "-":
            reward_full_name = reward_code
        start_dt = _parse_catalog_date_value(row.get("START_DT"))
        start_fmt = _format_prom_tab_column_start_dt(start_dt)
        if contest_type == "ТУРНИР":
            if not tournament_code:
                continue
            column_name = f"ТУРНИР {contest_full_name} ({start_fmt}) [{product}]"
        else:
            if not reward_code:
                continue
            column_name = f"НАГРАДА {reward_full_name} ({start_fmt}) [{product}]"
        pair = (tournament_code, reward_code)
        if column_name not in buckets:
            buckets[column_name] = _PromTabColumnSpec(
                column_name=column_name,
                contest_type=contest_type,
                product_group=product_group,
                product=product,
                contest_code=contest_code,
                sort_start_dt=start_dt,
                tournament_code=tournament_code if contest_type == "ТУРНИР" else "",
            )
        spec = buckets[column_name]
        if pair[0] and pair[1] and pair not in spec.pairs:
            spec.pairs.append(pair)
    specs = list(buckets.values())
    specs.sort(key=_prom_tab_column_sort_key)
    return specs


def _pretender_tab_column_sort_key(spec: _PromTabColumnSpec) -> Tuple[Any, ...]:
    """Порядок колонок претендентов: PRODUCT_GROUP → PRODUCT → CONTEST_CODE → START_DT."""
    start = spec.sort_start_dt if not pd.isna(spec.sort_start_dt) else pd.Timestamp.max
    return (
        _prom_catalog_sort_text_value(spec.product_group),
        _prom_catalog_sort_text_value(spec.product),
        _prom_catalog_sort_text_value(spec.contest_code),
        start,
        spec.column_name,
    )


def _build_pretender_tab_column_specs(
    df_catalog: pd.DataFrame,
    tournament_codes: Sequence[str],
    *,
    pretender_prefix: str,
) -> List[_PromTabColumnSpec]:
    """Колонки TAB_NUMBERS (претендент) по TOURNAMENT_CODE из leadersForAdmin JSON."""
    if df_catalog is None or df_catalog.empty or not tournament_codes:
        return []
    prefix = str(pretender_prefix or "ТУРНИР (претендент)").strip() or "ТУРНИР (претендент)"
    buckets: Dict[str, _PromTabColumnSpec] = {}
    wanted = {_cell_str(c) for c in tournament_codes if _cell_str(c)}
    for _, row in df_catalog.iterrows():
        if _cell_str(row.get("CONTEST_TYPE")) != "ТУРНИР":
            continue
        tournament_code = _cell_str(row.get("TOURNAMENT_CODE"))
        if tournament_code not in wanted:
            continue
        product_group = _cell_str(row.get("PRODUCT_GROUP")) or "-"
        product = _cell_str(row.get("PRODUCT")) or "-"
        contest_code = _cell_str(row.get("CONTEST_CODE"))
        contest_full_name = _cell_str(row.get("FULL_NAME")) or tournament_code
        if contest_full_name == "-":
            contest_full_name = tournament_code
        start_dt = _parse_catalog_date_value(row.get("START_DT"))
        start_fmt = _format_prom_tab_column_start_dt(start_dt)
        column_name = f"{prefix} {contest_full_name} ({start_fmt}) [{product}]"
        if column_name not in buckets:
            buckets[column_name] = _PromTabColumnSpec(
                column_name=column_name,
                contest_type="ТУРНИР_ПРЕТЕНДЕНТ",
                product_group=product_group,
                product=product,
                contest_code=contest_code,
                sort_start_dt=start_dt,
                tournament_code=tournament_code,
            )
    specs = list(buckets.values())
    specs.sort(key=_pretender_tab_column_sort_key)
    return specs


def _build_pretender_tab_columns_matrix(
    tabs: Sequence[str],
    specs: Sequence[_PromTabColumnSpec],
    counts_by_tournament: Mapping[str, Mapping[str, int]],
    *,
    default_num: int,
    summary_col: str,
) -> Tuple[pd.DataFrame, List[str]]:
    """Колонки (претендент): сумма попаданий в JSON по табельному и TOURNAMENT_CODE."""
    if not specs:
        return pd.DataFrame(index=range(len(tabs))), []
    detail_cols = [s.column_name for s in specs]
    col_names: List[str] = []
    if summary_col:
        col_names.append(summary_col)
    col_names.extend(detail_cols)

    detail = pd.DataFrame(default_num, index=list(tabs), columns=detail_cols)
    for spec in specs:
        t_code = _cell_str(spec.tournament_code)
        if not t_code:
            continue
        per_tab = counts_by_tournament.get(t_code) or {}
        for tab, cnt in per_tab.items():
            if tab in detail.index:
                detail.at[tab, spec.column_name] = int(cnt)

    out_cols: Dict[str, pd.Series] = {}
    if summary_col:
        out_cols[summary_col] = detail[detail_cols].sum(axis=1).astype(int)
    for name in detail_cols:
        out_cols[name] = detail[name].astype(int)
    return pd.DataFrame(out_cols, index=list(tabs))[col_names], col_names


def _list_rewards_counts_long_df(
    df_list_rewards: Optional[pd.DataFrame],
    *,
    date_year: str,
    pad_width: int,
) -> pd.DataFrame:
    """Длинный индекс LIST-REWARDS: tab, TOURNAMENT_CODE, REWARD_CODE, n (фильтр по году)."""
    empty = pd.DataFrame(columns=["tab", "TOURNAMENT_CODE", "REWARD_CODE", "n"])
    if df_list_rewards is None or df_list_rewards.empty:
        return empty
    tab_col = _resolve_df_column(df_list_rewards, "Табельный номер сотрудника")
    t_col = _resolve_df_column(df_list_rewards, "Код турнира")
    r_col = _resolve_df_column(df_list_rewards, "Код награды")
    created_col = _resolve_df_column(df_list_rewards, "Дата создания")
    if not tab_col or not t_col or not r_col or not created_col:
        return empty
    year = _cell_str(date_year)
    created_s = df_list_rewards[created_col].map(_cell_str)
    mask = created_s.str.contains(year, na=False)
    sub = df_list_rewards.loc[mask, [tab_col, t_col, r_col]].copy()
    sub.columns = ["tab", "TOURNAMENT_CODE", "REWARD_CODE"]
    sub["tab"] = sub["tab"].map(lambda x: normalize_tab_number(x, pad_width))
    sub["TOURNAMENT_CODE"] = sub["TOURNAMENT_CODE"].map(_cell_str)
    sub["REWARD_CODE"] = sub["REWARD_CODE"].map(_cell_str)
    sub = sub[
        (sub["tab"] != "")
        & (sub["TOURNAMENT_CODE"] != "")
        & (sub["REWARD_CODE"] != "")
    ]
    if sub.empty:
        return empty
    grouped = (
        sub.groupby(["tab", "TOURNAMENT_CODE", "REWARD_CODE"], sort=False)
        .size()
        .reset_index(name="n")
    )
    grouped["n"] = grouped["n"].astype(int)
    return grouped


def _build_list_rewards_employee_counts_by_year(
    df_list_rewards: Optional[pd.DataFrame],
    *,
    date_year: str,
    pad_width: int,
) -> Dict[str, Dict[Tuple[str, str], int]]:
    """Число строк LIST-REWARDS по табельному и паре Код турнира + Код награды (год в Дата создания)."""
    long_df = _list_rewards_counts_long_df(
        df_list_rewards,
        date_year=date_year,
        pad_width=pad_width,
    )
    if long_df.empty:
        return {}
    result: Dict[str, Dict[Tuple[str, str], int]] = defaultdict(dict)
    for row in long_df.itertuples(index=False):
        result[str(row.tab)][(str(row.TOURNAMENT_CODE), str(row.REWARD_CODE))] = int(row.n)
    return dict(result)


def _build_prom_tab_columns_matrix(
    tabs: Sequence[str],
    specs: Sequence[_PromTabColumnSpec],
    counts_long: pd.DataFrame,
    *,
    default_num: int,
    summary_nagrada_col: str,
    summary_turdir_col: str,
) -> Tuple[pd.DataFrame, List[str]]:
    """Векторизованная сборка PROM-колонок: merge + pivot вместо вложенных циклов."""
    nagrada_cols = [s.column_name for s in specs if s.contest_type == "НАГРАДА"]
    turdir_cols = [s.column_name for s in specs if s.contest_type == "ТУРНИР"]
    col_names: List[str] = []
    if nagrada_cols:
        if summary_nagrada_col:
            col_names.append(summary_nagrada_col)
        col_names.extend(nagrada_cols)
    if turdir_cols:
        if summary_turdir_col:
            col_names.append(summary_turdir_col)
        col_names.extend(turdir_cols)
    if not col_names:
        return pd.DataFrame(index=range(len(tabs))), []

    pair_rows: List[Dict[str, str]] = []
    for spec in specs:
        for t_code, r_code in spec.pairs:
            if not t_code or not r_code:
                continue
            pair_rows.append(
                {
                    "column_name": spec.column_name,
                    "TOURNAMENT_CODE": t_code,
                    "REWARD_CODE": r_code,
                }
            )
    if not pair_rows:
        detail = pd.DataFrame(default_num, index=list(tabs), columns=[s.column_name for s in specs])
    elif counts_long.empty:
        detail = pd.DataFrame(default_num, index=list(tabs), columns=[s.column_name for s in specs])
    else:
        pairs_df = pd.DataFrame(pair_rows)
        merged = pairs_df.merge(
            counts_long,
            on=["TOURNAMENT_CODE", "REWARD_CODE"],
            how="left",
        )
        merged["n"] = merged["n"].fillna(0).astype(int)
        per_tab_col = merged.groupby(["tab", "column_name"], sort=False)["n"].sum()
        detail = (
            per_tab_col.unstack(fill_value=0)
            .reindex(list(tabs), fill_value=0)
            .fillna(0)
            .astype(int)
        )
        for spec in specs:
            if spec.column_name not in detail.columns:
                detail[spec.column_name] = default_num
        detail = detail[[s.column_name for s in specs]]

    out_cols: Dict[str, pd.Series] = {}
    if nagrada_cols and summary_nagrada_col:
        out_cols[summary_nagrada_col] = detail[nagrada_cols].sum(axis=1).astype(int)
    if turdir_cols and summary_turdir_col:
        out_cols[summary_turdir_col] = detail[turdir_cols].sum(axis=1).astype(int)
    for name in col_names:
        if name in out_cols:
            continue
        out_cols[name] = detail[name].astype(int)
    return pd.DataFrame(out_cols, index=list(tabs))[col_names], col_names


def _append_prom_tournament_tab_columns(
    df_tabs: pd.DataFrame,
    sheets_data: Mapping[str, Any],
    mcfg: Mapping[str, Any],
    tabs: Sequence[str],
    *,
    df_catalog: Optional[pd.DataFrame] = None,
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """Добавляет на TAB_NUMBERS колонки LIST-REWARDS и (претендент) из leadersForAdmin JSON."""
    catalog_cfg = dict(mcfg.get("prom_tournament_catalog") or {})
    if catalog_cfg.get("tab_columns_enabled") is False:
        return df_tabs, [], []
    t0 = time.perf_counter()
    if df_catalog is None:
        df_catalog = build_prom_tournament_catalog_dataframe(sheets_data, mcfg)
    if df_catalog is None or df_catalog.empty:
        return df_tabs, [], []

    df_schedule = _get_sheet_df(
        sheets_data,
        str(catalog_cfg.get("schedule_sheet") or "TOURNAMENT-SCHEDULE").strip(),
    )
    df_contest = _get_sheet_df(
        sheets_data,
        str(catalog_cfg.get("contest_sheet") or "CONTEST-DATA").strip(),
    )
    js_tournament_codes: Set[str] = set()
    pretender_counts: Dict[str, Dict[str, int]] = {}
    if df_schedule is not None and df_contest is not None:
        leaders_codes = collect_leaders_for_admin_tournament_codes(
            df_schedule,
            df_contest,
            active_statuses=list(
                catalog_cfg.get("active_statuses") or ["АКТИВНЫЙ", "ПОДВЕДЕНИЕ ИТОГОВ"]
            ),
            contest_vid=str(catalog_cfg.get("contest_vid") or "ПРОМ").strip(),
            contest_type_raw=str(
                catalog_cfg.get("leaders_for_admin_contest_type") or "ТУРНИРНЫЙ"
            ).strip(),
        )
        from src.leaders_for_admin_json import (
            parse_leaders_for_admin_pretender_counts,
            pretender_categories_from_config,
            resolve_leaders_for_admin_json_path,
        )

        json_path = resolve_leaders_for_admin_json_path(
            catalog_cfg,
            paths_cfg=mcfg.get("_paths") if isinstance(mcfg.get("_paths"), dict) else None,
        )
        if json_path is not None:
            pad_width = int(mcfg.get("normalize_pad_width") or 20)
            pretender_counts = parse_leaders_for_admin_pretender_counts(
                json_path,
                tournament_codes=set(leaders_codes),
                pretender_categories=pretender_categories_from_config(catalog_cfg),
                pad_width=pad_width,
            )
            js_tournament_codes = set(pretender_counts.keys())

    specs = _build_prom_tab_column_specs(
        df_catalog,
        exclude_tournament_codes=js_tournament_codes,
    )
    prom_col_names: List[str] = []
    out = df_tabs.copy()

    list_rewards_sheet = str(catalog_cfg.get("list_rewards_sheet") or "LIST-REWARDS").strip()
    date_year = str(catalog_cfg.get("date_year") or "2026").strip()
    pad_width = int(mcfg.get("normalize_pad_width") or 20)
    default_val = catalog_cfg.get("tab_columns_default")
    if default_val is None:
        default_val = 0
    try:
        default_num = int(default_val)
    except (TypeError, ValueError):
        default_num = 0

    if specs:
        df_lr = _get_sheet_df(sheets_data, list_rewards_sheet)
        counts_long = _list_rewards_counts_long_df(
            df_lr,
            date_year=date_year,
            pad_width=pad_width,
        )
        summary_nagrada_col = str(
            catalog_cfg.get("tab_columns_total_nagrada") or "НАГРАДА всего"
        ).strip()
        summary_turdir_col = str(
            catalog_cfg.get("tab_columns_total_tournament") or "ТУРНИР всего"
        ).strip()
        prom_df, prom_col_names = _build_prom_tab_columns_matrix(
            tabs,
            specs,
            counts_long,
            default_num=default_num,
            summary_nagrada_col=summary_nagrada_col,
            summary_turdir_col=summary_turdir_col,
        )
        if not prom_df.empty:
            out = pd.concat([out, prom_df.set_index(out.index)], axis=1)

    pretender_prefix = str(
        catalog_cfg.get("tab_columns_pretender_prefix") or "ТУРНИР (претендент)"
    ).strip()
    summary_pretender_col = str(
        catalog_cfg.get("tab_columns_total_pretender") or "ТУРНИР (претендент) всего"
    ).strip()
    pretender_specs = _build_pretender_tab_column_specs(
        df_catalog,
        sorted(js_tournament_codes),
        pretender_prefix=pretender_prefix,
    )
    pretender_col_names: List[str] = []
    if pretender_specs:
        pretender_df, pretender_col_names = _build_pretender_tab_columns_matrix(
            tabs,
            pretender_specs,
            pretender_counts,
            default_num=default_num,
            summary_col=summary_pretender_col,
        )
        if not pretender_df.empty:
            out = pd.concat([out, pretender_df.set_index(out.index)], axis=1)

    logging.info(
        "[manager_stats] PROM колонки TAB_NUMBERS: %s + претендент %s за %.2f с "
        "(LIST-REWARDS %s, JSON турниров=%s, vid=%s)",
        len(prom_col_names),
        len(pretender_col_names),
        time.perf_counter() - t0,
        date_year,
        len(js_tournament_codes),
        catalog_cfg.get("contest_vid") or "ПРОМ",
    )
    return out, prom_col_names, pretender_col_names


def _prom_tab_columns_summary_rows(
    specs: Sequence[_PromTabColumnSpec],
    mcfg: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    """Строки сводки по динамическим колонкам PROM на TAB_NUMBERS."""
    catalog_cfg = dict(mcfg.get("prom_tournament_catalog") or {})
    date_year = str(catalog_cfg.get("date_year") or "2026").strip()
    rows: List[Dict[str, Any]] = []
    for spec in specs:
        pairs_note = "; ".join(f"{t}+{r}" for t, r in spec.pairs[:3])
        if len(spec.pairs) > 3:
            pairs_note += "; …"
        rows.append(
            {
                "Раздел": "PROM колонки",
                "Колонка TAB_NUMBERS": spec.column_name,
                "ID": "prom_tab_column",
                "Приоритет": spec.contest_type,
                "Лист": "LIST-REWARDS",
                "Сопоставление": "TOURNAMENT_CODE + REWARD_CODE",
                "Колонка значения": "count",
                "Режим": "count",
                "Логика": (
                    f"число строк LIST-REWARDS с «Дата создания» {date_year} "
                    f"по табельному и паре турнир+награда"
                ),
                "Фильтры": f"Дата создания ∋ {date_year}",
                "Примечание": (
                    f"{spec.product_group} / {spec.product} / {spec.contest_code}; пары: {pairs_note}"
                ),
            }
        )
    return rows


def _prom_tab_columns_width_params(
    col_names: Sequence[str],
    catalog_cfg: Mapping[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Узкая фиксированная ширина колонок НАГРАДА/ТУРНИР на TAB_NUMBERS."""
    try:
        width = int(catalog_cfg.get("tab_columns_width") or 7)
    except (TypeError, ValueError):
        width = 7
    width = max(4, width)
    rule = {"width_mode": width, "min_width": width, "max_width": width}
    return {name: dict(rule) for name in col_names if name}


def _prom_tab_columns_format_rules(catalog_cfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Число по центру для колонок НАГРАДА/ТУРНИР на TAB_NUMBERS."""
    raw = catalog_cfg.get("tab_columns_format")
    if isinstance(raw, dict) and raw:
        rule = dict(raw)
    else:
        rule = {
            "data_type": "number",
            "decimal_places": 0,
            "decimal_separator": ",",
            "thousands_separator": False,
            "horizontal": "center",
            "vertical": "center",
            "wrap_text": False,
        }
    rule["column_prefixes"] = ["НАГРАДА ", "ТУРНИР "]
    pretender_prefix = str(
        catalog_cfg.get("tab_columns_pretender_prefix") or "ТУРНИР (претендент)"
    ).strip()
    if pretender_prefix and pretender_prefix not in rule["column_prefixes"]:
        rule["column_prefixes"].append(pretender_prefix + " ")
    return [rule]


def build_prom_tournament_catalog_dataframe(
    sheets_data: Mapping[str, Any],
    cfg: Optional[Mapping[str, Any]] = None,
) -> Optional[pd.DataFrame]:
    """
    Каталог турниров/конкурсов/наград ПРОМ для отдельного листа MANAGER_STATS.

    Источники (объединение без дублей):
    1) TOURNAMENT-SCHEDULE (активные/подведение итогов или даты 2026) + REWARD-LINK;
    2) LIST-REWARDS (Дата создания 2026) + TOURNAMENT-SCHEDULE по Код турнира = TOURNAMENT_CODE.
    Только конкурсы CONTEST-DATA с CONTEST_FEATURE.vid = ПРОМ.
    """
    mcfg = merge_manager_stats_config(cfg)
    catalog_cfg = dict(mcfg.get("prom_tournament_catalog") or {})
    if catalog_cfg.get("enabled") is False:
        return None

    schedule_sheet = str(catalog_cfg.get("schedule_sheet") or "TOURNAMENT-SCHEDULE").strip()
    reward_link_sheet = str(catalog_cfg.get("reward_link_sheet") or "REWARD-LINK").strip()
    contest_sheet = str(catalog_cfg.get("contest_sheet") or "CONTEST-DATA").strip()
    reward_sheet = str(catalog_cfg.get("reward_sheet") or "REWARD").strip()
    list_rewards_sheet = str(catalog_cfg.get("list_rewards_sheet") or "LIST-REWARDS").strip()
    rewards_received_column = str(
        catalog_cfg.get("rewards_received_column") or "получено наград"
    ).strip()
    active_statuses = list(catalog_cfg.get("active_statuses") or ["АКТИВНЫЙ", "ПОДВЕДЕНИЕ ИТОГОВ"])
    date_year = str(catalog_cfg.get("date_year") or "2026").strip()
    contest_vid = str(catalog_cfg.get("contest_vid") or "ПРОМ").strip()

    df_schedule = _get_sheet_df(sheets_data, schedule_sheet)
    df_contest = _get_sheet_df(sheets_data, contest_sheet)
    if df_schedule is None or df_contest is None:
        logging.warning(
            "[manager_stats] PROM_TOURNAMENTS: нет листов «%s» или «%s» — лист пропущен",
            schedule_sheet,
            contest_sheet,
        )
        return None

    prom_codes, contest_names, contest_types, product_groups, products = _build_contest_prom_index(
        df_contest,
        contest_vid=contest_vid,
    )
    if not prom_codes:
        logging.warning("[manager_stats] PROM_TOURNAMENTS: нет конкурсов с vid=%s", contest_vid)
        return None

    schedule_all = _extract_schedule_pairs(df_schedule)
    schedule_mask = _schedule_selection_mask(
        df_schedule,
        active_statuses=active_statuses,
        date_year=date_year,
    )
    pairs_filtered = _extract_schedule_pairs(df_schedule, mask=schedule_mask)
    pairs_filtered = pairs_filtered[pairs_filtered["CONTEST_CODE"].isin(prom_codes)]

    parts: List[pd.DataFrame] = []
    n_schedule = 0
    n_list_rewards = 0

    if not pairs_filtered.empty:
        df_reward_link = _get_sheet_df(sheets_data, reward_link_sheet)
        from_schedule = _rows_from_schedule_reward_link(
            pairs_filtered,
            df_reward_link,
            reward_link_sheet,
        )
        if not from_schedule.empty:
            parts.append(from_schedule)
            n_schedule = len(from_schedule)

    df_list_rewards = _get_sheet_df(sheets_data, list_rewards_sheet)
    if df_list_rewards is not None:
        from_lr = _rows_from_list_rewards(
            df_list_rewards,
            schedule_all,
            prom_codes=prom_codes,
            date_year=date_year,
        )
        if not from_lr.empty:
            parts.append(from_lr)
            n_list_rewards = len(from_lr)
    else:
        logging.warning(
            "[manager_stats] PROM_TOURNAMENTS: лист «%s» недоступен — выдачи 2026 не добавляются",
            list_rewards_sheet,
        )

    if not parts:
        logging.info("[manager_stats] PROM_TOURNAMENTS: нет строк после объединения источников")
        return None

    combined = pd.concat(parts, ignore_index=True)
    combined = combined.drop_duplicates(subset=["TOURNAMENT_CODE", "CONTEST_CODE", "REWARD_CODE"])

    df_reward = _get_sheet_df(sheets_data, reward_sheet)
    reward_names = _reward_names_map(df_reward)
    received_counts = _build_list_rewards_received_counts(df_list_rewards)
    out = _apply_prom_catalog_enrichment(
        combined,
        contest_names=contest_names,
        contest_types=contest_types,
        product_groups=product_groups,
        products=products,
        reward_names=reward_names,
        received_counts=received_counts,
        rewards_received_column=rewards_received_column,
    )
    leaders_col = str(
        catalog_cfg.get("leaders_for_admin_column") or "запрос leadersForAdmin"
    ).strip()
    leaders_yes = str(catalog_cfg.get("leaders_for_admin_value_yes") or "ДА").strip() or "ДА"
    leaders_contest_type = str(
        catalog_cfg.get("leaders_for_admin_contest_type") or "ТУРНИРНЫЙ"
    ).strip()
    leaders_codes = set(
        collect_leaders_for_admin_tournament_codes(
            df_schedule,
            df_contest,
            active_statuses=active_statuses,
            contest_vid=contest_vid,
            contest_type_raw=leaders_contest_type,
        )
    )
    if leaders_col:
        out[leaders_col] = out["TOURNAMENT_CODE"].map(
            lambda t: leaders_yes if _cell_str(t) in leaders_codes else "-"
        )
    out = _finalize_prom_catalog_dataframe(
        out,
        rewards_received_column=rewards_received_column,
        leaders_for_admin_column=leaders_col,
    )

    logging.info(
        "[manager_stats] PROM_TOURNAMENTS: %s строк (расписание+REWARD-LINK: %s, LIST-REWARDS 2026: %s, vid=%s, leadersForAdmin: %s)",
        len(out),
        n_schedule,
        n_list_rewards,
        contest_vid,
        len(leaders_codes),
    )
    return out


def build_manager_stats_workbook_data(
    sheets_data: Mapping[str, Any],
    input_files: Optional[Sequence[Mapping[str, Any]]] = None,
    cfg: Optional[Mapping[str, Any]] = None,
    *,
    paths_cfg: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Tuple[pd.DataFrame, Dict[str, Any]]]:
    """Данные для write_to_excel: табельные (с enrich) и сводка по sources/enrich."""
    mcfg = merge_manager_stats_config(cfg)
    if paths_cfg:
        mcfg = {**mcfg, "_paths": dict(paths_cfg)}
    catalog_cfg = dict(mcfg.get("prom_tournament_catalog") or {})
    df_catalog: Optional[pd.DataFrame] = None
    if catalog_cfg.get("enabled") is not False:
        df_catalog = build_prom_tournament_catalog_dataframe(sheets_data, mcfg)

    df_tabs, df_sources_summary = collect_tab_numbers_from_sheets(sheets_data, input_files, mcfg)
    df_tabs = enrich_tab_dataframe(df_tabs, sheets_data, mcfg, df_catalog=df_catalog)

    from src.profile_gp_auto_js import prepare_tabs_for_profile_js

    _, profile_js_tab_count = prepare_tabs_for_profile_js(
        df_tabs,
        mcfg,
        paths_cfg=mcfg.get("_paths"),
    )
    logging.info(
        "[manager_stats] Profile AutoRun: %s табельных с пустыми полями после CSV+JSON enrich",
        len(profile_js_tab_count),
    )

    df_summary = build_manager_stats_summary_dataframe(
        df_sources_summary,
        mcfg,
        sheets_data,
        df_catalog=df_catalog,
    )
    tab_sheet = str(mcfg.get("output_sheet") or "TAB_NUMBERS")
    summary_sheet = str(mcfg.get("summary_sheet") or "MANAGER_STATS_SUMMARY")
    prom_tab_cols = [
        str(c)
        for c in df_tabs.columns
        if str(c).startswith("НАГРАДА ")
        or str(c).startswith("ТУРНИР ")
        or str(c).startswith(
            str(
                (catalog_cfg.get("tab_columns_pretender_prefix") or "ТУРНИР (претендент)")
            ).strip()
            + " "
        )
    ]
    base_params: Dict[str, Any] = {
        "max_col_width": 80,
        "freeze": str(mcfg.get("freeze") or "E2"),
        "col_width_mode": "AUTO",
        "min_col_width": 12,
        "added_columns_width": _added_columns_width_from_config(mcfg),
        "column_format_rules": _column_format_rules_from_config(mcfg),
    }
    tab_params: Dict[str, Any] = {
        **base_params,
        "sheet": tab_sheet,
        "added_columns_width": {
            **dict(base_params.get("added_columns_width") or {}),
            **_prom_tab_columns_width_params(prom_tab_cols, catalog_cfg),
        },
        "column_format_rules": list(base_params.get("column_format_rules") or [])
        + _prom_tab_columns_format_rules(catalog_cfg),
    }
    summary_params: Dict[str, Any] = {
        **base_params,
        "sheet": summary_sheet,
        "added_columns_width": {},
        "freeze": "A2",
    }
    result: Dict[str, Tuple[pd.DataFrame, Dict[str, Any]]] = {
        tab_sheet: (df_tabs, tab_params),
        summary_sheet: (df_summary, summary_params),
    }
    if catalog_cfg.get("enabled") is not False and df_catalog is not None and not df_catalog.empty:
        catalog_sheet = str(catalog_cfg.get("sheet_name") or "PROM_TOURNAMENTS").strip()
        catalog_params: Dict[str, Any] = {
            **base_params,
            "sheet": catalog_sheet,
            "added_columns_width": {},
            "freeze": "A2",
            "column_format_rules": list(base_params.get("column_format_rules") or [])
            + _prom_catalog_column_format_rules(catalog_cfg),
        }
        result[catalog_sheet] = (df_catalog, catalog_params)
    return result
