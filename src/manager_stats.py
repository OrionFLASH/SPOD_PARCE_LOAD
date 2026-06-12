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
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd

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
        "enrich_parallel": {
            "enabled": True,
            "max_workers": 0,
            "min_tabs_for_parallel": 50,
            "chunk_size": 500,
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
    return out


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
    return {
        **loc,
        "priority": priority,
        "tab_column": tab_column,
        "key_columns": key_columns,
        "value_column": value_column,
        "where_in": _normalize_filter_map(raw.get("where_in")),
        "where_not_in": _normalize_filter_map(raw.get("where_not_in")),
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
    present_value = str(field.get("present_value") or "ДА")

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
    if field["mode"] != "count" and not value_col:
        return None
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
) -> pd.DataFrame:
    """Добавляет в df_tabs колонки из manager_stats.enrich_columns."""
    if df_tabs is None or df_tabs.empty:
        return df_tabs
    mcfg = merge_manager_stats_config(cfg)
    pad_width = int(mcfg.get("normalize_pad_width") or 20)
    global_default = _cell_str(mcfg.get("enrich_default")) or "-"
    parallel_cfg = _merge_parallel_config(mcfg.get("enrich_parallel"))
    raw_fields = mcfg.get("enrich_columns") or []
    fields: List[Dict[str, Any]] = []
    for raw in raw_fields:
        if isinstance(raw, dict):
            norm = _normalize_enrich_field(raw, global_default)
            if norm:
                fields.append(norm)

    if not fields:
        return df_tabs

    available = [k for k in sheets_data.keys() if sheets_data.get(k) is not None]
    tabs = df_tabs["Табельный номер"].astype(str).tolist()
    n_tabs = len(tabs)

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

    out = df_tabs.copy()
    lookup_workers = _parallel_workers(parallel_cfg, n_tabs)
    t1 = time.perf_counter()
    for fld, ctx in zip(fields, contexts):
        col_name = fld["output_column"]
        lookup_keys = _build_lookup_keys_for_field(out, fld, tabs)
        out[col_name] = _lookup_keys_for_field(lookup_keys, ctx, parallel_cfg)
        key_kind = "составной" if fld.get("lookup_row_key") else "табельный"
        logging.info(
            "[manager_stats] enrich «%s» → «%s» (mode=%s, ключ=%s, индексов=%s, строк=%s)",
            fld["id"],
            col_name,
            fld["mode"],
            key_kind,
            len(ctx.sources),
            n_tabs,
        )
    t_lookup = time.perf_counter() - t1
    logging.info(
        "[manager_stats] enrich: индексы %s за %.2f с; lookup %s таб. за %.2f с (workers=%s)",
        total_index_entries,
        t_index,
        n_tabs,
        t_lookup,
        lookup_workers,
    )

    tail = ["Источники", "Число источников"]
    enrich_names = [f["output_column"] for f in fields]
    head = ["№", "Табельный номер"]
    ordered = head + enrich_names + [c for c in tail if c in out.columns]
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


def build_manager_stats_workbook_data(
    sheets_data: Mapping[str, Any],
    input_files: Optional[Sequence[Mapping[str, Any]]] = None,
    cfg: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Tuple[pd.DataFrame, Dict[str, Any]]]:
    """Данные для write_to_excel: табельные (с enrich) и сводка по sources."""
    mcfg = merge_manager_stats_config(cfg)
    df_tabs, df_sheet = collect_tab_numbers_from_sheets(sheets_data, input_files, mcfg)
    df_tabs = enrich_tab_dataframe(df_tabs, sheets_data, mcfg)
    tab_sheet = str(mcfg.get("output_sheet") or "TAB_NUMBERS")
    summary_sheet = str(mcfg.get("summary_sheet") or "MANAGER_STATS_SUMMARY")
    base_params: Dict[str, Any] = {
        "max_col_width": 80,
        "freeze": str(mcfg.get("freeze") or "E2"),
        "col_width_mode": "AUTO",
        "min_col_width": 12,
        "added_columns_width": _added_columns_width_from_config(mcfg),
    }
    return {
        tab_sheet: (df_tabs, {**base_params, "sheet": tab_sheet}),
        summary_sheet: (df_sheet, {**base_params, "sheet": summary_sheet, "added_columns_width": {}}),
    }
