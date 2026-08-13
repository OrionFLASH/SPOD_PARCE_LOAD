# -*- coding: utf-8 -*-
"""Запись Excel-формы BADGE через xlsxwriter (+ выпадающие списки)."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

import xlsxwriter
from xlsxwriter.utility import xl_col_to_name
from xlsxwriter.worksheet import Worksheet

from src.contest_badge_form import schema
from src.contest_badge_form.field_meta import (
    INPUT_KIND_COLORS,
    INPUT_KIND_LABELS,
    INPUT_KIND_ORDER,
    TABLE_COLUMN_HINTS,
    TABLE_DROPDOWNS,
    description_for,
    input_kind_for_kv,
    input_kind_for_table_col,
    merge_dropdowns,
)
from src.contest_badge_form.spod_json import form_cell_from_list, list_from_form_cell

_EXCEL_MAX = 32000
_LISTS_SHEET = "Lists"


def _s(value: Any) -> str:
    """Безопасная строка для ячейки."""
    if value is None:
        return ""
    text = str(value)
    text = "".join(ch for ch in text if ord(ch) >= 32 or ch in "\t\n\r")
    if len(text) > _EXCEL_MAX:
        text = text[: _EXCEL_MAX - 15] + "…[обрезано]"
    return text


def _feature_value(feature: Dict[str, Any], key: str, kind: str) -> str:
    raw = feature.get(key, [] if kind == "list" else "")
    if kind == "list":
        if isinstance(raw, list):
            return form_cell_from_list(raw)
        return form_cell_from_list(list_from_form_cell(raw))
    if raw is None:
        return ""
    if isinstance(raw, list):
        return form_cell_from_list(raw)
    return str(raw)


def _clean_list(values: Sequence[Any]) -> List[str]:
    out: List[str] = []
    for v in values:
        s = _s(v).strip()
        if s and s not in out:
            out.append(s)
    return out


def _needs_lists_sheet(values: List[str]) -> bool:
    """Длинные списки или значения с запятой — только через лист Lists."""
    if not values:
        return False
    if any("," in v for v in values):
        return True
    return len(",".join(values)) > 200


def _build_lists_sheet(
    workbook: Any, dropdown_map: Dict[str, List[str]]
) -> Dict[str, str]:
    """
    Скрытый лист Lists для длинных/с запятыми списков.
    Возвращает key → формула диапазона (=Lists!$A$2:$A$10).
    Лист создаётся в конце книги (после листов 1..N).
    """
    to_sheet = {
        k: _clean_list(v)
        for k, v in dropdown_map.items()
        if _needs_lists_sheet(_clean_list(v))
    }
    if not to_sheet:
        return {}

    # Только формулы заранее — сам лист добавим после данных.
    ranges: Dict[str, str] = {}
    for col, key in enumerate(sorted(to_sheet.keys())):
        values = to_sheet[key]
        letter = xl_col_to_name(col)
        ranges[key] = f"={_LISTS_SHEET}!${letter}$2:${letter}${1 + len(values)}"
    return ranges


def _fill_lists_sheet(
    workbook: Any, dropdown_map: Dict[str, List[str]]
) -> None:
    """Создать и заполнить скрытый лист Lists (вызвать после листов формы)."""
    to_sheet = {
        k: _clean_list(v)
        for k, v in dropdown_map.items()
        if _needs_lists_sheet(_clean_list(v))
    }
    if not to_sheet:
        return
    ws = workbook.add_worksheet(_LISTS_SHEET)
    ws.hide()
    for col, key in enumerate(sorted(to_sheet.keys())):
        values = to_sheet[key]
        ws.write_string(0, col, key[:80])
        for i, val in enumerate(values, start=1):
            ws.write_string(i, col, val)


def _dv_source(
    key: str, values: List[str], list_ranges: Dict[str, str]
) -> Any:
    """source для data_validation: список строк или формула."""
    cleaned = _clean_list(values)
    if not cleaned:
        return None
    if key in list_ranges:
        return list_ranges[key]
    if _needs_lists_sheet(cleaned):
        # не попали на лист — fallback без запятых
        return [v.replace(",", " ") for v in cleaned]
    return cleaned


def _apply_list_validation(
    ws: Worksheet,
    cells_a1: List[str],
    source: Any,
) -> None:
    """Навесить list-validation на ячейки (A1-нотация, API xlsxwriter)."""
    if not cells_a1 or source is None:
        return
    opts: Dict[str, Any] = {
        "validate": "list",
        "source": source,
        "ignore_blank": True,
        "show_error": False,
        "show_input": False,
        "dropdown": True,
    }
    if len(cells_a1) == 1:
        ws.data_validation(cells_a1[0], opts)
        return
    # Несмежные ячейки одного ключа (слоты BADGE:1..N)
    primary = cells_a1[0]
    opts["multi_range"] = " ".join(cells_a1)
    ws.data_validation(primary, opts)


def write_form_xlsx(
    path: str,
    payloads: List[Dict[str, Any]],
    *,
    dropdowns: Optional[Dict[str, List[str]]] = None,
    with_dropdowns: bool = True,
) -> str:
    """
    Записать форму (листы 1..N) через xlsxwriter.
    with_dropdowns=True — выпадающие списки из field_meta (Y/N, ПРОМ/ТЕСТ и др.).
    """
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)

    workbook = xlsxwriter.Workbook(
        path, {"strings_to_urls": False, "nan_inf_to_errors": False}
    )

    fmt_section = workbook.add_format(
        {
            "bold": True,
            "font_color": "FFFFFF",
            "bg_color": "#1F4E79",
            "font_size": 12,
            "valign": "vcenter",
        }
    )
    fmt_key = workbook.add_format(
        {"font_color": "#566573", "bg_color": "#D6EAF8", "font_size": 10}
    )
    fmt_label = workbook.add_format({"bg_color": "#D6EAF8", "font_size": 11})
    fmt_value_by_kind: Dict[str, Any] = {}
    for kind, color in INPUT_KIND_COLORS.items():
        fmt_value_by_kind[kind] = workbook.add_format(
            {"bg_color": color, "valign": "vcenter", "text_wrap": True}
        )
    fmt_desc = workbook.add_format(
        {
            "bg_color": "#F8F9F9",
            "font_color": "#5D6D7E",
            "italic": True,
            "font_size": 9,
            "text_wrap": True,
        }
    )
    fmt_header = workbook.add_format({"bold": True, "bg_color": "#D6EAF8"})
    fmt_header_val = workbook.add_format({"bold": True, "bg_color": "#FFF2CC"})
    fmt_header_desc = workbook.add_format({"bold": True, "bg_color": "#F8F9F9"})
    fmt_table_h = workbook.add_format({"bold": True, "bg_color": "#D5F5E3"})
    fmt_hint = workbook.add_format(
        {
            "italic": True,
            "font_size": 8,
            "font_color": "#1E8449",
            "bg_color": "#E8F8F5",
            "text_wrap": True,
        }
    )
    fmt_note = workbook.add_format({"text_wrap": True, "valign": "top"})
    fmt_legend_label = workbook.add_format(
        {"bold": True, "font_size": 10, "valign": "vcenter"}
    )
    fmt_legend_by_kind: Dict[str, Any] = {}
    for kind, color in INPUT_KIND_COLORS.items():
        fmt_legend_by_kind[kind] = workbook.add_format(
            {
                "bg_color": color,
                "bold": True,
                "font_size": 9,
                "align": "center",
                "valign": "vcenter",
                "border": 1,
            }
        )

    dd_map = merge_dropdowns(dropdowns) if with_dropdowns else {}
    # табличные списки тоже на Lists при необходимости
    for table_key, col_map in TABLE_DROPDOWNS.items():
        for col_name, values in col_map.items():
            dd_map[f"TBL:{table_key}:{col_name}"] = list(values)

    list_ranges = _build_lists_sheet(workbook, dd_map) if with_dropdowns else {}

    use_payloads = payloads if payloads else [{}]
    for idx, payload in enumerate(use_payloads, start=1):
        ws = workbook.add_worksheet(str(idx))
        _write_sheet(
            ws,
            payload,
            fmt_section=fmt_section,
            fmt_key=fmt_key,
            fmt_label=fmt_label,
            fmt_value_by_kind=fmt_value_by_kind,
            fmt_desc=fmt_desc,
            fmt_header=fmt_header,
            fmt_header_val=fmt_header_val,
            fmt_header_desc=fmt_header_desc,
            fmt_table_h=fmt_table_h,
            fmt_hint=fmt_hint,
            fmt_note=fmt_note,
            fmt_legend_label=fmt_legend_label,
            fmt_legend_by_kind=fmt_legend_by_kind,
            dropdown_map=dd_map if with_dropdowns else {},
            list_ranges=list_ranges,
            with_dropdowns=with_dropdowns,
        )

    if with_dropdowns:
        _fill_lists_sheet(workbook, dd_map)

    workbook.close()
    logging.info(
        "[contest_badge_form] Форма (xlsxwriter%s): %s",
        ", dropdowns" if with_dropdowns else "",
        path,
    )
    return path


def _write_kv(
    ws: Worksheet,
    row: int,
    key: str,
    label: str,
    value: Any,
    *,
    in_badge_slot: bool,
    schema_kind: Optional[str],
    fmt_key: Any,
    fmt_label: Any,
    fmt_value_by_kind: Dict[str, Any],
    fmt_desc: Any,
    kv_cells: Dict[str, List[str]],
    dropdown_map: Dict[str, List[str]],
) -> int:
    kind = input_kind_for_kv(
        key,
        schema_kind=schema_kind,
        has_dropdown=key in dropdown_map,
    )
    fmt_value = fmt_value_by_kind.get(kind) or fmt_value_by_kind["text"]
    ws.write_string(row, 0, _s(key), fmt_key)
    ws.write_string(row, 1, _s(label), fmt_label)
    ws.write_string(row, 2, _s(value), fmt_value)
    ws.write_string(
        row,
        3,
        _s(description_for(key, in_badge_slot=in_badge_slot)),
        fmt_desc,
    )
    ws.set_row(row, 28)
    # Excel A1: столбец C, строка row+1
    kv_cells.setdefault(key, []).append(f"C{row + 1}")
    return row + 1


def _write_section(ws: Worksheet, row: int, title: str, fmt_section: Any) -> int:
    for col in range(4):
        ws.write_string(row, col, _s(title) if col == 0 else "", fmt_section)
    ws.set_row(row, 20)
    return row + 1


def _write_legend(
    ws: Worksheet,
    row: int,
    *,
    fmt_legend_label: Any,
    fmt_legend_by_kind: Dict[str, Any],
) -> int:
    """Строка легенды цветов типов ввода (образцы в C–G)."""
    ws.write_string(row, 0, "#META:LEGEND", fmt_legend_label)
    ws.write_string(row, 1, "Цвет значения = тип ввода →", fmt_legend_label)
    for col_idx, kind in enumerate(INPUT_KIND_ORDER):
        label = INPUT_KIND_LABELS.get(kind, kind)
        fmt = fmt_legend_by_kind.get(kind)
        ws.write_string(row, 2 + col_idx, _s(label), fmt)
    ws.set_row(row, 22)
    return row + 1


def _write_table(
    ws: Worksheet,
    row: int,
    marker: str,
    columns: Sequence[str],
    rows: List[Dict[str, Any]],
    *,
    fmt_table_h: Any,
    fmt_hint: Any,
    fmt_value_by_kind: Dict[str, Any],
    min_empty: int = 3,
) -> Tuple[int, str, Sequence[str], int, int]:
    """Возвращает (next_row, table_key, columns, data_start_excel_row, data_end_excel_row)."""
    table_key = marker.replace("#TABLE:", "").strip().upper()
    if table_key == "REWARD_LINK":
        table_key = "REWARD-LINK"
    hints = TABLE_COLUMN_HINTS.get(table_key, {})
    col_kinds = [input_kind_for_table_col(table_key, c) for c in columns]
    col_fmts = [
        fmt_value_by_kind.get(k) or fmt_value_by_kind["text"] for k in col_kinds
    ]

    ws.write_string(row, 0, _s(marker), fmt_table_h)
    row += 1
    for col_idx, col_name in enumerate(columns):
        ws.write_string(row, col_idx, _s(col_name), fmt_table_h)
    row += 1
    for col_idx, col_name in enumerate(columns):
        hint = hints.get(col_name, "значение")
        kind_label = INPUT_KIND_LABELS.get(col_kinds[col_idx], "")
        hint_full = f"{hint} · {kind_label}" if kind_label else hint
        text = f"#HINT | {hint_full}" if col_idx == 0 else hint_full
        ws.write_string(row, col_idx, _s(text), fmt_hint)
    row += 1
    data_start = row + 1  # Excel 1-based
    for data_row in rows:
        for col_idx, col_name in enumerate(columns):
            ws.write_string(
                row, col_idx, _s(data_row.get(col_name, "")), col_fmts[col_idx]
            )
        row += 1
    for _ in range(min_empty):
        for col_idx in range(len(columns)):
            ws.write_string(row, col_idx, "", col_fmts[col_idx])
        row += 1
    data_end = row  # Excel 1-based last row (row is next empty 0-based index)
    return row, table_key, columns, data_start, data_end


def _write_sheet(
    ws: Worksheet,
    payload: Dict[str, Any],
    *,
    fmt_section: Any,
    fmt_key: Any,
    fmt_label: Any,
    fmt_value_by_kind: Dict[str, Any],
    fmt_desc: Any,
    fmt_header: Any,
    fmt_header_val: Any,
    fmt_header_desc: Any,
    fmt_table_h: Any,
    fmt_hint: Any,
    fmt_note: Any,
    fmt_legend_label: Any,
    fmt_legend_by_kind: Dict[str, Any],
    dropdown_map: Dict[str, List[str]],
    list_ranges: Dict[str, str],
    with_dropdowns: bool,
) -> None:
    ws.set_column(0, 0, 30)
    ws.set_column(1, 1, 34)
    ws.set_column(2, 2, 42)
    ws.set_column(3, 3, 62)
    # легенда использует колонки E–F
    ws.set_column(4, 6, 22)

    kv_cells: Dict[str, List[str]] = {}

    row = 0
    ws.write_string(row, 0, "#META:FORM_VERSION")
    ws.write_string(row, 1, "6")
    row = 1
    ws.write_string(row, 0, "#META:NOTE")
    note = (
        "Заполняйте цветные ячейки значений (столбец C и таблицы). "
        "Цвет = тип ввода (легенда ниже). Столбец D — описание. "
        "Где зелёный — выпадающий список. Персик — несколько значений через ; . "
        "Розовый — JSON как в SPOD. Лист = номер конкурса."
    )
    ws.write_string(row, 1, _s(note), fmt_note)
    ws.set_row(row, 40)
    row = 2
    row = _write_legend(
        ws,
        row,
        fmt_legend_label=fmt_legend_label,
        fmt_legend_by_kind=fmt_legend_by_kind,
    )
    row = 3
    ws.write_string(row, 0, "Ключ", fmt_header)
    ws.write_string(row, 1, "Подпись", fmt_header)
    ws.write_string(row, 2, "Значение (заполнять)", fmt_header_val)
    ws.write_string(row, 3, "Описание / значения", fmt_header_desc)
    row = 5

    contest_flat: Dict[str, Any] = payload.get("contest_flat") or {}
    contest_arrays: Dict[str, Any] = payload.get("contest_arrays") or {}
    feature: Dict[str, Any] = payload.get("contest_feature") or {}

    def _kv(
        r: int,
        key: str,
        label: str,
        value: Any,
        *,
        in_badge: bool,
        schema_kind: Optional[str] = None,
    ) -> int:
        return _write_kv(
            ws,
            r,
            key,
            label,
            value,
            in_badge_slot=in_badge,
            schema_kind=schema_kind,
            fmt_key=fmt_key,
            fmt_label=fmt_label,
            fmt_value_by_kind=fmt_value_by_kind,
            fmt_desc=fmt_desc,
            kv_cells=kv_cells,
            dropdown_map=dropdown_map,
        )

    row = _write_section(ws, row, "#SECTION:CONTEST", fmt_section)
    for key, label in schema.CONTEST_FLAT_FIELDS:
        row = _kv(row, key, label, contest_flat.get(key, ""), in_badge=False)
    for key, label in schema.CONTEST_ARRAY_FIELDS:
        row = _kv(
            row,
            key,
            label,
            contest_arrays.get(key, ""),
            in_badge=False,
            schema_kind="list",
        )
    for key, label, kind in schema.CONTEST_FEATURE_FIELDS:
        row = _kv(
            row,
            f"FEATURE.{key}",
            label,
            _feature_value(feature, key, kind),
            in_badge=False,
            schema_kind=kind,
        )
    row += 1

    badges: List[Dict[str, Any]] = list(payload.get("badges") or [])
    contest_type = str(contest_flat.get("CONTEST_TYPE") or "")
    slots = schema.max_badge_slots(contest_type)
    for slot_idx in range(1, slots + 1):
        row = _write_section(ws, row, f"#SECTION:BADGE:{slot_idx}", fmt_section)
        badge = badges[slot_idx - 1] if slot_idx - 1 < len(badges) else {}
        flat = badge.get("flat") or {}
        add_data = badge.get("add_data") or {}
        for key, label in schema.REWARD_FLAT_FIELDS:
            default = "BADGE" if key == "REWARD_TYPE" else ""
            row = _kv(
                row,
                key,
                label,
                flat.get(key, default),
                in_badge=True,
            )
        for key, label, kind in schema.REWARD_ADD_DATA_FIELDS:
            row = _kv(
                row,
                f"ADD.{key}",
                label,
                _feature_value(add_data, key, kind),
                in_badge=True,
                schema_kind=kind,
            )
        row += 1

    table_metas: List[Tuple[str, Sequence[str], int, int]] = []
    for marker, cols, payload_key in (
        ("#TABLE:REWARD-LINK", schema.REWARD_LINK_COLUMNS, "reward_link"),
        ("#TABLE:GROUP", schema.GROUP_COLUMNS, "group"),
        ("#TABLE:INDICATOR", schema.INDICATOR_COLUMNS, "indicator"),
        ("#TABLE:SCHEDULE", schema.SCHEDULE_COLUMNS, "schedule"),
    ):
        row, table_key, cols_out, data_start, data_end = _write_table(
            ws,
            row,
            marker,
            cols,
            list(payload.get(payload_key) or []),
            fmt_table_h=fmt_table_h,
            fmt_hint=fmt_hint,
            fmt_value_by_kind=fmt_value_by_kind,
        )
        table_metas.append((table_key, cols_out, data_start, data_end))

    if not with_dropdowns:
        return

    # KV dropdowns
    for key, cells in kv_cells.items():
        values = dropdown_map.get(key)
        if not values:
            continue
        source = _dv_source(key, values, list_ranges)
        _apply_list_validation(ws, cells, source)

    # Table dropdowns
    for table_key, columns, data_start, data_end in table_metas:
        col_lists = TABLE_DROPDOWNS.get(table_key) or {}
        if not col_lists or data_end < data_start:
            continue
        for col_name, values in col_lists.items():
            if col_name not in columns:
                continue
            col_idx = list(columns).index(col_name)
            letter = xl_col_to_name(col_idx)
            range_a1 = f"{letter}{data_start}:{letter}{data_end}"
            list_key = f"TBL:{table_key}:{col_name}"
            source = _dv_source(list_key, values, list_ranges)
            _apply_list_validation(ws, [range_a1], source)
