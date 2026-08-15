# -*- coding: utf-8 -*-
"""
Запись Excel-формы BADGE через stdlib (zipfile + xml).

Без openpyxl/xlsxwriter при записи — корректный OOXML (sharedStrings),
чтобы Excel/Mac не ругался на «ошибку содержимого».
Чтение формы по-прежнему через openpyxl (есть в Anaconda).
"""

from __future__ import annotations

import logging
import os
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from xml.sax.saxutils import escape

from src.contest_badge_form import schema
from src.contest_badge_form.field_meta import (
    INPUT_KIND_LABELS,
    INPUT_KIND_ORDER,
    TABLE_DROPDOWNS,
    description_for,
    input_kind_for_kv,
    input_kind_for_table_col,
    label_for,
    merge_dropdowns,
    table_hint_for,
)
from src.contest_badge_form.spod_json import form_cell_from_list, list_from_form_cell

_EXCEL_MAX = 32000
_LISTS_SHEET = "Lists"

# Индексы стилей в styles.xml (см. _STYLES_XML)
_S_DEFAULT = 0
_S_SECTION = 1
_S_KEY = 2
_S_LABEL = 3
_S_DESC = 4
_S_HEADER = 5
_S_HEADER_VAL = 6
_S_HEADER_DESC = 7
_S_TABLE = 8
_S_HINT = 9
_S_NOTE = 10
_S_LEGEND_LABEL = 11
# value kinds + legend kinds
_S_VALUE: Dict[str, int] = {
    "dropdown": 12,
    "text": 13,
    "list": 14,
    "json": 15,
    "date": 16,
    "number": 22,
}
_S_LEGEND: Dict[str, int] = {
    "dropdown": 17,
    "text": 18,
    "list": 19,
    "json": 20,
    "date": 21,
    "number": 23,
}


def _s(value: Any) -> str:
    """Безопасная строка для ячейки."""
    if value is None:
        return ""
    text = str(value)
    text = "".join(ch for ch in text if ord(ch) >= 32 or ch in "\t\n\r")
    if len(text) > _EXCEL_MAX:
        text = text[: _EXCEL_MAX - 15] + "…[обрезано]"
    return text


def _xml(text: str) -> str:
    return escape(_s(text), {"'": "&apos;", '"': "&quot;"})


def _col_letter(col_1based: int) -> str:
    n = col_1based
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


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
    if not values:
        return False
    if any("," in v for v in values):
        return True
    return len(",".join(values)) > 200


def _build_lists_ranges(dropdown_map: Dict[str, List[str]]) -> Dict[str, str]:
    to_sheet = {
        k: _clean_list(v)
        for k, v in dropdown_map.items()
        if _needs_lists_sheet(_clean_list(v))
    }
    ranges: Dict[str, str] = {}
    for col, key in enumerate(sorted(to_sheet.keys())):
        values = to_sheet[key]
        letter = _col_letter(col + 1)
        ranges[key] = f"{_LISTS_SHEET}!${letter}$2:${letter}${1 + len(values)}"
    return ranges


def _dv_formula(
    key: str, values: List[str], list_ranges: Dict[str, str]
) -> Optional[str]:
    cleaned = _clean_list(values)
    if not cleaned:
        return None
    if key in list_ranges:
        return list_ranges[key]
    if _needs_lists_sheet(cleaned):
        cleaned = [v.replace(",", " ") for v in cleaned]
    if all("," not in v for v in cleaned) and len(",".join(cleaned)) <= 240:
        return '"' + ",".join(cleaned) + '"'
    joined = ",".join(v.replace(",", " ") for v in cleaned)
    if len(joined) > 240:
        joined = ",".join(cleaned[:15])
    return f'"{joined}"'


class _SheetBuilder:
    """Сетка ячеек + DV + ширины колонок."""

    def __init__(self, name: str, *, hidden: bool = False) -> None:
        self.name = name
        self.hidden = hidden
        # (row0, col0) -> (text, style_id)
        self.cells: Dict[Tuple[int, int], Tuple[str, int]] = {}
        self.row_heights: Dict[int, float] = {}
        self.col_widths: Dict[int, float] = {}
        # list of (sqref, formula1)
        self.validations: List[Tuple[str, str]] = []

    def put(self, row_1: int, col_1: int, value: Any, style: int = _S_DEFAULT) -> None:
        self.cells[(row_1 - 1, col_1 - 1)] = (_s(value), style)

    def set_row_height(self, row_1: int, height: float) -> None:
        self.row_heights[row_1 - 1] = height

    def set_col_width(self, col_1: int, width: float) -> None:
        self.col_widths[col_1 - 1] = width

    def add_dv(self, sqref: str, formula: str) -> None:
        if sqref and formula:
            self.validations.append((sqref, formula))


def _collect_shared_strings(sheets: List[_SheetBuilder]) -> Tuple[List[str], Dict[str, int]]:
    order: List[str] = []
    index: Dict[str, int] = {}
    for sh in sheets:
        for text, _style in sh.cells.values():
            if text not in index:
                index[text] = len(order)
                order.append(text)
    return order, index


def _sheet_xml(sh: _SheetBuilder, sst: Dict[str, int]) -> str:
    if not sh.cells:
        max_r, max_c = 0, 0
    else:
        max_r = max(r for r, _c in sh.cells) + 1
        max_c = max(c for _r, c in sh.cells) + 1
    dim = f"A1:{_col_letter(max(max_c, 1))}{max(max_r, 1)}"

    cols_xml = ""
    if sh.col_widths:
        parts = []
        for c0, w in sorted(sh.col_widths.items()):
            parts.append(
                f'<col min="{c0 + 1}" max="{c0 + 1}" width="{w}" customWidth="1"/>'
            )
        cols_xml = "<cols>" + "".join(parts) + "</cols>"

    # group by row
    by_row: Dict[int, List[Tuple[int, str, int]]] = {}
    for (r0, c0), (text, style) in sh.cells.items():
        by_row.setdefault(r0, []).append((c0, text, style))

    rows_parts: List[str] = []
    for r0 in sorted(by_row):
        cells = sorted(by_row[r0], key=lambda x: x[0])
        ht = sh.row_heights.get(r0)
        ht_attr = f' ht="{ht}" customHeight="1"' if ht is not None else ""
        c_xml: List[str] = []
        for c0, text, style in cells:
            ref = f"{_col_letter(c0 + 1)}{r0 + 1}"
            si = sst[text]
            s_attr = f' s="{style}"' if style else ""
            c_xml.append(f'<c r="{ref}"{s_attr} t="s"><v>{si}</v></c>')
        rows_parts.append(
            f'<row r="{r0 + 1}"{ht_attr}>' + "".join(c_xml) + "</row>"
        )

    dv_xml = ""
    if sh.validations:
        # по одной записи на sqref (без multi_range — надёжнее для Excel Mac)
        items = []
        for sqref, formula in sh.validations:
            items.append(
                '<dataValidation type="list" allowBlank="1" '
                'showInputMessage="0" showErrorMessage="0" '
                f'sqref="{escape(sqref)}">'
                f"<formula1>{escape(formula)}</formula1>"
                "</dataValidation>"
            )
        dv_xml = (
            f'<dataValidations count="{len(items)}">'
            + "".join(items)
            + "</dataValidations>"
        )

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<dimension ref="{dim}"/>'
        "<sheetViews><sheetView workbookViewId=\"0\"/></sheetViews>"
        '<sheetFormatPr defaultRowHeight="15"/>'
        f"{cols_xml}"
        f'<sheetData>{"".join(rows_parts)}</sheetData>'
        f"{dv_xml}"
        "</worksheet>"
    )


def _shared_strings_xml(strings: List[str]) -> str:
    parts = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        f'count="{len(strings)}" uniqueCount="{len(strings)}">'
    ]
    for s in strings:
        # preserve spaces
        parts.append(f'<si><t xml:space="preserve">{_xml(s)}</t></si>')
    parts.append("</sst>")
    return "".join(parts)


def _styles_xml() -> str:
    """Фиксированный набор заливок/шрифтов под форму."""
    # fills: 0 none, 1 gray125, then solids
    fill_colors = [
        "1F4E79",  # section
        "D6EAF8",  # key/label/header
        "F8F9F9",  # desc / header desc
        "FFF2CC",  # header val / text
        "D5F5E3",  # table
        "E8F8F5",  # hint
        "C6EFCE",  # dropdown
        "FCE4D6",  # list
        "F5B7B1",  # json
        "DDEBF7",  # date
        "E8DAEF",  # number
    ]
    fills = [
        '<fill><patternFill patternType="none"/></fill>',
        '<fill><patternFill patternType="gray125"/></fill>',
    ]
    for rgb in fill_colors:
        fills.append(
            f'<fill><patternFill patternType="solid">'
            f'<fgColor rgb="FF{rgb}"/></patternFill></fill>'
        )

    # fonts: 0 default, 1 white bold section, 2 key gray, 3 desc italic,
    # 4 bold, 5 hint green, 6 legend bold
    fonts = [
        "<fonts count=\"7\">"
        '<font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>'
        '<font><b/><sz val="12"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>'
        '<font><sz val="10"/><color rgb="FF566573"/><name val="Calibri"/><family val="2"/></font>'
        '<font><i/><sz val="9"/><color rgb="FF5D6D7E"/><name val="Calibri"/><family val="2"/></font>'
        '<font><b/><sz val="11"/><name val="Calibri"/><family val="2"/></font>'
        '<font><i/><sz val="8"/><color rgb="FF1E8449"/><name val="Calibri"/><family val="2"/></font>'
        '<font><b/><sz val="10"/><name val="Calibri"/><family val="2"/></font>'
        "</fonts>"
    ]

    # cellXfs mapping — indices must match _S_* constants
    # fill index: 0 none, 1 gray, 2 section, 3 kv, 4 desc, 5 headerval/text,
    # 6 table, 7 hint, 8 dropdown, 9 list, 10 json, 11 date, 12 number
    xfs = [
        # 0 default
        '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>',
        # 1 section
        '<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1">'
        '<alignment vertical="center"/></xf>',
        # 2 key
        '<xf numFmtId="0" fontId="2" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1"/>',
        # 3 label
        '<xf numFmtId="0" fontId="0" fillId="3" borderId="0" xfId="0" applyFill="1"/>',
        # 4 desc
        '<xf numFmtId="0" fontId="3" fillId="4" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1">'
        '<alignment wrapText="1" vertical="center"/></xf>',
        # 5 header
        '<xf numFmtId="0" fontId="4" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1"/>',
        # 6 header val
        '<xf numFmtId="0" fontId="4" fillId="5" borderId="0" xfId="0" applyFont="1" applyFill="1"/>',
        # 7 header desc
        '<xf numFmtId="0" fontId="4" fillId="4" borderId="0" xfId="0" applyFont="1" applyFill="1"/>',
        # 8 table
        '<xf numFmtId="0" fontId="4" fillId="6" borderId="0" xfId="0" applyFont="1" applyFill="1"/>',
        # 9 hint
        '<xf numFmtId="0" fontId="5" fillId="7" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1">'
        '<alignment wrapText="1"/></xf>',
        # 10 note
        '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1">'
        '<alignment wrapText="1" vertical="top"/></xf>',
        # 11 legend label
        '<xf numFmtId="0" fontId="6" fillId="0" borderId="0" xfId="0" applyFont="1"/>',
        # 12-16 value kinds
        '<xf numFmtId="0" fontId="0" fillId="8" borderId="0" xfId="0" applyFill="1" applyAlignment="1">'
        '<alignment wrapText="1" vertical="center"/></xf>',
        '<xf numFmtId="0" fontId="0" fillId="5" borderId="0" xfId="0" applyFill="1" applyAlignment="1">'
        '<alignment wrapText="1" vertical="center"/></xf>',
        '<xf numFmtId="0" fontId="0" fillId="9" borderId="0" xfId="0" applyFill="1" applyAlignment="1">'
        '<alignment wrapText="1" vertical="center"/></xf>',
        '<xf numFmtId="0" fontId="0" fillId="10" borderId="0" xfId="0" applyFill="1" applyAlignment="1">'
        '<alignment wrapText="1" vertical="center"/></xf>',
        '<xf numFmtId="0" fontId="0" fillId="11" borderId="0" xfId="0" applyFill="1" applyAlignment="1">'
        '<alignment wrapText="1" vertical="center"/></xf>',
        # 17-21 legend samples (bold + kind fill)
        '<xf numFmtId="0" fontId="4" fillId="8" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1">'
        '<alignment horizontal="center" vertical="center"/></xf>',
        '<xf numFmtId="0" fontId="4" fillId="5" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1">'
        '<alignment horizontal="center" vertical="center"/></xf>',
        '<xf numFmtId="0" fontId="4" fillId="9" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1">'
        '<alignment horizontal="center" vertical="center"/></xf>',
        '<xf numFmtId="0" fontId="4" fillId="10" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1">'
        '<alignment horizontal="center" vertical="center"/></xf>',
        '<xf numFmtId="0" fontId="4" fillId="11" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1">'
        '<alignment horizontal="center" vertical="center"/></xf>',
        # 22-23 number value + legend
        '<xf numFmtId="0" fontId="0" fillId="12" borderId="0" xfId="0" applyFill="1" applyAlignment="1">'
        '<alignment wrapText="1" vertical="center"/></xf>',
        '<xf numFmtId="0" fontId="4" fillId="12" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1">'
        '<alignment horizontal="center" vertical="center"/></xf>',
    ]

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        + fonts[0]
        + f'<fills count="{len(fills)}">{"".join(fills)}</fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        f'<cellXfs count="{len(xfs)}">{"".join(xfs)}</cellXfs>'
        '<cellStyles count="1">'
        '<cellStyle name="Normal" xfId="0" builtinId="0"/>'
        "</cellStyles>"
        "</styleSheet>"
    )


def _workbook_xml(sheets: List[_SheetBuilder]) -> str:
    sheet_tags = []
    for i, sh in enumerate(sheets, start=1):
        state = ' state="hidden"' if sh.hidden else ""
        sheet_tags.append(
            f'<sheet name="{_xml(sh.name)}" sheetId="{i}"{state} r:id="rId{i}"/>'
        )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        "<sheets>"
        + "".join(sheet_tags)
        + "</sheets></workbook>"
    )


def _workbook_rels(n_sheets: int) -> str:
    rels = []
    for i in range(1, n_sheets + 1):
        rels.append(
            '<Relationship Id="rId{0}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            'Target="worksheets/sheet{0}.xml"/>'.format(i)
        )
    rid = n_sheets + 1
    rels.append(
        f'<Relationship Id="rId{rid}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
    )
    rid += 1
    rels.append(
        f'<Relationship Id="rId{rid}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" '
        'Target="sharedStrings.xml"/>'
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        + "".join(rels)
        + "</Relationships>"
    )


def _content_types(n_sheets: int) -> str:
    overrides = [
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
        '<Override PartName="/xl/sharedStrings.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>',
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
        '<Override PartName="/docProps/core.xml" '
        'ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>',
        '<Override PartName="/docProps/app.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>',
    ]
    for i in range(1, n_sheets + 1):
        overrides.append(
            f'<Override PartName="/xl/worksheets/sheet{i}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        + "".join(overrides)
        + "</Types>"
    )


def _root_rels() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" '
        'Target="docProps/core.xml"/>'
        '<Relationship Id="rId3" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" '
        'Target="docProps/app.xml"/>'
        "</Relationships>"
    )


def _core_xml() -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:dcterms="http://purl.org/dc/terms/" '
        'xmlns:dcmitype="http://purl.org/dc/dcmitype/" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        "<dc:creator>SPOD_PROM</dc:creator>"
        f"<dcterms:created xsi:type=\"dcterms:W3CDTF\">{now}</dcterms:created>"
        f"<dcterms:modified xsi:type=\"dcterms:W3CDTF\">{now}</dcterms:modified>"
        "</cp:coreProperties>"
    )


def _app_xml(sheet_names: List[str]) -> str:
    titles = "".join(f"<vt:lpstr>{_xml(n)}</vt:lpstr>" for n in sheet_names)
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
        'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
        "<Application>SPOD_PROM</Application>"
        f"<HeadingPairs><vt:vector size=\"2\" baseType=\"variant\">"
        "<vt:variant><vt:lpstr>Worksheets</vt:lpstr></vt:variant>"
        f"<vt:variant><vt:i4>{len(sheet_names)}</vt:i4></vt:variant>"
        "</vt:vector></HeadingPairs>"
        f"<TitlesOfParts><vt:vector size=\"{len(sheet_names)}\" baseType=\"lpstr\">"
        f"{titles}</vt:vector></TitlesOfParts>"
        "</Properties>"
    )


def _save_xlsx(path: str, sheets: List[_SheetBuilder]) -> None:
    strings, sst = _collect_shared_strings(sheets)
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", _content_types(len(sheets)))
        z.writestr("_rels/.rels", _root_rels())
        z.writestr("docProps/core.xml", _core_xml())
        z.writestr("docProps/app.xml", _app_xml([s.name for s in sheets]))
        z.writestr("xl/workbook.xml", _workbook_xml(sheets))
        z.writestr("xl/_rels/workbook.xml.rels", _workbook_rels(len(sheets)))
        z.writestr("xl/styles.xml", _styles_xml())
        z.writestr("xl/sharedStrings.xml", _shared_strings_xml(strings))
        for i, sh in enumerate(sheets, start=1):
            z.writestr(f"xl/worksheets/sheet{i}.xml", _sheet_xml(sh, sst))


def _write_kv(
    sh: _SheetBuilder,
    row: int,
    key: str,
    label: str,
    value: Any,
    *,
    in_badge_slot: bool,
    schema_kind: Optional[str],
    kv_cells: Dict[str, List[str]],
    dropdown_map: Dict[str, List[str]],
) -> int:
    kind = input_kind_for_kv(
        key,
        schema_kind=schema_kind,
        has_dropdown=key in dropdown_map,
    )
    sh.put(row, 1, key, _S_KEY)
    sh.put(row, 2, label, _S_LABEL)
    sh.put(row, 3, value, _S_VALUE.get(kind, _S_VALUE["text"]))
    sh.put(
        row,
        4,
        description_for(key, in_badge_slot=in_badge_slot),
        _S_DESC,
    )
    sh.set_row_height(row, 28)
    kv_cells.setdefault(key, []).append(f"C{row}")
    return row + 1


def _write_section(sh: _SheetBuilder, row: int, title: str) -> int:
    for col in range(1, 5):
        sh.put(row, col, title if col == 1 else "", _S_SECTION)
    sh.set_row_height(row, 20)
    return row + 1


def _write_legend(sh: _SheetBuilder, row: int) -> int:
    sh.put(row, 1, "#META:LEGEND", _S_LEGEND_LABEL)
    sh.put(row, 2, "Цвет значения = тип ввода →", _S_LEGEND_LABEL)
    for col_idx, kind in enumerate(INPUT_KIND_ORDER):
        sh.put(
            row,
            3 + col_idx,
            INPUT_KIND_LABELS.get(kind, kind),
            _S_LEGEND.get(kind, _S_DEFAULT),
        )
    sh.set_row_height(row, 22)
    return row + 1


def _write_table(
    sh: _SheetBuilder,
    row: int,
    marker: str,
    columns: Sequence[str],
    rows: List[Dict[str, Any]],
    *,
    min_empty: int = 3,
) -> Tuple[int, str, Sequence[str], int, int]:
    table_key = marker.replace("#TABLE:", "").strip().upper()
    if table_key == "REWARD_LINK":
        table_key = "REWARD-LINK"
    col_kinds = [input_kind_for_table_col(table_key, c) for c in columns]

    sh.put(row, 1, marker, _S_TABLE)
    row += 1
    for col_idx, col_name in enumerate(columns, start=1):
        sh.put(row, col_idx, col_name, _S_TABLE)
    row += 1
    for col_idx, col_name in enumerate(columns, start=1):
        hint = table_hint_for(table_key, col_name)
        kind_label = INPUT_KIND_LABELS.get(col_kinds[col_idx - 1], "")
        hint_full = f"{hint} · {kind_label}" if kind_label else hint
        text = f"#HINT | {hint_full}" if col_idx == 1 else hint_full
        sh.put(row, col_idx, text, _S_HINT)
    row += 1
    data_start = row
    for data_row in rows:
        for col_idx, col_name in enumerate(columns, start=1):
            kind = col_kinds[col_idx - 1]
            sh.put(
                row,
                col_idx,
                data_row.get(col_name, ""),
                _S_VALUE.get(kind, _S_VALUE["text"]),
            )
        row += 1
    for _ in range(min_empty):
        for col_idx in range(1, len(columns) + 1):
            kind = col_kinds[col_idx - 1]
            sh.put(row, col_idx, "", _S_VALUE.get(kind, _S_VALUE["text"]))
        row += 1
    data_end = row - 1
    return row, table_key, columns, data_start, data_end


def _write_sheet(
    sh: _SheetBuilder,
    payload: Dict[str, Any],
    *,
    dropdown_map: Dict[str, List[str]],
    list_ranges: Dict[str, str],
    with_dropdowns: bool,
) -> None:
    sh.set_col_width(1, 32)   # Ключ
    sh.set_col_width(2, 48)   # Подпись
    sh.set_col_width(3, 36)   # Значение
    sh.set_col_width(4, 96)   # Описание / значения
    for c in range(5, 8):
        sh.set_col_width(c, 22)

    kv_cells: Dict[str, List[str]] = {}

    sh.put(1, 1, "#META:FORM_VERSION")
    sh.put(1, 2, "7")
    note = (
        "Заполняйте цветные ячейки значений (столбец C и таблицы). "
        "Цвет = тип ввода (легенда ниже). Столбец D — описание. "
        "Где зелёный — выпадающий список. Персик — несколько значений через ; . "
        "Розовый — JSON как в SPOD. Лист = номер конкурса."
    )
    sh.put(2, 1, "#META:NOTE")
    sh.put(2, 2, note, _S_NOTE)
    sh.set_row_height(2, 40)
    _write_legend(sh, 3)

    sh.put(4, 1, "Ключ", _S_HEADER)
    sh.put(4, 2, "Подпись", _S_HEADER)
    sh.put(4, 3, "Значение (заполнять)", _S_HEADER_VAL)
    sh.put(4, 4, "Описание / значения", _S_HEADER_DESC)

    row = 6
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
            sh,
            r,
            key,
            label_for(key, label, in_badge_slot=in_badge),
            value,
            in_badge_slot=in_badge,
            schema_kind=schema_kind,
            kv_cells=kv_cells,
            dropdown_map=dropdown_map,
        )

    row = _write_section(sh, row, "#SECTION:CONTEST")
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
        row = _write_section(sh, row, f"#SECTION:BADGE:{slot_idx}")
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
            sh,
            row,
            marker,
            cols,
            list(payload.get(payload_key) or []),
        )
        table_metas.append((table_key, cols_out, data_start, data_end))

    if not with_dropdowns:
        return

    for key, cells in kv_cells.items():
        values = dropdown_map.get(key)
        if not values:
            continue
        formula = _dv_formula(key, values, list_ranges)
        if not formula:
            continue
        # отдельные DV на каждую ячейку — без multi sqref
        for cell in cells:
            sh.add_dv(cell, formula)

    for table_key, columns, data_start, data_end in table_metas:
        if data_end < data_start:
            continue
        for col_name in columns:
            list_key = f"TBL:{table_key}:{col_name}"
            values = dropdown_map.get(list_key)
            if not values:
                # fallback на старый словарь без префикса
                values = (TABLE_DROPDOWNS.get(table_key) or {}).get(col_name)
            if not values:
                continue
            col_idx = list(columns).index(col_name) + 1
            letter = _col_letter(col_idx)
            range_a1 = f"{letter}{data_start}:{letter}{data_end}"
            formula = _dv_formula(list_key, values, list_ranges)
            if formula:
                sh.add_dv(range_a1, formula)


def _lists_sheet(dropdown_map: Dict[str, List[str]]) -> Optional[_SheetBuilder]:
    to_sheet = {
        k: _clean_list(v)
        for k, v in dropdown_map.items()
        if _needs_lists_sheet(_clean_list(v))
    }
    if not to_sheet:
        return None
    sh = _SheetBuilder(_LISTS_SHEET, hidden=True)
    for col, key in enumerate(sorted(to_sheet.keys()), start=1):
        values = to_sheet[key]
        sh.put(1, col, key[:80])
        for i, val in enumerate(values, start=2):
            sh.put(i, col, val)
    return sh


def write_form_xlsx(
    path: str,
    payloads: List[Dict[str, Any]],
    *,
    dropdowns: Optional[Dict[str, List[str]]] = None,
    with_dropdowns: bool = True,
) -> str:
    """
    Записать форму (листы 1..N) через stdlib OOXML.
    with_dropdowns=True — выпадающие списки из field_meta.
    """
    dd_map = merge_dropdowns(dropdowns) if with_dropdowns else {}
    # TABLE_DROPDOWNS уже учтены в merge_dropdowns; не перезаписывать catalog

    list_ranges = _build_lists_ranges(dd_map) if with_dropdowns else {}

    sheets: List[_SheetBuilder] = []
    use_payloads = payloads if payloads else [{}]
    for idx, payload in enumerate(use_payloads, start=1):
        sh = _SheetBuilder(str(idx))
        _write_sheet(
            sh,
            payload,
            dropdown_map=dd_map if with_dropdowns else {},
            list_ranges=list_ranges,
            with_dropdowns=with_dropdowns,
        )
        sheets.append(sh)

    if with_dropdowns:
        lists = _lists_sheet(dd_map)
        if lists is not None:
            sheets.append(lists)

    _save_xlsx(path, sheets)
    logging.info(
        "[contest_badge_form] Форма (stdlib OOXML%s): %s",
        ", dropdowns" if with_dropdowns else "",
        path,
    )
    return path
