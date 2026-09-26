# -*- coding: utf-8 -*-
"""
Отпечаток выходной книги Excel для эталонного сравнения результата пайплайна (TEST-01).

Отпечаток — JSON без самих данных: для каждого листа порядок листов, число строк/колонок,
заголовок, хеш набора строк без учёта их порядка (row_set), хеш «разметки» листа (ширины колонок, закрепление, автофильтр, условное
форматирование — всё из XML листа, кроме sheetData) и по каждой колонке два хеша:
значений ячеек и их оформления (формат числа, шрифт, заливка, выравнивание, границы).
Персональные данные в отпечаток не попадают, поэтому его можно хранить рядом с кодом.

Нестабильные поля (время прогона, mtime входных файлов) исключаются: DEFAULT_VOLATILE.

Использование (только stdlib + openpyxl из Anaconda 3.12):
  python src/Tools/pipeline_fingerprint.py snapshot "OUT/.../SPOD_PROM main_....xlsx" -o fp.json
  python src/Tools/pipeline_fingerprint.py compare etalon.json fp.json
  python src/Tools/pipeline_fingerprint.py diff-cells old.xlsx new.xlsx --sheet GROUP --max 20
Код возврата compare: 0 — совпадает, 1 — есть расхождения.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import posixpath
import re
import sys
import zipfile
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree as ET

import openpyxl

SCHEMA_VERSION = 3

# Колонки, значения которых меняются от прогона к прогону при тех же входных данных.
# Для них хеш значений не считается (оформление и наличие колонки — сравниваются).
DEFAULT_VOLATILE: Dict[str, List[str]] = {
    "STAT_FILE": ["FILE_DATE", "DATA_UPDATE_DATE"],
}

_NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_NS_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_NS_PKG_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
_COLS_RE = re.compile(rb"<cols>.*?</cols>", re.S)
# <dimension> — производное от данных (строки/колонки сравниваются отдельно); потоковая запись его не пишет
_DIMENSION_RE = re.compile(rb"<(?:\w+:)?dimension\b[^>]*/>")
_SHEET_DATA_RE = re.compile(rb"<(?:\w+:)?sheetData\b.*?</(?:\w+:)?sheetData>|<(?:\w+:)?sheetData\b[^>]*/>", re.S)


def _value_token(value: Any) -> str:
    """Каноническое представление значения ячейки (тип + значение)."""
    if value is None:
        return "N"
    if isinstance(value, bool):
        return f"b:{int(value)}"
    if isinstance(value, int):
        return f"i:{value}"
    if isinstance(value, float):
        return f"f:{value!r}"
    if isinstance(value, datetime):
        return f"dt:{value.isoformat()}"
    if isinstance(value, date):
        return f"d:{value.isoformat()}"
    if isinstance(value, time):
        return f"t:{value.isoformat()}"
    if isinstance(value, timedelta):
        return f"td:{value.total_seconds()!r}"
    return f"s:{value}"


def _color_token(color: Any) -> str:
    if color is None:
        return ""
    for attr in ("rgb", "theme", "indexed"):
        v = getattr(color, attr, None)
        if isinstance(v, (str, int)) and v != "":
            tint = getattr(color, "tint", 0) or 0
            return f"{attr}={v}" + (f"/{tint}" if tint else "")
    return ""


def _style_token(cell: Any) -> str:
    """Описание оформления ячейки (без индексов стилей — они зависят от порядка создания)."""
    font = cell.font
    fill = cell.fill
    al = cell.alignment
    br = cell.border
    parts = [f"nf={cell.number_format}"]
    if font is not None:
        parts.append(
            f"font={font.name}|{font.sz}|{int(bool(font.b))}|{int(bool(font.i))}|{font.u or ''}|{_color_token(font.color)}"
        )
    if fill is not None:
        parts.append(
            f"fill={getattr(fill, 'patternType', None) or getattr(fill, 'fill_type', None) or ''}"
            f"|{_color_token(getattr(fill, 'fgColor', None))}"
        )
    if al is not None:
        parts.append(
            f"al={al.horizontal or ''}|{al.vertical or ''}|{int(bool(al.wrap_text))}|{al.indent or 0}|{al.textRotation or 0}"
        )
    if br is not None:
        sides = []
        for side_name in ("left", "right", "top", "bottom"):
            side = getattr(br, side_name, None)
            sides.append(f"{getattr(side, 'style', None) or ''}:{_color_token(getattr(side, 'color', None))}")
        parts.append("br=" + ",".join(sides))
    return ";".join(parts)


def _sheet_xml_paths(xlsx_path: str) -> Dict[str, str]:
    """Имя листа → путь XML внутри архива xlsx."""
    with zipfile.ZipFile(xlsx_path) as zf:
        wb_xml = ET.fromstring(zf.read("xl/workbook.xml"))
        rels_xml = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    targets = {}
    for rel in rels_xml.findall(f"{{{_NS_PKG_REL}}}Relationship"):
        target = rel.get("Target", "")
        if target.startswith("/"):
            path = target.lstrip("/")
        else:
            path = posixpath.normpath(posixpath.join("xl", target))
        targets[rel.get("Id")] = path
    result = {}
    sheets_el = wb_xml.find(f"{{{_NS_MAIN}}}sheets")
    for sh in sheets_el if sheets_el is not None else []:
        rid = sh.get(f"{{{_NS_REL}}}id")
        if rid in targets:
            result[sh.get("name")] = targets[rid]
    return result


def _layout_info(xlsx_path: str) -> Dict[str, Tuple[str, Dict[int, str]]]:
    """
    Разметка листов: (хеш XML без sheetData и cols, {номер колонки: ширина}).

    Ширины отделены от остальной разметки, чтобы привязать их к имени колонки:
    тогда перестановка колонок не выглядит как изменение ширин.
    """
    paths = _sheet_xml_paths(xlsx_path)
    out: Dict[str, Tuple[str, Dict[int, str]]] = {}
    with zipfile.ZipFile(xlsx_path) as zf:
        for name, path in paths.items():
            raw = _DIMENSION_RE.sub(b"", _SHEET_DATA_RE.sub(b"", zf.read(path)))
            widths: Dict[int, str] = {}
            cols_m = _COLS_RE.search(raw)
            if cols_m:
                cols_el = ET.fromstring(cols_m.group(0).replace(b"<cols>", f'<cols xmlns="{_NS_MAIN}">'.encode(), 1))
                for col in cols_el:
                    token = "|".join(
                        str(col.get(a, "")) for a in ("width", "customWidth", "hidden", "style", "bestFit")
                    )
                    for idx in range(int(col.get("min", "0")), int(col.get("max", "0")) + 1):
                        widths[idx] = token
                raw = raw[: cols_m.start()] + raw[cols_m.end():]
            out[name] = (hashlib.sha256(raw).hexdigest(), widths)
    return out


def _unique_column_keys(header: List[Any]) -> List[str]:
    """Ключи колонок в отпечатке: имя из заголовка; повторы — с суффиксом #2, #3…"""
    seen: Dict[str, int] = {}
    keys = []
    for idx, h in enumerate(header, start=1):
        name = str(h) if h is not None else f"<col{idx}>"
        seen[name] = seen.get(name, 0) + 1
        keys.append(name if seen[name] == 1 else f"{name}#{seen[name]}")
    return keys


def fingerprint_workbook(
    xlsx_path: str, volatile: Optional[Dict[str, List[str]]] = None
) -> Dict[str, Any]:
    """Снять отпечаток книги. volatile: {лист: [колонки]} без сравнения значений."""
    volatile = DEFAULT_VOLATILE if volatile is None else volatile
    layouts = _layout_info(xlsx_path)
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=False)
    style_cache: Dict[int, str] = {}
    sheets: Dict[str, Any] = {}
    try:
        for ws in wb.worksheets:
            max_col = ws.max_column
            if not max_col:
                # Нет <dimension> (потоковая запись) — размер листа считаем по ячейкам
                ws.calculate_dimension(force=True)
                max_col = ws.max_column or 0
            header: List[Any] = []
            value_h: List[Any] = []
            style_h: List[Any] = []
            row_digests: List[bytes] = []
            row_cols: List[int] = []
            nrows = 0
            skip_row = set(volatile.get(ws.title, []))
            for row in ws.iter_rows(min_col=1, max_col=max_col):
                nrows += 1
                if nrows == 1:
                    header = [c.value for c in row]
                    value_h = [hashlib.sha256() for _ in row]
                    style_h = [hashlib.sha256() for _ in row]
                    # Порядок колонок для хеша строки — по имени (не зависит от перестановки колонок)
                    keys_1 = _unique_column_keys(header)
                    row_cols = [j for _k, j in sorted((k, j) for j, k in enumerate(keys_1)) if keys_1[j] not in skip_row]
                else:
                    row_digests.append(
                        hashlib.sha256(
                            "\x1f".join(_value_token(row[j].value) for j in row_cols).encode("utf-8")
                        ).digest()
                    )
                for j, cell in enumerate(row):
                    # EmptyCell (нет ячейки в XML) — отдельный ключ кеша: её _style_id тоже 0
                    sid = (getattr(cell, "_style_id", 0) or 0) if cell.font is not None else -1
                    tok = style_cache.get(sid)
                    if tok is None:
                        tok = _style_token(cell) if sid != -1 else ""
                        style_cache[sid] = tok
                    value_h[j].update(_value_token(cell.value).encode("utf-8") + b"\x1f")
                    style_h[j].update(tok.encode("utf-8") + b"\x1f")
            keys = _unique_column_keys(header)
            skip = set(volatile.get(ws.title, []))
            layout_hash, widths = layouts.get(ws.title, ("", {}))
            columns = {}
            for idx, (key, vh, sh) in enumerate(zip(keys, value_h, style_h), start=1):
                columns[key] = {
                    "values": "volatile" if key in skip else vh.hexdigest(),
                    "styles": sh.hexdigest(),
                    "width": widths.get(idx, ""),
                }
            row_set = hashlib.sha256(b"".join(sorted(row_digests))).hexdigest()
            sheets[ws.title] = {
                "rows": nrows,
                "row_set": row_set,
                "cols": max_col,
                "header": [None if h is None else str(h) for h in header],
                "layout": layout_hash,
                "columns": columns,
            }
    finally:
        wb.close()
    return {
        "schema": SCHEMA_VERSION,
        "sheet_order": list(sheets.keys()),
        "volatile": volatile,
        "sheets": sheets,
    }


def compare_fingerprints(
    expected: Dict[str, Any], actual: Dict[str, Any], strict_order: bool = False
) -> Tuple[List[str], List[str]]:
    """
    Сравнить отпечатки. Возвращает (расхождения, примечания); пустые расхождения — совпадает.

    Порядок колонок по умолчанию не считается расхождением (попадает в примечания):
    проверки консистентности добавляют колонки из потоков, и их порядок сейчас зависит
    от того, какой поток завершился раньше. strict_order=True — считать расхождением.
    """
    diffs: List[str] = []
    notes: List[str] = []
    if expected.get("schema") != actual.get("schema"):
        diffs.append(f"версия схемы отпечатка: {expected.get('schema')} → {actual.get('schema')}")
    e_order, a_order = expected.get("sheet_order", []), actual.get("sheet_order", [])
    missing = [s for s in e_order if s not in a_order]
    extra = [s for s in a_order if s not in e_order]
    if missing:
        diffs.append(f"пропали листы: {missing}")
    if extra:
        diffs.append(f"новые листы: {extra}")
    common_e = [s for s in e_order if s in a_order]
    common_a = [s for s in a_order if s in e_order]
    if common_e != common_a:
        diffs.append(f"изменился порядок листов: {common_e} → {common_a}")
    for name in common_e:
        es, as_ = expected["sheets"][name], actual["sheets"][name]
        for key in ("rows", "cols"):
            if es[key] != as_[key]:
                diffs.append(f"[{name}] {key}: {es[key]} → {as_[key]}")
        gone = [c for c in es["columns"] if c not in as_["columns"]]
        new = [c for c in as_["columns"] if c not in es["columns"]]
        if gone or new:
            diffs.append(f"[{name}] колонки: пропали {gone}; новые {new}")
        elif es["header"] != as_["header"]:
            (diffs if strict_order else notes).append(f"[{name}] изменился только порядок колонок")
        if es["layout"] != as_["layout"]:
            diffs.append(f"[{name}] разметка листа (закрепление/автофильтр/размер/поля)")
        changed: Dict[str, List[str]] = {"values": [], "styles": [], "width": []}
        for col, ec in es["columns"].items():
            ac = as_["columns"].get(col)
            if ac is None:
                continue
            for aspect in changed:
                if ec.get(aspect) != ac.get(aspect):
                    changed[aspect].append(col)
        labels = {"values": "значения", "styles": "оформление", "width": "ширина"}
        if changed["values"] and es.get("row_set") and es.get("row_set") == as_.get("row_set"):
            # Набор строк тот же (без учёта порядка) — сообщаем кратко
            diffs.append(f"[{name}] изменился только порядок строк (набор строк тот же)")
            changed["values"] = []
        for aspect, cols in changed.items():
            if cols:
                diffs.append(f"[{name}] {labels[aspect]} в колонках ({len(cols)}): {cols}")
    return diffs, notes


def diff_cells(
    old_path: str, new_path: str, sheet: str, max_items: int = 20, column: Optional[str] = None
) -> List[str]:
    """
    Первые различающиеся ячейки листа двух книг (для разбора расхождения отпечатков).
    Колонки сопоставляются по имени из заголовка, поэтому перестановка колонок не мешает.
    """
    out: List[str] = []
    wbs = [openpyxl.load_workbook(p, read_only=True) for p in (old_path, new_path)]
    try:
        it_old, it_new = (iter(wb[sheet].iter_rows(values_only=True)) for wb in wbs)
        h_old = list(next(it_old, ()) or ())
        h_new = list(next(it_new, ()) or ())
        k_old, k_new = _unique_column_keys(h_old), _unique_column_keys(h_new)
        pos_new = {k: i for i, k in enumerate(k_new)}
        pairs = [(k, i, pos_new[k]) for i, k in enumerate(k_old) if k in pos_new]
        if column is not None:
            pairs = [p for p in pairs if p[0] == column]
        for rn, (ro, rw) in enumerate(_zip_longest(it_old, it_new), start=2):
            if ro is None or rw is None:
                out.append(f"строка {rn}: есть только в {'новой' if ro is None else 'старой'} книге")
            else:
                for key, io, iw in pairs:
                    vo = ro[io] if io < len(ro) else None
                    vn = rw[iw] if iw < len(rw) else None
                    if _value_token(vo) != _value_token(vn):
                        out.append(f"строка {rn}, колонка {key}: {vo!r} → {vn!r}")
                        if len(out) >= max_items:
                            return out
            if len(out) >= max_items:
                return out
    finally:
        for wb in wbs:
            wb.close()
    return out


def _zip_longest(*iters: Iterable[Any]) -> Iterable[Tuple[Any, ...]]:
    from itertools import zip_longest

    return zip_longest(*iters, fillvalue=None)


def _main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Отпечаток выходной книги SPOD для эталонного сравнения")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_snap = sub.add_parser("snapshot", help="снять отпечаток xlsx в JSON")
    p_snap.add_argument("xlsx")
    p_snap.add_argument("-o", "--output", required=True)
    p_cmp = sub.add_parser("compare", help="сравнить два отпечатка (JSON)")
    p_cmp.add_argument("expected")
    p_cmp.add_argument("actual")
    p_cmp.add_argument("--strict-order", action="store_true", help="порядок колонок — тоже расхождение")
    p_diff = sub.add_parser("diff-cells", help="первые различающиеся ячейки листа двух xlsx")
    p_diff.add_argument("old_xlsx")
    p_diff.add_argument("new_xlsx")
    p_diff.add_argument("--sheet", required=True)
    p_diff.add_argument("--column")
    p_diff.add_argument("--max", type=int, default=20)
    args = parser.parse_args(argv)

    if args.cmd == "snapshot":
        fp = fingerprint_workbook(args.xlsx)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(fp, f, ensure_ascii=False, indent=1)
        print(f"Отпечаток: {args.output} (листов: {len(fp['sheets'])})")
        return 0
    if args.cmd == "compare":
        with open(args.expected, encoding="utf-8") as f:
            expected = json.load(f)
        with open(args.actual, encoding="utf-8") as f:
            actual = json.load(f)
        diffs, notes = compare_fingerprints(expected, actual, strict_order=args.strict_order)
        for n in notes:
            print("  примечание: " + n)
        if not diffs:
            print("Совпадает с эталоном.")
            return 0
        print(f"Расхождений: {len(diffs)}")
        for d in diffs:
            print("  - " + d)
        return 1
    for line in diff_cells(args.old_xlsx, args.new_xlsx, args.sheet, args.max, args.column):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
