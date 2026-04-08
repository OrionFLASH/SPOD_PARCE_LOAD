# -*- coding: utf-8 -*-
"""
Проверка ячеек со «SPOD-JSON»: тройные кавычки вокруг ключей и строковых значений,
числовые поля из списка без кавычек у значения, валидность JSON после нормализации.

Используется типом правила consistency_checks: json_spod_format (см. consistency_checks.py).
"""

from __future__ import annotations

import json
import re
from typing import Any, List, Optional, Set, Tuple

import pandas as pd

from src.json_utils import safe_json_loads

# Совпадает с consistency_checks._MAX_SAMPLE — локальная копия, чтобы не импортировать циклом.
_MAX_SAMPLE = 20


def _get_sheet_item(sheets_data: dict, sheet_name: str) -> Optional[Tuple[pd.DataFrame, Any]]:
    """Возвращает (df, conf) для листа или None (дубликат логики consistency_checks)."""
    if sheet_name not in sheets_data or sheets_data[sheet_name] is None:
        return None
    item = sheets_data[sheet_name]
    if not isinstance(item, (list, tuple)) or len(item) < 2:
        return None
    df, conf = item[0], item[1]
    if not isinstance(df, pd.DataFrame):
        return None
    return (df, conf)


def _excel_row(idx: int) -> int:
    """Номер строки в Excel: строка 1 — заголовок, первая строка данных = 2."""
    return int(idx) + 2


def _strip_outer_matching_quotes(s: str) -> str:
    """Снимает одну внешнюю пару одинарных или двойных кавычек, если они с обеих сторон."""
    t = s.strip()
    if len(t) >= 2 and t[0] == t[-1] and t[0] in ("'", '"'):
        return t[1:-1].strip()
    return t


def _normalize_triple_to_double(s: str) -> str:
    """Замена тройных кавычек на обычные (как при разборе в других модулях)."""
    return s.replace('"""', '"')


def _raw_missing_value_after_key(raw: str) -> Optional[str]:
    """
    Ошибка: после ключа в тройных кавычках и двоеточия сразу идёт запятая или закрывающая скобка (нет значения).
    """
    # Ключ в тройных кавычках, затем : и сразу , или } или ]
    if re.search(r'"""[^"]+"""\s*:\s*([,}\]])', raw):
        return "пустое значение у ключа после двоеточия (сразу запятая или закрывающая скобка)"
    return None


def _raw_has_invalid_numeric_quoting(raw: str, numeric_keys: Set[str]) -> Optional[str]:
    """
    Для перечисленных ключей значение после двоеточия не должно начинаться с тройной кавычки
    (числовое значение задаётся без кавычек).
    """
    if not numeric_keys:
        return None
    keys_alt = "|".join(re.escape(k) for k in sorted(numeric_keys, key=len, reverse=True))
    pat = re.compile(r'"""(' + keys_alt + r')"""\s*:\s*')

    def _match_span(m: re.Match[str]) -> Optional[str]:
        rest = raw[m.end() :].lstrip()
        if not rest:
            return "пустое значение у числового поля"
        if rest.startswith('"""'):
            return f"числовое поле «{m.group(1)}» не должно иметь значение в тройных кавычках"
        if rest[0] in "-0123456789tfn":
            return None
        return f"ожидалось число (без кавычек) для «{m.group(1)}»"

    pos = 0
    while True:
        m = pat.search(raw, pos)
        if not m:
            break
        err = _match_span(m)
        if err:
            return err
        pos = m.end()
    return None


def _check_numeric_types(obj: Any, numeric_keys: Set[str], path: str) -> Optional[str]:
    """
    После json.loads: у ключей из numeric_keys значение должно быть int/float (не str, не bool).
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else str(k)
            if k in numeric_keys:
                if isinstance(v, bool):
                    return f"{p}: для числового ключа недопустимо логическое значение"
                if isinstance(v, str):
                    return f"{p}: ожидалось число, получена строка"
                if not isinstance(v, (int, float)):
                    return f"{p}: ожидалось число"
            err = _check_numeric_types(v, numeric_keys, p)
            if err:
                return err
    elif isinstance(obj, list):
        for i, it in enumerate(obj):
            err = _check_numeric_types(it, numeric_keys, f"{path}[{i}]")
            if err:
                return err
    return None


def validate_spod_json_cell(
    raw: Any,
    *,
    json_required: bool,
    numeric_value_keys: Optional[List[str]] = None,
) -> Tuple[bool, str]:
    """
    Проверка одной ячейки.

    Возвращает (успех, сообщение об ошибке или «OK»).
    """
    numeric_keys: Set[str] = set(str(x).strip() for x in (numeric_value_keys or []) if str(x).strip())

    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        if json_required:
            return False, "пустая ячейка при обязательном JSON"
        return True, "OK"

    s0 = str(raw).strip()
    if not s0 or s0 in ("-", "None", "null"):
        if json_required:
            return False, "пустое значение при обязательном JSON"
        return True, "OK"

    s1 = _strip_outer_matching_quotes(s0)

    miss = _raw_missing_value_after_key(s1)
    if miss:
        return False, miss

    err_raw = _raw_has_invalid_numeric_quoting(s1, numeric_keys)
    if err_raw:
        return False, err_raw

    # Ключ в одной паре кавычек вместо тройных (как в примерах некорректного SPOD-JSON).
    _bad_json_key = re.compile(r'(?:^\s*\{|[{,])\s*"(?!"")([A-Za-z_][A-Za-z0-9_]*)"\s*:')
    m_key = _bad_json_key.search(s1)
    if m_key:
        return False, f"ключ «{m_key.group(1)}» оформите в тройных кавычках (формат SPOD), не в одной паре"

    s2 = _normalize_triple_to_double(s1)
    s2 = _strip_outer_matching_quotes(s2)

    try:
        parsed = json.loads(s2)
    except json.JSONDecodeError as e:
        ok, alt = _try_safe_load(s2)
        if ok:
            parsed = alt
        else:
            return False, f"невалидный JSON после нормализации кавычек: {e}"

    err_num = _check_numeric_types(parsed, numeric_keys, "")
    if err_num:
        return False, err_num

    return True, "OK"


def _try_safe_load(s: str) -> Tuple[bool, Any]:
    try:
        v = safe_json_loads(s)
        if isinstance(v, (dict, list)):
            return True, v
    except Exception:
        pass
    return False, None


def run_json_spod_format_check(
    sheets_data: dict,
    rule: dict,
) -> dict:
    """
    Записывает колонку результата на лист и возвращает запись для свода CONSISTENCY.
    """
    sheet = rule.get("sheet")
    col_json = rule.get("json_column")
    json_required = bool(rule.get("json_required", True))
    numeric_value_keys = rule.get("numeric_value_keys") or []
    output = rule.get("output") or {}
    col_out = output.get("column_on_sheet") or "ПРОВЕРКА: JSON (формат SPOD)"
    check_id = rule.get("id", "")

    item = _get_sheet_item(sheets_data, sheet)
    if item is None:
        return {
            "check_id": check_id,
            "sheet": sheet,
            "name": rule.get("name", ""),
            "column_on_sheet": col_out,
            "type": "json_spod_format",
            "total_rows": 0,
            "violations": 0,
            "sample": [],
            "include_in_summary": output.get("include_in_summary", True),
        }

    df, conf = item
    if col_json not in df.columns:
        return {
            "check_id": check_id,
            "sheet": sheet,
            "name": rule.get("name", ""),
            "column_on_sheet": col_out,
            "type": "json_spod_format",
            "total_rows": len(df),
            "violations": len(df),
            "sample": [f"колонка «{col_json}» не найдена"],
            "include_in_summary": output.get("include_in_summary", True),
        }

    statuses: List[str] = []
    sample: List[str] = []
    violations = 0
    for idx, val in df[col_json].items():
        ok, msg = validate_spod_json_cell(
            val,
            json_required=json_required,
            numeric_value_keys=numeric_value_keys,
        )
        if ok:
            statuses.append("OK")
        else:
            violations += 1
            statuses.append(msg[:200])
            if len(sample) < _MAX_SAMPLE:
                sample.append(f"[{_excel_row(idx)}] {msg[:120]}")

    df = df.copy()
    df[col_out] = statuses
    sheets_data[sheet] = (df, conf)

    return {
        "check_id": check_id,
        "sheet": sheet,
        "name": rule.get("name", ""),
        "column_on_sheet": col_out,
        "type": "json_spod_format",
        "total_rows": len(df),
        "violations": violations,
        "sample": sample,
        "include_in_summary": output.get("include_in_summary", True),
    }
