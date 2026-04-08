# -*- coding: utf-8 -*-
"""
Проверка ячеек со «SPOD-JSON»: BOM и Unicode-пробелы вне блоков \"\"\"…\"\"\"; симметрия внешних кавычек;
рекурсивный разбор (ключи и строки в тройных кавычках, numeric_value_keys без кавычек);
типовые ошибки: \"\"key\"\" вместо \"\"\"key\"\"\", значение в одной паре кавычек как в JSON,
лишние {} вокруг одной строки в массиве. Нормализация кавычек и json.loads.
Сообщения в колонку на листе — короткие (путь + суть), см. Docs/CONSISTENCY_CHECKS_FORMAT.md п. 2.8.

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

# Лимит длины текста ошибки в колонке на листе (в Excel ~32767 символов на ячейку).
_MAX_CELL_ERROR_LEN = 12000

# Имя ключа в объекте SPOD: латиница, цифры, подчёркивание (как в проектных данных).
_KEY_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_NUM_RE = re.compile(r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?")


class SpodParseError(Exception):
    """Ошибка разбора сырой строки SPOD до нормализации."""

    def __init__(self, message: str, pos: int = 0) -> None:
        super().__init__(message)
        self.message = message
        self.pos = pos


def _snippet_around(s: str, pos: int, before: int = 28, after: int = 55) -> str:
    """Устар.: раньше длинный фрагмент; оставлено для редких случаев."""
    if pos < 0 or not s:
        return ""
    start = max(0, pos - before)
    end = min(len(s), pos + after)
    frag = s[start:end].replace("\r", " ").replace("\n", "↵")
    if start > 0:
        frag = "…" + frag
    if end < len(s):
        frag = frag + "…"
    return frag


def _short_json_string_literal(s: str, i: int, max_len: int = 36) -> str:
    """Если s[i]=='\"', возвращает короткое представление литерала \"…\" для текста ошибки."""
    if i >= len(s) or s[i] != '"':
        return ""
    j = i + 1
    while j < len(s) and s[j] != '"':
        j += 1
    body = s[i + 1 : j] if j < len(s) else s[i + 1 :]
    if len(body) > max_len:
        body = body[: max_len - 1] + "…"
    return '"' + body + ('"' if j < len(s) else "…")


def _object_body_is_only_triple_quoted_string(s: str, after_open_brace: int) -> Optional[Tuple[str, int]]:
    """
    Сразу после «{» только \"\"\"текст\"\"\" и «}» (нет ключа:значение).
    Возвращает (текст_внутри, индекс_после_закрывающей_»}»).
    """
    j = _skip_ws(s, after_open_brace)
    if not s.startswith('"""', j):
        return None
    k = s.find('"""', j + 3)
    if k < 0:
        return None
    m = _skip_ws(s, k + 3)
    if m < len(s) and s[m] == "}":
        return (s[j + 3 : k], m + 1)
    return None


def _format_spod_location(object_path: str) -> str:
    """Человекочитаемый префикс пути к объекту в SPOD."""
    return f"объект «{object_path}»" if object_path else "корень ячейки"


def _format_value_location(value_path: str) -> str:
    """Путь к значению (ключ или элемент массива)."""
    return f"«{value_path}»" if value_path else "«корень»"


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


def _check_outer_double_quote_symmetry(s: str) -> Optional[str]:
    """
    Если первая значимая кавычка — двойная в начале, в конце тоже должна быть двойная (и наоборот).
    """
    if not s:
        return None
    if s[0] == '"' and s[-1] != '"':
        return "строка начинается с двойной кавычки, но не заканчивается ею"
    if s[-1] == '"' and s[0] != '"':
        return "строка заканчивается двойной кавычкой, но не начинается с неё"
    return None


def _skip_ws(s: str, i: int) -> int:
    """
    Пропуск пробелов между токенами SPOD. Важно: не только ASCII-пробел/таб/CR/LF —
    в выгрузках Excel/CSV часто встречается неразрывный пробел U+00A0 и другие символы,
    для которых str.isspace() истинно; иначе после «{» или «,» парсер ошибочно сообщает,
    что ключ «не начинается с тройных кавычек».
    """
    n = len(s)
    while i < n and s[i].isspace():
        i += 1
    return i


def _strip_leading_bom(s: str) -> str:
    """Снимает BOM U+FEFF в начале строки (часто в CSV/Excel после чтения)."""
    if s.startswith("\ufeff"):
        return s[1:]
    return s


def _drop_whitespace_outside_spod_triple_strings(s: str) -> str:
    """
    Удаляет все символы, для которых str.isspace() истинно, вне блоков \"\"\"…\"\"\".

    Нужно для выгрузок Excel/CSV: неразрывный пробел U+00A0 и др. между «{» и ключом
    после замены тройных кавычек дают невалидный JSON; при этом пробелы внутри строковых
    значений (между открывающими и закрывающими \"\"\") сохраняются.
    """
    out: List[str] = []
    i = 0
    n = len(s)
    while i < n:
        if s.startswith('"""', i):
            out.append('"""')
            i += 3
            close = s.find('"""', i)
            if close < 0:
                out.append(s[i:])
                return "".join(out)
            out.append(s[i:close])
            out.append('"""')
            i = close + 3
            continue
        if s[i].isspace():
            while i < n and s[i].isspace():
                i += 1
            continue
        out.append(s[i])
        i += 1
    return "".join(out)


def _property_key_from_value_path(value_path: str) -> str:
    """Имя поля объекта из пути вида a.b[0].c → c."""
    if not value_path:
        return ""
    return value_path.rsplit(".", 1)[-1]


def _expect_literal(s: str, i: int, lit: str, where: str) -> int:
    if not s.startswith(lit, i):
        raise SpodParseError(f"{where}: ожидался символ «{lit}»", i)
    return i + len(lit)


def _read_spod_key(s: str, i: int, object_path: str) -> Tuple[str, int]:
    """После вызова i указывает на начало ключа: должно быть \"\"\"имя\"\"\"."""
    loc = _format_spod_location(object_path)
    i = _skip_ws(s, i)
    # Двойные кавычки ""key"" вместо тройных """key"""
    if s.startswith('""', i) and not s.startswith('"""', i):
        m_wrong = _KEY_NAME_RE.match(s, i + 2)
        if m_wrong:
            kn = m_wrong.group(0)
            raise SpodParseError(
                f"{loc}: ключ «{kn}» — оберните имя в тройные кавычки (\"\"\"{kn}\"\"\"), не в двойные (\"\"{kn}\"\")",
                i,
            )
    if not s.startswith('"""', i):
        raise SpodParseError(
            f"{loc}: следующий ключ должен начинаться с \"\"\" (тройных кавычек)",
            i,
        )
    i += 3
    m = _KEY_NAME_RE.match(s, i)
    if not m:
        raise SpodParseError(f"{loc}: недопустимое имя ключа (латиница, цифры, _)", i)
    name = m.group(0)
    i = m.end()
    if not s.startswith('"""', i):
        raise SpodParseError(
            f"{loc}, ключ «{name}»: после имени нужны три кавычки \"\"\" перед «:»",
            i,
        )
    i += 3
    return name, i


def _parse_triple_quoted_string(s: str, i: int, value_path: str) -> int:
    """С \"\"\" до закрывающих \"\"\" (содержимое не должно содержать неэкранированные \"\"\")."""
    vp = _format_value_location(value_path)
    if not s.startswith('"""', i):
        raise SpodParseError(f"значение {vp}: откройте строку тройными кавычками \"\"\"", i)
    i += 3
    end = s.find('"""', i)
    if end < 0:
        raise SpodParseError(f"значение {vp}: нет закрывающих тройных кавычек", i)
    return end + 3


def _parse_json_primitive(s: str, i: int, value_path: str) -> int:
    """Число, true, false, null без кавычек (для numeric_value_keys и элементов массива-чисел)."""
    vp = _format_value_location(value_path)
    i = _skip_ws(s, i)
    n = len(s)
    if i >= n:
        raise SpodParseError(f"значение {vp}: пусто (нужно число или true/false/null без кавычек)", i)
    for word in ("true", "false", "null"):
        lw = len(word)
        if s.startswith(word, i) and (i + lw >= n or s[i + lw] in ",}] \t\n\r"):
            return i + lw
    m = _NUM_RE.match(s, i)
    if not m:
        raise SpodParseError(
            f"значение {vp}: для numeric_value_keys нужно число или true/false/null без \"\"\"…\"\"\"",
            i,
        )
    return m.end()


def _parse_spod_value(s: str, i: int, numeric_keys: Set[str], value_path: str) -> int:
    """value_path — полный путь к значению (например getCondition.rewards[0].amount)."""
    prop = _property_key_from_value_path(value_path)
    vp = _format_value_location(value_path)
    i = _skip_ws(s, i)
    if i >= len(s):
        raise SpodParseError(f"значение {vp}: обрыв текста", i)

    if prop and prop in numeric_keys:
        return _parse_json_primitive(s, i, value_path)

    if s[i] == "{":
        return _parse_spod_object(s, i, numeric_keys, value_path)
    if s[i] == "[":
        return _parse_spod_array(s, i, numeric_keys, value_path)
    if s.startswith('"""', i):
        return _parse_triple_quoted_string(s, i, value_path)

    if s[i] == '"':
        lit = _short_json_string_literal(s, i)
        raise SpodParseError(
            f"значение {vp}: строка должна быть в \"\"\"…\"\"\", не в одной паре кавычек как в JSON; сейчас: {lit}",
            i,
        )

    raise SpodParseError(
        f"значение {vp}: ожидались \"\"\"строка\"\"\", объект {{}} или массив []",
        i,
    )


def _parse_spod_object(s: str, i: int, numeric_keys: Set[str], object_path: str) -> int:
    loc = _format_spod_location(object_path)
    i = _skip_ws(s, i)
    i = _expect_literal(s, i, "{", loc)
    only = _object_body_is_only_triple_quoted_string(s, i)
    if only is not None:
        inner, _end = only
        pv = (inner[:48] + "…") if len(inner) > 48 else inner
        raise SpodParseError(
            f"{loc}: в {{}} только строка без ключа — так нельзя; для списка строк используйте "
            f"[\"\"\"…\"\"\"], не {{\"\"\"…\"\"\"}} (содержимое: {pv!r})",
            i,
        )
    while True:
        i = _skip_ws(s, i)
        if i < len(s) and s[i] == "}":
            return i + 1
        key, j = _read_spod_key(s, i, object_path)
        value_path = f"{object_path}.{key}" if object_path else key
        i = _skip_ws(s, j)
        i = _expect_literal(s, i, ":", f"{loc}, ключ «{key}»")
        i = _parse_spod_value(s, i, numeric_keys, value_path)
        i = _skip_ws(s, i)
        if i < len(s) and s[i] == ",":
            i += 1
            continue
        if i < len(s) and s[i] == "}":
            return i + 1
        raise SpodParseError(f"{loc}, после «{key}»: нужна «,» или «}}»", i)


def _parse_spod_array(s: str, i: int, numeric_keys: Set[str], array_path: str) -> int:
    """Массив не в кавычках; строки — в тройных; числа/literal — без; объекты/массивы — как в JSON-структуре SPOD."""
    loc = f"массив {_format_value_location(array_path)}" if array_path else "массив в корне"
    i = _skip_ws(s, i)
    i = _expect_literal(s, i, "[", loc)
    elem_idx = 0
    while True:
        i = _skip_ws(s, i)
        if i < len(s) and s[i] == "]":
            return i + 1
        elem_path = f"{array_path}[{elem_idx}]" if array_path else f"[{elem_idx}]"
        if s[i] == "{":
            i = _parse_spod_object(s, i, numeric_keys, elem_path)
        elif s[i] == "[":
            i = _parse_spod_array(s, i, numeric_keys, elem_path)
        elif s.startswith('"""', i):
            i = _parse_triple_quoted_string(s, i, elem_path)
        elif s[i] in "-0123456789" or s.startswith("true", i) or s.startswith("false", i) or s.startswith("null", i):
            i = _parse_json_primitive(s, i, elem_path)
        else:
            raise SpodParseError(
                f"{loc}, элемент [{elem_idx}]: нужен \"\"\"строка\"\"\", число, {{}}, [] или literal",
                i,
            )
        elem_idx += 1
        i = _skip_ws(s, i)
        if i < len(s) and s[i] == ",":
            i += 1
            continue
        if i < len(s) and s[i] == "]":
            return i + 1
        raise SpodParseError(f"{loc}, после элемента [{elem_idx - 1}]: нужна «,» или «]»", i)


def _parse_spod_root(s: str, numeric_keys: Set[str]) -> None:
    """Проверяет, что вся строка — один корневой JSON-значение в нотации SPOD."""
    i = _skip_ws(s, 0)
    if i >= len(s):
        raise SpodParseError("пустая структура", i)
    if s[i] == "{":
        j = _parse_spod_object(s, i, numeric_keys, "")
    elif s[i] == "[":
        j = _parse_spod_array(s, i, numeric_keys, "")
    else:
        raise SpodParseError("корень: ожидается {{ или [", i)
    j = _skip_ws(s, j)
    if j != len(s):
        raise SpodParseError("лишний текст после конца JSON", j)


def _collect_numeric_type_errors(obj: Any, numeric_keys: Set[str], path: str) -> List[str]:
    """
    После json.loads: у ключей из numeric_keys значение должно быть int/float (не str, не bool).
    Возвращает список всех нарушений (короткие формулировки).
    """
    errs: List[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else str(k)
            if k in numeric_keys:
                if isinstance(v, bool):
                    errs.append(f"«{p}»: числовое поле — не bool, сейчас {v!r}")
                elif isinstance(v, str):
                    pv = v if len(v) <= 48 else v[:45] + "…"
                    errs.append(f"«{p}»: нужно число без кавычек в SPOD, после разбора получилась строка {pv!r}")
                elif not isinstance(v, (int, float)):
                    errs.append(f"«{p}»: ожидалось число, тип {type(v).__name__}")
            errs.extend(_collect_numeric_type_errors(v, numeric_keys, p))
    elif isinstance(obj, list):
        for idx, it in enumerate(obj):
            errs.extend(_collect_numeric_type_errors(it, numeric_keys, f"{path}[{idx}]"))
    return errs


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

    sym_err = _check_outer_double_quote_symmetry(s0)
    if sym_err:
        return False, sym_err

    s1 = _strip_outer_matching_quotes(s0)
    s1 = _strip_leading_bom(s1)
    s1 = _drop_whitespace_outside_spod_triple_strings(s1)

    try:
        _parse_spod_root(s1, numeric_keys)
    except SpodParseError as e:
        return False, e.message

    s2 = _normalize_triple_to_double(s1)
    s2 = _strip_outer_matching_quotes(s2)

    try:
        parsed = json.loads(s2)
    except json.JSONDecodeError as e:
        ok, alt = _try_safe_load(s2)
        if ok:
            parsed = alt
        else:
            detail = f"JSON после нормализации: {e.msg} (стр.{e.lineno}, кол.{e.colno})"
            return False, detail

    num_errs = _collect_numeric_type_errors(parsed, numeric_keys, "")
    if num_errs:
        return False, "numeric_value_keys:\n• " + "\n• ".join(num_errs)

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
            cell_msg = msg if len(msg) <= _MAX_CELL_ERROR_LEN else msg[: _MAX_CELL_ERROR_LEN - 1] + "…"
            statuses.append(cell_msg)
            if len(sample) < _MAX_SAMPLE:
                sample.append(f"[{_excel_row(idx)}] {msg[:200]}")

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
