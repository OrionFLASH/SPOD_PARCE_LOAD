# -*- coding: utf-8 -*-
"""
Проверка ячеек со «SPOD-JSON»: BOM и Unicode-пробелы вне блоков \"\"\"…\"\"\"; симметрия внешних кавычек;
рекурсивный разбор (ключи и строки в тройных кавычках, numeric_value_keys без кавычек);
типовые ошибки: \"\"key\"\" вместо \"\"\"key\"\"\", значение в одной паре кавычек как в JSON,
лишние {} вокруг одной строки в массиве. Нормализация кавычек и json.loads.
Сообщения в колонку на листе — короткие (путь + суть); по одной ячейке собираются **все** замечания этапа разбора SPOD (не только первое), см. Docs/CONSISTENCY_CHECKS_FORMAT.md п. 2.8.

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

# Сколько структурных замечаний SPOD собирать в одной ячейке (остальное — сводка «и ещё N»).
_MAX_STRUCTURE_ERRORS = 80

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
    """Если s[i]=='\"', короткое представление литерала для текста ошибки (JSON или ошибочное \"\"…\"\")."""
    if i >= len(s) or s[i] != '"':
        return ""
    n = len(s)
    if i + 1 < n and s[i + 1] == '"' and not s.startswith('"""', i):
        j = i + 2
        if j >= n or s[j] in ",]}:" or s[j].isspace():
            return '""'
        while j + 1 < n:
            if s[j] == '"' and s[j + 1] == '"':
                body = s[i + 2 : j]
                if len(body) > max_len:
                    body = body[: max_len - 1] + "…"
                return '""' + body + '""'
            j += 1
        body = s[i + 2 : min(i + 2 + max_len, n)]
        return '""' + body + "…"
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


def _skip_json_string_end(s: str, i: int) -> int:
    """
    Позиция сразу после строкового токена, начинающегося в i.
    Учитывает обычный JSON-литерал «"…"» и ошибочный шаблон «""текст""» (как у ключей SPOD).
    """
    if i >= len(s) or s[i] != '"':
        return i
    n = len(s)
    # Не тройные: «""…""» в значении (частая ошибка вместо """…""")
    if i + 1 < n and s[i + 1] == '"' and not s.startswith('"""', i):
        j = i + 2
        if j >= n:
            return n
        if s[j] in ",]}:" or s[j].isspace():
            return j
        while j + 1 < n:
            if s[j] == '"' and s[j + 1] == '"':
                return j + 2
            if s[j] == "\\" and j + 1 < n:
                j += 2
                continue
            j += 1
        return n
    j = i + 1
    while j < n:
        if s[j] == "\\" and j + 1 < n:
            j += 2
            continue
        if s[j] == '"':
            return j + 1
        j += 1
    return n


def _scan_to_property_boundary(s: str, i: int) -> int:
    """
    Следующая «,» или «}» на текущем уровне содержимого объекта (вложенные {}[] и строки учитываются).
    Используется для восстановления после ошибки ключа/значения.
    """
    brace = 0
    bracket = 0
    j = i
    n = len(s)
    while j < n:
        if s.startswith('"""', j):
            e = s.find('"""', j + 3)
            if e < 0:
                return n
            j = e + 3
            continue
        if s[j] == '"':
            j = _skip_json_string_end(s, j)
            continue
        c = s[j]
        if c == "{":
            brace += 1
        elif c == "}":
            if brace == 0 and bracket == 0:
                return j + 1
            brace = max(0, brace - 1)
        elif c == "[":
            bracket += 1
        elif c == "]":
            bracket = max(0, bracket - 1)
        elif c == "," and brace == 0 and bracket == 0:
            return j + 1
        j += 1
    return n


def _scan_to_array_element_boundary(s: str, i: int) -> int:
    """
    Следующая «,» или «]», завершающая текущий элемент верхнего массива (внешний «[» уже открыт).
    """
    brace = 0
    bracket = 1
    j = i
    n = len(s)
    while j < n:
        if s.startswith('"""', j):
            e = s.find('"""', j + 3)
            if e < 0:
                return n
            j = e + 3
            continue
        if s[j] == '"':
            j = _skip_json_string_end(s, j)
            continue
        c = s[j]
        if c == "{":
            brace += 1
        elif c == "}":
            brace = max(0, brace - 1)
        elif c == "[":
            bracket += 1
        elif c == "]":
            if brace == 0 and bracket == 1:
                return j + 1
            bracket -= 1
        elif c == "," and brace == 0 and bracket == 1:
            return j + 1
        j += 1
    return n


def _scan_balanced(s: str, i: int, open_ch: str, close_ch: str) -> int:
    """Индекс после парной закрывающей скобки для open_ch в позиции i."""
    if i >= len(s) or s[i] != open_ch:
        return i
    depth = 0
    j = i
    n = len(s)
    while j < n:
        if s.startswith('"""', j):
            e = s.find('"""', j + 3)
            if e < 0:
                return n
            j = e + 3
            continue
        if s[j] == '"':
            j = _skip_json_string_end(s, j)
            continue
        if s[j] == open_ch:
            depth += 1
        elif s[j] == close_ch:
            depth -= 1
            if depth == 0:
                return j + 1
        j += 1
    return n


def _skip_value_token(s: str, i: int) -> int:
    """Один токен значения SPOD/JSON с позиции i (грубый пропуск при сбое разбора)."""
    i = _skip_ws(s, i)
    if i >= len(s):
        return i
    if s.startswith('"""', i):
        e = s.find('"""', i + 3)
        return (e + 3) if e >= 0 else len(s)
    if s[i] == '"':
        return _skip_json_string_end(s, i)
    if s[i] == "{":
        return _scan_balanced(s, i, "{", "}")
    if s[i] == "[":
        return _scan_balanced(s, i, "[", "]")
    m = _NUM_RE.match(s, i)
    if m:
        return m.end()
    for w in ("true", "false", "null"):
        if s.startswith(w, i) and (i + len(w) >= len(s) or s[i + len(w)] in ",}] \t\n\r"):
            return i + len(w)
    return i + 1


def _read_spod_key_collect(
    s: str, i: int, object_path: str
) -> Tuple[Optional[str], int, List[str]]:
    """
    Читает ключ объекта SPOD или фиксирует ошибку и позицию для продолжения разбора.
    Возвращает (имя_ключа_или_None, индекс_после_токена_ключа, список_сообщений).
    """
    loc = _format_spod_location(object_path)
    errs: List[str] = []
    i = _skip_ws(s, i)
    # Неверный стиль ""key"" вместо """key"""
    if s.startswith('""', i) and not s.startswith('"""', i):
        m_wrong = _KEY_NAME_RE.match(s, i + 2)
        if m_wrong:
            kn = m_wrong.group(0)
            end = m_wrong.end()
            if s.startswith('""', end):
                msg = (
                    f"{loc}: ключ «{kn}» — оберните имя в тройные кавычки (\"\"\"{kn}\"\"\"), "
                    f"не в двойные (\"\"{kn}\"\")"
                )
                return kn, end + 2, [msg]
        errs.append(f"{loc}: ожидалось имя ключа в виде \"\"ключ\"\" или \"\"\"ключ\"\"\"")
        return None, _scan_to_property_boundary(s, i), errs
    if s.startswith('"""', i):
        i += 3
        m = _KEY_NAME_RE.match(s, i)
        if not m:
            errs.append(f"{loc}: недопустимое имя ключа (латиница, цифры, _)")
            return None, _scan_to_property_boundary(s, i), errs
        name = m.group(0)
        i = m.end()
        if not s.startswith('"""', i):
            errs.append(f"{loc}, ключ «{name}»: после имени нужны три кавычки \"\"\" перед «:»")
            return None, _scan_to_property_boundary(s, i), errs
        return name, i + 3, []
    # Одинарная «"» как в JSON: "key"
    if i < len(s) and s[i] == '"':
        j = i + 1
        while j < len(s) and s[j] != '"':
            if s[j] == "\\" and j + 1 < len(s):
                j += 2
                continue
            j += 1
        if j >= len(s):
            errs.append(f"{loc}: незакрытая кавычка у ключа")
            return None, len(s), errs
        inner = s[i + 1 : j]
        if _KEY_NAME_RE.fullmatch(inner):
            msg = f"{loc}: ключ «{inner}» — оберните имя в тройные кавычки (\"\"\"{inner}\"\"\")"
            return inner, j + 1, [msg]
        errs.append(f"{loc}: недопустимое имя ключа")
        return None, j + 1, errs
    errs.append(f"{loc}: следующий ключ должен начинаться с \"\"\" (тройных кавычек)")
    return None, _scan_to_property_boundary(s, i), errs


def _format_structure_errors_list(errs: List[str]) -> str:
    """Склеивает список структурных ошибок с ограничением длины списка."""
    if not errs:
        return ""
    if len(errs) <= _MAX_STRUCTURE_ERRORS:
        return "\n• ".join(["разбор SPOD:"] + errs)
    head = errs[: _MAX_STRUCTURE_ERRORS - 1]
    rest = len(errs) - len(head)
    return "\n• ".join(["разбор SPOD:"] + head + [f"… и ещё {rest} ошибок(ок)"])


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


def _parse_spod_value_collect(
    s: str, i: int, numeric_keys: Set[str], value_path: str
) -> Tuple[int, List[str]]:
    """Разбор значения SPOD с накоплением всех замечаний по поддереву."""
    errs: List[str] = []
    prop = _property_key_from_value_path(value_path)
    vp = _format_value_location(value_path)
    i = _skip_ws(s, i)
    if i >= len(s):
        errs.append(f"значение {vp}: обрыв текста")
        return i, errs
    if prop and prop in numeric_keys:
        try:
            return _parse_json_primitive(s, i, value_path), errs
        except SpodParseError as e:
            errs.append(e.message)
            return _skip_value_token(s, i), errs
    if s[i] == "{":
        return _parse_spod_object_collect(s, i, numeric_keys, value_path)
    if s[i] == "[":
        return _parse_spod_array_collect(s, i, numeric_keys, value_path)
    if s.startswith('"""', i):
        try:
            return _parse_triple_quoted_string(s, i, value_path), errs
        except SpodParseError as e:
            errs.append(e.message)
            e2 = s.find('"""', i + 3)
            return (e2 + 3) if e2 >= 0 else len(s), errs
    if s[i] == '"':
        lit = _short_json_string_literal(s, i)
        errs.append(
            f"значение {vp}: строка должна быть в \"\"\"…\"\"\", не в одной паре кавычек как в JSON; сейчас: {lit}"
        )
        return _skip_json_string_end(s, i), errs
    errs.append(f"значение {vp}: ожидались \"\"\"строка\"\"\", объект {{}} или массив []")
    return _skip_value_token(s, i), errs


def _parse_spod_object_collect(
    s: str, i: int, numeric_keys: Set[str], object_path: str
) -> Tuple[int, List[str]]:
    """Объект SPOD: все ошибки по полям, без остановки на первой."""
    errs: List[str] = []
    loc = _format_spod_location(object_path)
    i = _skip_ws(s, i)
    if i >= len(s) or s[i] != "{":
        errs.append(f"{loc}: ожидался символ «{{»")
        return i, errs
    i += 1
    only = _object_body_is_only_triple_quoted_string(s, i)
    if only is not None:
        inner, end_after = only
        pv = (inner[:48] + "…") if len(inner) > 48 else inner
        errs.append(
            f"{loc}: в {{}} только строка без ключа — так нельзя; для списка строк используйте "
            f"[\"\"\"…\"\"\"], не {{\"\"\"…\"\"\"}} (содержимое: {pv!r})"
        )
        return end_after, errs
    steps = 0
    while True:
        steps += 1
        if steps > 20000:
            errs.append(f"{loc}: разбор прерван (слишком много шагов)")
            return i, errs
        i = _skip_ws(s, i)
        if i >= len(s):
            errs.append(f"{loc}: незакрытый объект")
            return i, errs
        if s[i] == "}":
            return i + 1, errs
        key_start = i
        key, j, kerrs = _read_spod_key_collect(s, i, object_path)
        errs.extend(kerrs)
        if key is None:
            nxt = j if j > key_start else _scan_to_property_boundary(s, key_start)
            if nxt <= key_start:
                nxt = min(key_start + 1, len(s))
            i = nxt
            continue
        value_path = f"{object_path}.{key}" if object_path else key
        i = _skip_ws(s, j)
        if i >= len(s) or s[i] != ":":
            errs.append(f"{loc}, после ключа «{key}»: ожидался «:»")
            i = _scan_to_property_boundary(s, j)
            continue
        i += 1
        i, verrs = _parse_spod_value_collect(s, i, numeric_keys, value_path)
        errs.extend(verrs)
        i = _skip_ws(s, i)
        if i < len(s) and s[i] == ",":
            i += 1
            continue
        if i < len(s) and s[i] == "}":
            return i + 1, errs
        errs.append(f"{loc}, после «{key}»: нужна «,» или «}}»")
        i = _scan_to_property_boundary(s, i)


def _parse_spod_array_collect(
    s: str, i: int, numeric_keys: Set[str], array_path: str
) -> Tuple[int, List[str]]:
    """Массив SPOD с накоплением ошибок по элементам."""
    errs: List[str] = []
    loc = f"массив {_format_value_location(array_path)}" if array_path else "массив в корне"
    i = _skip_ws(s, i)
    if i >= len(s) or s[i] != "[":
        errs.append(f"{loc}: ожидался символ «[»")
        return i, errs
    i += 1
    elem_idx = 0
    steps = 0
    while True:
        steps += 1
        if steps > 20000:
            errs.append(f"{loc}: разбор прерван (слишком много шагов)")
            return i, errs
        i = _skip_ws(s, i)
        if i >= len(s):
            errs.append(f"{loc}: незакрытый массив")
            return i, errs
        if s[i] == "]":
            return i + 1, errs
        elem_path = f"{array_path}[{elem_idx}]" if array_path else f"[{elem_idx}]"
        if (
            s[i] == "{"
            or s[i] == "["
            or s.startswith('"""', i)
            or s[i] == '"'
            or s[i] in "-0123456789"
            or s.startswith("true", i)
            or s.startswith("false", i)
            or s.startswith("null", i)
        ):
            i, verrs = _parse_spod_value_collect(s, i, numeric_keys, elem_path)
            errs.extend(verrs)
        else:
            errs.append(
                f"{loc}, элемент [{elem_idx}]: нужен \"\"\"строка\"\"\", число, {{}}, [] или literal"
            )
            i = _skip_value_token(s, i)
        elem_idx += 1
        i = _skip_ws(s, i)
        if i < len(s) and s[i] == ",":
            i += 1
            continue
        if i < len(s) and s[i] == "]":
            return i + 1, errs
        errs.append(f"{loc}, после элемента [{elem_idx - 1}]: нужна «,» или «]»")
        i = _scan_to_array_element_boundary(s, i)


def _parse_spod_root_collect(s: str, numeric_keys: Set[str]) -> List[str]:
    """Полный проход по строке SPOD: список всех структурных ошибок (пустой — ОК)."""
    errs: List[str] = []
    i = _skip_ws(s, 0)
    if i >= len(s):
        return ["пустая структура"]
    if s[i] == "{":
        j, e = _parse_spod_object_collect(s, i, numeric_keys, "")
    elif s[i] == "[":
        j, e = _parse_spod_array_collect(s, i, numeric_keys, "")
    else:
        return ["корень: ожидается { или ["]
    errs.extend(e)
    j = _skip_ws(s, j)
    if j != len(s):
        errs.append("лишний текст после конца JSON")
    return errs


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
    При ошибках разбора SPOD в сообщение включаются все найденные нарушения (список через «•»),
    с ограничением длины списка константой _MAX_STRUCTURE_ERRORS.
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

    st_errs = _parse_spod_root_collect(s1, numeric_keys)
    if st_errs:
        return False, _format_structure_errors_list(st_errs)

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
