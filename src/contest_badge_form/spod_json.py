# -*- coding: utf-8 -*-
"""Разбор и сборка ячеек SPOD-JSON с тройными кавычками ``\"\"\"``."""

from __future__ import annotations

import json
import logging
from typing import Any, List, Optional, Union

JsonValue = Union[dict, list, str, int, float, bool, None]


def normalize_spod_json_text(raw: Any) -> str:
    """Тройные кавычки → обычные; снять внешнюю обёртку ``\"…\"`` вокруг объекта/массива."""
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s or s in {"-", "None", "null"}:
        return ""
    s = s.replace('"""', '"')
    while len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        inner = s[1:-1].strip()
        if inner.startswith("{") or inner.startswith("["):
            s = inner
        else:
            break
    return s.strip()


def parse_spod_json(raw: Any) -> Any:
    """
    Разбор ячейки SPOD в Python-объект.
    Пустая ячейка → None; при ошибке — логирование и None.
    """
    norm = normalize_spod_json_text(raw)
    if not norm:
        return None
    try:
        return json.loads(norm)
    except json.JSONDecodeError as exc:
        logging.warning(
            "[contest_badge_form] Не удалось разобрать SPOD-JSON: %s | %s",
            exc,
            norm[:120],
        )
        return None


def dumps_spod_json(obj: Any) -> str:
    """Сериализация в формат выгрузки SPOD (ключи и строки в ``\"\"\"``)."""
    return _dumps_value(obj)


def _dumps_value(obj: Any) -> str:
    if obj is None:
        return "null"
    if isinstance(obj, bool):
        return "true" if obj else "false"
    if isinstance(obj, int) and not isinstance(obj, bool):
        return str(obj)
    if isinstance(obj, float):
        if obj == int(obj):
            return str(int(obj))
        return str(obj)
    if isinstance(obj, str):
        return '"""' + obj + '"""'
    if isinstance(obj, list):
        if not obj:
            return "[]"
        return "[" + ", ".join(_dumps_value(x) for x in obj) + "]"
    if isinstance(obj, dict):
        if not obj:
            return "{}"
        parts: List[str] = []
        for key, value in obj.items():
            parts.append(f"{_dumps_value(str(key))}: {_dumps_value(value)}")
        return "{" + ", ".join(parts) + "}"
    return '"""' + str(obj) + '"""'


def list_from_form_cell(raw: Any) -> List[str]:
    """Массив из формы: ``a;b;c`` или уже SPOD/JSON-массив."""
    if raw is None:
        return []
    s = str(raw).strip()
    if not s:
        return []
    if s.startswith("["):
        parsed = parse_spod_json(s)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
        return []
    return [part.strip() for part in s.split(";") if part.strip()]


def form_cell_from_list(values: Optional[List[Any]]) -> str:
    """Список → строка формы через ``;``."""
    if not values:
        return ""
    return ";".join(str(v) for v in values)


def maybe_spod_cell(value: Any, *, as_json: bool) -> str:
    """Значение для CSV/Excel SPOD: JSON-колонка или плоская строка."""
    if value is None:
        return ""
    if as_json:
        if isinstance(value, str):
            # Уже готовая SPOD-строка
            stripped = value.strip()
            if stripped.startswith("{") or stripped.startswith("["):
                return stripped
            parsed = parse_spod_json(value)
            if parsed is not None:
                return dumps_spod_json(parsed)
            return value
        return dumps_spod_json(value)
    return "" if value is None else str(value)


def coerce_form_scalar(val: Any) -> Any:
    """
    Привести скаляр из формы к типу SPOD: числа без кавычек, остальное — строка.
    """
    if val is None:
        return ""
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val
    s = str(val).strip()
    if s == "":
        return ""
    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        return int(s)
    if s.replace(".", "", 1).isdigit() and s.count(".") == 1:
        try:
            return float(s)
        except ValueError:
            return s
    return s
