# -*- coding: utf-8 -*-
"""
Контекст текущего блока PROM/IFT/PSI: метки в логах/консоли, потокобезопасность вывода.
Только стандартная библиотека Python.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, List, Optional

# Текущий блок для потока (последовательный и параллельный режимы)
_tls = threading.local()
# Сериализация записи в stdout/stderr (непересекающийся вывод при параллели)
_console_lock = threading.Lock()


def set_current_block(block: Optional[str]) -> None:
    """Установить код блока для текущего потока (или сбросить)."""
    if block is None or not str(block).strip():
        _tls.block = None
    else:
        _tls.block = str(block).strip().upper()


def get_current_block() -> Optional[str]:
    """Код текущего блока или None."""
    return getattr(_tls, "block", None)


def block_label(block: Optional[str] = None) -> str:
    """Префикс вида [PROM] для сообщений."""
    b = (block if block is not None else get_current_block()) or ""
    b = str(b).strip().upper()
    return f"[{b}]" if b else ""


def prefix_message(msg: str, block: Optional[str] = None) -> str:
    """Добавить метку блока в начало сообщения, если ещё нет."""
    label = block_label(block)
    if not label:
        return msg
    text = str(msg)
    if text.startswith(label):
        return text
    return f"{label} {text}"


class BlockLogFilter(logging.Filter):
    """Вставляет [BLOCK] в record.msg для записей текущего потока."""

    def filter(self, record: logging.LogRecord) -> bool:
        label = block_label()
        if not label:
            return True
        try:
            msg = record.getMessage()
        except Exception:
            msg = str(record.msg)
        if not msg.startswith(label):
            record.msg = f"{label} {msg}"
            record.args = ()
        return True


def console_write(line: str = "", *, end: str = "\n", flush: bool = True) -> None:
    """Потокобезопасная печать одной строки в консоль (с учётом метки блока уже в тексте)."""
    with _console_lock:
        print(line, end=end, flush=flush)


def console_print_lines(lines: List[str]) -> None:
    """Атомарно вывести пачку строк (для параллельных блоков — без перемешивания)."""
    with _console_lock:
        for line in lines:
            print(line, flush=True)


def locked_console(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Выполнить функцию печати под общим замком консоли."""
    with _console_lock:
        return fn(*args, **kwargs)


def resolve_block_placeholders(value: Any, block: str) -> Any:
    """
    Подставить {BLOCK}/{block} в строках (рекурсивно для dict/list).
    Для обратной совместимости также заменяет устаревший OUT/DB/spod_… без блока — нет,
    только явные плейсхолдеры.
    """
    block_u = str(block).strip().upper()

    def _one(s: str) -> str:
        return s.replace("{BLOCK}", block_u).replace("{block}", block_u)

    if isinstance(value, str):
        return _one(value)
    if isinstance(value, list):
        return [resolve_block_placeholders(v, block_u) for v in value]
    if isinstance(value, dict):
        return {k: resolve_block_placeholders(v, block_u) for k, v in value.items()}
    return value
