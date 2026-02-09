# -*- coding: utf-8 -*-
"""
Настройка логирования: форматтер с именем функции и инициализация логгера.
"""

import inspect
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from src.config_loader import Config


class CallerFormatter(logging.Formatter):
    """Форматтер, добавляющий имя вызывающей функции к сообщению лога."""

    def format(self, record: logging.LogRecord) -> str:
        try:
            stack = inspect.stack()
            func_name = getattr(record, "funcName", "?")
            for frame_info in stack:
                filename = frame_info[1]
                func_name_in_frame = frame_info[3]
                if "logging" not in filename and func_name_in_frame not in ("format", "<module>"):
                    func_name = func_name_in_frame
                    break
        except Exception:
            func_name = getattr(record, "funcName", "?")
        if hasattr(record, "msg"):
            if isinstance(record.msg, str) and record.args:
                original_msg = record.msg % record.args
            else:
                original_msg = str(record.msg)
        else:
            original_msg = record.getMessage()
        record.msg = f"{original_msg} [def: {func_name}]"
        record.args = ()
        return super().format(record)


def setup_logger(config: Config) -> str:
    """
    Настраивает логирование: файл (DEBUG) и консоль (INFO).
    Возвращает путь к лог-файлу.
    """
    level_suffix = f"_{config.log_level}" if config.log_level else ""
    date_suffix = f"_{datetime.now().strftime('%Y%m%d_%H_%M')}.log"
    log_file = os.path.join(config.dir_logs, config.log_base_name + level_suffix + date_suffix)

    if logging.getLogger().hasHandlers():
        return log_file

    os.makedirs(config.dir_logs, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    file_formatter = CallerFormatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return log_file
