# -*- coding: utf-8 -*-
"""
Глобальный контейнер текущей конфигурации (для совместимости с кодом,
который пока читает конфиг из глобальных переменных).
"""

from typing import Optional

from src.config_loader import Config

_current_config: Optional[Config] = None


def set_current_config(config: Config) -> None:
    global _current_config
    _current_config = config


def get_current_config() -> Optional[Config]:
    return _current_config
