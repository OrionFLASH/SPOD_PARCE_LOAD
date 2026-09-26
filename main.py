# -*- coding: utf-8 -*-
"""
Точка входа: загрузка конфигурации, внедрение в контекст, запуск основного пайплайна.
Весь остальной код и модули находятся в каталоге src/.
"""

import sys

from src.config_loader import Config
from src.config_holder import set_current_config
from src import main_impl


def main() -> int:
    """Код возврата: 0 — успех, 1 — ошибки обработки/записи, 2 — нет входных файлов."""
    config = Config()
    set_current_config(config)
    return main_impl.main()


if __name__ == "__main__":
    sys.exit(main())
