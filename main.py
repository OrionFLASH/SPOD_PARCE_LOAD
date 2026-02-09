# -*- coding: utf-8 -*-
"""
Точка входа: загрузка конфигурации, внедрение в контекст, запуск основного пайплайна.
Весь остальной код и модули находятся в каталоге src/.
"""

import sys

from src.config_loader import Config
from src.config_holder import set_current_config
from src import main_impl


def main() -> None:
    config = Config()
    set_current_config(config)
    main_impl.main()


if __name__ == "__main__":
    main()
    sys.exit(0)
