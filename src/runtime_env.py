# -*- coding: utf-8 -*-
"""
Окружение запуска SPOD (ENV-01): Python и пакеты из Anaconda 3.12, без установки через pip.

- environment_summary() — строка с версиями для лога (пишется при старте main_impl.main);
- check_environment() — список проблем (пустой — окружение подходит);
- запуск модулем: python -m src.runtime_env — печать версий и код 0 / 3 (используют run_main.bat/.sh).
"""

from __future__ import annotations

import importlib
import platform
import re
import sys
from typing import List, Optional, Tuple

# Проверенное целевое окружение: Anaconda 3.12 (Python 3.12.7, pandas 2.2.2, numpy 1.26.4, openpyxl 3.1.5)
MIN_PYTHON: Tuple[int, int] = (3, 12)
REQUIRED_PACKAGES: List[Tuple[str, Tuple[int, int]]] = [
    ("pandas", (2, 2)),
    ("numpy", (1, 26)),
    ("openpyxl", (3, 1)),
]
# Код возврата run_main.bat/.sh, если окружение не подходит (коды main.py: 0/1/2)
EXIT_BAD_ENVIRONMENT = 3


def _version_tuple(version: str) -> Tuple[int, ...]:
    """'2.2.2' → (2, 2, 2); '2.3.0rc1' → (2, 3, 0)."""
    return tuple(int(x) for x in re.findall(r"\d+", version)[:3])


def _package_version(name: str) -> Optional[str]:
    try:
        module = importlib.import_module(name)
    except Exception:
        return None
    return str(getattr(module, "__version__", "?"))


def environment_summary() -> str:
    """Одна строка: Python, пакеты, платформа, интерпретатор."""
    parts = [f"Python {platform.python_version()}"]
    for name, _min in REQUIRED_PACKAGES:
        parts.append(f"{name} {_package_version(name) or 'НЕТ'}")
    parts.append(platform.platform())
    parts.append(sys.executable)
    return " | ".join(parts)


def check_environment() -> List[str]:
    """Проблемы окружения; пустой список — можно запускать main.py."""
    problems: List[str] = []
    if sys.version_info[:2] < MIN_PYTHON:
        problems.append(
            f"нужен Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+, сейчас {platform.python_version()} ({sys.executable})"
        )
    for name, min_version in REQUIRED_PACKAGES:
        version = _package_version(name)
        if version is None:
            problems.append(f"не найден пакет {name} (входит в Anaconda 3.12)")
        elif _version_tuple(version)[:2] < min_version:
            problems.append(
                f"пакет {name} {version} старше минимального {min_version[0]}.{min_version[1]}"
            )
    return problems


def main() -> int:
    print(f"Окружение: {environment_summary()}")
    problems = check_environment()
    if not problems:
        return 0
    print("Окружение не подходит для запуска SPOD:")
    for p in problems:
        print(f"  - {p}")
    print(
        "Пакеты НЕ устанавливаются (pip install запрещён). Запустите из Anaconda Prompt "
        "или укажите интерпретатор Anaconda 3.12 в переменной SPOD_PYTHON, например:\n"
        "  set SPOD_PYTHON=C:\\ProgramData\\anaconda3\\python.exe"
    )
    return EXIT_BAD_ENVIRONMENT


if __name__ == "__main__":
    sys.exit(main())
