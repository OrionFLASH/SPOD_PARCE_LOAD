#!/bin/bash
# Запуск SPOD (main.py) на macOS/Linux: ./run_main.sh [аргументы main.py]
# Только Python/Anaconda 3.12 и его пакеты; ничего не устанавливает (pip install запрещён).
# Интерпретатор: переменная SPOD_PYTHON, иначе python3.12 из PATH, иначе /opt/anaconda3/bin/python3.12.
# venv проекта (Python 3.14) целевым окружением НЕ является.
# Коды возврата: как у main.py; 3 — не найден подходящий Python или пакеты.

cd "$(dirname "$0")" || exit 3

PY="${SPOD_PYTHON:-}"
if [ -z "$PY" ]; then
    if command -v python3.12 >/dev/null 2>&1; then
        PY="python3.12"
    elif [ -x /opt/anaconda3/bin/python3.12 ]; then
        PY="/opt/anaconda3/bin/python3.12"
    else
        echo "ОШИБКА: не найден python3.12. Укажите интерпретатор Anaconda 3.12: SPOD_PYTHON=/путь/к/python3.12 ./run_main.sh"
        exit 3
    fi
fi

"$PY" -m src.runtime_env || exit 3
echo
"$PY" main.py "$@"
RC=$?
echo
echo "Код завершения: $RC"
exit $RC
