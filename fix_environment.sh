#!/bin/bash
# Скрипт для принудительного переключения на виртуальное окружение

echo "=== ПРИНУДИТЕЛЬНОЕ ПЕРЕКЛЮЧЕНИЕ НА ВИРТУАЛЬНОЕ ОКРУЖЕНИЕ ==="
echo ""

# Проверяем, что мы в правильной директории
if [ ! -f "main.py" ]; then
    echo "❌ ОШИБКА: main.py не найден!"
    echo "Убедитесь, что вы находитесь в каталоге проекта"
    exit 1
fi

# Проверяем виртуальное окружение
if [ ! -d "venv" ]; then
    echo "❌ ОШИБКА: venv не найден!"
    echo "Создаем новое виртуальное окружение..."
    python3 -m venv venv
fi

# Принудительно активируем виртуальное окружение
echo "Активация виртуального окружения..."
source venv/bin/activate

# Проверяем Python
echo "Python путь: $(which python)"
echo "Python версия: $(python --version)"

# Проверяем и устанавливаем модули
echo ""
echo "=== ПРОВЕРКА И УСТАНОВКА МОДУЛЕЙ ==="

# Проверяем pandas
echo "Проверка pandas:"
python -c "import pandas; print('✅ pandas доступен, версия:', pandas.__version__)" 2>/dev/null || {
    echo "❌ pandas НЕ доступен, устанавливаем..."
    pip install pandas
}

# Проверяем openpyxl
echo "Проверка openpyxl:"
python -c "import openpyxl; print('✅ openpyxl доступен, версия:', openpyxl.__version__)" 2>/dev/null || {
    echo "❌ openpyxl НЕ доступен, устанавливаем..."
    pip install openpyxl
}

# Проверяем numpy
echo "Проверка numpy:"
python -c "import numpy; print('✅ numpy доступен, версия:', numpy.__version__)" 2>/dev/null || {
    echo "❌ numpy НЕ доступен, устанавливаем..."
    pip install numpy
}

# Устанавливаем все необходимые модули
echo ""
echo "=== УСТАНОВКА ВСЕХ НЕОБХОДИМЫХ МОДУЛЕЙ ==="
pip install pandas openpyxl numpy python-dateutil pytz tzdata

# Проверяем все модули
echo ""
echo "=== ФИНАЛЬНАЯ ПРОВЕРКА ==="
python -c "
import sys
print('Python путь:', sys.executable)
print('Python версия:', sys.version)
print('')

try:
    import pandas as pd
    print('✅ pandas', pd.__version__)
except ImportError as e:
    print('❌ pandas:', e)

try:
    import openpyxl
    print('✅ openpyxl', openpyxl.__version__)
except ImportError as e:
    print('❌ openpyxl:', e)

try:
    import numpy as np
    print('✅ numpy', np.__version__)
except ImportError as e:
    print('❌ numpy:', e)

try:
    from datetime import datetime
    print('✅ datetime')
except ImportError as e:
    print('❌ datetime:', e)

try:
    import json
    print('✅ json')
except ImportError as e:
    print('❌ json:', e)

try:
    import os
    print('✅ os')
except ImportError as e:
    print('❌ os:', e)

try:
    import logging
    print('✅ logging')
except ImportError as e:
    print('❌ logging:', e)
"

echo ""
echo "=== ГОТОВО! ==="
echo "Теперь можете запускать:"
echo "  python main.py"
echo "  python generate_employee_data.py"
echo ""
echo "Для выхода из окружения используйте: deactivate"
