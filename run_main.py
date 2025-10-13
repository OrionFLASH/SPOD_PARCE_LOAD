#!/bin/bash
# Скрипт для запуска main.py с правильным Python

echo "=== ЗАПУСК MAIN.PY С ПРАВИЛЬНЫМ PYTHON ==="
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
    echo "Создайте виртуальное окружение: python3 -m venv venv"
    exit 1
fi

# Активируем виртуальное окружение
echo "Активация виртуального окружения..."
source venv/bin/activate

# Проверяем Python
echo "Python путь: $(which python)"
echo "Python версия: $(python --version)"

# Проверяем pandas
echo "Проверка pandas:"
python -c "import pandas; print('✅ pandas доступен, версия:', pandas.__version__)" 2>/dev/null || {
    echo "❌ pandas НЕ доступен в виртуальном окружении"
    echo "Устанавливаем pandas..."
    pip install pandas openpyxl
}

echo ""
echo "=== ЗАПУСК MAIN.PY ==="
python main.py
