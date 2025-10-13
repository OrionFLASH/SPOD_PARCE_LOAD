#!/bin/bash
# Скрипт для активации виртуального окружения

echo "=== АКТИВАЦИЯ ВИРТУАЛЬНОГО ОКРУЖЕНИЯ ==="
echo "Текущая директория: $(pwd)"
echo ""

# Проверяем, что мы в правильной директории
if [ ! -d "venv" ]; then
    echo "❌ ОШИБКА: Папка venv не найдена!"
    echo "Убедитесь, что вы находитесь в каталоге проекта:"
    echo "cd /Users/orionflash/Desktop/MyProject/SPOD_PROM/SPOD-cursor-Parce/SPOD_PARCE_LOAD"
    exit 1
fi

# Активируем виртуальное окружение
echo "Активация виртуального окружения..."
source venv/bin/activate

# Проверяем активацию
echo ""
echo "=== ПРОВЕРКА АКТИВАЦИИ ==="
echo "Python путь: $(which python)"
echo "Python версия: $(python --version)"

# Проверяем pandas
echo ""
echo "Проверка pandas:"
python -c "import pandas; print('✅ pandas доступен, версия:', pandas.__version__)" 2>/dev/null || echo "❌ pandas НЕ доступен"

# Проверяем openpyxl
echo "Проверка openpyxl:"
python -c "import openpyxl; print('✅ openpyxl доступен, версия:', openpyxl.__version__)" 2>/dev/null || echo "❌ openpyxl НЕ доступен"

echo ""
echo "=== ГОТОВО! ==="
echo "Теперь можете запускать:"
echo "  python main.py"
echo "  python generate_employee_data.py"
echo ""
echo "Для выхода из окружения используйте: deactivate"
