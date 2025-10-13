#!/bin/bash
# Скрипт для активации нового виртуального окружения

echo "=== АКТИВАЦИЯ НОВОГО ВИРТУАЛЬНОГО ОКРУЖЕНИЯ ==="
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

# Проверяем все модули
echo ""
echo "=== ПРОВЕРКА ВСЕХ МОДУЛЕЙ ==="
python -c "
import sys
print('Python путь:', sys.executable)
print('Python версия:', sys.version)
print('')

modules = [
    ('pandas', 'pd'),
    ('openpyxl', 'openpyxl'),
    ('numpy', 'np'),
    ('datetime', 'datetime'),
    ('json', 'json'),
    ('os', 'os'),
    ('sys', 'sys'),
    ('logging', 'logging'),
    ('time', 'time'),
    ('re', 're')
]

for module_name, alias in modules:
    try:
        if alias:
            exec(f'import {module_name} as {alias}')
        else:
            exec(f'import {module_name}')
        print(f'✅ {module_name} - OK')
    except ImportError as e:
        print(f'❌ {module_name} - ОШИБКА: {e}')
"

echo ""
echo "=== ГОТОВО! ==="
echo "Теперь можете запускать:"
echo "  python main.py"
echo "  python generate_employee_data.py"
echo ""
echo "Для выхода из окружения используйте: deactivate"
