#!/bin/bash
# Скрипт запуска админ-панели

echo "Запуск SPOD Admin Panel..."
echo ""

# Проверяем наличие виртуального окружения
if [ ! -d "venv" ]; then
    echo "Создание виртуального окружения..."
    python3 -m venv venv
fi

# Активируем виртуальное окружение
source venv/bin/activate

# Устанавливаем зависимости
echo "Установка зависимостей..."
pip install -q -r requirements.txt

# Создаем необходимые каталоги
mkdir -p ../EDIT
mkdir -p ../BACKUP

# Запускаем приложение
echo ""
echo "Админ-панель запущена!"
echo "Откройте в браузере: http://localhost:5000"
echo ""
python app.py
