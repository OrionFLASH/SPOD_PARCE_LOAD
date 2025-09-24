#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт запуска SPOD Configuration Manager
"""

import os
import sys
import subprocess
from pathlib import Path

def check_dependencies():
    """Проверка зависимостей"""
    try:
        import flask
        import pandas
        import openpyxl
        print("✅ Все зависимости установлены")
        return True
    except ImportError as e:
        print(f"❌ Отсутствует зависимость: {e}")
        print("📦 Установите зависимости: pip install -r requirements_config_manager.txt")
        return False

def install_dependencies():
    """Установка зависимостей"""
    print("📦 Установка зависимостей...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements_config_manager.txt"])
        print("✅ Зависимости установлены")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка установки зависимостей: {e}")
        return False

def main():
    """Главная функция"""
    print("🚀 SPOD Configuration Manager")
    print("=" * 50)
    
    # Проверяем наличие файлов
    if not os.path.exists("spod_config_manager.py"):
        print("❌ Файл spod_config_manager.py не найден")
        return
    
    if not os.path.exists("templates/index.html"):
        print("❌ Файл templates/index.html не найден")
        return
    
    # Проверяем зависимости
    if not check_dependencies():
        print("\n🔧 Попытка автоматической установки зависимостей...")
        if not install_dependencies():
            print("❌ Не удалось установить зависимости автоматически")
            print("💡 Установите вручную: pip install -r requirements_config_manager.txt")
            return
        
        # Повторная проверка
        if not check_dependencies():
            print("❌ Зависимости не установлены")
            return
    
    print("\n🌐 Запуск веб-сервера...")
    print("📱 Откройте браузер и перейдите по адресу: http://localhost:5000")
    print("⏹️  Для остановки нажмите Ctrl+C")
    print("=" * 50)
    
    try:
        # Запускаем приложение
        from spod_config_manager import app
        app.run(debug=True, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        print("\n👋 Приложение остановлено")
    except Exception as e:
        print(f"❌ Ошибка запуска: {e}")

if __name__ == "__main__":
    main()
