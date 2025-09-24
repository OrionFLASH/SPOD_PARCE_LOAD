#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Демонстрация SPOD Configuration Manager
"""

import json
import os
from spod_config_manager import SPODConfigManager

def demo_config_manager():
    """Демонстрация возможностей конфигурационного менеджера"""
    print("🎯 Демонстрация SPOD Configuration Manager")
    print("=" * 60)
    
    # Создаем экземпляр менеджера
    manager = SPODConfigManager()
    
    # Получаем текущую конфигурацию
    config = manager.get_config()
    
    print("📊 Текущая конфигурация:")
    print(f"  • Входных файлов: {len(config['input_files'])}")
    print(f"  • Правил объединения: {len(config['merge_fields'])}")
    print(f"  • Расширенных правил: {len(config['merge_fields_advanced'])}")
    print(f"  • Проверок дублей: {len(config['check_duplicates'])}")
    print(f"  • Цветовых схем: {len(config['color_scheme'])}")
    print(f"  • JSON колонок: {len(config['json_columns'])}")
    print(f"  • Валидаций полей: {len(config['field_length_validations'])}")
    
    print("\n🔍 Доступные листы:")
    sheets = manager.get_available_sheets()
    for sheet in sheets:
        print(f"  • {sheet}")
    
    print("\n📝 Пример входного файла:")
    if config['input_files']:
        example_file = config['input_files'][0]
        print(f"  • Файл: {example_file.get('file', 'Не указан')}")
        print(f"  • Лист: {example_file.get('sheet', 'Не указан')}")
        print(f"  • Ширина: {example_file.get('max_col_width', 'Не указана')}")
        print(f"  • Закрепление: {example_file.get('freeze', 'Не указано')}")
    
    print("\n🔗 Пример правила объединения:")
    if config['merge_fields']:
        example_merge = config['merge_fields'][0]
        print(f"  • Источник: {example_merge.get('sheet_src', 'Не указан')}")
        print(f"  • Цель: {example_merge.get('sheet_dst', 'Не указан')}")
        print(f"  • Ключи: {example_merge.get('src_key', 'Не указаны')} → {example_merge.get('dst_key', 'Не указаны')}")
        print(f"  • Колонки: {example_merge.get('column', 'Не указаны')}")
        print(f"  • Режим: {example_merge.get('mode', 'value')}")
    
    print("\n🎨 Справочники для валидации:")
    validators = manager.validators
    for key, values in validators.items():
        print(f"  • {key}: {values}")
    
    print("\n📤 Экспорт конфигурации:")
    try:
        config_code = manager.export_config()
        print(f"  • Сгенерировано {len(config_code)} символов кода")
        print(f"  • Содержит {config_code.count('INPUT_FILES')} секций конфигурации")
    except Exception as e:
        print(f"  ❌ Ошибка экспорта: {e}")
    
    print("\n✅ Демонстрация завершена!")
    print("\n🚀 Для запуска веб-интерфейса выполните:")
    print("   python run_config_manager.py")
    print("   или")
    print("   python spod_config_manager.py")

if __name__ == "__main__":
    demo_config_manager()
