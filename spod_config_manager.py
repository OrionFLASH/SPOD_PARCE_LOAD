#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPOD Configuration Manager
Веб-приложение для управления настройками SPOD с визуальным интерфейсом
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import re

# Добавляем путь к основному модулю для импорта конфигураций
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from main import (
        INPUT_FILES, MERGE_FIELDS, MERGE_FIELDS_ADVANCED, 
        CHECK_DUPLICATES, COLOR_SCHEME, SUMMARY_SHEET,
        JSON_COLUMNS, FIELD_LENGTH_VALIDATIONS
    )
except ImportError as e:
    print(f"Ошибка импорта конфигураций: {e}")
    # Создаем пустые конфигурации если не удалось импортировать
    INPUT_FILES = []
    MERGE_FIELDS = []
    MERGE_FIELDS_ADVANCED = []
    CHECK_DUPLICATES = []
    COLOR_SCHEME = []
    SUMMARY_SHEET = {}
    JSON_COLUMNS = {}
    FIELD_LENGTH_VALIDATIONS = {}

from flask import Flask, render_template, request, jsonify, send_file
import tempfile

app = Flask(__name__)

class SPODConfigManager:
    """Менеджер конфигураций SPOD"""
    
    def __init__(self):
        self.config = {
            'input_files': INPUT_FILES.copy(),
            'merge_fields': MERGE_FIELDS.copy(),
            'merge_fields_advanced': MERGE_FIELDS_ADVANCED.copy(),
            'check_duplicates': CHECK_DUPLICATES.copy(),
            'color_scheme': COLOR_SCHEME.copy(),
            'summary_sheet': SUMMARY_SHEET.copy(),
            'json_columns': JSON_COLUMNS.copy(),
            'field_length_validations': FIELD_LENGTH_VALIDATIONS.copy()
        }
        
        # Справочники для валидации
        self.validators = {
            'col_width_mode': ['AUTO', 'FIXED', 'MIN', 'MAX'],
            'mode': ['value', 'count'],
            'multiply_rows': [True, False],
            'status_values': ['АКТИВНЫЙ', 'ЗАВЕРШЕН', 'ОТМЕНЕН', 'УДАЛЕН', 'ПОДВЕДЕНИЕ ИТОГОВ'],
            'aggregate_functions': ['sum', 'count', 'avg', 'max', 'min', 'first', 'last'],
            'operators': ['=', '<=', '>=', '<', '>', '!=', 'in', 'not in']
        }
    
    def get_config(self) -> Dict[str, Any]:
        """Получить текущую конфигурацию"""
        return self.config
    
    def update_config(self, section: str, data: Any) -> bool:
        """Обновить секцию конфигурации"""
        try:
            if section in self.config:
                self.config[section] = data
                return True
            return False
        except Exception as e:
            print(f"Ошибка обновления конфигурации: {e}")
            return False
    
    def validate_input_file(self, file_config: Dict[str, Any]) -> List[str]:
        """Валидация конфигурации файла"""
        errors = []
        
        required_fields = ['file', 'sheet', 'max_col_width', 'freeze']
        for field in required_fields:
            if field not in file_config:
                errors.append(f"Отсутствует обязательное поле: {field}")
        
        if 'col_width_mode' in file_config:
            if file_config['col_width_mode'] not in self.validators['col_width_mode']:
                errors.append(f"Некорректный col_width_mode: {file_config['col_width_mode']}")
        
        if 'max_col_width' in file_config:
            try:
                width = int(file_config['max_col_width'])
                if width <= 0:
                    errors.append("max_col_width должен быть положительным числом")
            except (ValueError, TypeError):
                errors.append("max_col_width должен быть числом")
        
        return errors
    
    def validate_merge_field(self, merge_config: Dict[str, Any]) -> List[str]:
        """Валидация правила объединения"""
        errors = []
        
        required_fields = ['sheet_src', 'sheet_dst', 'src_key', 'dst_key', 'column']
        for field in required_fields:
            if field not in merge_config:
                errors.append(f"Отсутствует обязательное поле: {field}")
        
        if 'mode' in merge_config:
            if merge_config['mode'] not in self.validators['mode']:
                errors.append(f"Некорректный mode: {merge_config['mode']}")
        
        if 'multiply_rows' in merge_config:
            if not isinstance(merge_config['multiply_rows'], bool):
                errors.append("multiply_rows должен быть boolean")
        
        # Проверка статусов в status_filters
        if 'status_filters' in merge_config and merge_config['status_filters']:
            for column, values in merge_config['status_filters'].items():
                if not isinstance(values, list):
                    errors.append(f"status_filters.{column} должен быть списком")
                else:
                    for value in values:
                        if value not in self.validators['status_values']:
                            errors.append(f"Некорректный статус: {value}")
        
        # Проверка aggregate функций
        if 'aggregate' in merge_config and merge_config['aggregate']:
            for column, func in merge_config['aggregate'].items():
                if func not in self.validators['aggregate_functions']:
                    errors.append(f"Некорректная функция агрегации: {func}")
        
        return errors
    
    def get_available_sheets(self) -> List[str]:
        """Получить список доступных листов"""
        sheets = set()
        for file_config in self.config['input_files']:
            if 'sheet' in file_config:
                sheets.add(file_config['sheet'])
        return sorted(list(sheets))
    
    def get_available_columns(self, sheet: str) -> List[str]:
        """Получить список доступных колонок для листа (заглушка)"""
        # В реальном приложении здесь бы парсились CSV файлы
        common_columns = [
            'CONTEST_CODE', 'TOURNAMENT_CODE', 'REWARD_CODE', 'GROUP_CODE',
            'EMPLOYEE_CODE', 'FULL_NAME', 'BUSINESS_STATUS', 'TOURNAMENT_STATUS',
            'CONTEST_TYPE', 'BUSINESS_BLOCK', 'START_DT', 'END_DT', 'RESULT_DT'
        ]
        return common_columns
    
    def export_config(self) -> str:
        """Экспортировать конфигурацию в Python код"""
        config_code = f"""# === КОНФИГУРАЦИЯ SPOD (Сгенерировано через Config Manager) ===

# Входные файлы
INPUT_FILES = {json.dumps(self.config['input_files'], indent=4, ensure_ascii=False)}

# Правила объединения
MERGE_FIELDS = {json.dumps(self.config['merge_fields'], indent=4, ensure_ascii=False)}

# Расширенные правила объединения
MERGE_FIELDS_ADVANCED = {json.dumps(self.config['merge_fields_advanced'], indent=4, ensure_ascii=False)}

# Проверка дублей
CHECK_DUPLICATES = {json.dumps(self.config['check_duplicates'], indent=4, ensure_ascii=False)}

# Цветовая схема
COLOR_SCHEME = {json.dumps(self.config['color_scheme'], indent=4, ensure_ascii=False)}

# Сводный лист
SUMMARY_SHEET = {json.dumps(self.config['summary_sheet'], indent=4, ensure_ascii=False)}

# JSON колонки
JSON_COLUMNS = {json.dumps(self.config['json_columns'], indent=4, ensure_ascii=False)}

# Валидация длины полей
FIELD_LENGTH_VALIDATIONS = {json.dumps(self.config['field_length_validations'], indent=4, ensure_ascii=False)}
"""
        return config_code

# Глобальный экземпляр менеджера
config_manager = SPODConfigManager()

@app.route('/')
def index():
    """Главная страница"""
    return render_template('index.html')

@app.route('/api/config')
def get_config():
    """API: Получить конфигурацию"""
    return jsonify(config_manager.get_config())

@app.route('/api/validators')
def get_validators():
    """API: Получить справочники для валидации"""
    return jsonify(config_manager.validators)

@app.route('/api/sheets')
def get_sheets():
    """API: Получить список листов"""
    return jsonify(config_manager.get_available_sheets())

@app.route('/api/columns/<sheet>')
def get_columns(sheet):
    """API: Получить колонки для листа"""
    return jsonify(config_manager.get_available_columns(sheet))

@app.route('/api/config/<section>', methods=['POST'])
def update_section(section):
    """API: Обновить секцию конфигурации"""
    data = request.get_json()
    
    if config_manager.update_config(section, data):
        return jsonify({'success': True})
    else:
        return jsonify({'success': False, 'error': 'Неизвестная секция'}), 400

@app.route('/api/validate/<section>', methods=['POST'])
def validate_section(section):
    """API: Валидировать секцию"""
    data = request.get_json()
    errors = []
    
    if section == 'input_files':
        for item in data:
            errors.extend(config_manager.validate_input_file(item))
    elif section in ['merge_fields', 'merge_fields_advanced']:
        for item in data:
            errors.extend(config_manager.validate_merge_field(item))
    
    return jsonify({'valid': len(errors) == 0, 'errors': errors})

@app.route('/api/export')
def export_config():
    """API: Экспортировать конфигурацию"""
    config_code = config_manager.export_config()
    
    # Создаем временный файл
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
        f.write(config_code)
        temp_path = f.name
    
    return send_file(temp_path, as_attachment=True, download_name='spod_config.py')

@app.route('/api/import', methods=['POST'])
def import_config():
    """API: Импортировать конфигурацию из файла"""
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'Файл не найден'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'Файл не выбран'}), 400
    
    try:
        # Читаем содержимое файла
        content = file.read().decode('utf-8')
        
        # Парсим Python код (упрощенная версия)
        # В реальном приложении здесь бы был более сложный парсер
        config_data = {}
        
        # Ищем переменные в коде
        patterns = {
            'INPUT_FILES': r'INPUT_FILES\s*=\s*(\[.*?\])',
            'MERGE_FIELDS': r'MERGE_FIELDS\s*=\s*(\[.*?\])',
            'MERGE_FIELDS_ADVANCED': r'MERGE_FIELDS_ADVANCED\s*=\s*(\[.*?\])',
            'CHECK_DUPLICATES': r'CHECK_DUPLICATES\s*=\s*(\[.*?\])',
            'COLOR_SCHEME': r'COLOR_SCHEME\s*=\s*(\[.*?\])',
            'SUMMARY_SHEET': r'SUMMARY_SHEET\s*=\s*(\{.*?\})',
            'JSON_COLUMNS': r'JSON_COLUMNS\s*=\s*(\{.*?\})',
            'FIELD_LENGTH_VALIDATIONS': r'FIELD_LENGTH_VALIDATIONS\s*=\s*(\{.*?\})'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, content, re.DOTALL)
            if match:
                try:
                    config_data[key.lower()] = eval(match.group(1))
                except:
                    pass
        
        # Обновляем конфигурацию
        for key, value in config_data.items():
            config_manager.update_config(key, value)
        
        return jsonify({'success': True, 'imported': list(config_data.keys())})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

if __name__ == '__main__':
    # Создаем папку для шаблонов если её нет
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    
    print("🚀 Запуск SPOD Configuration Manager...")
    print("📱 Откройте браузер и перейдите по адресу: http://localhost:5000")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
