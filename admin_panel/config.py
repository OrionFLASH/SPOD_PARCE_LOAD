# Конфигурация файлов данных
# Имена файлов автоматически извлекаются из INPUT_FILES в main.py во время выполнения

import sys
import os
import ast
import re

# Путь к main.py (на уровень выше)
MAIN_PY_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'main.py')

def get_file_names_from_main():
    """Извлекает FILE_NAMES из INPUT_FILES в main.py"""
    try:
        with open(MAIN_PY_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Находим начало INPUT_FILES
        start_line = None
        for i, line in enumerate(lines):
            if 'INPUT_FILES' in line and '=' in line:
                start_line = i
                break
        
        if start_line is None:
            raise ValueError("INPUT_FILES не найден в main.py")
        
        # Находим конец списка
        bracket_count = 0
        end_line = None
        for i in range(start_line, len(lines)):
            line = lines[i]
            bracket_count += line.count('[') - line.count(']')
            if bracket_count == 0 and i > start_line:
                end_line = i + 1
                break
        
        if end_line is None:
            raise ValueError("Не удалось найти конец INPUT_FILES в main.py")
        
        # Извлекаем блок
        input_files_block = ''.join(lines[start_line:end_line])
        
        # Парсим как Python код
        match = re.search(r'INPUT_FILES\s*=\s*(\[.*\])', input_files_block, re.DOTALL)
        if not match:
            raise ValueError("Не удалось извлечь список из INPUT_FILES")
        
        list_str = match.group(1)
        input_files = ast.literal_eval(list_str)
        
        # Преобразуем в словарь {sheet: filename}
        file_names = {}
        for item in input_files:
            if isinstance(item, dict) and 'file' in item and 'sheet' in item:
                filename = item['file']
                sheet = item['sheet']
                # Добавляем .csv если нет расширения
                if not filename.endswith('.csv'):
                    filename = filename + '.csv'
                file_names[sheet] = filename
        
        return file_names
    except Exception as e:
        print(f"Ошибка чтения INPUT_FILES из main.py: {e}")
        # Возвращаем пустой словарь или значения по умолчанию
        return {}

# Извлекаем имена файлов из main.py
FILE_NAMES = get_file_names_from_main()

# Каталоги
DIR_INPUT = "SPOD"
DIR_EDIT = "EDIT"
DIR_BACKUP = "BACKUP"

# Разделитель CSV
CSV_SEPARATOR = ";"

# Кодировка
CSV_ENCODING = "utf-8"

# Структура JSON полей

# Зависимости полей в JSON (для автодополнения)
JSON_FIELD_DEPENDENCIES = {
    "REWARD": {
        "REWARD_ADD_DATA": {
            "depends_on": "REWARD_TYPE",
            "fields_by_type": {
                # Будет заполняться динамически при анализе данных
            }
        }
    },
    "CONTEST-DATA": {
        "CONTEST_FEATURE": {
            "common_fields": ["feature", "vid", "momentRewarding", "minNumber", "capacity", "accuracy", "masking"]
        }
    }
}

JSON_FIELDS = {
    "CONTEST-DATA": ["CONTEST_FEATURE", "CONTEST_PERIOD", "BUSINESS_BLOCK"],
    "TOURNAMENT-SCHEDULE": ["TARGET_TYPE", "FILTER_PERIOD_ARR"],
    "INDICATOR": ["INDICATOR_FILTER"],
    "GROUP": ["GROUP_VALUE"],
    "REWARD": ["REWARD_ADD_DATA"]
}

# Поля со списками значений (не JSON, а разделенные значения)
MULTI_VALUE_FIELDS = {
    "REWARD-LINK": {
        "GROUP_CODE": {
            "separator": ",",  # Разделитель значений
            "source_file": "GROUP",  # Файл для получения возможных значений
            "source_field": "GROUP_CODE"  # Поле в исходном файле
        }
    }
}


# Связи между файлами (для каскадных операций)
FILE_DEPENDENCIES = {
    "CONTEST-DATA": ["GROUP", "INDICATOR", "TOURNAMENT-SCHEDULE", "REWARD-LINK", "REPORT"],
    "GROUP": ["REWARD-LINK"],
    "TOURNAMENT-SCHEDULE": ["REPORT"],
    "REWARD": ["REWARD-LINK"]
}

# Внешние ключи
FOREIGN_KEYS = {
    "GROUP": {"CONTEST_CODE": "CONTEST-DATA"},
    "INDICATOR": {"CONTEST_CODE": "CONTEST-DATA"},
    "TOURNAMENT-SCHEDULE": {"CONTEST_CODE": "CONTEST-DATA"},
    "REPORT": {
        "CONTEST_CODE": "CONTEST-DATA",
        "TOURNAMENT_CODE": "TOURNAMENT-SCHEDULE"
    },
    "REWARD-LINK": {
        "CONTEST_CODE": "CONTEST-DATA",
        "GROUP_CODE": "GROUP",
        "REWARD_CODE": "REWARD"
    }
}

# Уникальные ключи
UNIQUE_KEYS = {
    "CONTEST-DATA": ["CONTEST_CODE"],
    "REWARD": ["REWARD_CODE"],
    "GROUP": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"],
    "INDICATOR": ["CONTEST_CODE", "INDICATOR_ADD_CALC_TYPE"],
    "REPORT": ["MANAGER_PERSON_NUMBER", "TOURNAMENT_CODE", "CONTEST_CODE"]
}

# Списки значений
FIXED_LISTS = {
    "CONTEST-DATA": {
        "BUSINESS_STATUS": ["АКТИВНЫЙ", "АРХИВНЫЙ"],
        "CONTEST_TYPE": ["ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ", "ТУРНИРНЫЙ", "ИНДИВИДУАЛЬНЫЙ"],
        "TARGET_TYPE": ["ПРОМ", "ТЕСТ"],
        "CALC_TYPE": ["1", "0"]
    },
    "REWARD": {
        "REWARD_TYPE": ["ITEM", "BADGE", "LABEL", "CRYSTAL"]
    },
    "TOURNAMENT-SCHEDULE": {
        "TOURNAMENT_STATUS": ["УДАЛЕН", "ЗАВЕРШЕН", "АКТИВНЫЙ", "ОТМЕНЕН", "ПОДВЕДЕНИЕ ИТОГОВ"],
        "CALC_TYPE": ["1", "3", "0"]
    },
    "GROUP": {
        "GET_CALC_METHOD": ["1", "2", "3"]
    }
}
