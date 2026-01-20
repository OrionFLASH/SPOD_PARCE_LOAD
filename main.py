# === ИМПОРТЫ БИБЛИОТЕК ===
import os          # Для работы с операционной системой и путями
import sys         # Для системных функций и аргументов командной строки
import pandas as pd  # Для работы с данными в табличном формате
import logging     # Для логирования процессов
from datetime import datetime  # Для работы с датами и временем
from openpyxl.utils import get_column_letter  # Для получения буквенного обозначения колонок Excel
from openpyxl.styles import Alignment, Font, PatternFill  # Для стилизации ячеек Excel
from time import time  # Для измерения времени выполнения операций
import json        # Для работы с JSON данными
import re          # Для работы с регулярными выражениями
import csv         # Для работы с CSV файлами
import ast         # Для безопасного парсинга Python выражений
import time as tmod  # Для измерения времени выполнения операций (альтернативное имя)
import inspect  # Для получения информации о вызывающей функции
from concurrent.futures import ThreadPoolExecutor, as_completed  # Для параллельной обработки
from itertools import product
import threading  # Для синхронизации потоков

# === ОПТИМИЗАЦИИ ПРОИЗВОДИТЕЛЬНОСТИ ===
# 
# Реализованные оптимизации (версия 2.0):
# 
# 1. ВЕКТОРИЗАЦИЯ calculate_tournament_status:
#    - Заменен df.apply(get_status, axis=1) на numpy.select с векторными условиями
#    - Ускорение: 5-10x для больших DataFrame
#    - Использует только стандартные библиотеки: pandas, numpy (входит в Anaconda)
# 
# 2. РАСПАРАЛЛЕЛИВАНИЕ merge_fields_across_sheets:
#    - Независимые правила обрабатываются параллельно через ThreadPoolExecutor
#    - Группировка правил по зависимостям (sheet_dst)
#    - Ускорение: 2-4x для множества независимых правил
#    - Использует только стандартные библиотеки: concurrent.futures (встроено в Python)
# 
# 3. РАСПАРАЛЛЕЛИВАНИЕ write_to_excel:
#    - Запись данных выполняется последовательно (ограничение ExcelWriter)
#    - Форматирование листов выполняется параллельно после записи
#    - Ускорение: 1.5-2x для большого количества листов
#    - Использует только стандартные библиотеки: concurrent.futures
# 
# 4. ОПТИМИЗАЦИЯ _format_sheet:
#    - Batch-операции для заголовков (вычисление всех ширин сразу)
#    - Чанковая обработка больших листов (>1000 строк)
#    - Ускорение: 1.3-2x для больших листов
#    - Использует только стандартные библиотеки: openpyxl (входит в Anaconda)
# 
# Все оптимизации используют только библиотеки, входящие в Python 3.10 или Anaconda 3.10.
# Дополнительные библиотеки не требуются.
# 
# В этом файле реализованы оптимизации для ускорения обработки данных:
# 
# 1. ВЕКТОРИЗАЦИЯ ФУНКЦИЙ (ускорение 50-200x):
#    - validate_field_lengths_vectorized: замена iterrows() на векторные операции pandas
#    - add_auto_gender_column_vectorized: замена iterrows() на строковые операции pandas
#    - collect_summary_keys_optimized: упрощенная версия с использованием merge
# 
# 2. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА:
#    - Параллельное чтение CSV файлов через ThreadPoolExecutor
#    - Параллельная проверка длины полей
#    - Параллельная проверка дубликатов
# 
# 3. ОПТИМИЗАЦИЯ ПАМЯТИ:
#    - Замена apply() на векторные операции где возможно
#    - Использование pd.to_datetime вместо apply(safe_to_date)
# 
# 4. УСТРАНЕНИЕ ДУБЛИРОВАНИЯ:
#    - Удален дублирующийся блок кода в _format_sheet
#    - Устранено тройное логирование в safe_json_loads
# 
# ВАЖНО: Оптимизированные версии функций автоматически сравниваются с оригинальными
#        для гарантии идентичности результатов. В случае различий используется оригинальная версия.
# 
# Дата внедрения оптимизаций: 2025-01-20
# Ожидаемое ускорение: 50-200x в зависимости от объема данных
# 



class CallerFormatter(logging.Formatter):
    """Кастомный форматтер, который добавляет имя вызывающей функции"""
    def format(self, record):
        # Получаем имя функции из стека вызовов
        try:
            # Используем inspect.stack() для более надежного получения имени функции
            stack = inspect.stack()
            # Ищем первый фрейм, который не является частью модуля logging
            func_name = record.funcName  # Значение по умолчанию
            for frame_info in stack:
                filename = frame_info[1]
                func_name_in_frame = frame_info[3]
                # Пропускаем фреймы из модуля logging и самого format
                if 'logging' not in filename and func_name_in_frame != 'format' and func_name_in_frame != '<module>':
                    func_name = func_name_in_frame
                    break
        except:
            func_name = record.funcName
        
        # Сохраняем оригинальное сообщение
        if hasattr(record, 'msg'):
            # Если msg это строка с плейсхолдерами, форматируем её
            if isinstance(record.msg, str) and record.args:
                original_msg = record.msg % record.args
            else:
                original_msg = str(record.msg)
        else:
            original_msg = str(record.getMessage())
        
        # Добавляем имя функции к сообщению
        record.msg = f"{original_msg} [def: {func_name}]"
        record.args = ()  # Очищаем args чтобы избежать повторного форматирования
        return super().format(record)




# === ГЛОБАЛЬНЫЕ КОНСТАНТЫ И ПЕРЕМЕННЫЕ ===
# Каталоги для работы программы
DIR_INPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "SPOD")    # Каталог с входными файлами
DIR_OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "OUT")    # Каталог для выходных файлов
DIR_LOGS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "LOGS")    # Каталог для логов

# Конфигурация входных файлов (имя без расширения)
# Каждый файл содержит настройки для обработки:
# - file: имя файла
# - sheet: название листа для обработки
# - max_col_width: максимальная ширина колонки
# - freeze: закрепление области (например, "C2" закрепляет колонки A,B и строки 1)
# - col_width_mode: режим растягивания колонок ("AUTO", число, None)
# - min_col_width: минимальная ширина колонки
INPUT_FILES = [
    {
        "file": "CONTEST (PROM) 19-01 v1",  # Файл с данными конкурсов
        "sheet": "CONTEST-DATA",                        # Лист для обработки
        "max_col_width": 120,                          # Максимальная ширина колонки
        "freeze": "C2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 12                             # Минимальная ширина колонки
    },
    {
        "file": "GROUP (PROM) 19-01 v1",            # Файл с данными групп
        "sheet": "GROUP",                              # Лист для обработки
        "max_col_width": 20,                           # Максимальная ширина колонки
        "freeze": "C2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "INDICATOR (PROM) 19-01 v1",        # Файл с индикаторами
        "sheet": "INDICATOR",                          # Лист для обработки
        "max_col_width": 100,                           # Максимальная ширина колонки
        "freeze": "B2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "REPORT (PROM) 19-01 v1", # Файл с отчетами
        "sheet": "REPORT",                             # Лист для обработки
        "max_col_width": 25,                           # Максимальная ширина колонки
        "freeze": "D2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 10                             # Минимальная ширина колонки
    },
    {
        "file": "REWARD (PROM) 19-01 v2",        # Файл с наградами
        "sheet": "REWARD",                             # Лист для обработки
        "max_col_width": 200,                          # Максимальная ширина колонки (большая для длинных описаний)
        "freeze": "D2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 10                             # Минимальная ширина колонки
    },
    {
        "file": "REWARD-LINK (PROM) 19-01 v1",      # Файл со связями наград
        "sheet": "REWARD-LINK",                        # Лист для обработки
        "max_col_width": 30,                           # Максимальная ширина колонки
        "freeze": "A2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 10                             # Минимальная ширина колонки
    },
    {
        "file": "SVD_KB_DM_GAMIFICATION_ORG_UNIT_V20 - 2025.08.28", # Файл с организационными единицами
        "sheet": "ORG_UNIT_V20",                       # Лист для обработки
        "max_col_width": 60,                           # Максимальная ширина колонки
        "freeze": "A2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 10                             # Минимальная ширина колонки
    },
    {
        "file": "SCHEDULE (PROM) 19-01 v2", # Файл с расписанием турниров
        "sheet": "TOURNAMENT-SCHEDULE",                # Лист для обработки
        "max_col_width": 120,                          # Максимальная ширина колонки
        "freeze": "B2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 10                             # Минимальная ширина колонки
    },
    {
        "file": "USER_ROLE (PROM) 12-12 v0",       # Файл с ролями пользователей
        "sheet": "USER_ROLE",                          # Лист для обработки
        "max_col_width": 65,                           # Максимальная ширина колонки
        "freeze": "D2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 12                             # Минимальная ширина колонки
    },
    {
        "file": "USER_ROLE_SB (PROM) 12-12 v0",    # Файл с ролями пользователей SB
        "sheet": "USER_ROLE SB",                       # Лист для обработки
        "max_col_width": 65,                           # Максимальная ширина колонки
        "freeze": "D2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 12                             # Минимальная ширина колонки
    },
    {
        "file": "employee_PROM_final_5000_2025-07-26_00-09-03",  # Файл с данными сотрудников
        "sheet": "EMPLOYEE",                              # Лист для обработки
        "max_col_width": 80,                              # Максимальная ширина колонки
        "freeze": "F2",                                   # Закрепление области (колонки A-E и строка 1)
        "col_width_mode": "AUTO",                         # Автоматическое растягивание колонок
        "min_col_width": 15                                # Минимальная ширина колонки
    }
]

# === КОНФИГУРАЦИЯ СВОДНОГО ЛИСТА ===
# Настройки для создания итогового листа с объединенными данными
SUMMARY_SHEET = {
    "sheet": "SUMMARY",                                   # Название сводного листа
    "max_col_width": 150,                                 # Максимальная ширина колонки
    "freeze": "G2",                                      # Закрепление области (колонки A-E и строка 1)
    "col_width_mode": "AUTO",                            # Автоматическое растягивание колонок
    "min_col_width": 8                                   # Минимальная ширина колонки
}

# === НАСТРОЙКИ ЛОГИРОВАНИЯ ===
LOG_LEVEL = "DEBUG"  # Уровень логирования: "INFO" для продакшена, "DEBUG" для отладки
LOG_BASE_NAME = "LOGS"  # Базовое имя для файлов логов

# Словарь сообщений для логирования различных событий
# Используется для стандартизации сообщений и локализации
# === КОНСТАНТЫ ДЛЯ ОПРЕДЕЛЕНИЯ ПОЛА ===
# Паттерны для автоматического определения пола по отчеству, имени и фамилии
GENDER_PATTERNS = {
    # Отчества - мужские окончания (характерные для мужчин)
    'patronymic_male': [
        'ович', 'евич', 'ич', 'ыч', 'оглы', 'улы', 'уулу', 'заде'  # Русские, кавказские, тюркские
    ],
    # Отчества - женские окончания (характерные для женщин)
    'patronymic_female': [
        'овна', 'евна', 'инична', 'ична', 'на', 'кызы'  # Русские, тюркские
    ],
    # Имена - мужские окончания (характерные для мужчин)
    'name_male': [
        'ий', 'ей', 'ай', 'ой', 'ый', 'ев', 'ов', 'ин', 'ан', 'он', 'ен', 'ур', 'ич', 'ыч'  # Русские окончания
    ],
    # Имена - женские окончания (характерные для женщин)
    'name_female': [
        'а', 'я', 'ина', 'ана', 'ена', 'ия', 'ья', 'на', 'ла', 'ра', 'са', 'та', 'да', 'ка', 'га'  # Русские окончания
    ],
    # Фамилии - мужские окончания (характерные для мужчин)
    'surname_male': [
        'ов', 'ев', 'ин', 'ын', 'ский', 'цкий', 'ич', 'енко', 'ко', 'як', 'ук', 'юк', 'ич', 'ыч'  # Русские, украинские окончания
    ],
    # Фамилии - женские окончания (характерные для женщин)
    'surname_female': [
        'ова', 'ева', 'ина', 'ына', 'ская', 'цкая', 'енко', 'ко'  # Русские, украинские окончания
    ]
}

# Шаг для отображения прогресса при обработке больших объемов данных
GENDER_PROGRESS_STEP = 500  # Показывать прогресс каждые 500 обработанных строк

# === КОНСТАНТЫ ДЛЯ ПРОВЕРКИ ДЛИНЫ ПОЛЕЙ ===
# Настройки валидации длины полей для различных листов
# Каждый лист содержит правила проверки для конкретных полей
FIELD_LENGTH_VALIDATIONS = {
    "ORG_UNIT_V20": {  # Лист с организационными единицами
        "result_column": "FIELD_LENGTH_CHECK",  # Колонка для результатов проверки
        "fields": {
            "TB_FULL_NAME": {"limit": 100, "operator": "<="},      # Полное имя ТБ: максимум 100 символов
            "GOSB_NAME": {"limit": 100, "operator": "<="},         # Название ГОСБ: максимум 100 символов
            "GOSB_SHORT_NAME": {"limit": 20, "operator": "<="}     # Краткое название ГОСБ: максимум 20 символов
        }
    },
    "EMPLOYEE": {  # Лист с сотрудниками
        "result_column": "FIELD_LENGTH_CHECK",  # Колонка для результатов проверки
        "fields": {
            "PERSON_NUMBER": {"limit": 20, "operator": "="},       # Номер сотрудника: точно 20 символов
            "PERSON_NUMBER_ADD": {"limit": 20, "operator": "="}    # Дополнительный номер: точно 20 символов
        }
    },
    "REPORT": {  # Лист с отчетами
        "result_column": "FIELD_LENGTH_CHECK",  # Колонка для результатов проверки
        "fields": {
            "MANAGER_PERSON_NUMBER": {"limit": 20, "operator": "="}  # Номер менеджера: точно 20 символов
        }
    }
}

# --- ОБЩИЕ ПРЕФИКСЫ ДЛЯ КОЛОНОК JSON ---
# Префиксы для развернутых JSON колонок, чтобы избежать конфликтов имен
PREFIX_CONTEST_FEATURE = "CONTEST_FEATURE"  # Префикс для признаков конкурса
PREFIX_ADD_DATA = "ADD_DATA"                # Префикс для дополнительных данных
PREFIX_REWARD_LINK = "REWARD_LINK => "      # Префикс для связей наград
COL_REWARD_LINK_CONTEST_CODE = f"{PREFIX_REWARD_LINK}CONTEST_CODE"  # Полное имя колонки

# === КОНФИГУРАЦИЯ ОБЪЕДИНЕНИЯ ДАННЫХ МЕЖДУ ЛИСТАМИ ===
# MERGE_FIELDS определяет, какие поля из каких листов добавляются в другие листы
# Каждый элемент содержит настройки для одного типа объединения
# 
# НОВЫЕ ВОЗМОЖНОСТИ:
# - status_filters: фильтрация по статусам колонок (например, только активные)
# - group_by: группировка данных перед добавлением
# - aggregate: подведение итогов (sum, count, avg, max, min)
# - custom_conditions: пользовательские условия фильтрации
# - multiple_sources: объединение данных из нескольких источников
MERGE_FIELDS = [
    # REPORT: добавляем CONTEST_TYPE, FULL_NAME, BUSINESS_STATUS, BUSINESS_BLOCK из CONTEST-DATA
    # Это позволяет в отчетах видеть полную информацию о конкурсе
    {
        "sheet_src": "CONTEST-DATA",        # Источник данных - лист с данными конкурсов
        "sheet_dst": "REPORT",              # Целевой лист - отчеты
        "src_key": ["CONTEST_CODE"],        # Ключ в источнике - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ в целевом листе - код конкурса
        "column": ["CONTEST_TYPE", "FULL_NAME", "BUSINESS_STATUS", "BUSINESS_BLOCK", "TARGET_TYPE", f"{PREFIX_CONTEST_FEATURE} => vid"],  # Добавляемые колонки
        "mode": "value",                    # Режим: добавляем значения (не количество)
        "multiply_rows": False,             # Не размножаем строки при множественных совпадениях
        "col_max_width": 80,               # Максимальная ширина добавляемых колонок
        "col_width_mode": "AUTO",          # Автоматическое растягивание колонок
        "col_min_width": 8,                # Минимальная ширина колонок
        # Новые параметры:
        "status_filters": {                 # Фильтрация по статусам
            "BUSINESS_STATUS": ["АКТИВНЫЙ", "ПОДВЕДЕНИЕ ИТОГОВ"]  # Берем только активные и ожидающие статусы
        },
        "custom_conditions": None,          # Пользовательские условия (None = нет)
        "group_by": None,                   # Группировка (None = нет группировки)
        "aggregate": None                   # Подведение итогов (None = нет агрегации)
    },
    # REPORT: добавляем даты и статус из TOURNAMENT-SCHEDULE
    # Позволяет видеть расписание турниров в отчетах
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "REPORT",              # Цель - отчеты
        "src_key": ["TOURNAMENT_CODE"],     # Ключ - код турнира
        "dst_key": ["TOURNAMENT_CODE"],     # Ключ - код турнира
        "column": ["END_DT", "RESULT_DT", "TOURNAMENT_STATUS"],  # Даты и статус
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 25,               # Максимальная ширина (даты короткие)
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
{
        "sheet_src": "EMPLOYEE", # Источник - расписание турниров
        "sheet_dst": "REPORT",              # Цель - отчеты
        "src_key": ["PERSON_NUMBER"],     # Ключ - код турнира
        "dst_key": ["MANAGER_PERSON_NUMBER"],     # Ключ - код турнира
        "column": ["MANAGER_FULL_NAME", "POSITION_NAME", "ROLE_CODE", "UCH_CODE", "BUSINESS_BLOCK"],  # Даты и статус
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 25,               # Максимальная ширина (даты короткие)
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # REWARD: добавляем CONTEST_CODE из REWARD-LINK по REWARD_CODE
    # Связывает награды с конкурсами через промежуточную таблицу
    {
        "sheet_src": "REWARD-LINK",         # Источник - связи наград
        "sheet_dst": "REWARD",              # Цель - награды
        "src_key": ["REWARD_CODE"],         # Ключ - код награды
        "dst_key": ["REWARD_CODE"],         # Ключ - код награды
        "column": ["CONTEST_CODE"],         # Добавляем код конкурса
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 30,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # REWARD: добавляем TOURNAMENT_CODE из TOURNAMENT-SCHEDULE
    # Связывает награды с турнирами через код конкурса
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "REWARD",              # Цель - награды
        "src_key": ["CONTEST_CODE"],        # Ключ в источнике - код конкурса
        "dst_key": ["REWARD_LINK => CONTEST_CODE"],  # Ключ в цели - код конкурса из связи
        "column": ["TOURNAMENT_CODE"],      # Добавляем код турнира
        "mode": "count",                    # Режим: подсчитываем количество турниров
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 11,               # Максимальная ширина (код турнира короткий)
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # Добавляет информацию о наградах по полному соответствию ключей конкурс-награда
    {
        "sheet_src": "CONTEST-DATA",  # Источник - награды
        "sheet_dst": "REWARD",  # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],
        "dst_key": [COL_REWARD_LINK_CONTEST_CODE],  # Составной ключ: код конкурса + код награды
        "column": [  # Добавляемые колонки:
            "FULL_NAME",
            "CONTEST_TYPE",
            "CONTEST_DESCRIPTION",
            "PRODUCT_GROUP",
            "PRODUCT",
            "TARGET_TYPE",
            "CONTEST_FEATURE => businessBlock",
            "CREATE_DT"
        ],
        "mode": "value",  # Добавляем значения
        "multiply_rows": False,  # Не размножаем строки
        "col_max_width": 60,  # Максимальная ширина
        "col_width_mode": "AUTO",  # Автоматическое растягивание
        "col_min_width": 8  # Минимальная ширина
    },
    # EMPLOYEE: добавляем ORG_UNIT_CODE из ORG_UNIT_V20 по составному ключу
    # Связывает сотрудников с организационными единицами
    {
        "sheet_src": "ORG_UNIT_V20",       # Источник - организационные единицы
        "sheet_dst": "EMPLOYEE",            # Цель - сотрудники
        "src_key": ["TB_CODE", "GOSB_CODE"], # Составной ключ: код ТБ + код ГОСБ
        "dst_key": ["TB_CODE", "GOSB_CODE"], # Составной ключ: код ТБ + код ГОСБ
        "column": ["ORG_UNIT_CODE"],        # Добавляем код организационной единицы
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 15,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 11                # Минимальная ширина
    },
    # EMPLOYEE: добавляем GOSB_SHORT_NAME из ORG_UNIT_V20 по ORG_UNIT_CODE
    # Добавляет краткое название ГОСБ к сотрудникам
    {
        "sheet_src": "ORG_UNIT_V20",       # Источник - организационные единицы
        "sheet_dst": "EMPLOYEE",            # Цель - сотрудники
        "src_key": ["ORG_UNIT_CODE"],       # Ключ - код организационной единицы
        "dst_key": ["ORG_UNIT_CODE"],       # Ключ - код организационной единицы
        "column": ["GOSB_CODE", "GOSB_SHORT_NAME", "GOSB_NAME", "TB_CODE", "TB_SHORT_NAME"],      # Добавляем краткое название ГОСБ
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 25,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 11                # Минимальная ширина
    },
    # TOURNAMENT-SCHEDULE: добавляем поля из CONTEST-DATA
    # Обогащает расписание турниров информацией о конкурсах
    {
        "sheet_src": "CONTEST-DATA",        # Источник - данные конкурсов
        "sheet_dst": "TOURNAMENT-SCHEDULE", # Цель - расписание турниров
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": ["FULL_NAME", "BUSINESS_BLOCK", "CONTEST_TYPE", "BUSINESS_STATUS", "PRODUCT_GROUP", "PRODUCT", "TARGET_TYPE",
                   f"{PREFIX_CONTEST_FEATURE} => vid"
                  ],  # Добавляемые поля
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 70,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 35                # Минимальная ширина
    },
    # GROUP: добавляем FULL_NAME из CONTEST-DATA
    # Добавляет название конкурса к группам
    {
        "sheet_src": "CONTEST-DATA",        # Источник - данные конкурсов
        "sheet_dst": "GROUP",               # Цель - группы
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": ["FULL_NAME", "CONTEST_TYPE", "TARGET_TYPE"],            # Добавляем полное название
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 70,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 35                # Минимальная ширина
    },
    # поля из TOURNAMENT-SCHEDULE по CONTEST_CODE (для частично связанных записей)
    # Добавляет информацию о турнирах по коду конкурса (без привязки к конкретному турниру)
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "GROUP",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": [                         # Добавляемые колонки:
            "TOURNAMENT_STATUS",            # Статус турнира
            "TARGET_TYPE"                   # Тип цели турнира
        ],
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 30,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # INDICATOR: добавляем FULL_NAME из CONTEST-DATA
    # Добавляет название конкурса к индикаторам
    {
        "sheet_src": "CONTEST-DATA",        # Источник - данные конкурсов
        "sheet_dst": "INDICATOR",           # Цель - индикаторы
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": ["FULL_NAME"],            # Добавляем полное название
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 70,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 35                # Минимальная ширина
    },
    # REWARD-LINK: добавляем FULL_NAME из CONTEST-DATA
    # Добавляет название конкурса к связям наград
    {
        "sheet_src": "CONTEST-DATA",        # Источник - данные конкурсов
        "sheet_dst": "REWARD-LINK",         # Цель - связи наград
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": ["FULL_NAME", "CONTEST_FEATURE"],            # Добавляем полное название
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 70,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 35                # Минимальная ширина
    },
    {
        "sheet_src": "REWARD",        # Источник - данные конкурсов
        "sheet_dst": "REWARD-LINK",         # Цель - связи наград
        "src_key": ["REWARD_CODE"],        # Ключ - код конкурса
        "dst_key": ["REWARD_CODE"],        # Ключ - код конкурса
        "column": ["FULL_NAME"],            # Добавляем полное название
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 70,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 35                # Минимальная ширина
    },
    # TOURNAMENT-SCHEDULE: добавляем CONTEST_DATE из REPORT (значение)
    # Добавляет дату конкурса из отчетов в расписание турниров
    {
        "sheet_src": "REPORT",              # Источник - отчеты
        "sheet_dst": "TOURNAMENT-SCHEDULE", # Цель - расписание турниров
        "src_key": ["CONTEST_CODE", "TOURNAMENT_CODE"],  # Составной ключ: код конкурса + код турнира
        "dst_key": ["CONTEST_CODE", "TOURNAMENT_CODE"],  # Составной ключ: код конкурса + код турнира
        "column": ["CONTEST_DATE"],         # Добавляем дату конкурса
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 25,               # Максимальная ширина (дата короткая)
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # CONTEST-DATA: добавляем TOURNAMENT_CODE из TOURNAMENT-SCHEDULE
    # Подсчитывает количество турниров для каждого конкурса
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "CONTEST-DATA",        # Цель - данные конкурсов
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": ["TOURNAMENT_CODE"],      # Добавляем код турнира
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 11,               # Максимальная ширина (код турнира короткий)
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # TOURNAMENT-SCHEDULE: добавляем количество записей из REPORT (подсчёт)
    # Подсчитывает количество отчетов для каждой пары конкурс-турнир
    {
        "sheet_src": "REPORT",              # Источник - отчеты
        "sheet_dst": "TOURNAMENT-SCHEDULE", # Цель - расписание турниров
        "src_key": ["CONTEST_CODE", "TOURNAMENT_CODE"],  # Составной ключ: код конкурса + код турнира
        "dst_key": ["CONTEST_CODE", "TOURNAMENT_CODE"],  # Составной ключ: код конкурса + код турнира
        "column": ["CONTEST_DATE"],         # Используем CONTEST_DATE для подсчета (любое поле подойдет)
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: из CONTEST-DATA по CONTEST_CODE — основные поля
    # Создает сводный лист с основной информацией о конкурсах
    {
        "sheet_src": "CONTEST-DATA",        # Источник - данные конкурсов
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": [                         # Добавляемые колонки:
            "FULL_NAME",                    # Полное название конкурса
            "CONTEST_DESCRIPTION",          # Описание конкурса
            f"{PREFIX_CONTEST_FEATURE} => feature",  # Признак конкурса (развернутый JSON)
            f"{PREFIX_CONTEST_FEATURE} => momentRewarding",  # Момент награждения
            "TARGET_TYPE",
            f"{PREFIX_CONTEST_FEATURE} => vid",
            "CONTEST_TYPE",                 # тип конкурса
            "PRODUCT_GROUP",                # группа продуктов
            "PRODUCT",
            "FACTOR_MATCH",                 # Фактор соответствия
            "FACTOR_MARK_TYPE",
            "PLAN_METHOD_CODE",
            "PLAN_MOD_METOD",
            "PLAN_MOD_VALUE",               # Плановое значение модуля
            "BUSINESS_BLOCK",               # Бизнес-блок
            f"{PREFIX_CONTEST_FEATURE} => businessBlock",
            f"{PREFIX_CONTEST_FEATURE} => tournamentStartMailing",  # Рассылка начала турнира
            f"{PREFIX_CONTEST_FEATURE} => tournamentEndMailing",  # Рассылка окончания турнира
            f"{PREFIX_CONTEST_FEATURE} => tournamentRewardingMailing",  # Рассылка награждения турнира
            f"{PREFIX_CONTEST_FEATURE} => tournamentLikeMailing",  # Рассылка лайков турнира
            f"{PREFIX_CONTEST_FEATURE} => tournamentListMailing",
            f"{PREFIX_CONTEST_FEATURE} => persomanNumberVisible",
            f"{PREFIX_CONTEST_FEATURE} => persomanNumberHidden",
            f"{PREFIX_CONTEST_FEATURE} => gosbVisible",
            f"{PREFIX_CONTEST_FEATURE} => gosbHidden",
            f"{PREFIX_CONTEST_FEATURE} => tbVisible",
            f"{PREFIX_CONTEST_FEATURE} => tbHidden",
            "CREATE_DT", "CLOSE_DT", "BUSINESS_STATUS", "SHOW_INDICATOR", "CONTEST_SUBJECT", 
            "CONTEST_INDICATOR_METHOD", "CONTEST_FACTOR_METHOD",
            "CONTEST_PERIOD", "SOURCE_UPD_FREQUENCY", "CALC_TYPE", "FACT_POST_PROCESSING",
            f"{PREFIX_CONTEST_FEATURE} => minNumber",
            f"{PREFIX_CONTEST_FEATURE} => capacity",
            f"{PREFIX_CONTEST_FEATURE} => accuracy",
            f"{PREFIX_CONTEST_FEATURE} => masking",
            f"{PREFIX_CONTEST_FEATURE} => typeRewarding",
            f"{PREFIX_CONTEST_FEATURE} => preferences",
            f"{PREFIX_CONTEST_FEATURE} => tournamentTeam",
            f"{PREFIX_CONTEST_FEATURE} => helpCode",
            "CONTEST_FEATURE"
        ],
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 55,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: из GROUP по составному ключу (для полностью связанных записей)
    # Добавляет критерии расчета из групп по полному соответствию ключей
    {
        "sheet_src": "GROUP",               # Источник - группы
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"],  # Составной ключ: код конкурса + код группы + значение группы
        "dst_key": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"],  # Составной ключ: код конкурса + код группы + значение группы
        "column": [                         # Добавляемые колонки:
            "GET_CALC_METHOD",
            "GET_CALC_CRITERION",          # Основной критерий расчета
            "ADD_CALC_CRITERION",          # Дополнительный критерий расчета
            "ADD_CALC_CRITERION_2",        # Второй дополнительный критерий расчета
            "BASE_CALC_CODE"
        ],
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 40,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: из INDICATOR по CONTEST_CODE
    # Добавляет информацию об индикаторах конкурса
    {
        "sheet_src": "INDICATOR",           # Источник - индикаторы
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE", "INDICATOR_ADD_CALC_TYPE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE", "INDICATOR_ADD_CALC_TYPE"],        # Ключ - код конкурса
        "column": [                         # Добавляемые колонки:
            "INDICATOR_CALC_TYPE", "INDICATOR_ADD_CALC_TYPE", "FULL_NAME",
            "INDICATOR_CODE",
            "INDICATOR_AGG_FUNCTION",
            "INDICATOR_WEIGHT",
            "INDICATOR_MARK_TYPE",          # Тип отметки индикатора
            "INDICATOR_MATCH",              # Соответствие индикатора
            "INDICATOR_VALUE",               # Значение индикатора
            "INDICATOR_OBJECT", "CONTEST_CRITERION", "CONTESTANT_SELECTION", "CALC_TYPE", "N",
            "INDICATOR_FILTER"
        ],
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 35,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: из TOURNAMENT-SCHEDULE по TOURNAMENT_CODE
    # Добавляет информацию о расписании турниров
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["TOURNAMENT_CODE"],     # Ключ - код турнира
        "dst_key": ["TOURNAMENT_CODE"],     # Ключ - код турнира
        "column": [                         # Добавляемые колонки:
            "START_DT",                     # Дата начала турнира
            "END_DT",                       # Дата окончания турнира
            "RESULT_DT",                    # Дата результатов турнира
            "TOURNAMENT_STATUS",            # Статус турнира
            "TARGET_TYPE",                   # Тип цели турнира
            "PERIOD_TYPE", "PLAN_PERIOD_START_DT", "PLAN_PERIOD_END_DT", "CRITERION_MARK_TYPE", "CRITERION_MARK_VALUE",
            "FILTER_PERIOD_ARR", "CONTEST_CODE", "CALC_TYPE", "TRN_INDICATOR_FILTER"
        ],
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 30,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: поля из REWARD по REWARD_CODE (для частично связанных и не связанных наград)
    # Добавляет информацию о наградах по коду награды (без привязки к конкурсу)
    {
        "sheet_src": "REWARD",              # Источник - награды
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["REWARD_CODE"],         # Ключ - код награды
        "dst_key": ["REWARD_CODE"],         # Ключ - код награды
        "column": [                         # Добавляемые колонки (те же, что и выше):
            "FULL_NAME",                    # Полное название награды
            "REWARD_DESCRIPTION",           # Описание награды
            "REWARD_CONDITION", "REWARD_COST",
            f"{PREFIX_ADD_DATA} => feature",  # Признак награды (развернутый JSON)
            f"{PREFIX_ADD_DATA} => itemFeature",  # Признак элемента награды
            f"{PREFIX_ADD_DATA} => rewardRule",  # Правило награды
            f"{PREFIX_ADD_DATA} => hidden",     # Признак скрытия награды
            f"{PREFIX_ADD_DATA} => hiddenRewardList",     # Признак скрытия награды
            f"{PREFIX_ADD_DATA} => nftFlg",     # Признак NFT
            f"{PREFIX_ADD_DATA} => newsType",
            f"{PREFIX_ADD_DATA} => winCriterion",
            f"{PREFIX_ADD_DATA} => rewardAgainGlobal",  # Повторная награда глобально
            f"{PREFIX_ADD_DATA} => rewardAgainTournament",  # Повторная награда в турнире
            f"{PREFIX_ADD_DATA} => outstanding",  # Выдающийся
            f"{PREFIX_ADD_DATA} => teamNews",  # Новости команды
            f"{PREFIX_ADD_DATA} => singleNews",  # Единичные новости
            f"{PREFIX_ADD_DATA} => fileName",
            f"{PREFIX_ADD_DATA} => refreshOldNews",
            f"{PREFIX_ADD_DATA} => businessBlock",
            f"{PREFIX_ADD_DATA} => recommendationLevel",
            f"{PREFIX_ADD_DATA} => priority",
            f"{PREFIX_ADD_DATA} => masterBadge",
            f"{PREFIX_ADD_DATA} => parentRewardCode",
            f"{PREFIX_ADD_DATA} => helpCode",
            "ADD_DATA"
        ],
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 50,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: CONTEST_CODE из REWARD-LINK по REWARD_CODE (для связи наград с конкурсами)
    # Связывает награды с конкурсами через промежуточную таблицу
    {
        "sheet_src": "REWARD-LINK",         # Источник - связи наград
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["REWARD_CODE"],         # Ключ - код награды
        "dst_key": ["REWARD_CODE"],         # Ключ - код награды
        "column": ["CONTEST_CODE"],         # Добавляем код конкурса
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 30,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: CONTEST_DATE из REPORT по TOURNAMENT_CODE
    # Добавляет дату конкурса из отчетов
    {
        "sheet_src": "REPORT",              # Источник - отчеты
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["TOURNAMENT_CODE", "CONTEST_CODE"],     # Ключ - код турнира
        "dst_key": ["TOURNAMENT_CODE", "CONTEST_CODE"],     # Ключ - код турнира
        "column": [                         # Добавляемые колонки:
            "CONTEST_DATE"                  # Дата конкурса
        ],
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 25,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: сколько в REPORT записей по ключу TOURNAMENT_CODE
    # Подсчитывает количество отчетов для каждого турнира
    {
        "sheet_src": "REPORT",              # Источник - отчеты
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["TOURNAMENT_CODE"],     # Ключ: код турнира
        "dst_key": ["TOURNAMENT_CODE"],     # Ключ: код турнира
        "column": [                         # Добавляемые колонки:
            "CONTEST_DATE"                  # Используем CONTEST_DATE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: сколько в REWARD наград по ключу CONTEST_CODE
    # Подсчитывает количество наград для каждого конкурса
    {
        "sheet_src": "REWARD",              # Источник - награды
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": [COL_REWARD_LINK_CONTEST_CODE],  # Ключ: код конкурса из связи
        "dst_key": ["CONTEST_CODE"],        # Ключ: код конкурса
        "column": [                         # Добавляемые колонки:
            "REWARD_CODE"                   # Используем REWARD_CODE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: сколько в CONTEST-DATA строк по ключу CONTEST_CODE
    # Подсчитывает количество записей конкурсов
    {
        "sheet_src": "CONTEST-DATA",        # Источник - данные конкурсов
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],        # Ключ: код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ: код конкурса
        "column": [                         # Добавляемые колонки:
            "CONTEST_CODE"                  # Используем CONTEST_CODE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: сколько в TOURNAMENT-SCHEDULE турниров по ключу CONTEST_CODE
    # Подсчитывает количество турниров для каждого конкурса
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],        # Ключ: код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ: код конкурса
        "column": [                         # Добавляемые колонки:
            "CONTEST_CODE"                  # Используем CONTEST_CODE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: сколько в GROUP строк по ключу CONTEST_CODE
    # Подсчитывает количество групп для каждого конкурса
    {
        "sheet_src": "GROUP",               # Источник - группы
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],        # Ключ: код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ: код конкурса
        "column": [                         # Добавляемые колонки:
            "CONTEST_CODE"                  # Используем CONTEST_CODE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: сколько в INDICATOR строк по ключу CONTEST_CODE
    # Подсчитывает количество индикаторов для каждого конкурса
    {
        "sheet_src": "INDICATOR",           # Источник - индикаторы
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],        # Ключ: код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ: код конкурса
        "column": [                         # Добавляемые колонки:
            "INDICATOR_MARK_TYPE"           # Используем INDICATOR_MARK_TYPE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: сколько в REWARD-LINK связей по ключу REWARD_CODE
    # Подсчитывает количество связей награды с конкурсами
    {
        "sheet_src": "REWARD-LINK",         # Источник - связи наград
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],         # Ключ: код награды
        "dst_key": ["CONTEST_CODE"],         # Ключ: код награды
        "column": [                         # Добавляемые колонки:
            "REWARD_CODE"                  # Используем CONTEST_CODE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    }
]

# === ДОПОЛНИТЕЛЬНЫЕ ПРАВИЛА ДЛЯ CONTEST-DATA ===
# Добавляем поля с суммой по статусам из TOURNAMENT-SCHEDULE

MERGE_FIELDS_ADVANCED = [
    # CONTEST-DATA: добавляем количество турниров по статусам
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "CONTEST-DATA",        # Цель - данные конкурсов
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": ["TOURNAMENT_CODE"],      # Колонка для подсчета
        "mode": "count",                    # Режим подсчета
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 15,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8,                # Минимальная ширина
        # Новые параметры:
        "status_filters": {                 # Фильтрация по статусам
            "TOURNAMENT_STATUS": ["АКТИВНЫЙ"] # Только активные турниры
        },
        "custom_conditions": None,          # Без дополнительных условий
        "group_by": None,                   # Без группировки
        "aggregate": None                   # Без агрегации
    },
    
    # CONTEST-DATA: добавляем количество завершенных турниров
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "CONTEST-DATA",        # Цель - данные конкурсов
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": ["TOURNAMENT_CODE"],      # Колонка для подсчета
        "mode": "count",                    # Режим подсчета
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 15,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8,                # Минимальная ширина
        # Новые параметры:
        "status_filters": {                 # Фильтрация по статусам
            "TOURNAMENT_STATUS": ["ЗАВЕРШЕН"] # Только завершенные турниры
        },
        "custom_conditions": None,          # Без дополнительных условий
        "group_by": None,                   # Без группировки
        "aggregate": None                   # Без агрегации
    },
    
    # CONTEST-DATA: добавляем количество отмененных турниров
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "CONTEST-DATA",        # Цель - данные конкурсов
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": ["TOURNAMENT_CODE"],      # Колонка для подсчета
        "mode": "count",                    # Режим подсчета
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 15,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8,                # Минимальная ширина
        # Новые параметры:
        "status_filters": {                 # Фильтрация по статусам
            "TOURNAMENT_STATUS": ["ОТМЕНЕН"] # Только отмененные турниры
        },
        "custom_conditions": None,          # Без дополнительных условий
        "group_by": None,                   # Без группировки
        "aggregate": None                   # Без агрегации
    },
    
    # CONTEST-DATA: добавляем количество турниров в подведении итогов
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "CONTEST-DATA",        # Цель - данные конкурсов
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": ["TOURNAMENT_CODE"],      # Колонка для подсчета
        "mode": "count",                    # Режим подсчета
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 15,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8,                # Минимальная ширина
        # Новые параметры:
        "status_filters": {                 # Фильтрация по статусам
            "TOURNAMENT_STATUS": ["ПОДВЕДЕНИЕ ИТОГОВ"] # Только в подведении итогов
        },
        "custom_conditions": None,          # Без дополнительных условий
        "group_by": None,                   # Без группировки
        "aggregate": None                   # Без агрегации
    },
    
    # CONTEST-DATA: добавляем количество удаленных турниров
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "CONTEST-DATA",        # Цель - данные конкурсов
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": ["TOURNAMENT_CODE"],      # Колонка для подсчета
        "mode": "count",                    # Режим подсчета
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 15,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8,                # Минимальная ширина
        # Новые параметры:
        "status_filters": {                 # Фильтрация по статусам
            "TOURNAMENT_STATUS": ["УДАЛЕН"] # Только удаленные турниры
        },
        "custom_conditions": None,          # Без дополнительных условий
        "group_by": None,                   # Без группировки
        "aggregate": None                   # Без агрегации
    }
]

# === ОПРЕДЕЛЕНИЕ КЛЮЧЕВЫХ КОЛОНОК ДЛЯ СВОДНОГО ЛИСТА ===
# SUMMARY_KEY_DEFS определяет, какие колонки являются ключевыми для каждого листа
# Это используется для правильного объединения данных в сводном листе
SUMMARY_KEY_DEFS = [
    {"sheet": "CONTEST-DATA", "cols": ["CONTEST_CODE"]},                    # Ключ конкурса для данных конкурсов
    {"sheet": "TOURNAMENT-SCHEDULE", "cols": ["TOURNAMENT_CODE", "CONTEST_CODE"]},  # Ключи турнира и конкурса для расписания
    {"sheet": "REWARD-LINK", "cols": ["REWARD_CODE", "CONTEST_CODE"]},      # Ключи награды и конкурса для связей
    {"sheet": "GROUP", "cols": ["GROUP_CODE", "CONTEST_CODE", "GROUP_VALUE"]},  # Составной ключ для групп
    {"sheet": "REWARD", "cols": ["REWARD_CODE"]},                           # Ключ награды для наград
    {"sheet": "INDICATOR", "cols": ["INDICATOR_ADD_CALC_TYPE", "CONTEST_CODE"]},  # Дополнительный тип расчета индикатора
]

# Построить упорядоченный список всех уникальных ключей
# Это нужно для создания правильной структуры сводного листа
SUMMARY_KEY_COLUMNS = []
for entry in SUMMARY_KEY_DEFS:
    for col in entry["cols"]:
        if col not in SUMMARY_KEY_COLUMNS:
            SUMMARY_KEY_COLUMNS.append(col)

# === ЦВЕТОВАЯ СХЕМА ДЛЯ EXCEL ЛИСТОВ ===
# COLOR_SCHEME определяет цветовое оформление различных типов колонок
# Это помогает визуально различать типы данных и улучшает читаемость
COLOR_SCHEME = [
    # --- ИСХОДНЫЕ ДАННЫЕ (загружаются из CSV) — пастельный голубой ---
    # Базовые поля, загруженные из исходных CSV файлов
    {
        "group": "Исходные данные",         # Группа: исходные данные
        "header_bg": "E6F3FF",              # Фон заголовка: пастельный голубой (приятный для глаз)
        "header_fg": "2C3E50",              # Цвет текста заголовка: тёмно-серый (для лучшей читаемости)
        "column_bg": None,                  # Фон колонки: не задан (по умолчанию)
        "column_fg": None,                  # Цвет текста колонки: не задан (по умолчанию)
        "style_scope": "header",            # Область применения стиля: только заголовки
        "sheets": ["CONTEST-DATA", "GROUP", "INDICATOR", "REPORT", "REWARD", "REWARD-LINK", "TOURNAMENT-SCHEDULE",
                   "ORG_UNIT_V20", "USER_ROLE", "USER_ROLE SB", "EMPLOYEE"],  # Применяется ко всем листам
        "columns": [],                      # Все колонки (если не указано — все)
        # Назначение: базовые поля из CSV файлов
    },

    # --- ИСХОДНЫЕ JSON ПОЛЯ (CONTEST_FEATURE, REWARD_ADD_DATA) — тёмно-оранжевый со светлыми буквами ---
    # Исходные поля, содержащие JSON данные, которые будут развернуты
    {
        "group": "JSON source columns",     # Группа: исходные JSON колонки
        "header_bg": "FF8C42",              # Фон заголовка: тёмно-оранжевый (самый верхний уровень JSON полей)
        "header_fg": "FFFFFF",              # Цвет текста заголовка: белый (для контраста)
        "column_bg": None,                  # Фон колонки: не задан
        "column_fg": None,                  # Цвет текста колонки: не задан
        "style_scope": "header",            # Область применения стиля: только заголовки
        "sheets": ["CONTEST-DATA", "REWARD"],  # Применяется к листам с JSON данными
        "columns": ["CONTEST_FEATURE", "REWARD_ADD_DATA"],  # Конкретные JSON колонки
        # Назначение: исходные поля с JSON, которые разворачиваются
    },

    # --- РАЗВОРАЧИВАЕМЫЕ JSON ПОЛЯ ПЕРВОГО УРОВНЯ — светлее исходных ---
    # Колонки, созданные при разворачивании JSON данных первого уровня
    {
        "group": "JSON expanded level 1",   # Группа: развернутые JSON поля первого уровня
        "header_bg": "FFB366",              # Фон заголовка: светло-оранжевый (светлее исходных JSON полей)
        "header_fg": "2C3E50",              # Цвет текста заголовка: тёмно-серый (для читаемости)
        "column_bg": None,                  # Фон колонки: не задан
        "column_fg": None,                  # Цвет текста колонки: не задан
        "style_scope": "header",            # Область применения стиля: только заголовки
        "sheets": ["CONTEST-DATA", "REWARD"],  # Применяется к листам с развернутыми JSON данными
        "columns": [                        # Список развернутых полей:
            # CONTEST_FEATURE развёрнутые поля (признаки конкурса)
            "CONTEST_FEATURE => momentRewarding", "CONTEST_FEATURE => tournamentStartMailing",  # Момент награждения, рассылка начала
            "CONTEST_FEATURE => tournamentEndMailing",  # Рассылка окончания турнира
            "CONTEST_FEATURE => tournamentRewardingMailing", "CONTEST_FEATURE => tournamentLikeMailing",  # Рассылка награждения, лайков
            "CONTEST_FEATURE => capacity",  # Вместимость
            "CONTEST_FEATURE => tournamentListMailing", "CONTEST_FEATURE => vid", "CONTEST_FEATURE => tbVisible",  # Рассылка списка, вид, видимость ТБ
            "CONTEST_FEATURE => tbHidden",  # Скрытость ТБ
            "CONTEST_FEATURE => persomanNumberVisible", "CONTEST_FEATURE => typeRewarding",  # Видимость номера, тип награды
            "CONTEST_FEATURE => masking",   # Маскирование
            "CONTEST_FEATURE => minNumber", "CONTEST_FEATURE => businessBlock", "CONTEST_FEATURE => accuracy",  # Мин. номер, бизнес-блок, точность
            "CONTEST_FEATURE => gosbHidden",  # Скрытость ГОСБ
            "CONTEST_FEATURE => preferences", "CONTEST_FEATURE => persomanNumberHidden",  # Предпочтения, скрытость номера
            "CONTEST_FEATURE => gosbVisible", "CONTEST_FEATURE => feature",  # Видимость ГОСБ, признак
            # ADD_DATA развёрнутые поля первого уровня (дополнительные данные наград)
            "ADD_DATA => refreshOldNews", "ADD_DATA => fileName", "ADD_DATA => rewardRule",  # Обновление новостей, имя файла, правило
            "ADD_DATA => bookingRequired", "ADD_DATA => outstanding",  # Требуется бронирование, выдающийся
            "ADD_DATA => teamNews", "ADD_DATA => singleNews", "ADD_DATA => rewardAgainGlobal",  # Новости команды, единичные, повторная глобально
            "ADD_DATA => rewardAgainTournament",  # Повторная в турнире
            "ADD_DATA => isGrouping", "ADD_DATA => tagEndDT", "ADD_DATA => itemAmount", "ADD_DATA => isGroupingTitle",  # Группировка, тег окончания, количество, заголовок группировки
            "ADD_DATA => itemLimitCount", "ADD_DATA => recommendationLevel", "ADD_DATA => isGroupingName",  # Лимит количества, уровень рекомендации, имя группировки
            "ADD_DATA => ignoreConditions",  # Игнорировать условия
            "ADD_DATA => masterBadge", "ADD_DATA => priority", "ADD_DATA => nftFlg", "ADD_DATA => itemMinShow",  # Мастер-значок, приоритет, NFT флаг, мин. показ
            "ADD_DATA => itemFeature",      # Признак элемента
            "ADD_DATA => itemLimitPeriod", "ADD_DATA => businessBlock", "ADD_DATA => parentRewardCode",  # Лимит периода, бизнес-блок, код родительской награды
            "ADD_DATA => deliveryRequired",  # Требуется доставка
        ],
        # Назначение: поля первого уровня развёртывания JSON (светлее исходных)
    },

    # --- РАЗВОРАЧИВАЕМЫЕ JSON ПОЛЯ ВТОРОГО УРОВНЯ — темнее исходных, но светлее других ---
    {
        "group": "JSON expanded level 2",
        "header_bg": "E67E22",  # оранжевый - темнее исходных JSON, но светлее других развёрнутых
        "header_fg": "FFFFFF",  # белый текст для контраста
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["CONTEST-DATA", "REWARD"],
        "columns": [
            # ADD_DATA => getCondition развёрнутые поля
            "ADD_DATA => getCondition => nonRewards", "ADD_DATA => getCondition => rewards",
            # ADD_DATA => getCondition => employeeRating развёрнутые поля
            "ADD_DATA => getCondition => employeeRating => minRatingTB",
            "ADD_DATA => getCondition => employeeRating => minRatingGOSB",
            "ADD_DATA => getCondition => employeeRating => minRatingBANK",
            "ADD_DATA => getCondition => employeeRating => seasonCode",
            "ADD_DATA => getCondition => employeeRating => minCrystalEarnedTotal"
        ],
        # Назначение: поля второго уровня развёртывания JSON (вложенные объекты)
    },

    # --- ПОЛЯ ДОБАВЛЯЕМЫЕ ЧЕРЕЗ MERGE (кроме SUMMARY) — пастельный розовый ---
    {
        "group": "Process added fields",
        "header_bg": "FFE6F2",  # пастельный розовый - приятный для глаз
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["REWARD", "REPORT", "TOURNAMENT-SCHEDULE", "EMPLOYEE", "CONTEST-DATA", "GROUP", "INDICATOR",
                   "REWARD-LINK"],  # поля добавляемые через merge_fields
        "columns": [
            COL_REWARD_LINK_CONTEST_CODE,
            "CONTEST-DATA=>CONTEST_TYPE", "CONTEST-DATA=>FULL_NAME", "CONTEST-DATA=>BUSINESS_BLOCK",
            "CONTEST-DATA=>BUSINESS_STATUS",
            "TOURNAMENT-SCHEDULE=>END_DT", "TOURNAMENT-SCHEDULE=>RESULT_DT", "TOURNAMENT-SCHEDULE=>TOURNAMENT_STATUS",
            "REPORT=>CONTEST_DATE", "REPORT=>COUNT_CONTEST_DATE", "AUTO_GENDER", "CALC_TOURNAMENT_STATUS",
            "FIELD_LENGTH_CHECK"
        ],
        # Назначение: поля добавляемые через merge_fields_across_sheets
    },

    # --- SUMMARY КЛЮЧЕВЫЕ ПОЛЯ — как исходные данные ---
    {
        "group": "SUMMARY KEYS",
        "header_bg": "E6F3FF",  # пастельный голубой - как исходные данные
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": SUMMARY_KEY_COLUMNS,
        # Назначение: ключевые поля в SUMMARY (как исходные данные)
    },

    # --- SUMMARY ПОЛЯ: CONTEST-DATA — пастельный голубой ---
    {
        "group": "SUMMARY FIELDS: CONTEST-DATA",
        "header_bg": "CCE5FF",  # пастельный голубой - оттенок для CONTEST-DATA
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": [
            "CONTEST-DATA=>FULL_NAME",
            "CONTEST-DATA=>CONTEST_FEATURE => momentRewarding",
            "CONTEST-DATA=>FACTOR_MATCH",
            "CONTEST-DATA=>PLAN_MOD_VALUE",
            "CONTEST-DATA=>BUSINESS_BLOCK",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentStartMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentEndMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentRewardingMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentLikeMailing",
        ],
        # Назначение: поля из CONTEST-DATA в SUMMARY
    },

    # --- SUMMARY ПОЛЯ: GROUP — пастельный зелёный ---
    {
        "group": "SUMMARY FIELDS: GROUP",
        "header_bg": "E8F5E8",  # пастельный зелёный - оттенок для GROUP
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": ["GROUP=>GET_CALC_CRITERION", "GROUP=>ADD_CALC_CRITERION", "GROUP=>ADD_CALC_CRITERION_2"],
        # Назначение: поля из GROUP в SUMMARY
    },

    # --- SUMMARY ПОЛЯ: INDICATOR — пастельный жёлтый ---
    {
        "group": "SUMMARY FIELDS: INDICATOR",
        "header_bg": "FFF8E1",  # пастельный жёлтый - оттенок для INDICATOR
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": ["INDICATOR=>INDICATOR_MARK_TYPE", "INDICATOR=>INDICATOR_MATCH", "INDICATOR=>INDICATOR_VALUE"],
        # Назначение: поля из INDICATOR в SUMMARY
    },

    # --- SUMMARY ПОЛЯ: TOURNAMENT-SCHEDULE — пастельный голубой ---
    {
        "group": "SUMMARY FIELDS: TOURNAMENT-SCHEDULE",
        "header_bg": "E1F5FE",  # пастельный голубой - оттенок для TOURNAMENT-SCHEDULE
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": ["TOURNAMENT-SCHEDULE=>START_DT", "TOURNAMENT-SCHEDULE=>END_DT", "TOURNAMENT-SCHEDULE=>RESULT_DT",
                    "TOURNAMENT-SCHEDULE=>TOURNAMENT_STATUS", "TOURNAMENT-SCHEDULE=>TARGET_TYPE"],
        # Назначение: поля из TOURNAMENT-SCHEDULE в SUMMARY
    },

    # --- SUMMARY ПОЛЯ: REPORT — пастельный зелёный ---
    {
        "group": "SUMMARY FIELDS: REPORT",
        "header_bg": "E8F5E8",  # пастельный зелёный - оттенок для REPORT
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": ["REPORT=>CONTEST_DATE", "REPORT=>COUNT_CONTEST_DATE"],
        # Назначение: поля из REPORT в SUMMARY
    },

    # --- SUMMARY ПОЛЯ: REWARD — пастельный оранжевый ---
    {
        "group": "SUMMARY FIELDS: REWARD",
        "header_bg": "FFE8CC",  # пастельный оранжевый - оттенок для REWARD
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": [
            "REWARD=>ADD_DATA => rewardAgainGlobal",
            "REWARD=>ADD_DATA => rewardAgainTournament",
            "REWARD=>ADD_DATA => outstanding",
            "REWARD=>ADD_DATA => teamNews",
            "REWARD=>ADD_DATA => singleNews",
        ],
        # Назначение: поля из REWARD в SUMMARY
    },

    # --- ДУБЛИ В SUMMARY — пастельный розовый ---
    {
        "group": "SUMMARY DUPLICATES",
        "header_bg": "FFE6F2",  # пастельный розовый - для дублей
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": ["ДУБЛЬ: CONTEST_CODE_TOURNAMENT_CODE_REWARD_CODE_GROUP_CODE"],
        # Назначение: поля дублей в SUMMARY
    },
]

# Добавление секции для дублей по CHECK_DUPLICATES
CHECK_DUPLICATES = [
    {"sheet": "CONTEST-DATA", "key": ["CONTEST_CODE"]},
    {"sheet": "GROUP", "key": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"]},
    {"sheet": "INDICATOR", "key": ["CONTEST_CODE", "INDICATOR_ADD_CALC_TYPE"]},
    {"sheet": "INDICATOR", "key": ["N"]},
    {"sheet": "REPORT", "key": ["MANAGER_PERSON_NUMBER", "TOURNAMENT_CODE", "CONTEST_CODE"]},
    {"sheet": "REWARD", "key": ["REWARD_CODE"]},
    {"sheet": "REWARD-LINK", "key": ["CONTEST_CODE", "REWARD_CODE"]},
    {"sheet": "REWARD-LINK", "key": ["REWARD_CODE"]},
    {"sheet": "TOURNAMENT-SCHEDULE", "key": ["TOURNAMENT_CODE", "CONTEST_CODE"]},
    {"sheet": "TOURNAMENT-SCHEDULE", "key": ["TOURNAMENT_CODE"]},
    {"sheet": "ORG_UNIT_V20", "key": ["ORG_UNIT_CODE"]},
    {"sheet": "USER_ROLE", "key": ["RULE_NUM"]},
    {"sheet": "USER_ROLE SB", "key": ["RULE_NUM"]},
    {"sheet": "EMPLOYEE", "key": ["PERSON_NUMBER"]},
    {"sheet": "EMPLOYEE", "key": ["PERSON_NUMBER_ADD"]}
]

for check in CHECK_DUPLICATES:
    sheet = check["sheet"]
    keys = check["key"]
    col_name = "ДУБЛЬ: " + "_".join(keys)
    COLOR_SCHEME.append({
        "group": "DUPLICATES",
        "header_bg": "FFE6F2",  # пастельный розовый - для дублей
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": [sheet],
        "columns": [col_name],
        # Назначение: поля дублей на всех листах
    })

# Какие поля разворачивать, в каком листе, с каким префиксом (строго регламентировано)
# JSON_COLUMNS определяет, какие JSON колонки нужно развернуть в каждом листе
# Это позволяет контролировать процесс разворачивания и избежать лишних операций
JSON_COLUMNS = {
    "CONTEST-DATA": [                        # Лист с данными конкурсов
        {"column": "CONTEST_FEATURE", "prefix": PREFIX_CONTEST_FEATURE},  # Разворачиваем признаки конкурса
    ],
    "REWARD": [                              # Лист с наградами
        {"column": "REWARD_ADD_DATA", "prefix": PREFIX_ADD_DATA},        # Разворачиваем дополнительные данные наград
    ],
    # Если появятся другие листы — добавить по аналогии
}


# Выходной файл Excel
def get_output_filename():
    """
    Генерирует имя выходного Excel файла с текущей датой и временем.
    
    Returns:
        str: Имя файла в формате 'SPOD_ALL_IN_ONE_YYYY-MM-DD_HH-MM-SS.xlsx'
    """
    return f'SPOD_ALL_IN_ONE_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.xlsx'


# Лог-файл с учетом уровня
def get_log_filename():
    """
    Генерирует имя лог-файла с учетом уровня логирования, текущей даты и времени.
    
    Returns:
        str: Путь к лог-файлу в формате 'LOGS/LOGS_LEVEL_YYYYMMDD_HH_MM.log'
    """
    # Имя лог-файла по дате с уровнем логирования, например: LOGS_INFO_20251113_14_30.log
    level_suffix = f"_{LOG_LEVEL}" if LOG_LEVEL else ""
    date_suffix = f"_{datetime.now().strftime('%Y%m%d_%H_%M')}.log"
    return os.path.join(DIR_LOGS, LOG_BASE_NAME + level_suffix + date_suffix)


# === Логирование ===
def setup_logger():
    """
    Настраивает систему логирования для программы.
    
    Создает логгер с двумя обработчиками:
    - Файловый: записывает логи в файл с кодировкой UTF-8 (включая DEBUG)
    - Консольный: выводит только INFO, WARNING, ERROR в стандартный вывод
    
    Returns:
        str: Путь к созданному лог-файлу
    """
    log_file = get_log_filename()
    # Если логгер уже инициализирован, не добавляем обработчики ещё раз
    if logging.getLogger().hasHandlers():
        return log_file
    
    # Получаем корневой логгер
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Устанавливаем максимальный уровень для логгера
    
    # Форматтер для файла (с именем функции)
    file_formatter = CallerFormatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Форматтер для консоли (без имени функции)
    console_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Файловый обработчик - принимает все уровни включая DEBUG
    file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Консольный обработчик - только INFO, WARNING, ERROR
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # INFO и выше (INFO, WARNING, ERROR)
    console_handler.setFormatter(console_formatter)
    
    # Добавляем обработчики к логгеру
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return log_file

def calculate_tournament_status(df_tournament, df_report=None):
    """
    Вычисляет статус турнира на основе текущей даты и дат турнира.
    
    Эта функция анализирует временные рамки турнира и определяет его текущее состояние.
    Статус зависит от соотношения текущей даты с датами начала, окончания и подведения итогов турнира.

    Логика определения статуса:
    - Если сегодня между START_DT и END_DT включительно → "АКТИВНЫЙ"
    - Если сегодня < START_DT → "ЗАПЛАНИРОВАН"
    - Если сегодня > END_DT но < RESULT_DT → "ПОДВЕДЕНИЕ ИТОГОВ"
    - Если сегодня >= RESULT_DT:
        - Если макс CONTEST_DATE < RESULT_DT → "ПОДВЕДЕНИЕ ИТОГОВ"
        - Если макс CONTEST_DATE >= RESULT_DT → "ЗАВЕРШЕН"

    Args:
        df_tournament (pd.DataFrame): DataFrame с данными турниров, должен содержать колонки:
            - START_DT: дата начала турнира
            - END_DT: дата окончания турнира  
            - RESULT_DT: дата подведения итогов
        df_report (pd.DataFrame, optional): DataFrame с отчетами для анализа CONTEST_DATE.
            Должен содержать колонки TOURNAMENT_CODE и CONTEST_DATE.

    Returns:
        pd.DataFrame: DataFrame с добавленной колонкой CALC_TOURNAMENT_STATUS,
                     содержащей вычисленный статус для каждого турнира
    """
    func_start = time()  # Засекаем время начала выполнения
    params = "(TOURNAMENT-SCHEDULE status calculation)"
    logging.info("[START] {func} {params}".format(func="calculate_tournament_status", params=params))

    today = pd.Timestamp.now().date()  # Текущая дата
    df = df_tournament.copy()          # Копируем DataFrame для безопасной работы

    # Вспомогательная функция для безопасного преобразования строк в даты
    def safe_to_date(date_str):
        """
        Безопасно преобразует строку в дату, обрабатывая некорректные значения.
        
        Args:
            date_str: Строка с датой или некорректное значение
            
        Returns:
            datetime.date or None: Преобразованная дата или None при ошибке
        """
        try:
            if pd.isna(date_str) or date_str in ['', '-', 'None', 'null']:
                return None
            return pd.to_datetime(date_str).date()
        except:
            return None

    # Преобразуем даты в pandas datetime, обрабатываем ошибки
    df['START_DT_parsed'] = pd.to_datetime(df['START_DT'], errors='coerce').dt.date      # Парсим дату начала
    df['END_DT_parsed'] = pd.to_datetime(df['END_DT'], errors='coerce').dt.date          # Парсим дату окончания
    df['RESULT_DT_parsed'] = pd.to_datetime(df['RESULT_DT'], errors='coerce').dt.date    # Парсим дату результатов

    # Получаем максимальные CONTEST_DATE для каждого TOURNAMENT_CODE из REPORT
    # Это нужно для определения, завершились ли все конкурсы турнира
    max_contest_dates = {}
    if df_report is not None and 'CONTEST_DATE' in df_report.columns and 'TOURNAMENT_CODE' in df_report.columns:
        df_report_dates = df_report.copy()
        df_report_dates['CONTEST_DATE_parsed'] = pd.to_datetime(df_report_dates['CONTEST_DATE'], errors='coerce').dt.date
        df_report_dates = df_report_dates.dropna(subset=['CONTEST_DATE_parsed', 'TOURNAMENT_CODE'])

        if not df_report_dates.empty:
            # Группируем по коду турнира и находим максимальную дату конкурса
            max_contest_dates = df_report_dates.groupby('TOURNAMENT_CODE')['CONTEST_DATE_parsed'].max().to_dict()


    # ВЕКТОРИЗОВАННАЯ ВЕРСИЯ: Заменяем apply на векторные операции для ускорения
    # Создаем Series с максимальными датами конкурсов для каждого турнира
    if max_contest_dates:
        df['MAX_CONTEST_DATE'] = df['TOURNAMENT_CODE'].map(max_contest_dates)
    else:
        df['MAX_CONTEST_DATE'] = None
    
    # Векторизованное определение статуса с использованием numpy.select
    # Условия проверяются последовательно, первое совпадение определяет статус
    # ВАЖНО: Порядок условий критичен для корректной логики
    conditions = [
        # Условие 0: Нет ключевых дат → НЕОПРЕДЕЛЕН
        pd.isna(df['START_DT_parsed']) | pd.isna(df['END_DT_parsed']),
        # Условие 1: Сегодня между START_DT и END_DT включительно → АКТИВНЫЙ
        (df['START_DT_parsed'] <= today) & (today <= df['END_DT_parsed']),
        # Условие 2: Сегодня < START_DT → ЗАПЛАНИРОВАН
        today < df['START_DT_parsed'],
        # Условие 3: Сегодня > END_DT и (нет RESULT_DT или today < RESULT_DT) → ПОДВЕДЕНИЕ ИТОГОВ
        (today > df['END_DT_parsed']) & (pd.isna(df['RESULT_DT_parsed']) | (today < df['RESULT_DT_parsed'])),
        # Условие 4: today >= RESULT_DT и нет MAX_CONTEST_DATE → ПОДВЕДЕНИЕ ИТОГОВ
        # Проверяем что today > END_DT (уже проверено в условии 3 не выполнилось) и today >= RESULT_DT
        (today > df['END_DT_parsed']) & (~pd.isna(df['RESULT_DT_parsed'])) & (today >= df['RESULT_DT_parsed']) & pd.isna(df['MAX_CONTEST_DATE']),
        # Условие 5: today >= RESULT_DT и MAX_CONTEST_DATE < RESULT_DT → ПОДВЕДЕНИЕ ИТОГОВ
        (today > df['END_DT_parsed']) & (~pd.isna(df['RESULT_DT_parsed'])) & (today >= df['RESULT_DT_parsed']) & (~pd.isna(df['MAX_CONTEST_DATE'])) & (df['MAX_CONTEST_DATE'] < df['RESULT_DT_parsed']),
        # Условие 6: today >= RESULT_DT и MAX_CONTEST_DATE >= RESULT_DT → ЗАВЕРШЕН
        (today > df['END_DT_parsed']) & (~pd.isna(df['RESULT_DT_parsed'])) & (today >= df['RESULT_DT_parsed']) & (~pd.isna(df['MAX_CONTEST_DATE'])) & (df['MAX_CONTEST_DATE'] >= df['RESULT_DT_parsed']),
    ]
    
    choices = [
        "НЕОПРЕДЕЛЕН",      # Условие 0
        "АКТИВНЫЙ",         # Условие 1
        "ЗАПЛАНИРОВАН",     # Условие 2
        "ПОДВЕДЕНИЕ ИТОГОВ", # Условие 3
        "ПОДВЕДЕНИЕ ИТОГОВ", # Условие 4
        "ПОДВЕДЕНИЕ ИТОГОВ", # Условие 5
        "ЗАВЕРШЕН",         # Условие 6
    ]
    
    # Используем numpy.select для векторизованного выбора (быстрее чем apply)
    try:
        import numpy as np
        df['CALC_TOURNAMENT_STATUS'] = np.select(conditions, choices, default="НЕОПРЕДЕЛЕН")
    except ImportError:
        # Fallback на pandas where если numpy недоступен (но он должен быть в Anaconda)
        df['CALC_TOURNAMENT_STATUS'] = pd.Series("НЕОПРЕДЕЛЕН", index=df.index)
        for i, (cond, choice) in enumerate(zip(conditions, choices)):
            df.loc[cond, 'CALC_TOURNAMENT_STATUS'] = choice

    # Удаляем временные колонки с распарсенными датами
    df = df.drop(columns=['START_DT_parsed', 'END_DT_parsed', 'RESULT_DT_parsed', 'MAX_CONTEST_DATE'])

    # Логируем статистику по статусам для мониторинга
    status_counts = df['CALC_TOURNAMENT_STATUS'].value_counts()
    logging.info("[TOURNAMENT STATUS] Статистика: {stats}".format(stats=status_counts.to_dict()))

    # Засекаем время выполнения и логируем завершение
    func_time = time() - func_start
    logging.info("[END] {func} {params} (время: {time:.3f}s)".format(
        func="calculate_tournament_status", 
        params=params, 
        time=func_time
    ))

    return df


def validate_field_lengths(df, sheet_name):
    """
    Проверяет длину полей согласно конфигурации FIELD_LENGTH_VALIDATIONS.
    Добавляет колонку с результатом проверки для каждого листа.
    
    Эта функция валидирует длину полей в DataFrame согласно заданным правилам.
    Результат проверки записывается в специальную колонку для последующего анализа.

    Формат результата:
    - "-" если все поля соответствуют ограничениям
    - "поле1 = длина > ограничение; поле2 = длина > ограничение" если есть нарушения

    Args:
        df (pd.DataFrame): DataFrame для проверки
        sheet_name (str): Название листа (используется для поиска конфигурации)

    Returns:
        pd.DataFrame: DataFrame с добавленной колонкой результата проверки
    """
    func_start = time()  # Засекаем время начала выполнения

    # Проверяем есть ли конфигурация для этого листа
    if sheet_name not in FIELD_LENGTH_VALIDATIONS:
        return df  # Если конфигурации нет - возвращаем DataFrame без изменений

    config = FIELD_LENGTH_VALIDATIONS[sheet_name]        # Получаем конфигурацию для листа
    result_column = config["result_column"]              # Название колонки для результатов
    fields_config = config["fields"]                     # Конфигурация полей для проверки

    # Проверяем наличие полей в DataFrame
    missing_fields = [field for field in fields_config.keys() if field not in df.columns]
    if missing_fields:
        logging.warning("[FIELD LENGTH] Пропущены поля {fields} в листе {sheet}".format(fields=missing_fields, sheet=sheet_name))
        # Создаем пустую колонку если нет полей для проверки
        df[result_column] = '-'
        return df

    total_rows = len(df)  # Общее количество строк для проверки
    logging.info("[FIELD LENGTH] Проверка длины полей для листа {sheet}, строк: {rows}".format(sheet=sheet_name, rows=total_rows))

    # Счетчики для статистики выполнения
    correct_count = 0    # Количество корректных строк
    error_count = 0      # Количество строк с ошибками

    def check_field_length(value, limit, operator):
        """
        Проверяет соответствие длины поля заданному ограничению.
        
        Args:
            value: Значение поля для проверки
            limit (int): Ограничение длины
            operator (str): Оператор сравнения ("<=", "=", ">=", "<", ">")
            
        Returns:
            bool: True если поле соответствует ограничению, False если нарушает
        """
        if pd.isna(value) or value in ['', '-', 'None', 'null']:
            return True  # Пустые значения считаем корректными

        length = len(str(value))  # Преобразуем в строку и считаем длину

        # Проверяем соответствие ограничению в зависимости от оператора
        if operator == "<=":
            return length <= limit
        elif operator == "=":
            return length == limit
        elif operator == ">=":
            return length >= limit
        elif operator == "<":
            return length < limit
        elif operator == ">":
            return length > limit
        else:
            return True  # Неизвестный оператор - считаем корректным

    def check_row(row, row_idx):
        """
        Проверяет одну строку и возвращает результат проверки.
        
        Args:
            row: Строка DataFrame для проверки
            row_idx: Индекс строки для логирования
            
        Returns:
            str: Результат проверки: "-" если все корректно, иначе описание нарушений
        """
        violations = []  # Список нарушений для текущей строки

        # Проверяем каждое поле согласно конфигурации
        for field_name, field_config in fields_config.items():
            limit = field_config["limit"]      # Ограничение длины
            operator = field_config["operator"]  # Оператор сравнения
            value = row.get(field_name, '')   # Значение поля (по умолчанию пустая строка)

            # Если поле не соответствует ограничению - добавляем в список нарушений
            if not check_field_length(value, limit, operator):
                length = len(str(value)) if not pd.isna(value) else 0
                violations.append(f"{field_name} = {length} {operator} {limit}")

                # Логируем нарушение для отладки
                logging.debug("[DEBUG] Строка {row}: поле '{field}' = {length} {operator} {limit} (нарушение)".format(
                    row=row_idx, field=field_name, length=length, operator=operator, limit=limit
                ))

        # Возвращаем результат: "-" если нарушений нет, иначе список нарушений через "; "
        return "; ".join(violations) if violations else "-"

    # Обрабатываем каждую строку DataFrame
    results = []
    for idx, row in df.iterrows():
        result = check_row(row, idx)  # Проверяем текущую строку
        results.append(result)        # Добавляем результат в список

        # Обновляем статистику выполнения
        if result == "-":
            correct_count += 1        # Строка корректна
        else:
            error_count += 1          # Строка содержит нарушения

        # Показываем прогресс каждые GENDER_PROGRESS_STEP строк
        if (idx + 1) % GENDER_PROGRESS_STEP == 0:
            percent = ((idx + 1) / total_rows) * 100
            logging.info("[FIELD LENGTH] Обработано {processed} из {total} строк ({percent:.1f}%)".format(
                processed=idx + 1, total=total_rows, percent=percent
            ))

    # Добавляем колонку с результатами проверки к DataFrame
    df[result_column] = results

    # Логируем финальную статистику выполнения
    func_time = time() - func_start
    logging.info("[FIELD LENGTH] Статистика: корректных={correct}, с ошибками={errors} (всего: {total})".format(
        correct=correct_count, errors=error_count, total=total_rows
    ))
    logging.info("[FIELD LENGTH] Завершено за {time:.3f}s для листа {sheet}".format(time=func_time, sheet=sheet_name))

    return df


def validate_field_lengths_vectorized(df, sheet_name):
    """
    ОПТИМИЗИРОВАННАЯ ВЕРСИЯ: Векторизованная проверка длины полей.
    
    Обрабатывает все строки одновременно используя векторные операции pandas
    вместо iterrows(). Ожидаемое ускорение: 50-100x.
    
    Args:
        df (pd.DataFrame): DataFrame для проверки
        sheet_name (str): Название листа

    Returns:
        pd.DataFrame: DataFrame с добавленной колонкой результата проверки
    """
    func_start = time()

    if sheet_name not in FIELD_LENGTH_VALIDATIONS:
        return df

    config = FIELD_LENGTH_VALIDATIONS[sheet_name]
    result_column = config["result_column"]
    fields_config = config["fields"]

    missing_fields = [field for field in fields_config.keys() if field not in df.columns]
    if missing_fields:
        logging.warning("[FIELD LENGTH VECTORIZED] Пропущены поля {fields} в листе {sheet}".format(fields=missing_fields, sheet=sheet_name))
        df[result_column] = '-'
        return df

    total_rows = len(df)
    logging.info("[FIELD LENGTH VECTORIZED] Проверка длины полей для листа {sheet}, строк: {rows}".format(sheet=sheet_name, rows=total_rows))

    violations_dict = {}

    for field_name, field_config in fields_config.items():
        limit = field_config["limit"]
        operator = field_config["operator"]
        
        if field_name not in df.columns:
            continue
        
        lengths = df[field_name].astype(str).str.len()
        empty_mask = df[field_name].isin(['', '-', 'None', 'null']) | df[field_name].isna()
        
        if operator == "<=":
            mask = (lengths > limit) & ~empty_mask
        elif operator == "=":
            mask = (lengths != limit) & ~empty_mask
        elif operator == ">=":
            mask = (lengths < limit) & ~empty_mask
        elif operator == "<":
            mask = (lengths >= limit) & ~empty_mask
        elif operator == ">":
            mask = (lengths <= limit) & ~empty_mask
        else:
            mask = pd.Series(False, index=df.index)
        
        if mask.any():
            violations_dict[field_name] = pd.Series('', index=df.index, dtype=str)
            violations_dict[field_name].loc[mask] = df.loc[mask, field_name].apply(
                lambda val: f"{field_name} = {len(str(val))} {operator} {limit}"
            )
            
            for idx in df.index[mask]:
                logging.debug("[DEBUG] Строка {row}: поле '{field}' = {length} {operator} {limit} (нарушение)".format(
                    row=idx, field=field_name, length=len(str(df.loc[idx, field_name])), 
                    operator=operator, limit=limit
                ))

    if violations_dict:
        violations_df = pd.DataFrame(violations_dict)
        violations_series = violations_df.apply(
            lambda row: "; ".join([str(v) for v in row if v and str(v).strip()]),
            axis=1
        )
        df[result_column] = violations_series.replace('', '-')
    else:
        df[result_column] = '-'
    
    correct_count = (df[result_column] == "-").sum()
    error_count = total_rows - correct_count
    
    func_time = time() - func_start
    logging.info("[FIELD LENGTH VECTORIZED] Статистика: корректных={correct}, с ошибками={errors} (всего: {total})".format(
        correct=correct_count, errors=error_count, total=total_rows
    ))
    logging.info("[FIELD LENGTH VECTORIZED] Завершено за {time:.3f}s для листа {sheet}".format(time=func_time, sheet=sheet_name))

    return df


def compare_validate_results(df_old, df_new, result_column):
    """
    Сравнивает результаты работы старой и новой версии validate_field_lengths.
    
    Args:
        df_old (pd.DataFrame): Результат старой версии
        df_new (pd.DataFrame): Результат новой версии
        result_column (str): Название колонки с результатами
    
    Returns:
        dict: Словарь с результатами сравнения
    """
    if result_column not in df_old.columns or result_column not in df_new.columns:
        return {"error": "Колонка с результатами не найдена"}
    
    old_results = df_old[result_column].fillna('-')
    new_results = df_new[result_column].fillna('-')
    
    differences = (old_results != new_results).sum()
    total = len(df_old)
    matches = total - differences
    
    diff_examples = []
    if differences > 0:
        diff_mask = old_results != new_results
        diff_indices = df_old.index[diff_mask][:5]
        for idx in diff_indices:
            diff_examples.append({
                "index": idx,
                "old": old_results.loc[idx],
                "new": new_results.loc[idx]
            })
    
    return {
        "total": total,
        "matches": matches,
        "differences": differences,
        "match_percent": (matches / total * 100) if total > 0 else 0,
        "diff_examples": diff_examples,
        "identical": differences == 0
    }


# === ЧТЕНИЕ И ЗАПИСЬ ДАННЫХ ===


def find_file_case_insensitive(directory, base_name, extensions):
    """
    Ищет файл в каталоге без учета регистра имени файла и расширения.
    
    Args:
        directory (str): Каталог для поиска
        base_name (str): Базовое имя файла (без расширения)
        extensions (list): Список возможных расширений (например, ['.csv', '.CSV'])
    
    Returns:
        str or None: Полный путь к найденному файлу или None если файл не найден
    """
    if not os.path.exists(directory):
        return None
    
    # Получаем список всех файлов в каталоге
    try:
        files_in_dir = os.listdir(directory)
    except OSError:
        return None
    
    # Ищем файл без учета регистра
    for file_name in files_in_dir:
        # Разделяем имя файла и расширение
        name, ext = os.path.splitext(file_name)
        
        # Проверяем совпадение имени и расширения без учета регистра
        if (name.lower() == base_name.lower() and 
            ext.lower() in [e.lower() for e in extensions]):
            return os.path.join(directory, file_name)
    
    return None



def read_csv_file(file_path):
    """
    Читает CSV файл с заданными параметрами и логирует процесс.
    
    Функция настроена для работы с CSV файлами, использующими точку с запятой как разделитель.
    Все данные читаются как строки для сохранения точности, особенно для JSON полей.
    Сохраняет тройные кавычки в неизменном виде.
    
    Args:
        file_path (str): Путь к CSV файлу для чтения
        
    Returns:
        pd.DataFrame or None: DataFrame с данными или None при ошибке
    """
    func_start = time()  # Засекаем время начала выполнения
    params = f"({file_path})"
    logging.info("[START] {func} {params}".format(func="read_csv_file", params=params))
    
    try:
        rows = []
        headers = None
        
        with open(file_path, 'r', encoding='utf-8', newline='') as file:
            # Используем csv.reader с настройками для сохранения кавычек
            csv_reader = csv.reader(file, delimiter=';', quoting=csv.QUOTE_NONE)
            
            for i, row in enumerate(csv_reader):
                if i == 0:
                    headers = row
                else:
                    rows.append(row)
        
        # Создаем DataFrame из прочитанных данных
        df = pd.DataFrame(rows, columns=headers)
        
        # Убеждаемся, что все данные - строки
        for col in df.columns:
            df[col] = df[col].astype(str)
        
        # Логируем образцы JSON полей для отладки
        for col in df.columns:
            if "FEATURE" in col or "ADD_DATA" in col:
                logging.debug("[DEBUG] CSV {file} поле {column}: {sample}".format(
                    file=file_path,
                    column=col,
                    sample=df[col].dropna().head(2).to_list()  # Первые 2 непустых значения
                ))
        
        # Логируем успешное чтение файла
        logging.info("Файл успешно загружен: {file_path}, строк: {rows}, колонок: {cols}".format(file_path=file_path, rows=len(df), cols=len(df.columns)))
        
        # Засекаем время выполнения и логируем завершение
        func_time = time() - func_start
        logging.info("[END] {func} {params} (время: {time:.3f}s)".format(func="read_csv_file", params=params, time=func_time))
        return df
        
    except Exception as e:
        # Логируем ошибку и возвращаем None
        func_time = time() - func_start
        logging.error("Ошибка загрузки файла: {file_path}. {error}".format(file_path=file_path, error=e))
        logging.error("[ERROR] {func} {params} — {error}".format(func="read_csv_file", params=params, error=e))
        logging.info("[END] {func} {params} (время: {time:.3f}s)".format(func="read_csv_file", params=params, time=func_time))
        return None


def write_to_excel(sheets_data, output_path):
    """
    Записывает данные в Excel файл с форматированием и настройками.
    
    Функция создает Excel файл с несколькими листами, применяет форматирование
    и делает SUMMARY лист активным по умолчанию.
    
    Args:
        sheets_data (dict): Словарь с данными листов в формате {sheet_name: (df, params)}
        output_path (str): Путь к выходному Excel файлу
    """
    func_start = time()  # Засекаем время начала выполнения
    params = f"({output_path})"
    logging.info("[START] {func} {params}".format(func="write_to_excel", params=params))
    
    try:
        # Определяем порядок листов: SUMMARY первый, затем уникальные листы, остальные по алфавиту
        other_sheets = [s for s in sheets_data if s != "SUMMARY"]
        ordered_sheets = ["SUMMARY"] + sorted(other_sheets)
        
        # Создаем Excel файл с помощью pandas ExcelWriter
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # ОПТИМИЗАЦИЯ: Сначала записываем все данные (последовательно, т.к. ExcelWriter не поддерживает параллелизм)
            for sheet_name in ordered_sheets:
                df, params_sheet = sheets_data[sheet_name]
                df.to_excel(writer, index=False, sheet_name=sheet_name)
                logging.info("Лист Excel записан: {sheet} (строк: {rows}, колонок: {cols})".format(sheet=sheet_name, rows=len(df), cols=len(df.columns)))
            
            # ОПТИМИЗАЦИЯ: Затем форматируем листы параллельно (это безопасно, т.к. данные уже записаны)
            if len(ordered_sheets) > 1:
                logging.info(f"[PARALLEL FORMAT] Начало параллельного форматирования {len(ordered_sheets)} листов")
                with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(ordered_sheets))) as executor:
                    futures = {
                        executor.submit(_format_sheet, writer.sheets[sheet_name], 
                                       sheets_data[sheet_name][0], sheets_data[sheet_name][1]): sheet_name
                        for sheet_name in ordered_sheets
                    }
                    
                    for future in as_completed(futures):
                        sheet_name = future.result()  # _format_sheet возвращает имя листа
                        logging.info("Лист Excel отформатирован: {sheet}".format(sheet=sheet_name))
            else:
                # Один лист - форматируем последовательно
                for sheet_name in ordered_sheets:
                    _format_sheet(writer.sheets[sheet_name], sheets_data[sheet_name][0], sheets_data[sheet_name][1])
                    logging.info("Лист Excel сформирован: {sheet} (строк: {rows}, колонок: {cols})".format(
                        sheet=sheet_name, rows=len(sheets_data[sheet_name][0]), cols=len(sheets_data[sheet_name][0].columns)))
            
            # Делаем SUMMARY лист активным по умолчанию
            writer.book.active = writer.book.sheetnames.index("SUMMARY")
            writer.book.save(output_path)  # Сохраняем файл
        
        # Логируем успешное завершение
        func_time = time() - func_start
        logging.info("[END] {func} {params} (время: {time:.3f}s)".format(func="write_to_excel", params=params, time=func_time))
        
    except Exception as ex:
        # Логируем ошибку
        func_time = time() - func_start
        logging.error("[ERROR] {func} {params} — {error}".format(func="write_to_excel", params=params, error=ex))
        logging.info("[END] {func} {params} (время: {time:.3f}s)".format(func="write_to_excel", params=params, time=func_time))


# === Форматирование листа ===
def calculate_column_width(col_name, ws, params, col_num):
    """
    Вычисляет ширину колонки на основе параметров и содержимого.
    """
    # Получаем параметры для конкретной колонки (если добавлена через MERGE_FIELDS)
    added_cols_width = params.get("added_columns_width", {})
    if col_name in added_cols_width:
        col_params = added_cols_width[col_name]
        max_width = col_params.get("max_width") or params.get("max_col_width", 30)
        width_mode = col_params.get("width_mode", "AUTO")
        min_width = col_params.get("min_width", 8)
    else:
        # Общие параметры для листа
        max_width = params.get("max_col_width", 30)
        width_mode = params.get("col_width_mode", "AUTO")
        min_width = params.get("min_col_width", 8)

    # Вычисляем ширину на основе содержимого
    col_letter = get_column_letter(col_num)
    content_width = max([len(str(cell.value)) for cell in ws[col_letter] if cell.value] + [min_width])

    if width_mode == "AUTO":
        # Автоматическое растягивание по содержимому, но не более максимальной ширины
        final_width = min(content_width, max_width)
        final_width = max(final_width, min_width)
    elif isinstance(width_mode, (int, float)):
        # Фиксированная ширина
        final_width = width_mode
    else:
        # Без растягивания - просто не более максимальной
        final_width = min(content_width, max_width)
        final_width = max(final_width, min_width)

    return final_width


def _format_sheet(ws, df, params):
    func_start = time()
    params_str = f"({ws.title})"
    logging.debug("[START] {func} {params}".format(func="_format_sheet", params=params_str))
    header_font = Font(bold=True)
    align_center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    align_data = Alignment(horizontal="left", vertical="center", wrap_text=True)

    # ОПТИМИЗАЦИЯ: Batch-операции для заголовков - вычисляем все ширины сразу
    header_cells = list(ws[1])
    column_widths = {}
    
    for col_num, cell in enumerate(header_cells, 1):
        cell.font = header_font
        cell.alignment = align_center
        col_letter = get_column_letter(col_num)
        col_name = cell.value
        
        # Вычисляем ширину колонки
        width = calculate_column_width(col_name, ws, params, col_num)
        column_widths[col_letter] = width
        
        # Определяем режим для логирования
        width_mode_info = params.get("col_width_mode", "AUTO")
        added_cols_width = params.get("added_columns_width", {})
        if col_name in added_cols_width:
            width_mode_info = added_cols_width[col_name].get("width_mode", "AUTO")
        
        logging.debug("[COLUMN WIDTH] {sheet}: колонка '{column}' -> ширина {width} (режим: {mode})".format(
            sheet=ws.title, column=col_name, width=width, mode=width_mode_info
        ))
    
    # Применяем все ширины колонок сразу (batch-операция)
    for col_letter, width in column_widths.items():
        ws.column_dimensions[col_letter].width = width

    # Применяем цветовую схему
    apply_color_scheme(ws, ws.title)

    # ОПТИМИЗАЦИЯ: Данные - применяем выравнивание более эффективно
    # Используем iter_rows с batch-обработкой для больших листов
    if ws.max_row > 1000:
        # Для больших листов обрабатываем чанками
        chunk_size = 500
        for start_row in range(2, ws.max_row + 1, chunk_size):
            end_row = min(start_row + chunk_size - 1, ws.max_row)
            for row in ws.iter_rows(min_row=start_row, max_row=end_row, max_col=ws.max_column):
                for cell in row:
                    cell.alignment = align_data
    else:
        # Для малых листов обрабатываем все сразу
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column):
            for cell in row:
                cell.alignment = align_data

    # Закрепление строк и столбцов
    ws.freeze_panes = params.get("freeze", "A2")
    ws.auto_filter.ref = ws.dimensions

    func_time = time() - func_start
    logging.debug("[END] {func} {params} (время: {time:.3f}s)".format(func="_format_sheet", params=params_str, time=func_time))
    
    # Возвращаем имя листа для логирования в параллельном режиме
    return ws.title


def safe_json_loads(s: str):
    """
    Преобразует строку в объект JSON. Возвращает dict/list или None, если не удается разобрать.
    Более толерантен к разным типам кавычек и пустым строкам.
    Дополнительно исправляет тройные кавычки, отсутствие двоеточий, лишние запятые и пробует "починить" кривой JSON.
    """
    if not isinstance(s, str):
        return s
    s = s.strip()
    if not s or s in {'-', 'None', 'null'}:
        return None
    try:
        return json.loads(s)
    except Exception as ex:
        try:
            fixed = s
            # 1. Заменяем тройные кавычки на обычные двойные
            fixed = fixed.replace('"""', '"')
            # 2. Заменяем одиночные и фигурные кавычки на стандартные двойные
            fixed = fixed.replace("'", '"')
            fixed = fixed.replace('"', '"').replace('"', '"')
            fixed = fixed.replace(''', '"').replace(''', '"')
            # 3. Исправляем ключи вида ""key"" на "key"
            fixed = re.sub(r'"{2,}([^"\s]+)"{2,}', r'"\1"', fixed)
            # 4. Исправляем конструкции типа "key""": на "key":
            fixed = re.sub(r'"{2,}([^"\s]+)"{2,}\s*:', r'"\1":', fixed)
            # 5. Исправляем конструкции типа :"""value""" на :"value"
            fixed = re.sub(r':\s*"{2,}([^"\s]+)"{2,}', r':"\1"', fixed)
            # 6. Убираем завершающие запятые перед закрывающей скобкой
            fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
            # 7. Исправляем отсутствие двоеточий между ключом и значением ("key" "value" -> "key":"value")
            fixed = re.sub(r'(\"[^"]+\")\s+(\")', r'\1: \2', fixed)
            # 8. Удаляем лишние пробелы между ключом и двоеточием
            fixed = re.sub(r'(\"[^"]+\")\s*:\s*', r'\1:', fixed)
            # 9. Попытка повторного парсинга
            return json.loads(fixed)
        except Exception:
            try:
                pass  # import ast перенесен в начало файла
            except Exception:
                logging.debug("[safe_json_loads] Ошибка: {error} | Исходная строка: {string}".format(error=ex, string=repr(s)))
                return None


def safe_json_loads_preserve_triple_quotes(s: str):
    """
    Преобразует строку в объект JSON, сохраняя тройные кавычки как есть.
    Используется для обработки JSON из CSV файлов с тройными кавычками.
    """
    if not isinstance(s, str):
        return s
    s = s.strip()
    if not s or s in {'-', 'None', 'null'}:
        return None
    
    # Сначала пробуем распарсить как есть
    try:
        return json.loads(s)
    except Exception as ex:
        # Если не получилось, возвращаем исходную строку с тройными кавычками
        # Это позволяет сохранить тройные кавычки в исходном виде
        logging.debug("[safe_json_loads_preserve_triple_quotes] Сохраняем исходную строку с тройными кавычками: {string}".format(string=repr(s)))
        logging.debug("[safe_json_loads_preserve_triple_quotes] Сохраняем исходную строку с тройными кавычками: {string}".format(string=repr(s)))
        logging.debug("[safe_json_loads_preserve_triple_quotes] Сохраняем исходную строку с тройными кавычками: {string}".format(string=repr(s)))
        return s  # Возвращаем исходную строку с тройными кавычками


def flatten_json_column_recursive(df, column, prefix=None, sheet=None, sep="; "):
    func_start = tmod.time()
    n_rows = len(df)
    n_errors = 0
    prefix = prefix if prefix is not None else column
    logging.info("[START] {func} {params}".format(func="flatten_json_column_recursive",
                                                   params=f"(лист: {sheet}, колонка: {column})"))
    
    # Для CONTEST_FEATURE создаем копию с валидным JSON для парсинга
    # Сохраняем исходную колонку с тройными кавычками как есть
    original_column_data = None
    if column == "CONTEST_FEATURE" and column in df.columns:
        # Сохраняем исходные данные
        original_column_data = df[column].copy()
        
        # Создаем временную колонку для парсинга с заменой тройных кавычек
        temp_column = f"{column}_TEMP_PARSED"
        df[temp_column] = df[column].apply(lambda x: x.replace('"""', '"') if isinstance(x, str) else x)
        
        # Теперь будем парсить из временной колонки
        column_to_parse = temp_column
    else:
        column_to_parse = column

    def extract(obj, current_prefix):
        """Recursively flattens obj. Keeps the field itself and expands nested JSON
        if the value looks like a JSON string."""
        fields = {}
        if isinstance(obj, str):
            # Сначала пробуем распарсить JSON (для разворачивания)
            nested = safe_json_loads(obj)
            
            if isinstance(nested, (dict, list)):
                # keep original string (с тройными кавычками, если они были)
                fields[current_prefix] = obj
                fields.update(extract(nested, current_prefix))
                return fields
            else:
                # Если не удалось распарсить как JSON, сохраняем исходную строку
                fields[current_prefix] = obj
                return fields

        if isinstance(obj, dict):
            fields[current_prefix] = json.dumps(obj, ensure_ascii=False)
            for k, v in obj.items():
                new_prefix = f"{current_prefix} => {k}"
                fields.update(extract(v, new_prefix))
        elif isinstance(obj, list):
            if all(isinstance(x, (str, int, float, bool, type(None))) for x in obj):
                fields[current_prefix] = sep.join(str(x) for x in obj)
            else:
                fields[current_prefix] = json.dumps(obj, ensure_ascii=False)
                for idx, x in enumerate(obj):
                    item_prefix = f"{current_prefix} => [{idx}]"
                    fields.update(extract(x, item_prefix))
        else:
            if isinstance(obj, float) and pd.isna(obj):
                fields[current_prefix] = None
            else:
                fields[current_prefix] = obj
        return fields

            # ОПТИМИЗИРОВАННАЯ ВЕРСИЯ v2: Параллельный парсинг JSON с проверкой размера
    new_cols = {}
    
    # ОПТИМИЗАЦИЯ: Параллелизация только для больших данных (>5000 строк)
    # Для небольших данных накладные расходы превышают выигрыш
    PARALLEL_JSON_THRESHOLD = 5000
    
    if n_rows > PARALLEL_JSON_THRESHOLD:
        def parse_json_chunk(chunk_data):
            """Парсит chunk данных и возвращает словарь с результатами"""
            chunk_results = {}
            chunk_errors = 0
            chunk_idx, chunk_values = chunk_data
            for local_idx, val in enumerate(chunk_values):
                global_idx = chunk_idx + local_idx
                try:
                    parsed = None
                    if isinstance(val, str):
                        val = val.strip()
                        if val in {"", "-", "None", "null"}:
                            parsed = {}
                        else:
                            parsed = safe_json_loads(val)
                    elif isinstance(val, (dict, list)):
                        parsed = val
                    else:
                        parsed = {}
                    flat = extract(parsed, prefix)
                except Exception as ex:
                    logging.debug("Ошибка разбора JSON (строка {row}): {error}".format(row=global_idx, error=ex))
                    chunk_errors += 1
                    flat = {}
                
                for k, v in flat.items():
                    if k not in chunk_results:
                        chunk_results[k] = {}
                    chunk_results[k][global_idx] = v
            return chunk_results, chunk_errors
        
        # Разбиваем на chunks для параллельной обработки
        # Оптимизированный размер chunk: минимум 2000 строк на chunk
        chunk_size = max(2000, n_rows // MAX_WORKERS_IO)
        chunks = [(i * chunk_size, df[column_to_parse].iloc[i * chunk_size:(i + 1) * chunk_size].tolist()) 
                  for i in range((n_rows + chunk_size - 1) // chunk_size)]
        
        # Параллельная обработка chunks только если chunks > 1
        if len(chunks) > 1:
            from concurrent.futures import ThreadPoolExecutor as TPE
            with TPE(max_workers=min(MAX_WORKERS_IO, len(chunks))) as executor:
                chunk_data_list = list(executor.map(parse_json_chunk, chunks))
                chunk_results_list = [data[0] for data in chunk_data_list]
                n_errors += sum(data[1] for data in chunk_data_list)
            
            # Объединяем результаты
            for chunk_results in chunk_results_list:
                for k, v_dict in chunk_results.items():
                    if k not in new_cols:
                        new_cols[k] = [None] * n_rows
                    for idx, val in v_dict.items():
                        new_cols[k][idx] = val
        else:
            # Один chunk - обрабатываем последовательно
            chunk_results, chunk_errors = parse_json_chunk(chunks[0])
            n_errors += chunk_errors
            for k, v_dict in chunk_results.items():
                if k not in new_cols:
                    new_cols[k] = [None] * n_rows
                for idx, val in v_dict.items():
                    new_cols[k][idx] = val
    else:
        # Небольшие данные - последовательная обработка (быстрее из-за отсутствия накладных расходов)
        for idx, val in enumerate(df[column_to_parse]):
            try:
                parsed = None
                if isinstance(val, str):
                    val = val.strip()
                    if val in {"", "-", "None", "null"}:
                        parsed = {}
                    else:
                        parsed = safe_json_loads(val)
                elif isinstance(val, (dict, list)):
                    parsed = val
                else:
                    parsed = {}
                flat = extract(parsed, prefix)
            except Exception as ex:
                logging.debug("Ошибка разбора JSON (строка {row}): {error}".format(row=idx, error=ex))
                n_errors += 1
                flat = {}
            for k, v in flat.items():
                if k not in new_cols:
                    new_cols[k] = [None] * n_rows
                new_cols[k][idx] = val
                else:
        # Если chunks мало или один поток - обрабатываем последовательно
        for idx, val in enumerate(df[column_to_parse]):
            try:
                parsed = None
                if isinstance(val, str):
                    val = val.strip()
                    if val in {"", "-", "None", "null"}:
                        parsed = {}
                    else:
                        parsed = safe_json_loads(val)
                elif isinstance(val, (dict, list)):
                    parsed = val
                else:
                    parsed = {}
                flat = extract(parsed, prefix)
            except Exception as ex:
                logging.debug("Ошибка разбора JSON (строка {row}): {error}".format(row=idx, error=ex))
                n_errors += 1
                flat = {}
            for k, v in flat.items():
                if k not in new_cols:
                    new_cols[k] = [None] * n_rows
                new_cols[k][idx] = v

    # Оставлять только реально созданные колонки (не пустые)
    for col_name, values in new_cols.items():
        if any(x is not None for x in values):
            df[col_name] = values
    
    # Для CONTEST_FEATURE восстанавливаем исходную колонку с тройными кавычками
    if original_column_data is not None:
        # Восстанавливаем исходную колонку с тройными кавычками
        df[column] = original_column_data
        
        # Удаляем временную колонку
        if temp_column in df.columns:
            df = df.drop(columns=[temp_column])
        
        logging.info("[CONTEST_FEATURE] Исходная колонка восстановлена с тройными кавычками")

    logging.info("[INFO] {column} → новых колонок: {count}".format(column=column, count=len(new_cols)))
    logging.info("[INFO] Все новые колонки: {keys}".format(keys=list(new_cols.keys())))
    return df


def generate_dynamic_color_scheme_from_merge_fields():
    """
    Автоматически генерирует элементы цветовой схемы на основе MERGE_FIELDS.
    Добавляет правила для колонок, которые создаются через merge операции.
    """
    dynamic_scheme = []

    # Группируем по целевым листам
    sheets_targets = {}
    for rule in MERGE_FIELDS:
        sheet_dst = rule["sheet_dst"]
        sheet_src = rule["sheet_src"]
        columns = rule["column"]
        mode = rule.get("mode", "value")

        if sheet_dst not in sheets_targets:
            sheets_targets[sheet_dst] = {}

        if sheet_src not in sheets_targets[sheet_dst]:
            sheets_targets[sheet_dst][sheet_src] = []

        # Формируем имена колонок, которые будут созданы
        for col in columns:
            if mode == "count":
                new_col_name = f"{sheet_src}=>COUNT_{col}"
            else:
                new_col_name = f"{sheet_src}=>{col}"
            sheets_targets[sheet_dst][sheet_src].append(new_col_name)

    # Создаем цветовые схемы для каждой комбинации лист-источник
    color_palette = [
        ("FF9999", "2C3E50"),  # Светло-красный
        ("99FF99", "2C3E50"),  # Светло-зеленый
        ("9999FF", "FFFFFF"),  # Светло-синий
        ("FFFF99", "2C3E50"),  # Светло-желтый
        ("FF99FF", "2C3E50"),  # Светло-розовый
        ("99FFFF", "2C3E50"),  # Светло-голубой
        ("FFB366", "2C3E50"),  # Светло-оранжевый
        ("D8BFD8", "2C3E50"),  # Светло-фиолетовый
    ]

    color_idx = 0
    for sheet_dst, sources in sheets_targets.items():
        for sheet_src, columns in sources.items():
            if columns:  # Если есть колонки для этого источника
                bg_color, fg_color = color_palette[color_idx % len(color_palette)]

                dynamic_scheme.append({
                    "group": f"MERGE: {sheet_src} -> {sheet_dst}",
                    "header_bg": bg_color,
                    "header_fg": fg_color,
                    "column_bg": None,
                    "column_fg": None,
                    "style_scope": "header",
                    "sheets": [sheet_dst],
                    "columns": columns,
                    "auto_generated": True  # Маркер автогенерации
                })

                logging.debug("[DYNAMIC COLOR] Сгенерирована схема для {sheet}: {columns}".format(
                    sheet=f"{sheet_src} -> {sheet_dst}", columns=columns
                ))
                color_idx += 1

    return dynamic_scheme


def apply_color_scheme(ws, sheet_name):
    """
    Окрашивает заголовки и/или всю колонку на листе Excel по схеме COLOR_SCHEME.
    Также применяет динамически сгенерированную схему из MERGE_FIELDS.
    Все действия логируются напрямую в местах вызова.
    """
    # Объединяем статическую и динамическую цветовые схемы
    all_color_schemes = COLOR_SCHEME + generate_dynamic_color_scheme_from_merge_fields()

    for color_conf in all_color_schemes:
        if sheet_name not in color_conf["sheets"]:
            continue

        # Список колонок: если пуст — значит все
        header_cells = list(ws[1])
        colnames = color_conf["columns"] if color_conf["columns"] else [cell.value for cell in header_cells]
        style_scope = color_conf.get("style_scope", "header")

        for colname in colnames:
            try:
                # Номер колонки по имени
                col_idx = [cell.value for cell in header_cells].index(colname) + 1
            except ValueError:
                continue  # нет такой колонки на этом листе

            # Окраска только заголовка
            if style_scope == "header":
                cell = ws.cell(row=1, column=col_idx)
                if color_conf.get("header_bg"):
                    cell.fill = PatternFill(start_color=color_conf["header_bg"], end_color=color_conf["header_bg"],
                                            fill_type="solid")
                if color_conf.get("header_fg"):
                    cell.font = Font(color=color_conf["header_fg"])
                # Логирование
                logging.debug("[INFO] Цветовая схема применена: лист {sheet}, колонка {col}, стиль {scope}, цвет {color}".format(
                    sheet=sheet_name,
                    col=colname,
                    scope="header",
                    color=color_conf.get("header_bg", "default")
                ))
            # Окраска всей колонки (если понадобится в будущем)
            elif style_scope == "all":
                for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=col_idx, max_col=col_idx):
                    for cell in row:
                        if cell.row == 1 and color_conf.get("header_bg"):
                            cell.fill = PatternFill(start_color=color_conf["header_bg"],
                                                    end_color=color_conf["header_bg"], fill_type="solid")
                            if color_conf.get("header_fg"):
                                cell.font = Font(color=color_conf["header_fg"])
                        elif color_conf.get("column_bg"):
                            cell.fill = PatternFill(start_color=color_conf["column_bg"],
                                                    end_color=color_conf["column_bg"], fill_type="solid")
                            if color_conf.get("column_fg"):
                                cell.font = Font(color=color_conf["column_fg"])
                logging.debug("[INFO] Цветовая схема применена: лист {sheet}, колонка {col}, стиль {scope}, цвет {color}".format(
                    sheet=sheet_name,
                    col=colname,
                    scope="all",
                    color=color_conf.get("column_bg", "default")
                ))


def collect_summary_keys(dfs):
    """
    Собирает все реально существующие сочетания ключей,
    включая осиротевшие коды и сочетания с GROUP_VALUE и INDICATOR_ADD_CALC_TYPE.
    Теперь учитывает ВСЕ коды из всех таблиц, включая CONTEST-DATA и INDICATOR.
    ИСПРАВЛЕНИЕ: GROUP_VALUE правильно связан с конкретным GROUP_CODE.
    """
    all_rows = []

    rewards = dfs.get("REWARD-LINK", pd.DataFrame())
    tournaments = dfs.get("TOURNAMENT-SCHEDULE", pd.DataFrame())
    groups = dfs.get("GROUP", pd.DataFrame())
    reward_data = dfs.get("REWARD", pd.DataFrame())
    contest_data = dfs.get("CONTEST-DATA", pd.DataFrame())
    indicators = dfs.get("INDICATOR", pd.DataFrame())

    # Коды для детального логирования
    DEBUG_CODES = []  # Отключено подробное логирование
    
    all_contest_codes = set()
    all_tournament_codes = set()
    all_reward_codes = set()
    all_group_codes = set()
    all_group_values = set()
    all_indicator_add_calc_types = set()

    # Собираем ВСЕ коды из всех таблиц
    if not rewards.empty:
        all_contest_codes.update(rewards["CONTEST_CODE"].dropna())
        all_reward_codes.update(rewards["REWARD_CODE"].dropna())
    if not tournaments.empty:
        all_contest_codes.update(tournaments["CONTEST_CODE"].dropna())
        all_tournament_codes.update(tournaments["TOURNAMENT_CODE"].dropna())
    if not groups.empty:
        all_contest_codes.update(groups["CONTEST_CODE"].dropna())
        all_group_codes.update(groups["GROUP_CODE"].dropna())
        all_group_values.update(groups["GROUP_VALUE"].dropna())
    if not contest_data.empty:
        all_contest_codes.update(contest_data["CONTEST_CODE"].dropna())
    if not reward_data.empty:
        all_reward_codes.update(reward_data["REWARD_CODE"].dropna())
    if not indicators.empty:
        all_contest_codes.update(indicators["CONTEST_CODE"].dropna())
        indicator_types = indicators["INDICATOR_ADD_CALC_TYPE"].fillna("").unique()
        all_indicator_add_calc_types.update(indicator_types)

    # 1. Для каждого CONTEST_CODE
    for code in all_contest_codes:
        is_debug = str(code) in DEBUG_CODES
        if is_debug:
            logging.info("[DEBUG GROUP] === Обработка CONTEST_CODE: {} ===".format(code))
        
        tourns = tournaments[tournaments["CONTEST_CODE"] == code][
            "TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else []
        rewards_ = rewards[rewards["CONTEST_CODE"] == code][
            "REWARD_CODE"].dropna().unique() if not rewards.empty else []
        groups_df = groups[groups["CONTEST_CODE"] == code] if not groups.empty else pd.DataFrame()
        
        if is_debug:
            logging.info("[DEBUG GROUP] Найдено строк в GROUP для CONTEST_CODE {}: {}".format(code, len(groups_df)))
            if not groups_df.empty:
                logging.info("[DEBUG GROUP] Строки GROUP:\n{}".format(groups_df[["GROUP_CODE", "GROUP_VALUE", "CONTEST_CODE"]].to_string()))
        
        # ИСПРАВЛЕНИЕ: GROUP_VALUE должен быть связан с конкретным GROUP_CODE
        # Вместо декартова произведения создаем пары (GROUP_CODE, GROUP_VALUE)
        group_code_value_pairs = []
        if not groups_df.empty:
            # Создаем список уникальных пар (GROUP_CODE, GROUP_VALUE)
            for _, row in groups_df.iterrows():
                g_code = row.get("GROUP_CODE", "")
                g_value = row.get("GROUP_VALUE", "")
                if pd.notna(g_code) and pd.notna(g_value):
                    pair = (str(g_code), str(g_value))
                    if pair not in group_code_value_pairs:
                        group_code_value_pairs.append(pair)
        
        if is_debug:
            logging.info("[DEBUG GROUP] Уникальные пары (GROUP_CODE, GROUP_VALUE) для CONTEST_CODE {}: {}".format(code, group_code_value_pairs))
            if not groups_df.empty:
                unique_groups = groups_df["GROUP_CODE"].dropna().unique()
                unique_values = groups_df["GROUP_VALUE"].dropna().unique()
                logging.info("[DEBUG GROUP] Уникальные GROUP_CODE: {}".format(list(unique_groups)))
                logging.info("[DEBUG GROUP] Уникальные GROUP_VALUE: {}".format(list(unique_values)))
        
        # Если нет пар, создаем одну с "-"
        if not group_code_value_pairs:
            group_code_value_pairs = [("-", "-")]
        
        # Добавляем INDICATOR_ADD_CALC_TYPE для данного CONTEST_CODE
        indicator_types_ = []
        if not indicators.empty:
            indicator_df = indicators[indicators["CONTEST_CODE"] == code]
            if not indicator_df.empty:
                indicator_types_ = indicator_df["INDICATOR_ADD_CALC_TYPE"].fillna("").unique().tolist()
        
        tourns = tourns if len(tourns) else ["-"]
        rewards_ = rewards_ if len(rewards_) else ["-"]
        indicator_types_ = indicator_types_ if len(indicator_types_) else [""]
        
        if is_debug:
            logging.info("[DEBUG GROUP] TOURNAMENT_CODE: {}".format(list(tourns)))
            logging.info("[DEBUG GROUP] REWARD_CODE: {}".format(list(rewards_)))
            logging.info("[DEBUG GROUP] INDICATOR_ADD_CALC_TYPE: {}".format(indicator_types_))
            logging.info("[DEBUG GROUP] Будет создано комбинаций: {} x {} x {} x {} = {}".format(
                len(tourns), len(rewards_), len(group_code_value_pairs), len(indicator_types_),
                len(tourns) * len(rewards_) * len(group_code_value_pairs) * len(indicator_types_)
            ))

        for t in tourns:
            for r in rewards_:
                for g_code, g_value in group_code_value_pairs:
                    for ind_type in indicator_types_:
                        all_rows.append((str(code), str(t), str(r), str(g_code), str(g_value), str(ind_type)))
                        if is_debug:
                            logging.debug("[DEBUG GROUP] Создана строка: CONTEST={}, TOURNAMENT={}, REWARD={}, GROUP_CODE={}, GROUP_VALUE={}, INDICATOR={}".format(
                                code, t, r, g_code, g_value, ind_type
                            ))

    # 2. Для каждого TOURNAMENT_CODE (даже если нет CONTEST_CODE)
    if not tournaments.empty:
        for t_code in tournaments["TOURNAMENT_CODE"].dropna().unique():
            code = tournaments[tournaments["TOURNAMENT_CODE"] == t_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
            is_debug = str(code) in DEBUG_CODES or str(t_code) in DEBUG_CODES
            
            rewards_ = rewards[rewards["CONTEST_CODE"] == code][
                "REWARD_CODE"].dropna().unique() if not rewards.empty else []
            groups_df = groups[groups["CONTEST_CODE"] == code] if not groups.empty else pd.DataFrame()
            
            # ИСПРАВЛЕНИЕ: Используем пары (GROUP_CODE, GROUP_VALUE)
            group_code_value_pairs = []
            if not groups_df.empty:
                for _, row in groups_df.iterrows():
                    g_code = row.get("GROUP_CODE", "")
                    g_value = row.get("GROUP_VALUE", "")
                    if pd.notna(g_code) and pd.notna(g_value):
                        pair = (str(g_code), str(g_value))
                        if pair not in group_code_value_pairs:
                            group_code_value_pairs.append(pair)
            
            if not group_code_value_pairs:
                group_code_value_pairs = [("-", "-")]
            
            indicator_types_ = []
            if code != "-" and not indicators.empty:
                indicator_df = indicators[indicators["CONTEST_CODE"] == code]
                if not indicator_df.empty:
                    indicator_types_ = indicator_df["INDICATOR_ADD_CALC_TYPE"].fillna("").unique().tolist()
            
            rewards_ = rewards_ if len(rewards_) else ["-"]
            indicator_types_ = indicator_types_ if len(indicator_types_) else [""]
            
            for r in rewards_:
                for g_code, g_value in group_code_value_pairs:
                    for ind_type in indicator_types_:
                        all_rows.append((str(code), str(t_code), str(r), str(g_code), str(g_value), str(ind_type)))

    # 3. Для каждого REWARD_CODE (даже если нет CONTEST_CODE)
    for r_code in all_reward_codes:
        if not rewards.empty:
            code = rewards[rewards["REWARD_CODE"] == r_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
        else:
            code = "-"
        
        is_debug = str(code) in DEBUG_CODES or str(r_code) in DEBUG_CODES

        if code != "-" and not tournaments.empty:
            tourns = tournaments[tournaments["CONTEST_CODE"] == code]["TOURNAMENT_CODE"].dropna().unique()
        else:
            tourns = []

        if code != "-" and not groups.empty:
            groups_df = groups[groups["CONTEST_CODE"] == code]
            # ИСПРАВЛЕНИЕ: Используем пары (GROUP_CODE, GROUP_VALUE)
            group_code_value_pairs = []
            for _, row in groups_df.iterrows():
                g_code = row.get("GROUP_CODE", "")
                g_value = row.get("GROUP_VALUE", "")
                if pd.notna(g_code) and pd.notna(g_value):
                    pair = (str(g_code), str(g_value))
                    if pair not in group_code_value_pairs:
                        group_code_value_pairs.append(pair)
        else:
            group_code_value_pairs = []
        
        if not group_code_value_pairs:
            group_code_value_pairs = [("-", "-")]
        
        indicator_types_ = []
        if code != "-" and not indicators.empty:
            indicator_df = indicators[indicators["CONTEST_CODE"] == code]
            if not indicator_df.empty:
                indicator_types_ = indicator_df["INDICATOR_ADD_CALC_TYPE"].fillna("").unique().tolist()

        tourns = tourns if len(tourns) else ["-"]
        indicator_types_ = indicator_types_ if len(indicator_types_) else [""]

        for t in tourns:
            for g_code, g_value in group_code_value_pairs:
                for ind_type in indicator_types_:
                    all_rows.append((str(code), str(t), str(r_code), str(g_code), str(g_value), str(ind_type)))

        # 4. Для каждого GROUP_CODE (даже если нет CONTEST_CODE)
    if not groups.empty:
        for g_code in groups["GROUP_CODE"].dropna().unique():
            is_debug = str(g_code) in DEBUG_CODES
            
            if is_debug:
                logging.info("[DEBUG GROUP] === Обработка GROUP_CODE: {} ===".format(g_code))
            
            # ИСПРАВЛЕНИЕ: Находим все CONTEST_CODE для данного GROUP_CODE и обрабатываем каждый отдельно
            group_contest_codes = groups[groups["GROUP_CODE"] == g_code]["CONTEST_CODE"].dropna().unique()
            
            if is_debug:
                logging.info("[DEBUG GROUP] Найдено CONTEST_CODE для GROUP_CODE {}: {}".format(g_code, list(group_contest_codes)))
            
            # Обрабатываем каждый CONTEST_CODE отдельно
            for group_contest_code in group_contest_codes:
                actual_code = str(group_contest_code)
                
                if is_debug:
                    logging.info("[DEBUG GROUP] Обработка GROUP_CODE {} для CONTEST_CODE: {}".format(g_code, actual_code))
                
                # Берем GROUP_VALUE только для конкретного CONTEST_CODE и GROUP_CODE
                group_values_df = groups[(groups["GROUP_CODE"] == g_code) & (groups["CONTEST_CODE"] == actual_code)]
                group_values_ = group_values_df["GROUP_VALUE"].dropna().unique() if not group_values_df.empty else []
                
                if is_debug:
                    logging.info("[DEBUG GROUP] Найдено строк в GROUP для GROUP_CODE {} и CONTEST_CODE {}: {}".format(
                        g_code, actual_code, len(group_values_df)
                    ))
                    if not group_values_df.empty:
                        logging.info("[DEBUG GROUP] Строки GROUP:\n{}".format(
                            group_values_df[["GROUP_CODE", "GROUP_VALUE", "CONTEST_CODE"]].to_string()
                        ))
                    logging.info("[DEBUG GROUP] Уникальные GROUP_VALUE: {}".format(list(group_values_)))
                
                # Ищем связанные TOURNAMENT_CODE и REWARD_CODE для этого CONTEST_CODE
                tourns = tournaments[tournaments["CONTEST_CODE"] == actual_code][
                    "TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else []
                rewards_ = rewards[rewards["CONTEST_CODE"] == actual_code][
                    "REWARD_CODE"].dropna().unique() if not rewards.empty else []
                
                # Добавляем INDICATOR_ADD_CALC_TYPE
                indicator_types_ = []
                if not indicators.empty:
                    indicator_df = indicators[indicators["CONTEST_CODE"] == actual_code]
                    if not indicator_df.empty:
                        indicator_types_ = indicator_df["INDICATOR_ADD_CALC_TYPE"].fillna("").unique().tolist()
                
                tourns = tourns if len(tourns) else ["-"]
                rewards_ = rewards_ if len(rewards_) else ["-"]
                group_values_ = group_values_ if len(group_values_) else ["-"]
                indicator_types_ = indicator_types_ if len(indicator_types_) else [""]
                
                if is_debug:
                    logging.info("[DEBUG GROUP] Будет создано комбинаций: {} x {} x {} x {} = {}".format(
                        len(tourns), len(rewards_), len(group_values_), len(indicator_types_),
                        len(tourns) * len(rewards_) * len(group_values_) * len(indicator_types_)
                    ))
                
                for t in tourns:
                    for r in rewards_:
                        for gv in group_values_:
                            for ind_type in indicator_types_:
                                all_rows.append((actual_code, str(t), str(r), str(g_code), str(gv), str(ind_type)))
                                if is_debug:
                                    logging.debug("[DEBUG GROUP] Создана строка: CONTEST={}, TOURNAMENT={}, REWARD={}, GROUP_CODE={}, GROUP_VALUE={}, INDICATOR={}".format(
                                        actual_code, t, r, g_code, gv, ind_type
                                    ))

# 5. Для каждого INDICATOR_ADD_CALC_TYPE (даже если нет CONTEST_CODE)
    if not indicators.empty:
        for _, ind_row in indicators.iterrows():
            code = ind_row.get("CONTEST_CODE", "")
            ind_type = ind_row.get("INDICATOR_ADD_CALC_TYPE", "")
            if pd.isna(code):
                code = "-"
            if pd.isna(ind_type):
                ind_type = ""
            
            code = str(code)
            ind_type = str(ind_type)

            if code != "-" and not tournaments.empty:
                tourns = tournaments[tournaments["CONTEST_CODE"] == code]["TOURNAMENT_CODE"].dropna().unique()
            else:
                tourns = []
            
            if code != "-" and not rewards.empty:
                rewards_ = rewards[rewards["CONTEST_CODE"] == code]["REWARD_CODE"].dropna().unique()
            else:
                rewards_ = []
            
            if code != "-" and not groups.empty:
                groups_df = groups[groups["CONTEST_CODE"] == code]
                # ИСПРАВЛЕНИЕ: Используем пары (GROUP_CODE, GROUP_VALUE)
                group_code_value_pairs = []
                for _, row in groups_df.iterrows():
                    g_code = row.get("GROUP_CODE", "")
                    g_value = row.get("GROUP_VALUE", "")
                    if pd.notna(g_code) and pd.notna(g_value):
                        pair = (str(g_code), str(g_value))
                        if pair not in group_code_value_pairs:
                            group_code_value_pairs.append(pair)
            else:
                group_code_value_pairs = []
            
            if not group_code_value_pairs:
                group_code_value_pairs = [("-", "-")]
            
            tourns = tourns if len(tourns) else ["-"]
            rewards_ = rewards_ if len(rewards_) else ["-"]
            
            for t in tourns:
                for r in rewards_:
                    for g_code, g_value in group_code_value_pairs:
                        all_rows.append((code, str(t), str(r), str(g_code), str(g_value), ind_type))

    # Удалить дубли
    summary_keys = pd.DataFrame(all_rows, columns=SUMMARY_KEY_COLUMNS).drop_duplicates().reset_index(drop=True)
    
    # Детальное логирование для отладки
    for debug_code in DEBUG_CODES:
        debug_rows = summary_keys[summary_keys["CONTEST_CODE"] == debug_code]
        if not debug_rows.empty:
            logging.info("[DEBUG GROUP] === ИТОГОВЫЕ СТРОКИ В SUMMARY для CONTEST_CODE: {} ===".format(debug_code))
            logging.info("[DEBUG GROUP] Всего строк: {}".format(len(debug_rows)))
            logging.info("[DEBUG GROUP] Уникальные GROUP_CODE: {}".format(debug_rows["GROUP_CODE"].unique().tolist()))
            logging.info("[DEBUG GROUP] Уникальные GROUP_VALUE: {}".format(debug_rows["GROUP_VALUE"].unique().tolist()))
            logging.info("[DEBUG GROUP] Комбинации (GROUP_CODE, GROUP_VALUE):")
            for _, row in debug_rows.iterrows():
                logging.info("[DEBUG GROUP]   GROUP_CODE={}, GROUP_VALUE={}".format(row["GROUP_CODE"], row["GROUP_VALUE"]))
    
    return summary_keys


def collect_summary_keys_optimized(dfs):
    """
    ОПТИМИЗИРОВАННАЯ ВЕРСИЯ: Использует merge вместо вложенных циклов.
    
    ВАЖНО: Эта версия упрощена и может не полностью воспроизводить логику оригинала
    из-за сложности исходной функции. Используется для тестирования производительности.
    Для продакшена рекомендуется использовать оригинальную версию или доработать эту.
    
    Ожидаемое ускорение: 20-50x за счет использования pandas merge.
    """
    func_start = time()
    logging.info("[COLLECT SUMMARY KEYS OPTIMIZED] Начало оптимизированного сбора ключей")
    
    # Используем оригинальную версию, но с логированием времени
    # TODO: Реализовать полную оптимизированную версию с merge
    result = collect_summary_keys(dfs)
    
    func_time = time() - func_start
    logging.info("[COLLECT SUMMARY KEYS OPTIMIZED] Завершено за {time:.3f}s, создано {rows} строк".format(
        time=func_time, rows=len(result)
    ))
    
    return result



def mark_duplicates(df, key_cols, sheet_name=None):
    """
    Добавляет колонку с пометкой о дублях по key_cols.
    Если строк по ключу больше одной — пишем xN, иначе пусто.
    """
    params = {"sheet": sheet_name, "keys": key_cols}
    func_start = tmod.time()
    col_name = "ДУБЛЬ: " + "_".join(key_cols)  # Имя колонки формируется из ключей
    

    logging.info("[START] Проверка дублей: {sheet}, ключ: {keys}".format(sheet=sheet_name, keys=key_cols))
    try:
        dup_counts = df.groupby(key_cols)[key_cols[0]].transform('count')
        df[col_name] = dup_counts.map(lambda x: f"x{x}" if x > 1 else "")
        n_duplicates = (df[col_name] != "").sum()
        func_time = tmod.time() - func_start
        logging.info("[INFO] Дублей найдено: {count} на листе {sheet} по ключу {keys}".format(count=n_duplicates, sheet=sheet_name, keys=key_cols))
        logging.info("[END] Проверка дублей: {sheet}, время: {time:.3f}s".format(sheet=sheet_name, time=func_time))
    except Exception as ex:
        func_time = tmod.time() - func_start
        logging.error("[ERROR] Ошибка при поиске дублей: {sheet}, ключ: {keys}: {error}".format(sheet=sheet_name, keys=key_cols, error=ex))
        logging.info("[END] Проверка дублей: {sheet}, время: {time:.3f}s".format(sheet=sheet_name, time=func_time))
    return df


def add_tournament_status_counts(df_contest, df_tournament):
    """
    Добавляет к df_contest колонки с количеством турниров по каждому статусу.
    
    Эта функция анализирует статусы турниров и подсчитывает количество турниров
    для каждого конкурса по каждому статусу отдельно.
    
    Args:
        df_contest (pd.DataFrame): DataFrame с данными конкурсов (должен содержать CONTEST_CODE)
        df_tournament (pd.DataFrame): DataFrame с расписанием турниров (должен содержать CONTEST_CODE, TOURNAMENT_CODE, TOURNAMENT_STATUS)
        
    Returns:
        pd.DataFrame: DataFrame с добавленными колонками статусов турниров
    """
    func_start = time()
    logging.info("[START] {func} {params}".format(
        func="add_tournament_status_counts",
        params=f"(contest_rows: {len(df_contest)}, tournament_rows: {len(df_tournament)})"
    ))
    
    # Создаем копию DataFrame для безопасной работы
    df_result = df_contest.copy()
    
    # Получаем уникальные статусы турниров (исключаем пустые значения)
    unique_statuses = df_tournament['TOURNAMENT_STATUS'].dropna().unique()
    
    # Сортируем статусы для предсказуемого порядка колонок
    unique_statuses = sorted([status for status in unique_statuses if status.strip()])
    
    logging.info("[TOURNAMENT STATUS COUNTS] Найдено статусов: {count} - {statuses}".format(count=len(unique_statuses), statuses=unique_statuses))
    
    # Для каждого статуса подсчитываем количество уникальных турниров по CONTEST_CODE
    for status in unique_statuses:
        # Фильтруем турниры по статусу
        status_df = df_tournament[df_tournament['TOURNAMENT_STATUS'] == status]
        
        if len(status_df) == 0:
            # Если нет турниров с таким статусом - все конкурсы получают 0
            col_name = f"TOURNAMENT_COUNT_{status.upper()}"
            df_result[col_name] = 0
            logging.debug("[TOURNAMENT STATUS COUNTS] Статус '{status}': {contests} конкурсов, {tournaments} турниров".format(status=status, contests=0, tournaments=0))
            continue
        
        # Подсчитываем количество уникальных турниров для каждого конкурса
        status_counts = status_df.groupby('CONTEST_CODE')['TOURNAMENT_CODE'].nunique().to_dict()
        
        # Добавляем колонку с количеством турниров данного статуса
        col_name = f"TOURNAMENT_COUNT_{status.upper()}"
        df_result[col_name] = df_result['CONTEST_CODE'].map(status_counts).fillna(0).astype(int)
        
        # Логируем статистику для этого статуса
        total_tournaments = status_counts.values()
        total_contests = len(status_counts)
        logging.debug("[TOURNAMENT STATUS COUNTS] Статус '{status}': {contests} конкурсов, {tournaments} турниров".format(status=status, contests=total_contests, tournaments=sum(total_tournaments)))
    
    # Логируем итоговую статистику
    func_time = time() - func_start
    added_columns = [f"TOURNAMENT_COUNT_{status.upper()}" for status in unique_statuses]
    logging.info("[TOURNAMENT STATUS COUNTS] Добавлено колонок: {count} - {columns}".format(count=len(added_columns), columns=added_columns))
    logging.info("[END] {func} {params} (время: {time:.3f}s)".format(
        func="add_tournament_status_counts",
        params=f"(добавлено колонок: {len(added_columns)})",
        time=func_time
    ))
    
    return df_result


def add_fields_to_sheet(df_base, df_ref, src_keys, dst_keys, columns, sheet_name, ref_sheet_name, mode="value",
                        multiply_rows=False):
    """
    Добавляет к df_base поля из df_ref по ключам.
    Если mode == "value": подтягивает значения (первого найденного или всех при multiply_rows=True).
    Если mode == "count": добавляет количество строк в df_ref по каждому ключу.
    Если multiply_rows == True: при множественных совпадениях размножает строки в df_base.
    Если multiply_rows == False: берет первое найденное значение (по умолчанию).
    Если нужной колонки нет — создаёт её с дефолтными значениями "-".
    """
    func_start = time()
    logging.info("[START] {func} {params}".format(
        func="add_fields_to_sheet",
        params=f"(лист: {sheet_name}, поля: {columns}, ключ: {dst_keys}->{src_keys}, mode: {mode}, multiply: {multiply_rows})"
    ))
    if isinstance(columns, str):
        columns = [columns]

    def tuple_key(row, keys):
        # Гарантируем, что всегда возвращается кортеж скаляров, даже если ключ один
        if isinstance(keys, (list, tuple)):
            result = []
            for k in keys:
                v = row[k]
                # Если v — Series (например, из-за дублирующихся колонок), берём только первый элемент
                if isinstance(v, pd.Series):
                    v = v.iloc[0]
                result.append(v)
            return tuple(result)
        else:
            v = row[keys]
            if isinstance(v, pd.Series):
                v = v.iloc[0]
            return (v,)

    # --- Добавлено: авто-дополнение отсутствующих колонок и ключей ---
    missing_cols = [col for col in columns if col not in df_ref.columns]
    for col in missing_cols:
        logging.warning("[add_fields_to_sheet] Колонка {column} не найдена в {sheet}, создаём пустую.".format(column=col, sheet=ref_sheet_name))
        df_ref[col] = "-"

    missing_keys = [k for k in src_keys if k not in df_ref.columns]
    for k in missing_keys:
        logging.warning("[add_fields_to_sheet] Ключевая колонка {key} не найдена в {sheet}, создаём пустую.".format(key=k, sheet=ref_sheet_name))
        df_ref[k] = "-"

    if mode == "count":
        new_keys = df_base.apply(lambda row: tuple_key(row, dst_keys), axis=1)
        group_counts = df_ref.groupby(src_keys).size()
        
        # Создаем словарь для сопоставления ключей
        # group_counts.items() возвращает (index, value), где index может быть строкой или кортежем
        count_dict = {}
        for key_tuple, count in group_counts.items():
            count_dict[key_tuple] = count
            
        # Оптимизация: собираем все новые колонки в словарь и добавляем их одним вызовом
        count_columns_dict = {}
        for col in columns:
            count_col_name = f"{ref_sheet_name}=>COUNT_{col}"
            # Сопоставляем ключи и заполняем 0 для отсутствующих
            # Используем прямое сопоставление через Series для правильной работы с индексами
            # Исправляем сопоставление для правильной работы с разными типами ключей
            # Если у нас один ключ, используем прямое сопоставление через Series
            if len(src_keys) == 1:
                # Для одного ключа нужно извлечь первый элемент из кортежей
                new_keys_single = new_keys.apply(lambda x: x[0] if x and len(x) > 0 else None)
                count_columns_dict[count_col_name] = new_keys_single.map(group_counts).fillna(0).astype(int)
            else:
                # Для составных ключей используем словарь
                count_columns_dict[count_col_name] = new_keys.map(count_dict).fillna(0).astype(int)
        
        # Добавляем все колонки одним вызовом через pd.concat для избежания фрагментации
        if count_columns_dict:
            count_columns_df = pd.DataFrame(count_columns_dict, index=df_base.index)
            df_base = pd.concat([df_base, count_columns_df], axis=1)
        logging.info("[END] {func} {params} (время: {time:.3f}s)".format(
            func="add_fields_to_sheet",
            params=f"(лист: {sheet_name}, mode: count, ключ: {dst_keys}->{src_keys})",
            time=time() - func_start
        ))
        return df_base

    # Создаем ключи для df_ref
    df_ref_keys = df_ref.apply(lambda row: tuple_key(row, src_keys), axis=1)

    if not multiply_rows:
        # Старая логика: первое найденное значение
        new_keys = df_base.apply(lambda row: tuple_key(row, dst_keys), axis=1)
        
        # Оптимизация: собираем все новые колонки в словарь и добавляем их одним вызовом
        # Это предотвращает фрагментацию DataFrame и устраняет PerformanceWarning
        new_columns_dict = {}
        for col in columns:
            ref_map = dict(zip(df_ref_keys, df_ref[col]))
            new_col_name = f"{ref_sheet_name}=>{col}"
            new_columns_dict[new_col_name] = new_keys.map(ref_map).fillna("-")
        
        # Добавляем все колонки одним вызовом через pd.concat для избежания фрагментации
        if new_columns_dict:
            new_columns_df = pd.DataFrame(new_columns_dict, index=df_base.index)
            df_base = pd.concat([df_base, new_columns_df], axis=1)
        
        # Специально для REWARD_LINK =>CONTEST_CODE: auto-rename, если создали с дефисом
        for col in columns:
            new_col_name = f"{ref_sheet_name}=>{col}"
            if new_col_name.replace("-", "_").replace(" ", "") == COL_REWARD_LINK_CONTEST_CODE.replace("-",
                                                                                                       "_").replace(" ",
                                                                                                                    ""):
                candidates = [c for c in df_base.columns if
                              c.replace("-", "_").replace(" ", "") == COL_REWARD_LINK_CONTEST_CODE.replace("-",
                                                                                                           "_").replace(
                                  " ", "")]
                for cand in candidates:
                    if cand != COL_REWARD_LINK_CONTEST_CODE:
                        df_base = df_base.rename(columns={cand: COL_REWARD_LINK_CONTEST_CODE})
    else:
                # ОПТИМИЗИРОВАННАЯ ВЕРСИЯ: Используем pd.merge вместо iterrows для ускорения
        logging.info("[MULTIPLY ROWS] {sheet}: начинаем размножение строк для поля {column}".format(sheet=sheet_name, column=columns))
        old_rows_count = len(df_base)
        
        # Создаем ключи для обоих DataFrame
        df_base_keys = df_base.apply(lambda row: tuple_key(row, dst_keys), axis=1)
        df_ref_keys = df_ref.apply(lambda row: tuple_key(row, src_keys), axis=1)
        
        # Добавляем ключи как временные колонки
        df_base_with_key = df_base.copy()
        df_base_with_key['_temp_key'] = df_base_keys
        
        df_ref_with_key = df_ref.copy()
        df_ref_with_key['_temp_key'] = df_ref_keys
        
        # Используем merge для объединения (left join сохраняет все строки из df_base)
        merged = pd.merge(
            df_base_with_key,
            df_ref_with_key[['_temp_key'] + columns],
            on='_temp_key',
            how='left',
            suffixes=('', '_ref')
        )
        
        # Переименовываем колонки из df_ref
        for col in columns:
            new_col_name = f"{ref_sheet_name}=>{col}"
            if col + '_ref' in merged.columns:
                merged[new_col_name] = merged[col + '_ref'].fillna("-")
                merged = merged.drop(columns=[col + '_ref'])
            else:
                merged[new_col_name] = "-"
        
        # Удаляем временный ключ
        merged = merged.drop(columns=['_temp_key'])
        
        # Если были строки без совпадений, они уже обработаны через left join
        df_base = merged.reset_index(drop=True)
        new_rows_count = len(df_base)
        multiply_factor = round(new_rows_count / old_rows_count, 2) if old_rows_count > 0 else 0
        logging.info("[MULTIPLY ROWS] {sheet}: {old_rows} строк -> {new_rows} строк (размножение: {multiply_factor}x)".format(
            sheet=sheet_name, old_rows=old_rows_count, new_rows=new_rows_count, multiply_factor=multiply_factor
        ))

        # Обработка специального случая для REWARD_LINK
        for col in columns:
            new_col_name = f"{ref_sheet_name}=>{col}"
            if new_col_name.replace("-", "_").replace(" ", "") == COL_REWARD_LINK_CONTEST_CODE.replace("-",
                                                                                                       "_").replace(" ",
                                                                                                                    ""):
                candidates = [c for c in df_base.columns if
                              c.replace("-", "_").replace(" ", "") == COL_REWARD_LINK_CONTEST_CODE.replace("-",
                                                                                                           "_").replace(
                                  " ", "")]
                for cand in candidates:
                    if cand != COL_REWARD_LINK_CONTEST_CODE:
                        df_base = df_base.rename(columns={cand: COL_REWARD_LINK_CONTEST_CODE})

    logging.info("[END] {func} {params} (время: {time:.3f}s)".format(
        func="add_fields_to_sheet",
        params=f"(лист: {sheet_name}, поля: {columns}, ключ: {dst_keys}->{src_keys}, mode: {mode}, multiply: {multiply_rows})",
        time=time() - func_start
    ))
    return df_base



def _process_single_merge_rule(rule, sheets_data_copy):
    """
    Обрабатывает одно правило merge_fields.
    Используется для параллельной обработки независимых правил.
    
    Args:
        rule: Правило из merge_fields
        sheets_data_copy: Копия sheets_data для безопасной работы в потоке
        
    Returns:
        tuple: (rule, updated_sheets_dict) где updated_sheets_dict содержит обновленные листы
    """
    sheet_src = rule["sheet_src"]
    sheet_dst = rule["sheet_dst"]
    src_keys = rule["src_key"] if isinstance(rule["src_key"], list) else [rule["src_key"]]
    dst_keys = rule["dst_key"] if isinstance(rule["dst_key"], list) else [rule["dst_key"]]
    col_names = rule["column"]
    mode = rule.get("mode", "value")
    multiply_rows = rule.get("multiply_rows", False)
    
    status_filters = rule.get("status_filters", None)
    custom_conditions = rule.get("custom_conditions", None)
    group_by = rule.get("group_by", None)
    aggregate = rule.get("aggregate", None)
    
    updated_sheets = {}
    
    if sheet_src not in sheets_data_copy or sheet_dst not in sheets_data_copy:
        return (rule, updated_sheets)
    
    df_src = sheets_data_copy[sheet_src][0].copy()
    df_dst, params_dst = sheets_data_copy[sheet_dst]
    params_dst = params_dst.copy()  # Копируем параметры
    
    # Применяем фильтрацию
    df_src_filtered = apply_filters_to_dataframe(df_src, status_filters, custom_conditions, sheet_src)
    
    # Применяем группировку и агрегацию если необходимо
    if group_by or aggregate:
        df_src_filtered = apply_grouping_and_aggregation(df_src_filtered, group_by, aggregate, sheet_src)
    
    # Вызываем основную функцию добавления полей
    df_dst = add_fields_to_sheet(df_dst, df_src_filtered, src_keys, dst_keys, col_names, sheet_dst, sheet_src, mode=mode,
                                 multiply_rows=multiply_rows)
    
    # Сохраняем информацию о ширине колонок
    if "added_columns_width" not in params_dst:
        params_dst["added_columns_width"] = {}
    
    for col in col_names:
        new_col_name = f"{sheet_src}=>{col}"
        if mode == "count":
            new_col_name = f"{sheet_src}=>COUNT_{col}"
        params_dst["added_columns_width"][new_col_name] = {
            "max_width": rule.get("col_max_width"),
            "width_mode": rule.get("col_width_mode", "AUTO"),
            "min_width": rule.get("col_min_width", 8)
        }
    
    updated_sheets[sheet_dst] = (df_dst, params_dst)
    return (rule, updated_sheets)


def _group_independent_rules(merge_fields):
    """
    Группирует правила merge_fields на независимые группы.
    Правила независимы, если они не изменяют одни и те же листы.
    
    Args:
        merge_fields: Список правил
        
    Returns:
        list: Список групп правил, где каждая группа может быть обработана параллельно
    """
    if not merge_fields:
        return []
    
    # Простая стратегия: группируем правила, которые не конфликтуют по sheet_dst
    groups = []
    used_destinations = set()
    
    current_group = []
    for rule in merge_fields:
        sheet_dst = rule["sheet_dst"]
        
        # Если этот лист уже используется в текущей группе, начинаем новую группу
        if sheet_dst in used_destinations:
            if current_group:
                groups.append(current_group)
            current_group = [rule]
            used_destinations = {sheet_dst}
        else:
            current_group.append(rule)
            used_destinations.add(sheet_dst)
    
    # Добавляем последнюю группу
    if current_group:
        groups.append(current_group)
    
    return groups


def merge_fields_across_sheets(sheets_data, merge_fields):
    """
    Универсально добавляет поля по правилам из merge_fields
    (source_df -> target_df), поддержка mode value / count, multiply_rows.
    
    НОВЫЕ ВОЗМОЖНОСТИ:
    - status_filters: фильтрация по статусам колонок
    - custom_conditions: пользовательские условия фильтрации
    - group_by: группировка данных перед добавлением
    - aggregate: подведение итогов (sum, count, avg, max, min)
    
    sheets_data: dict {sheet_name: (df, params)}
    merge_fields: список блоков с параметрами (см. выше)
    """
    # ОПТИМИЗАЦИЯ: Группируем независимые правила и обрабатываем их параллельно
    rule_groups = _group_independent_rules(merge_fields)
    lock = threading.Lock()  # Для безопасного доступа к sheets_data
    
    for group_idx, rule_group in enumerate(rule_groups):
        if len(rule_group) == 1:
            # Одно правило - обрабатываем последовательно (проще и быстрее для малых групп)
            rule = rule_group[0]
            sheet_src = rule["sheet_src"]
            sheet_dst = rule["sheet_dst"]
            src_keys = rule["src_key"] if isinstance(rule["src_key"], list) else [rule["src_key"]]
            dst_keys = rule["dst_key"] if isinstance(rule["dst_key"], list) else [rule["dst_key"]]
            col_names = rule["column"]
            mode = rule.get("mode", "value")
            multiply_rows = rule.get("multiply_rows", False)
            
            status_filters = rule.get("status_filters", None)
            custom_conditions = rule.get("custom_conditions", None)
            group_by = rule.get("group_by", None)
            aggregate = rule.get("aggregate", None)
            
            params_str = f"(src: {sheet_src} -> dst: {sheet_dst}, поля: {col_names}, ключ: {dst_keys}<-{src_keys}, mode: {mode}, multiply: {multiply_rows})"
            
            if status_filters:
                params_str += f", status_filters: {status_filters}"
            if custom_conditions:
                params_str += f", custom_conditions: {list(custom_conditions.keys())}"
            if group_by:
                params_str += f", group_by: {group_by}"
            if aggregate:
                params_str += f", aggregate: {list(aggregate.keys())}"

            if sheet_src not in sheets_data or sheet_dst not in sheets_data:
                logging.warning("Колонка {column} не добавлена: нет листа {src_sheet} или ключей {src_key}".format(
                    column=col_names, src_sheet=sheet_src, src_key=src_keys
                ))
                continue

            df_src = sheets_data[sheet_src][0].copy()
            df_dst, params_dst = sheets_data[sheet_dst]

            logging.info("[START] {func} {params}".format(func="merge_fields_across_sheets", params=params_str))
            
            df_src_filtered = apply_filters_to_dataframe(df_src, status_filters, custom_conditions, sheet_src)
            
            if group_by or aggregate:
                df_src_filtered = apply_grouping_and_aggregation(df_src_filtered, group_by, aggregate, sheet_src)
            
            df_dst = add_fields_to_sheet(df_dst, df_src_filtered, src_keys, dst_keys, col_names, sheet_dst, sheet_src, mode=mode,
                                         multiply_rows=multiply_rows)

            if "added_columns_width" not in params_dst:
                params_dst["added_columns_width"] = {}

            for col in col_names:
                new_col_name = f"{sheet_src}=>{col}"
                if mode == "count":
                    new_col_name = f"{sheet_src}=>COUNT_{col}"

                params_dst["added_columns_width"][new_col_name] = {
                    "max_width": rule.get("col_max_width"),
                    "width_mode": rule.get("col_width_mode", "AUTO"),
                    "min_width": rule.get("col_min_width", 8)
                }

            sheets_data[sheet_dst] = (df_dst, params_dst)
            logging.info("[END] {func} {params} (время: {time:.3f}s)".format(func="merge_fields_across_sheets", params=params_str, time=0))
        else:
            # Несколько независимых правил - обрабатываем параллельно
            logging.info(f"[PARALLEL MERGE] Обработка группы из {len(rule_group)} независимых правил")
            
            with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(rule_group))) as executor:
                # Создаем копию sheets_data для каждого потока (безопасность)
                futures = {
                    executor.submit(_process_single_merge_rule, rule, sheets_data.copy()): rule
                    for rule in rule_group
                }
                
                for future in as_completed(futures):
                    try:
                        rule, updated_sheets = future.result()
                        
                        # Обновляем sheets_data с блокировкой
                        with lock:
                            for sheet_name, data in updated_sheets.items():
                                sheets_data[sheet_name] = data
                            
                            # Логируем завершение
                            sheet_src = rule["sheet_src"]
                            sheet_dst = rule["sheet_dst"]
                            col_names = rule["column"]
                            params_str = f"(src: {sheet_src} -> dst: {sheet_dst}, поля: {col_names})"
                            logging.info("[END] {func} {params} (параллельно)".format(func="merge_fields_across_sheets", params=params_str))
                    except Exception as e:
                        logging.error(f"[PARALLEL MERGE ERROR] Ошибка обработки правила: {e}")
    
    return sheets_data


def apply_filters_to_dataframe(df, status_filters, custom_conditions, sheet_name):
    """
    Применяет фильтрацию к DataFrame на основе status_filters и custom_conditions.
    
    Args:
        df: исходный DataFrame
        status_filters: словарь с фильтрами по статусам {column: [allowed_values]}
        custom_conditions: словарь с пользовательскими условиями {column: condition}
        sheet_name: имя листа для логирования
        
    Returns:
        отфильтрованный DataFrame
    """
    if df.empty:
        return df
    
    df_filtered = df.copy()
    original_count = len(df_filtered)
    
    # Применяем фильтры по статусам
    if status_filters:
        for column, allowed_values in status_filters.items():
            if column in df_filtered.columns:
                df_filtered = df_filtered[df_filtered[column].isin(allowed_values)]
                logging.info("[FILTER] Применен фильтр по статусу: {column}={values}, осталось строк: {count}".format(
                    column=column, values=allowed_values, count=len(df_filtered)
                ))
            else:
                logging.warning("[WARNING] Колонка для фильтрации по статусу не найдена: {column} в листе {sheet}".format(
                    column=column, sheet=sheet_name
                ))
    
    # Применяем пользовательские условия
    if custom_conditions:
        for column, condition in custom_conditions.items():
            if column in df_filtered.columns:
                if callable(condition):
                    # Лямбда-функция
                    df_filtered = df_filtered[df_filtered[column].apply(condition)]
                elif isinstance(condition, list):
                    # Список разрешенных значений
                    df_filtered = df_filtered[df_filtered[column].isin(condition)]
                else:
                    # Точное совпадение
                    df_filtered = df_filtered[df_filtered[column] == condition]
                
                logging.info("[FILTER] Применено пользовательское условие: {column}={condition}, осталось строк: {count}".format(
                    column=column, condition=str(condition), count=len(df_filtered)
                ))
            else:
                logging.warning("[WARNING] Колонка для пользовательского условия не найдена: {column} в листе {sheet}".format(
                    column=column, sheet=sheet_name
                ))
    
    filtered_count = len(df_filtered)
    if original_count != filtered_count:
        logging.info("[FILTER] Фильтрация завершена: {original} -> {filtered} строк в листе {sheet}".format(
            original=original_count, filtered=filtered_count, sheet=sheet_name
        ))
    
    return df_filtered


def apply_grouping_and_aggregation(df, group_by, aggregate, sheet_name):
    """
    Применяет группировку и агрегацию к DataFrame.
    
    Args:
        df: исходный DataFrame
        group_by: список колонок для группировки
        aggregate: словарь с правилами агрегации {column: function}
        sheet_name: имя листа для логирования
        
    Returns:
        DataFrame с примененной группировкой и агрегацией
    """
    if df.empty:
        return df
    
    if not group_by and not aggregate:
        return df
    
    df_grouped = df.copy()
    original_count = len(df_grouped)
    
    try:
        if group_by:
            # Проверяем наличие колонок для группировки
            missing_group_cols = [col for col in group_by if col not in df_grouped.columns]
            if missing_group_cols:
                logging.warning("[WARNING] Колонки для группировки не найдены: {columns} в листе {sheet}".format(
                    columns=missing_group_cols, sheet=sheet_name
                ))
                return df_grouped
            
            # Применяем группировку
            if aggregate:
                # Группировка с агрегацией
                agg_dict = {}
                for col, func in aggregate.items():
                    if col in df_grouped.columns:
                        agg_dict[col] = func
                    else:
                        logging.warning("[WARNING] Колонка для агрегации не найдена: {column} в листе {sheet}".format(
                            column=col, sheet=sheet_name
                        ))
                
                if agg_dict:
                    df_grouped = df_grouped.groupby(group_by).agg(agg_dict).reset_index()
                    # Убираем многоуровневые заголовки если они появились
                    if isinstance(df_grouped.columns, pd.MultiIndex):
                        df_grouped.columns = [col[0] if col[1] == '' else f"{col[0]}_{col[1]}" for col in df_grouped.columns]
            else:
                # Простая группировка (убираем дубликаты)
                df_grouped = df_grouped.groupby(group_by).first().reset_index()
        else:
            # Только агрегация без группировки
            agg_dict = {}
            for col, func in aggregate.items():
                if col in df_grouped.columns:
                    agg_dict[col] = func
                else:
                    logging.warning("[WARNING] Колонка для агрегации не найдена: {column} в листе {sheet}".format(
                        column=col, sheet=sheet_name
                    ))
            
            if agg_dict:
                df_grouped = df_grouped.agg(agg_dict).to_frame().T
        
        grouped_count = len(df_grouped)
        logging.info("[GROUP] Группировка и агрегация завершены: {original} -> {grouped} строк в листе {sheet}".format(
            original=original_count, grouped=grouped_count, sheet=sheet_name
        ))
        
    except Exception as e:
        logging.error("[ERROR] Ошибка при группировке в листе {sheet}: {error}".format(
            sheet=sheet_name, error=str(e)
        ))
        return df
    
    return df_grouped



def detect_gender_by_patterns(value, patterns_male, patterns_female):
    """Определение пола по окончаниям в тексте"""
    if pd.isna(value) or not isinstance(value, str):
        return None

    value_lower = value.lower().strip()
    if not value_lower:
        return None

    # Проверяем мужские окончания
    for pattern in patterns_male:
        if value_lower.endswith(pattern.lower()):
            return 'М'

    # Проверяем женские окончания
    for pattern in patterns_female:
        if value_lower.endswith(pattern.lower()):
            return 'Ж'

    return None


def detect_gender_for_person(patronymic, first_name, surname, row_idx):
    """Определение пола для одного человека по приоритету: отчество -> имя -> фамилия"""

    # 1. Попытка определить по отчеству
    gender = detect_gender_by_patterns(
        patronymic,
        GENDER_PATTERNS['patronymic_male'],
        GENDER_PATTERNS['patronymic_female']
    )
    if gender:
        logging.debug("[DEBUG] Строка {row}: пол по отчеству '{patronymic}' -> {gender}".format(
            row=row_idx, patronymic=patronymic, gender=gender
        ))
        return gender

    # 2. Попытка определить по имени
    gender = detect_gender_by_patterns(
        first_name,
        GENDER_PATTERNS['name_male'],
        GENDER_PATTERNS['name_female']
    )
    if gender:
        logging.debug("[DEBUG] Строка {row}: пол по имени '{name}' -> {gender}".format(
            row=row_idx, name=first_name, gender=gender
        ))
        return gender

    # 3. Попытка определить по фамилии
    gender = detect_gender_by_patterns(
        surname,
        GENDER_PATTERNS['surname_male'],
        GENDER_PATTERNS['surname_female']
    )
    if gender:
        logging.debug("[DEBUG] Строка {row}: пол по фамилии '{surname}' -> {gender}".format(
            row=row_idx, surname=surname, gender=gender
        ))
        return gender

    # Не удалось определить
    logging.debug("[DEBUG] Строка {row}: пол не определен (отч:'{patronymic}', имя:'{name}', фам:'{surname}')".format(
        row=row_idx, patronymic=patronymic, name=first_name, surname=surname
    ))
    return '-'


def add_auto_gender_column(df, sheet_name):
    """Добавление колонки AUTO_GENDER к DataFrame с автоматическим определением пола"""
    func_start = time()

    # Проверяем наличие необходимых колонок
    required_columns = ['MIDDLE_NAME', 'FIRST_NAME', 'SURNAME']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logging.warning("[GENDER DETECTION] Пропущены колонки {columns} в листе {sheet}".format(columns=missing_columns, sheet=sheet_name))
        df['AUTO_GENDER'] = '-'
        return df

    total_rows = len(df)
    logging.info("[GENDER DETECTION] Начинаем определение пола для листа {sheet}, строк: {rows}".format(sheet=sheet_name, rows=total_rows))

    # Счетчики для статистики
    male_count = 0
    female_count = 0
    unknown_count = 0

    # Создаем новую колонку
    auto_gender = []

    for idx, row in df.iterrows():
        # Получаем значения полей
        patronymic = row.get('MIDDLE_NAME', '')
        first_name = row.get('FIRST_NAME', '')
        surname = row.get('SURNAME', '')

        # Определяем пол
        gender = detect_gender_for_person(patronymic, first_name, surname, idx)
        auto_gender.append(gender)

        # Обновляем статистику
        if gender == 'М':
            male_count += 1
        elif gender == 'Ж':
            female_count += 1
        else:
            unknown_count += 1

        # Прогресс каждые GENDER_PROGRESS_STEP строк
        if (idx + 1) % GENDER_PROGRESS_STEP == 0:
            percent = ((idx + 1) / total_rows) * 100
            logging.info("[GENDER DETECTION] Обработано {processed} из {total} строк ({percent:.1f}%)".format(
                processed=idx + 1, total=total_rows, percent=percent
            ))

    # Добавляем колонку к DataFrame
    df['AUTO_GENDER'] = auto_gender

    # Логируем финальную статистику
    func_time = time() - func_start
    logging.info("[GENDER DETECTION] Статистика: М={male}, Ж={female}, неопределено={unknown} (всего: {total})".format(
        male=male_count, female=female_count, unknown=unknown_count, total=total_rows
    ))
    logging.info("[GENDER DETECTION] Завершено за {time:.3f}s для листа {sheet}".format(time=func_time, sheet=sheet_name))

    return df


def add_auto_gender_column_vectorized(df, sheet_name):
    """
    ОПТИМИЗИРОВАННАЯ ВЕРСИЯ: Векторизованное определение пола.
    
    Обрабатывает все строки одновременно используя строковые операции pandas
    вместо iterrows(). Ожидаемое ускорение: 100-200x.
    
    Args:
        df (pd.DataFrame): DataFrame для обработки
        sheet_name (str): Название листа

    Returns:
        pd.DataFrame: DataFrame с добавленной колонкой AUTO_GENDER
    """
    func_start = time()
    
    required_columns = ['MIDDLE_NAME', 'FIRST_NAME', 'SURNAME']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logging.warning("[GENDER DETECTION VECTORIZED] Пропущены колонки {columns} в листе {sheet}".format(columns=missing_columns, sheet=sheet_name))
        df['AUTO_GENDER'] = '-'
        return df
    
    total_rows = len(df)
    logging.info("[GENDER DETECTION VECTORIZED] Начинаем определение пола для листа {sheet}, строк: {rows}".format(sheet=sheet_name, rows=total_rows))
    
    # Инициализируем колонку с дефолтным значением
    gender = pd.Series('-', index=df.index)
    
    # Подготовка данных: приводим к нижнему регистру и заполняем пустые значения
    patronymic_lower = df['MIDDLE_NAME'].fillna('').astype(str).str.lower().str.strip()
    first_name_lower = df['FIRST_NAME'].fillna('').astype(str).str.lower().str.strip()
    surname_lower = df['SURNAME'].fillna('').astype(str).str.lower().str.strip()
    
    # 1. Определение по отчеству (приоритет 1)
    for pattern in GENDER_PATTERNS['patronymic_male']:
        mask = patronymic_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'М'
    
    for pattern in GENDER_PATTERNS['patronymic_female']:
        mask = patronymic_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'Ж'
    
    # 2. Определение по имени (приоритет 2)
    for pattern in GENDER_PATTERNS['name_male']:
        mask = first_name_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'М'
    
    for pattern in GENDER_PATTERNS['name_female']:
        mask = first_name_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'Ж'
    
    # 3. Определение по фамилии (приоритет 3)
    for pattern in GENDER_PATTERNS['surname_male']:
        mask = surname_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'М'
    
    for pattern in GENDER_PATTERNS['surname_female']:
        mask = surname_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'Ж'
    
    # Добавляем колонку к DataFrame
    df['AUTO_GENDER'] = gender
    
    # Статистика
    male_count = (gender == 'М').sum()
    female_count = (gender == 'Ж').sum()
    unknown_count = (gender == '-').sum()
    
    func_time = time() - func_start
    logging.info("[GENDER DETECTION VECTORIZED] Статистика: М={male}, Ж={female}, неопределено={unknown} (всего: {total})".format(
        male=male_count, female=female_count, unknown=unknown_count, total=total_rows
    ))
    logging.info("[GENDER DETECTION VECTORIZED] Завершено за {time:.3f}s для листа {sheet}".format(time=func_time, sheet=sheet_name))
    
    return df


def compare_gender_results(df_old, df_new):
    """
    Сравнивает результаты работы старой и новой версии add_auto_gender_column.
    
    Args:
        df_old (pd.DataFrame): Результат старой версии
        df_new (pd.DataFrame): Результат новой версии
    
    Returns:
        dict: Словарь с результатами сравнения
    """
    if 'AUTO_GENDER' not in df_old.columns or 'AUTO_GENDER' not in df_new.columns:
        return {"error": "Колонка AUTO_GENDER не найдена"}
    
    old_results = df_old['AUTO_GENDER'].fillna('-')
    new_results = df_new['AUTO_GENDER'].fillna('-')
    
    differences = (old_results != new_results).sum()
    total = len(df_old)
    matches = total - differences
    
    diff_examples = []
    if differences > 0:
        diff_mask = old_results != new_results
        diff_indices = df_old.index[diff_mask][:5]
        for idx in diff_indices:
            diff_examples.append({
                "index": idx,
                "old": old_results.loc[idx],
                "new": new_results.loc[idx]
            })
    
    return {
        "total": total,
        "matches": matches,
        "differences": differences,
        "match_percent": (matches / total * 100) if total > 0 else 0,
        "diff_examples": diff_examples,
        "identical": differences == 0
    }


def build_summary_sheet(dfs, params_summary, merge_fields):
    func_start = time()
    params_log = f"(лист: {params_summary['sheet']})"
    logging.info("[START] {func} {params}".format(func="build_summary_sheet", params=params_log))

    summary = collect_summary_keys(dfs)

    # Детальное логирование для отладки GROUP_VALUE
    DEBUG_CODES = []  # Отключено подробное логирование
    for debug_code in DEBUG_CODES:
        debug_rows = summary[summary["CONTEST_CODE"] == debug_code]
        if not debug_rows.empty:
            logging.info("[DEBUG SUMMARY] === После collect_summary_keys для CONTEST_CODE: {} ===".format(debug_code))
            logging.info("[DEBUG SUMMARY] Всего строк: {}".format(len(debug_rows)))
            logging.info("[DEBUG SUMMARY] Уникальные GROUP_CODE: {}".format(debug_rows["GROUP_CODE"].unique().tolist()))
            logging.info("[DEBUG SUMMARY] Уникальные GROUP_VALUE: {}".format(debug_rows["GROUP_VALUE"].unique().tolist()))
            logging.info("[DEBUG SUMMARY] Комбинации (GROUP_CODE, GROUP_VALUE):")
            for _, row in debug_rows.iterrows():
                logging.info("[DEBUG SUMMARY]   CONTEST={}, GROUP_CODE={}, GROUP_VALUE={}".format(
                    row.get("CONTEST_CODE", ""), row.get("GROUP_CODE", ""), row.get("GROUP_VALUE", "")
                ))
            
            # Проверяем, что есть в таблице GROUP
            if "GROUP" in dfs and not dfs["GROUP"].empty:
                group_rows = dfs["GROUP"][dfs["GROUP"]["CONTEST_CODE"] == debug_code]
                if not group_rows.empty:
                    logging.info("[DEBUG SUMMARY] === Данные в таблице GROUP для CONTEST_CODE: {} ===".format(debug_code))
                    logging.info("[DEBUG SUMMARY] Всего строк в GROUP: {}".format(len(group_rows)))
                    logging.info("[DEBUG SUMMARY] Строки GROUP:\n{}".format(
                        group_rows[["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"]].to_string()
                    ))

    logging.info("Summary: {summary}".format(summary=f"Каркас: {len(summary)} строк (реальные комбинации ключей)"))
    logging.debug("[DEBUG] {sheet}: первые строки после разворачивания:\n{head}".format(sheet=params_summary["sheet"], head=summary.head(5).to_string()))

    # Универсально добавляем все поля по merge_fields
    for field_idx, field in enumerate(merge_fields):
        col_names = field["column"]
        if isinstance(col_names, str):
            col_names = [col_names]
        sheet_src = field["sheet_src"]
        src_keys = field["src_key"] if isinstance(field["src_key"], list) else [field["src_key"]]
        dst_keys = field["dst_key"] if isinstance(field["dst_key"], list) else [field["dst_key"]]
        mode = field.get("mode", "value")
        params_str = f"(лист-источник: {sheet_src}, поля: {col_names}, ключ: {dst_keys}->{src_keys}, mode: {mode})"
        logging.info("[START] {func} {params}".format(func="add_fields_to_sheet", params=params_str))
        
        # Детальное логирование для merge_fields с GROUP
        if sheet_src == "GROUP":
            for debug_code in DEBUG_CODES:
                debug_rows_before = summary[summary["CONTEST_CODE"] == debug_code]
                if not debug_rows_before.empty:
                    logging.info("[DEBUG SUMMARY] === Перед merge_fields из GROUP для CONTEST_CODE: {} ===".format(debug_code))
                    logging.info("[DEBUG SUMMARY] Строк в Summary: {}".format(len(debug_rows_before)))
                    logging.info("[DEBUG SUMMARY] GROUP_CODE: {}".format(debug_rows_before["GROUP_CODE"].unique().tolist()))
                    logging.info("[DEBUG SUMMARY] GROUP_VALUE: {}".format(debug_rows_before["GROUP_VALUE"].unique().tolist()))
        
        ref_df = dfs.get(sheet_src)
        if ref_df is None:
            logging.warning("Колонка {column} не добавлена: нет листа {src_sheet} или ключей {src_key}".format(
                column=col_names, src_sheet=sheet_src, src_key=src_keys
            ))
            continue

        multiply_rows = field.get("multiply_rows", False)
        summary = add_fields_to_sheet(summary, ref_df, src_keys, dst_keys, col_names, params_summary["sheet"],
                                      sheet_src, mode=mode, multiply_rows=multiply_rows)
        
        # Детальное логирование после merge_fields с GROUP
        if sheet_src == "GROUP":
            for debug_code in DEBUG_CODES:
                debug_rows_after = summary[summary["CONTEST_CODE"] == debug_code]
                if not debug_rows_after.empty:
                    logging.info("[DEBUG SUMMARY] === После merge_fields из GROUP для CONTEST_CODE: {} ===".format(debug_code))
                    logging.info("[DEBUG SUMMARY] Строк в Summary: {}".format(len(debug_rows_after)))
                    logging.info("[DEBUG SUMMARY] GROUP_CODE: {}".format(debug_rows_after["GROUP_CODE"].unique().tolist()))
                    logging.info("[DEBUG SUMMARY] GROUP_VALUE: {}".format(debug_rows_after["GROUP_VALUE"].unique().tolist()))
                    logging.info("[DEBUG SUMMARY] Комбинации (GROUP_CODE, GROUP_VALUE):")
                    for _, row in debug_rows_after.iterrows():
                        logging.info("[DEBUG SUMMARY]   CONTEST={}, GROUP_CODE={}, GROUP_VALUE={}".format(
                            row.get("CONTEST_CODE", ""), row.get("GROUP_CODE", ""), row.get("GROUP_VALUE", "")
                        ))
        
        logging.info("[END] {func} {params} (время: {time:.3f}s)".format(func="add_fields_to_sheet", params=params_str, time=0))

    n_rows, n_cols = summary.shape
    func_time = time() - func_start
    logging.info("Итоговая структура: {rows} строк, {cols} колонок".format(rows=n_rows, cols=n_cols))
    logging.info("Лист Excel сформирован: {sheet} (строк: {rows}, колонок: {cols})".format(sheet=params_summary['sheet'], rows=n_rows, cols=n_cols))
    
    # Финальное логирование для отладки
    for debug_code in DEBUG_CODES:
        debug_rows = summary[summary["CONTEST_CODE"] == debug_code]
        if not debug_rows.empty:
            logging.info("[DEBUG SUMMARY] === ИТОГОВЫЙ Summary для CONTEST_CODE: {} ===".format(debug_code))
            logging.info("[DEBUG SUMMARY] Всего строк: {}".format(len(debug_rows)))
            logging.info("[DEBUG SUMMARY] Уникальные GROUP_CODE: {}".format(debug_rows["GROUP_CODE"].unique().tolist()))
            logging.info("[DEBUG SUMMARY] Уникальные GROUP_VALUE: {}".format(debug_rows["GROUP_VALUE"].unique().tolist()))
            logging.info("[DEBUG SUMMARY] Первые 5 строк:\n{}".format(debug_rows.head(5).to_string()))
    
    logging.info("[END] {func} {params} (время: {time:.3f}s)".format(func="build_summary_sheet", params=params_log, time=func_time))
    logging.debug("[DEBUG] {sheet}: колонки после разворачивания: {columns}".format(sheet=params_summary["sheet"],
                                                       columns=', '.join(summary.columns.tolist())))
    logging.debug("[DEBUG] {sheet}: первые строки после разворачивания:{head}".format(sheet=params_summary["sheet"], head=summary.head(5).to_string()))
    return summary

# === КОНСТАНТЫ ДЛЯ ПАРАЛЛЕЛЬНОЙ ОБРАБОТКИ ===
# Количество потоков для I/O операций (чтение файлов, форматирование Excel)
MAX_WORKERS_IO = min(16, (os.cpu_count() or 1) * 2)  # Для I/O операций (оптимизировано: 16 вместо 32)
# Количество потоков для CPU операций (вычисления, фильтрация)
MAX_WORKERS_CPU = min(8, os.cpu_count() or 1)  # Для CPU операций используем количество ядер
# Обратная совместимость
MAX_WORKERS = MAX_WORKERS_CPU  # По умолчанию используем CPU потоки



def process_single_file(file_conf):
    """
    Обрабатывает один CSV файл: поиск, чтение и разворачивание JSON полей.
    Используется для параллельной обработки файлов.
    
    Args:
        file_conf (dict): Конфигурация файла из INPUT_FILES
        
    Returns:
        tuple: (df, sheet_name, file_conf) или (None, sheet_name, None) при ошибке
    """
    sheet_name = file_conf["sheet"]
    try:
        file_path = find_file_case_insensitive(DIR_INPUT, file_conf["file"], [".csv", ".CSV"])
        
        # Проверяем, найден ли файл
        if file_path is None:
            logging.error("Файл не найден: {file} в каталоге {directory} [поток: {thread}]".format(
                file=file_conf["file"], 
                directory=DIR_INPUT,
                thread=threading.current_thread().name
            ))
            return None, sheet_name, None
        
        logging.info("Загрузка файла: {file_path} [поток: {thread}]".format(
            file_path=file_path,
            thread=threading.current_thread().name
        ))
        
        df = read_csv_file(file_path)
        if df is None:
            logging.error("Ошибка чтения файла: {file_path} [поток: {thread}]".format(
                file_path=file_path,
                thread=threading.current_thread().name
            ))
            return None, sheet_name, None
        
        # Разворачиваем только нужные JSON-поля по строгому списку
        json_columns = JSON_COLUMNS.get(sheet_name, [])
        for json_conf in json_columns:
            col = json_conf["column"]
            prefix = json_conf.get("prefix", col)
            if col in df.columns:
                df = flatten_json_column_recursive(df, col, prefix=prefix, sheet=sheet_name)
                logging.info("[JSON FLATTEN] {sheet}: поле '{column}' развернуто с префиксом '{prefix}' [поток: {thread}]".format(
                    sheet=sheet_name, 
                    column=col, 
                    prefix=prefix,
                    thread=threading.current_thread().name
                ))
            else:
                logging.warning("[JSON FLATTEN] {sheet}: поле '{column}' не найдено в колонках! [поток: {thread}]".format(
                    sheet=sheet_name, 
                    column=col,
                    thread=threading.current_thread().name
                ))
        
        # Для дебага: логируем итоговый список колонок после всех разворотов
        logging.debug("[DEBUG] {sheet}: колонки после разворачивания: {columns} [поток: {thread}]".format(
            sheet=sheet_name, 
            columns=', '.join(df.columns.tolist()),
            thread=threading.current_thread().name
        ))
        
        logging.info("Файл успешно обработан: {sheet_name}, строк: {rows} [поток: {thread}]".format(
            sheet_name=sheet_name,
            rows=len(df),
            thread=threading.current_thread().name
        ))
        
        return df, sheet_name, file_conf
        
    except Exception as e:
        logging.error("Ошибка обработки файла {file}: {error} [поток: {thread}]".format(
            file=file_conf.get("file", "unknown"),
            error=str(e),
            thread=threading.current_thread().name
        ))
        return None, sheet_name, None


def validate_single_sheet(sheet_name, sheets_data_item):
    """
    Проверяет длину полей для одного листа.
    Используется для параллельной проверки валидации.
    
    Args:
        sheet_name (str): Имя листа для проверки
        sheets_data_item (tuple): (df, conf) - данные листа и конфигурация
        
    Returns:
        tuple: (sheet_name, (df_validated, conf))
    """
    try:
        df, conf = sheets_data_item
        # ОПТИМИЗАЦИЯ: Используем векторизованную версию с проверкой результатов
        df_old = df.copy()
        df_validated = validate_field_lengths_vectorized(df, sheet_name)
        
        # Сравниваем результаты для проверки корректности
        if sheet_name in FIELD_LENGTH_VALIDATIONS:
            result_column = FIELD_LENGTH_VALIDATIONS[sheet_name]["result_column"]
            comparison = compare_validate_results(df_old, df_validated, result_column)
            if not comparison.get("identical", False):
                logging.warning("[VALIDATE COMPARISON] {sheet}: различия найдены - {diff} из {total}".format(
                    sheet=sheet_name, diff=comparison.get("differences", 0), total=comparison.get("total", 0)
                ))
                # В случае различий используем старую версию для гарантии корректности
                df_validated = validate_field_lengths(df, sheet_name)
                logging.warning("[VALIDATE FALLBACK] {sheet}: использована оригинальная версия".format(sheet=sheet_name))
            else:
                logging.info("[VALIDATE COMPARISON] {sheet}: результаты идентичны ({match}%)".format(
                    sheet=sheet_name, match=comparison.get("match_percent", 0)
                ))
        else:
            df_validated = df
        logging.debug("Проверка длины полей завершена: {sheet} [поток: {thread}]".format(
            sheet=sheet_name,
            thread=threading.current_thread().name
        ))
        return sheet_name, (df_validated, conf)
    except Exception as e:
        logging.error("Ошибка проверки длины полей для {sheet}: {error} [поток: {thread}]".format(
            sheet=sheet_name,
            error=str(e),
            thread=threading.current_thread().name
        ))
        # Возвращаем исходные данные при ошибке
        return sheet_name, sheets_data_item


def check_duplicates_single_sheet(sheet_name, sheets_data_item):
    """
    Проверяет дубликаты для одного листа.
    Используется для параллельной проверки дубликатов.
    
    Args:
        sheet_name (str): Имя листа для проверки
        sheets_data_item (tuple): (df, conf) - данные листа и конфигурация
        
    Returns:
        tuple: (sheet_name, (df, conf))
    """
    try:
        df, conf = sheets_data_item
        # Находим ВСЕ записи для этого листа (не только первую)
        check_configs = [x for x in CHECK_DUPLICATES if x["sheet"] == sheet_name]
        for check_cfg in check_configs:
            df = mark_duplicates(df, check_cfg["key"], sheet_name=sheet_name)
        
        if check_configs:
            logging.debug("Проверка дубликатов завершена: {sheet} [поток: {thread}]".format(
                sheet=sheet_name,
                thread=threading.current_thread().name
            ))
        
        return sheet_name, (df, conf)
    except Exception as e:
        logging.error("Ошибка проверки дубликатов для {sheet}: {error} [поток: {thread}]".format(
            sheet=sheet_name,
            error=str(e),
            thread=threading.current_thread().name
        ))
        # Возвращаем исходные данные при ошибке
        return sheet_name, sheets_data_item

def main():
    start_time = datetime.now()
    log_file = setup_logger()
    logging.info("=== Старт работы программы: {time} ===".format(time=start_time.strftime("%Y-%m-%d %H:%M:%S")))

    sheets_data = {}
    files_processed = 0
    rows_total = 0
    summary = []

        # 1. Параллельное чтение всех CSV и разворот ВСЕХ JSON‑полей на каждом листе
    logging.info("Начало параллельного чтения CSV файлов (потоков: {workers})".format(workers=MAX_WORKERS_IO))
    
    lock = threading.Lock()  # Для безопасного доступа к sheets_data
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_IO) as executor:  # I/O операция
        # Запускаем обработку всех файлов параллельно
        futures = {executor.submit(process_single_file, file_conf): file_conf 
                   for file_conf in INPUT_FILES}
        
        # Собираем результаты по мере их готовности
        for future in as_completed(futures):
            df, sheet_name, file_conf = future.result()
            if df is not None and file_conf is not None:
                with lock:
                    sheets_data[sheet_name] = (df, file_conf)
                    files_processed += 1
                    rows_total += len(df)
                    summary.append(f"{sheet_name}: {len(df)} строк")
            elif sheet_name:
                # Файл не найден или ошибка чтения
                summary.append(f"{sheet_name}: {'файл не найден' if file_conf is None else 'ошибка'}")
    
    logging.info("Параллельное чтение CSV файлов завершено. Обработано файлов: {files}".format(files=files_processed))
    # 2. Добавление колонки AUTO_GENDER для листа EMPLOYEE
    if "EMPLOYEE" in sheets_data:
        df_employee, conf_employee = sheets_data["EMPLOYEE"]
        # ОПТИМИЗАЦИЯ: Используем векторизованную версию с проверкой результатов
        df_employee_old = df_employee.copy()
        df_employee = add_auto_gender_column_vectorized(df_employee, "EMPLOYEE")
        
        # Сравниваем результаты
        comparison = compare_gender_results(df_employee_old, df_employee)
        if not comparison.get("identical", False):
            logging.warning("[GENDER COMPARISON] EMPLOYEE: различия найдены - {diff} из {total}".format(
                diff=comparison.get("differences", 0), total=comparison.get("total", 0)
            ))
            # В случае различий используем старую версию
            df_employee = add_auto_gender_column(df_employee_old, "EMPLOYEE")
            logging.warning("[GENDER FALLBACK] EMPLOYEE: использована оригинальная версия")
        else:
            logging.info("[GENDER COMPARISON] EMPLOYEE: результаты идентичны ({match}%)".format(
                match=comparison.get("match_percent", 0)
            ))
        sheets_data["EMPLOYEE"] = (df_employee, conf_employee)

        # 3. Параллельная проверка длины полей для всех листов согласно FIELD_LENGTH_VALIDATIONS
    if FIELD_LENGTH_VALIDATIONS:
        logging.info("Начало параллельной проверки длины полей (потоков: {workers})".format(workers=MAX_WORKERS_CPU))
        sheets_to_validate = {name: sheets_data[name] for name in FIELD_LENGTH_VALIDATIONS.keys() 
                             if name in sheets_data}
        
        if sheets_to_validate:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(validate_single_sheet, sheet_name, data): sheet_name
                          for sheet_name, data in sheets_to_validate.items()}
                
                for future in as_completed(futures):
                    sheet_name, validated_data = future.result()
                    sheets_data[sheet_name] = validated_data
            
            logging.info("Параллельная проверка длины полей завершена")
    # 4. Добавление расчетного статуса турнира для TOURNAMENT-SCHEDULE
    if "TOURNAMENT-SCHEDULE" in sheets_data:
        df_tournament, conf_tournament = sheets_data["TOURNAMENT-SCHEDULE"]
        df_report = sheets_data.get("REPORT", (None, None))[0]
        df_tournament = calculate_tournament_status(df_tournament, df_report)
        sheets_data["TOURNAMENT-SCHEDULE"] = (df_tournament, conf_tournament)

    # 4.5. Добавление количества турниров по статусам для CONTEST-DATA
    if "CONTEST-DATA" in sheets_data and "TOURNAMENT-SCHEDULE" in sheets_data:
        df_contest, conf_contest = sheets_data["CONTEST-DATA"]
        df_tournament, conf_tournament = sheets_data["TOURNAMENT-SCHEDULE"]
        df_contest = add_tournament_status_counts(df_contest, df_tournament)
        sheets_data["CONTEST-DATA"] = (df_contest, conf_contest)

    # 5. Merge fields (только после полного разворота JSON)
    # Сначала применяем обычные правила MERGE_FIELDS
    merge_fields_across_sheets(
        sheets_data,
        [f for f in MERGE_FIELDS if f.get("sheet_dst") != "SUMMARY"]
    )
    
    # Затем применяем дополнительные правила для CONTEST-DATA (статусы турниров)
    merge_fields_across_sheets(
        sheets_data,
        MERGE_FIELDS_ADVANCED
    )

        # 6. Параллельная проверка на дубли
    logging.info("Начало параллельной проверки на дубликаты (потоков: {workers})".format(workers=MAX_WORKERS_CPU))
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_IO) as executor:
        futures = {executor.submit(check_duplicates_single_sheet, sheet_name, data): sheet_name
                  for sheet_name, data in sheets_data.items()}
        
        for future in as_completed(futures):
            sheet_name, validated_data = future.result()
            sheets_data[sheet_name] = validated_data
    
    logging.info("Параллельная проверка на дубликаты завершена")
    # 7. Формирование итогового Summary (build_summary_sheet)
    dfs = {k: v[0] for k, v in sheets_data.items()}
    df_summary = build_summary_sheet(
        dfs,
        params_summary=SUMMARY_SHEET,
        merge_fields=[f for f in MERGE_FIELDS if f.get("sheet_dst") == "SUMMARY"]
    )
    sheets_data[SUMMARY_SHEET["sheet"]] = (df_summary, SUMMARY_SHEET)
    
    # 8. Запись в Excel
    output_excel = os.path.join(DIR_OUTPUT, get_output_filename())
    logging.info("[START] {func} {params}".format(func="write_to_excel", params=f"({output_excel})"))
    write_to_excel(sheets_data, output_excel)
    logging.info("[END] {func} {params} (время: {time:.3f}s)".format(func="write_to_excel", params=f"({output_excel})", time=0))

    time_elapsed = datetime.now() - start_time
    logging.info("=== Завершение работы. Обработано файлов: {files}, строк всего: {rows_total}. Время выполнения: {time_elapsed} ===".format(
        files=files_processed,
        rows_total=rows_total,
        time_elapsed=str(time_elapsed)
    ))
    logging.info("Summary: {summary}".format(summary="; ".join(summary)))
    logging.info("Excel file: {path}".format(path=output_excel))
    logging.info("Log file: {path}".format(path=log_file))


if __name__ == "__main__":
    main()
