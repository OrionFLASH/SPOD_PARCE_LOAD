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
        "file": "CONTEST-DATA (PROM) 2025-10-16 v0 (new pole)",  # Файл с данными конкурсов
        "sheet": "CONTEST-DATA",                        # Лист для обработки
        "max_col_width": 120,                          # Максимальная ширина колонки
        "freeze": "C2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "GROUP (PROM) 2025-10-08 v0",            # Файл с данными групп
        "sheet": "GROUP",                              # Лист для обработки
        "max_col_width": 20,                           # Максимальная ширина колонки
        "freeze": "C2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "INDICATOR (PROM) 2025-10-07 v0",        # Файл с индикаторами
        "sheet": "INDICATOR",                          # Лист для обработки
        "max_col_width": 100,                           # Максимальная ширина колонки
        "freeze": "B2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "REPORT (PROM-KMKKSB) 2025-10-09 v0", # Файл с отчетами
        "sheet": "REPORT",                             # Лист для обработки
        "max_col_width": 25,                           # Максимальная ширина колонки
        "freeze": "D2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "REWARD (PROM) 2025-10-08 v2",        # Файл с наградами
        "sheet": "REWARD",                             # Лист для обработки
        "max_col_width": 200,                          # Максимальная ширина колонки (большая для длинных описаний)
        "freeze": "D2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "REWARD-LINK (PROM) 2025-10-07 v0",      # Файл со связями наград
        "sheet": "REWARD-LINK",                        # Лист для обработки
        "max_col_width": 30,                           # Максимальная ширина колонки
        "freeze": "A2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "SVD_KB_DM_GAMIFICATION_ORG_UNIT_V20 - 2025.08.28", # Файл с организационными единицами
        "sheet": "ORG_UNIT_V20",                       # Лист для обработки
        "max_col_width": 60,                           # Максимальная ширина колонки
        "freeze": "A2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "TOURNAMENT-SCHEDULE (PROM) 2025-10-09 v0", # Файл с расписанием турниров
        "sheet": "TOURNAMENT-SCHEDULE",                # Лист для обработки
        "max_col_width": 120,                          # Максимальная ширина колонки
        "freeze": "B2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "PROM_USER_ROLE 2025-09-17 v2",       # Файл с ролями пользователей
        "sheet": "USER_ROLE",                          # Лист для обработки
        "max_col_width": 60,                           # Максимальная ширина колонки
        "freeze": "D2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "PROM_USER_ROLE SB 2025-09-17 v2",    # Файл с ролями пользователей SB
        "sheet": "USER_ROLE SB",                       # Лист для обработки
        "max_col_width": 60,                           # Максимальная ширина колонки
        "freeze": "D2",                                # Закрепление области
        "col_width_mode": "AUTO",                      # Автоматическое растягивание колонок
        "min_col_width": 8                             # Минимальная ширина колонки
    },
    {
        "file": "employee_PROM_final_5000_2025-07-26_00-09-03",  # Файл с данными сотрудников
        "sheet": "EMPLOYEE",                              # Лист для обработки
        "max_col_width": 70,                              # Максимальная ширина колонки
        "freeze": "F2",                                   # Закрепление области (колонки A-E и строка 1)
        "col_width_mode": "AUTO",                         # Автоматическое растягивание колонок
        "min_col_width": 8                                # Минимальная ширина колонки
    }
]

# === КОНФИГУРАЦИЯ СВОДНОГО ЛИСТА ===
# Настройки для создания итогового листа с объединенными данными
SUMMARY_SHEET = {
    "sheet": "SUMMARY",                                   # Название сводного листа
    "max_col_width": 100,                                 # Максимальная ширина колонки
    "freeze": "F2",                                      # Закрепление области (колонки A-E и строка 1)
    "col_width_mode": "AUTO",                            # Автоматическое растягивание колонок
    "min_col_width": 8                                   # Минимальная ширина колонки
}

# === НАСТРОЙКИ ЛОГИРОВАНИЯ ===
LOG_LEVEL = "DEBUG"  # Уровень логирования: "INFO" для продакшена, "DEBUG" для отладки
LOG_BASE_NAME = "LOGS"  # Базовое имя для файлов логов

# Словарь сообщений для логирования различных событий
# Используется для стандартизации сообщений и локализации
LOG_MESSAGES = {
    # Основные события программы
    "start": "=== Старт работы программы: {time} ===",                    # Начало работы
    "reading_file": "Загрузка файла: {file_path}",                        # Чтение файла
    "read_ok": "Файл успешно загружен: {file_path}, строк: {rows}, колонок: {cols}",  # Успешное чтение
    "read_fail": "Ошибка загрузки файла: {file_path}. {error}",           # Ошибка чтения
    "sheet_written": "Лист Excel сформирован: {sheet} (строк: {rows}, колонок: {cols})",  # Лист создан
    "finish": "=== Завершение работы. Обработано файлов: {files}, строк всего: {rows_total}. Время выполнения: {time_elapsed} ===",  # Завершение
    "summary": "Summary: {summary}",                                      # Сводка
    
    # События функций
    "func_start": "[START] {func} {params}",                              # Начало выполнения функции
    "func_end": "[END] {func} {params} (время: {time:.3f}s)",            # Завершение функции
    "func_error": "[ERROR] {func} {params} — {error}",                    # Ошибка в функции
    
    # Обработка JSON данных
    "json_flatten_start": "Разворачивание колонки {column} (строк: {rows})",  # Начало разворачивания JSON
    "json_flatten_end": "Развёрнуто {n_cols} колонок из {n_keys} ключей, ошибок JSON: {n_errors}, строк: {rows}, время: {time:.3f}s",  # Завершение разворачивания
    "json_flatten_error": "Ошибка разбора JSON (строка {row}): {error}",  # Ошибка парсинга JSON
    "debug_columns": "[DEBUG] {sheet}: колонки после разворачивания: {columns}",  # Отладка колонок
    "debug_head": "[DEBUG] {sheet}: первые строки после разворачивания:\n{head}",  # Отладка данных
    
    # Присоединение полей между листами
    "field_joined": "Колонка {column} присоединена из {src_sheet} по ключу {dst_key} -> {src_key}",  # Поле присоединено
    "field_missing": "Колонка {column} не добавлена: нет листа {src_sheet} или ключей {src_key}",  # Поле не найдено
    "fields_summary": "Итоговая структура: {rows} строк, {cols} колонок",  # Сводка по полям
    
    # Проверка дублей
    "duplicates_start": "[START] Проверка дублей: {sheet}, ключ: {keys}",  # Начало проверки
    "duplicates_found": "[INFO] Дублей найдено: {count} на листе {sheet} по ключу {keys}",  # Дубли найдены
    "duplicates_error": "[ERROR] Ошибка при поиске дублей: {sheet}, ключ: {keys}: {error}",  # Ошибка поиска
    "duplicates_end": "[END] Проверка дублей: {sheet}, время: {time:.3f}s",  # Завершение проверки
    
    # Цветовые схемы
    "color_scheme_applied": "[INFO] Цветовая схема применена: лист {sheet}, колонка {col}, стиль {scope}, цвет {color}",  # Схема применена
    
    # Дополнительные сообщения для JSON
    "json_flatten_summary": "[INFO] {column} → новых колонок: {count}",  # Сводка по разворачиванию
    "json_flatten_keys": "[INFO] Все новые колонки: {keys}",  # Список новых колонок
    "csv_sample": "[DEBUG] CSV {file} поле {column}: {sample}",  # Образец CSV данных
    "excel_path": "Excel file: {path}",  # Путь к Excel файлу
    "log_path": "Log file: {path}",  # Путь к лог файлу
    "json_flatten_done": "[JSON FLATTEN] {sheet}: поле '{column}' развернуто с префиксом '{prefix}'",  # JSON развернут
    "json_flatten_missing": "[JSON FLATTEN] {sheet}: поле '{column}' не найдено в колонках!",  # Поле не найдено
    
    # Добавление полей
    "missing_column": "[add_fields_to_sheet] Колонка {column} не найдена в {sheet}, создаём пустую.",  # Колонка отсутствует
    "missing_key": "[add_fields_to_sheet] Ключевая колонка {key} не найдена в {sheet}, создаём пустую.",  # Ключ отсутствует
    
    # Безопасный парсинг JSON
    
    # Новые сообщения для расширенной системы MERGE_FIELDS
    "status_filter_applied": "[FILTER] Применен фильтр по статусу: {column}={values}, осталось строк: {count}",  # Фильтр по статусу применен
    "status_filter_column_missing": "[WARNING] Колонка для фильтрации по статусу не найдена: {column} в листе {sheet}",  # Колонка для фильтрации отсутствует
    "custom_filter_applied": "[FILTER] Применено пользовательское условие: {column}={condition}, осталось строк: {count}",  # Пользовательское условие применено
    "custom_filter_column_missing": "[WARNING] Колонка для пользовательского условия не найдена: {column} в листе {sheet}",  # Колонка для пользовательского условия отсутствует
    "filtering_completed": "[FILTER] Фильтрация завершена: {original} -> {filtered} строк в листе {sheet}",  # Фильтрация завершена
    "grouping_columns_missing": "[WARNING] Колонки для группировки не найдены: {columns} в листе {sheet}",  # Колонки для группировки отсутствуют
    "aggregate_column_missing": "[WARNING] Колонка для агрегации не найдена: {column} в листе {sheet}",  # Колонка для агрегации отсутствует
    "grouping_completed": "[GROUP] Группировка и агрегация завершены: {original} -> {grouped} строк в листе {sheet}",  # Группировка завершена
    "grouping_error": "[ERROR] Ошибка при группировке в листе {sheet}: {error}",  # Ошибка группировки
    
    # Новые сообщения для создания уникальных листов SUMMARY
    "summary_sheet_start": "[SUMMARY_SHEET] Создание уникального листа {sheet_name} по ключу {key_column}",  # Начало создания листа
    "summary_sheet_processing": "[SUMMARY_SHEET] Обработка {total_rows} строк, найдено {unique_count} уникальных значений",  # Обработка данных
    "summary_sheet_duplicates_removed": "[SUMMARY_SHEET] Удалено {removed_count} дублей, осталось {final_count} уникальных строк",  # Удаление дублей
    "summary_sheet_completed": "[SUMMARY_SHEET] Лист {sheet_name} создан: {final_count} уникальных строк по ключу {key_column}",  # Завершение создания
    "summary_sheet_error": "[ERROR] Ошибка создания листа {sheet_name}: {error}",  # Ошибка создания листа
    "safe_json_error": "[safe_json_loads] Ошибка: {error} | Исходная строка: {line}",  # Ошибка парсинга
    
    # Размножение строк
    "multiply_rows_start": "[MULTIPLY ROWS] {sheet}: начинаем размножение строк для поля {column}",  # Начало размножения
    "multiply_rows_result": "[MULTIPLY ROWS] {sheet}: {old_rows} строк -> {new_rows} строк (размножение: {multiply_factor}x)",  # Результат размножения
    
    # Ширина колонок
    "column_width_set": "[COLUMN WIDTH] {sheet}: колонка '{column}' -> ширина {width} (режим: {mode})",  # Установка ширины
    
    # Динамические цветовые схемы
    "dynamic_color_scheme": "[DYNAMIC COLOR] Сгенерирована схема для {sheet}: {columns}",  # Схема сгенерирована
    
    # Определение пола
    "gender_detection_start": "[GENDER DETECTION] Начинаем определение пола для листа {sheet}, строк: {rows}",  # Начало определения
    "gender_detection_progress": "[GENDER DETECTION] Обработано {processed} из {total} строк ({percent:.1f}%)",  # Прогресс определения
    "gender_detection_stats": "[GENDER DETECTION] Статистика: М={male}, Ж={female}, неопределено={unknown} (всего: {total})",  # Статистика
    "gender_detection_end": "[GENDER DETECTION] Завершено за {time:.3f}s для листа {sheet}",  # Завершение определения
    "gender_by_patronymic": "[DEBUG] Строка {row}: пол по отчеству '{patronymic}' -> {gender}",  # Пол по отчеству
    "gender_by_name": "[DEBUG] Строка {row}: пол по имени '{name}' -> {gender}",  # Пол по имени
    "gender_by_surname": "[DEBUG] Строка {row}: пол по фамилии '{surname}' -> {gender}",  # Пол по фамилии
    "gender_unknown": "[DEBUG] Строка {row}: пол не определен (отч:'{patronymic}', имя:'{name}', фам:'{surname}')",  # Пол не определен
    
    # Проверка длины полей
    "field_length_start": "[FIELD LENGTH] Проверка длины полей для листа {sheet}, строк: {rows}",  # Начало проверки
    "field_length_progress": "[FIELD LENGTH] Обработано {processed} из {total} строк ({percent:.1f}%)",  # Прогресс проверки
    "field_length_stats": "[FIELD LENGTH] Статистика: корректных={correct}, с ошибками={errors} (всего: {total})",  # Статистика проверки
    "field_length_end": "[FIELD LENGTH] Завершено за {time:.3f}s для листа {sheet}",  # Завершение проверки
    "field_length_violation": "[DEBUG] Строка {row}: поле '{field}' = {length} {operator} {limit} (нарушение)",  # Нарушение ограничений
    "tournament_status_stats": "[TOURNAMENT STATUS] Статистика: {stats}",
    "field_length_missing": "[FIELD LENGTH] Пропущены поля {fields} в листе {sheet}",
    "contest_feature_restored": "[CONTEST_FEATURE] Исходная колонка восстановлена с тройными кавычками",
    "tournament_status_counts_found": "[TOURNAMENT STATUS COUNTS] Найдено статусов: {count} - {statuses}",
    "tournament_status_counts_debug": "[TOURNAMENT STATUS COUNTS] Статус '{status}': {contests} конкурсов, {tournaments} турниров",
    "tournament_status_counts_added": "[TOURNAMENT STATUS COUNTS] Добавлено колонок: {count} - {columns}",
    "gender_detection_missing": "[GENDER DETECTION] Пропущены колонки {columns} в листе {sheet}",
    "file_not_found": "Файл не найден: {file} в каталоге {directory}",
    "debug_json_preserve": "[safe_json_loads_preserve_triple_quotes] Сохраняем исходную строку с тройными кавычками: {string}",
    "debug_json_error": "[safe_json_loads] Ошибка: {error} | Исходная строка: {string}"

}

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
        "column": ["CONTEST_TYPE", "FULL_NAME", "BUSINESS_STATUS", "BUSINESS_BLOCK"],  # Добавляемые колонки
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
        "column": ["GOSB_SHORT_NAME"],      # Добавляем краткое название ГОСБ
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
        "column": ["FULL_NAME", "BUSINESS_BLOCK", "CONTEST_TYPE", "BUSINESS_STATUS"],  # Добавляемые поля
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
        "column": ["FULL_NAME"],            # Добавляем полное название
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 70,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 35                # Минимальная ширина
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
            "FACTOR_MATCH",                 # Фактор соответствия
            "PLAN_MOD_VALUE",               # Плановое значение модуля
            "BUSINESS_BLOCK",               # Бизнес-блок
            f"{PREFIX_CONTEST_FEATURE} => tournamentStartMailing",  # Рассылка начала турнира
            f"{PREFIX_CONTEST_FEATURE} => tournamentEndMailing",  # Рассылка окончания турнира
            f"{PREFIX_CONTEST_FEATURE} => tournamentRewardingMailing",  # Рассылка награждения турнира
            f"{PREFIX_CONTEST_FEATURE} => tournamentLikeMailing"  # Рассылка лайков турнира
        ],
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 60,               # Максимальная ширина
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
            "GET_CALC_CRITERION",          # Основной критерий расчета
            "ADD_CALC_CRITERION",          # Дополнительный критерий расчета
            "ADD_CALC_CRITERION_2"        # Второй дополнительный критерий расчета
        ],
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 40,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: из GROUP по составному ключу
    # Добавляет критерии расчета из групп по коду группы (частичное соответствие)
    {
        "sheet_src": "GROUP",               # Источник - группы
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["GROUP_CODE"],          # Ключ - только код группы
        "dst_key": ["GROUP_CODE"],          # Ключ - только код группы
        "column": [                         # Добавляемые колонки:
            "GET_CALC_CRITERION",          # Основной критерий расчета
            "ADD_CALC_CRITERION",          # Дополнительный критерий расчета
            "ADD_CALC_CRITERION_2"        # Второй дополнительный критерий расчета
        ],
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 40,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: CONTEST_CODE из GROUP по GROUP_CODE (для частично связанных групп)
    # Добавляет код конкурса из групп по коду группы
    # {
    #     "sheet_src": "GROUP",               # Источник - группы
    #     "sheet_dst": "SUMMARY",             # Цель - сводный лист
    #     "src_key": ["GROUP_CODE"],          # Ключ - код группы
    #     "dst_key": ["GROUP_CODE"],          # Ключ - код группы
    #     "column": ["CONTEST_CODE"],         # Добавляем код конкурса
    #     "mode": "value",                    # Добавляем значение
    #     "multiply_rows": False,             # Не размножаем строки
    #     "col_max_width": 30,               # Максимальная ширина
    #     "col_width_mode": "AUTO",          # Автоматическое растягивание
    #     "col_min_width": 8                 # Минимальная ширина
    # },
    # SUMMARY: GROUP_VALUE из GROUP по GROUP_CODE (для частично связанных групп)
    # Добавляет значение группы из групп по коду группы
    # {
    #     "sheet_src": "GROUP",               # Источник - группы
    #     "sheet_dst": "SUMMARY",             # Цель - сводный лист
    #     "src_key": ["GROUP_CODE"],          # Ключ - код группы
    #     "dst_key": ["GROUP_CODE"],          # Ключ - код группы
    #     "column": ["GROUP_VALUE"],          # Добавляем значение группы
    #     "mode": "value",                    # Добавляем значение
    #     "multiply_rows": False,             # Не размножаем строки
    #     "col_max_width": 20,               # Максимальная ширина
    #     "col_width_mode": "AUTO",          # Автоматическое растягивание
    #     "col_min_width": 8                 # Минимальная ширина
    # },
    # SUMMARY: из INDICATOR по CONTEST_CODE
    # Добавляет информацию об индикаторах конкурса
    {
        "sheet_src": "INDICATOR",           # Источник - индикаторы
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": [                         # Добавляемые колонки:
            "INDICATOR_MARK_TYPE",          # Тип отметки индикатора
            "INDICATOR_MATCH",              # Соответствие индикатора
            "INDICATOR_VALUE"               # Значение индикатора
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
            "TARGET_TYPE"                   # Тип цели турнира
        ],
        "mode": "value",                    # Добавляем значения
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
    # SUMMARY: сколько в REPORT строк по паре TOURNAMENT_CODE + CONTEST_CODE (для полностью связанных записей)
    # Подсчитывает количество отчетов для каждой пары турнир-конкурс
    {
        "sheet_src": "REPORT",              # Источник - отчеты
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["TOURNAMENT_CODE", "CONTEST_CODE"],  # Составной ключ: код турнира + код конкурса
        "dst_key": ["TOURNAMENT_CODE", "CONTEST_CODE"],  # Составной ключ: код турнира + код конкурса
        "column": [                         # Добавляемые колонки:
            "CONTEST_DATE"                  # Используем CONTEST_DATE для подсчета (любое поле подойдет)
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
    # SUMMARY: сколько в GROUP строк по составному ключу CONTEST_CODE + GROUP_CODE + GROUP_VALUE
    # Подсчитывает количество уникальных комбинаций группа-значение для каждого конкурса
    {
        "sheet_src": "GROUP",               # Источник - группы
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"],  # Составной ключ: код конкурса + код группы + значение группы
        "dst_key": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"],  # Составной ключ: код конкурса + код группы + значение группы
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
        "src_key": ["REWARD_CODE"],         # Ключ: код награды
        "dst_key": ["REWARD_CODE"],         # Ключ: код награды
        "column": [                         # Добавляемые колонки:
            "CONTEST_CODE"                  # Используем CONTEST_CODE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: сколько в REWARD-LINK строк по составному ключу CONTEST_CODE + REWARD_CODE
    # Подсчитывает количество связей конкурс-награда
    {
        "sheet_src": "REWARD-LINK",         # Источник - связи наград
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE", "REWARD_CODE"],  # Составной ключ: код конкурса + код награды
        "dst_key": ["CONTEST_CODE", "REWARD_CODE"],  # Составной ключ: код конкурса + код награды
        "column": [                         # Добавляемые колонки:
            "CONTEST_CODE"                  # Используем CONTEST_CODE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: сколько в REWARD строк по ключу REWARD_CODE
    # Подсчитывает количество наград
    {
        "sheet_src": "REWARD",              # Источник - награды
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["REWARD_CODE"],         # Ключ: код награды
        "dst_key": ["REWARD_CODE"],         # Ключ: код награды
        "column": [                         # Добавляемые колонки:
            "REWARD_CODE"                   # Используем REWARD_CODE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: сколько в TOURNAMENT-SCHEDULE строк по ключу CONTEST_CODE
    # Подсчитывает количество записей расписания для каждого конкурса
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
    # SUMMARY: сколько в REPORT записей по ключу CONTEST_CODE
    # Подсчитывает количество отчетов для каждого конкурса
    {
        "sheet_src": "REPORT",              # Источник - отчеты
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],        # Ключ: код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ: код конкурса
        "column": [                         # Добавляемые колонки:
            "CONTEST_DATE"                  # Используем CONTEST_DATE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
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
    # SUMMARY: сколько в TOURNAMENT-SCHEDULE строк по ключу TOURNAMENT_CODE
    # Подсчитывает количество записей расписания для каждого турнира
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["TOURNAMENT_CODE"],     # Ключ: код турнира
        "dst_key": ["TOURNAMENT_CODE"],     # Ключ: код турнира
        "column": [                         # Добавляемые колонки:
            "CONTEST_CODE"                  # Используем CONTEST_CODE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: сколько в TOURNAMENT-SCHEDULE строк по составному ключу CONTEST_CODE + TOURNAMENT_CODE
    # Подсчитывает количество записей расписания для каждой пары конкурс-турнир
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE", "TOURNAMENT_CODE"],  # Составной ключ: код конкурса + код турнира
        "dst_key": ["CONTEST_CODE", "TOURNAMENT_CODE"],  # Составной ключ: код конкурса + код турнира
        "column": [                         # Добавляемые колонки:
            "CONTEST_CODE"                  # Используем CONTEST_CODE для подсчета
        ],
        "mode": "count",                    # Режим: подсчитываем количество
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 20,               # Максимальная ширина
        "col_width_mode": 15,              # Фиксированная ширина (15 символов)
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: все нужные поля из REWARD по составному ключу (для полностью связанных записей)
    # Добавляет информацию о наградах по полному соответствию ключей конкурс-награда
    {
        "sheet_src": "REWARD",              # Источник - награды
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": [COL_REWARD_LINK_CONTEST_CODE, "REWARD_CODE"],  # Составной ключ: код конкурса из связи + код награды
        "dst_key": ["CONTEST_CODE", "REWARD_CODE"],                # Составной ключ: код конкурса + код награды
        "column": [                         # Добавляемые колонки:
            "FULL_NAME",                    # Полное название награды
            "REWARD_DESCRIPTION",           # Описание награды
            f"{PREFIX_ADD_DATA} => feature",  # Признак награды (развернутый JSON)
            f"{PREFIX_ADD_DATA} => itemFeature",  # Признак элемента награды
            f"{PREFIX_ADD_DATA} => rewardRule",  # Правило награды
            f"{PREFIX_ADD_DATA} => hidden",     # Признак скрытия награды
            f"{PREFIX_ADD_DATA} => rewardAgainGlobal",  # Повторная награда глобально
            f"{PREFIX_ADD_DATA} => rewardAgainTournament",  # Повторная награда в турнире
            f"{PREFIX_ADD_DATA} => outstanding",  # Выдающийся
            f"{PREFIX_ADD_DATA} => teamNews",  # Новости команды
            f"{PREFIX_ADD_DATA} => singleNews"  # Единичные новости
        ],
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 50,               # Максимальная ширина
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
            f"{PREFIX_ADD_DATA} => feature",  # Признак награды
            f"{PREFIX_ADD_DATA} => itemFeature",  # Признак элемента награды
            f"{PREFIX_ADD_DATA} => rewardRule",  # Правило награды
            f"{PREFIX_ADD_DATA} => rewardAgainGlobal",  # Повторная награда глобально
            f"{PREFIX_ADD_DATA} => rewardAgainTournament",  # Повторная награда в турнире
            f"{PREFIX_ADD_DATA} => outstanding",  # Выдающийся
            f"{PREFIX_ADD_DATA} => teamNews",  # Новости команды
            f"{PREFIX_ADD_DATA} => singleNews"  # Единичные новости
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
    # SUMMARY: CONTEST_DATE из REPORT по CONTEST_CODE (для частично связанных записей)
    # Добавляет дату конкурса из отчетов для частично связанных записей
    {
        "sheet_src": "REPORT",              # Источник - отчеты
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": [                         # Добавляемые колонки:
            "CONTEST_DATE"                  # Дата конкурса
        ],
        "mode": "value",                    # Добавляем значение
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 25,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
        "col_min_width": 8                 # Минимальная ширина
    },
    # SUMMARY: поля из TOURNAMENT-SCHEDULE по CONTEST_CODE (для частично связанных записей)
    # Добавляет информацию о турнирах по коду конкурса (без привязки к конкретному турниру)
    {
        "sheet_src": "TOURNAMENT-SCHEDULE", # Источник - расписание турниров
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "dst_key": ["CONTEST_CODE"],        # Ключ - код конкурса
        "column": [                         # Добавляемые колонки:
            "START_DT",                     # Дата начала турнира
            "END_DT",                       # Дата окончания турнира
            "RESULT_DT",                    # Дата результатов турнира
            "TOURNAMENT_STATUS",            # Статус турнира
            "TARGET_TYPE"                   # Тип цели турнира
        ],
        "mode": "value",                    # Добавляем значения
        "multiply_rows": False,             # Не размножаем строки
        "col_max_width": 30,               # Максимальная ширина
        "col_width_mode": "AUTO",          # Автоматическое растягивание
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
    # SUMMARY: сколько в REWARD наград по составному ключу CONTEST_CODE + REWARD_CODE
    # Подсчитывает количество наград для каждой пары конкурс-награда
    {
        "sheet_src": "REWARD",              # Источник - награды
        "sheet_dst": "SUMMARY",             # Цель - сводный лист
        "src_key": [COL_REWARD_LINK_CONTEST_CODE, "REWARD_CODE"],  # Составной ключ: код конкурса из связи + код награды
        "dst_key": ["CONTEST_CODE", "REWARD_CODE"],  # Составной ключ: код конкурса + код награды
        "column": [                         # Добавляемые колонки:
            "REWARD_CODE"                   # Используем REWARD_CODE для подсчета
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

    # --- SUMMARY_REWARD: КЛЮЧЕВЫЕ ПОЛЯ — пастельный голубой ---
    {
        "group": "SUMMARY_REWARD: Key Fields",
        "header_bg": "E6F3FF",  # пастельный голубой - как исходные данные
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_REWARD"],
        "columns": ["REWARD_CODE", "CONTEST_CODE", "TOURNAMENT_CODE", "GROUP_CODE"],
        # Назначение: ключевые поля в SUMMARY_REWARD
    },

    # --- SUMMARY_REWARD: CONTEST-DATA — пастельный голубой ---
    {
        "group": "SUMMARY_REWARD: CONTEST-DATA",
        "header_bg": "CCE5FF",  # пастельный голубой - оттенок для CONTEST-DATA
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_REWARD"],
        "columns": [
            "CONTEST-DATA=>FULL_NAME", "CONTEST-DATA=>CONTEST_FEATURE => momentRewarding",
            "CONTEST-DATA=>FACTOR_MATCH", "CONTEST-DATA=>PLAN_MOD_VALUE", "CONTEST-DATA=>BUSINESS_BLOCK",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentStartMailing", "CONTEST-DATA=>CONTEST_FEATURE => tournamentEndMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentRewardingMailing", "CONTEST-DATA=>CONTEST_FEATURE => tournamentLikeMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => capacity", "CONTEST-DATA=>CONTEST_FEATURE => tournamentListMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => vid", "CONTEST-DATA=>CONTEST_FEATURE => tbVisible",
            "CONTEST-DATA=>CONTEST_FEATURE => tbHidden", "CONTEST-DATA=>CONTEST_FEATURE => persomanNumberVisible",
            "CONTEST-DATA=>CONTEST_FEATURE => typeRewarding", "CONTEST-DATA=>CONTEST_FEATURE => masking",
            "CONTEST-DATA=>CONTEST_FEATURE => minNumber", "CONTEST-DATA=>CONTEST_FEATURE => businessBlock",
            "CONTEST-DATA=>CONTEST_FEATURE => accuracy", "CONTEST-DATA=>CONTEST_FEATURE => gosbHidden",
            "CONTEST-DATA=>CONTEST_FEATURE => preferences", "CONTEST-DATA=>CONTEST_FEATURE => persomanNumberHidden",
            "CONTEST-DATA=>CONTEST_FEATURE => gosbVisible", "CONTEST-DATA=>CONTEST_FEATURE => feature"
        ],
        # Назначение: поля из CONTEST-DATA в SUMMARY_REWARD
    },

    # --- SUMMARY_REWARD: GROUP — пастельный зелёный ---
    {
        "group": "SUMMARY_REWARD: GROUP",
        "header_bg": "E8F5E8",  # пастельный зелёный - оттенок для GROUP
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_REWARD"],
        "columns": ["GROUP=>GET_CALC_CRITERION", "GROUP=>ADD_CALC_CRITERION", "GROUP=>ADD_CALC_CRITERION_2"],
        # Назначение: поля из GROUP в SUMMARY_REWARD
    },

    # --- SUMMARY_REWARD: INDICATOR — пастельный жёлтый ---
    {
        "group": "SUMMARY_REWARD: INDICATOR",
        "header_bg": "FFF8E1",  # пастельный жёлтый - оттенок для INDICATOR
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_REWARD"],
        "columns": ["INDICATOR=>INDICATOR_MARK_TYPE", "INDICATOR=>INDICATOR_MATCH", "INDICATOR=>INDICATOR_VALUE"],
        # Назначение: поля из INDICATOR в SUMMARY_REWARD
    },

    # --- SUMMARY_REWARD: TOURNAMENT-SCHEDULE — пастельный голубой ---
    {
        "group": "SUMMARY_REWARD: TOURNAMENT-SCHEDULE",
        "header_bg": "E1F5FE",  # пастельный голубой - оттенок для TOURNAMENT-SCHEDULE
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_REWARD"],
        "columns": ["TOURNAMENT-SCHEDULE=>START_DT", "TOURNAMENT-SCHEDULE=>END_DT", "TOURNAMENT-SCHEDULE=>RESULT_DT",
                    "TOURNAMENT-SCHEDULE=>TOURNAMENT_STATUS", "TOURNAMENT-SCHEDULE=>TARGET_TYPE"],
        # Назначение: поля из TOURNAMENT-SCHEDULE в SUMMARY_REWARD
    },

    # --- SUMMARY_REWARD: REPORT — пастельный зелёный ---
    {
        "group": "SUMMARY_REWARD: REPORT",
        "header_bg": "E8F5E8",  # пастельный зелёный - оттенок для REPORT
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_REWARD"],
        "columns": ["REPORT=>CONTEST_DATE", "REPORT=>COUNT_CONTEST_DATE"],
        # Назначение: поля из REPORT в SUMMARY_REWARD
    },

    # --- SUMMARY_REWARD: REWARD — пастельный оранжевый ---
    {
        "group": "SUMMARY_REWARD: REWARD",
        "header_bg": "FFE8CC",  # пастельный оранжевый - оттенок для REWARD
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_REWARD"],
        "columns": [
            "REWARD=>ADD_DATA => rewardAgainGlobal", "REWARD=>ADD_DATA => rewardAgainTournament",
            "REWARD=>ADD_DATA => outstanding", "REWARD=>ADD_DATA => teamNews", "REWARD=>ADD_DATA => singleNews"
        ],
        # Назначение: поля из REWARD в SUMMARY_REWARD
    },

    # --- SUMMARY_REWARD: ДУБЛИ — пастельный розовый ---
    {
        "group": "SUMMARY_REWARD: DUPLICATES",
        "header_bg": "FFE6F2",  # пастельный розовый - для дублей
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_REWARD"],
        "columns": ["ДУБЛЬ: CONTEST_CODE_TOURNAMENT_CODE_REWARD_CODE_GROUP_CODE"],
        # Назначение: поля дублей в SUMMARY_REWARD
    },

    # --- SUMMARY_CONTEST: КЛЮЧЕВЫЕ ПОЛЯ — пастельный голубой ---
    {
        "group": "SUMMARY_CONTEST: Key Fields",
        "header_bg": "E6F3FF",  # пастельный голубой - как исходные данные
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_CONTEST"],
        "columns": ["REWARD_CODE", "CONTEST_CODE", "TOURNAMENT_CODE", "GROUP_CODE"],
        # Назначение: ключевые поля в SUMMARY_CONTEST
    },

    # --- SUMMARY_CONTEST: CONTEST-DATA — пастельный голубой ---
    {
        "group": "SUMMARY_CONTEST: CONTEST-DATA",
        "header_bg": "CCE5FF",  # пастельный голубой - оттенок для CONTEST-DATA
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_CONTEST"],
        "columns": [
            "CONTEST-DATA=>FULL_NAME", "CONTEST-DATA=>CONTEST_FEATURE => momentRewarding",
            "CONTEST-DATA=>FACTOR_MATCH", "CONTEST-DATA=>PLAN_MOD_VALUE", "CONTEST-DATA=>BUSINESS_BLOCK",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentStartMailing", "CONTEST-DATA=>CONTEST_FEATURE => tournamentEndMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentRewardingMailing", "CONTEST-DATA=>CONTEST_FEATURE => tournamentLikeMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => capacity", "CONTEST-DATA=>CONTEST_FEATURE => tournamentListMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => vid", "CONTEST-DATA=>CONTEST_FEATURE => tbVisible",
            "CONTEST-DATA=>CONTEST_FEATURE => tbHidden", "CONTEST-DATA=>CONTEST_FEATURE => persomanNumberVisible",
            "CONTEST-DATA=>CONTEST_FEATURE => typeRewarding", "CONTEST-DATA=>CONTEST_FEATURE => masking",
            "CONTEST-DATA=>CONTEST_FEATURE => minNumber", "CONTEST-DATA=>CONTEST_FEATURE => businessBlock",
            "CONTEST-DATA=>CONTEST_FEATURE => accuracy", "CONTEST-DATA=>CONTEST_FEATURE => gosbHidden",
            "CONTEST-DATA=>CONTEST_FEATURE => preferences", "CONTEST-DATA=>CONTEST_FEATURE => persomanNumberHidden",
            "CONTEST-DATA=>CONTEST_FEATURE => gosbVisible", "CONTEST-DATA=>CONTEST_FEATURE => feature"
        ],
        # Назначение: поля из CONTEST-DATA в SUMMARY_CONTEST
    },

    # --- SUMMARY_CONTEST: GROUP — пастельный зелёный ---
    {
        "group": "SUMMARY_CONTEST: GROUP",
        "header_bg": "E8F5E8",  # пастельный зелёный - оттенок для GROUP
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_CONTEST"],
        "columns": ["GROUP=>GET_CALC_CRITERION", "GROUP=>ADD_CALC_CRITERION", "GROUP=>ADD_CALC_CRITERION_2"],
        # Назначение: поля из GROUP в SUMMARY_CONTEST
    },

    # --- SUMMARY_CONTEST: INDICATOR — пастельный жёлтый ---
    {
        "group": "SUMMARY_CONTEST: INDICATOR",
        "header_bg": "FFF8E1",  # пастельный жёлтый - оттенок для INDICATOR
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_CONTEST"],
        "columns": ["INDICATOR=>INDICATOR_MARK_TYPE", "INDICATOR=>INDICATOR_MATCH", "INDICATOR=>INDICATOR_VALUE"],
        # Назначение: поля из INDICATOR в SUMMARY_CONTEST
    },

    # --- SUMMARY_CONTEST: TOURNAMENT-SCHEDULE — пастельный голубой ---
    {
        "group": "SUMMARY_CONTEST: TOURNAMENT-SCHEDULE",
        "header_bg": "E1F5FE",  # пастельный голубой - оттенок для TOURNAMENT-SCHEDULE
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_CONTEST"],
        "columns": ["TOURNAMENT-SCHEDULE=>START_DT", "TOURNAMENT-SCHEDULE=>END_DT", "TOURNAMENT-SCHEDULE=>RESULT_DT",
                    "TOURNAMENT-SCHEDULE=>TOURNAMENT_STATUS", "TOURNAMENT-SCHEDULE=>TARGET_TYPE"],
        # Назначение: поля из TOURNAMENT-SCHEDULE в SUMMARY_CONTEST
    },

    # --- SUMMARY_CONTEST: REPORT — пастельный зелёный ---
    {
        "group": "SUMMARY_CONTEST: REPORT",
        "header_bg": "E8F5E8",  # пастельный зелёный - оттенок для REPORT
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_CONTEST"],
        "columns": ["REPORT=>CONTEST_DATE", "REPORT=>COUNT_CONTEST_DATE"],
        # Назначение: поля из REPORT в SUMMARY_CONTEST
    },

    # --- SUMMARY_CONTEST: REWARD — пастельный оранжевый ---
    {
        "group": "SUMMARY_CONTEST: REWARD",
        "header_bg": "FFE8CC",  # пастельный оранжевый - оттенок для REWARD
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_CONTEST"],
        "columns": [
            "REWARD=>ADD_DATA => rewardAgainGlobal", "REWARD=>ADD_DATA => rewardAgainTournament",
            "REWARD=>ADD_DATA => outstanding", "REWARD=>ADD_DATA => teamNews", "REWARD=>ADD_DATA => singleNews"
        ],
        # Назначение: поля из REWARD в SUMMARY_CONTEST
    },

    # --- SUMMARY_CONTEST: ДУБЛИ — пастельный розовый ---
    {
        "group": "SUMMARY_CONTEST: DUPLICATES",
        "header_bg": "FFE6F2",  # пастельный розовый - для дублей
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_CONTEST"],
        "columns": ["ДУБЛЬ: CONTEST_CODE_TOURNAMENT_CODE_REWARD_CODE_GROUP_CODE"],
        # Назначение: поля дублей в SUMMARY_CONTEST
    },

    # --- SUMMARY_SCHEDULE: КЛЮЧЕВЫЕ ПОЛЯ — пастельный голубой ---
    {
        "group": "SUMMARY_SCHEDULE: Key Fields",
        "header_bg": "E6F3FF",  # пастельный голубой - как исходные данные
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_SCHEDULE"],
        "columns": ["REWARD_CODE", "CONTEST_CODE", "TOURNAMENT_CODE", "GROUP_CODE"],
        # Назначение: ключевые поля в SUMMARY_SCHEDULE
    },

    # --- SUMMARY_SCHEDULE: CONTEST-DATA — пастельный голубой ---
    {
        "group": "SUMMARY_SCHEDULE: CONTEST-DATA",
        "header_bg": "CCE5FF",  # пастельный голубой - оттенок для CONTEST-DATA
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_SCHEDULE"],
        "columns": [
            "CONTEST-DATA=>FULL_NAME", "CONTEST-DATA=>CONTEST_FEATURE => momentRewarding",
            "CONTEST-DATA=>FACTOR_MATCH", "CONTEST-DATA=>PLAN_MOD_VALUE", "CONTEST-DATA=>BUSINESS_BLOCK",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentStartMailing", "CONTEST-DATA=>CONTEST_FEATURE => tournamentEndMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentRewardingMailing", "CONTEST-DATA=>CONTEST_FEATURE => tournamentLikeMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => capacity", "CONTEST-DATA=>CONTEST_FEATURE => tournamentListMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => vid", "CONTEST-DATA=>CONTEST_FEATURE => tbVisible",
            "CONTEST-DATA=>CONTEST_FEATURE => tbHidden", "CONTEST-DATA=>CONTEST_FEATURE => persomanNumberVisible",
            "CONTEST-DATA=>CONTEST_FEATURE => typeRewarding", "CONTEST-DATA=>CONTEST_FEATURE => masking",
            "CONTEST-DATA=>CONTEST_FEATURE => minNumber", "CONTEST-DATA=>CONTEST_FEATURE => businessBlock",
            "CONTEST-DATA=>CONTEST_FEATURE => accuracy", "CONTEST-DATA=>CONTEST_FEATURE => gosbHidden",
            "CONTEST-DATA=>CONTEST_FEATURE => preferences", "CONTEST-DATA=>CONTEST_FEATURE => persomanNumberHidden",
            "CONTEST-DATA=>CONTEST_FEATURE => gosbVisible", "CONTEST-DATA=>CONTEST_FEATURE => feature"
        ],
        # Назначение: поля из CONTEST-DATA в SUMMARY_SCHEDULE
    },

    # --- SUMMARY_SCHEDULE: GROUP — пастельный зелёный ---
    {
        "group": "SUMMARY_SCHEDULE: GROUP",
        "header_bg": "E8F5E8",  # пастельный зелёный - оттенок для GROUP
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_SCHEDULE"],
        "columns": ["GROUP=>GET_CALC_CRITERION", "GROUP=>ADD_CALC_CRITERION", "GROUP=>ADD_CALC_CRITERION_2"],
        # Назначение: поля из GROUP в SUMMARY_SCHEDULE
    },

    # --- SUMMARY_SCHEDULE: INDICATOR — пастельный жёлтый ---
    {
        "group": "SUMMARY_SCHEDULE: INDICATOR",
        "header_bg": "FFF8E1",  # пастельный жёлтый - оттенок для INDICATOR
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_SCHEDULE"],
        "columns": ["INDICATOR=>INDICATOR_MARK_TYPE", "INDICATOR=>INDICATOR_MATCH", "INDICATOR=>INDICATOR_VALUE"],
        # Назначение: поля из INDICATOR в SUMMARY_SCHEDULE
    },

    # --- SUMMARY_SCHEDULE: TOURNAMENT-SCHEDULE — пастельный голубой ---
    {
        "group": "SUMMARY_SCHEDULE: TOURNAMENT-SCHEDULE",
        "header_bg": "E1F5FE",  # пастельный голубой - оттенок для TOURNAMENT-SCHEDULE
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_SCHEDULE"],
        "columns": ["TOURNAMENT-SCHEDULE=>START_DT", "TOURNAMENT-SCHEDULE=>END_DT", "TOURNAMENT-SCHEDULE=>RESULT_DT",
                    "TOURNAMENT-SCHEDULE=>TOURNAMENT_STATUS", "TOURNAMENT-SCHEDULE=>TARGET_TYPE"],
        # Назначение: поля из TOURNAMENT-SCHEDULE в SUMMARY_SCHEDULE
    },

    # --- SUMMARY_SCHEDULE: REPORT — пастельный зелёный ---
    {
        "group": "SUMMARY_SCHEDULE: REPORT",
        "header_bg": "E8F5E8",  # пастельный зелёный - оттенок для REPORT
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_SCHEDULE"],
        "columns": ["REPORT=>CONTEST_DATE", "REPORT=>COUNT_CONTEST_DATE"],
        # Назначение: поля из REPORT в SUMMARY_SCHEDULE
    },

    # --- SUMMARY_SCHEDULE: REWARD — пастельный оранжевый ---
    {
        "group": "SUMMARY_SCHEDULE: REWARD",
        "header_bg": "FFE8CC",  # пастельный оранжевый - оттенок для REWARD
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_SCHEDULE"],
        "columns": [
            "REWARD=>ADD_DATA => rewardAgainGlobal", "REWARD=>ADD_DATA => rewardAgainTournament",
            "REWARD=>ADD_DATA => outstanding", "REWARD=>ADD_DATA => teamNews", "REWARD=>ADD_DATA => singleNews"
        ],
        # Назначение: поля из REWARD в SUMMARY_SCHEDULE
    },

    # --- SUMMARY_SCHEDULE: ДУБЛИ — пастельный розовый ---
    {
        "group": "SUMMARY_SCHEDULE: DUPLICATES",
        "header_bg": "FFE6F2",  # пастельный розовый - для дублей
        "header_fg": "2C3E50",  # тёмно-серый для читаемости
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY_SCHEDULE"],
        "columns": ["ДУБЛЬ: CONTEST_CODE_TOURNAMENT_CODE_REWARD_CODE_GROUP_CODE"],
        # Назначение: поля дублей в SUMMARY_SCHEDULE
    }
]

# Добавление секции для дублей по CHECK_DUPLICATES
CHECK_DUPLICATES = [
    {"sheet": "CONTEST-DATA", "key": ["CONTEST_CODE"]},
    {"sheet": "GROUP", "key": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"]},
    {"sheet": "INDICATOR", "key": ["CONTEST_CODE", "INDICATOR_ADD_CALC_TYPE"]},
    {"sheet": "REPORT", "key": ["MANAGER_PERSON_NUMBER", "TOURNAMENT_CODE", "CONTEST_CODE"]},
    {"sheet": "REWARD", "key": ["REWARD_CODE"]},
    {"sheet": "REWARD-LINK", "key": ["CONTEST_CODE", "REWARD_CODE"]},
    {"sheet": "TOURNAMENT-SCHEDULE", "key": ["TOURNAMENT_CODE", "CONTEST_CODE"]},
    {"sheet": "ORG_UNIT_V20", "key": ["ORG_UNIT_CODE"]},
    {"sheet": "USER_ROLE", "key": ["RULE_NUM"]},
    {"sheet": "USER_ROLE SB", "key": ["RULE_NUM"]},
    {"sheet": "EMPLOYEE", "key": ["PERSON_NUMBER"]}
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
    Генерирует имя лог-файла с учетом уровня логирования и текущей даты.
    
    Returns:
        str: Путь к лог-файлу в формате 'LOGS/LOGS_LEVEL_YYYY-MM-DD.log'
    """
    # Имя лог-файла по дате с уровнем логирования, например: LOGS_INFO_2025-07-25.log
    level_suffix = f"_{LOG_LEVEL}" if LOG_LEVEL else ""
    date_suffix = f"_{datetime.now().strftime('%Y-%m-%d')}.log"
    return os.path.join(DIR_LOGS, LOG_BASE_NAME + level_suffix + date_suffix)


# === Логирование ===
def setup_logger():
    """
    Настраивает систему логирования для программы.
    
    Создает логгер с двумя обработчиками:
    - Файловый: записывает логи в файл с кодировкой UTF-8
    - Консольный: выводит логи в стандартный вывод
    
    Returns:
        str: Путь к созданному лог-файлу
    """
    log_file = get_log_filename()
    # Если логгер уже инициализирован, не добавляем обработчики ещё раз
    if logging.getLogger().hasHandlers():
        return log_file
    logging.basicConfig(
        level=logging.DEBUG if LOG_LEVEL == "DEBUG" else logging.INFO,  # Уровень логирования
        format="%(asctime)s | %(levelname)s | %(message)s",           # Формат сообщений
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8", mode="a"),  # Файловый обработчик (режим добавления)
            logging.StreamHandler(sys.stdout)                           # Консольный обработчик
        ]
    )
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
    logging.info(LOG_MESSAGES["func_start"].format(func="calculate_tournament_status", params=params))

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
    df['START_DT_parsed'] = df['START_DT'].apply(safe_to_date)      # Парсим дату начала
    df['END_DT_parsed'] = df['END_DT'].apply(safe_to_date)          # Парсим дату окончания
    df['RESULT_DT_parsed'] = df['RESULT_DT'].apply(safe_to_date)    # Парсим дату результатов

    # Получаем максимальные CONTEST_DATE для каждого TOURNAMENT_CODE из REPORT
    # Это нужно для определения, завершились ли все конкурсы турнира
    max_contest_dates = {}
    if df_report is not None and 'CONTEST_DATE' in df_report.columns and 'TOURNAMENT_CODE' in df_report.columns:
        df_report_dates = df_report.copy()
        df_report_dates['CONTEST_DATE_parsed'] = df_report_dates['CONTEST_DATE'].apply(safe_to_date)
        df_report_dates = df_report_dates.dropna(subset=['CONTEST_DATE_parsed', 'TOURNAMENT_CODE'])

        if not df_report_dates.empty:
            # Группируем по коду турнира и находим максимальную дату конкурса
            max_contest_dates = df_report_dates.groupby('TOURNAMENT_CODE')['CONTEST_DATE_parsed'].max().to_dict()

    def get_status(row):
        """
        Определяет статус турнира для конкретной строки данных.
        
        Args:
            row: Строка DataFrame с данными турнира
            
        Returns:
            str: Статус турнира: "АКТИВНЫЙ", "ЗАПЛАНИРОВАН", "ПОДВЕДЕНИЕ ИТОГОВ", "ЗАВЕРШЕН", "НЕОПРЕДЕЛЕН"
        """
        start_dt = row['START_DT_parsed']      # Дата начала турнира
        end_dt = row['END_DT_parsed']          # Дата окончания турнира
        result_dt = row['RESULT_DT_parsed']    # Дата подведения итогов
        tournament_code = row['TOURNAMENT_CODE']  # Код турнира

        # Если нет ключевых дат - возвращаем неопределенный статус
        if not start_dt or not end_dt:
            return "НЕОПРЕДЕЛЕН"

        # 1. Если сегодня между START_DT и END_DT включительно → турнир активен
        if start_dt <= today <= end_dt:
            return "АКТИВНЫЙ"

        # 2. Если сегодня < START_DT → турнир еще не начался
        if today < start_dt:
            return "ЗАПЛАНИРОВАН"

        # 3. Если сегодня > END_DT → турнир закончился, но возможно еще подводятся итоги
        if today > end_dt:
            # Если нет RESULT_DT или сегодня < RESULT_DT → еще идет подведение итогов
            if not result_dt or today < result_dt:
                return "ПОДВЕДЕНИЕ ИТОГОВ"

            # 4. Если сегодня >= RESULT_DT → проверяем, завершились ли все конкурсы
            if today >= result_dt:
                max_contest_date = max_contest_dates.get(tournament_code)

                # Если нет данных в REPORT для этого турнира → не можем определить завершение
                if not max_contest_date:
                    return "ПОДВЕДЕНИЕ ИТОГОВ"

                # Сравниваем максимальную CONTEST_DATE с RESULT_DT
                # Если последний конкурс был до подведения итогов → турнир завершен
                if max_contest_date < result_dt:
                    return "ПОДВЕДЕНИЕ ИТОГОВ"
                else:
                    return "ЗАВЕРШЕН"

        return "НЕОПРЕДЕЛЕН"  # Запасной вариант для неожиданных случаев

    # Применяем функцию для каждой строки DataFrame
    df['CALC_TOURNAMENT_STATUS'] = df.apply(get_status, axis=1)

    # Удаляем временные колонки с распарсенными датами
    df = df.drop(columns=['START_DT_parsed', 'END_DT_parsed', 'RESULT_DT_parsed'])

    # Логируем статистику по статусам для мониторинга
    status_counts = df['CALC_TOURNAMENT_STATUS'].value_counts()
    logging.info(LOG_MESSAGES["tournament_status_stats"].format(stats=status_counts.to_dict()))

    # Засекаем время выполнения и логируем завершение
    func_time = time() - func_start
    logging.info(LOG_MESSAGES["func_end"].format(
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
        logging.warning(LOG_MESSAGES["field_length_missing"].format(fields=missing_fields, sheet=sheet_name))
        # Создаем пустую колонку если нет полей для проверки
        df[result_column] = '-'
        return df

    total_rows = len(df)  # Общее количество строк для проверки
    logging.info(LOG_MESSAGES["field_length_start"].format(sheet=sheet_name, rows=total_rows))

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
                logging.debug(LOG_MESSAGES["field_length_violation"].format(
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
            logging.info(LOG_MESSAGES["field_length_progress"].format(
                processed=idx + 1, total=total_rows, percent=percent
            ))

    # Добавляем колонку с результатами проверки к DataFrame
    df[result_column] = results

    # Логируем финальную статистику выполнения
    func_time = time() - func_start
    logging.info(LOG_MESSAGES["field_length_stats"].format(
        correct=correct_count, errors=error_count, total=total_rows
    ))
    logging.info(LOG_MESSAGES["field_length_end"].format(time=func_time, sheet=sheet_name))

    return df


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
    logging.info(LOG_MESSAGES["func_start"].format(func="read_csv_file", params=params))
    
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
                logging.debug(LOG_MESSAGES["csv_sample"].format(
                    file=file_path,
                    column=col,
                    sample=df[col].dropna().head(2).to_list()  # Первые 2 непустых значения
                ))
        
        # Логируем успешное чтение файла
        logging.info(LOG_MESSAGES["read_ok"].format(file_path=file_path, rows=len(df), cols=len(df.columns)))
        
        # Засекаем время выполнения и логируем завершение
        func_time = time() - func_start
        logging.info(LOG_MESSAGES["func_end"].format(func="read_csv_file", params=params, time=func_time))
        return df
        
    except Exception as e:
        # Логируем ошибку и возвращаем None
        func_time = time() - func_start
        logging.error(LOG_MESSAGES["read_fail"].format(file_path=file_path, error=e))
        logging.error(LOG_MESSAGES["func_error"].format(func="read_csv_file", params=params, error=e))
        logging.info(LOG_MESSAGES["func_end"].format(func="read_csv_file", params=params, time=func_time))
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
    logging.info(LOG_MESSAGES["func_start"].format(func="write_to_excel", params=params))
    
    try:
        # Определяем порядок листов: SUMMARY первый, затем уникальные листы, остальные по алфавиту
        unique_summary_sheets = ["SUMMARY_REWARD", "SUMMARY_CONTEST", "SUMMARY_SCHEDULE"]
        other_sheets = [s for s in sheets_data if s not in ["SUMMARY"] + unique_summary_sheets]
        ordered_sheets = ["SUMMARY"] + [s for s in unique_summary_sheets if s in sheets_data] + sorted(other_sheets)
        
        # Создаем Excel файл с помощью pandas ExcelWriter
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # Записываем каждый лист
            for sheet_name in ordered_sheets:
                df, params_sheet = sheets_data[sheet_name]  # Получаем данные и параметры листа
                df.to_excel(writer, index=False, sheet_name=sheet_name)  # Записываем данные
                
                # Получаем объект листа для форматирования
                ws = writer.sheets[sheet_name]
                _format_sheet(ws, df, params_sheet)  # Применяем форматирование
                
                # Логируем создание листа
                logging.info(LOG_MESSAGES["sheet_written"].format(sheet=sheet_name, rows=len(df), cols=len(df.columns)))
            
            # Делаем SUMMARY лист активным по умолчанию
            writer.book.active = writer.book.sheetnames.index("SUMMARY")
            writer.book.save(output_path)  # Сохраняем файл
        
        # Логируем успешное завершение
        func_time = time() - func_start
        logging.info(LOG_MESSAGES["func_end"].format(func="write_to_excel", params=params, time=func_time))
        
    except Exception as ex:
        # Логируем ошибку
        func_time = time() - func_start
        logging.error(LOG_MESSAGES["func_error"].format(func="write_to_excel", params=params, error=ex))
        logging.info(LOG_MESSAGES["func_end"].format(func="write_to_excel", params=params, time=func_time))


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
    logging.debug(LOG_MESSAGES["func_start"].format(func="_format_sheet", params=params_str))
    header_font = Font(bold=True)
    align_center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    align_data = Alignment(horizontal="left", vertical="center", wrap_text=True)

    for col_num, cell in enumerate(ws[1], 1):
        cell.font = header_font
        cell.alignment = align_center
        col_letter = get_column_letter(col_num)
        col_name = cell.value

        # Вычисляем ширину колонки с учетом новых параметров
        width = calculate_column_width(col_name, ws, params, col_num)
        ws.column_dimensions[col_letter].width = width

        # Определяем режим для логирования
        width_mode_info = params.get("col_width_mode", "AUTO")
        added_cols_width = params.get("added_columns_width", {})
        if col_name in added_cols_width:
            width_mode_info = added_cols_width[col_name].get("width_mode", "AUTO")

        logging.debug(LOG_MESSAGES["column_width_set"].format(
            sheet=ws.title, column=col_name, width=width, mode=width_mode_info
        ))

    # Применяем цветовую схему
    apply_color_scheme(ws, ws.title)

    # Данные: перенос строк, выравнивание по левому краю, по вертикали по центру
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            cell.alignment = align_data

    # Закрепление строк и столбцов
    ws.freeze_panes = params.get("freeze", "A2")
    ws.auto_filter.ref = ws.dimensions

    func_time = time() - func_start
    logging.debug(LOG_MESSAGES["func_end"].format(func="_format_sheet", params=params_str, time=func_time))

    # Данные: перенос строк, выравнивание по левому краю, по вертикали по центру
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            cell.alignment = align_data


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
                logging.debug(LOG_MESSAGES["debug_json_error"].format(error=ex, string=repr(s)))
                logging.debug(LOG_MESSAGES["debug_json_error"].format(error=ex, string=repr(s)))
                logging.debug(LOG_MESSAGES["debug_json_error"].format(error=ex, string=repr(s)))
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
        logging.debug(LOG_MESSAGES["debug_json_preserve"].format(string=repr(s)))
        logging.debug(LOG_MESSAGES["debug_json_preserve"].format(string=repr(s)))
        logging.debug(LOG_MESSAGES["debug_json_preserve"].format(string=repr(s)))
        return s  # Возвращаем исходную строку с тройными кавычками


def flatten_json_column_recursive(df, column, prefix=None, sheet=None, sep="; "):
    func_start = tmod.time()
    n_rows = len(df)
    n_errors = 0
    prefix = prefix if prefix is not None else column
    logging.info(LOG_MESSAGES["func_start"].format(func="flatten_json_column_recursive",
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

    new_cols = {}
    for idx, val in enumerate(df[column_to_parse]):
        try:
            parsed = None
            # Строка — парсим JSON; dict/list — оставляем; иначе пропускаем
            if isinstance(val, str):
                val = val.strip()
                if val in {"", "-", "None", "null"}:
                    parsed = {}
                else:
                    parsed = safe_json_loads(val)
            elif isinstance(val, (dict, list)):
                parsed = val
            else:
                # Необрабатываемые типы (например float('nan'))
                parsed = {}
            flat = extract(parsed, prefix)
        except Exception as ex:
            logging.debug(LOG_MESSAGES["json_flatten_error"].format(row=idx, error=ex))
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
        
        logging.info(LOG_MESSAGES["contest_feature_restored"])

    logging.info(LOG_MESSAGES["json_flatten_summary"].format(column=column, count=len(new_cols)))
    logging.info(LOG_MESSAGES["json_flatten_keys"].format(keys=list(new_cols.keys())))
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

                logging.debug(LOG_MESSAGES["dynamic_color_scheme"].format(
                    sheet=f"{sheet_src} -> {sheet_dst}", columns=columns
                ))
                color_idx += 1

    return dynamic_scheme


def apply_color_scheme(ws, sheet_name):
    """
    Окрашивает заголовки и/или всю колонку на листе Excel по схеме COLOR_SCHEME.
    Также применяет динамически сгенерированную схему из MERGE_FIELDS.
    Все действия логируются через LOG_MESSAGES.
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
                logging.debug(LOG_MESSAGES["color_scheme_applied"].format(
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
                logging.debug(LOG_MESSAGES["color_scheme_applied"].format(
                    sheet=sheet_name,
                    col=colname,
                    scope="all",
                    color=color_conf.get("column_bg", "default")
                ))


def collect_summary_keys(dfs):
    """
    Собирает все реально существующие сочетания ключей,
    включая осиротевшие коды и сочетания с GROUP_VALUE.
    Теперь учитывает ВСЕ коды из всех таблиц, даже если связи нет.
    """
    all_rows = []

    rewards = dfs.get("REWARD-LINK", pd.DataFrame())
    tournaments = dfs.get("TOURNAMENT-SCHEDULE", pd.DataFrame())
    groups = dfs.get("GROUP", pd.DataFrame())
    reward_data = dfs.get("REWARD", pd.DataFrame())  # Добавляем доступ к таблице REWARD

    all_contest_codes = set()
    all_tournament_codes = set()
    all_reward_codes = set()
    all_group_codes = set()
    all_group_values = set()

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

    # КРИТИЧНО: Добавляем ВСЕ REWARD_CODE из таблицы REWARD, даже если их нет в REWARD-LINK
    if not reward_data.empty:
        all_reward_codes.update(reward_data["REWARD_CODE"].dropna())

    # 1. Для каждого CONTEST_CODE
    for code in all_contest_codes:
        tourns = tournaments[tournaments["CONTEST_CODE"] == code][
            "TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else []
        rewards_ = rewards[rewards["CONTEST_CODE"] == code][
            "REWARD_CODE"].dropna().unique() if not rewards.empty else []
        groups_df = groups[groups["CONTEST_CODE"] == code] if not groups.empty else pd.DataFrame()
        groups_ = groups_df["GROUP_CODE"].dropna().unique() if not groups_df.empty else []
        group_values_ = groups_df["GROUP_VALUE"].dropna().unique() if not groups_df.empty else []

        tourns = tourns if len(tourns) else ["-"]
        rewards_ = rewards_ if len(rewards_) else ["-"]
        groups_ = groups_ if len(groups_) else ["-"]
        group_values_ = group_values_ if len(group_values_) else ["-"]

        for t in tourns:
            for r in rewards_:
                for g in groups_:
                    for gv in group_values_:
                        all_rows.append((str(code), str(t), str(r), str(g), str(gv)))

    # 2. Для каждого TOURNAMENT_CODE (даже если нет CONTEST_CODE)
    if not tournaments.empty:
        for t_code in tournaments["TOURNAMENT_CODE"].dropna().unique():
            code = tournaments[tournaments["TOURNAMENT_CODE"] == t_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
            rewards_ = rewards[rewards["CONTEST_CODE"] == code][
                "REWARD_CODE"].dropna().unique() if not rewards.empty else ["-"]
            groups_df = groups[groups["CONTEST_CODE"] == code] if not groups.empty else pd.DataFrame()
            groups_ = groups_df["GROUP_CODE"].dropna().unique() if not groups_df.empty else []
            group_values_ = groups_df["GROUP_VALUE"].dropna().unique() if not groups_df.empty else []
            rewards_ = rewards_ if len(rewards_) else ["-"]
            groups_ = groups_ if len(groups_) else ["-"]
            group_values_ = group_values_ if len(group_values_) else ["-"]
            for r in rewards_:
                for g in groups_:
                    for gv in group_values_:
                        all_rows.append((str(code), str(t_code), str(r), str(g), str(gv)))

    # 3. Для каждого REWARD_CODE (даже если нет CONTEST_CODE)
    # ИСПРАВЛЕНИЕ: Теперь обрабатываем ВСЕ REWARD_CODE, включая осиротевшие
    for r_code in all_reward_codes:
        # Ищем CONTEST_CODE для этого REWARD_CODE
        if not rewards.empty:
            code = rewards[rewards["REWARD_CODE"] == r_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
        else:
            code = "-"

        # Ищем связанные TOURNAMENT_CODE
        if code != "-" and not tournaments.empty:
            tourns = tournaments[tournaments["CONTEST_CODE"] == code]["TOURNAMENT_CODE"].dropna().unique()
        else:
            tourns = []

        # Ищем связанные GROUP_CODE и GROUP_VALUE
        if code != "-" and not groups.empty:
            groups_df = groups[groups["CONTEST_CODE"] == code]
            groups_ = groups_df["GROUP_CODE"].dropna().unique() if not groups_df.empty else []
            group_values_ = groups_df["GROUP_VALUE"].dropna().unique() if not groups_df.empty else []
        else:
            groups_ = []
            group_values_ = []

        # Если нет связей, создаем строки с "-" для остальных полей
        tourns = tourns if len(tourns) else ["-"]
        groups_ = groups_ if len(groups_) else ["-"]
        group_values_ = group_values_ if len(group_values_) else ["-"]

        for t in tourns:
            for g in groups_:
                for gv in group_values_:
                    all_rows.append((str(code), str(t), str(r_code), str(g), str(gv)))

    # 4. Для каждого GROUP_CODE (даже если нет CONTEST_CODE)
    if not groups.empty:
        for g_code in groups["GROUP_CODE"].dropna().unique():
            code = groups[groups["GROUP_CODE"] == g_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
            tourns = tournaments[tournaments["CONTEST_CODE"] == code][
                "TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else []
            rewards_ = rewards[rewards["CONTEST_CODE"] == code][
                "REWARD_CODE"].dropna().unique() if not rewards.empty else []
            group_values_ = groups[groups["GROUP_CODE"] == g_code][
                "GROUP_VALUE"].dropna().unique() if not groups.empty else []
            tourns = tourns if len(tourns) else ["-"]
            rewards_ = rewards_ if len(rewards_) else ["-"]
            group_values_ = group_values_ if len(group_values_) else ["-"]
            for t in tourns:
                for r in rewards_:
                    for gv in group_values_:
                        all_rows.append((str(code), str(t), str(r), str(g_code), str(gv)))

    # Удалить дубли
    summary_keys = pd.DataFrame(all_rows, columns=SUMMARY_KEY_COLUMNS).drop_duplicates().reset_index(drop=True)
    return summary_keys


def mark_duplicates(df, key_cols, sheet_name=None):
    """
    Добавляет колонку с пометкой о дублях по key_cols.
    Если строк по ключу больше одной — пишем xN, иначе пусто.
    """
    params = {"sheet": sheet_name, "keys": key_cols}
    func_start = tmod.time()
    col_name = "DUPLICATES"
    

    logging.info(LOG_MESSAGES["duplicates_start"].format(sheet=sheet_name, keys=key_cols))
    try:
        dup_counts = df.groupby(key_cols)[key_cols[0]].transform('count')
        df[col_name] = dup_counts.apply(lambda x: f"x{x}" if x > 1 else "")
        n_duplicates = (df[col_name] != "").sum()
        func_time = tmod.time() - func_start
        logging.info(LOG_MESSAGES["duplicates_found"].format(count=n_duplicates, sheet=sheet_name, keys=key_cols))
        logging.info(LOG_MESSAGES["duplicates_end"].format(sheet=sheet_name, time=func_time))
    except Exception as ex:
        func_time = tmod.time() - func_start
        logging.error(LOG_MESSAGES["duplicates_error"].format(sheet=sheet_name, keys=key_cols, error=ex))
        logging.info(LOG_MESSAGES["duplicates_end"].format(sheet=sheet_name, time=func_time))
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
    logging.info(LOG_MESSAGES["func_start"].format(
        func="add_tournament_status_counts",
        params=f"(contest_rows: {len(df_contest)}, tournament_rows: {len(df_tournament)})"
    ))
    
    # Создаем копию DataFrame для безопасной работы
    df_result = df_contest.copy()
    
    # Получаем уникальные статусы турниров (исключаем пустые значения)
    unique_statuses = df_tournament['TOURNAMENT_STATUS'].dropna().unique()
    
    # Сортируем статусы для предсказуемого порядка колонок
    unique_statuses = sorted([status for status in unique_statuses if status.strip()])
    
    logging.info(LOG_MESSAGES["tournament_status_counts_found"].format(count=len(unique_statuses), statuses=unique_statuses))
    
    # Для каждого статуса подсчитываем количество уникальных турниров по CONTEST_CODE
    for status in unique_statuses:
        # Фильтруем турниры по статусу
        status_df = df_tournament[df_tournament['TOURNAMENT_STATUS'] == status]
        
        if len(status_df) == 0:
            # Если нет турниров с таким статусом - все конкурсы получают 0
            col_name = f"TOURNAMENT_COUNT_{status.upper()}"
            df_result[col_name] = 0
            logging.debug(LOG_MESSAGES["tournament_status_counts_debug"].format(status=status, contests=0, tournaments=0))
            continue
        
        # Подсчитываем количество уникальных турниров для каждого конкурса
        status_counts = status_df.groupby('CONTEST_CODE')['TOURNAMENT_CODE'].nunique().to_dict()
        
        # Добавляем колонку с количеством турниров данного статуса
        col_name = f"TOURNAMENT_COUNT_{status.upper()}"
        df_result[col_name] = df_result['CONTEST_CODE'].map(status_counts).fillna(0).astype(int)
        
        # Логируем статистику для этого статуса
        total_tournaments = status_counts.values()
        total_contests = len(status_counts)
        logging.debug(LOG_MESSAGES["tournament_status_counts_debug"].format(status=status, contests=total_contests, tournaments=sum(total_tournaments)))
    
    # Логируем итоговую статистику
    func_time = time() - func_start
    added_columns = [f"TOURNAMENT_COUNT_{status.upper()}" for status in unique_statuses]
    logging.info(LOG_MESSAGES["tournament_status_counts_added"].format(count=len(added_columns), columns=added_columns))
    logging.info(LOG_MESSAGES["func_end"].format(
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
    logging.info(LOG_MESSAGES["func_start"].format(
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
        logging.warning(LOG_MESSAGES["missing_column"].format(column=col, sheet=ref_sheet_name))
        df_ref[col] = "-"

    missing_keys = [k for k in src_keys if k not in df_ref.columns]
    for k in missing_keys:
        logging.warning(LOG_MESSAGES["missing_key"].format(key=k, sheet=ref_sheet_name))
        df_ref[k] = "-"

    if mode == "count":
        new_keys = df_base.apply(lambda row: tuple_key(row, dst_keys), axis=1)
        group_counts = df_ref.groupby(src_keys).size()
        
        # Создаем словарь для сопоставления ключей
        # group_counts.items() возвращает (index, value), где index может быть строкой или кортежем
        count_dict = {}
        for key_tuple, count in group_counts.items():
            count_dict[key_tuple] = count
            
        for col in columns:
            count_col_name = f"{ref_sheet_name}=>COUNT_{col}"
            # Сопоставляем ключи и заполняем 0 для отсутствующих
            # Используем прямое сопоставление через Series для правильной работы с индексами
            # Исправляем сопоставление для правильной работы с разными типами ключей
            # Если у нас один ключ, используем прямое сопоставление через Series
            if len(src_keys) == 1:
                # Для одного ключа нужно извлечь первый элемент из кортежей
                new_keys_single = new_keys.apply(lambda x: x[0] if x and len(x) > 0 else None)
                df_base[count_col_name] = new_keys_single.map(group_counts).fillna(0).astype(int)
            else:
                # Для составных ключей используем словарь
                df_base[count_col_name] = new_keys.map(count_dict).fillna(0).astype(int)
        logging.info(LOG_MESSAGES["func_end"].format(
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
        for col in columns:
            ref_map = dict(zip(df_ref_keys, df_ref[col]))
            new_col_name = f"{ref_sheet_name}=>{col}"
            df_base[new_col_name] = new_keys.map(ref_map).fillna("-")
            # Специально для REWARD_LINK =>CONTEST_CODE: auto-rename, если создали с дефисом
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
        # Новая логика: размножение строк при множественных совпадениях
        logging.info(LOG_MESSAGES["multiply_rows_start"].format(sheet=sheet_name, column=columns))
        result_rows = []
        old_rows_count = len(df_base)

        for base_idx, base_row in df_base.iterrows():
            base_key = tuple_key(base_row, dst_keys)
            # Находим все строки в df_ref с таким ключом
            matching_ref_rows = df_ref[df_ref_keys == base_key]

            if matching_ref_rows.empty:
                # Нет совпадений - добавляем строку с пустыми значениями
                new_row = base_row.copy()
                for col in columns:
                    new_col_name = f"{ref_sheet_name}=>{col}"
                    new_row[new_col_name] = "-"
                result_rows.append(new_row)
            else:
                # Есть совпадения - создаем строку для каждого совпадения
                for ref_idx, ref_row in matching_ref_rows.iterrows():
                    new_row = base_row.copy()
                    for col in columns:
                        new_col_name = f"{ref_sheet_name}=>{col}"
                        new_row[new_col_name] = ref_row[col]
                    result_rows.append(new_row)

        # Создаем новый DataFrame из размноженных строк
        df_base = pd.DataFrame(result_rows).reset_index(drop=True)
        new_rows_count = len(df_base)
        multiply_factor = round(new_rows_count / old_rows_count, 2) if old_rows_count > 0 else 0
        logging.info(LOG_MESSAGES["multiply_rows_result"].format(
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

    logging.info(LOG_MESSAGES["func_end"].format(
        func="add_fields_to_sheet",
        params=f"(лист: {sheet_name}, поля: {columns}, ключ: {dst_keys}->{src_keys}, mode: {mode}, multiply: {multiply_rows})",
        time=time() - func_start
    ))
    return df_base


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
    for rule in merge_fields:
        sheet_src = rule["sheet_src"]
        sheet_dst = rule["sheet_dst"]
        src_keys = rule["src_key"] if isinstance(rule["src_key"], list) else [rule["src_key"]]
        dst_keys = rule["dst_key"] if isinstance(rule["dst_key"], list) else [rule["dst_key"]]
        col_names = rule["column"]
        mode = rule.get("mode", "value")
        multiply_rows = rule.get("multiply_rows", False)
        
        # Новые параметры
        status_filters = rule.get("status_filters", None)
        custom_conditions = rule.get("custom_conditions", None)
        group_by = rule.get("group_by", None)
        aggregate = rule.get("aggregate", None)
        
        params_str = f"(src: {sheet_src} -> dst: {sheet_dst}, поля: {col_names}, ключ: {dst_keys}<-{src_keys}, mode: {mode}, multiply: {multiply_rows})"
        
        # Добавляем информацию о новых параметрах в логирование
        if status_filters:
            params_str += f", status_filters: {status_filters}"
        if custom_conditions:
            params_str += f", custom_conditions: {list(custom_conditions.keys())}"
        if group_by:
            params_str += f", group_by: {group_by}"
        if aggregate:
            params_str += f", aggregate: {list(aggregate.keys())}"

        if sheet_src not in sheets_data or sheet_dst not in sheets_data:
            logging.warning(LOG_MESSAGES.get("field_missing", LOG_MESSAGES["func_error"]).format(
                column=col_names, src_sheet=sheet_src, src_key=src_keys
            ))
            continue

        df_src = sheets_data[sheet_src][0].copy()
        df_dst, params_dst = sheets_data[sheet_dst]

        logging.info(LOG_MESSAGES["func_start"].format(func="merge_fields_across_sheets", params=params_str))
        
        # Применяем фильтрацию к исходным данным
        df_src_filtered = apply_filters_to_dataframe(df_src, status_filters, custom_conditions, sheet_src)
        
        # Применяем группировку и агрегацию если необходимо
        if group_by or aggregate:
            df_src_filtered = apply_grouping_and_aggregation(df_src_filtered, group_by, aggregate, sheet_src)
        
        # Вызываем основную функцию добавления полей
        df_dst = add_fields_to_sheet(df_dst, df_src_filtered, src_keys, dst_keys, col_names, sheet_dst, sheet_src, mode=mode,
                                     multiply_rows=multiply_rows)

        # Сохраняем информацию о ширине колонок для добавленных полей
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
        logging.info(LOG_MESSAGES["func_end"].format(func="merge_fields_across_sheets", params=params_str, time=0))
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
                logging.info(LOG_MESSAGES.get("status_filter_applied", "Применен фильтр по статусу: {column}={values}, осталось строк: {count}").format(
                    column=column, values=allowed_values, count=len(df_filtered)
                ))
            else:
                logging.warning(LOG_MESSAGES.get("status_filter_column_missing", "Колонка для фильтрации по статусу не найдена: {column} в листе {sheet}").format(
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
                
                logging.info(LOG_MESSAGES.get("custom_filter_applied", "Применено пользовательское условие: {column}={condition}, осталось строк: {count}").format(
                    column=column, condition=str(condition), count=len(df_filtered)
                ))
            else:
                logging.warning(LOG_MESSAGES.get("custom_filter_column_missing", "Колонка для пользовательского условия не найдена: {column} в листе {sheet}").format(
                    column=column, sheet=sheet_name
                ))
    
    filtered_count = len(df_filtered)
    if original_count != filtered_count:
        logging.info(LOG_MESSAGES.get("filtering_completed", "Фильтрация завершена: {original} -> {filtered} строк в листе {sheet}").format(
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
                logging.warning(LOG_MESSAGES.get("grouping_columns_missing", "Колонки для группировки не найдены: {columns} в листе {sheet}").format(
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
                        logging.warning(LOG_MESSAGES.get("aggregate_column_missing", "Колонка для агрегации не найдена: {column} в листе {sheet}").format(
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
                    logging.warning(LOG_MESSAGES.get("aggregate_column_missing", "Колонка для агрегации не найдена: {column} в листе {sheet}").format(
                        column=col, sheet=sheet_name
                    ))
            
            if agg_dict:
                df_grouped = df_grouped.agg(agg_dict).to_frame().T
        
        grouped_count = len(df_grouped)
        logging.info(LOG_MESSAGES.get("grouping_completed", "Группировка и агрегация завершены: {original} -> {grouped} строк в листе {sheet}").format(
            original=original_count, grouped=grouped_count, sheet=sheet_name
        ))
        
    except Exception as e:
        logging.error(LOG_MESSAGES.get("grouping_error", "Ошибка при группировке в листе {sheet}: {error}").format(
            sheet=sheet_name, error=str(e)
        ))
        return df
    
    return df_grouped


def create_unique_summary_sheet(df_summary, key_column, sheet_name):
    """
    Создает уникальный лист SUMMARY по указанному ключевому столбцу.
    Каждый код = 1 строка, если под этот код других кодов несколько - оставляем первый попавшийся.
    
    Args:
        df_summary: исходный DataFrame SUMMARY
        key_column: ключевой столбец для уникальности (REWARD_CODE, CONTEST_CODE, TOURNAMENT_CODE)
        sheet_name: имя создаваемого листа
        
    Returns:
        DataFrame с уникальными строками по ключевому столбцу
    """
    func_start = time()
    logging.info(LOG_MESSAGES["summary_sheet_start"].format(sheet_name=sheet_name, key_column=key_column))
    
    try:
        if df_summary.empty:
            logging.warning(LOG_MESSAGES.get("summary_sheet_error", "Исходный DataFrame SUMMARY пуст").format(
                sheet_name=sheet_name, error="Пустой DataFrame"
            ))
            return pd.DataFrame()
        
        if key_column not in df_summary.columns:
            logging.error(LOG_MESSAGES.get("summary_sheet_error", "Ключевой столбец {key_column} не найден в SUMMARY").format(
                sheet_name=sheet_name, error=f"Столбец {key_column} отсутствует"
            ))
            return pd.DataFrame()
        
        # Создаем копию для работы
        df_unique = df_summary.copy()
        original_count = len(df_unique)
        
        # Удаляем строки с пустыми значениями ключевого столбца
        df_unique = df_unique[df_unique[key_column].notna() & (df_unique[key_column] != '') & (df_unique[key_column] != '-')]
        
        if df_unique.empty:
            logging.warning(LOG_MESSAGES.get("summary_sheet_error", "Нет данных с заполненным ключевым столбцом {key_column}").format(
                sheet_name=sheet_name, error=f"Нет данных для {key_column}"
            ))
            return pd.DataFrame()
        
        # Подсчитываем уникальные значения
        unique_count = df_unique[key_column].nunique()
        logging.info(LOG_MESSAGES["summary_sheet_processing"].format(
            total_rows=original_count, unique_count=unique_count
        ))
        
        # Группируем по ключевому столбцу и берем первую строку для каждой группы
        df_unique = df_unique.groupby(key_column).first().reset_index()
        
        final_count = len(df_unique)
        removed_count = original_count - final_count
        
        logging.info(LOG_MESSAGES["summary_sheet_duplicates_removed"].format(
            removed_count=removed_count, final_count=final_count
        ))
        
        # Сортируем по ключевому столбцу для удобства
        df_unique = df_unique.sort_values(by=key_column)
        
        func_time = time() - func_start
        logging.info(LOG_MESSAGES["summary_sheet_completed"].format(
            sheet_name=sheet_name, final_count=final_count, key_column=key_column
        ))
        
        return df_unique
        
    except Exception as e:
        func_time = time() - func_start
        logging.error(LOG_MESSAGES.get("summary_sheet_error", "Ошибка создания листа {sheet_name}: {error}").format(
            sheet_name=sheet_name, error=str(e)
        ))
        return pd.DataFrame()


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
        logging.debug(LOG_MESSAGES["gender_by_patronymic"].format(
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
        logging.debug(LOG_MESSAGES["gender_by_name"].format(
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
        logging.debug(LOG_MESSAGES["gender_by_surname"].format(
            row=row_idx, surname=surname, gender=gender
        ))
        return gender

    # Не удалось определить
    logging.debug(LOG_MESSAGES["gender_unknown"].format(
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
        logging.warning(LOG_MESSAGES["gender_detection_missing"].format(columns=missing_columns, sheet=sheet_name))
        df['AUTO_GENDER'] = '-'
        return df

    total_rows = len(df)
    logging.info(LOG_MESSAGES["gender_detection_start"].format(sheet=sheet_name, rows=total_rows))

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
            logging.info(LOG_MESSAGES["gender_detection_progress"].format(
                processed=idx + 1, total=total_rows, percent=percent
            ))

    # Добавляем колонку к DataFrame
    df['AUTO_GENDER'] = auto_gender

    # Логируем финальную статистику
    func_time = time() - func_start
    logging.info(LOG_MESSAGES["gender_detection_stats"].format(
        male=male_count, female=female_count, unknown=unknown_count, total=total_rows
    ))
    logging.info(LOG_MESSAGES["gender_detection_end"].format(time=func_time, sheet=sheet_name))

    return df


def build_summary_sheet(dfs, params_summary, merge_fields):
    func_start = time()
    params_log = f"(лист: {params_summary['sheet']})"
    logging.info(LOG_MESSAGES["func_start"].format(func="build_summary_sheet", params=params_log))

    summary = collect_summary_keys(dfs)

    logging.info(LOG_MESSAGES["summary"].format(summary=f"Каркас: {len(summary)} строк (реальные комбинации ключей)"))
    logging.debug(LOG_MESSAGES["debug_head"].format(sheet=params_summary["sheet"], head=summary.head(5).to_string()))

    # Универсально добавляем все поля по merge_fields
    for field in merge_fields:
        col_names = field["column"]
        if isinstance(col_names, str):
            col_names = [col_names]
        sheet_src = field["sheet_src"]
        src_keys = field["src_key"] if isinstance(field["src_key"], list) else [field["src_key"]]
        dst_keys = field["dst_key"] if isinstance(field["dst_key"], list) else [field["dst_key"]]
        mode = field.get("mode", "value")
        params_str = f"(лист-источник: {sheet_src}, поля: {col_names}, ключ: {dst_keys}->{src_keys}, mode: {mode})"
        logging.info(LOG_MESSAGES["func_start"].format(func="add_fields_to_sheet", params=params_str))
        ref_df = dfs.get(sheet_src)
        if ref_df is None:
            logging.warning(LOG_MESSAGES.get("field_missing", LOG_MESSAGES["func_error"]).format(
                column=col_names, src_sheet=sheet_src, src_key=src_keys
            ))
            continue

        multiply_rows = field.get("multiply_rows", False)
        summary = add_fields_to_sheet(summary, ref_df, src_keys, dst_keys, col_names, params_summary["sheet"],
                                      sheet_src, mode=mode, multiply_rows=multiply_rows)
        logging.info(LOG_MESSAGES["func_end"].format(func="add_fields_to_sheet", params=params_str, time=0))

    n_rows, n_cols = summary.shape
    func_time = time() - func_start
    logging.info(LOG_MESSAGES["fields_summary"].format(rows=n_rows, cols=n_cols))
    logging.info(LOG_MESSAGES["sheet_written"].format(sheet=params_summary['sheet'], rows=n_rows, cols=n_cols))
    logging.info(LOG_MESSAGES["func_end"].format(func="build_summary_sheet", params=params_log, time=func_time))
    logging.debug(LOG_MESSAGES["debug_columns"].format(sheet=params_summary["sheet"],
                                                       columns=', '.join(summary.columns.tolist())))
    logging.debug(LOG_MESSAGES["debug_head"].format(sheet=params_summary["sheet"], head=summary.head(5).to_string()))
    return summary


# Функция enrich_reward_with_contest_code удалена - CONTEST_CODE теперь добавляется через merge_fields_across_sheets

def main():
    start_time = datetime.now()
    log_file = setup_logger()
    logging.info(LOG_MESSAGES["start"].format(time=start_time.strftime("%Y-%m-%d %H:%M:%S")))

    sheets_data = {}
    files_processed = 0
    rows_total = 0
    summary = []

    # 1. Чтение всех CSV и разворот ВСЕХ JSON‑полей на каждом листе
    for file_conf in INPUT_FILES:
        file_path = find_file_case_insensitive(DIR_INPUT, file_conf["file"], [".csv", ".CSV"])
        sheet_name = file_conf["sheet"]
        
        # Проверяем, найден ли файл
        if file_path is None:
            logging.error(LOG_MESSAGES["file_not_found"].format(file=file_conf["file"], directory=DIR_INPUT))
            summary.append(f"{sheet_name}: файл не найден")
            continue
        
        logging.info(LOG_MESSAGES["reading_file"].format(file_path=file_path))
        df = read_csv_file(file_path)
        if df is not None:
            # --- Разворачиваем только нужные JSON-поля по строгому списку ---
            json_columns = JSON_COLUMNS.get(sheet_name, [])
            for json_conf in json_columns:
                col = json_conf["column"]
                prefix = json_conf.get("prefix", col)
                if col in df.columns:
                    df = flatten_json_column_recursive(df, col, prefix=prefix, sheet=sheet_name)
                    logging.info(LOG_MESSAGES["json_flatten_done"].format(sheet=sheet_name, column=col, prefix=prefix))
                else:
                    logging.warning(LOG_MESSAGES["json_flatten_missing"].format(sheet=sheet_name, column=col))

            # Для дебага: логируем итоговый список колонок после всех разворотов
            logging.debug(
                LOG_MESSAGES["debug_columns"].format(sheet=sheet_name, columns=', '.join(df.columns.tolist())))
            sheets_data[sheet_name] = (df, file_conf)
            files_processed += 1
            rows_total += len(df)
            summary.append(f"{sheet_name}: {len(df)} строк")
        else:
            summary.append(f"{sheet_name}: ошибка")
    # CONTEST_CODE добавляется через merge_fields_across_sheets, поэтому enrich_reward_with_contest_code больше не нужен
    # if "REWARD" in sheets_data and "REWARD-LINK" in sheets_data:
    #     df_reward, conf_reward = sheets_data["REWARD"]
    #     df_link, conf_link = sheets_data["REWARD-LINK"]
    #     # Всегда пересоздаём колонку с нужным именем (автоочистка битых вариантов)
    #     df_reward = enrich_reward_with_contest_code(df_reward, df_link)
    #     sheets_data["REWARD"] = (df_reward, conf_reward)

    # 2. Добавление колонки AUTO_GENDER для листа EMPLOYEE
    if "EMPLOYEE" in sheets_data:
        df_employee, conf_employee = sheets_data["EMPLOYEE"]
        df_employee = add_auto_gender_column(df_employee, "EMPLOYEE")
        sheets_data["EMPLOYEE"] = (df_employee, conf_employee)

    # 3. Проверка длины полей для всех листов согласно FIELD_LENGTH_VALIDATIONS
    for sheet_name in FIELD_LENGTH_VALIDATIONS.keys():
        if sheet_name in sheets_data:
            df, conf = sheets_data[sheet_name]
            df = validate_field_lengths(df, sheet_name)
            sheets_data[sheet_name] = (df, conf)

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

    # 6. Проверка на дубли
    for sheet_name, (df, conf) in sheets_data.items():
        check_cfg = next((x for x in CHECK_DUPLICATES if x["sheet"] == sheet_name), None)
        if check_cfg:
            df = mark_duplicates(df, check_cfg["key"], sheet_name=sheet_name)
            sheets_data[sheet_name] = (df, conf)

    # 7. Формирование итогового Summary (build_summary_sheet)
    dfs = {k: v[0] for k, v in sheets_data.items()}
    df_summary = build_summary_sheet(
        dfs,
        params_summary=SUMMARY_SHEET,
        merge_fields=[f for f in MERGE_FIELDS if f.get("sheet_dst") == "SUMMARY"]
    )
    sheets_data[SUMMARY_SHEET["sheet"]] = (df_summary, SUMMARY_SHEET)
    
    # 7.1. Создание уникальных листов SUMMARY по ключевым кодам
    logging.info(LOG_MESSAGES.get("func_start", "[START] {func} {params}").format(
        func="create_unique_summary_sheets", params="(создание уникальных листов SUMMARY)"
    ))
    
    # SUMMARY_REWARD - по коду REWARD_CODE
    df_summary_reward = create_unique_summary_sheet(df_summary, "REWARD_CODE", "SUMMARY_REWARD")
    if not df_summary_reward.empty:
        sheets_data["SUMMARY_REWARD"] = (df_summary_reward, {
            "sheet": "SUMMARY_REWARD",
            "max_col_width": 80,
            "freeze": "B2",
            "col_width_mode": "AUTO",
            "min_col_width": 8
        })
    
    # SUMMARY_CONTEST - по коду CONTEST_CODE
    df_summary_contest = create_unique_summary_sheet(df_summary, "CONTEST_CODE", "SUMMARY_CONTEST")
    if not df_summary_contest.empty:
        sheets_data["SUMMARY_CONTEST"] = (df_summary_contest, {
            "sheet": "SUMMARY_CONTEST",
            "max_col_width": 80,
            "freeze": "B2",
            "col_width_mode": "AUTO",
            "min_col_width": 8
        })
    
    # SUMMARY_SCHEDULE - по коду TOURNAMENT_CODE
    df_summary_schedule = create_unique_summary_sheet(df_summary, "TOURNAMENT_CODE", "SUMMARY_SCHEDULE")
    if not df_summary_schedule.empty:
        sheets_data["SUMMARY_SCHEDULE"] = (df_summary_schedule, {
            "sheet": "SUMMARY_SCHEDULE",
            "max_col_width": 80,
            "freeze": "B2",
            "col_width_mode": "AUTO",
            "min_col_width": 8
        })
    
    logging.info(LOG_MESSAGES.get("func_end", "[END] {func} {params} (время: {time:.3f}s)").format(
        func="create_unique_summary_sheets", params="(создание уникальных листов SUMMARY)", time=0
    ))

    # 8. Запись в Excel
    output_excel = os.path.join(DIR_OUTPUT, get_output_filename())
    logging.info(LOG_MESSAGES["func_start"].format(func="write_to_excel", params=f"({output_excel})"))
    write_to_excel(sheets_data, output_excel)
    logging.info(LOG_MESSAGES["func_end"].format(func="write_to_excel", params=f"({output_excel})", time=0))

    time_elapsed = datetime.now() - start_time
    logging.info(LOG_MESSAGES["finish"].format(
        files=files_processed,
        rows_total=rows_total,
        time_elapsed=str(time_elapsed)
    ))
    logging.info(LOG_MESSAGES["summary"].format(summary="; ".join(summary)))
    logging.info(LOG_MESSAGES["excel_path"].format(path=output_excel))
    logging.info(LOG_MESSAGES["log_path"].format(path=log_file))


if __name__ == "__main__":
    main()
