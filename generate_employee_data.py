#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Генератор данных для файла EMPLOYEE.csv
Создает 5000 записей сотрудников с использованием данных из существующих файлов
"""

import os
import sys
import csv
import random
import logging
import time
from datetime import datetime
from typing import List, Dict, Tuple, Set
import json

# === ГЛОБАЛЬНЫЕ КОНСТАНТЫ ===

# Настройки генерации
TARGET_RECORDS = 5000
OUTPUT_FILENAME = 'employee_PROM_final_5000'
OUTPUT_EXTENSION = '.CSV'
CSV_SEPARATOR = ';'
CSV_ENCODING = 'utf-8'

# Директории
DIR_INPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "SPOD")
DIR_OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "OUT")
DIR_LOGS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "LOGS")

# Имена входных файлов (без расширения)
INPUT_FILES = {
    'ORG_UNIT': 'SVD_KB_DM_GAMIFICATION_ORG_UNIT_V20 2025_07_11 v1',
    'CONTEST_DATA': 'CONTEST-DATA (PROM) 2025-07-24 v4',
    'USER_ROLE': 'PROM_USER_ROLE 2025-07-21 v0',
    'REPORT': 'REPORT (PROM-KMKKSB) 2025-07-25 v6'
}

FILE_EXTENSION = '.CSV'

# Настройки КПК
KPK_EMPLOYEE_RATIO = 0.4  # 40% сотрудников в КПК
KPK_AVG_SIZE = 7  # Средний размер КПК
KPK_MANAGER_MIN = 3  # Минимум менеджеров в КПК
KPK_MANAGER_MAX = 10  # Максимум менеджеров в КПК
KPK_CODE_LENGTH = 8  # Длина кода КПК

# Настройки распределения
GENDER_MALE_WEIGHT = 60  # Вес мужского пола
GENDER_FEMALE_WEIGHT = 40  # Вес женского пола
UCH_CODE_SPECIAL_RATIO = 0.006  # Доля UCH_CODE=3 среди обычных сотрудников

# Настройки табельных номеров
PERSON_NUMBER_LENGTH = 20  # Общая длина табельного номера
PERSON_NUMBER_MIN_DIGITS = 4  # Минимум значащих цифр
PERSON_NUMBER_MAX_DIGITS = 10  # Максимум значащих цифр

# Коды полей
PRIORITY_TYPE_VALUE = "1"  # Всегда 1
UCH_CODE_NOT_PARTICIPANT = "0"  # Не участник
UCH_CODE_PARTICIPANT = "1"  # Участник (КМ)
UCH_CODE_MANAGER = "2"  # Руководитель
UCH_CODE_SPECIAL = "3"  # Особый статус
GENDER_MALE = 1
GENDER_FEMALE = 2
BUSINESS_BLOCK_KMKKSB = "KMKKSB"  # Блок для клиентских менеджеров
ROLE_CODE_KM = "KM_KKSB"  # Роль для клиентских менеджеров

# Структура полей EMPLOYEE
EMPLOYEE_FIELDS = [
    'PERSON_NUMBER', 'PERSON_NUMBER_ADD', 'SURNAME', 'FIRST_NAME', 'MIDDLE_NAME',
    'MANAGER_FULL_NAME', 'POSITION_NAME', 'TB_CODE', 'GOSB_CODE', 'BUSINESS_BLOCK',
    'PRIORITY_TYPE', 'KPK_CODE', 'KPK_NAME', 'ROLE_CODE', 'UCH_CODE', 'GENDER', 'ORG_UNIT_CODE'
]

# Сообщения логирования
LOG_MESSAGES = {
    # Общие сообщения
    'start': "=== СТАРТ ГЕНЕРАЦИИ ДАННЫХ EMPLOYEE ===",
    'finish': "=== ГЕНЕРАЦИЯ ЗАВЕРШЕНА УСПЕШНО ===",
    'error_finish': "=== ГЕНЕРАЦИЯ ЗАВЕРШЕНА С ОШИБКАМИ ===",
    'total_time': "Общее время выполнения: {time:.3f} сек",
    
    # Загрузка данных
    'loading_start': "Загрузка исходных данных...",
    'loading_time': "Время загрузки данных: {time:.3f} сек",
    'file_loaded': "Загружено {count} записей из {source}",
    'file_error': "Ошибка загрузки {source}: {error}",
    'fallback_data': "Использованы fallback данные для {source}",
    
    # Обработка бизнес-блоков
    'business_blocks_raw': "Найдено {count} сырых бизнес-блоков",
    'business_blocks_processed': "Обработано {count} бизнес-блоков: {blocks}",
    'business_blocks_json_error': "Ошибка парсинга JSON для блока: {block}",
    
    # Генерация КПК
    'kpk_generation_start': "Генерация структуры КПК...",
    'kpk_generation_time': "Время генерации КПК: {time:.3f} сек",
    'kpk_planned': "Планируется создать {count} КПК с общим количеством сотрудников {employees}",
    'kpk_created': "Создано {positions} позиций в {kpk_count} КПК",
    'kpk_details': "КПК #{kpk_num}: {managers} менеджеров, код {code}, название: {name}",
    
    # Генерация записей
    'records_generation_start': "Генерация {count} записей сотрудников...",
    'records_generation_time': "Время генерации записей: {time:.3f} сек",
    'records_progress': "Сгенерировано {current} из {total} записей ({percent:.1f}%)",
    'records_completed': "Генерация записей завершена",
    
    # Сохранение файла
    'save_start': "Сохранение файла...",
    'saving_time': "Время сохранения: {time:.3f} сек",
    'file_saved': "Файл сохранен: {path}",
    'file_save_error': "Ошибка сохранения файла: {error}",
    
    # Статистика
    'statistics_start': "=== СТАТИСТИКА СГЕНЕРИРОВАННЫХ ДАННЫХ ===",
    'total_records': "Общее количество записей: {count}",
    'gender_stats': "Распределение по полу: М={male} ({male_pct:.1f}%), Ж={female} ({female_pct:.1f}%)",
    'uch_code_header': "Распределение UCH_CODE:",
    'uch_code_item': "  {code} ({desc}): {count} ({pct:.1f}%)",
    'kpk_stats': "КПК статистика: {in_kpk} сотрудников ({in_kpk_pct:.1f}%) в {kpk_count} командах",
    'non_kpk_stats': "Вне КПК: {count} сотрудников",
    'business_block_header': "Распределение по бизнес-блокам:",
    'business_block_item': "  {block}: {count} ({pct:.1f}%)",
    'position_stats': "Должности: КМ={km_count}, Руководители={dir_count}, Прочие={other_count}",
    'uniqueness_check': "Проверка уникальности: номера={unique_numbers}/{total}, ФИО={unique_names}/{total}",
    'org_stats': "Организационные единицы: ТБ={tb_count}, ГОСБ={gosb_count}, ORG_UNIT={org_count}",
    'km_validation': "Проверка КМ в KMKKSB: {km_in_kmkksb}/{km_total}",
    'validation_success': "✅ Все клиентские менеджеры корректно размещены в блоке KMKKSB",
    'validation_warning': "⚠️  {count} клиентских менеджеров НЕ в блоке KMKKSB!",
    'uniqueness_warning_numbers': "⚠️  НАЙДЕНЫ ДУБЛИРУЮЩИЕСЯ ТАБЕЛЬНЫЕ НОМЕРА!",
    'uniqueness_warning_names': "⚠️  НАЙДЕНЫ ДУБЛИРУЮЩИЕСЯ ФИО!",
    
    # Debug сообщения
    'debug_person_number': "Генерация табельного номера: использован из REPORT={from_report}, номер={number}",
    'debug_kpk_employee': "Создан сотрудник КПК: индекс={index}, КПК={kpk_code}, руководитель={is_manager}",
    'debug_regular_employee': "Создан обычный сотрудник: индекс={index}, UCH_CODE={uch_code}",
    'debug_name_generation': "Генерация ФИО: попытка={attempt}, пол={gender}, результат={name}",
    'debug_org_unit': "Выбрано подразделение: ТБ={tb}, ГОСБ={gosb}, ORG_UNIT={org}",
    'debug_business_block_raw': "Сырой бизнес-блок: {block}",
    'debug_business_block_parsed': "Парсед бизнес-блок: {original} -> {parsed}"
}

# Справочники русских имен
RUSSIAN_SURNAMES_M = [
    "Иванов", "Петров", "Сидоров", "Смирнов", "Козлов", "Новиков", "Морозов", "Петухов", 
    "Волков", "Соловьёв", "Васильев", "Зайцев", "Павлов", "Семёнов", "Голубев", "Виноградов",
    "Богданов", "Воробьёв", "Фёдоров", "Михайлов", "Беляев", "Тарасов", "Белов", "Комаров",
    "Орлов", "Киселёв", "Макаров", "Андреев", "Ковалёв", "Ильин", "Гусев", "Титов",
    "Кузнецов", "Кудрявцев", "Баранов", "Куликов", "Алексеев", "Степанов", "Яковлев", "Сорокин",
    "Сергеев", "Романов", "Захаров", "Борисов", "Королёв", "Герасимов", "Пономарёв", "Григорьев",
    "Лазарев", "Медведев", "Ершов", "Никитин", "Соболев", "Рябов", "Поляков", "Цветков",
    "Данилов", "Жуков", "Фролов", "Журавлёв", "Николаев", "Крылов", "Максимов", "Сидоренко",
    "Осипов", "Белоусов", "Федотов", "Дорофеев", "Егоров", "Матвеев", "Бобров", "Дмитриев",
    "Калинин", "Анисимов", "Петрашев", "Антонов", "Тимофеев", "Никифоров", "Веселов", "Филиппов"
]

RUSSIAN_NAMES_M = [
    "Александр", "Дмитрий", "Максим", "Сергей", "Андрей", "Алексей", "Артём", "Илья", "Кирилл", "Михаил",
    "Никита", "Матвей", "Роман", "Егор", "Арсений", "Иван", "Денис", "Евгений", "Даниил", "Тимур",
    "Владислав", "Игорь", "Владимир", "Павел", "Руслан", "Марк", "Константин", "Тимофей", "Артур", "Антон",
    "Юрий", "Аркадий", "Георгий", "Николай", "Виктор", "Олег", "Валентин", "Анатолий", "Степан", "Вадим"
]

RUSSIAN_NAMES_F = [
    "Анна", "Елена", "Ирина", "Татьяна", "Наталья", "Ольга", "Юлия", "Светлана", "Екатерина", "Мария",
    "Александра", "Дарья", "Алина", "Ксения", "Анастасия", "Виктория", "Валентина", "Галина", "Нина", "Любовь",
    "Людмила", "Надежда", "Вера", "Полина", "Маргарита", "Евгения", "Лариса", "Тамара", "Зоя", "Лидия",
    "Антонина", "Марина", "Алла", "Клавдия", "Инна", "Раиса", "Римма", "Валерия", "Кристина", "Жанна"
]

RUSSIAN_PATRONYMICS_M = [
    "Александрович", "Дмитриевич", "Максимович", "Сергеевич", "Андреевич", "Алексеевич", "Артёмович", 
    "Ильич", "Кириллович", "Михайлович", "Никитич", "Матвеевич", "Романович", "Егорович", "Арсеньевич",
    "Иванович", "Денисович", "Евгеньевич", "Данилович", "Тимурович", "Владиславович", "Игоревич", 
    "Владимирович", "Павлович", "Русланович", "Маркович", "Константинович", "Тимофеевич", "Артурович", "Антонович"
]

# Генерируем женские варианты фамилий и отчеств
RUSSIAN_SURNAMES_F = [s[:-2] + "ова" if s.endswith("ов") else s[:-2] + "ева" if s.endswith("ев") else s + "а" for s in RUSSIAN_SURNAMES_M]
RUSSIAN_PATRONYMICS_F = [p[:-2] + "на" for p in RUSSIAN_PATRONYMICS_M]

# Справочники должностей
BANK_POSITIONS_KM = [
    "Клиентский менеджер по работе с ВИП-клиентами",
    "Клиентский менеджер по корпоративному бизнесу",
    "Клиентский менеджер по малому бизнесу",
    "Клиентский менеджер по работе с частными клиентами",
    "Клиентский менеджер розничного банкинга",
    "Старший клиентский менеджер",
    "Ведущий клиентский менеджер"
]

BANK_POSITIONS_MANAGERS = [
    "Исполнительный директор отделения",
    "Управляющий отделением", 
    "Заместитель управляющего отделением",
    "Начальник отдела"
]

BANK_POSITIONS_REGULAR = [
    "Ведущий специалист", "Главный специалист", "Старший специалист", 
    "Специалист", "Консультант", "Старший консультант",
    "Заместитель начальника отдела", "Заместитель исполнительного директора"
]

# Настройки прогресса
PROGRESS_STEP = 500  # Шаг для вывода прогресса

# UCH_CODE описания
UCH_CODE_DESCRIPTIONS = {
    UCH_CODE_NOT_PARTICIPANT: "не участник",
    UCH_CODE_PARTICIPANT: "участник (КМ)",
    UCH_CODE_MANAGER: "руководитель",
    UCH_CODE_SPECIAL: "особый статус"
}

# Fallback данные
FALLBACK_BUSINESS_BLOCKS = ["KMKKSB", "RSB1", "MNS", "SERVICEMEN", "KMFACTORING"]
FALLBACK_ROLE_CODES = ["MANAGER", "SPECIALIST", "CONSULTANT", "DIRECTOR"]

# === НАСТРОЙКА ЛОГИРОВАНИЯ ===
def setup_logging() -> str:
    """Настройка системы логирования с разделением на INFO и DEBUG"""
    log_filename = f'generate_employee_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
    log_path = os.path.join(DIR_LOGS, log_filename)
    
    # Создаем директорию если не существует
    os.makedirs(DIR_LOGS, exist_ok=True)
    
    # Настройка форматирования
    formatter = logging.Formatter("%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s", 
                                  datefmt="%Y-%m-%d %H:%M:%S")
    
    # Настройка логгера
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Очистка существующих обработчиков
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Обработчик для файла (все сообщения)
    file_handler = logging.FileHandler(log_path, encoding=CSV_ENCODING)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Обработчик для консоли (только INFO и выше)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return log_path

# === ОСНОВНОЙ КЛАСС ГЕНЕРАТОРА ===

def find_file_case_insensitive(directory, base_name, extensions):
    """
    Ищет файл в каталоге без учета регистра имени файла и расширения.
    
    Args:
        directory (str): Каталог для поиска
        base_name (str): Базовое имя файла (без расширения)
        extensions (list): Список возможных расширений (например, ['.csv', '.CSV'])
    
    Returns:
        str or None: Полный путь к найденному файла или None если файл не найден
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



class EmployeeGenerator:
    def __init__(self):
        self.org_units: List[Dict] = []
        self.business_blocks: List[str] = []
        self.role_codes: List[str] = []
        self.existing_person_numbers: List[str] = []
        self.used_person_numbers: Set[str] = set()
        self.used_full_names: Set[str] = set()
        self.kpk_data: List[Dict] = []
        
        # Счетчики для статистики
        self.stats = {
            'files_loaded': 0,
            'fallback_used': 0,
            'kpk_created': 0,
            'records_generated': 0
        }
        
        # Времена выполнения
        self.times = {
            'total_start': 0,
            'loading': 0,
            'kpk_generation': 0,
            'records_generation': 0,
            'saving': 0
        }
        
    def _time_operation(self, operation_name: str, func, *args, **kwargs):
        """Измеряет время выполнения операции"""
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        self.times[operation_name] = elapsed_time
        logging.info(LOG_MESSAGES[f'{operation_name}_time'].format(time=elapsed_time))
        return result
        
    def load_csv_file(self, file_key: str, required_columns: List[str] = None) -> List[Dict]:
        """Загружает CSV файл и возвращает список словарей"""
        filename = INPUT_FILES[file_key]
        file_path = find_file_case_insensitive(DIR_INPUT, filename, [".csv", ".CSV"])
        
        # Проверяем, найден ли файл
        if file_path is None:
            logging.error(f"Файл не найден: {filename} в каталоге {DIR_INPUT}")
            return []
        
        try:
            with open(file_path, 'r', encoding=CSV_ENCODING) as f:
                reader = csv.DictReader(f, delimiter=CSV_SEPARATOR)
                data = list(reader)
                
            logging.info(LOG_MESSAGES['file_loaded'].format(count=len(data), source=file_key))
            logging.debug(f"Загружены колонки из {file_key}: {list(data[0].keys()) if data else 'нет данных'}")
            
            self.stats['files_loaded'] += 1
            return data
            
        except Exception as e:
            logging.error(LOG_MESSAGES['file_error'].format(source=file_key, error=e))
            return []
    
    def process_business_blocks(self, contest_data: List[Dict]) -> List[str]:
        """Обрабатывает бизнес-блоки из CONTEST-DATA"""
        if not contest_data:
            return FALLBACK_BUSINESS_BLOCKS
            
        raw_blocks = []
        for row in contest_data:
            if 'BUSINESS_BLOCK' in row and row['BUSINESS_BLOCK']:
                raw_blocks.append(row['BUSINESS_BLOCK'])
        
        raw_blocks = list(set(raw_blocks))  # Убираем дубли
        logging.debug(LOG_MESSAGES['business_blocks_raw'].format(count=len(raw_blocks)))
        
        processed_blocks = []
        for block in raw_blocks:
            logging.debug(LOG_MESSAGES['debug_business_block_raw'].format(block=block))
            
            if block.startswith('['):
                try:
                    parsed = json.loads(block.replace('"""', '"'))
                    if parsed and len(parsed) > 0 and parsed[0]:
                        processed_blocks.append(parsed[0])
                        logging.debug(LOG_MESSAGES['debug_business_block_parsed'].format(
                            original=block, parsed=parsed[0]))
                except Exception as e:
                    logging.debug(LOG_MESSAGES['business_blocks_json_error'].format(block=block))
            else:
                if block.strip():
                    processed_blocks.append(block.strip())
        
        # Убираем дубли и пустые значения
        processed_blocks = list(set([b for b in processed_blocks if b and b.strip()]))
        
        if not processed_blocks:
            processed_blocks = FALLBACK_BUSINESS_BLOCKS
            logging.info(LOG_MESSAGES['fallback_data'].format(source='BUSINESS_BLOCKS'))
            self.stats['fallback_used'] += 1
            
        logging.info(LOG_MESSAGES['business_blocks_processed'].format(
            count=len(processed_blocks), blocks=processed_blocks))
        
        return processed_blocks
    
    def load_source_data(self) -> bool:
        """Загружает данные из источников"""
        return self._time_operation('loading', self._load_source_data_impl)
    
    def _load_source_data_impl(self) -> bool:
        """Реализация загрузки данных из источников"""
        logging.info(LOG_MESSAGES['loading_start'])
        
        # Загрузка ORG_UNIT_V20
        org_unit_data = self.load_csv_file('ORG_UNIT')
        if org_unit_data:
            self.org_units = org_unit_data
        else:
            logging.error("Критическая ошибка: не удалось загрузить ORG_UNIT_V20")
            return False
            
        # Загрузка CONTEST-DATA для BUSINESS_BLOCK
        contest_data = self.load_csv_file('CONTEST_DATA')
        self.business_blocks = self.process_business_blocks(contest_data)
            
        # Загрузка USER_ROLE для ROLE_CODE
        role_data = self.load_csv_file('USER_ROLE')
        if role_data:
            self.role_codes = list(set([row.get('ROLE_CODE', '') for row in role_data if row.get('ROLE_CODE')]))
            logging.info(LOG_MESSAGES['file_loaded'].format(count=len(self.role_codes), source='ROLE_CODES'))
        else:
            self.role_codes = FALLBACK_ROLE_CODES
            logging.info(LOG_MESSAGES['fallback_data'].format(source='ROLE_CODES'))
            self.stats['fallback_used'] += 1
            
        # Загрузка REPORT для существующих PERSON_NUMBER
        report_data = self.load_csv_file('REPORT')
        if report_data:
            person_numbers = []
            for row in report_data:
                if 'MANAGER_PERSON_NUMBER' in row and row['MANAGER_PERSON_NUMBER']:
                    person_numbers.append(row['MANAGER_PERSON_NUMBER'])
            
            self.existing_person_numbers = list(set(person_numbers))
            logging.info(LOG_MESSAGES['file_loaded'].format(
                count=len(self.existing_person_numbers), source='PERSON_NUMBERS'))
        else:
            self.existing_person_numbers = []
            logging.info(LOG_MESSAGES['fallback_data'].format(source='PERSON_NUMBERS'))
            self.stats['fallback_used'] += 1
            
        return True
        
    def generate_person_number(self) -> str:
        """Генерирует уникальный табельный номер"""
        # Сначала используем существующие номера из REPORT
        if self.existing_person_numbers:
            available = [num for num in self.existing_person_numbers if num not in self.used_person_numbers]
            if available:
                number = random.choice(available)
                self.used_person_numbers.add(number)
                padded = number.zfill(PERSON_NUMBER_LENGTH)
                logging.debug(LOG_MESSAGES['debug_person_number'].format(from_report=True, number=padded))
                return padded
                
        # Генерируем новый номер
        while True:
            significant_digits = random.randint(PERSON_NUMBER_MIN_DIGITS, PERSON_NUMBER_MAX_DIGITS)
            number = str(random.randint(10**(significant_digits-1), 10**significant_digits - 1))
            padded_number = number.zfill(PERSON_NUMBER_LENGTH)
            
            if padded_number not in self.used_person_numbers:
                self.used_person_numbers.add(padded_number)
                logging.debug(LOG_MESSAGES['debug_person_number'].format(from_report=False, number=padded_number))
                return padded_number
                
    def generate_full_name(self, gender: int, max_attempts: int = 1000) -> Tuple[str, str, str, str]:
        """Генерирует уникальное ФИО"""
        for attempt in range(max_attempts):
            if gender == GENDER_MALE:
                surname = random.choice(RUSSIAN_SURNAMES_M)
                first_name = random.choice(RUSSIAN_NAMES_M)
                middle_name = random.choice(RUSSIAN_PATRONYMICS_M)
            else:
                surname = random.choice(RUSSIAN_SURNAMES_F)
                first_name = random.choice(RUSSIAN_NAMES_F)
                middle_name = random.choice(RUSSIAN_PATRONYMICS_F)
                
            full_name = f"{surname} {first_name} {middle_name}"
            
            if full_name not in self.used_full_names:
                self.used_full_names.add(full_name)
                logging.debug(LOG_MESSAGES['debug_name_generation'].format(
                    attempt=attempt+1, gender=gender, name=full_name))
                return surname, first_name, middle_name, full_name
                
        # Если не удалось создать уникальное имя, добавляем номер
        base_name = f"{surname} {first_name} {middle_name}"
        counter = 1
        while True:
            full_name = f"{base_name} #{counter}"
            if full_name not in self.used_full_names:
                self.used_full_names.add(full_name)
                return surname, first_name, f"{middle_name} #{counter}", full_name
            counter += 1
                
    def get_random_org_unit(self) -> Dict[str, str]:
        """Возвращает случайное организационное подразделение"""
        if self.org_units:
            org_unit = random.choice(self.org_units)
            result = {
                'TB_CODE': org_unit.get('TB_CODE', ''),
                'GOSB_CODE': org_unit.get('GOSB_CODE', ''),
                'ORG_UNIT_CODE': org_unit.get('ORG_UNIT_CODE', '')
            }
            logging.debug(LOG_MESSAGES['debug_org_unit'].format(
                tb=result['TB_CODE'], gosb=result['GOSB_CODE'], org=result['ORG_UNIT_CODE']))
            return result
        else:
            # Fallback если данные не загружены
            return {
                'TB_CODE': f"TB{random.randint(1000, 9999)}",
                'GOSB_CODE': f"GOSB{random.randint(100, 999)}",
                'ORG_UNIT_CODE': f"ORG{random.randint(10000, 99999)}"
            }
            
    def generate_kpk_structure(self):
        """Генерирует структуру КПК с руководителями и менеджерами"""
        return self._time_operation('kpk_generation', self._generate_kpk_structure_impl)
    
    def _generate_kpk_structure_impl(self):
        """Реализация генерации структуры КПК с руководителями и менеджерами"""
        logging.info(LOG_MESSAGES['kpk_generation_start'])
        
        kpk_employees_count = int(TARGET_RECORDS * KPK_EMPLOYEE_RATIO)
        kpk_count = kpk_employees_count // KPK_AVG_SIZE
        
        logging.info(LOG_MESSAGES['kpk_planned'].format(count=kpk_count, employees=kpk_employees_count))
        
        kpk_types = ['Корпоративные клиенты', 'ВИП клиенты', 'Малый бизнес', 'Розничные клиенты']
        
        for i in range(kpk_count):
            kpk_code = f"{random.randint(10**(KPK_CODE_LENGTH-1), 10**KPK_CODE_LENGTH - 1)}"
            kpk_name = f"КПК №{i+1:03d} - {random.choice(kpk_types)}"
            
            managers_count = random.randint(KPK_MANAGER_MIN, KPK_MANAGER_MAX)
            
            logging.debug(LOG_MESSAGES['kpk_details'].format(
                kpk_num=i+1, managers=managers_count, code=kpk_code, name=kpk_name))
            
            # Добавляем руководителя
            self.kpk_data.append({
                'is_manager': True,
                'kpk_code': kpk_code,
                'kpk_name': kpk_name
            })
            
            # Добавляем менеджеров
            for _ in range(managers_count):
                self.kpk_data.append({
                    'is_manager': False,
                    'kpk_code': kpk_code,
                    'kpk_name': kpk_name
                })
        
        self.stats['kpk_created'] = len(self.kpk_data)
        logging.info(LOG_MESSAGES['kpk_created'].format(positions=len(self.kpk_data), kpk_count=kpk_count))
        
    def generate_employee_record(self, index: int) -> Dict[str, str]:
        """Генерирует одну запись сотрудника"""
        
        # Определяем, будет ли сотрудник в КПК
        is_in_kpk = index < len(self.kpk_data)
        
        # Генерируем базовые данные
        gender = random.choices([GENDER_MALE, GENDER_FEMALE], 
                               weights=[GENDER_MALE_WEIGHT, GENDER_FEMALE_WEIGHT])[0]
        person_number = self.generate_person_number()
        surname, first_name, middle_name, full_name = self.generate_full_name(gender)
        org_unit = self.get_random_org_unit()
        
        if is_in_kpk:
            # Сотрудник КПК
            kpk_info = self.kpk_data[index]
            
            logging.debug(LOG_MESSAGES['debug_kpk_employee'].format(
                index=index, kpk_code=kpk_info['kpk_code'], is_manager=kpk_info['is_manager']))
            
            if kpk_info['is_manager']:
                # Руководитель КПК
                position_name = random.choice(BANK_POSITIONS_MANAGERS)
                business_block = random.choice(self.business_blocks)
                role_code = random.choice(self.role_codes)
                uch_code = UCH_CODE_MANAGER
            else:
                # Клиентский менеджер
                position_name = random.choice(BANK_POSITIONS_KM)
                business_block = BUSINESS_BLOCK_KMKKSB
                role_code = ROLE_CODE_KM
                uch_code = UCH_CODE_PARTICIPANT
                
            kpk_code = kpk_info['kpk_code']
            kpk_name = kpk_info['kpk_name']
            
        else:
            # Обычный сотрудник (не в КПК)
            position_name = random.choice(BANK_POSITIONS_REGULAR)
            business_block = random.choice(self.business_blocks)
            role_code = random.choice(self.role_codes)
            kpk_code = ""
            kpk_name = ""
            
            # UCH_CODE для обычных сотрудников
            if random.random() < UCH_CODE_SPECIAL_RATIO:
                uch_code = UCH_CODE_SPECIAL
            else:
                uch_code = UCH_CODE_NOT_PARTICIPANT
                
            logging.debug(LOG_MESSAGES['debug_regular_employee'].format(index=index, uch_code=uch_code))
        
        return {
            'PERSON_NUMBER': person_number,
            'PERSON_NUMBER_ADD': person_number,
            'SURNAME': surname,
            'FIRST_NAME': first_name,
            'MIDDLE_NAME': middle_name,
            'MANAGER_FULL_NAME': full_name,
            'POSITION_NAME': position_name,
            'TB_CODE': org_unit['TB_CODE'],
            'GOSB_CODE': org_unit['GOSB_CODE'],
            'BUSINESS_BLOCK': business_block,
            'PRIORITY_TYPE': PRIORITY_TYPE_VALUE,
            'KPK_CODE': kpk_code,
            'KPK_NAME': kpk_name,
            'ROLE_CODE': role_code,
            'UCH_CODE': uch_code,
            'GENDER': str(gender),
            'ORG_UNIT_CODE': org_unit['ORG_UNIT_CODE']
        }
        
    def generate_all_records(self) -> List[Dict[str, str]]:
        """Генерирует все записи сотрудников"""
        return self._time_operation('records_generation', self._generate_all_records_impl)
    
    def _generate_all_records_impl(self) -> List[Dict[str, str]]:
        """Реализация генерации всех записей сотрудников"""
        logging.info(LOG_MESSAGES['records_generation_start'].format(count=TARGET_RECORDS))
        
        records = []
        for i in range(TARGET_RECORDS):
            if (i + 1) % PROGRESS_STEP == 0:
                percent = (i + 1) / TARGET_RECORDS * 100
                logging.info(LOG_MESSAGES['records_progress'].format(
                    current=i+1, total=TARGET_RECORDS, percent=percent))
                
            record = self.generate_employee_record(i)
            records.append(record)
            
        self.stats['records_generated'] = len(records)
        logging.info(LOG_MESSAGES['records_completed'])
        
        return records
        
    def save_to_csv(self, records: List[Dict[str, str]]):
        """Сохраняет записи в CSV файл"""
        return self._time_operation('saving', self._save_to_csv_impl, records)
    
    def _save_to_csv_impl(self, records: List[Dict[str, str]]):
        """Реализация сохранения записей в CSV файл"""
        logging.info(LOG_MESSAGES['save_start'])
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_filename_with_timestamp = f"{OUTPUT_FILENAME}_{timestamp}{OUTPUT_EXTENSION}"
        output_path = os.path.join(DIR_OUTPUT, output_filename_with_timestamp)
        
        try:
            os.makedirs(DIR_OUTPUT, exist_ok=True)
            
            with open(output_path, 'w', newline='', encoding=CSV_ENCODING) as f:
                writer = csv.DictWriter(f, fieldnames=EMPLOYEE_FIELDS, delimiter=CSV_SEPARATOR)
                writer.writeheader()
                writer.writerows(records)
                
            logging.info(LOG_MESSAGES['file_saved'].format(path=output_path))
            
        except Exception as e:
            logging.error(LOG_MESSAGES['file_save_error'].format(error=e))
            raise
            
    def print_statistics(self, records: List[Dict[str, str]]):
        """Выводит статистику по сгенерированным данным"""
        logging.info(LOG_MESSAGES['statistics_start'])
        logging.info(LOG_MESSAGES['total_records'].format(count=len(records)))
        
        # Статистика по полу
        gender_stats = {}
        for record in records:
            gender = record['GENDER']
            gender_stats[gender] = gender_stats.get(gender, 0) + 1
            
        male_count = gender_stats.get(str(GENDER_MALE), 0)
        female_count = gender_stats.get(str(GENDER_FEMALE), 0)
        male_pct = male_count / len(records) * 100
        female_pct = female_count / len(records) * 100
        
        logging.info(LOG_MESSAGES['gender_stats'].format(
            male=male_count, male_pct=male_pct, female=female_count, female_pct=female_pct))
        
        # Статистика по UCH_CODE
        uch_stats = {}
        for record in records:
            uch = record['UCH_CODE']
            uch_stats[uch] = uch_stats.get(uch, 0) + 1
            
        logging.info(LOG_MESSAGES['uch_code_header'])
        for uch_code in sorted(uch_stats.keys()):
            count = uch_stats[uch_code]
            pct = count / len(records) * 100
            desc = UCH_CODE_DESCRIPTIONS.get(uch_code, 'неизвестно')
            logging.info(LOG_MESSAGES['uch_code_item'].format(code=uch_code, desc=desc, count=count, pct=pct))
        
        # Статистика по КПК
        kpk_employees = [r for r in records if r['KPK_CODE']]
        unique_kpk = set([r['KPK_CODE'] for r in kpk_employees])
        kpk_count = len(kpk_employees)
        kpk_pct = kpk_count / len(records) * 100
        
        logging.info(LOG_MESSAGES['kpk_stats'].format(
            in_kpk=kpk_count, in_kpk_pct=kpk_pct, kpk_count=len(unique_kpk)))
        logging.info(LOG_MESSAGES['non_kpk_stats'].format(count=len(records) - kpk_count))
        
        # Статистика по бизнес-блокам
        business_stats = {}
        for record in records:
            block = record['BUSINESS_BLOCK']
            business_stats[block] = business_stats.get(block, 0) + 1
            
        logging.info(LOG_MESSAGES['business_block_header'])
        for block in sorted(business_stats.keys(), key=lambda x: business_stats[x], reverse=True):
            count = business_stats[block]
            pct = count / len(records) * 100
            logging.info(LOG_MESSAGES['business_block_item'].format(block=block, count=count, pct=pct))
        
        # Статистика по должностям
        km_count = len([r for r in records if 'клиентский менеджер' in r['POSITION_NAME'].lower()])
        director_count = len([r for r in records if any(word in r['POSITION_NAME'].lower() 
                                                       for word in ['директор', 'управляющий', 'начальник'])])
        other_count = len(records) - km_count - director_count
        
        logging.info(LOG_MESSAGES['position_stats'].format(
            km_count=km_count, dir_count=director_count, other_count=other_count))
        
        # Уникальность
        unique_numbers = set([r['PERSON_NUMBER'] for r in records])
        unique_names = set([r['MANAGER_FULL_NAME'] for r in records])
        
        logging.info(LOG_MESSAGES['uniqueness_check'].format(
            unique_numbers=len(unique_numbers), unique_names=len(unique_names), total=len(records)))
        
        # Предупреждения
        if len(unique_numbers) != len(records):
            logging.warning(LOG_MESSAGES['uniqueness_warning_numbers'])
        if len(unique_names) != len(records):
            logging.warning(LOG_MESSAGES['uniqueness_warning_names'])
            
        # Статистика по организационным единицам
        unique_tb = set([r['TB_CODE'] for r in records])
        unique_gosb = set([r['GOSB_CODE'] for r in records])
        unique_org = set([r['ORG_UNIT_CODE'] for r in records])
        
        logging.info(LOG_MESSAGES['org_stats'].format(
            tb_count=len(unique_tb), gosb_count=len(unique_gosb), org_count=len(unique_org)))
        
        # Проверка КМ в KMKKSB
        km_in_kmkksb = len([r for r in records if 'клиентский менеджер' in r['POSITION_NAME'].lower() 
                           and r['BUSINESS_BLOCK'] == BUSINESS_BLOCK_KMKKSB])
        
        logging.info(LOG_MESSAGES['km_validation'].format(km_in_kmkksb=km_in_kmkksb, km_total=km_count))
        
        if km_in_kmkksb == km_count:
            logging.info(LOG_MESSAGES['validation_success'])
        else:
            logging.warning(LOG_MESSAGES['validation_warning'].format(count=km_count - km_in_kmkksb))

def main():
    """Основная функция"""
    # Настройка логирования
    log_path = setup_logging()
    
    # Запуск
    start_time = time.time()
    logging.info(LOG_MESSAGES['start'])
    
    # Создаем директории
    os.makedirs(DIR_OUTPUT, exist_ok=True)
    os.makedirs(DIR_LOGS, exist_ok=True)
    
    try:
        generator = EmployeeGenerator()
        generator.times['total_start'] = start_time
        
        # Загружаем исходные данные
        if not generator.load_source_data():
            logging.error("Критическая ошибка загрузки данных")
            return False
            
        # Генерируем структуру КПК
        generator.generate_kpk_structure()
        
        # Генерируем все записи
        records = generator.generate_all_records()
        
        # Сохраняем в файл
        generator.save_to_csv(records)
        
        # Выводим статистику
        generator.print_statistics(records)
        
        # Общее время выполнения
        total_time = time.time() - start_time
        logging.info(LOG_MESSAGES['total_time'].format(time=total_time))
        
        logging.info(LOG_MESSAGES['finish'])
        return True
        
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        logging.info(LOG_MESSAGES['error_finish'])
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 