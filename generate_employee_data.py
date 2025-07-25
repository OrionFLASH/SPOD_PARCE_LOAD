#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Генератор данных для файла EMPLOYEE.csv
Создает 5000 записей сотрудников с использованием данных из существующих файлов
"""

import os
import sys
import pandas as pd
import random
import logging
from datetime import datetime
from typing import List, Dict, Tuple
import re

# === Настройки ===
DIR_INPUT = r'/Users/orionflash/Desktop/MyProject/SPOD_PROM/SPOD'
DIR_OUTPUT = r'/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT'
DIR_LOGS = r'/Users/orionflash/Desktop/MyProject/SPOD_PROM/LOGS'

TARGET_RECORDS = 5000
OUTPUT_FILENAME = 'employee_PROM_final_5000.CSV'

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(DIR_LOGS, f'generate_employee_{datetime.now().strftime("%Y%m%d_%H%M")}.log'), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

# === Структура полей EMPLOYEE ===
EMPLOYEE_FIELDS = [
    'PERSON_NUMBER',      # Табельный номер (PK)
    'PERSON_NUMBER_ADD',  # Табельный номер дополнительный  
    'SURNAME',            # Фамилия сотрудника
    'FIRST_NAME',         # Имя сотрудника
    'MIDDLE_NAME',        # Отчество сотрудника
    'MANAGER_FULL_NAME',  # ФИО сотрудника
    'POSITION_NAME',      # Наименование должности
    'TB_CODE',            # Код тербанка
    'GOSB_CODE',          # Код ГОСБа
    'BUSINESS_BLOCK',     # Бизнес блок
    'PRIORITY_TYPE',      # Тип приоритета (всегда 1)
    'KPK_CODE',           # Код КПК
    'KPK_NAME',           # Название КПК
    'ROLE_CODE',          # Функциональная роль
    'UCH_CODE',           # Признак участника
    'GENDER',             # Пол (1-муж, 2-жен)
    'ORG_UNIT_CODE'       # Код подразделения
]

# === Справочники данных ===
BANK_POSITIONS = [
    "Клиентский менеджер по работе с ВИП-клиентами",
    "Клиентский менеджер по корпоративному бизнесу", 
    "Клиентский менеджер по малому бизнесу",
    "Клиентский менеджер по работе с частными клиентами",
    "Клиентский менеджер розничного банкинга",
    "Старший клиентский менеджер",
    "Ведущий клиентский менеджер",
    "Исполнительный директор регионального банка",
    "Исполнительный директор территориального банка", 
    "Исполнительный директор отделения",
    "Заместитель исполнительного директора",
    "Управляющий отделением",
    "Заместитель управляющего отделением",
    "Начальник отдела",
    "Заместитель начальника отдела",
    "Ведущий специалист",
    "Главный специалист",
    "Старший специалист",
    "Специалист",
    "Консультант",
    "Старший консультант"
]

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

RUSSIAN_SURNAMES_F = [s[:-2] + "ова" if s.endswith("ов") else s[:-2] + "ева" if s.endswith("ев") else s + "а" for s in RUSSIAN_SURNAMES_M]

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

RUSSIAN_PATRONYMICS_F = [p[:-2] + "на" for p in RUSSIAN_PATRONYMICS_M]

class EmployeeGenerator:
    def __init__(self):
        self.org_units = None
        self.business_blocks = None
        self.role_codes = None
        self.existing_person_numbers = None
        self.used_person_numbers = set()
        self.used_full_names = set()
        self.kpk_data = []  # Список КПК с их сотрудниками
        
    def load_source_data(self):
        """Загружает данные из источников"""
        logging.info("Загрузка исходных данных...")
        
        # Загрузка ORG_UNIT_V20
        org_unit_path = os.path.join(DIR_INPUT, "SVD_KB_DM_GAMIFICATION_ORG_UNIT_V20 2025_07_11 v1.CSV")
        try:
            self.org_units = pd.read_csv(org_unit_path, sep=";", dtype=str, keep_default_na=False)
            logging.info(f"Загружено {len(self.org_units)} записей из ORG_UNIT_V20")
        except Exception as e:
            logging.error(f"Ошибка загрузки ORG_UNIT_V20: {e}")
            return False
            
        # Загрузка CONTEST-DATA для BUSINESS_BLOCK
        contest_path = os.path.join(DIR_INPUT, "CONTEST-DATA (PROM) 2025-07-24 v4.CSV")
        try:
            contest_data = pd.read_csv(contest_path, sep=";", dtype=str, keep_default_na=False)
            raw_blocks = contest_data['BUSINESS_BLOCK'].dropna().unique().tolist()
            
            # Обрабатываем JSON строки и извлекаем значения
            self.business_blocks = []
            for block in raw_blocks:
                if block.startswith('['):
                    try:
                        import json
                        parsed = json.loads(block.replace('"""', '"'))
                        if parsed and len(parsed) > 0:
                            self.business_blocks.append(parsed[0])
                    except:
                        pass
                else:
                    self.business_blocks.append(block)
            
            # Убираем дубли и пустые значения
            self.business_blocks = list(set([b for b in self.business_blocks if b and b.strip()]))
            
            if not self.business_blocks:
                self.business_blocks = ["KMKKSB", "RSB1", "MNS", "SERVICEMEN", "KMFACTORING"]
                
            logging.info(f"Загружено {len(self.business_blocks)} бизнес-блоков: {self.business_blocks}")
        except Exception as e:
            logging.error(f"Ошибка загрузки CONTEST-DATA: {e}")
            self.business_blocks = ["KMKKSB", "RSB1", "MNS", "SERVICEMEN", "KMFACTORING"]
            
        # Загрузка USER_ROLE для ROLE_CODE
        role_path = os.path.join(DIR_INPUT, "PROM_USER_ROLE 2025-07-21 v0.CSV") 
        try:
            role_data = pd.read_csv(role_path, sep=";", dtype=str, keep_default_na=False)
            self.role_codes = role_data['ROLE_CODE'].dropna().unique().tolist()
            logging.info(f"Загружено {len(self.role_codes)} ролей")
        except Exception as e:
            logging.error(f"Ошибка загрузки USER_ROLE: {e}")
            self.role_codes = ["MANAGER", "SPECIALIST", "CONSULTANT", "DIRECTOR"]
            
        # Загрузка REPORT для существующих PERSON_NUMBER
        report_path = os.path.join(DIR_INPUT, "REPORT (PROM-KMKKSB) 2025-07-25 v6.csv")
        try:
            report_data = pd.read_csv(report_path, sep=";", dtype=str, keep_default_na=False)
            if 'MANAGER_PERSON_NUMBER' in report_data.columns:
                self.existing_person_numbers = report_data['MANAGER_PERSON_NUMBER'].dropna().unique().tolist()
                logging.info(f"Загружено {len(self.existing_person_numbers)} табельных номеров из REPORT")
            else:
                self.existing_person_numbers = []
        except Exception as e:
            logging.error(f"Ошибка загрузки REPORT: {e}")
            self.existing_person_numbers = []
            
        return True
        
    def generate_person_number(self) -> str:
        """Генерирует уникальный табельный номер"""
        # Сначала используем существующие номера из REPORT
        if self.existing_person_numbers:
            available = [num for num in self.existing_person_numbers if num not in self.used_person_numbers]
            if available:
                number = random.choice(available)
                self.used_person_numbers.add(number)
                return number.zfill(20)  # Дополняем до 20 символов нулями
                
        # Генерируем новый номер
        while True:
            # 4-10 значащих цифр в конце
            significant_digits = random.randint(4, 10)
            number = str(random.randint(10**(significant_digits-1), 10**significant_digits - 1))
            # Дополняем нулями до 20 символов
            padded_number = number.zfill(20)
            
            if padded_number not in self.used_person_numbers:
                self.used_person_numbers.add(padded_number)
                return padded_number
                
    def generate_full_name(self, gender: int) -> Tuple[str, str, str, str]:
        """Генерирует уникальное ФИО"""
        while True:
            if gender == 1:  # Мужской
                surname = random.choice(RUSSIAN_SURNAMES_M)
                first_name = random.choice(RUSSIAN_NAMES_M)
                middle_name = random.choice(RUSSIAN_PATRONYMICS_M)
            else:  # Женский
                surname = random.choice(RUSSIAN_SURNAMES_F)
                first_name = random.choice(RUSSIAN_NAMES_F)
                middle_name = random.choice(RUSSIAN_PATRONYMICS_F)
                
            full_name = f"{surname} {first_name} {middle_name}"
            
            if full_name not in self.used_full_names:
                self.used_full_names.add(full_name)
                return surname, first_name, middle_name, full_name
                
    def get_random_org_unit(self) -> Dict[str, str]:
        """Возвращает случайное организационное подразделение"""
        if self.org_units is not None and len(self.org_units) > 0:
            org_unit = self.org_units.sample(1).iloc[0]
            return {
                'TB_CODE': org_unit.get('TB_CODE', ''),
                'GOSB_CODE': org_unit.get('GOSB_CODE', ''),
                'ORG_UNIT_CODE': org_unit.get('ORG_UNIT_CODE', '')
            }
        else:
            # Fallback если данные не загружены
            return {
                'TB_CODE': f"TB{random.randint(1000, 9999)}",
                'GOSB_CODE': f"GOSB{random.randint(100, 999)}",
                'ORG_UNIT_CODE': f"ORG{random.randint(10000, 99999)}"
            }
            
    def generate_kpk_structure(self):
        """Генерирует структуру КПК с руководителями и менеджерами"""
        logging.info("Генерация структуры КПК...")
        
        # Примерно 40% сотрудников будут в КПК (клиентские менеджеры + руководители)
        kpk_employees_count = int(TARGET_RECORDS * 0.4)
        
        # Количество КПК (1 руководитель + 3-10 менеджеров = 4-11 человек в среднем 7)
        avg_kpk_size = 7
        kpk_count = kpk_employees_count // avg_kpk_size
        
        logging.info(f"Планируется создать {kpk_count} КПК с общим количеством сотрудников {kpk_employees_count}")
        
        for i in range(kpk_count):
            kpk_code = f"{random.randint(10000000, 99999999)}"  # 8-значный код
            kpk_name = f"КПК №{i+1:03d} - {random.choice(['Корпоративные клиенты', 'ВИП клиенты', 'Малый бизнес', 'Розничные клиенты'])}"
            
            # 1 руководитель + 3-10 менеджеров
            managers_count = random.randint(3, 10)
            total_in_kpk = 1 + managers_count
            
            kpk_employees = []
            
            # Добавляем руководителя
            kpk_employees.append({
                'is_manager': True,
                'kpk_code': kpk_code,
                'kpk_name': kpk_name
            })
            
            # Добавляем менеджеров
            for _ in range(managers_count):
                kpk_employees.append({
                    'is_manager': False,
                    'kpk_code': kpk_code,
                    'kpk_name': kpk_name
                })
                
            self.kpk_data.extend(kpk_employees)
            
        logging.info(f"Создано {len(self.kpk_data)} позиций в КПК")
        
    def generate_employee_record(self, index: int) -> Dict[str, str]:
        """Генерирует одну запись сотрудника"""
        
        # Определяем, будет ли сотрудник в КПК
        is_in_kpk = index < len(self.kpk_data)
        
        # Генерируем базовые данные
        gender = random.choices([1, 2], weights=[60, 40])[0]  # 60% мужчин, 40% женщин
        person_number = self.generate_person_number()
        surname, first_name, middle_name, full_name = self.generate_full_name(gender)
        org_unit = self.get_random_org_unit()
        
        if is_in_kpk:
            # Сотрудник КПК
            kpk_info = self.kpk_data[index]
            
            if kpk_info['is_manager']:
                # Руководитель КПК
                position_name = random.choice([
                    "Исполнительный директор отделения",
                    "Управляющий отделением", 
                    "Заместитель управляющего отделением",
                    "Начальник отдела"
                ])
                business_block = random.choice(self.business_blocks)
                role_code = random.choice(self.role_codes)
                uch_code = "2"  # Руководитель
            else:
                # Клиентский менеджер
                position_name = random.choice([
                    "Клиентский менеджер по работе с ВИП-клиентами",
                    "Клиентский менеджер по корпоративному бизнесу",
                    "Клиентский менеджер по малому бизнесу",
                    "Клиентский менеджер по работе с частными клиентами",
                    "Клиентский менеджер розничного банкинга",
                    "Старший клиентский менеджер",
                    "Ведущий клиентский менеджер"
                ])
                business_block = "KMKKSB"  # Все клиентские менеджеры в KMKKSB
                role_code = "KM_KKSB"
                uch_code = "1"  # Участник
                
            kpk_code = kpk_info['kpk_code']
            kpk_name = kpk_info['kpk_name']
            
        else:
            # Обычный сотрудник (не в КПК)
            position_name = random.choice([
                "Ведущий специалист", "Главный специалист", "Старший специалист", 
                "Специалист", "Консультант", "Старший консультант",
                "Заместитель начальника отдела", "Заместитель исполнительного директора"
            ])
            business_block = random.choice(self.business_blocks)
            role_code = random.choice(self.role_codes)
            kpk_code = ""
            kpk_name = ""
            
            # UCH_CODE для обычных сотрудников
            if random.random() < 0.006:  # 0.6% (3 человека из 500 оставшихся)
                uch_code = "3"
            else:
                uch_code = "0"
        
        return {
            'PERSON_NUMBER': person_number,
            'PERSON_NUMBER_ADD': person_number,  # Такой же как основной
            'SURNAME': surname,
            'FIRST_NAME': first_name,
            'MIDDLE_NAME': middle_name,
            'MANAGER_FULL_NAME': full_name,
            'POSITION_NAME': position_name,
            'TB_CODE': org_unit['TB_CODE'],
            'GOSB_CODE': org_unit['GOSB_CODE'],
            'BUSINESS_BLOCK': business_block,
            'PRIORITY_TYPE': "1",  # Всегда 1
            'KPK_CODE': kpk_code,
            'KPK_NAME': kpk_name,
            'ROLE_CODE': role_code,
            'UCH_CODE': uch_code,
            'GENDER': str(gender),
            'ORG_UNIT_CODE': org_unit['ORG_UNIT_CODE']
        }
        
    def generate_all_records(self) -> pd.DataFrame:
        """Генерирует все записи сотрудников"""
        logging.info(f"Генерация {TARGET_RECORDS} записей сотрудников...")
        
        records = []
        for i in range(TARGET_RECORDS):
            if (i + 1) % 500 == 0:
                logging.info(f"Сгенерировано {i + 1} записей...")
                
            record = self.generate_employee_record(i)
            records.append(record)
            
        df = pd.DataFrame(records, columns=EMPLOYEE_FIELDS)
        logging.info("Генерация записей завершена")
        
        return df
        
    def save_to_csv(self, df: pd.DataFrame):
        """Сохраняет DataFrame в CSV файл"""
        output_path = os.path.join(DIR_OUTPUT, OUTPUT_FILENAME)
        
        try:
            df.to_csv(output_path, sep=';', index=False, encoding='utf-8')
            logging.info(f"Файл сохранен: {output_path}")
            
            # Статистика
            self.print_statistics(df)
            
        except Exception as e:
            logging.error(f"Ошибка сохранения файла: {e}")
            
    def print_statistics(self, df: pd.DataFrame):
        """Выводит статистику по сгенерированным данным"""
        logging.info("=== СТАТИСТИКА СГЕНЕРИРОВАННЫХ ДАННЫХ ===")
        logging.info(f"Общее количество записей: {len(df)}")
        
        # Статистика по полу
        gender_stats = df['GENDER'].value_counts()
        logging.info(f"Мужчин (1): {gender_stats.get('1', 0)} ({gender_stats.get('1', 0)/len(df)*100:.1f}%)")
        logging.info(f"Женщин (2): {gender_stats.get('2', 0)} ({gender_stats.get('2', 0)/len(df)*100:.1f}%)")
        
        # Статистика по UCH_CODE
        uch_stats = df['UCH_CODE'].value_counts()
        logging.info("UCH_CODE статистика:")
        for uch, count in uch_stats.items():
            desc = {"0": "не участник", "1": "участник (КМ)", "2": "руководитель", "3": "особый статус"}
            logging.info(f"  {uch} ({desc.get(uch, 'неизвестно')}): {count}")
        
        # Статистика по КПК
        kpk_count = len(df[df['KPK_CODE'] != ''])
        unique_kpk = df[df['KPK_CODE'] != '']['KPK_CODE'].nunique()
        logging.info(f"Сотрудников в КПК: {kpk_count} ({kpk_count/len(df)*100:.1f}%)")
        logging.info(f"Количество КПК: {unique_kpk}")
        logging.info(f"Сотрудников вне КПК: {len(df) - kpk_count}")
        
        # Статистика по бизнес-блокам
        business_stats = df['BUSINESS_BLOCK'].value_counts()
        logging.info("Распределение по бизнес-блокам:")
        for block, count in business_stats.items():
            logging.info(f"  {block}: {count} ({count/len(df)*100:.1f}%)")
        
        # Статистика по должностям
        km_count = len(df[df['POSITION_NAME'].str.contains('Клиентский менеджер', case=False, na=False)])
        director_count = len(df[df['POSITION_NAME'].str.contains('директор|управляющий|начальник', case=False, na=False)])
        logging.info(f"Клиентских менеджеров: {km_count}")
        logging.info(f"Руководителей: {director_count}")
        
        # Уникальность табельных номеров и ФИО
        unique_person_numbers = df['PERSON_NUMBER'].nunique()
        unique_names = df['MANAGER_FULL_NAME'].nunique()
        logging.info(f"Уникальных табельных номеров: {unique_person_numbers} (должно быть {len(df)})")
        logging.info(f"Уникальных ФИО: {unique_names} (должно быть {len(df)})")
        
        # Проверка корректности данных
        if unique_person_numbers != len(df):
            logging.warning("⚠️  НАЙДЕНЫ ДУБЛИРУЮЩИЕСЯ ТАБЕЛЬНЫЕ НОМЕРА!")
        if unique_names != len(df):
            logging.warning("⚠️  НАЙДЕНЫ ДУБЛИРУЮЩИЕСЯ ФИО!")
            
        # Статистика по организационным единицам
        unique_tb = df['TB_CODE'].nunique()
        unique_gosb = df['GOSB_CODE'].nunique()
        unique_org = df['ORG_UNIT_CODE'].nunique()
        logging.info(f"Уникальных ТБ: {unique_tb}, ГОСБ: {unique_gosb}, ORG_UNIT: {unique_org}")
        
        # Проверка что все КМ в блоке KMKKSB
        km_in_kmkksb = len(df[(df['POSITION_NAME'].str.contains('Клиентский менеджер', case=False, na=False)) & 
                             (df['BUSINESS_BLOCK'] == 'KMKKSB')])
        if km_in_kmkksb == km_count:
            logging.info("✅ Все клиентские менеджеры корректно размещены в блоке KMKKSB")
        else:
            logging.warning(f"⚠️  {km_count - km_in_kmkksb} клиентских менеджеров НЕ в блоке KMKKSB!")

def main():
    """Основная функция"""
    logging.info("=== СТАРТ ГЕНЕРАЦИИ ДАННЫХ EMPLOYEE ===")
    
    # Создаем директории если не существуют
    os.makedirs(DIR_OUTPUT, exist_ok=True)
    os.makedirs(DIR_LOGS, exist_ok=True)
    
    generator = EmployeeGenerator()
    
    # Загружаем исходные данные
    if not generator.load_source_data():
        logging.error("Ошибка загрузки исходных данных")
        return False
        
    # Генерируем структуру КПК
    generator.generate_kpk_structure()
    
    # Генерируем все записи
    df = generator.generate_all_records()
    
    # Сохраняем в файл
    generator.save_to_csv(df)
    
    logging.info("=== ГЕНЕРАЦИЯ ЗАВЕРШЕНА УСПЕШНО ===")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 