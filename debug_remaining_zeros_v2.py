#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
from pathlib import Path

def debug_remaining_zeros_v2():
    """Отладка оставшихся нулевых счетчиков после исправлений"""
    
    # Находим последний созданный файл
    out_dir = Path("/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT")
    excel_files = list(out_dir.glob("SPOD_ALL_IN_ONE_*.xlsx"))
    latest_file = max(excel_files, key=os.path.getctime)
    
    print(f"🔍 Отладка файла: {latest_file.name}")
    
    # Читаем все листы
    sheets = {
        'SUMMARY': pd.read_excel(latest_file, sheet_name='SUMMARY'),
        'INDICATOR': pd.read_excel(latest_file, sheet_name='INDICATOR'),
        'REPORT': pd.read_excel(latest_file, sheet_name='REPORT'),
        'TOURNAMENT-SCHEDULE': pd.read_excel(latest_file, sheet_name='TOURNAMENT-SCHEDULE')
    }
    
    print(f"📊 Загружено листов: {len(sheets)}")
    for name, df in sheets.items():
        print(f"  {name}: {len(df)} строк, {len(df.columns)} колонок")
    
    # Анализируем проблемные счетчики
    problematic_counters = [
        'INDICATOR=>COUNT_CONTEST_CODE',
        'REPORT=>COUNT_CONTEST_DATE', 
        'TOURNAMENT-SCHEDULE=>COUNT_TOURNAMENT_CODE'
    ]
    
    summary_df = sheets['SUMMARY']
    
    for counter in problematic_counters:
        if counter not in summary_df.columns:
            print(f"\n❌ {counter} - колонка не найдена")
            continue
            
        print(f"\n🔍 Анализ {counter}:")
        print("=" * 60)
        
        # Извлекаем источник и ключ из названия счетчика
        if 'INDICATOR=>COUNT_CONTEST_CODE' in counter:
            source_sheet = 'INDICATOR'
            source_key = 'CONTEST_CODE'
            dest_key = 'CONTEST_CODE'
        elif 'REPORT=>COUNT_CONTEST_DATE' in counter:
            source_sheet = 'REPORT'
            source_key = 'CONTEST_CODE'
            dest_key = 'CONTEST_CODE'
        elif 'TOURNAMENT-SCHEDULE=>COUNT_TOURNAMENT_CODE' in counter:
            source_sheet = 'REPORT'  # Изменено на REPORT согласно исправлению
            source_key = 'TOURNAMENT_CODE'
            dest_key = 'TOURNAMENT_CODE'
        else:
            print(f"  Неизвестный счетчик: {counter}")
            continue
        
        print(f"  Источник: {source_sheet}, ключ: {source_key}")
        print(f"  Назначение: SUMMARY, ключ: {dest_key}")
        
        # Проверяем данные в источнике
        if source_sheet in sheets:
            source_df = sheets[source_sheet]
            print(f"  Строк в источнике: {len(source_df)}")
            
            if source_key in source_df.columns:
                source_values = source_df[source_key].dropna().unique()
                print(f"  Уникальных значений {source_key} в источнике: {len(source_values)}")
                print(f"  Примеры: {list(source_values[:5])}")
            else:
                print(f"  ❌ Колонка {source_key} не найдена в {source_sheet}")
                continue
        else:
            print(f"  ❌ Лист {source_sheet} не найден")
            continue
        
        # Проверяем данные в SUMMARY
        if dest_key in summary_df.columns:
            summary_values = summary_df[dest_key].dropna().unique()
            print(f"  Уникальных значений {dest_key} в SUMMARY: {len(summary_values)}")
            print(f"  Примеры: {list(summary_values[:5])}")
        else:
            print(f"  ❌ Колонка {dest_key} не найдена в SUMMARY")
            continue
        
        # Проверяем пересечение
        if source_sheet in sheets and source_key in sheets[source_sheet].columns and dest_key in summary_df.columns:
            source_set = set(sheets[source_sheet][source_key].dropna())
            summary_set = set(summary_df[dest_key].dropna())
            intersection = source_set & summary_set
            
            print(f"  Пересечение ключей: {len(intersection)}")
            if len(intersection) > 0:
                print(f"  Примеры пересечений: {list(intersection)[:5]}")
            else:
                print(f"  ❌ Нет пересечений между ключами!")
                
                # Показываем примеры из каждого набора
                print(f"  Примеры из источника: {list(source_set)[:5]}")
                print(f"  Примеры из SUMMARY: {list(summary_set)[:5]}")
                
                # Проверяем типы данных
                if len(source_set) > 0 and len(summary_set) > 0:
                    source_sample = list(source_set)[0]
                    summary_sample = list(summary_set)[0]
                    print(f"  Тип данных в источнике: {type(source_sample)} = '{source_sample}'")
                    print(f"  Тип данных в SUMMARY: {type(summary_sample)} = '{summary_sample}'")
        
        # Проверяем сам счетчик
        counter_values = summary_df[counter].dropna()
        non_zero = (counter_values > 0).sum()
        print(f"  Ненулевых значений в счетчике: {non_zero} из {len(counter_values)}")
        
        if non_zero == 0:
            print(f"  ❌ Все значения счетчика равны 0")
        else:
            print(f"  ✅ Счетчик работает! Примеры: {list(counter_values[counter_values > 0].unique())[:5]}")

if __name__ == "__main__":
    debug_remaining_zeros_v2()
