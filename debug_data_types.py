#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

def debug_data_types():
    """Отлаживает типы данных и форматирование"""
    
    excel_file = "/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT/SPOD_ALL_IN_ONE_2025-10-09_12-45-17.xlsx"
    
    try:
        # Читаем таблицы
        summary_df = pd.read_excel(excel_file, sheet_name='SUMMARY')
        reward_df = pd.read_excel(excel_file, sheet_name='REWARD')
        
        print("=== АНАЛИЗ ТИПОВ ДАННЫХ ===")
        
        # Проверяем типы данных
        print(f"Тип REWARD_CODE в SUMMARY: {summary_df['REWARD_CODE'].dtype}")
        print(f"Тип REWARD_CODE в REWARD: {reward_df['REWARD_CODE'].dtype}")
        
        # Проверяем конкретные значения
        print(f"\nПримеры из SUMMARY:")
        for i in range(5):
            val = summary_df['REWARD_CODE'].iloc[i]
            print(f"  {i}: '{val}' (тип: {type(val)})")
        
        print(f"\nПримеры из REWARD:")
        for i in range(5):
            val = reward_df['REWARD_CODE'].iloc[i]
            print(f"  {i}: '{val}' (тип: {type(val)})")
        
        # Проверяем, есть ли совпадения
        print(f"\n=== ПРОВЕРКА СОВПАДЕНИЙ ===")
        
        # Берем первые 5 REWARD_CODE из SUMMARY и ищем их в REWARD
        for i in range(5):
            summary_code = summary_df['REWARD_CODE'].iloc[i]
            found = reward_df[reward_df['REWARD_CODE'] == summary_code]
            print(f"'{summary_code}' в REWARD: {len(found)} совпадений")
            if len(found) > 0:
                print(f"  Найдено: {found['REWARD_CODE'].iloc[0]}")
        
        # Проверяем группировку
        print(f"\n=== ПРОВЕРКА ГРУППИРОВКИ ===")
        group_counts = reward_df.groupby(['REWARD_CODE']).size()
        
        # Проверяем первые 5 ключей из SUMMARY
        for i in range(5):
            summary_code = summary_df['REWARD_CODE'].iloc[i]
            key_tuple = (summary_code,)
            count = group_counts.get(key_tuple, 'НЕ НАЙДЕНО')
            print(f"'{summary_code}' -> {count}")
        
        # Проверяем, есть ли проблемы с пробелами или другими символами
        print(f"\n=== ПРОВЕРКА ФОРМАТИРОВАНИЯ ===")
        
        # Проверяем длину строк
        summary_lengths = summary_df['REWARD_CODE'].astype(str).str.len()
        reward_lengths = reward_df['REWARD_CODE'].astype(str).str.len()
        
        print(f"Длина REWARD_CODE в SUMMARY: мин={summary_lengths.min()}, макс={summary_lengths.max()}")
        print(f"Длина REWARD_CODE в REWARD: мин={reward_lengths.min()}, макс={reward_lengths.max()}")
        
        # Проверяем на наличие пробелов
        summary_with_spaces = summary_df['REWARD_CODE'].astype(str).str.contains(' ').sum()
        reward_with_spaces = reward_df['REWARD_CODE'].astype(str).str.contains(' ').sum()
        
        print(f"REWARD_CODE с пробелами в SUMMARY: {summary_with_spaces}")
        print(f"REWARD_CODE с пробелами в REWARD: {reward_with_spaces}")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_data_types()
