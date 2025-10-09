#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

def debug_reward_count():
    """Отлаживает подсчет REWARD"""
    
    excel_file = "/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT/SPOD_ALL_IN_ONE_2025-10-09_12-45-17.xlsx"
    
    try:
        # Читаем таблицы
        summary_df = pd.read_excel(excel_file, sheet_name='SUMMARY')
        reward_df = pd.read_excel(excel_file, sheet_name='REWARD')
        
        print("=== ОТЛАДКА REWARD ПОДСЧЕТА ===")
        
        # Проверяем ключи
        print(f"REWARD_CODE в REWARD: {len(reward_df['REWARD_CODE'].unique())}")
        print(f"REWARD_CODE в SUMMARY: {len(summary_df['REWARD_CODE'].unique())}")
        
        # Проверяем пересечение
        reward_codes_in_data = set(reward_df['REWARD_CODE'].dropna().unique())
        reward_codes_in_summary = set(summary_df['REWARD_CODE'].dropna().unique())
        
        intersection = reward_codes_in_data & reward_codes_in_summary
        print(f"Пересечение: {len(intersection)}")
        
        # Проверяем группировку в REWARD
        group_counts = reward_df.groupby(['REWARD_CODE']).size()
        print(f"Группировка в REWARD: {len(group_counts)} групп")
        print(f"Примеры групп: {list(group_counts.head())}")
        
        # Проверяем ключи в SUMMARY
        summary_keys = summary_df.apply(lambda row: (row['REWARD_CODE'],), axis=1)
        print(f"Ключи в SUMMARY: {len(summary_keys)}")
        print(f"Примеры ключей: {list(summary_keys.head())}")
        
        # Проверяем сопоставление
        count_dict = {}
        for key_tuple, count in group_counts.items():
            count_dict[key_tuple] = count
        
        print(f"Словарь подсчетов: {len(count_dict)}")
        print(f"Примеры из словаря: {list(count_dict.items())[:5]}")
        
        # Проверяем сопоставление
        mapped_counts = summary_keys.map(count_dict)
        print(f"Сопоставленные значения: {len(mapped_counts)}")
        print(f"Ненулевых: {(mapped_counts > 0).sum()}")
        print(f"Нулевых: {(mapped_counts == 0).sum()}")
        print(f"NaN: {mapped_counts.isna().sum()}")
        
        # Проверяем конкретные примеры
        print("\n=== КОНКРЕТНЫЕ ПРИМЕРЫ ===")
        for i in range(min(5, len(summary_df))):
            row = summary_df.iloc[i]
            reward_code = row['REWARD_CODE']
            key_tuple = (reward_code,)
            count = count_dict.get(key_tuple, 'НЕ НАЙДЕНО')
            print(f"Строка {i}: REWARD_CODE={reward_code}, ключ={key_tuple}, счет={count}")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_reward_count()
