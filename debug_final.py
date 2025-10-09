#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

def debug_final():
    """Финальная отладка подсчета"""
    
    excel_file = "/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT/SPOD_ALL_IN_ONE_2025-10-09_12-51-41.xlsx"
    
    try:
        # Читаем таблицы
        summary_df = pd.read_excel(excel_file, sheet_name='SUMMARY')
        reward_df = pd.read_excel(excel_file, sheet_name='REWARD')
        
        print("=== ФИНАЛЬНАЯ ОТЛАДКА ===")
        
        # Проверяем REWARD_CODE в обеих таблицах
        print(f"REWARD_CODE в REWARD: {len(reward_df['REWARD_CODE'].unique())}")
        print(f"REWARD_CODE в SUMMARY: {len(summary_df['REWARD_CODE'].unique())}")
        
        # Проверяем пересечение
        reward_codes_in_reward = set(reward_df['REWARD_CODE'].dropna().unique())
        reward_codes_in_summary = set(summary_df['REWARD_CODE'].dropna().unique())
        
        intersection = reward_codes_in_reward & reward_codes_in_summary
        print(f"Пересечение: {len(intersection)}")
        
        # Проверяем группировку
        group_counts = reward_df.groupby(['REWARD_CODE']).size()
        print(f"Группировка в REWARD: {len(group_counts)} групп")
        
        # Проверяем ключи в SUMMARY
        summary_keys = summary_df.apply(lambda row: (row['REWARD_CODE'],), axis=1)
        print(f"Ключи в SUMMARY: {len(summary_keys)}")
        
        # Проверяем сопоставление
        print(f"\n=== ПРОВЕРКА СОПОСТАВЛЕНИЯ ===")
        
        # Берем первые 5 ключей из SUMMARY
        test_keys = summary_keys.head(5)
        print(f"Тестовые ключи: {list(test_keys)}")
        
        # Сопоставляем через Series (как в исправленном коде)
        mapped_series = test_keys.map(group_counts)
        print(f"Через Series: {list(mapped_series)}")
        print(f"Ненулевых через Series: {(mapped_series > 0).sum()}")
        
        # Проверяем конкретные примеры
        print(f"\n=== КОНКРЕТНЫЕ ПРИМЕРЫ ===")
        for i, key in enumerate(test_keys):
            if key in group_counts.index:
                count = group_counts[key]
                print(f"Ключ {i}: {key} -> {count} (найден в индексе)")
            else:
                print(f"Ключ {i}: {key} -> НЕ НАЙДЕН в индексе")
        
        # Проверяем, есть ли проблемы с типами данных
        print(f"\n=== ПРОВЕРКА ТИПОВ ДАННЫХ ===")
        
        # Проверяем типы в индексе group_counts
        index_types = [type(x) for x in group_counts.index[:5]]
        print(f"Типы в индексе group_counts: {index_types}")
        
        # Проверяем типы в ключах SUMMARY
        key_types = [type(x[0]) for x in test_keys]
        print(f"Типы в ключах SUMMARY: {key_types}")
        
        # Проверяем, есть ли проблемы с NaN
        print(f"\n=== ПРОВЕРКА NaN ===")
        has_nan_in_index = group_counts.index.isna().any()
        has_nan_in_keys = test_keys.isna().any()
        print(f"Есть NaN в индексе group_counts: {has_nan_in_index}")
        print(f"Есть NaN в ключах SUMMARY: {has_nan_in_keys}")
        
        # Проверяем, есть ли проблемы с пробелами
        print(f"\n=== ПРОВЕРКА ПРОБЕЛОВ ===")
        index_with_spaces = group_counts.index.astype(str).str.contains(' ').sum()
        keys_with_spaces = test_keys.astype(str).str.contains(' ').sum()
        print(f"Индекс с пробелами: {index_with_spaces}")
        print(f"Ключи с пробелами: {keys_with_spaces}")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_final()
