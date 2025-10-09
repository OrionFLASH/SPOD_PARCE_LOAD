#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

def test_count_fix():
    """Тестирует исправление подсчета"""
    
    excel_file = "/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT/SPOD_ALL_IN_ONE_2025-10-09_12-45-17.xlsx"
    
    try:
        # Читаем таблицы
        summary_df = pd.read_excel(excel_file, sheet_name='SUMMARY')
        reward_df = pd.read_excel(excel_file, sheet_name='REWARD')
        
        print("=== ТЕСТ ИСПРАВЛЕНИЯ ПОДСЧЕТА ===")
        
        # Симулируем логику из add_fields_to_sheet
        dst_keys = ['REWARD_CODE']
        src_keys = ['REWARD_CODE']
        
        # Создаем ключи для SUMMARY
        def tuple_key(row, keys):
            if isinstance(keys, (list, tuple)):
                result = []
                for k in keys:
                    v = row[k]
                    if isinstance(v, pd.Series):
                        v = v.iloc[0]
                    result.append(v)
                return tuple(result)
            else:
                v = row[keys]
                if isinstance(v, pd.Series):
                    v = v.iloc[0]
                return (v,)
        
        new_keys = summary_df.apply(lambda row: tuple_key(row, dst_keys), axis=1)
        group_counts = reward_df.groupby(src_keys).size()
        
        print(f"Тип group_counts: {type(group_counts)}")
        print(f"Тип индекса: {type(group_counts.index)}")
        print(f"Примеры индексов: {list(group_counts.index[:5])}")
        
        # Создаем словарь для сопоставления ключей
        count_dict = {}
        for key_tuple, count in group_counts.items():
            count_dict[key_tuple] = count
        
        print(f"Словарь создан: {len(count_dict)} записей")
        print(f"Примеры из словаря: {list(count_dict.items())[:5]}")
        
        # Тестируем сопоставление
        print(f"\n=== ТЕСТ СОПОСТАВЛЕНИЯ ===")
        
        # Берем первые 5 ключей из SUMMARY
        test_keys = new_keys.head(5)
        print(f"Тестовые ключи: {list(test_keys)}")
        
        # Сопоставляем
        mapped_counts = test_keys.map(count_dict)
        print(f"Сопоставленные значения: {list(mapped_counts)}")
        print(f"Ненулевых: {(mapped_counts > 0).sum()}")
        print(f"Нулевых: {(mapped_counts == 0).sum()}")
        print(f"NaN: {mapped_counts.isna().sum()}")
        
        # Проверяем конкретные примеры
        print(f"\n=== КОНКРЕТНЫЕ ПРИМЕРЫ ===")
        for i, key in enumerate(test_keys):
            count = count_dict.get(key, 'НЕ НАЙДЕНО')
            print(f"Ключ {i}: {key} -> {count}")
        
        # Тестируем альтернативный подход
        print(f"\n=== АЛЬТЕРНАТИВНЫЙ ПОДХОД ===")
        
        # Создаем Series с правильным индексом
        count_series = pd.Series(group_counts.values, index=group_counts.index)
        
        # Сопоставляем через Series
        mapped_series = test_keys.map(count_series)
        print(f"Через Series: {list(mapped_series)}")
        print(f"Ненулевых через Series: {(mapped_series > 0).sum()}")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_count_fix()
