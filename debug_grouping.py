#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

def debug_grouping():
    """Отлаживает группировку"""
    
    excel_file = "/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT/SPOD_ALL_IN_ONE_2025-10-09_12-45-17.xlsx"
    
    try:
        # Читаем таблицы
        summary_df = pd.read_excel(excel_file, sheet_name='SUMMARY')
        reward_df = pd.read_excel(excel_file, sheet_name='REWARD')
        
        print("=== ОТЛАДКА ГРУППИРОВКИ ===")
        
        # Группируем REWARD
        group_counts = reward_df.groupby(['REWARD_CODE']).size()
        
        print(f"Тип group_counts: {type(group_counts)}")
        print(f"Индекс group_counts: {type(group_counts.index)}")
        print(f"Примеры индексов: {list(group_counts.index[:5])}")
        print(f"Примеры значений: {list(group_counts.values[:5])}")
        
        # Проверяем конкретные ключи
        print(f"\n=== ПРОВЕРКА КОНКРЕТНЫХ КЛЮЧЕЙ ===")
        
        for i in range(5):
            summary_code = summary_df['REWARD_CODE'].iloc[i]
            key_tuple = (summary_code,)
            
            print(f"\nКлюч: {key_tuple}")
            print(f"Тип ключа: {type(key_tuple)}")
            print(f"Тип элемента: {type(key_tuple[0])}")
            
            # Проверяем, есть ли в индексе
            if summary_code in group_counts.index:
                count = group_counts[summary_code]
                print(f"Найдено в индексе: {count}")
            else:
                print("НЕ найдено в индексе")
            
            # Проверяем через get
            count_get = group_counts.get(key_tuple, 'НЕ НАЙДЕНО')
            print(f"Через get: {count_get}")
            
            # Проверяем через loc
            try:
                count_loc = group_counts.loc[summary_code]
                print(f"Через loc: {count_loc}")
            except KeyError:
                print("Через loc: KeyError")
        
        # Проверяем, есть ли проблемы с MultiIndex
        print(f"\n=== ПРОВЕРКА ИНДЕКСА ===")
        print(f"Индекс является MultiIndex: {isinstance(group_counts.index, pd.MultiIndex)}")
        print(f"Уровни индекса: {group_counts.index.nlevels}")
        
        if isinstance(group_counts.index, pd.MultiIndex):
            print(f"Имена уровней: {group_counts.index.names}")
        else:
            print(f"Имя индекса: {group_counts.index.name}")
        
        # Проверяем, есть ли проблемы с типами в индексе
        print(f"\n=== ПРОВЕРКА ТИПОВ В ИНДЕКСЕ ===")
        index_types = [type(x) for x in group_counts.index[:5]]
        print(f"Типы в индексе: {index_types}")
        
        # Проверяем, есть ли проблемы с NaN
        print(f"\n=== ПРОВЕРКА NaN ===")
        has_nan = group_counts.index.isna().any()
        print(f"Есть NaN в индексе: {has_nan}")
        
        if has_nan:
            nan_positions = group_counts.index.isna()
            print(f"Позиции с NaN: {nan_positions.sum()}")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_grouping()
