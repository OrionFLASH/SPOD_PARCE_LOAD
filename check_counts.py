#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import sys

def check_summary_counts():
    """Проверяет колонки с подсчетами в SUMMARY листе"""
    
    # Путь к созданному Excel файлу
    excel_file = "/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT/SPOD_ALL_IN_ONE_2025-10-09_12-45-17.xlsx"
    
    try:
        # Читаем SUMMARY лист
        df = pd.read_excel(excel_file, sheet_name='SUMMARY')
        
        print(f"SUMMARY лист: {len(df)} строк, {len(df.columns)} колонок")
        print("\nКолонки с подсчетами:")
        
        # Ищем все колонки с COUNT
        count_columns = [col for col in df.columns if 'COUNT' in col]
        
        for col in count_columns:
            print(f"\n{col}:")
            print(f"  - Минимум: {df[col].min()}")
            print(f"  - Максимум: {df[col].max()}")
            print(f"  - Среднее: {df[col].mean():.2f}")
            print(f"  - Ненулевых значений: {(df[col] > 0).sum()}")
            print(f"  - Нулевых значений: {(df[col] == 0).sum()}")
            
            # Показываем несколько примеров ненулевых значений
            non_zero = df[df[col] > 0][col].head(5)
            if len(non_zero) > 0:
                print(f"  - Примеры ненулевых значений: {list(non_zero)}")
        
        print(f"\nВсего колонок с подсчетами: {len(count_columns)}")
        
        # Проверяем конкретные подсчеты
        print("\n=== ПРОВЕРКА КОНКРЕТНЫХ ПОДСЧЕТОВ ===")
        
        # CONTEST-DATA count
        if 'CONTEST-DATA=>COUNT_CONTEST_CODE' in df.columns:
            contest_count = df['CONTEST-DATA=>COUNT_CONTEST_CODE']
            print(f"CONTEST-DATA count: мин={contest_count.min()}, макс={contest_count.max()}, ненулевых={(contest_count > 0).sum()}")
        
        # GROUP count по CONTEST_CODE
        if 'GROUP=>COUNT_CONTEST_CODE' in df.columns:
            group_count = df['GROUP=>COUNT_CONTEST_CODE']
            print(f"GROUP count по CONTEST_CODE: мин={group_count.min()}, макс={group_count.max()}, ненулевых={(group_count > 0).sum()}")
        
        # REPORT count
        if 'REPORT=>COUNT_CONTEST_DATE' in df.columns:
            report_count = df['REPORT=>COUNT_CONTEST_DATE']
            print(f"REPORT count: мин={report_count.min()}, макс={report_count.max()}, ненулевых={(report_count > 0).sum()}")
        
        # REWARD count
        if 'REWARD=>COUNT_REWARD_CODE' in df.columns:
            reward_count = df['REWARD=>COUNT_REWARD_CODE']
            print(f"REWARD count: мин={reward_count.min()}, макс={reward_count.max()}, ненулевых={(reward_count > 0).sum()}")
        
        # TOURNAMENT-SCHEDULE count по CONTEST_CODE
        if 'TOURNAMENT-SCHEDULE=>COUNT_CONTEST_CODE' in df.columns:
            ts_contest_count = df['TOURNAMENT-SCHEDULE=>COUNT_CONTEST_CODE']
            print(f"TOURNAMENT-SCHEDULE count по CONTEST_CODE: мин={ts_contest_count.min()}, макс={ts_contest_count.max()}, ненулевых={(ts_contest_count > 0).sum()}")
        
        # TOURNAMENT-SCHEDULE count по TOURNAMENT_CODE
        if 'TOURNAMENT-SCHEDULE=>COUNT_TOURNAMENT_CODE' in df.columns:
            ts_tournament_count = df['TOURNAMENT-SCHEDULE=>COUNT_TOURNAMENT_CODE']
            print(f"TOURNAMENT-SCHEDULE count по TOURNAMENT_CODE: мин={ts_tournament_count.min()}, макс={ts_tournament_count.max()}, ненулевых={(ts_tournament_count > 0).sum()}")
        
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return False
    
    return True

if __name__ == "__main__":
    check_summary_counts()
