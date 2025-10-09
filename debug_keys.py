#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

def debug_summary_keys():
    """Отлаживает ключи в SUMMARY листе"""
    
    excel_file = "/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT/SPOD_ALL_IN_ONE_2025-10-09_12-45-17.xlsx"
    
    try:
        # Читаем SUMMARY лист
        df = pd.read_excel(excel_file, sheet_name='SUMMARY')
        
        print("Ключевые колонки в SUMMARY:")
        key_cols = ['CONTEST_CODE', 'TOURNAMENT_CODE', 'REWARD_CODE', 'GROUP_CODE', 'GROUP_VALUE']
        for col in key_cols:
            if col in df.columns:
                unique_count = df[col].nunique()
                print(f"  {col}: {unique_count} уникальных значений")
                print(f"    Примеры: {list(df[col].dropna().unique()[:5])}")
            else:
                print(f"  {col}: ОТСУТСТВУЕТ")
        
        print(f"\nВсего строк в SUMMARY: {len(df)}")
        
        # Проверяем исходные таблицы
        print("\n=== ИСХОДНЫЕ ТАБЛИЦЫ ===")
        
        # CONTEST-DATA
        try:
            contest_df = pd.read_excel(excel_file, sheet_name='CONTEST-DATA')
            print(f"CONTEST-DATA: {len(contest_df)} строк")
            if 'CONTEST_CODE' in contest_df.columns:
                print(f"  CONTEST_CODE: {contest_df['CONTEST_CODE'].nunique()} уникальных")
        except:
            print("CONTEST-DATA: не удалось прочитать")
        
        # INDICATOR
        try:
            indicator_df = pd.read_excel(excel_file, sheet_name='INDICATOR')
            print(f"INDICATOR: {len(indicator_df)} строк")
            if 'CONTEST_CODE' in indicator_df.columns:
                print(f"  CONTEST_CODE: {indicator_df['CONTEST_CODE'].nunique()} уникальных")
        except:
            print("INDICATOR: не удалось прочитать")
        
        # REWARD
        try:
            reward_df = pd.read_excel(excel_file, sheet_name='REWARD')
            print(f"REWARD: {len(reward_df)} строк")
            if 'REWARD_CODE' in reward_df.columns:
                print(f"  REWARD_CODE: {reward_df['REWARD_CODE'].nunique()} уникальных")
        except:
            print("REWARD: не удалось прочитать")
        
        # TOURNAMENT-SCHEDULE
        try:
            ts_df = pd.read_excel(excel_file, sheet_name='TOURNAMENT-SCHEDULE')
            print(f"TOURNAMENT-SCHEDULE: {len(ts_df)} строк")
            if 'TOURNAMENT_CODE' in ts_df.columns:
                print(f"  TOURNAMENT_CODE: {ts_df['TOURNAMENT_CODE'].nunique()} уникальных")
            if 'CONTEST_CODE' in ts_df.columns:
                print(f"  CONTEST_CODE: {ts_df['CONTEST_CODE'].nunique()} уникальных")
        except:
            print("TOURNAMENT-SCHEDULE: не удалось прочитать")
        
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    debug_summary_keys()
