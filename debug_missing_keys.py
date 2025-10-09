#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

def debug_missing_keys():
    """Отлаживает отсутствующие ключи между SUMMARY и исходными таблицами"""
    
    excel_file = "/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT/SPOD_ALL_IN_ONE_2025-10-09_12-45-17.xlsx"
    
    try:
        # Читаем все таблицы
        summary_df = pd.read_excel(excel_file, sheet_name='SUMMARY')
        contest_df = pd.read_excel(excel_file, sheet_name='CONTEST-DATA')
        indicator_df = pd.read_excel(excel_file, sheet_name='INDICATOR')
        reward_df = pd.read_excel(excel_file, sheet_name='REWARD')
        ts_df = pd.read_excel(excel_file, sheet_name='TOURNAMENT-SCHEDULE')
        
        print("=== АНАЛИЗ ОТСУТСТВУЮЩИХ КЛЮЧЕЙ ===")
        
        # CONTEST-DATA vs SUMMARY
        print("\n1. CONTEST-DATA vs SUMMARY:")
        contest_codes_in_data = set(contest_df['CONTEST_CODE'].dropna().unique())
        contest_codes_in_summary = set(summary_df['CONTEST_CODE'].dropna().unique())
        
        missing_in_data = contest_codes_in_summary - contest_codes_in_data
        missing_in_summary = contest_codes_in_data - contest_codes_in_summary
        
        print(f"  CONTEST_CODE в CONTEST-DATA: {len(contest_codes_in_data)}")
        print(f"  CONTEST_CODE в SUMMARY: {len(contest_codes_in_summary)}")
        print(f"  Отсутствует в CONTEST-DATA: {len(missing_in_data)}")
        print(f"  Отсутствует в SUMMARY: {len(missing_in_summary)}")
        
        if missing_in_data:
            print(f"  Примеры отсутствующих в CONTEST-DATA: {list(missing_in_data)[:5]}")
        if missing_in_summary:
            print(f"  Примеры отсутствующих в SUMMARY: {list(missing_in_summary)[:5]}")
        
        # INDICATOR vs SUMMARY
        print("\n2. INDICATOR vs SUMMARY:")
        indicator_codes_in_data = set(indicator_df['CONTEST_CODE'].dropna().unique())
        indicator_codes_in_summary = set(summary_df['CONTEST_CODE'].dropna().unique())
        
        missing_in_indicator = indicator_codes_in_summary - indicator_codes_in_data
        missing_in_summary_indicator = indicator_codes_in_data - indicator_codes_in_summary
        
        print(f"  CONTEST_CODE в INDICATOR: {len(indicator_codes_in_data)}")
        print(f"  CONTEST_CODE в SUMMARY: {len(indicator_codes_in_summary)}")
        print(f"  Отсутствует в INDICATOR: {len(missing_in_indicator)}")
        print(f"  Отсутствует в SUMMARY: {len(missing_in_summary_indicator)}")
        
        if missing_in_indicator:
            print(f"  Примеры отсутствующих в INDICATOR: {list(missing_in_indicator)[:5]}")
        if missing_in_summary_indicator:
            print(f"  Примеры отсутствующих в SUMMARY: {list(missing_in_summary_indicator)[:5]}")
        
        # REWARD vs SUMMARY
        print("\n3. REWARD vs SUMMARY:")
        reward_codes_in_data = set(reward_df['REWARD_CODE'].dropna().unique())
        reward_codes_in_summary = set(summary_df['REWARD_CODE'].dropna().unique())
        
        missing_in_reward = reward_codes_in_summary - reward_codes_in_data
        missing_in_summary_reward = reward_codes_in_data - reward_codes_in_summary
        
        print(f"  REWARD_CODE в REWARD: {len(reward_codes_in_data)}")
        print(f"  REWARD_CODE в SUMMARY: {len(reward_codes_in_summary)}")
        print(f"  Отсутствует в REWARD: {len(missing_in_reward)}")
        print(f"  Отсутствует в SUMMARY: {len(missing_in_summary_reward)}")
        
        if missing_in_reward:
            print(f"  Примеры отсутствующих в REWARD: {list(missing_in_reward)[:5]}")
        if missing_in_summary_reward:
            print(f"  Примеры отсутствующих в SUMMARY: {list(missing_in_summary_reward)[:5]}")
        
        # TOURNAMENT-SCHEDULE vs SUMMARY
        print("\n4. TOURNAMENT-SCHEDULE vs SUMMARY:")
        ts_codes_in_data = set(ts_df['TOURNAMENT_CODE'].dropna().unique())
        ts_codes_in_summary = set(summary_df['TOURNAMENT_CODE'].dropna().unique())
        
        missing_in_ts = ts_codes_in_summary - ts_codes_in_data
        missing_in_summary_ts = ts_codes_in_data - ts_codes_in_summary
        
        print(f"  TOURNAMENT_CODE в TOURNAMENT-SCHEDULE: {len(ts_codes_in_data)}")
        print(f"  TOURNAMENT_CODE в SUMMARY: {len(ts_codes_in_summary)}")
        print(f"  Отсутствует в TOURNAMENT-SCHEDULE: {len(missing_in_ts)}")
        print(f"  Отсутствует в SUMMARY: {len(missing_in_summary_ts)}")
        
        if missing_in_ts:
            print(f"  Примеры отсутствующих в TOURNAMENT-SCHEDULE: {list(missing_in_ts)[:5]}")
        if missing_in_summary_ts:
            print(f"  Примеры отсутствующих в SUMMARY: {list(missing_in_summary_ts)[:5]}")
        
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    debug_missing_keys()
