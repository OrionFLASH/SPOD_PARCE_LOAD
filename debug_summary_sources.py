#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

def debug_summary_sources():
    """Отлаживает источники ключей в SUMMARY"""
    
    excel_file = "/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT/SPOD_ALL_IN_ONE_2025-10-09_12-45-17.xlsx"
    
    try:
        # Читаем все таблицы
        summary_df = pd.read_excel(excel_file, sheet_name='SUMMARY')
        reward_df = pd.read_excel(excel_file, sheet_name='REWARD')
        reward_link_df = pd.read_excel(excel_file, sheet_name='REWARD-LINK')
        
        print("=== АНАЛИЗ ИСТОЧНИКОВ REWARD_CODE В SUMMARY ===")
        
        # Проверяем REWARD_CODE в разных таблицах
        print(f"REWARD_CODE в REWARD: {len(reward_df['REWARD_CODE'].unique())}")
        print(f"Примеры из REWARD: {list(reward_df['REWARD_CODE'].unique()[:5])}")
        
        print(f"REWARD_CODE в REWARD-LINK: {len(reward_link_df['REWARD_CODE'].unique())}")
        print(f"Примеры из REWARD-LINK: {list(reward_link_df['REWARD_CODE'].unique()[:5])}")
        
        print(f"REWARD_CODE в SUMMARY: {len(summary_df['REWARD_CODE'].unique())}")
        print(f"Примеры из SUMMARY: {list(summary_df['REWARD_CODE'].unique()[:5])}")
        
        # Проверяем пересечения
        reward_codes_in_reward = set(reward_df['REWARD_CODE'].dropna().unique())
        reward_codes_in_link = set(reward_link_df['REWARD_CODE'].dropna().unique())
        reward_codes_in_summary = set(summary_df['REWARD_CODE'].dropna().unique())
        
        print(f"\nПересечение REWARD и REWARD-LINK: {len(reward_codes_in_reward & reward_codes_in_link)}")
        print(f"Пересечение REWARD и SUMMARY: {len(reward_codes_in_reward & reward_codes_in_summary)}")
        print(f"Пересечение REWARD-LINK и SUMMARY: {len(reward_codes_in_link & reward_codes_in_summary)}")
        
        # Проверяем, откуда берутся REWARD_CODE в SUMMARY
        print(f"\n=== ИСТОЧНИКИ REWARD_CODE В SUMMARY ===")
        
        # REWARD_CODE, которые есть в REWARD, но нет в SUMMARY
        only_in_reward = reward_codes_in_reward - reward_codes_in_summary
        print(f"Только в REWARD: {len(only_in_reward)}")
        if only_in_reward:
            print(f"Примеры: {list(only_in_reward)[:5]}")
        
        # REWARD_CODE, которые есть в SUMMARY, но нет в REWARD
        only_in_summary = reward_codes_in_summary - reward_codes_in_reward
        print(f"Только в SUMMARY: {len(only_in_summary)}")
        if only_in_summary:
            print(f"Примеры: {list(only_in_summary)[:5]}")
        
        # REWARD_CODE, которые есть в REWARD-LINK, но нет в REWARD
        only_in_link = reward_codes_in_link - reward_codes_in_reward
        print(f"Только в REWARD-LINK: {len(only_in_link)}")
        if only_in_link:
            print(f"Примеры: {list(only_in_link)[:5]}")
        
        # Проверяем CONTEST_CODE
        print(f"\n=== АНАЛИЗ CONTEST_CODE ===")
        contest_df = pd.read_excel(excel_file, sheet_name='CONTEST-DATA')
        contest_codes_in_data = set(contest_df['CONTEST_CODE'].dropna().unique())
        contest_codes_in_summary = set(summary_df['CONTEST_CODE'].dropna().unique())
        
        print(f"CONTEST_CODE в CONTEST-DATA: {len(contest_codes_in_data)}")
        print(f"CONTEST_CODE в SUMMARY: {len(contest_codes_in_summary)}")
        
        only_in_contest_data = contest_codes_in_data - contest_codes_in_summary
        only_in_summary_contest = contest_codes_in_summary - contest_codes_in_data
        
        print(f"Только в CONTEST-DATA: {len(only_in_contest_data)}")
        print(f"Только в SUMMARY: {len(only_in_summary_contest)}")
        if only_in_summary_contest:
            print(f"Примеры только в SUMMARY: {list(only_in_summary_contest)[:5]}")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_summary_sources()
