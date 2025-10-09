#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
from pathlib import Path

def check_count_columns():
    """Проверяем колонки счетчиков в SUMMARY листе"""
    
    # Находим последний созданный файл
    out_dir = Path("/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT")
    excel_files = list(out_dir.glob("SPOD_ALL_IN_ONE_*.xlsx"))
    
    if not excel_files:
        print("❌ Файлы Excel не найдены в папке OUT")
        return
    
    latest_file = max(excel_files, key=os.path.getctime)
    print(f"📊 Анализируем файл: {latest_file.name}")
    
    try:
        # Читаем SUMMARY лист
        df = pd.read_excel(latest_file, sheet_name='SUMMARY')
        print(f"📋 SUMMARY лист: {len(df)} строк, {len(df.columns)} колонок")
        
        # Находим все колонки счетчиков
        count_columns = [col for col in df.columns if '=>COUNT_' in col]
        print(f"\n🔢 Найдено колонок счетчиков: {len(count_columns)}")
        
        if not count_columns:
            print("❌ Колонки счетчиков не найдены!")
            return
        
        print("\n📈 Статистика по счетчикам:")
        print("=" * 80)
        
        for col in sorted(count_columns):
            values = df[col].dropna()
            non_zero = (values > 0).sum()
            zero_count = (values == 0).sum()
            total = len(values)
            
            if total > 0:
                non_zero_pct = (non_zero / total) * 100
                print(f"{col:50} | Всего: {total:4d} | Ненулевых: {non_zero:4d} ({non_zero_pct:5.1f}%) | Нулевых: {zero_count:4d}")
            else:
                print(f"{col:50} | Нет данных")
        
        print("\n🎯 Детальная статистика по каждому счетчику:")
        print("=" * 80)
        
        for col in sorted(count_columns):
            values = df[col].dropna()
            if len(values) > 0:
                print(f"\n{col}:")
                print(f"  Минимум: {values.min()}")
                print(f"  Максимум: {values.max()}")
                print(f"  Среднее: {values.mean():.2f}")
                print(f"  Медиана: {values.median():.2f}")
                print(f"  Ненулевых значений: {(values > 0).sum()}")
                print(f"  Нулевых значений: {(values == 0).sum()}")
                
                # Показываем примеры ненулевых значений
                non_zero_values = values[values > 0]
                if len(non_zero_values) > 0:
                    print(f"  Примеры ненулевых значений: {sorted(non_zero_values.unique())[:10]}")
        
        # Проверяем конкретные счетчики, которые должны работать
        expected_counters = [
            'CONTEST-DATA=>COUNT_CONTEST_CODE',
            'GROUP=>COUNT_CONTEST_CODE', 
            'GROUP=>COUNT_CONTEST_CODE',
            'INDICATOR=>COUNT_CONTEST_CODE',
            'REWARD=>COUNT_REWARD_CODE',
            'REWARD-LINK=>COUNT_CONTEST_CODE',
            'TOURNAMENT-SCHEDULE=>COUNT_CONTEST_CODE',
            'TOURNAMENT-SCHEDULE=>COUNT_TOURNAMENT_CODE',
            'TOURNAMENT-SCHEDULE=>COUNT_CONTEST_CODE'
        ]
        
        print(f"\n✅ Проверка ожидаемых счетчиков:")
        print("=" * 80)
        
        for expected in expected_counters:
            if expected in df.columns:
                non_zero = (df[expected] > 0).sum()
                total = len(df[expected].dropna())
                status = "✅" if non_zero > 0 else "❌"
                print(f"{status} {expected:50} | Ненулевых: {non_zero:4d} из {total:4d}")
            else:
                print(f"❌ {expected:50} | Колонка не найдена")
        
    except Exception as e:
        print(f"❌ Ошибка при анализе файла: {e}")

if __name__ == "__main__":
    check_count_columns()
