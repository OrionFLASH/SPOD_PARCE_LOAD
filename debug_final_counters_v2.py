#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
from pathlib import Path

def debug_final_counters_v2():
    """Отладка оставшихся нулевых счетчиков после всех исправлений"""
    
    # Находим последний созданный файл
    out_dir = Path("/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT")
    excel_files = list(out_dir.glob("SPOD_ALL_IN_ONE_*.xlsx"))
    latest_file = max(excel_files, key=os.path.getctime)
    
    print(f"🔍 Отладка файла: {latest_file.name}")
    
    # Читаем все листы
    sheets = {
        'SUMMARY': pd.read_excel(latest_file, sheet_name='SUMMARY'),
        'INDICATOR': pd.read_excel(latest_file, sheet_name='INDICATOR'),
        'REPORT': pd.read_excel(latest_file, sheet_name='REPORT')
    }
    
    print(f"📊 SUMMARY: {len(sheets['SUMMARY'])} строк")
    print(f"📊 INDICATOR: {len(sheets['INDICATOR'])} строк")
    print(f"📊 REPORT: {len(sheets['REPORT'])} строк")
    
    # Проверяем проблемные счетчики
    problematic_counters = [
        'INDICATOR=>COUNT_INDICATOR_MARK_TYPE',
        'REPORT=>COUNT_CONTEST_DATE'
    ]
    
    for counter in problematic_counters:
        print(f"\n🔍 Анализ счетчика: {counter}")
        
        if counter in sheets['SUMMARY'].columns:
            values = sheets['SUMMARY'][counter]
            non_zero = (values != 0).sum()
            print(f"  📈 Ненулевых значений: {non_zero} из {len(values)}")
            
            if non_zero > 0:
                print(f"  📊 Примеры ненулевых значений: {values[values != 0].head().tolist()}")
            else:
                print(f"  ❌ Все значения равны 0")
                
                # Анализируем источник данных
                if counter == 'INDICATOR=>COUNT_INDICATOR_MARK_TYPE':
                    print(f"  🔍 Анализ INDICATOR:")
                    print(f"    - Уникальных CONTEST_CODE в INDICATOR: {sheets['INDICATOR']['CONTEST_CODE'].nunique()}")
                    print(f"    - Уникальных CONTEST_CODE в SUMMARY: {sheets['SUMMARY']['CONTEST_CODE'].nunique()}")
                    
                    # Проверяем пересечения
                    indicator_codes = set(sheets['INDICATOR']['CONTEST_CODE'].dropna())
                    summary_codes = set(sheets['SUMMARY']['CONTEST_CODE'].dropna())
                    intersection = indicator_codes.intersection(summary_codes)
                    print(f"    - Пересечений: {len(intersection)}")
                    
                    # Проверяем колонку INDICATOR_MARK_TYPE
                    print(f"    - Уникальных INDICATOR_MARK_TYPE в INDICATOR: {sheets['INDICATOR']['INDICATOR_MARK_TYPE'].nunique()}")
                    print(f"    - Примеры INDICATOR_MARK_TYPE: {sheets['INDICATOR']['INDICATOR_MARK_TYPE'].dropna().head().tolist()}")
                    
                elif counter == 'REPORT=>COUNT_CONTEST_DATE':
                    print(f"  🔍 Анализ REPORT по CONTEST_DATE:")
                    print(f"    - Уникальных CONTEST_CODE в REPORT: {sheets['REPORT']['CONTEST_CODE'].nunique()}")
                    print(f"    - Уникальных CONTEST_CODE в SUMMARY: {sheets['SUMMARY']['CONTEST_CODE'].nunique()}")
                    
                    # Проверяем пересечения
                    report_codes = set(sheets['REPORT']['CONTEST_CODE'].dropna())
                    summary_codes = set(sheets['SUMMARY']['CONTEST_CODE'].dropna())
                    intersection = report_codes.intersection(summary_codes)
                    print(f"    - Пересечений: {len(intersection)}")
                    
                    # Проверяем колонку CONTEST_DATE
                    print(f"    - Уникальных CONTEST_DATE в REPORT: {sheets['REPORT']['CONTEST_DATE'].nunique()}")
                    print(f"    - Примеры CONTEST_DATE: {sheets['REPORT']['CONTEST_DATE'].dropna().head().tolist()}")
        else:
            print(f"  ❌ Колонка не найдена в SUMMARY")

if __name__ == "__main__":
    debug_final_counters_v2()
