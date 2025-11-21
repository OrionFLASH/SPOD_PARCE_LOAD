#!/usr/bin/env python3
"""Автоматическое тестирование интерфейса админ-панели"""
import urllib.request
import json
from urllib.parse import quote

BASE = "http://localhost:5001"
files = ["CONTEST-DATA", "GROUP", "INDICATOR", "REPORT", "REWARD", "REWARD-LINK", 
         "ORG_UNIT_V20", "TOURNAMENT-SCHEDULE", "USER_ROLE", "USER_ROLE SB", "EMPLOYEE"]

print("="*70)
print("АВТОМАТИЧЕСКОЕ ТЕСТИРОВАНИЕ ИНТЕРФЕЙСА АДМИН-ПАНЕЛИ")
print("="*70)

# Тест 1: Все файлы
print("\n1. ТЕСТ: Чтение всех файлов")
ok_files = 0
for f in files:
    url = f"{BASE}/api/files/{quote(f, safe='')}/records?page=1&per_page=1"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            d = json.loads(r.read())
            if 'records' in d and len(d['records']) > 0:
                ok_files += 1
                print(f"   ✅ {f}")
            else:
                print(f"   ❌ {f}: нет записей")
    except Exception as e:
        print(f"   ❌ {f}: {str(e)[:50]}")

print(f"\n   Результат: {ok_files}/{len(files)} файлов работают")

# Тест 2: Поиск
print("\n2. ТЕСТ: Поиск по всем полям")
try:
    url = f"{BASE}/api/files/{quote('CONTEST-DATA', safe='')}/records?page=1&per_page=10&search=АКТИВНЫЙ"
    with urllib.request.urlopen(url, timeout=5) as r:
        d = json.loads(r.read())
        if 'records' in d:
            print(f"   ✅ Поиск работает: найдено {len(d['records'])} записей")
        else:
            print(f"   ❌ Поиск не работает")
except Exception as e:
    print(f"   ❌ Ошибка поиска: {e}")

# Тест 3: Фильтрация по полю
print("\n3. ТЕСТ: Фильтрация по полю (field:value)")
try:
    url = f"{BASE}/api/files/{quote('CONTEST-DATA', safe='')}/records?page=1&per_page=10&search=BUSINESS_STATUS:АКТИВНЫЙ"
    with urllib.request.urlopen(url, timeout=5) as r:
        d = json.loads(r.read())
        if 'records' in d:
            print(f"   ✅ Фильтрация работает: найдено {len(d['records'])} записей")
        else:
            print(f"   ❌ Фильтрация не работает")
except Exception as e:
    print(f"   ❌ Ошибка фильтрации: {e}")

# Тест 4: Получение одной записи
print("\n4. ТЕСТ: Получение одной записи")
try:
    url = f"{BASE}/api/files/{quote('CONTEST-DATA', safe='')}/records/0"
    with urllib.request.urlopen(url, timeout=5) as r:
        d = json.loads(r.read())
        if isinstance(d, dict) and len(d) > 0:
            print(f"   ✅ Получение записи работает: {len(d)} полей")
        else:
            print(f"   ❌ Получение записи не работает")
except Exception as e:
    print(f"   ❌ Ошибка: {e}")

# Тест 5: Структура JSON поля
print("\n5. ТЕСТ: Структура JSON поля")
try:
    url = f"{BASE}/api/files/{quote('REWARD', safe='')}/json-field/{quote('REWARD_ADD_DATA', safe='')}/structure"
    with urllib.request.urlopen(url, timeout=5) as r:
        d = json.loads(r.read())
        if 'common_fields' in d:
            print(f"   ✅ Структура JSON работает: {len(d.get('common_fields', {}))} полей")
        else:
            print(f"   ❌ Структура JSON не работает")
except Exception as e:
    print(f"   ❌ Ошибка: {e}")

print("\n" + "="*70)
print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
print("="*70)
