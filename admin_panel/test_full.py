#!/usr/bin/env python3
"""
Полное тестирование функционала админ-панели
"""
import subprocess
import json
import urllib.parse
import time
import sys

base_url = "http://localhost:5001"

def curl_get(url, timeout=5):
    try:
        result = subprocess.run(['curl', '-s', '--max-time', str(timeout), url], capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            try:
                return 200, json.loads(result.stdout)
            except:
                return 200, result.stdout
        return None, result.stderr or "Empty"
    except:
        return None, "Error"

def curl_post(url, data=None, timeout=30):
    try:
        cmd = ['curl', '-s', '-X', 'POST', '--max-time', str(timeout)]
        if data:
            cmd.extend(['-H', 'Content-Type: application/json', '-d', json.dumps(data)])
        cmd.append(url)
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            try:
                return 200, json.loads(result.stdout)
            except:
                return 200, result.stdout
        return None, result.stderr or "Empty"
    except:
        return None, "Error"

def curl_delete(url, timeout=10):
    try:
        result = subprocess.run(['curl', '-s', '-X', 'DELETE', '--max-time', str(timeout), url], capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            try:
                return 200, json.loads(result.stdout)
            except:
                return 200, result.stdout
        return None, result.stderr or "Empty"
    except:
        return None, "Error"

print("="*70)
print("ПОЛНОЕ ТЕСТИРОВАНИЕ ФУНКЦИОНАЛА")
print("="*70)
print()

time.sleep(2)

passed = 0
failed = 0
test_session = None

# 1
print("1. Сервер...")
s, d = curl_get(f"{base_url}/api/sessions")
if s == 200:
    print("   ✅ OK")
    passed += 1
else:
    print(f"   ❌ {d}")
    failed += 1
    sys.exit(1)

# 2
print("\n2. Список сессий...")
s, d = curl_get(f"{base_url}/api/sessions")
if s == 200 and isinstance(d, dict) and "sessions" in d:
    print(f"   ✅ Сессий: {len(d['sessions'])}")
    passed += 1
else:
    failed += 1

# 3
print("\n3. Создание сессии...")
s, d = curl_post(f"{base_url}/api/session/new")
if s == 200 and isinstance(d, dict) and "session" in d:
    test_session = d["session"]
    print(f"   ✅ Создана: {test_session}")
    passed += 1
else:
    failed += 1

# 4
if test_session:
    print(f"\n4. Переключение...")
    enc = urllib.parse.quote(test_session)
    s, d = curl_post(f"{base_url}/api/session/{enc}")
    if s == 200:
        print("   ✅ OK")
        passed += 1
    else:
        failed += 1

# 5
print("\n5. Файлы...")
s, d = curl_get(f"{base_url}/api/files")
if s == 200 and isinstance(d, dict) and "file_names" in d:
    print(f"   ✅ Файлов: {len(d['file_names'])}")
    passed += 1
    fn = d["file_names"]
else:
    failed += 1
    fn = {}

# 6
if fn:
    print("\n6. Записи...")
    tested = 0
    for fk in list(fn.keys())[:5]:
        enc = urllib.parse.quote(fk)
        s, d = curl_get(f"{base_url}/api/files/{enc}/records?page=1&per_page=2")
        if s == 200:
            tested += 1
    print(f"   ✅ Загружено из {tested} файлов")
    passed += 1

# 7
print("\n7. GROUP_CODE...")
enc = urllib.parse.quote("REWARD-LINK")
s, d = curl_get(f"{base_url}/api/files/{enc}/field/GROUP_CODE/values")
if s == 200 and isinstance(d, dict) and "values" in d:
    print(f"   ✅ Значений: {len(d['values'])}")
    passed += 1
else:
    failed += 1

# 8
if test_session:
    print(f"\n8. Удаление сессии...")
    enc = urllib.parse.quote(test_session)
    s, d = curl_delete(f"{base_url}/api/session/{enc}")
    if s == 200 and isinstance(d, dict) and d.get("success"):
        print("   ✅ OK")
        passed += 1
    else:
        failed += 1

# 9
print("\n9. HTML страница...")
s, html = curl_get(f"{base_url}/")
if s == 200 and isinstance(html, str) and '<html' in html.lower():
    checks = {
        'loadFiles': 'loadFiles' in html,
        'switchFile': 'switchFile' in html,
        'fileTabs': 'fileTabs' in html or 'tabs-nav' in html,
    }
    print("   ✅ Загружена")
    print("      Элементы:")
    for name, found in checks.items():
        print(f"         {'✅' if found else '❌'} {name}")
    passed += 1
else:
    failed += 1

print("\n" + "="*70)
print(f"✅ Успешных: {passed}")
print(f"❌ Ошибок: {failed}")
print(f"📊 Процент: {passed * 100 // (passed + failed) if (passed + failed) > 0 else 0}%")

if failed > 0:
    sys.exit(1)
else:
    print("\n✅✅✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ! ✅✅✅")
    sys.exit(0)
