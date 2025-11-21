#!/usr/bin/env python3
"""Тестирование всех операций админ-панели"""
import requests
import json
import sys

BASE_URL = "http://localhost:5001"

def test_read_records(file_key, page=1, per_page=5):
    """Тест чтения записей"""
    print(f"\n=== Тест: Чтение записей {file_key} ===")
    try:
        response = requests.get(f"{BASE_URL}/api/files/{file_key}/records", 
                              params={"page": page, "per_page": per_page})
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Успешно: {len(data.get('records', []))} записей")
            return data.get('records', [])
        else:
            print(f"❌ Ошибка {response.status_code}: {response.text[:200]}")
            return None
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return None

def test_get_record(file_key, record_id=0):
    """Тест получения одной записи"""
    print(f"\n=== Тест: Получение записи {record_id} из {file_key} ===")
    try:
        response = requests.get(f"{BASE_URL}/api/files/{file_key}/records/{record_id}")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Успешно: получена запись")
            return data
        else:
            print(f"❌ Ошибка {response.status_code}: {response.text[:200]}")
            return None
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return None

def test_create_record(file_key, record_data):
    """Тест создания записи"""
    print(f"\n=== Тест: Создание записи в {file_key} ===")
    try:
        response = requests.post(f"{BASE_URL}/api/files/{file_key}/records", 
                               json=record_data)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Успешно: создана запись ID {data.get('id')}")
            return data.get('id')
        else:
            print(f"❌ Ошибка {response.status_code}: {response.text[:200]}")
            return None
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return None

def test_update_record(file_key, record_id, record_data):
    """Тест обновления записи"""
    print(f"\n=== Тест: Обновление записи {record_id} в {file_key} ===")
    try:
        response = requests.put(f"{BASE_URL}/api/files/{file_key}/records/{record_id}", 
                              json=record_data)
        if response.status_code == 200:
            print(f"✅ Успешно: запись обновлена")
            return True
        else:
            print(f"❌ Ошибка {response.status_code}: {response.text[:200]}")
            return False
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return False

def test_delete_record(file_key, record_id):
    """Тест удаления записи"""
    print(f"\n=== Тест: Удаление записи {record_id} из {file_key} ===")
    try:
        response = requests.delete(f"{BASE_URL}/api/files/{file_key}/records/{record_id}")
        if response.status_code == 200:
            print(f"✅ Успешно: запись удалена")
            return True
        else:
            print(f"❌ Ошибка {response.status_code}: {response.text[:200]}")
            return False
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return False

if __name__ == "__main__":
    print("=== Тестирование админ-панели ===")
    
    # Тест 1: Чтение записей
    records = test_read_records("CONTEST-DATA", 1, 3)
    
    if records and len(records) > 0:
        # Тест 2: Получение одной записи
        record = test_get_record("CONTEST-DATA", 0)
        
        if record:
            # Тест 3: Обновление записи (используем первую запись)
            # Создаем копию для обновления
            update_data = record.copy()
            # Меняем одно поле для теста
            if 'CONTEST_DESCRIPTION' in update_data:
                original = update_data['CONTEST_DESCRIPTION']
                update_data['CONTEST_DESCRIPTION'] = original + " [TEST]"
                test_update_record("CONTEST-DATA", 0, update_data)
                # Возвращаем обратно
                update_data['CONTEST_DESCRIPTION'] = original
                test_update_record("CONTEST-DATA", 0, update_data)
    
    print("\n=== Тестирование завершено ===")
