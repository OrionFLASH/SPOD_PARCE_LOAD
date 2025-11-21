"""
Редактор JSON полей
"""
import json
import pandas as pd
from .file_manager import FileManager
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

class JSONEditor:
    """Редактирование JSON полей"""
    
    def __init__(self):
        self.file_manager = FileManager()
    
    def get_json_field(self, file_key, record_id, field):
        """Получает JSON поле"""
        df = self.file_manager.read_csv(file_key)
        if record_id < 0 or record_id >= len(df):
            raise ValueError("Запись не найдена")
        
        value = df.at[record_id, field]
        if pd.isna(value) or not str(value).strip():
            return None
        
        try:
            return json.loads(str(value))
        except:
            return None
    
    def set_json_field(self, file_key, record_id, field, json_data):
        """Устанавливает JSON поле"""
        df = self.file_manager.read_csv(file_key)
        if record_id < 0 or record_id >= len(df):
            raise ValueError("Запись не найдена")
        
        # Валидация JSON
        if json_data is None:
            df.at[record_id, field] = ""
        else:
            json_str = json.dumps(json_data, ensure_ascii=False)
            df.at[record_id, field] = json_str
        
        self.file_manager.write_csv(file_key, df)
        return record_id
    
    def is_json_field(self, file_key, field):
        """Проверяет, является ли поле JSON"""
        return file_key in config.JSON_FIELDS and field in config.JSON_FIELDS[file_key]
