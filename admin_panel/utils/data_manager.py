"""
Менеджер данных для работы с записями
"""
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def safe_json_loads(value, field_name=None, record_id=None):
    """Безопасный парсинг JSON с подробным логированием"""
    if pd.isna(value) or value is None:
        return None
    if not isinstance(value, str):
        return value
    if not value.strip():
        return None
    try:
        result = json.loads(value)
        return result
    except json.JSONDecodeError as e:
        logger.debug(f"[safe_json_loads] JSON decode error (не критично, возвращаем строку): field={field_name}, record_id={record_id}, value={str(value)[:200]}...")
        logger.debug(f"  Ошибка: {e} (позиция {e.pos if hasattr(e, 'pos') else 'unknown'})")
        # Возвращаем исходное значение если не JSON
        return value
    except Exception as e:
        logger.error(f"[safe_json_loads] Неожиданная ошибка при парсинге JSON: field={field_name}, record_id={record_id}, error={str(e)}", exc_info=True)
        logger.debug(f"  Значение: {str(value)[:200]}...")
        return value

import json
from .file_manager import FileManager
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

class DataManager:
    """Управление данными"""
    
    def __init__(self):
        self.file_manager = FileManager()
    
    def get_records(self, file_key, page=1, per_page=50, search=None, sort_by=None, sort_order="asc"):
        """Получает записи с пагинацией"""
        try:
            logger.debug(f"get_records: file_key={file_key}, page={page}, per_page={per_page}")
            df = self.file_manager.read_csv(file_key)
            logger.debug(f"Файл прочитан: {len(df)} строк, {len(df.columns)} колонок")
            
            # Поиск по всем полям или по конкретному полю
            if search:
                logger.debug(f"Поиск: {search}")
                # Если search содержит ":", то это фильтр по полю (field:value)
                if ':' in search:
                    parts = search.split(':', 1)
                    field_name = parts[0].strip()
                    search_value = parts[1].strip()
                    if field_name in df.columns:
                        mask = df[field_name].astype(str).str.contains(search_value, case=False, na=False)
                        df = df[mask]
                        logger.debug(f"Фильтр по полю {field_name}: {len(df)} строк")
                    else:
                        logger.warning(f"Поле {field_name} не найдено")
                else:
                    # Поиск по всем полям
                    mask = df.astype(str).apply(lambda x: x.str.contains(search, case=False, na=False)).any(axis=1)
                    df = df[mask]
                    logger.debug(f"Поиск по всем полям: {len(df)} строк")
            
            # Сортировка
            if sort_by and sort_by in df.columns:
                logger.debug(f"Сортировка: {sort_by} {sort_order}")
                df = df.sort_values(by=sort_by, ascending=(sort_order == "asc"))
            
            total = len(df)
            logger.debug(f"Всего записей: {total}")
            
            # Пагинация
            start = (page - 1) * per_page
            end = start + per_page
            df_page = df.iloc[start:end]
            logger.debug(f"Страница {page}: строки {start}-{end}")
            
            # Конвертация в словари
                        # Конвертация в словари с обработкой ошибок
            try:
                # Сохраняем порядок колонок
                column_order = list(df.columns)
                records = df_page.to_dict('records')
                # Обрабатываем NaN и другие проблемные значения
                for record in records:
                    for key, value in record.items():
                        if pd.isna(value):
                            record[key] = None
            except Exception as e:
                logger.exception(f"Ошибка конвертации в словари: {e}")
                raise
            logger.debug(f"Конвертировано {len(records)} записей")
            
            return {
                "records": records,
                "total": total,
                "page": page,
                "per_page": per_page,
                "pages": (total + per_page - 1) // per_page
            }
        except Exception as e:
            logger.exception(f"Ошибка в get_records для {file_key}: {e}")
            raise

    def get_record(self, file_key, record_id):
        """Получает одну запись по ID"""
        try:
            logger.debug(f"get_record: file_key={file_key}, record_id={record_id}")
            df = self.file_manager.read_csv(file_key)
            logger.debug(f"Файл прочитан: {len(df)} строк")
            
            if record_id < 0 or record_id >= len(df):
                logger.warning(f"Запись {record_id} не найдена в {file_key}")
                return None
            
            record = df.iloc[record_id].to_dict()
            logger.debug(f"Запись получена: {len(record)} полей")
            return record
        except Exception as e:
            logger.error(f"[get_record] Ошибка получения записи {record_id} из {file_key}: {str(e)}", exc_info=True)
            logger.exception(f"Ошибка в get_record для {file_key}, record_id={record_id}: {e}")
            raise
