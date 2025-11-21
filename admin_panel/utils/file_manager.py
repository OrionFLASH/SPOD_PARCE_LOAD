"""
Менеджер файлов для работы с CSV данными
"""
import os
import logging

logger = logging.getLogger(__name__)
import shutil
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

class FileManager:
    """Управление файлами данных"""
    
    def __init__(self):
        # Пути относительно корня проекта (на уровень выше admin_panel)
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.input_dir = os.path.join(base_dir, config.DIR_INPUT)
        self.edit_dir = os.path.join(base_dir, config.DIR_EDIT)
        self.backup_dir = os.path.join(base_dir, config.DIR_BACKUP)
        self.current_edit_dir = None
        import logging
        self.logger = logging.getLogger(__name__)
        self.logger = logger
        
    def create_edit_session(self):
        """Создает новую сессию редактирования"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            self.current_edit_dir = os.path.join(self.edit_dir, timestamp)
            logger.info(f"Создание новой сессии: {os.path.basename(self.current_edit_dir)}")
            os.makedirs(self.current_edit_dir, exist_ok=True)
            
            # Копируем все файлы из SPOD
            copied_count = 0
            for key, filename in config.FILE_NAMES.items():
                src = os.path.join(self.input_dir, filename)
                dst = os.path.join(self.current_edit_dir, filename)
                if os.path.exists(src):
                    shutil.copy2(src, dst)
                    logger.debug(f"Скопирован файл: {filename}")
                    copied_count += 1
                else:
                    logger.warning(f"Файл не найден в SPOD: {filename}")
            
            logger.info(f"Сессия создана: скопировано {copied_count} файлов из {len(config.FILE_NAMES)}")
            return self.current_edit_dir
        except Exception as e:
            logger.exception(f"Ошибка создания сессии: {e}")
            raise
    
    def get_edit_session(self):
        """Получает текущую или последнюю сессию редактирования"""
        if not os.path.exists(self.edit_dir):
            return None
        
        # Ищем последнюю сессию
        sessions = [d for d in os.listdir(self.edit_dir) 
                   if os.path.isdir(os.path.join(self.edit_dir, d))]
        if not sessions:
            return None
        
        sessions.sort(reverse=True)
        self.current_edit_dir = os.path.join(self.edit_dir, sessions[0])
        return self.current_edit_dir
    
    def read_csv(self, file_key):
        """Читает CSV файл"""
        try:
            logger.debug(f"read_csv: file_key={file_key}")
            if not self.current_edit_dir:
                logger.debug("Нет текущей сессии, получаем последнюю")
                self.get_edit_session()
                if not self.current_edit_dir:
                    raise ValueError("Нет активной сессии редактирования")
            
            logger.debug(f"Текущая сессия: {self.current_edit_dir}")
            filename = config.FILE_NAMES.get(file_key)
            if not filename:
                raise ValueError(f"Неизвестный ключ файла: {file_key}")
            
            logger.debug(f"Имя файла: {filename}")
            # Формируем абсолютный путь
            if not os.path.isabs(self.current_edit_dir):
                base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                filepath = os.path.join(base_dir, self.current_edit_dir, filename)
            else:
                filepath = os.path.join(self.current_edit_dir, filename)
            
            logger.debug(f"Путь к файлу: {filepath}")
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"Файл не найден: {filepath}")
            
            logger.debug(f"Чтение CSV файла: {filepath}")
            df = pd.read_csv(filepath, sep=config.CSV_SEPARATOR, encoding=config.CSV_ENCODING, on_bad_lines='skip', engine='python')
            logger.debug(f"Файл прочитан: {len(df)} строк, {len(df.columns)} колонок")
            return df
        except Exception as e:
            logger.exception(f"Ошибка чтения файла {file_key}: {e}")
            raise
    
    def write_csv(self, file_key, df):
        """Записывает CSV файл"""
        if not self.current_edit_dir:
            raise ValueError("Нет активной сессии редактирования")
        
        filename = config.FILE_NAMES.get(file_key)
        if not filename:
            raise ValueError(f"Неизвестный ключ файла: {file_key}")
        
        # Формируем абсолютный путь
        if not os.path.isabs(self.current_edit_dir):
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            filepath = os.path.join(base_dir, self.current_edit_dir, filename)
        else:
            filepath = os.path.join(self.current_edit_dir, filename)
        
        # Создаем резервную копию
        backup_path = os.path.join(self.backup_dir, datetime.now().strftime("%Y%m%d_%H%M%S"))
        os.makedirs(backup_path, exist_ok=True)
        if os.path.exists(filepath):
            shutil.copy2(filepath, os.path.join(backup_path, filename))
        
        df.to_csv(filepath, sep=config.CSV_SEPARATOR, encoding=config.CSV_ENCODING, index=False)
        return filepath


    def delete_session(self, session_name: str) -> dict:
        """Удаление сессии редактирования"""
        try:
            session_path = os.path.join(self.edit_dir, session_name)
            
            if not os.path.exists(session_path):
                return {'error': f'Сессия {session_name} не найдена'}
            
            # Если это текущая сессия, сбрасываем
            if self.current_edit_dir == session_path:
                self.current_edit_dir = None
            
            # Удаляем каталог
            shutil.rmtree(session_path)
            
            self.logger.info(f"Сессия {session_name} удалена")
            
            return {'success': True, 'session': session_name}
        except Exception as e:
            self.logger.error(f"Ошибка удаления сессии {session_name}: {e}")
            return {'error': str(e)}
