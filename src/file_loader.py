# -*- coding: utf-8 -*-
"""
Поиск и загрузка CSV-файлов: поиск без учёта регистра, чтение, разворот JSON.
"""

import csv
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from src.config_loader import Config
from src.json_utils import flatten_json_column_recursive


class FileLoader:
    """Загрузка входных CSV: поиск файла, чтение, разворот JSON-колонок по конфигу."""

    def __init__(self, config: Config) -> None:
        self.config = config

    def find_file_case_insensitive(
        self, directory: str, base_name: str, extensions: List[str]
    ) -> Optional[str]:
        """
        Ищет файл в каталоге без учёта регистра имени и расширения.
        """
        if not os.path.exists(directory):
            return None
        name_stem, name_ext = os.path.splitext(base_name)
        if name_ext and name_ext.lower() in [e.lower() for e in extensions]:
            match_stem = name_stem.lower()
            match_ext = name_ext.lower()
            match_full_name = True
        else:
            match_stem = base_name.lower()
            match_ext = None
            match_full_name = False
        try:
            files_in_dir = os.listdir(directory)
        except OSError:
            return None
        for file_name in files_in_dir:
            name, ext = os.path.splitext(file_name)
            if match_full_name:
                if name.lower() == match_stem and ext.lower() == match_ext:
                    return os.path.join(directory, file_name)
            else:
                if name.lower() == match_stem and ext.lower() in [e.lower() for e in extensions]:
                    return os.path.join(directory, file_name)
        return None

    def check_input_files_exist(self) -> List[Dict[str, str]]:
        """
        Проверяет наличие всех файлов из config.input_files в config.dir_input.
        Возвращает список ненайденных: [{"file": "...", "sheet": "..."}, ...].
        """
        missing: List[Dict[str, str]] = []
        for file_conf in self.config.input_files:
            base_name = file_conf["file"]
            sheet_name = file_conf["sheet"]
            path = self.find_file_case_insensitive(
                self.config.dir_input, base_name, [".csv", ".CSV"]
            )
            if path is None:
                missing.append({"file": base_name, "sheet": sheet_name})
        return missing

    def read_csv_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Читает CSV с разделителем ';', quoting=csv.QUOTE_NONE, все значения как строки.
        """
        func_start = time.time()
        params = f"({file_path})"
        logging.info(f"[START] read_csv_file {params}")
        try:
            rows: List[List[str]] = []
            headers: Optional[List[str]] = None
            with open(file_path, "r", encoding="utf-8", newline="") as file:
                csv_reader = csv.reader(file, delimiter=";", quoting=csv.QUOTE_NONE)
                for i, row in enumerate(csv_reader):
                    if i == 0:
                        headers = row
                    else:
                        rows.append(row)
            if headers is None:
                return None
            df = pd.DataFrame(rows, columns=headers)
            for col in df.columns:
                df[col] = df[col].astype(str)
            for col in df.columns:
                if "FEATURE" in col or "ADD_DATA" in col:
                    logging.debug(
                        f"[DEBUG] CSV {file_path} поле {col}: {df[col].dropna().head(2).to_list()}"
                    )
            logging.info(
                f"Файл успешно загружен: {file_path}, строк: {len(df)}, колонок: {len(df.columns)}"
            )
            return df
        except Exception as e:
            func_time = time.time() - func_start
            logging.error(f"Ошибка загрузки файла: {file_path}. {e}")
            logging.info(f"[END] read_csv_file {params} (время: {func_time:.3f}s)")
            return None

    def process_single_file(
        self, file_conf: Dict[str, Any]
    ) -> Tuple[Optional[pd.DataFrame], str, Optional[Dict[str, Any]]]:
        """
        Обрабатывает один CSV: поиск, чтение, разворот JSON-полей по config.json_columns.
        Возвращает (df, sheet_name, file_conf) или (None, sheet_name, None) при ошибке.
        """
        sheet_name = file_conf["sheet"]
        try:
            file_path = self.find_file_case_insensitive(
                self.config.dir_input, file_conf["file"], [".csv", ".CSV"]
            )
            if file_path is None and sheet_name == "LIST-TOURNAMENT" and file_conf.get("file") == "gamification-tournamentList-2":
                file_path = self.find_file_case_insensitive(
                    self.config.dir_input, "gamification-tournamentList", [".csv", ".CSV"]
                )
                if file_path:
                    logging.info(
                        f"LIST-TOURNAMENT: использован файл по альтернативному имени: {file_path}"
                    )
            if file_path is None:
                th = threading.current_thread().name
                logging.error(
                    f"Файл не найден: {file_conf['file']} в каталоге {self.config.dir_input} [поток: {th}]"
                )
                return None, sheet_name, None

            th = threading.current_thread().name
            logging.info(f"Загрузка файла: {file_path} [поток: {th}]")

            df = self.read_csv_file(file_path)
            if df is None:
                logging.error(f"Ошибка чтения файла: {file_path} [поток: {th}]")
                return None, sheet_name, None

            json_columns = self.config.json_columns.get(sheet_name, [])
            for json_conf in json_columns:
                col = json_conf["column"]
                prefix = json_conf.get("prefix", col)
                if col in df.columns:
                    df = flatten_json_column_recursive(
                        df,
                        col,
                        prefix=prefix,
                        sheet=sheet_name,
                        max_workers_io=self.config.max_workers_io,
                    )
                    logging.info(
                        f"[JSON FLATTEN] {sheet_name}: поле '{col}' развернуто с префиксом '{prefix}' [поток: {th}]"
                    )
                else:
                    logging.warning(
                        f"[JSON FLATTEN] {sheet_name}: поле '{col}' не найдено в колонках! [поток: {th}]"
                    )
            logging.debug(
                f"[DEBUG] {sheet_name}: колонки после разворачивания: {', '.join(df.columns.tolist())} [поток: {th}]"
            )
            logging.info(f"Файл успешно обработан: {sheet_name}, строк: {len(df)} [поток: {th}]")
            return df, sheet_name, file_conf
        except Exception as e:
            logging.error(
                f"Ошибка обработки файла {file_conf.get('file', 'unknown')}: {e} [поток: {threading.current_thread().name}]"
            )
            return None, sheet_name, None
