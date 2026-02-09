# -*- coding: utf-8 -*-
"""
Загрузка и хранение конфигурации из config.json.
Все параметры обработки доступны через атрибуты класса Config.
"""

import json
import os
from typing import Any, Dict, List, Optional


class Config:
    """
    Конфигурация приложения из config.json.
    Пути вычисляются относительно каталога, в котором лежит config.json (корень проекта).
    """

    # Имя колонки связи наград (константа для сравнения/переименования)
    COL_REWARD_LINK_CONTEST_CODE: str = "REWARD_LINK => CONTEST_CODE"

    def __init__(self, config_path: Optional[str] = None) -> None:
        """
        Загружает config.json. Путь по умолчанию — config.json в каталоге выше src/ (корень проекта).
        """
        if config_path is None:
            _base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_path = os.path.join(_base_dir, "config.json")
        self._base_dir = os.path.dirname(config_path)
        with open(config_path, "r", encoding="utf-8") as f:
            self._cfg: Dict[str, Any] = json.load(f)

        # Пути
        self.dir_input: str = os.path.join(self._base_dir, self._cfg["paths"]["input"])
        self.dir_output: str = os.path.join(self._base_dir, self._cfg["paths"]["output"])
        self.dir_logs: str = os.path.join(self._base_dir, self._cfg["paths"]["logs"])

        # Логирование
        self.log_level: str = self._cfg["logging"]["level"]
        self.log_base_name: str = self._cfg["logging"]["base_name"]

        # Входные файлы и сводный лист
        self.input_files: List[Dict[str, Any]] = self._cfg["input_files"]
        self.summary_sheet: Dict[str, Any] = self._cfg["summary_sheet"]
        self.sheet_order: List[str] = self._cfg.get("sheet_order") or []

        # Ключевые колонки сводного листа (порядок из summary_key_defs)
        self.summary_key_defs: List[Dict[str, Any]] = self._cfg["summary_key_defs"]
        self.summary_key_columns: List[str] = []
        for _entry in self.summary_key_defs:
            for _col in _entry["cols"]:
                if _col not in self.summary_key_columns:
                    self.summary_key_columns.append(_col)

        # Пол (паттерны и шаг прогресса)
        self.gender_patterns: Dict[str, List[str]] = self._cfg["gender"]["patterns"]
        self.gender_progress_step: int = self._cfg["gender"].get("progress_step", 500)

        # Валидация длины полей
        self.field_length_validations: Dict[str, Any] = self._cfg.get("field_length_validations") or {}

        # Merge, цвета, форматы, дубликаты, JSON
        self.merge_fields_advanced: List[Dict[str, Any]] = self._cfg["merge_fields_advanced"]
        self.color_scheme: List[Dict[str, Any]] = self._cfg.get("color_scheme") or []
        self.column_formats: List[Dict[str, Any]] = self._cfg.get("column_formats") or []
        self.check_duplicates: List[Dict[str, Any]] = self._cfg.get("check_duplicates") or []
        self.json_columns: Dict[str, List[Dict[str, Any]]] = self._cfg.get("json_columns") or {}
        self.derived_columns: List[Dict[str, Any]] = self._cfg.get("derived_columns") or []

        # Параллелизм
        self.max_workers_io: int = self._cfg["performance"]["max_workers_io"]
        self.max_workers_cpu: int = self._cfg["performance"]["max_workers_cpu"]
        self.max_workers: int = self.max_workers_cpu

        # Статусы турнира
        _default_statuses = [
            "НЕОПРЕДЕЛЕН", "АКТИВНЫЙ", "ЗАПЛАНИРОВАН",
            "ПОДВЕДЕНИЕ ИТОГОВ", "ПОДВЕДЕНИЕ ИТОГОВ", "ПОДВЕДЕНИЕ ИТОГОВ", "ЗАВЕРШЕН",
        ]
        self.tournament_status_choices: List[str] = (
            self._cfg.get("tournament_status_choices") or _default_statuses
        )

    @property
    def base_dir(self) -> str:
        """Корень проекта (каталог с config.json)."""
        return self._base_dir

    def get_output_filename(self) -> str:
        """Имя выходного Excel с датой и временем."""
        from datetime import datetime
        return f"SPOD_ALL_IN_ONE_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
