# -*- coding: utf-8 -*-
"""
Основной пайплайн SPOD (запуск: main.py → main()).

Блок PROM / IFT / PSI: чтение CSV и разворот JSON → проверки консистентности → AUTO_GENDER,
статус турниров, merge_fields_advanced → SUMMARY → запись Excel (main / source / consistency /
MANAGER_STATS). Коды возврата main(): 0 — успех, 1 — ошибки обработки/записи, 2 — нет входных файлов.

История оптимизаций и параллелизма: Docs/PERFORMANCE_AND_PARALLELIZATION_HISTORY.md;
разбор и план рефакторинга: Docs/REFACTORING_REVIEW_2026-09-26.json.
"""
# === ИМПОРТЫ БИБЛИОТЕК ===
import os          # Для работы с операционной системой и путями
import sys         # Для системных функций и аргументов командной строки
from collections import defaultdict
from typing import Optional, List, Dict, Any, Tuple, Set, Mapping, Sequence  # Для аннотаций типов
import pandas as pd  # Для работы с данными в табличном формате
import logging     # Для логирования процессов
from datetime import datetime  # Для работы с датами и временем
from datetime import date as date_cls  # Тип даты (расчёт ширины колонок, разбор дат)
import numpy as np  # Типы значений при расчёте ширины колонок (PERF-04)
from openpyxl.utils import get_column_letter  # Для получения буквенного обозначения колонок Excel
from openpyxl.styles import Alignment, Font, PatternFill  # Для стилизации ячеек Excel
from openpyxl.styles.cell_style import StyleArray  # PERF-01: копирование индекса стиля
import json        # Для работы с JSON данными
import csv         # Для работы с CSV файлами
from concurrent.futures import ThreadPoolExecutor, as_completed  # Для параллельной обработки
import threading  # Для синхронизации потоков
import copy  # Копия конфигов листов для синтетических агрегированных листов
import functools  # lru_cache нормализации имён колонок (PERF-05)

from src import console_ui  # Краткий вывод этапов и сводок в консоль (stdlib)
from src.block_runtime import (
    BlockLogFilter,
    console_print_lines,
    resolve_block_placeholders,
    set_current_block,
)
from src.config_loader import (
    default_config_path,
    get_input_files_for_block,
    parse_input_files_by_block,
    parse_run_blocks_config,
    parse_run_blocks_parallel,
    parse_run_outputs_config,
    parse_run_outputs_for_block,
    parse_skip_data_alignment_sheets,
    resolve_output_filename_template,
    sheet_skips_data_alignment,
)  # Разбор run_outputs / run_blocks / шаблоны имён / skip Alignment
from src.consistency_checks import run_consistency_checks_and_attach_summary  # Проверки консистентности (отдельный модуль)
from src.json_utils import safe_json_loads  # Единая реализация разбора JSON (BUG-03)
from src.runtime_env import environment_summary  # Версии Python/пакетов и платформа — в лог при старте
from src.debug_timing import (
    debug_phase,
    debug_timed,
    get_run_summary_for_console,
    reset_run_timing,
    run_elapsed_sec,
    set_debug_phase_console_hooks,
    write_performance_statistics_excel,
)  # DEBUG [PERF] и отдельный Excel «STAT_FILE <таймштамп>.xlsx» со временем этапов и функций
import warnings   # Для подавления UserWarning при парсинге дат без формата


# === Ошибки пайплайна и коды возврата (BUG-01, BUG-06, LOG-02) ===
EXIT_OK = 0                # успех
EXIT_PROCESSING_ERROR = 1  # ошибка обработки/записи (исключение в блоке или ERROR в логе)
EXIT_MISSING_INPUT = 2     # нет входных файлов блока


class OutputWriteError(Exception):
    """Не удалось записать выходной файл (сообщение — для консоли пользователя)."""


class MissingInputFilesError(Exception):
    """Во входном каталоге нет файлов из input_files блока."""

    def __init__(self, message_lines: List[str]) -> None:
        super().__init__("\n".join(message_lines))
        self.message_lines = message_lines


# ENV-02: на Windows без включённых длинных путей полный путь ограничен MAX_PATH = 260 символов
_LONG_PATH_WARN_LEN = 240


def _warn_if_long_path(path: str) -> None:
    """WARNING, если полный путь выходного файла близок к пределу Windows (MAX_PATH = 260)."""
    full = os.path.abspath(path)
    if len(full) > _LONG_PATH_WARN_LEN:
        logging.warning(
            f"[write] Длинный путь ({len(full)} символов > {_LONG_PATH_WARN_LEN}): на Windows запись может "
            f"не удаться (MAX_PATH = 260). Перенесите проект ближе к корню диска. Путь: {full}"
        )


def _remove_partial_file(path: str) -> None:
    """Удалить недописанный выходной файл после ошибки записи (если он успел появиться)."""
    try:
        if path and os.path.isfile(path):
            os.remove(path)
            logging.info(f"[write] Удалён недописанный файл: {path}")
    except OSError:
        logging.warning(f"[write] Не удалось удалить недописанный файл: {path}", exc_info=True)


# === ЗАГРУЗКА КОНФИГУРАЦИИ ИЗ config.json или из внедрённого Config ===
def _load_config_globals():
    """Устанавливает глобальные переменные из Config (внедрённый) или из config.json."""
    global DIR_INPUT, DIR_OUTPUT, DIR_LOGS, LOG_LEVEL, LOG_BASE_NAME, LOG_RETENTION_DAYS, INPUT_FILES, RUN_MODE
    global ALL_INPUT_FILES, INPUT_FILES_BY_BLOCK, RUN_BLOCKS, CURRENT_RUN_BLOCK
    global RUN_BLOCKS_PARALLEL, CFG_RAW, CONFIG_PATH
    global RUN_OUTPUTS, RUN_SOURCE_ONLY_EXIT, RUN_WRITE_SOURCE, RUN_WRITE_MAIN
    global RUN_WRITE_CONSISTENCY_FILE, RUN_CONSISTENCY_EARLY
    global RUN_WRITE_MANAGER_STATS, MANAGER_STATS_EARLY
    global RUN_WRITE_STAT_FILE
    global RUN_RATING_ITEM_MATRIX, RUN_SEASON_ORDER_SUMMARY
    global OUTPUT_FILENAME_MAIN, OUTPUT_FILENAME_SOURCE, OUTPUT_FILENAME_CONSISTENCY
    global OUTPUT_FILENAME_MANAGER_STATS
    global OUTPUT_FILENAME_MAIN_TEMPLATE, OUTPUT_FILENAME_SOURCE_TEMPLATE
    global OUTPUT_FILENAME_CONSISTENCY_TEMPLATE, OUTPUT_FILENAME_MANAGER_STATS_TEMPLATE
    global APPLY_SORT_TO_SOURCE, APPLY_SORT_TO_MAIN
    global SUMMARY_SHEET, SHEET_ORDER, SUMMARY_KEY_DEFS, SUMMARY_KEY_COLUMNS
    global GENDER_PATTERNS, GENDER_PROGRESS_STEP
    global COL_REWARD_LINK_CONTEST_CODE, MERGE_FIELDS_ADVANCED, COLOR_SCHEME
    global COLUMN_FORMATS, CONSISTENCY_CHECKS, JSON_COLUMNS, REWARD_GETCONDITION_SUMMARY
    global MAX_WORKERS_IO, MAX_WORKERS_CPU, MAX_WORKERS, TOURNAMENT_STATUS_CHOICES
    global SOURCE_EXPORT_SORT
    global INPUT_ARCHIVE_SQLITE, PROJECT_BASE_DIR, RATING_ITEM_MATRIX, SEASON_ORDER_SUMMARY
    global MANAGER_STATS
    global SKIP_DATA_ALIGNMENT_SHEETS, EXCEL_WRITER

    from src.config_holder import get_current_config

    _c = get_current_config()
    try:
        if _c is not None:
            _BASE_DIR = _c.base_dir
            DIR_INPUT = _c.dir_input
            DIR_OUTPUT = _c.dir_output
            DIR_LOGS = _c.dir_logs
            LOG_LEVEL = _c.log_level
            LOG_BASE_NAME = _c.log_base_name
            LOG_RETENTION_DAYS = getattr(_c, "log_retention_days", 0)
            INPUT_FILES_BY_BLOCK = dict(
                getattr(_c, "input_files_by_block", None)
                or parse_input_files_by_block({"input_files": _c.input_files})
            )
            ALL_INPUT_FILES = INPUT_FILES_BY_BLOCK  # алиас: разделы по блокам
            RUN_BLOCKS = list(getattr(_c, "run_blocks", None) or ["PROM"])
            RUN_BLOCKS_PARALLEL = bool(getattr(_c, "run_blocks_parallel", False))
            CFG_RAW = dict(getattr(_c, "_cfg", {}) or {})
            CONFIG_PATH = str(getattr(_c, "config_path", "") or "")
            CURRENT_RUN_BLOCK = RUN_BLOCKS[0] if RUN_BLOCKS else "PROM"
            INPUT_FILES = get_input_files_for_block(INPUT_FILES_BY_BLOCK, CURRENT_RUN_BLOCK)
            SUMMARY_SHEET = _c.summary_sheet
            SHEET_ORDER = _c.sheet_order
            SUMMARY_KEY_DEFS = _c.summary_key_defs
            SUMMARY_KEY_COLUMNS = list(_c.summary_key_columns)
            GENDER_PATTERNS = _c.gender_patterns
            GENDER_PROGRESS_STEP = getattr(_c, "gender_progress_step", 500)
            COL_REWARD_LINK_CONTEST_CODE = "REWARD_LINK => CONTEST_CODE"
            MERGE_FIELDS_ADVANCED = _c.merge_fields_advanced
            COLOR_SCHEME = _c.color_scheme
            COLUMN_FORMATS = _c.column_formats
            CONSISTENCY_CHECKS = getattr(_c, "consistency_checks", None) or {"summary_sheet_name": "CONSISTENCY", "rules": [], "csv_columns_count": {}}
            RUN_MODE = getattr(_c, "run_mode", 1)
            # Экземпляр Config после добавления run_outputs; иначе — только legacy run_mode
            if hasattr(_c, "run_write_main"):
                RUN_OUTPUTS = list(getattr(_c, "run_outputs", []))
                RUN_SOURCE_ONLY_EXIT = bool(_c.run_source_only_exit)
                RUN_WRITE_SOURCE = bool(_c.run_write_source)
                RUN_WRITE_MAIN = bool(_c.run_write_main)
                RUN_WRITE_CONSISTENCY_FILE = bool(_c.run_write_consistency_file)
                RUN_CONSISTENCY_EARLY = bool(_c.run_consistency_early)
                RUN_WRITE_MANAGER_STATS = bool(getattr(_c, "run_write_manager_stats", False))
                MANAGER_STATS_EARLY = bool(getattr(_c, "run_manager_stats_early", False))
                RUN_WRITE_STAT_FILE = bool(getattr(_c, "run_write_stat_file", False))
                RUN_RATING_ITEM_MATRIX = bool(getattr(_c, "run_rating_item_matrix", False))
                RUN_SEASON_ORDER_SUMMARY = bool(getattr(_c, "run_season_order_summary", False))
            else:
                _ro = parse_run_outputs_config({"run_mode": RUN_MODE})
                RUN_OUTPUTS = list(_ro[0])
                RUN_SOURCE_ONLY_EXIT = _ro[1]
                RUN_WRITE_SOURCE = _ro[2]
                RUN_WRITE_MAIN = _ro[3]
                RUN_WRITE_CONSISTENCY_FILE = _ro[4]
                RUN_CONSISTENCY_EARLY = _ro[5]
                RUN_WRITE_MANAGER_STATS = _ro[6]
                MANAGER_STATS_EARLY = _ro[7]
                RUN_WRITE_STAT_FILE = _ro[8]
                RUN_RATING_ITEM_MATRIX = _ro[10]
                RUN_SEASON_ORDER_SUMMARY = _ro[11]
                RUN_MODE = _ro[9]
            OUTPUT_FILENAME_MAIN_TEMPLATE = getattr(
                _c, "output_filename_main_template", getattr(_c, "output_filename_main", "SPOD_{BLOCK} main")
            )
            OUTPUT_FILENAME_SOURCE_TEMPLATE = getattr(
                _c, "output_filename_source_template", getattr(_c, "output_filename_source", "SPOD_{BLOCK} source")
            )
            OUTPUT_FILENAME_CONSISTENCY_TEMPLATE = getattr(
                _c,
                "output_filename_consistency_template",
                getattr(_c, "output_filename_consistency", "SPOD_{BLOCK} consistency"),
            )
            OUTPUT_FILENAME_MANAGER_STATS_TEMPLATE = getattr(
                _c,
                "output_filename_manager_stats_template",
                getattr(_c, "output_filename_manager_stats", "SPOD_{BLOCK} MANAGER_STATS"),
            )
            OUTPUT_FILENAME_MAIN = resolve_output_filename_template(
                OUTPUT_FILENAME_MAIN_TEMPLATE, CURRENT_RUN_BLOCK
            )
            OUTPUT_FILENAME_SOURCE = resolve_output_filename_template(
                OUTPUT_FILENAME_SOURCE_TEMPLATE, CURRENT_RUN_BLOCK
            )
            OUTPUT_FILENAME_CONSISTENCY = resolve_output_filename_template(
                OUTPUT_FILENAME_CONSISTENCY_TEMPLATE, CURRENT_RUN_BLOCK
            )
            OUTPUT_FILENAME_MANAGER_STATS = resolve_output_filename_template(
                OUTPUT_FILENAME_MANAGER_STATS_TEMPLATE, CURRENT_RUN_BLOCK
            )
            APPLY_SORT_TO_SOURCE = getattr(_c, "apply_sort_to_source", True)
            APPLY_SORT_TO_MAIN = getattr(_c, "apply_sort_to_main", False)
            JSON_COLUMNS = _c.json_columns
            REWARD_GETCONDITION_SUMMARY = getattr(_c, "reward_getcondition_summary", None) or {}
            SOURCE_EXPORT_SORT = getattr(_c, "source_export_sort", []) or []
            MAX_WORKERS_IO = _c.max_workers_io
            MAX_WORKERS_CPU = _c.max_workers_cpu
            MAX_WORKERS = _c.max_workers_cpu
            _skip_align = getattr(_c, "skip_data_alignment_sheets", None)
            if _skip_align is None:
                SKIP_DATA_ALIGNMENT_SHEETS = parse_skip_data_alignment_sheets(
                    getattr(_c, "_cfg", {}) or {}
                )
            else:
                SKIP_DATA_ALIGNMENT_SHEETS = list(_skip_align)
            EXCEL_WRITER = getattr(_c, "excel_writer", "openpyxl")
            TOURNAMENT_STATUS_CHOICES = _c.tournament_status_choices
            PROJECT_BASE_DIR = _c.base_dir
            INPUT_ARCHIVE_SQLITE = getattr(_c, "input_archive_sqlite", None) or {"enabled": False}
            RATING_ITEM_MATRIX = getattr(_c, "rating_item_matrix", None) or {}
            SEASON_ORDER_SUMMARY = getattr(_c, "season_order_summary", None) or {}
            MANAGER_STATS = getattr(_c, "manager_stats", None) or {}
            return
    except Exception as e:
        # BUG-12: не переходить молча на config/config.json — это другой конфиг и, возможно, другие данные
        _path = getattr(_c, "config_path", "?")
        logging.exception(f"[config] Не удалось применить загруженную конфигурацию {_path}: {e}")
        raise RuntimeError(f"Не удалось применить конфигурацию {_path}: {e}") from e

    # Загрузка из config/config.json (корень проекта = родитель каталога src)
    from src.config_loader import default_config_path, load_config_dict, resolve_project_base_dir

    _CONFIG_PATH = default_config_path()
    _cfg = load_config_dict(_CONFIG_PATH)
    _BASE_DIR = resolve_project_base_dir(_CONFIG_PATH)
    PROJECT_BASE_DIR = _BASE_DIR

    DIR_INPUT = os.path.join(_BASE_DIR, _cfg["paths"]["input"])
    DIR_OUTPUT = os.path.join(_BASE_DIR, _cfg["paths"]["output"])
    DIR_LOGS = os.path.join(_BASE_DIR, _cfg["paths"]["logs"])
    LOG_LEVEL = _cfg["logging"]["level"]
    LOG_BASE_NAME = _cfg["logging"]["base_name"]
    from src.config_loader import parse_log_retention_days

    LOG_RETENTION_DAYS = parse_log_retention_days(_cfg)
    INPUT_FILES_BY_BLOCK = parse_input_files_by_block(_cfg)
    ALL_INPUT_FILES = INPUT_FILES_BY_BLOCK
    RUN_BLOCKS = parse_run_blocks_config(_cfg)
    RUN_BLOCKS_PARALLEL = parse_run_blocks_parallel(_cfg)
    CFG_RAW = dict(_cfg)
    CONFIG_PATH = _CONFIG_PATH
    CURRENT_RUN_BLOCK = RUN_BLOCKS[0] if RUN_BLOCKS else "PROM"
    INPUT_FILES = get_input_files_for_block(INPUT_FILES_BY_BLOCK, CURRENT_RUN_BLOCK)
    SUMMARY_SHEET = _cfg["summary_sheet"]
    SHEET_ORDER = _cfg.get("sheet_order") or []
    SUMMARY_KEY_DEFS = _cfg["summary_key_defs"]
    SUMMARY_KEY_COLUMNS = []
    for _entry in SUMMARY_KEY_DEFS:
        for _col in _entry["cols"]:
            if _col not in SUMMARY_KEY_COLUMNS:
                SUMMARY_KEY_COLUMNS.append(_col)
    GENDER_PATTERNS = _cfg["gender"]["patterns"]
    GENDER_PROGRESS_STEP = _cfg["gender"].get("progress_step", 500)
    COL_REWARD_LINK_CONTEST_CODE = "REWARD_LINK => CONTEST_CODE"
    MERGE_FIELDS_ADVANCED = _cfg["merge_fields_advanced"]
    COLOR_SCHEME = _cfg.get("color_scheme") or []
    COLUMN_FORMATS = _cfg.get("column_formats") or []
    _cc = _cfg.get("consistency_checks") or {}
    CONSISTENCY_CHECKS = {
        "summary_sheet_name": _cc.get("summary_sheet_name", "CONSISTENCY"),
        "rules": _cc.get("rules") or [],
        "csv_columns_count": _cc.get("csv_columns_count") or {},
    }
    for _cc_k, _cc_v in _cc.items():
        if _cc_k not in CONSISTENCY_CHECKS:
            CONSISTENCY_CHECKS[_cc_k] = _cc_v
    _ro = parse_run_outputs_for_block(_cfg, CURRENT_RUN_BLOCK)
    RUN_OUTPUTS = list(_ro[0])
    RUN_SOURCE_ONLY_EXIT = _ro[1]
    RUN_WRITE_SOURCE = _ro[2]
    RUN_WRITE_MAIN = _ro[3]
    RUN_WRITE_CONSISTENCY_FILE = _ro[4]
    RUN_CONSISTENCY_EARLY = _ro[5]
    RUN_WRITE_MANAGER_STATS = _ro[6]
    MANAGER_STATS_EARLY = _ro[7]
    RUN_WRITE_STAT_FILE = _ro[8]
    RUN_RATING_ITEM_MATRIX = _ro[10]
    RUN_SEASON_ORDER_SUMMARY = _ro[11]
    RUN_MODE = _ro[9]
    _of = _cfg.get("output_filenames") or {}
    OUTPUT_FILENAME_MAIN_TEMPLATE = _of.get("main", "SPOD_{BLOCK} main")
    OUTPUT_FILENAME_SOURCE_TEMPLATE = _of.get("source", "SPOD_{BLOCK} source")
    OUTPUT_FILENAME_CONSISTENCY_TEMPLATE = _of.get("consistency", "SPOD_{BLOCK} consistency")
    OUTPUT_FILENAME_MANAGER_STATS_TEMPLATE = _of.get(
        "manager_stats", "SPOD_{BLOCK} MANAGER_STATS"
    )
    OUTPUT_FILENAME_MAIN = resolve_output_filename_template(
        OUTPUT_FILENAME_MAIN_TEMPLATE, CURRENT_RUN_BLOCK
    )
    OUTPUT_FILENAME_SOURCE = resolve_output_filename_template(
        OUTPUT_FILENAME_SOURCE_TEMPLATE, CURRENT_RUN_BLOCK
    )
    OUTPUT_FILENAME_CONSISTENCY = resolve_output_filename_template(
        OUTPUT_FILENAME_CONSISTENCY_TEMPLATE, CURRENT_RUN_BLOCK
    )
    OUTPUT_FILENAME_MANAGER_STATS = resolve_output_filename_template(
        OUTPUT_FILENAME_MANAGER_STATS_TEMPLATE, CURRENT_RUN_BLOCK
    )
    APPLY_SORT_TO_SOURCE = _cfg.get("apply_sort_to_source", True)
    APPLY_SORT_TO_MAIN = _cfg.get("apply_sort_to_main", False)
    JSON_COLUMNS = _cfg.get("json_columns") or {}
    REWARD_GETCONDITION_SUMMARY = _cfg.get("reward_getcondition_summary") or {}
    SOURCE_EXPORT_SORT = (_cfg.get("source_export") or {}).get("sort_rules") or []
    MAX_WORKERS_IO = _cfg["performance"]["max_workers_io"]
    MAX_WORKERS_CPU = _cfg["performance"]["max_workers_cpu"]
    MAX_WORKERS = MAX_WORKERS_CPU
    SKIP_DATA_ALIGNMENT_SHEETS = parse_skip_data_alignment_sheets(_cfg)
    from src.config_loader import parse_excel_writer

    EXCEL_WRITER = parse_excel_writer(_cfg)
    _TOURNAMENT_STATUS_DEFAULT = [
        "НЕОПРЕДЕЛЕН", "АКТИВНЫЙ", "ЗАПЛАНИРОВАН",
        "ПОДВЕДЕНИЕ ИТОГОВ", "ПОДВЕДЕНИЕ ИТОГОВ", "ПОДВЕДЕНИЕ ИТОГОВ", "ЗАВЕРШЕН",
    ]
    TOURNAMENT_STATUS_CHOICES = _cfg.get("tournament_status_choices") or _TOURNAMENT_STATUS_DEFAULT
    from src.input_archive_sqlite_v2 import merge_archive_v2_config

    INPUT_ARCHIVE_SQLITE = merge_archive_v2_config(_cfg.get("input_archive_sqlite"))
    RATING_ITEM_MATRIX = _cfg.get("rating_item_matrix") or {}
    SEASON_ORDER_SUMMARY = _cfg.get("season_order_summary") or {}
    MANAGER_STATS = _cfg.get("manager_stats") or {}


def apply_run_block_context(block: str) -> None:
    """
    Переключает контекст прогона на блок:
    input_files раздела, имена Excel, run_outputs блока, плейсхолдеры {BLOCK} в путях архива.
    """
    global INPUT_FILES, CURRENT_RUN_BLOCK, INPUT_ARCHIVE_SQLITE, MANAGER_STATS
    global OUTPUT_FILENAME_MAIN, OUTPUT_FILENAME_SOURCE
    global OUTPUT_FILENAME_CONSISTENCY, OUTPUT_FILENAME_MANAGER_STATS
    global RUN_OUTPUTS, RUN_SOURCE_ONLY_EXIT, RUN_WRITE_SOURCE, RUN_WRITE_MAIN
    global RUN_WRITE_CONSISTENCY_FILE, RUN_CONSISTENCY_EARLY
    global RUN_WRITE_MANAGER_STATS, MANAGER_STATS_EARLY
    global RUN_WRITE_STAT_FILE, RUN_MODE
    global RUN_RATING_ITEM_MATRIX, RUN_SEASON_ORDER_SUMMARY

    CURRENT_RUN_BLOCK = str(block).strip().upper()
    set_current_block(CURRENT_RUN_BLOCK)

    # Файлы блока + подстановка {BLOCK} в archive_db_path / subdir на всякий случай
    raw_files = get_input_files_for_block(INPUT_FILES_BY_BLOCK, CURRENT_RUN_BLOCK)
    INPUT_FILES = [
        resolve_block_placeholders(dict(fc), CURRENT_RUN_BLOCK) for fc in raw_files
    ]

    OUTPUT_FILENAME_MAIN = resolve_output_filename_template(
        OUTPUT_FILENAME_MAIN_TEMPLATE, CURRENT_RUN_BLOCK
    )
    OUTPUT_FILENAME_SOURCE = resolve_output_filename_template(
        OUTPUT_FILENAME_SOURCE_TEMPLATE, CURRENT_RUN_BLOCK
    )
    OUTPUT_FILENAME_CONSISTENCY = resolve_output_filename_template(
        OUTPUT_FILENAME_CONSISTENCY_TEMPLATE, CURRENT_RUN_BLOCK
    )
    OUTPUT_FILENAME_MANAGER_STATS = resolve_output_filename_template(
        OUTPUT_FILENAME_MANAGER_STATS_TEMPLATE, CURRENT_RUN_BLOCK
    )

    # run_outputs именно этого блока
    cfg_for_ro = CFG_RAW if isinstance(CFG_RAW, dict) and CFG_RAW else {}
    (
        RUN_OUTPUTS,
        RUN_SOURCE_ONLY_EXIT,
        RUN_WRITE_SOURCE,
        RUN_WRITE_MAIN,
        RUN_WRITE_CONSISTENCY_FILE,
        RUN_CONSISTENCY_EARLY,
        RUN_WRITE_MANAGER_STATS,
        MANAGER_STATS_EARLY,
        RUN_WRITE_STAT_FILE,
        RUN_MODE,
        RUN_RATING_ITEM_MATRIX,
        RUN_SEASON_ORDER_SUMMARY,
    ) = parse_run_outputs_for_block(cfg_for_ro, CURRENT_RUN_BLOCK)

    # Архив SQLite: пути с {BLOCK}
    base_arch = dict(INPUT_ARCHIVE_SQLITE or {})
    if cfg_for_ro.get("input_archive_sqlite"):
        from src.input_archive_sqlite_v2 import merge_archive_v2_config

        base_arch = merge_archive_v2_config(cfg_for_ro.get("input_archive_sqlite"))
    INPUT_ARCHIVE_SQLITE = resolve_block_placeholders(base_arch, CURRENT_RUN_BLOCK)

    # manager_stats: подкаталоги JS с {BLOCK}
    if isinstance(MANAGER_STATS, dict) and MANAGER_STATS:
        MANAGER_STATS = resolve_block_placeholders(dict(MANAGER_STATS), CURRENT_RUN_BLOCK)
    elif cfg_for_ro.get("manager_stats"):
        MANAGER_STATS = resolve_block_placeholders(
            dict(cfg_for_ro.get("manager_stats") or {}), CURRENT_RUN_BLOCK
        )

    logging.info(
        f"[main] Блок {CURRENT_RUN_BLOCK}: файлов={len(INPUT_FILES)}, "
        f"run_outputs={RUN_OUTPUTS}, main→«{OUTPUT_FILENAME_MAIN}», "
        f"archive_db→«{INPUT_ARCHIVE_SQLITE.get('db_path')}»"
    )


_load_config_globals()
# Инициализация CFG_RAW, если импорт пошёл через Config без _cfg
try:
    CFG_RAW
except NameError:
    CFG_RAW = {}
try:
    RUN_BLOCKS_PARALLEL
except NameError:
    RUN_BLOCKS_PARALLEL = False
try:
    CONFIG_PATH
except NameError:
    CONFIG_PATH = ""
try:
    SKIP_DATA_ALIGNMENT_SHEETS
except NameError:
    SKIP_DATA_ALIGNMENT_SHEETS = parse_skip_data_alignment_sheets({})
try:
    EXCEL_WRITER
except NameError:
    EXCEL_WRITER = "openpyxl"
# === КОНЕЦ ЗАГРУЗКИ КОНФИГА ===

# Выходной файл Excel (шаблон из конфига output_filenames.main)
def get_output_filename() -> str:
    """
    Генерирует имя выходного Excel файла с текущей датой и временем.

    Returns:
        Имя файла в формате '{output_filenames.main}_YYYY-MM-DD_HH-MM-SS.xlsx'
    """
    return f"{OUTPUT_FILENAME_MAIN}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"


def get_output_dir_for_run(base_dir: str, block: Optional[str] = None) -> str:
    """
    Возвращает подкаталог для выходных файлов: base_dir/<BLOCK>/YYYY/DD-MM.
    Без block — прежний вид base_dir/YYYY/DD-MM (обратная совместимость).
    Год — 4 цифры, день и месяц — по 2 цифры (например 01-01 для 1 января).
    Каталог создаётся при необходимости.

    Args:
        base_dir: Базовый каталог вывода (из config paths.output, например OUT).
        block: Код блока (PROM / IFT / PSI); при наличии — первый уровень под OUT.

    Returns:
        Путь вида OUT/PROM/2026/16-03 для файлов блока PROM от 16.03.2026.
    """
    now = datetime.now()
    year = now.strftime("%Y")
    day_month = now.strftime("%d-%m")
    if block:
        path = os.path.join(base_dir, str(block).strip().upper(), year, day_month)
    else:
        path = os.path.join(base_dir, year, day_month)
    os.makedirs(path, exist_ok=True)
    return path


def get_log_dir_for_run() -> str:
    """
    Возвращает подкаталог для логов по дате: DIR_LOGS/YYYY/DD-MM (как для OUT).
    Каталог создаётся при необходимости.
    """
    now = datetime.now()
    year = now.strftime("%Y")
    day_month = now.strftime("%d-%m")
    path = os.path.join(DIR_LOGS, year, day_month)
    os.makedirs(path, exist_ok=True)
    return path


# Лог-файл с учетом уровня
def get_log_filename(block_suffix: Optional[str] = None):
    """
    Генерирует путь к лог-файлу: LOGS/YYYY/DD-MM/имя_уровень_ГГГГММДД_ЧЧ_ММ_СС[_БЛОК].log
    (подкаталоги по дате по тому же принципу, что и для OUT).
    Секунды и код блока (процесс-блок параллельного режима) — чтобы запуски в одну минуту
    и параллельные блоки не писали в один файл вперемешку (BUG-11).
    """
    level_suffix = f"_{LOG_LEVEL}" if LOG_LEVEL else ""
    block_part = f"_{block_suffix}" if block_suffix else ""
    date_suffix = f"_{datetime.now().strftime('%Y%m%d_%H_%M_%S')}{block_part}.log"
    log_dir = get_log_dir_for_run()
    return os.path.join(log_dir, LOG_BASE_NAME + level_suffix + date_suffix)


# === Логирование ===
def _logging_level_from_config(name: str) -> int:
    """
    Преобразует строку logging.level из config.json в константу logging.
    Неизвестные значения — INFO (чтобы не засорять файл сообщениями DEBUG при опечатке).
    """
    mapping = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    return mapping.get((name or "INFO").strip().upper(), logging.INFO)


class _QuietExpectedMergeConsoleFilter(logging.Filter):
    """
    Не выводит в консоль ожидаемые INFO merge (пустой результат фильтра, сопоставление регистра).
    В лог-файл эти сообщения по-прежнему пишутся.
    """

    _SKIP_MARKERS: Tuple[str, ...] = (
        "Не ошибка: после фильтра",
        "после фильтра на листе-источнике",
        "ожидаемо: нет строк с нужным статусом",
        "сопоставлена с",
        "сопоставлен с",
        "без учёта регистра",
        "контекст фильтра не передан",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.WARNING:
            return True
        try:
            msg = record.getMessage()
        except Exception:
            return True
        return not any(m in msg for m in self._SKIP_MARKERS)


def _debug_enabled() -> bool:
    """PERF-06: DEBUG реально пишется (уровень корневого логгера = минимум уровней обработчиков)."""
    return logging.getLogger().isEnabledFor(logging.DEBUG)


def _log_info_file_only(msg: str) -> None:
    """
    Пишет INFO только в FileHandler (не в консоль).
    Для ожидаемых ситуаций merge, которые не являются ошибками.
    """
    logger = logging.getLogger()
    emitted = False
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            record = logging.LogRecord(
                name=logger.name,
                level=logging.INFO,
                pathname=__file__,
                lineno=0,
                msg=msg,
                args=(),
                exc_info=None,
                func="add_fields_to_sheet",
            )
            # Учитываем фильтры хендлера (BlockLogFilter и т.п.)
            if handler.level <= logging.INFO and (not handler.filters or all(f.filter(record) for f in handler.filters)):
                handler.emit(record)
                emitted = True
    if not emitted:
        # Fallback: обычный INFO (консоль отфильтрует Quiet-фильтром)
        logging.info(msg)


def cleanup_old_logs(logs_dir: str, base_name: str, retention_days: int, now: Optional[datetime] = None) -> int:
    """
    LOG-04: удалить лог-файлы программы старше retention_days дней (по времени изменения).
    Удаляются только файлы «<base_name>_*.log» внутри logs_dir и опустевшие после этого подкаталоги дат.
    retention_days <= 0 — ничего не удаляется. Возвращает число удалённых файлов.
    """
    if retention_days <= 0 or not logs_dir or not os.path.isdir(logs_dir):
        return 0
    cutoff = (now or datetime.now()).timestamp() - retention_days * 86400
    removed = 0
    for root, dirs, files in os.walk(logs_dir, topdown=False):
        for name in files:
            if not (name.startswith(f"{base_name}_") and name.endswith(".log")):
                continue
            path = os.path.join(root, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    removed += 1
            except OSError:
                logging.warning(f"[logs] Не удалось удалить старый лог: {path}", exc_info=True)
        if root != logs_dir:
            try:
                if not os.listdir(root):
                    os.rmdir(root)
            except OSError:
                pass
    return removed


# Маркер обработчиков, созданных setup_logger (снимаются при повторной настройке — BUG-05)
_SPOD_HANDLER_ATTR = "_spod_handler"


def setup_logger(block_suffix: Optional[str] = None):
    """
    Настраивает систему логирования для программы.

    Обработчики программы (с маркером _spod_handler):
    - Файловый: уровень из config.json → logging.level (имя файла уже содержит суффикс уровня);
      добавляется всегда — даже если у корневого логгера уже есть чужие обработчики (BUG-05:
      консоль IDE, повторный main() в той же сессии, basicConfig библиотеки);
    - Консольный: WARNING и выше (краткий ход — console_ui); не добавляется, если консольный
      обработчик уже есть (чужой) — тогда тот получает уровень WARNING и Quiet-фильтр.
    Обработчики предыдущей настройки (повторный main(), унаследованные дочерним процессом) снимаются.

    Returns:
        str: Путь к созданному лог-файлу
    """
    log_file = get_log_filename(block_suffix)
    # get_log_filename() уже создаёт каталог LOGS/YYYY/DD-MM через get_log_dir_for_run()
    logger = logging.getLogger()
    for h in list(logger.handlers):
        if getattr(h, _SPOD_HANDLER_ATTR, False):
            logger.removeHandler(h)
            h.close()

    # Чужие консольные обработчики: Quiet-фильтр и только WARNING+ (ожидаемые INFO merge не светятся)
    foreign_console = False
    for h in logger.handlers:
        if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
            foreign_console = True
            if not any(isinstance(f, _QuietExpectedMergeConsoleFilter) for f in h.filters):
                h.addFilter(_QuietExpectedMergeConsoleFilter())
            if h.level < logging.WARNING:
                h.setLevel(logging.WARNING)

    # Уровень файла совпадает с config: при level=INFO в лог-файл не попадают записи DEBUG
    file_level = _logging_level_from_config(LOG_LEVEL)

    # Уровень корневого логгера = самый подробный из обработчиков (файл — из config, консоль — WARNING):
    # при INFO вызовы logging.debug(...) отсекаются сразу, без создания записи (PERF-06)
    logger.setLevel(min(file_level, logging.WARNING))

    # Форматтер для файла: имя вызывающей функции даёт сам logging (%(funcName)s) — без inspect.stack()
    # и без изменения record.msg (иначе суффикс попадал и в консоль) — BUG-04
    file_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s [def: %(funcName)s]",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(file_formatter)
    file_handler.addFilter(BlockLogFilter())
    setattr(file_handler, _SPOD_HANDLER_ATTR, True)
    logger.addHandler(file_handler)

    if not foreign_console:
        # Консольный обработчик: WARNING и ERROR (INFO — только в файл; консоль — console_ui)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.WARNING)
        console_handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        console_handler.addFilter(BlockLogFilter())
        console_handler.addFilter(_QuietExpectedMergeConsoleFilter())
        setattr(console_handler, _SPOD_HANDLER_ATTR, True)
        logger.addHandler(console_handler)

    return log_file

def _parse_date_column_to_date(series: pd.Series, col_label: str = "") -> pd.Series:
    """
    Парсит колонку дат в ``datetime.date`` без шумного pandas UserWarning в консоли.

    Основной формат SPOD — YYYY-MM-DD через ``strptime`` (поддерживает и 4000-01-01,
    который не влезает в pandas Timestamp). Мягкий fallback — только в лог DEBUG.
    """
    from datetime import date as date_cls
    from datetime import datetime as dt_cls

    label = col_label or str(getattr(series, "name", "") or "date")
    empty_markers = {"", "-", "None", "null", "nan", "NaT", "<NA>"}

    def _one(val: Any):
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(val, dt_cls):
            return val.date()
        if isinstance(val, date_cls):
            return val
        if isinstance(val, pd.Timestamp):
            return None if pd.isna(val) else val.date()
        s = str(val).strip()
        if s in empty_markers or s.lower() == "nat":
            return None
        try:
            return dt_cls.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return False  # маркер: нужен fallback

    primary = series.map(_one)
    need_fallback = primary.apply(lambda x: x is False)
    n_fallback = int(need_fallback.sum())
    out = primary.map(lambda x: None if x is False else x)

    if n_fallback > 0:
        logging.debug(
            f"[DATE] {label}: {n_fallback} значений не в формате YYYY-MM-DD — "
            f"повторный разбор (без UserWarning в консоли)"
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            fb = pd.to_datetime(series[need_fallback], errors="coerce")

        def _ts_to_date(v: Any):
            try:
                if v is None or pd.isna(v):
                    return None
            except (TypeError, ValueError):
                return None
            if isinstance(v, pd.Timestamp):
                return v.date()
            return None

        out.loc[need_fallback] = fb.map(_ts_to_date).values
    return out


@debug_timed()
def calculate_tournament_status(df_tournament, df_report=None):
    """
    Вычисляет статус турнира на основе текущей даты и дат турнира.
    
    Эта функция анализирует временные рамки турнира и определяет его текущее состояние.
    Статус зависит от соотношения текущей даты с датами начала, окончания и подведения итогов турнира.

    Логика определения статуса:
    - Если сегодня между START_DT и END_DT включительно → "АКТИВНЫЙ"
    - Если сегодня < START_DT → "ЗАПЛАНИРОВАН"
    - Если сегодня > END_DT но < RESULT_DT → "ПОДВЕДЕНИЕ ИТОГОВ"
    - Если сегодня >= RESULT_DT:
        - Если макс CONTEST_DATE < RESULT_DT → "ПОДВЕДЕНИЕ ИТОГОВ"
        - Если макс CONTEST_DATE >= RESULT_DT → "ЗАВЕРШЕН"

    Args:
        df_tournament (pd.DataFrame): DataFrame с данными турниров, должен содержать колонки:
            - START_DT: дата начала турнира
            - END_DT: дата окончания турнира  
            - RESULT_DT: дата подведения итогов
        df_report (pd.DataFrame, optional): DataFrame с отчетами для анализа CONTEST_DATE.
            Должен содержать колонки TOURNAMENT_CODE и CONTEST_DATE.

    Returns:
        pd.DataFrame: DataFrame с добавленной колонкой CALC_TOURNAMENT_STATUS,
                     содержащей вычисленный статус для каждого турнира
    """

    today = pd.Timestamp.now().date()  # Текущая дата
    df = df_tournament.copy()          # Копируем DataFrame для безопасной работы

    # Парсим даты явно (YYYY-MM-DD), без pandas UserWarning в консоль
    df['START_DT_parsed'] = _parse_date_column_to_date(df['START_DT'], "START_DT")
    df['END_DT_parsed'] = _parse_date_column_to_date(df['END_DT'], "END_DT")
    df['RESULT_DT_parsed'] = _parse_date_column_to_date(df['RESULT_DT'], "RESULT_DT")

    # Получаем максимальные CONTEST_DATE для каждого TOURNAMENT_CODE из REPORT
    # Это нужно для определения, завершились ли все конкурсы турнира
    max_contest_dates = {}
    if df_report is not None and 'CONTEST_DATE' in df_report.columns and 'TOURNAMENT_CODE' in df_report.columns:
        df_report_dates = df_report.copy()
        df_report_dates['CONTEST_DATE_parsed'] = _parse_date_column_to_date(
            df_report_dates['CONTEST_DATE'], "CONTEST_DATE"
        )
        df_report_dates = df_report_dates.dropna(subset=['CONTEST_DATE_parsed', 'TOURNAMENT_CODE'])

        if not df_report_dates.empty:
            # Группируем по коду турнира и находим максимальную дату конкурса
            max_contest_dates = df_report_dates.groupby('TOURNAMENT_CODE')['CONTEST_DATE_parsed'].max().to_dict()


    # ВЕКТОРИЗОВАННАЯ ВЕРСИЯ: Заменяем apply на векторные операции для ускорения
    # Создаем Series с максимальными датами конкурсов для каждого турнира
    if max_contest_dates:
        df['MAX_CONTEST_DATE'] = df['TOURNAMENT_CODE'].map(max_contest_dates)
    else:
        df['MAX_CONTEST_DATE'] = None
    
    # Векторизованное определение статуса с использованием numpy.select
    # Условия проверяются последовательно, первое совпадение определяет статус
    # ВАЖНО: Порядок условий критичен для корректной логики
    conditions = [
        # Условие 0: Нет ключевых дат → НЕОПРЕДЕЛЕН
        pd.isna(df['START_DT_parsed']) | pd.isna(df['END_DT_parsed']),
        # Условие 1: Сегодня между START_DT и END_DT включительно → АКТИВНЫЙ
        (df['START_DT_parsed'] <= today) & (today <= df['END_DT_parsed']),
        # Условие 2: Сегодня < START_DT → ЗАПЛАНИРОВАН
        today < df['START_DT_parsed'],
        # Условие 3: Сегодня > END_DT и (нет RESULT_DT или today < RESULT_DT) → ПОДВЕДЕНИЕ ИТОГОВ
        (today > df['END_DT_parsed']) & (pd.isna(df['RESULT_DT_parsed']) | (today < df['RESULT_DT_parsed'])),
        # Условие 4: today >= RESULT_DT и нет MAX_CONTEST_DATE → ПОДВЕДЕНИЕ ИТОГОВ
        # Проверяем что today > END_DT (уже проверено в условии 3 не выполнилось) и today >= RESULT_DT
        (today > df['END_DT_parsed']) & (~pd.isna(df['RESULT_DT_parsed'])) & (today >= df['RESULT_DT_parsed']) & pd.isna(df['MAX_CONTEST_DATE']),
        # Условие 5: today >= RESULT_DT и MAX_CONTEST_DATE < RESULT_DT → ПОДВЕДЕНИЕ ИТОГОВ
        (today > df['END_DT_parsed']) & (~pd.isna(df['RESULT_DT_parsed'])) & (today >= df['RESULT_DT_parsed']) & (~pd.isna(df['MAX_CONTEST_DATE'])) & (df['MAX_CONTEST_DATE'] < df['RESULT_DT_parsed']),
        # Условие 6: today >= RESULT_DT и MAX_CONTEST_DATE >= RESULT_DT → ЗАВЕРШЕН
        (today > df['END_DT_parsed']) & (~pd.isna(df['RESULT_DT_parsed'])) & (today >= df['RESULT_DT_parsed']) & (~pd.isna(df['MAX_CONTEST_DATE'])) & (df['MAX_CONTEST_DATE'] >= df['RESULT_DT_parsed']),
    ]
    
    # Метки статусов из config.json (tournament_status_choices); порядок соответствует conditions[0..6]
    choices = TOURNAMENT_STATUS_CHOICES if len(TOURNAMENT_STATUS_CHOICES) >= len(conditions) else (
        TOURNAMENT_STATUS_CHOICES + ["НЕОПРЕДЕЛЕН"] * (len(conditions) - len(TOURNAMENT_STATUS_CHOICES))
    )[:len(conditions)]
    default_label = TOURNAMENT_STATUS_CHOICES[0] if TOURNAMENT_STATUS_CHOICES else "НЕОПРЕДЕЛЕН"
    
    # Используем numpy.select для векторизованного выбора (быстрее чем apply)
    try:
        import numpy as np
        df['CALC_TOURNAMENT_STATUS'] = np.select(conditions, choices, default=default_label)
    except ImportError:
        # Fallback на pandas where если numpy недоступен (но он должен быть в Anaconda)
        df['CALC_TOURNAMENT_STATUS'] = pd.Series(default_label, index=df.index)
        for i, (cond, choice) in enumerate(zip(conditions, choices)):
            df.loc[cond, 'CALC_TOURNAMENT_STATUS'] = choice

    # Удаляем временные колонки с распарсенными датами
    df = df.drop(columns=['START_DT_parsed', 'END_DT_parsed', 'RESULT_DT_parsed', 'MAX_CONTEST_DATE'])

    # Логируем статистику по статусам для мониторинга
    status_counts = df['CALC_TOURNAMENT_STATUS'].value_counts()
    logging.info(f"[TOURNAMENT STATUS] Статистика: {status_counts.to_dict()}")

    # Засекаем время выполнения и логируем завершение

    return df








# === ЧТЕНИЕ И ЗАПИСЬ ДАННЫХ ===


def find_file_case_insensitive(directory: str, base_name: str, extensions: List[str]) -> Optional[str]:
    """
    Ищет файл в каталоге без учета регистра имени файла и расширения.
    
    Args:
        directory (str): Каталог для поиска
        base_name (str): Имя файла — либо полное с расширением (например, "file.csv"),
                         либо базовое без расширения
        extensions (list): Список возможных расширений (например, ['.csv', '.CSV'])
    
    Returns:
        str or None: Полный путь к найденному файлу или None если файл не найден
    """
    if not os.path.exists(directory):
        return None
    
    # Если передано полное имя с расширением — используем его для сравнения
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
            if (name.lower() == match_stem and
                    ext.lower() in [e.lower() for e in extensions]):
                return os.path.join(directory, file_name)
    
    return None


def check_input_files_exist() -> List[Dict[str, str]]:
    """
    Проверяет наличие всех файлов из INPUT_FILES в каталоге DIR_INPUT.
    Использует ту же логику поиска, что и при загрузке (find_file_case_insensitive).
    
    Returns:
        list: Список ненайденных файлов. Каждый элемент — dict с ключами "file", "sheet".
              Пустой список, если все файлы найдены.
    """
    missing = []
    for file_conf in INPUT_FILES:
        base_name = file_conf["file"]
        sheet_name = file_conf["sheet"]
        # Подкаталог (один уровень): если задан subdir — ищем в paths.input / subdir
        subdir = (file_conf.get("subdir") or "").strip()
        search_dir = os.path.join(DIR_INPUT, subdir) if subdir else DIR_INPUT
        path = _find_input_file(search_dir, sheet_name, base_name)
        if path is None:
            missing.append({"file": base_name, "sheet": sheet_name})
    return missing


# STR-03: зашитые имена листов/колонок/файлов источников — в одном месте; переопределяются ключом
# config «source_aliases»: {лист: {"keys": {имя: замена}, "columns": {имя: замена}, "file_fallbacks": {файл: замена}}}
_DEFAULT_SOURCE_ALIASES: Dict[str, Dict[str, Dict[str, str]]] = {
    "LIST-TOURNAMENT": {
        # В выгрузке геймификации ключ и статус иногда приходят под другими заголовками
        "keys": {"Код турнира": "TOURNAMENT_CODE"},
        "columns": {"Бизнес-статус турнира": "Бизнес-статус"},
        "file_fallbacks": {"gamification-tournamentList-2": "gamification-tournamentList"},
    },
}


def _source_aliases(sheet: str) -> Dict[str, Dict[str, str]]:
    cfg_aliases = CFG_RAW.get("source_aliases") if isinstance(CFG_RAW, dict) else None
    if isinstance(cfg_aliases, dict) and sheet in cfg_aliases:
        return cfg_aliases[sheet] or {}
    return _DEFAULT_SOURCE_ALIASES.get(sheet, {})


def apply_source_aliases(sheet_src: str, df_src: pd.DataFrame, src_keys: List[str], columns: Any, context: str) -> List[str]:
    """
    Подстановка ключей и колонок источника по source_aliases (одна реализация для merge, merge в
    потоках и SUMMARY). Ключ заменяется, если его нет на листе, а замена есть; колонка копируется
    из колонки-замены, если нужной нет. Возвращает (возможно изменённый) список ключей; df_src
    дополняется колонками на месте.
    """
    aliases = _source_aliases(sheet_src)
    if not aliases or df_src is None:
        return src_keys
    key_map = aliases.get("keys") or {}
    new_keys = [src_keys] if isinstance(src_keys, str) else list(src_keys)
    for i, key in enumerate(new_keys):
        alt = key_map.get(key)
        if alt and key not in df_src.columns and alt in df_src.columns:
            new_keys[i] = alt
            logging.info(f"[MERGE] {context} {sheet_src}: подстановка ключа {alt} вместо '{key}'")
    col_map = aliases.get("columns") or {}
    for col in (columns if isinstance(columns, list) else [columns]):
        alt = col_map.get(col)
        if alt and col not in df_src.columns and alt in df_src.columns:
            df_src[col] = df_src[alt]
            logging.info(f"[MERGE] {context} {sheet_src}: подстановка колонки '{alt}' для '{col}'")
    return new_keys


def _find_input_file(search_dir: str, sheet_name: str, file_name: str) -> Optional[str]:
    """Файл input_files с учётом запасного имени из source_aliases.file_fallbacks."""
    path = find_file_case_insensitive(search_dir, file_name, [".csv", ".CSV"])
    if path is None:
        alt = (_source_aliases(sheet_name).get("file_fallbacks") or {}).get(file_name)
        if alt:
            path = find_file_case_insensitive(search_dir, alt, [".csv", ".CSV"])
            if path:
                logging.info(f"{sheet_name}: использован файл по альтернативному имени: {path}")
    return path


def _raise_if_input_files_missing() -> None:
    """Если каких-то файлов input_files текущего блока нет — MissingInputFilesError (код возврата 2)."""
    missing_files = check_input_files_exist()
    if not missing_files:
        return
    msg_lines = [
        "Не найдены следующие файлы из INPUT_FILES:",
        f"  (ожидаемый каталог: {DIR_INPUT})",
    ]
    for m in missing_files:
        msg_lines.append(f"  - {m['file']} (лист: {m['sheet']})")
    raise MissingInputFilesError(msg_lines)


@debug_timed(log_args_len=True)
def read_csv_file(
    file_path: str,
    expected_columns: int = 0,
) -> Optional[Tuple[pd.DataFrame, List[Dict[str, Any]]]]:
    """
    Читает CSV файл с заданными параметрами и логирует процесс.

    Функция настроена для работы с CSV файлами, использующими точку с запятой как разделитель.
    Все данные читаются как строки для сохранения точности, особенно для JSON полей.
    Сохраняет тройные кавычки в неизменном виде.
    Строки с числом полей, отличным от ожидаемого, нормализуются (дополняются/обрезаются),
    при этом фиксируются расхождения для итогового отчёта и листа CONSISTENCY.

    Args:
        file_path: Путь к CSV файлу для чтения.
        expected_columns: Ожидаемое число полей в каждой строке. 0 — АВТО: берётся из заголовка;
            число > 0 — сравнение с этим значением.

    Returns:
        (pd.DataFrame, list) или None при ошибке. Список — записи о расхождениях по числу полей
        в строке: [{"row_index", "expected_cols", "actual_cols", "direction": "больше"|"меньше"}, ...].
    """
    params = f"({file_path}, expected_columns={expected_columns})"

    try:
        rows = []
        headers = None
        issues: List[Dict[str, Any]] = []

        with open(file_path, "r", encoding="utf-8-sig", newline="") as file:
            csv_reader = csv.reader(file, delimiter=';', quoting=csv.QUOTE_NONE)

            for i, row in enumerate(csv_reader):
                if i == 0:
                    headers = [_normalize_column_name_for_format_match(h) for h in row]
                    # АВТО (expected_columns=0): ожидаемое число полей = длина заголовка; иначе — из конфига
                    n = expected_columns if expected_columns > 0 else len(headers)
                else:
                    actual = len(row)
                    if actual < n:
                        row = list(row) + [""] * (n - actual)
                        issues.append({"row_index": i + 1, "expected_cols": n, "actual_cols": actual, "direction": "меньше"})
                    elif actual > n:
                        # Последняя колонка может содержать JSON с точкой с запятой внутри — склеиваем хвост в одну ячейку
                        row = list(row[: n - 1]) + [";".join(row[n - 1 :])]
                        issues.append({"row_index": i + 1, "expected_cols": n, "actual_cols": actual, "direction": "больше"})
                    rows.append(row)

        # Значения csv.reader — уже str (короткие строки дополнены ""), поэтому без astype(str) по колонкам (PERF-09)
        df = pd.DataFrame(rows, columns=headers, dtype=object)

        for col in df.columns:
            if "FEATURE" in col or "ADD_DATA" in col:
                if _debug_enabled():
                    logging.debug(f"CSV {file_path} поле {col}: {df[col].dropna().head(2).to_list()}")

        if issues:
            logging.warning(f"[CSV] Расхождение по числу полей: {file_path}, строк с расхождением: {len(issues)}")
        logging.info(f"Файл успешно загружен: {file_path}, строк: {len(df)}, колонок: {len(df.columns)}")

        return (df, issues)

    except Exception:
        # Одна запись с traceback (LOG-01)
        logging.exception(f"Ошибка загрузки файла: {file_path} [read_csv_file {params}]")
        return None


@debug_timed()
def _sort_sheets_by_config(sheets: Dict[str, Any], tag: str) -> None:
    """
    Сортировка листов по sort_columns (или source_sort) из input_files — на месте в словаре sheets (STR-03).

    Порядок в конфиге = последовательность применения ко всему списку: 1 → 2 → 3 (последнее поле задаёт
    основной порядок). Для pandas ключи переворачиваются: первый в конфиге — уточняющий, последний — основной.
    """
    sort_by_sheet: Dict[str, Any] = {}
    for file_conf in INPUT_FILES:
        sheet_name = file_conf.get("sheet")
        if not sheet_name:
            continue
        cols = file_conf.get("sort_columns") or file_conf.get("source_sort") or []
        if cols:
            sort_by_sheet[sheet_name] = cols
    for sheet_name, cols_conf in sort_by_sheet.items():
        item = sheets.get(sheet_name)
        if item is None or len(item) < 1 or item[0] is None:
            continue
        df, params = item
        if not isinstance(df, pd.DataFrame) or len(cols_conf) == 0:
            continue
        by_cols = []
        ascending_list = []
        for c in cols_conf:
            col_name = c.get("column") if isinstance(c, dict) else c
            order = (c.get("order", "asc") or "asc").lower() if isinstance(c, dict) else "asc"
            if not col_name:
                continue
            if col_name in df.columns:
                by_cols.append(col_name)
                ascending_list.append(order != "desc")
            elif _debug_enabled():
                logging.debug(f"[{tag}] Лист {sheet_name}: поле сортировки '{col_name}' не найдено, пропуск")
        if by_cols:
            by_cols.reverse()
            ascending_list.reverse()
            try:
                sheets[sheet_name] = (df.sort_values(by=by_cols, ascending=ascending_list), params)
            except Exception as e:
                logging.warning(f"[{tag}] Сортировка листа {sheet_name} пропущена: {e}")
        else:
            logging.info(f"[{tag}] Лист {sheet_name}: ни одно поле сортировки не найдено, запись без сортировки")


def _apply_source_sheet_layout(ws: Any, sheet_item: Any, df_written: pd.DataFrame) -> None:
    """Ширины, закрепление, автофильтр и перенос по словам на листе source (параметры — из input_files)."""
    sheet_name = ws.title
    params: Dict[str, Any] = {}
    if isinstance(sheet_item, (list, tuple)) and len(sheet_item) >= 2 and isinstance(sheet_item[1], dict):
        file_conf = sheet_item[1]
        params = {
            "max_col_width": file_conf.get("max_col_width", 60),
            "freeze": file_conf.get("freeze", "A2"),
            "col_width_mode": file_conf.get("col_width_mode", "AUTO"),
            "min_col_width": file_conf.get("min_col_width", 10),
        }
    if not params:
        params = {"max_col_width": 60, "freeze": "A2", "col_width_mode": "AUTO", "min_col_width": 10}
    header_cells = list(ws[1])
    data_lengths: Optional[List[int]] = None
    if df_written.shape[1] == len(header_cells):
        data_lengths = _content_lengths_from_df(df_written)
    for col_num, cell in enumerate(header_cells, 1):
        width = calculate_column_width(
            cell.value, ws, params, col_num,
            data_len=data_lengths[col_num - 1] if data_lengths is not None else None,
        )
        ws.column_dimensions[get_column_letter(col_num)].width = width
    ws.freeze_panes = params.get("freeze", "A2")
    # Автофильтр по умолчанию на всех листах source (только при валидных границах листа)
    try:
        if ws.max_row and ws.max_column and ws.dimensions:
            ws.auto_filter.ref = ws.dimensions
    except Exception as ex:
        logging.warning(f"[source_export] Лист «{sheet_name}»: автофильтр не применён: {ex}")
    # Перенос по словам во всех ячейках листа source (по умолчанию)
    if ws.max_row is not None and ws.max_column is not None and ws.max_row >= 1:
        wrap_setter = _StyleIdCopier("alignment", Alignment(wrap_text=True, vertical="top"))
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
            for cell in row:
                wrap_setter.apply(cell)


def write_source_excel(
    raw_sheets_data: Dict[str, Any],
    output_dir: str,
) -> str:
    """
    Записывает отдельный Excel-файл с сырыми данными (без доп. колонок и проверок).
    Имя файла: SPOD_PROM source YYYY-MM-DD_HH-MM-SS.xlsx.
    Для отсутствующих файлов создаются пустые листы. Перед записью к листам применяется
    сортировка по настройке sort_columns в каждом элементе input_files (вложенная под файл/лист).
    После записи для всех ячеек листов включается перенос по словам (wrap_text) и верхнее выравнивание.

    Args:
        raw_sheets_data: словарь {sheet_name: (df, params)} — данные сразу после загрузки CSV
        output_dir: каталог для сохранения файла

    Returns:
        str: полный путь к записанному файлу
    """
    # BUG-12: работаем с копией словаря — вызывающий код дальше передаёт raw_sheets в проверки
    # консистентности, и они не должны зависеть от того, писался ли source (сортировка, пустые листы)
    raw_sheets_data = dict(raw_sheets_data)
    # Дополняем сырые данные пустыми листами только для тех, кого включаем в source (include_in_source != false)
    for file_conf in INPUT_FILES:
        if not file_conf.get("include_in_source", True):
            continue
        sheet_name = file_conf.get("sheet")
        if not sheet_name:
            continue
        if sheet_name not in raw_sheets_data:
            raw_sheets_data[sheet_name] = (pd.DataFrame(), file_conf)

    # Сортировка: только если в конфиге включено apply_sort_to_source
    if APPLY_SORT_TO_SOURCE:
        _sort_sheets_by_config(raw_sheets_data, "source_export")

    # Порядок листов: по SHEET_ORDER, затем остальные по алфавиту
    if SHEET_ORDER:
        ordered_sheets = [s for s in SHEET_ORDER if s in raw_sheets_data]
        remaining = sorted([s for s in raw_sheets_data if s not in SHEET_ORDER])
        ordered_sheets = ordered_sheets + remaining
    else:
        ordered_sheets = sorted(raw_sheets_data.keys())

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{OUTPUT_FILENAME_SOURCE} {timestamp}.xlsx"
    output_path = os.path.join(output_dir, filename)
    os.makedirs(output_dir, exist_ok=True)
    _warn_if_long_path(output_path)

    # PERF-07 (этап 1): оформление — по листам открытого ExcelWriter, до единственного сохранения
    # (раньше: сохранить → load_workbook → оформить все ячейки → сохранить повторно)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        written: Dict[str, pd.DataFrame] = {}
        for sheet_name in ordered_sheets:
            df, _ = raw_sheets_data[sheet_name]
            df_out = pd.DataFrame() if df is None else df
            df_out.to_excel(writer, index=False, sheet_name=sheet_name)
            written[sheet_name] = df_out
        try:
            for sheet_name in ordered_sheets:
                _apply_source_sheet_layout(writer.sheets[sheet_name], raw_sheets_data[sheet_name], written[sheet_name])
        except Exception as e:
            logging.warning(f"[source_export] Не удалось применить параметры листов к {output_path}: {e}")

    logging.info(f"Выгрузка сырых данных записана: {output_path}")
    return output_path


def _force_int_cell_value(value: Any) -> Any:
    """Как force_int в apply_column_formats: целое значение вместо 1.0 / "1" (прочие — без изменений)."""
    if value is None or isinstance(value, int):
        return value
    try:
        raw = _normalize_string_for_numeric_cell(value)
        if raw != "":
            v = float(raw)
            if v == int(v):
                return int(v)
    except (TypeError, ValueError, OverflowError):
        pass
    return value


def _build_write_only_sheet_plan(sheet_name: str, df_written: pd.DataFrame, params: Dict[str, Any],
                                 use_color_scheme: bool):
    """
    План листа для потоковой записи (PERF-07): то же оформление, что даёт прежний путь
    pandas.to_excel → _format_sheet (заголовок pandas → шрифт/выравнивание заголовка → цветовая
    схема → выравнивание данных → COLUMN_FORMATS), ширины, закрепление, автофильтр.
    """
    from pandas.io.excel._openpyxl import OpenpyxlWriter
    from pandas.io.formats.excel import ExcelFormatter

    from src.excel_write_only import CellStyle, ColumnPlan, SheetPlan, excel_value_with_format

    params = params if isinstance(params, dict) else {}
    n_cols = df_written.shape[1]
    n_rows = len(df_written)
    header_values: List[Any] = [excel_value_with_format(c)[0] for c in df_written.columns] if n_cols else [None]
    width_cols = len(header_values)

    pandas_header = OpenpyxlWriter._convert_to_style_kwargs(ExcelFormatter(pd.DataFrame()).header_style)
    align_center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    align_data = Alignment(horizontal="left", vertical="center", wrap_text=True)
    header_styles = [
        CellStyle(font=Font(bold=True), border=pandas_header.get("border") if n_cols else None, alignment=align_center)
        for _ in range(width_cols)
    ]
    data_fill: List[Optional[PatternFill]] = [None] * width_cols
    data_font: List[Optional[Font]] = [None] * width_cols

    if use_color_scheme:
        for conf in _all_color_schemes():
            if sheet_name not in conf["sheets"]:
                continue
            colnames = conf["columns"] if conf["columns"] else list(header_values)
            scope = conf.get("style_scope", "header")
            for colname in colnames:
                try:
                    idx = header_values.index(colname)
                except ValueError:
                    continue
                hb, hf = conf.get("header_bg"), conf.get("header_fg")
                cb, cf = conf.get("column_bg"), conf.get("column_fg")
                if scope == "header":
                    if hb:
                        header_styles[idx].fill = PatternFill(start_color=hb, end_color=hb, fill_type="solid")
                    if hf:
                        header_styles[idx].font = Font(color=hf)
                elif scope == "all":
                    if hb:
                        header_styles[idx].fill = PatternFill(start_color=hb, end_color=hb, fill_type="solid")
                        if hf:
                            header_styles[idx].font = Font(color=hf)
                    elif cb:
                        header_styles[idx].fill = PatternFill(start_color=cb, end_color=cb, fill_type="solid")
                        if cf:
                            header_styles[idx].font = Font(color=cf)
                    if cb:
                        data_fill[idx] = PatternFill(start_color=cb, end_color=cb, fill_type="solid")
                        if cf:
                            data_font[idx] = Font(color=cf)

    skip_align = sheet_skips_data_alignment(sheet_name, SKIP_DATA_ALIGNMENT_SHEETS)
    extra_fmt = params.get("column_format_rules")
    covered = _column_indices_covered_by_column_formats(sheet_name, header_values, extra_rules=extra_fmt)
    align: List[Optional[Alignment]] = [
        None if (skip_align or (j + 1) in covered) else align_data for j in range(width_cols)
    ]
    rule_fmt: List[Optional[str]] = [None] * width_cols
    force_int = [False] * width_cols
    for rule in _iter_sheet_format_rules(sheet_name, extra_fmt):
        if not _format_rule_has_column_selector(rule):
            continue
        data_type = (rule.get("data_type") or "general").lower()
        if data_type == "number":
            num_fmt = _build_excel_number_format(rule)
        elif data_type == "date":
            num_fmt = _build_excel_date_format(rule)
        else:
            num_fmt = None
        h = rule.get("horizontal", "left").lower()
        v = rule.get("vertical", "center").lower()
        rule_align = Alignment(
            horizontal={"left": "left", "center": "center", "right": "right"}.get(h, "left"),
            vertical={"top": "top", "center": "center", "bottom": "bottom"}.get(v, "center"),
            wrap_text=bool(rule.get("wrap_text", False)),
        )
        is_force_int = data_type == "number" and int(rule.get("decimal_places", 0)) == 0
        for j, raw_header in enumerate(header_values):
            header = str(raw_header) if raw_header is not None else ""
            if not _column_matches_format_rule(header, rule):
                continue
            if num_fmt is not None:
                rule_fmt[j] = num_fmt
            if is_force_int:
                force_int[j] = True
            if not skip_align:
                align[j] = rule_align

    columns = [
        ColumnPlan(
            style=CellStyle(font=data_font[j], fill=data_fill[j], alignment=align[j]),
            rule_number_format=rule_fmt[j],
            convert=_force_int_cell_value if force_int[j] else None,
        )
        for j in range(n_cols)
    ]
    lengths = _content_lengths_from_df(df_written) if n_cols else [0]
    widths = {
        get_column_letter(j + 1): calculate_column_width(
            header_values[j], None, params, j + 1, data_len=lengths[j], header_value=header_values[j]
        )
        for j in range(width_cols)
    }
    last = f"{get_column_letter(width_cols)}{n_rows + 1 if n_cols else 1}"
    return SheetPlan(
        title=sheet_name,
        df=df_written,
        header_styles=header_styles,
        columns=columns,
        widths=widths,
        freeze=params.get("freeze", "A2"),
        auto_filter=f"A1:{last}",
        empty_header_style=header_styles[0] if not n_cols else None,
    )


@debug_timed()
def write_to_excel(
    sheets_data: Dict[str, Any],
    output_path: str,
    use_color_scheme: bool = True,
) -> None:
    """
    Записывает данные в Excel файл с форматированием и настройками.

    Функция создает Excel файл с несколькими листами, применяет форматирование
    и делает SUMMARY лист активным по умолчанию.

    Args:
        sheets_data: Словарь с данными листов в формате {sheet_name: (df, params)}
        output_path: Путь к выходному Excel файлу
        use_color_scheme: Применять ли цветовую схему (False для режима «только консистентность»)
    """
    logging.debug(f"[write_to_excel] === НАЧАЛО === Путь: {output_path}")
    logging.debug(f"[write_to_excel] Листов для записи: {len(sheets_data)}")
    for sheet_name, sheet_data in sheets_data.items():
        if sheet_data is not None and len(sheet_data) > 0:
            df, params = sheet_data
            if df is not None and isinstance(df, pd.DataFrame):
                logging.debug(f"[write_to_excel] Лист {sheet_name}: shape={df.shape}, колонок={len(df.columns)}")
                if len(df) == 0:
                    logging.warning(f"[write_to_excel] [WARN] Лист {sheet_name} ПУСТОЙ (0 строк)!")
                else:
                    if _debug_enabled():
                        logging.debug(f"[write_to_excel] Лист {sheet_name} первые 3 строки:\n{df.head(3).to_string()}")
            else:
                logging.warning(f"[write_to_excel] [WARN] Лист {sheet_name}: DataFrame равен None")
        else:
            logging.warning(f"[write_to_excel] [WARN] Лист {sheet_name}: sheet_data равен None или пуст")

    params = f"({output_path})"
    _warn_if_long_path(output_path)
    
    try:
        # Сортировка листов для main-файла: только если в конфиге включено apply_sort_to_main
        if APPLY_SORT_TO_MAIN:
            _sort_sheets_by_config(sheets_data, "write_to_excel")

        # Определяем порядок листов: по SHEET_ORDER из config, затем остальные по алфавиту
        if SHEET_ORDER:
            ordered_sheets = [s for s in SHEET_ORDER if s in sheets_data]
            remaining = sorted([s for s in sheets_data if s not in SHEET_ORDER])
            ordered_sheets = ordered_sheets + remaining
        else:
            other_sheets = [s for s in sheets_data if s != "SUMMARY"]
            ordered_sheets = ["SUMMARY"] + sorted(other_sheets)
        
        # ОПТИМИЗАЦИЯ: Параллельная подготовка DataFrame с преобразованием типов по COLUMN_FORMATS
        def _prepare_sheet_for_write(sheet_name):
            if sheet_name not in sheets_data or sheets_data[sheet_name] is None:
                return sheet_name, None
            sheet_data = sheets_data[sheet_name]
            if len(sheet_data) < 1 or sheet_data[0] is None:
                return sheet_name, None
            df, params_sheet = sheet_data
            extra_fmt = params_sheet.get("column_format_rules") if isinstance(params_sheet, dict) else None
            if not any(_format_rule_has_column_selector(r) for r in _iter_sheet_format_rules(sheet_name, extra_fmt)):
                # PERF-08: правил COLUMN_FORMATS для листа нет — преобразовывать нечего, пишем без копии
                return sheet_name, (df, params_sheet)
            df_write = df.copy()
            try:
                apply_column_format_conversion(df_write, sheet_name, extra_rules=extra_fmt)
            except Exception as ex:
                logging.exception(
                    f"[COLUMN_FORMATS] Ошибка преобразования типов для листа «{sheet_name}»: {ex}. "
                    "Используется копия без преобразования."
                )
                df_write = df.copy()
            return sheet_name, (df_write, params_sheet)

        sheets_to_prepare = [s for s in ordered_sheets if s in sheets_data and sheets_data[s] is not None]
        prepared_sheets = {}
        if sheets_to_prepare and COLUMN_FORMATS:
            with ThreadPoolExecutor(max_workers=min(MAX_WORKERS_IO, len(sheets_to_prepare))) as executor:
                futures = {executor.submit(_prepare_sheet_for_write, sn): sn for sn in sheets_to_prepare}
                for fut in as_completed(futures):
                    sn = futures[fut]
                    try:
                        _sn, data = fut.result()
                    except Exception as ex:
                        logging.exception(
                            f"[write_to_excel] Поток подготовки листа «{sn}» завершился с ошибкой: {ex}"
                        )
                        _sd = sheets_data.get(sn)
                        if _sd is not None and len(_sd) >= 2 and _sd[0] is not None:
                            prepared_sheets[sn] = (_sd[0].copy(), _sd[1])
                        continue
                    if data is not None:
                        prepared_sheets[sn] = data
        # Листы без правил COLUMN_FORMATS или без параллельной подготовки — берём исходные данные
        for sn in ordered_sheets:
            if sn not in prepared_sheets and sn in sheets_data and sheets_data[sn] is not None:
                prepared_sheets[sn] = sheets_data[sn]

        if EXCEL_WRITER == "write_only":
            # PERF-07 (этап 2): потоковая запись по плану — тот же вид книги, меньше времени и памяти
            from src.excel_write_only import write_workbook

            plans = []
            for sheet_name in ordered_sheets:
                data = prepared_sheets.get(sheet_name)
                if data is None or len(data) < 1 or data[0] is None:
                    logging.warning(f"[write_to_excel] Пропущен лист {sheet_name}: данные отсутствуют или равны None")
                    continue
                plans.append(_build_write_only_sheet_plan(sheet_name, data[0], data[1], use_color_scheme))
            write_workbook(output_path, plans, active_title="SUMMARY")
            logging.info(f"[write_to_excel] Книга записана потоково (write_only): листов {len(plans)}")
            return

        # Создаем Excel файл с помощью pandas ExcelWriter
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # ОПТИМИЗАЦИЯ: Сначала записываем все данные (последовательно, т.к. ExcelWriter не поддерживает параллелизм)
            for sheet_name in ordered_sheets:
                if sheet_name not in prepared_sheets or prepared_sheets[sheet_name] is None:
                    logging.warning(f"[write_to_excel] Пропущен лист {sheet_name}: данные отсутствуют или равны None")
                    continue
                sheet_data = prepared_sheets[sheet_name]
                if len(sheet_data) < 1 or sheet_data[0] is None:
                    logging.warning(f"[write_to_excel] Пропущен лист {sheet_name}: DataFrame равен None")
                    continue
                
                df_write, params_sheet = sheet_data
                logging.debug(f"[write_to_excel] Записываем лист {sheet_name}...")
                logging.debug(f"[write_to_excel] DataFrame shape: {df_write.shape}, колонок: {len(df_write.columns)}")
                if len(df_write) == 0:
                    logging.warning(f"[write_to_excel] [WARN] Лист {sheet_name} ПУСТОЙ перед записью (0 строк)")
                else:
                    if _debug_enabled():
                        logging.debug(f"[write_to_excel] Первые 3 строки перед записью:\n{df_write.head(3).to_string()}")

                df_write.to_excel(writer, index=False, sheet_name=sheet_name)
                logging.info(f"Лист Excel записан: {sheet_name} (строк: {len(df_write)}, колонок: {len(df_write.columns)})")
            
            # ОПТИМИЗАЦИЯ: Форматируем листы последовательно (openpyxl не thread-safe для параллельной записи)
            # Примечание: Параллелизация форматирования Excel была откачена, т.к. openpyxl не thread-safe
            # и параллельная запись в один файл создает блокировки, замедляющие выполнение
            for sheet_name in ordered_sheets:
                # ОПТИМИЗАЦИЯ v5.0: Проверка на None перед форматированием
                if sheet_name not in sheets_data or sheets_data[sheet_name] is None:
                    logging.warning(f"[write_to_excel] Пропущен лист {sheet_name} при форматировании: данные отсутствуют или равны None")
                    continue
                
                sheet_data = sheets_data[sheet_name]
                if len(sheet_data) < 1 or sheet_data[0] is None:
                    logging.warning(f"[write_to_excel] Пропущен лист {sheet_name} при форматировании: DataFrame равен None")
                    continue
                
                df, params_sheet = sheet_data
                ws = writer.sheets[sheet_name]
                written = prepared_sheets.get(sheet_name)
                df_written = written[0] if written is not None and len(written) >= 1 else None
                _format_sheet(ws, df, params_sheet, use_color_scheme=use_color_scheme, df_written=df_written)
                logging.info(f"Лист Excel сформирован: {sheet_name} (строк: {len(df)}, колонок: {len(df.columns)})")
            
            # Делаем SUMMARY лист активным по умолчанию (если он есть в файле)
            try:
                if "SUMMARY" in writer.book.sheetnames:
                    writer.book.active = writer.book.sheetnames.index("SUMMARY")
                else:
                    writer.book.active = 0
            except Exception as ex:
                logging.warning(f"[write_to_excel] Не удалось выставить активный лист: {ex}")
                try:
                    writer.book.active = 0
                except Exception:
                    pass
            # Не вызывать writer.book.save() здесь: контекстный менеджер ExcelWriter при выходе из ``with``
            # сам сохраняет файл. Повторное сохранение на тот же путь часто даёт повреждённый ZIP (xlsx не открывается).

        # Логируем успешное завершение
        
    except Exception as ex:
        # BUG-01: ошибку не глотаем — traceback в лог, недописанный файл удаляем, исключение — вызывающему
        logging.exception(f"[ERROR] write_to_excel {params} — {ex}")
        _remove_partial_file(output_path)
        if isinstance(ex, PermissionError):
            raise OutputWriteError(
                f"Нет доступа к файлу {output_path}. Если он открыт в Excel — закройте его и повторите запуск."
            ) from ex
        raise OutputWriteError(f"Не удалось записать Excel {output_path}: {ex}") from ex


# === Форматирование листа ===
# При AUTO-ширине не сканируем весь столбец (на крупных листах это десятки миллионов обращений к ячейкам):
# заголовок + первые N строк данных. Фиксированная ширина (число в col_width_mode) не меняется.
_AUTO_COLUMN_WIDTH_MAX_DATA_ROWS = 500


def _excel_cell_text(val: Any) -> Optional[str]:
    """
    Текст значения так, как его увидит расчёт ширины после записи pandas → openpyxl
    (pandas ExcelWriter: пропуск → пустая ячейка, целые → int, дробные → float, Timestamp → datetime).
    None — пустая ячейка.
    """
    if val is None:
        return None
    if isinstance(val, str):
        return val
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(val, pd.Timestamp):
        return str(val.to_pydatetime())
    if isinstance(val, (bool, np.bool_)):
        return str(bool(val))
    if isinstance(val, (int, np.integer)):
        return str(int(val))
    if isinstance(val, (float, np.floating)):
        return str(float(val))
    if isinstance(val, (datetime, date_cls)):
        return str(val)
    return str(val)


def _content_lengths_from_df(df: pd.DataFrame, max_rows: int = _AUTO_COLUMN_WIDTH_MAX_DATA_ROWS) -> List[int]:
    """PERF-04: максимальная длина текста в первых max_rows строках каждой колонки (без обхода ячеек листа)."""
    head = df.iloc[:max_rows]
    lengths: List[int] = []
    for j in range(head.shape[1]):
        best = 0
        for v in head.iloc[:, j].tolist():
            t = _excel_cell_text(v)
            if t is not None and len(t) > best:
                best = len(t)
        lengths.append(best)
    return lengths


def calculate_column_width(col_name, ws, params, col_num, data_len: Optional[int] = None, header_value: Any = None):
    """
    Вычисляет ширину колонки на основе параметров и содержимого.

    - col_width_mode == "AUTO": ширина по содержимому в пределах [min_col_width, max_col_width]
      (оценка по заголовку и первым N строкам данных, см. ``_AUTO_COLUMN_WIDTH_MAX_DATA_ROWS``).
    - col_width_mode == число (или строка-число): фиксированная ширина, min/max не используются.
    - Иначе: ширина по содержимому, ограниченная min/max.
    ws=None (потоковая запись, PERF-07): заголовок — header_value, длина данных — data_len.
    """
    # Получаем параметры для конкретной колонки (если добавлена через merge — MERGE_FIELDS_ADVANCED)
    added_cols_width = params.get("added_columns_width", {})
    if col_name in added_cols_width:
        col_params = added_cols_width[col_name]
        max_width = col_params.get("max_width") or params.get("max_col_width", 30)
        width_mode = col_params.get("width_mode", "AUTO")
        min_width = col_params.get("min_width") or params.get("min_col_width", 8)
    else:
        # Общие параметры для листа (из input_files, summary_sheet, stat_file_params и т.д.)
        max_width = params.get("max_col_width", 30)
        width_mode = params.get("col_width_mode", "AUTO")
        min_width = params.get("min_col_width", 8)

    # Фиксированная ширина: число (в т.ч. если в JSON пришло строкой "50")
    try:
        if isinstance(width_mode, (int, float)):
            return max(1, int(width_mode))
        if isinstance(width_mode, str) and width_mode.strip() and width_mode.strip().upper() != "AUTO":
            fixed = float(width_mode.strip())
            if fixed > 0:
                return max(1, int(fixed))
    except (ValueError, TypeError):
        pass

    # Вычисляем ширину на основе содержимого (выборка строк — ускорение; фиксированный режим выше уже обработан)
    content_width = min_width
    hval = ws.cell(row=1, column=col_num).value if ws is not None else header_value
    if hval is not None:
        content_width = max(content_width, len(str(hval)))
    if data_len is not None:
        # Длина данных посчитана заранее по записанному DataFrame (PERF-04)
        content_width = max(content_width, data_len)
    elif ws is not None and ws.max_row >= 2:
        last_scan = min(ws.max_row, 1 + _AUTO_COLUMN_WIDTH_MAX_DATA_ROWS)
        for row_idx in range(2, last_scan + 1):
            val = ws.cell(row=row_idx, column=col_num).value
            if val is not None:
                content_width = max(content_width, len(str(val)))

    if width_mode == "AUTO" or (isinstance(width_mode, str) and str(width_mode).strip().upper() == "AUTO"):
        # Автоматически: уместить между min и max
        final_width = min(content_width, max_width)
        final_width = max(final_width, min_width)
    else:
        # Резерв: ограничить содержимое min/max
        final_width = min(content_width, max_width)
        final_width = max(final_width, min_width)

    return final_width


def _build_excel_number_format(rule):
    """
    Строит строку формата Excel для числовых ячеек по правилу COLUMN_FORMATS.
    В коде формата Excel точка (.) — всегда десятичный разделитель; запятая (,) — разделитель разрядов.
    Строка "#.##0" даёт дробную часть (0 отображается как ",0"). Для целых без дробной части
    используем только "#,##0" или "0" (без точки в коде формата).

    Args:
        rule (dict): Элемент из COLUMN_FORMATS с data_type="number"

    Returns:
        str: Строка формата для cell.number_format (напр. "#,##0" для целых, "#,##0.00" для дробных)
    """
    decimal_places = int(rule.get("decimal_places", 0))
    decimal_sep = rule.get("decimal_separator", ",")
    thousands = rule.get("thousands_separator", True)
    # Целое число (0 знаков после запятой): в коде формата НЕ должно быть точки (.) — иначе Excel
    # интерпретирует её как десятичный разделитель и показывает ",0". Стандарт: "#,##0" или "0".
    if decimal_places == 0:
        return "#,##0" if thousands else "0"
    # Дробная часть: в Excel в коде формата десятичный разделитель — точка
    if decimal_sep == ",":
        # Отображение с запятой как десятичным разделителем задаётся локалью Excel; в коде оставляем точку
        dec_part = "." + "0" * decimal_places
    else:
        dec_part = "." + "0" * decimal_places
    int_part = "#,##0" if thousands else "0"
    return int_part + dec_part


def _build_excel_date_format(rule):
    """
    Строит строку формата Excel для дат по правилу COLUMN_FORMATS.

    Args:
        rule (dict): Элемент из COLUMN_FORMATS с data_type="date"

    Returns:
        str: Строка формата для cell.number_format (напр. "yyyy-mm-dd" или "dd/mm/yyyy")
    """
    fmt = (rule.get("date_format") or "YYYY-MM-DD").strip().upper()
    # Excel openpyxl: yyyy-mm-dd, dd/mm/yyyy
    if "DD/MM/YYYY" in fmt or "DD-MM-YYYY" in fmt:
        return "dd/mm/yyyy"
    return "yyyy-mm-dd"


def _config_date_format_to_pandas(fmt: Optional[str]) -> Optional[str]:
    """
    Преобразует строку формата даты из config (YYYY-MM-DD, DD/MM/YYYY и т.д.) в формат pandas.
    Возвращает None, если fmt пустой или не распознан (тогда pd.to_datetime будет без format).
    """
    if not fmt or not isinstance(fmt, str):
        return None
    fmt = fmt.strip().upper()
    # YYYY, DD, HH, SS — однозначны; MM в дате — месяц (%m), во времени (HH:MM:SS) — минуты (%M)
    fmt = fmt.replace("YYYY", "%Y").replace("DD", "%d").replace("HH", "%H").replace("SS", "%S")
    fmt = fmt.replace(":MM:", ":%M:")  # минуты во времени
    fmt = fmt.replace("MM", "%m")       # оставшиеся MM — месяц
    return fmt if "%" in fmt else None


@functools.lru_cache(maxsize=65536)
def _normalize_header_cached(name: str) -> str:
    from src.csv_headers import normalize_csv_column_header

    return normalize_csv_column_header(name)


def _normalize_column_name_for_format_match(name: Optional[str]) -> str:
    """
    Имя колонки для сравнения с ``except_columns`` / ``columns`` в COLUMN_FORMATS.
    Делегирует в csv_headers (BOM, NFKC, пробелы); строки кешируются (PERF-05).
    """
    if isinstance(name, str):
        return _normalize_header_cached(name)
    from src.csv_headers import normalize_csv_column_header

    return normalize_csv_column_header(name)


# PERF-05: нормализованные except_columns / columns / column_prefixes правила — один раз на правило
_format_rule_sets_cache: Dict[int, Tuple[Mapping[str, Any], frozenset, frozenset, Tuple[str, ...]]] = {}


def _format_rule_sets(rule: Mapping[str, Any]) -> Tuple[frozenset, frozenset, Tuple[str, ...]]:
    cached = _format_rule_sets_cache.get(id(rule))
    if cached is not None and cached[0] is rule:
        return cached[1], cached[2], cached[3]
    except_norm = frozenset(_normalize_column_name_for_format_match(x) for x in (rule.get("except_columns") or []))
    allowed_norm = frozenset(_normalize_column_name_for_format_match(x) for x in (rule.get("columns") or []))
    prefixes = tuple(_normalize_column_name_for_format_match(x) for x in (rule.get("column_prefixes") or []))
    _format_rule_sets_cache[id(rule)] = (rule, except_norm, allowed_norm, prefixes)
    return except_norm, allowed_norm, prefixes


def _format_header_match_keys(col_name: str) -> set[str]:
    """
    Ключи для сопоставления с except_columns / columns:
    полное имя и «лист» после «=>» (ORG_UNIT_V20=>TB_SHORT_NAME → TB_SHORT_NAME).
    """
    header_norm = _normalize_column_name_for_format_match(col_name)
    keys = {header_norm}
    if "=>" in header_norm:
        leaf = header_norm.split("=>")[-1].strip()
        if leaf:
            keys.add(leaf)
    return keys


def _normalize_string_for_numeric_cell(val: Any) -> str:
    """
    Подготовка значения ячейки (после чтения CSV всё приходит строкой) к ``pd.to_numeric``:
    удаляются разряды — обычный пробел, NBSP, узкий NBSP и др.; запятая как десятичный разделитель.
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    if s in ("", "nan", "None", "-"):
        return ""
    for sep in ("\u00a0", "\u202f", "\u2009", "\u2007", " "):
        s = s.replace(sep, "")
    return s.replace(",", ".")


def _column_matches_format_rule(col_name: str, rule: Mapping[str, Any]) -> bool:
    """Проверка, попадает ли колонка под правило columns / except_columns / column_prefixes."""
    header_keys = _format_header_match_keys(col_name)
    header_norm = _normalize_column_name_for_format_match(col_name)
    except_cols = rule.get("except_columns") or []
    columns_list = rule.get("columns") or []
    except_norm, allowed_norm, prefixes = _format_rule_sets(rule)
    if except_cols:
        # Не применять правило, если полное имя или суффикс после => в except
        return header_keys.isdisjoint(except_norm)
    if columns_list:
        return not header_keys.isdisjoint(allowed_norm)
    for pnorm in prefixes:
        if pnorm and header_norm.startswith(pnorm):
            return True
    return False


def _format_rule_has_column_selector(rule: Mapping[str, Any]) -> bool:
    return bool(rule.get("except_columns") or rule.get("columns") or rule.get("column_prefixes"))


def _iter_sheet_format_rules(
    sheet_name: str,
    extra_rules: Optional[Sequence[Mapping[str, Any]]] = None,
) -> List[Mapping[str, Any]]:
    """Правила COLUMN_FORMATS для листа + дополнительные правила из params (MANAGER_STATS и др.)."""
    rules: List[Mapping[str, Any]] = [
        r for r in COLUMN_FORMATS if r.get("sheet") == sheet_name
    ]
    if extra_rules:
        rules.extend(r for r in extra_rules if isinstance(r, dict))
    return rules


@debug_timed()
def apply_column_format_conversion(
    df: pd.DataFrame,
    sheet_name: str,
    extra_rules: Optional[Sequence[Mapping[str, Any]]] = None,
) -> None:
    """
    Преобразует типы колонок в DataFrame по правилам COLUMN_FORMATS перед записью в Excel.
    Вызывается для копии DataFrame перед to_excel, чтобы Excel получал числа/даты, а не строки.
    Для числа с 0 знаков после запятой записываются целые (без .0), чтобы Excel не показывал дробную часть.

    Args:
        df (pd.DataFrame): DataFrame листа (будет изменён in-place)
        sheet_name (str): Имя листа
        extra_rules: Доп. правила из params листа (без поля sheet)
    """
    for rule in _iter_sheet_format_rules(sheet_name, extra_rules):
        if not _format_rule_has_column_selector(rule):
            continue
        dtype = (rule.get("data_type") or "general").lower()
        for col in df.columns:
            if not _column_matches_format_rule(col, rule):
                continue
            col_data = df[col]
            if isinstance(col_data, pd.DataFrame):
                logging.warning(
                    f"[COLUMN_FORMATS] Лист «{sheet_name}»: имя колонки «{col}» дублируется — пропуск преобразования"
                )
                continue
            try:
                if dtype == "number":
                    # После read_csv_file значения строковые; убираем разряды (пробел/NBSP), запятую в десятичную точку.
                    # Текстовые значения (имена ТБ/ГОСБ после merge) не затираем в NA.
                    original = col_data
                    # Поэлементно: векторный вариант через .str (6 проходов) на object-колонках медленнее в ~2 раза
                    normalized = original.map(_normalize_string_for_numeric_cell)
                    ser = pd.to_numeric(normalized, errors="coerce")
                    decimal_places = int(rule.get("decimal_places", 0))
                    non_empty_text = normalized.astype(str).str.len() > 0
                    failed_numeric = ser.isna() & non_empty_text
                    if failed_numeric.any():
                        # Смешанная колонка: числа → int/float, остальное — исходный текст
                        if decimal_places == 0:
                            num_vals = ser.round().astype("Int64")
                        else:
                            num_vals = ser
                        mixed = original.astype(object).copy()
                        ok = ~failed_numeric
                        mixed.loc[ok] = num_vals.loc[ok]
                        df[col] = mixed
                    else:
                        if decimal_places == 0:
                            df[col] = ser.astype("Int64")
                        else:
                            df[col] = ser
                elif dtype == "date":
                    raw_ser = col_data.astype(str).str.strip()
                    pd_fmt = _config_date_format_to_pandas(rule.get("date_format"))
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        if pd_fmt:
                            parsed = pd.to_datetime(col_data, format=pd_fmt, errors="coerce")
                        else:
                            parsed = pd.to_datetime(col_data, errors="coerce")
                    nat_mask = parsed.isna()
                    # Повтор имеет смысл только без явного формата: pandas заново угадывает формат
                    # по оставшимся строкам. С тем же явным форматом он всегда даёт тот же NaT (BUG-09).
                    if not pd_fmt and nat_mask.any():
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", UserWarning)
                            second = pd.to_datetime(col_data.loc[nat_mask], errors="coerce")
                        parsed = parsed.fillna(second)
                    still_nat = parsed.isna()
                    if still_nat.any():
                        parsed = parsed.astype(object)
                        parsed.loc[still_nat] = raw_ser.loc[still_nat].values
                    df[col] = parsed
                elif dtype == "text":
                    df[col] = col_data.astype(str)
            except Exception as ex:
                logging.warning(
                    f"[COLUMN_FORMATS] Лист «{sheet_name}», колонка «{col}»: преобразование пропущено: {ex}"
                )


def _column_indices_covered_by_column_formats(
    sheet_name: str,
    col_names: List[Any],
    extra_rules: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Set[int]:
    """
    Возвращает номера столбцов (1-based), к которым будут применены правила COLUMN_FORMATS на листе.
    Нужно, чтобы не выставлять общий alignment второй раз тем же ячейкам в _format_sheet (перенос и пр. из правил сохраняются).
    """
    rules_for_sheet = _iter_sheet_format_rules(sheet_name, extra_rules)
    covered: Set[int] = set()
    if not rules_for_sheet:
        return covered
    for rule in rules_for_sheet:
        if not _format_rule_has_column_selector(rule):
            continue
        for col_idx, raw_header in enumerate(col_names, start=1):
            header = str(raw_header) if raw_header is not None else ""
            if _column_matches_format_rule(header, rule):
                covered.add(col_idx)
    return covered


class _StyleIdCopier:
    """
    Быстрое назначение одного и того же стиля многим ячейкам (PERF-01).

    Присваивание ``cell.alignment = X`` / ``cell.number_format = F`` каждый раз хеширует объект стиля
    и ищет его в индексе книги. Здесь стиль ставится первой ячейке обычным способом, а остальным
    копируется уже вычисленный индекс в их StyleArray — результат в xlsx тот же, в разы быстрее.
    Остальные атрибуты стиля ячейки (шрифт, заливка, формат) не затрагиваются.
    """

    __slots__ = ("attr", "value", "id_field", "style_id")

    _ID_FIELD = {"alignment": "alignmentId", "number_format": "numFmtId"}

    def __init__(self, attr: str, value: Any) -> None:
        self.attr = attr
        self.value = value
        self.id_field = self._ID_FIELD[attr]
        self.style_id: Optional[int] = None

    def apply(self, cell: Any) -> None:
        if self.style_id is None:
            setattr(cell, self.attr, self.value)
            self.style_id = getattr(cell._style, self.id_field)
        else:
            # Как в openpyxl StyleDescriptor.__set__: у ячейки без стиля StyleArray создаётся лениво
            if not cell._style:
                cell._style = StyleArray()
            setattr(cell._style, self.id_field, self.style_id)


def apply_column_formats(
    ws: Any,
    sheet_name: str,
    extra_rules: Optional[Sequence[Mapping[str, Any]]] = None,
) -> None:
    """
    Применяет к ячейкам листа Excel формат числа/даты и выравнивание по правилам COLUMN_FORMATS.
    Вызывается из _format_sheet после базового форматирования. Обрабатывает только колонки,
    перечисленные в правилах для данного листа (batch по колонкам).
    Имена колонок берутся из заголовка листа (ws), не из DataFrame.

    Args:
        ws: openpyxl Worksheet
        sheet_name (str): Имя листа
    """
    header_cells = list(ws[1])
    col_names = [c.value for c in header_cells]
    rules_for_sheet = _iter_sheet_format_rules(sheet_name, extra_rules)
    if not rules_for_sheet:
        return

    # На листах из skip_data_alignment_sheets: number_format остаётся, Alignment данных — нет
    try:
        _skip_pats = SKIP_DATA_ALIGNMENT_SHEETS
    except NameError:
        _skip_pats = None
    skip_data_align = sheet_skips_data_alignment(sheet_name, _skip_pats)

    for rule in rules_for_sheet:
        if not _format_rule_has_column_selector(rule):
            continue
        data_type = (rule.get("data_type") or "general").lower()
        # Строка формата Excel
        if data_type == "number":
            num_fmt = _build_excel_number_format(rule)
        elif data_type == "date":
            num_fmt = _build_excel_date_format(rule)
        else:
            num_fmt = None
        # Выравнивание
        h = rule.get("horizontal", "left").lower()
        v = rule.get("vertical", "center").lower()
        wrap = bool(rule.get("wrap_text", False))
        h_map = {"left": "left", "center": "center", "right": "right"}
        v_map = {"top": "top", "center": "center", "bottom": "bottom"}
        alignment = Alignment(
            horizontal=h_map.get(h, "left"),
            vertical=v_map.get(v, "center"),
            wrap_text=wrap,
        )
        # Обход по индексу столбца: совпадение с except/columns/prefixes
        for col_idx, raw_header in enumerate(col_names, start=1):
            header = str(raw_header) if raw_header is not None else ""
            if not _column_matches_format_rule(header, rule):
                continue
            # Для числа с 0 знаков после запятой: записать в ячейку целое значение (1, 2), а не 1.0, 2.0,
            # иначе Excel в части локалей отображает "1,0"
            force_int = (data_type == "number" and int(rule.get("decimal_places", 0)) == 0)
            fmt_setter = _StyleIdCopier("number_format", num_fmt) if num_fmt is not None else None
            align_setter = _StyleIdCopier("alignment", alignment) if not skip_data_align else None
            for (cell,) in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=col_idx, max_col=col_idx):
                if fmt_setter is not None:
                    fmt_setter.apply(cell)
                # int уже целое — пропускаем (PERF-05); bool — тоже int, и раньше он не менялся
                if force_int and cell.value is not None and not isinstance(cell.value, int):
                    try:
                        raw = _normalize_string_for_numeric_cell(cell.value)
                        if raw != "":
                            v = float(raw)
                            if v == int(v):
                                cell.value = int(v)
                    except (TypeError, ValueError, OverflowError):
                        pass
                if align_setter is not None:
                    align_setter.apply(cell)
            logging.debug(
                f"[COLUMN_FORMATS] Применён формат к листу {sheet_name}, колонка {col_idx} "
                f"«{raw_header}» (тип: {data_type})"
            )
    return


@debug_timed()
def _format_sheet(ws, df, params, use_color_scheme: bool = True, df_written: Optional[pd.DataFrame] = None):
    header_font = Font(bold=True)
    align_center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    align_data = Alignment(horizontal="left", vertical="center", wrap_text=True)

    # ОПТИМИЗАЦИЯ: Batch-операции для заголовков - вычисляем все ширины сразу
    header_cells = list(ws[1])
    column_widths = {}
    # PERF-04: длины данных — по DataFrame, который записан на лист (а не ws.cell() по 500 строк на колонку)
    data_lengths: Optional[List[int]] = None
    if df_written is not None and df_written.shape[1] == len(header_cells):
        data_lengths = _content_lengths_from_df(df_written)

    for col_num, cell in enumerate(header_cells, 1):
        cell.font = header_font
        cell.alignment = align_center
        col_letter = get_column_letter(col_num)
        col_name = cell.value
        
        # Вычисляем ширину колонки
        width = calculate_column_width(
            col_name, ws, params, col_num,
            data_len=data_lengths[col_num - 1] if data_lengths is not None else None,
        )
        column_widths[col_letter] = width
        
        # Определяем режим для логирования
        width_mode_info = params.get("col_width_mode", "AUTO")
        added_cols_width = params.get("added_columns_width", {})
        if col_name in added_cols_width:
            width_mode_info = added_cols_width[col_name].get("width_mode", "AUTO")
        
        logging.debug(f"[COLUMN WIDTH] {ws.title}: колонка '{col_name}' -> ширина {width} (режим: {width_mode_info})")
    
    # Применяем все ширины колонок сразу (batch-операция)
    for col_letter, width in column_widths.items():
        ws.column_dimensions[col_letter].width = width

    # Применяем цветовую схему (в режиме «только консистентность» не применяем)
    if use_color_scheme:
        apply_color_scheme(ws, ws.title)

    # Выравнивание и перенос для данных: столбцы из COLUMN_FORMATS обрабатывает только apply_column_formats
    # (там wrap_text и т.д. как в конфиге), остальные — общий стиль с переносом по словам как раньше.
    # Листы из performance.skip_data_alignment_sheets — без Alignment на данных (ускорение).
    skip_data_align = sheet_skips_data_alignment(
        ws.title, SKIP_DATA_ALIGNMENT_SHEETS
    )
    if ws.max_row > 1:
        col_names_header = [c.value for c in header_cells]
        extra_fmt = params.get("column_format_rules") if isinstance(params, dict) else None
        if not skip_data_align:
            cols_covered_by_rules = _column_indices_covered_by_column_formats(
                ws.title, col_names_header, extra_rules=extra_fmt
            )
            align_setter = _StyleIdCopier("alignment", align_data)
            for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column):
                for cell in row:
                    if cell.column in cols_covered_by_rules:
                        continue
                    align_setter.apply(cell)
        else:
            logging.debug(
                f"[_format_sheet] {ws.title}: Alignment данных пропущен "
                f"(performance.skip_data_alignment_sheets)"
            )

        # Формат чисел/дат и выравнивание по правилам (включая wrap_text из конфига)
        apply_column_formats(ws, ws.title, extra_rules=extra_fmt)

    # Закрепление строк и столбцов
    ws.freeze_panes = params.get("freeze", "A2")
    # Автофильтр: при некорректном dimensions (пустой лист, сбой расчёта границ) openpyxl может выбросить
    # исключение или записать невалидный диапазон — тогда файл xlsx становится нечитаемым.
    try:
        if ws.max_row and ws.max_column and ws.dimensions:
            ws.auto_filter.ref = ws.dimensions
    except Exception as ex:
        logging.warning(f"[_format_sheet] Лист «{ws.title}»: автофильтр не применён: {ex}")

    
    # Возвращаем имя листа для логирования в параллельном режиме
    return ws.title




@debug_timed(hot=True)
def flatten_json_column_recursive(df, column, prefix=None, sheet=None, sep="; "):
    n_rows = len(df)
    n_errors = 0
    prefix = prefix if prefix is not None else column
    
    # Для CONTEST_FEATURE создаем копию с валидным JSON для парсинга
    # Сохраняем исходную колонку с тройными кавычками как есть
    original_column_data = None
    if column == "CONTEST_FEATURE" and column in df.columns:
        # Сохраняем исходные данные
        original_column_data = df[column].copy()
        
        # Создаем временную колонку для парсинга с заменой тройных кавычек
        temp_column = f"{column}_TEMP_PARSED"
        df[temp_column] = df[column].apply(lambda x: x.replace('"""', '"') if isinstance(x, str) else x)
        
        # Теперь будем парсить из временной колонки
        column_to_parse = temp_column
    else:
        column_to_parse = column

    def extract(obj, current_prefix):
        """Recursively flattens obj. Keeps the field itself and expands nested JSON
        if the value looks like a JSON string."""
        fields = {}
        if isinstance(obj, str):
            # Сначала пробуем распарсить JSON (для разворачивания)
            nested = safe_json_loads(obj)
            
            if isinstance(nested, (dict, list)):
                # keep original string (с тройными кавычками, если они были)
                fields[current_prefix] = obj
                fields.update(extract(nested, current_prefix))
                return fields
            else:
                # Если не удалось распарсить как JSON, сохраняем исходную строку
                fields[current_prefix] = obj
                return fields

        if isinstance(obj, dict):
            fields[current_prefix] = json.dumps(obj, ensure_ascii=False)
            for k, v in obj.items():
                new_prefix = f"{current_prefix} => {k}"
                fields.update(extract(v, new_prefix))
        elif isinstance(obj, list):
            if all(isinstance(x, (str, int, float, bool, type(None))) for x in obj):
                fields[current_prefix] = sep.join(str(x) for x in obj)
            else:
                fields[current_prefix] = json.dumps(obj, ensure_ascii=False)
                for idx, x in enumerate(obj):
                    item_prefix = f"{current_prefix} => [{idx}]"
                    fields.update(extract(x, item_prefix))
        else:
            if isinstance(obj, float) and pd.isna(obj):
                fields[current_prefix] = None
            else:
                fields[current_prefix] = obj
        return fields

            # ОПТИМИЗИРОВАННАЯ ВЕРСИЯ v2: Параллельный парсинг JSON с проверкой размера
    new_cols = {}
    
    # ОПТИМИЗАЦИЯ: Параллелизация только для больших данных (>5000 строк)
    # Для небольших данных накладные расходы превышают выигрыш
    PARALLEL_JSON_THRESHOLD = 5000
    
    if n_rows > PARALLEL_JSON_THRESHOLD:
        def parse_json_chunk(chunk_data):
            """Парсит chunk данных и возвращает словарь с результатами"""
            chunk_results = {}
            chunk_errors = 0
            chunk_idx, chunk_values = chunk_data
            for local_idx, val in enumerate(chunk_values):
                global_idx = chunk_idx + local_idx
                try:
                    parsed = None
                    if isinstance(val, str):
                        val = val.strip()
                        if val in {"", "-", "None", "null"}:
                            parsed = {}
                        else:
                            parsed = safe_json_loads(val)
                    elif isinstance(val, (dict, list)):
                        parsed = val
                    else:
                        parsed = {}
                    flat = extract(parsed, prefix)
                except Exception as ex:
                    logging.debug(f"Ошибка разбора JSON (строка {global_idx}): {ex}")
                    chunk_errors += 1
                    flat = {}
                
                for k, v in flat.items():
                    if k not in chunk_results:
                        chunk_results[k] = {}
                    chunk_results[k][global_idx] = v
            return chunk_results, chunk_errors
        
        # Разбиваем на chunks для параллельной обработки
        # Оптимизированный размер chunk: минимум 2000 строк на chunk
        chunk_size = max(2000, n_rows // MAX_WORKERS_IO)
        chunks = [(i * chunk_size, df[column_to_parse].iloc[i * chunk_size:(i + 1) * chunk_size].tolist()) 
                  for i in range((n_rows + chunk_size - 1) // chunk_size)]
        
        # Параллельная обработка chunks только если chunks > 1
        if len(chunks) > 1:
            from concurrent.futures import ThreadPoolExecutor as TPE
            with TPE(max_workers=min(MAX_WORKERS_IO, len(chunks))) as executor:
                chunk_data_list = list(executor.map(parse_json_chunk, chunks))
                chunk_results_list = [data[0] for data in chunk_data_list]
                n_errors += sum(data[1] for data in chunk_data_list)
            
            # Объединяем результаты
            for chunk_results in chunk_results_list:
                for k, v_dict in chunk_results.items():
                    if k not in new_cols:
                        new_cols[k] = [None] * n_rows
                    for idx, val in v_dict.items():
                        new_cols[k][idx] = val
        else:
            # Один chunk - обрабатываем последовательно
            chunk_results, chunk_errors = parse_json_chunk(chunks[0])
            n_errors += chunk_errors
            for k, v_dict in chunk_results.items():
                if k not in new_cols:
                    new_cols[k] = [None] * n_rows
                for idx, val in v_dict.items():
                    new_cols[k][idx] = val
    else:
        # Небольшие данные - последовательная обработка (быстрее из-за отсутствия накладных расходов)
        for idx, val in enumerate(df[column_to_parse]):
            try:
                parsed = None
                if isinstance(val, str):
                    val = val.strip()
                    if val in {"", "-", "None", "null"}:
                        parsed = {}
                    else:
                        parsed = safe_json_loads(val)
                elif isinstance(val, (dict, list)):
                    parsed = val
                else:
                    parsed = {}
                flat = extract(parsed, prefix)
            except Exception as ex:
                logging.debug(f"Ошибка разбора JSON (строка {idx}): {ex}")
                n_errors += 1
                flat = {}
            for k, v in flat.items():
                if k not in new_cols:
                    new_cols[k] = [None] * n_rows
                new_cols[k][idx] = v
    # Оставлять только реально созданные колонки (не пустые); пакетная вставка — без фрагментации DataFrame
    cols_to_add = {
        col_name: values
        for col_name, values in new_cols.items()
        if any(x is not None for x in values)
    }
    if cols_to_add:
        cols_new = {k: v for k, v in cols_to_add.items() if k not in df.columns}
        cols_overwrite = {k: v for k, v in cols_to_add.items() if k in df.columns}
        if cols_new:
            new_columns_df = pd.DataFrame(cols_new, index=df.index)
            df = pd.concat([df, new_columns_df], axis=1)
        for col_name, values in cols_overwrite.items():
            df[col_name] = values
        logging.debug(
            f"[flatten_json] {column}: пакетно добавлено {len(cols_new)} колонок, "
            f"перезаписано {len(cols_overwrite)}"
        )
    
    # Для CONTEST_FEATURE восстанавливаем исходную колонку с тройными кавычками
    if original_column_data is not None:
        # Восстанавливаем исходную колонку с тройными кавычками
        df[column] = original_column_data
        
        # Удаляем временную колонку
        if temp_column in df.columns:
            df = df.drop(columns=[temp_column])
        
        logging.info("[CONTEST_FEATURE] Исходная колонка восстановлена с тройными кавычками")

    logging.info(f"[INFO] {column} → новых колонок: {len(new_cols)}")
    logging.info(f"[INFO] Все новые колонки: {list(new_cols.keys())}")
    return df



# ОПТИМИЗАЦИЯ v5.0: Кэш для цветовых схем (избегаем повторной генерации)
_color_scheme_cache = None
_color_scheme_cache_key = None

# Расхождения по числу полей в CSV (строка с большим/меньшим числом колонок, чем заголовок)
_csv_column_mismatches: List[Dict[str, Any]] = []
_csv_mismatches_lock = threading.Lock()

def generate_dynamic_color_scheme_from_merge_fields():
    """
    Автоматически генерирует элементы цветовой схемы на основе MERGE_FIELDS_ADVANCED.
    Добавляет правила для колонок, которые создаются через merge операции.
    """
    dynamic_scheme = []



    # Группируем по целевым листам (используем MERGE_FIELDS_ADVANCED — единый список правил)
    sheets_targets = {}
    for rule in MERGE_FIELDS_ADVANCED:
        sheet_dst = rule["sheet_dst"]
        sheet_src = rule["sheet_src"]
        columns = rule["column"]
        mode = rule.get("mode", "value")
        count_label = rule.get("count_label")
        count_aggregation = rule.get("count_aggregation", "size")

        if sheet_dst not in sheets_targets:
            sheets_targets[sheet_dst] = {}

        if sheet_src not in sheets_targets[sheet_dst]:
            sheets_targets[sheet_dst][sheet_src] = []

        # Формируем имена колонок, которые будут созданы (как в add_fields_to_sheet: COUNT_* или COUNT_agg_label)
        if mode == "count" and count_label is not None:
            new_col_name = f"{sheet_src}=>COUNT_{count_aggregation}_{count_label}"
            sheets_targets[sheet_dst][sheet_src].append(new_col_name)
        else:
            for col in columns:
                if mode == "count":
                    new_col_name = f"{sheet_src}=>COUNT_{col}"
                else:
                    new_col_name = f"{sheet_src}=>{col}"
                sheets_targets[sheet_dst][sheet_src].append(new_col_name)

    # Создаем цветовые схемы для каждой комбинации лист-источник
    color_palette = [
        ("FF9999", "2C3E50"),  # Светло-красный
        ("99FF99", "2C3E50"),  # Светло-зеленый
        ("9999FF", "FFFFFF"),  # Светло-синий
        ("FFFF99", "2C3E50"),  # Светло-желтый
        ("FF99FF", "2C3E50"),  # Светло-розовый
        ("99FFFF", "2C3E50"),  # Светло-голубой
        ("FFB366", "2C3E50"),  # Светло-оранжевый
        ("D8BFD8", "2C3E50"),  # Светло-фиолетовый
    ]

    color_idx = 0
    for sheet_dst, sources in sheets_targets.items():
        for sheet_src, columns in sources.items():
            if columns:  # Если есть колонки для этого источника
                bg_color, fg_color = color_palette[color_idx % len(color_palette)]

                dynamic_scheme.append({
                    "group": f"MERGE: {sheet_src} -> {sheet_dst}",
                    "header_bg": bg_color,
                    "header_fg": fg_color,
                    "column_bg": None,
                    "column_fg": None,
                    "style_scope": "header",
                    "sheets": [sheet_dst],
                    "columns": columns,
                    "auto_generated": True  # Маркер автогенерации
                })

                logging.debug(f"[DYNAMIC COLOR] Сгенерирована схема для {sheet_src} -> {sheet_dst}: {columns}")
                color_idx += 1

    return dynamic_scheme


def _all_color_schemes() -> List[Dict[str, Any]]:
    """COLOR_SCHEME из конфига + схемы, сгенерированные по MERGE_FIELDS_ADVANCED (с кешем)."""
    global _color_scheme_cache, _color_scheme_cache_key
    current_key = id(MERGE_FIELDS_ADVANCED)  # Простая проверка на изменение
    if _color_scheme_cache is None or _color_scheme_cache_key != current_key:
        _color_scheme_cache = COLOR_SCHEME + generate_dynamic_color_scheme_from_merge_fields()
        _color_scheme_cache_key = current_key
    return _color_scheme_cache


def apply_color_scheme(ws, sheet_name):
    """
    Окрашивает заголовки и/или всю колонку на листе Excel по схеме COLOR_SCHEME.
    Также применяет динамически сгенерированную схему из MERGE_FIELDS_ADVANCED.
    Все действия логируются напрямую в местах вызова.
    """
    all_color_schemes = _all_color_schemes()

    for color_conf in all_color_schemes:
        if sheet_name not in color_conf["sheets"]:
            continue

        # Список колонок: если пуст — значит все
        header_cells = list(ws[1])
        colnames = color_conf["columns"] if color_conf["columns"] else [cell.value for cell in header_cells]
        style_scope = color_conf.get("style_scope", "header")

        for colname in colnames:
            try:
                # Номер колонки по имени
                col_idx = [cell.value for cell in header_cells].index(colname) + 1
            except ValueError:
                continue  # нет такой колонки на этом листе

            # Окраска только заголовка
            if style_scope == "header":
                cell = ws.cell(row=1, column=col_idx)
                if color_conf.get("header_bg"):
                    cell.fill = PatternFill(start_color=color_conf["header_bg"], end_color=color_conf["header_bg"],
                                            fill_type="solid")
                if color_conf.get("header_fg"):
                    cell.font = Font(color=color_conf["header_fg"])
                # Логирование
                logging.debug(f"[INFO] Цветовая схема применена: лист {sheet_name}, колонка {colname}, стиль header, цвет {color_conf.get('header_bg', 'default')}")
            # Окраска всей колонки (если понадобится в будущем)
            elif style_scope == "all":
                for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=col_idx, max_col=col_idx):
                    for cell in row:
                        if cell.row == 1 and color_conf.get("header_bg"):
                            cell.fill = PatternFill(start_color=color_conf["header_bg"],
                                                    end_color=color_conf["header_bg"], fill_type="solid")
                            if color_conf.get("header_fg"):
                                cell.font = Font(color=color_conf["header_fg"])
                        elif color_conf.get("column_bg"):
                            cell.fill = PatternFill(start_color=color_conf["column_bg"],
                                                    end_color=color_conf["column_bg"], fill_type="solid")
                            if color_conf.get("column_fg"):
                                cell.font = Font(color=color_conf["column_fg"])
                logging.debug(f"[INFO] Цветовая схема применена: лист {sheet_name}, колонка {colname}, стиль all, цвет {color_conf.get('column_bg', 'default')}")


def _unique_by_key(df: pd.DataFrame, key_col: str, val_col: str) -> Dict[Any, List[Any]]:
    """{ключ: уникальные непустые значения val_col в порядке появления} — один проход вместо фильтра на ключ."""
    out: Dict[Any, List[Any]] = {}
    seen: Dict[Any, Set[Any]] = {}
    if df.empty:
        return out
    for k, v in zip(df[key_col], df[val_col]):
        if pd.isna(k) or pd.isna(v):
            continue
        bucket = seen.setdefault(k, set())
        if v not in bucket:
            bucket.add(v)
            out.setdefault(k, []).append(v)
    return out


def collect_summary_keys(dfs):
    """
    Собирает все реально существующие сочетания ключей SUMMARY
    (CONTEST_CODE, TOURNAMENT_CODE, REWARD_CODE, GROUP_CODE, GROUP_VALUE, INDICATOR_CODE, INDICATOR_ADD_CALC_TYPE),
    включая осиротевшие коды; GROUP_VALUE связан с конкретным GROUP_CODE.

    PERF-02: все выборки по коду считаются один раз (словари), а не фильтром DataFrame на каждую итерацию;
    набор строк — как в прежней версии (тест src/Tests/test_collect_summary_keys.py).
    BUG-13: строки отсортированы по ключам — порядок SUMMARY не зависит от PYTHONHASHSEED.
    """
    def _sheet(name: str) -> pd.DataFrame:
        df = dfs.get(name)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    rewards = _sheet("REWARD-LINK")
    tournaments = _sheet("TOURNAMENT-SCHEDULE")
    groups = _sheet("GROUP")
    reward_data = _sheet("REWARD")
    contest_data = _sheet("CONTEST-DATA")
    indicators = _sheet("INDICATOR")

    # --- Справочники «код конкурса → …» (порядок значений — как в исходных листах) ---
    tourns_by_contest = _unique_by_key(tournaments, "CONTEST_CODE", "TOURNAMENT_CODE") if not tournaments.empty else {}
    rewards_by_contest = _unique_by_key(rewards, "CONTEST_CODE", "REWARD_CODE") if not rewards.empty else {}
    # Конкурс для турнира / награды: первый непустой CONTEST_CODE среди строк с этим кодом
    contest_by_tourn = {k: v[0] for k, v in _unique_by_key(tournaments, "TOURNAMENT_CODE", "CONTEST_CODE").items()} if not tournaments.empty else {}
    contest_by_reward = {k: v[0] for k, v in _unique_by_key(rewards, "REWARD_CODE", "CONTEST_CODE").items()} if not rewards.empty else {}

    pairs_by_contest: Dict[Any, List[Tuple[str, str]]] = {}
    values_by_group_contest: Dict[Tuple[Any, str], List[Any]] = {}
    group_codes: List[Any] = []
    contests_by_group: Dict[Any, List[Any]] = {}
    if not groups.empty:
        g_seen: Set[Any] = set()
        pair_seen: Dict[Any, Set[Tuple[str, str]]] = {}
        for c, g, v in zip(groups["CONTEST_CODE"], groups["GROUP_CODE"], groups["GROUP_VALUE"]):
            if pd.notna(g) and g not in g_seen:
                g_seen.add(g)
                group_codes.append(g)
            if pd.notna(g) and pd.notna(v):
                pair = (str(g), str(v))
                ps = pair_seen.setdefault(c, set())
                if pair not in ps:
                    ps.add(pair)
                    pairs_by_contest.setdefault(c, []).append(pair)
        contests_by_group = _unique_by_key(groups, "GROUP_CODE", "CONTEST_CODE")
        for c, g, v in zip(groups["CONTEST_CODE"], groups["GROUP_CODE"], groups["GROUP_VALUE"]):
            if pd.isna(c) or pd.isna(g) or pd.isna(v):
                continue
            vals = values_by_group_contest.setdefault((g, str(c)), [])
            if v not in vals:
                vals.append(v)

    itypes_by_contest: Dict[Any, List[Any]] = {}
    ind_code_first: Dict[Tuple[str, str], str] = {}
    if not indicators.empty:
        itype_col = indicators["INDICATOR_ADD_CALC_TYPE"].fillna("")
        for c, t in zip(indicators["CONTEST_CODE"], itype_col):
            lst = itypes_by_contest.setdefault(c, [])
            if t not in lst:
                lst.append(t)
        # (CONTEST_CODE, INDICATOR_ADD_CALC_TYPE) без пробелов по краям → первый непустой INDICATOR_CODE
        cc_norm = indicators["CONTEST_CODE"].astype(str).str.strip()
        it_norm = itype_col.astype(str).str.strip()
        for c, t, code in zip(cc_norm, it_norm, indicators["INDICATOR_CODE"]):
            if pd.isna(code):
                continue
            ind_code_first.setdefault((c, t), str(code).strip())

    def _ind_code(contest_code: Any, ind_type: Any) -> str:
        if indicators.empty or contest_code == "-":
            return ""
        return ind_code_first.get((str(contest_code).strip(), str(ind_type).strip()), "")

    def _or(values: List[Any], default: List[Any]) -> List[Any]:
        return values if values else default

    all_rows: List[Tuple[str, ...]] = []

    def _emit(code: Any, tourns: List[Any], rewards_: List[Any], pairs: List[Tuple[str, str]], itypes: List[Any]) -> None:
        for t in tourns:
            for r in rewards_:
                for g_code, g_value in pairs:
                    for ind_type in itypes:
                        all_rows.append(
                            (str(code), str(t), str(r), str(g_code), str(g_value), _ind_code(str(code), ind_type), str(ind_type))
                        )

    # Все коды конкурсов и наград из всех таблиц
    all_contest_codes: Set[Any] = set()
    all_reward_codes: Set[Any] = set()
    for df_, col in ((rewards, "CONTEST_CODE"), (tournaments, "CONTEST_CODE"), (groups, "CONTEST_CODE"),
                     (contest_data, "CONTEST_CODE"), (indicators, "CONTEST_CODE")):
        if not df_.empty:
            all_contest_codes.update(df_[col].dropna())
    for df_ in (rewards, reward_data):
        if not df_.empty:
            all_reward_codes.update(df_["REWARD_CODE"].dropna())

    # 1. Для каждого CONTEST_CODE
    for code in all_contest_codes:
        _emit(
            code,
            _or(tourns_by_contest.get(code, []), ["-"]),
            _or(rewards_by_contest.get(code, []), ["-"]),
            _or(pairs_by_contest.get(code, []), [("-", "-")]),
            _or(itypes_by_contest.get(code, []), [""]),
        )

    # 2. Для каждого TOURNAMENT_CODE (даже если нет CONTEST_CODE)
    if not tournaments.empty:
        for t_code in tournaments["TOURNAMENT_CODE"].dropna().unique():
            code = contest_by_tourn.get(t_code, "-")
            _emit(
                code,
                [t_code],
                _or(rewards_by_contest.get(code, []), ["-"]),
                _or(pairs_by_contest.get(code, []), [("-", "-")]),
                _or(itypes_by_contest.get(code, []) if code != "-" else [], [""]),
            )

    # 3. Для каждого REWARD_CODE (даже если нет CONTEST_CODE)
    for r_code in all_reward_codes:
        code = contest_by_reward.get(r_code, "-")
        known = code != "-"
        _emit(
            code,
            _or(tourns_by_contest.get(code, []) if known else [], ["-"]),
            [r_code],
            _or(pairs_by_contest.get(code, []) if known else [], [("-", "-")]),
            _or(itypes_by_contest.get(code, []) if known else [], [""]),
        )

    # 4. Для каждого GROUP_CODE: каждый его CONTEST_CODE отдельно, GROUP_VALUE — только этой пары
    for g_code in group_codes:
        for group_contest_code in contests_by_group.get(g_code, []):
            actual_code = str(group_contest_code)
            gvals = _or(values_by_group_contest.get((g_code, actual_code), []), ["-"])
            _emit(
                actual_code,
                _or(tourns_by_contest.get(actual_code, []), ["-"]),
                _or(rewards_by_contest.get(actual_code, []), ["-"]),
                [(g_code, gv) for gv in gvals],
                _or(itypes_by_contest.get(actual_code, []), [""]),
            )

    # 5. Для каждой строки INDICATOR — её собственные INDICATOR_CODE и INDICATOR_ADD_CALC_TYPE
    if not indicators.empty:
        for code, ind_type, ind_code in zip(
            indicators["CONTEST_CODE"], indicators["INDICATOR_ADD_CALC_TYPE"], indicators["INDICATOR_CODE"]
        ):
            code = "-" if pd.isna(code) else str(code)
            ind_type = "" if pd.isna(ind_type) else str(ind_type)
            ind_code = "" if pd.isna(ind_code) else str(ind_code)
            known = code != "-"
            for t in _or(tourns_by_contest.get(code, []) if known else [], ["-"]):
                for r in _or(rewards_by_contest.get(code, []) if known else [], ["-"]):
                    for g_code, g_value in _or(pairs_by_contest.get(code, []) if known else [], [("-", "-")]):
                        all_rows.append((code, str(t), str(r), str(g_code), str(g_value), ind_code, ind_type))

    # Удалить дубли и строку-заглушку (все ключи "-" и пустые индикаторы); отсортировать (BUG-13)
    _placeholder_row = ("-", "-", "-", "-", "-", "", "")
    unique_rows = sorted({r for r in all_rows if r != _placeholder_row})
    if not unique_rows:
        return pd.DataFrame(columns=SUMMARY_KEY_COLUMNS)
    return pd.DataFrame(unique_rows, columns=SUMMARY_KEY_COLUMNS)





# Допустимые режимы сравнения ключей merge (см. key_compare в CONFIG_MERGE).
# as_text — оба ключа приводятся к тексту и сравниваются как строки;
# number_as_text — устаревший алиас as_text (совместимость с конфигами).
KEY_COMPARE_EXACT = "exact"
KEY_COMPARE_AS_TEXT = "as_text"
KEY_COMPARE_NUMBER_AS_TEXT = "number_as_text"  # алиас as_text
_KEY_COMPARE_ALLOWED = frozenset(
    {KEY_COMPARE_EXACT, KEY_COMPARE_AS_TEXT, KEY_COMPARE_NUMBER_AS_TEXT}
)


def _normalize_key_compare_mode(raw: Any) -> str:
    """
    Нормализует значение key_compare из правила merge.
    По умолчанию exact (строгое сравнение, как раньше).
    Алиас number_as_text → as_text.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return KEY_COMPARE_EXACT
    s = str(raw).strip().lower()
    if not s:
        return KEY_COMPARE_EXACT
    if s == KEY_COMPARE_NUMBER_AS_TEXT:
        return KEY_COMPARE_AS_TEXT
    if s in _KEY_COMPARE_ALLOWED:
        return s
    logging.warning(
        f"[MERGE] Неизвестный key_compare={raw!r}, используем '{KEY_COMPARE_EXACT}' "
        f"(допустимо: exact, as_text; алиас: number_as_text)"
    )
    return KEY_COMPARE_EXACT


def _merge_key_value_as_text(val: Any) -> str:
    """
    Приводит значение ключа к тексту для сравнения.

    Оба ключа (src/dst) проходят одну нормализацию:
    - целые числа без «.0»: 18, \"18\", \"18.0\" → \"18\";
    - разделители разрядов убираются: \"1 802\", \"1\\u00a0802\" → \"1802\".
    Иначе — str(val).strip().
    """
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    # Уже целое число (int / целый float) — сразу в текст без «.0»
    if isinstance(val, bool):
        return str(val)
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        if abs(val - round(val)) < 1e-9:
            return str(int(round(val)))
        return str(val).strip()
    s = str(val).strip()
    if not s or s == "-":
        return ""
    low = s.lower()
    if low in ("nan", "none", "null"):
        return ""
    # Типографские/обычные кавычки вокруг значения («18», "18")
    if len(s) >= 2 and (
        (s[0] == s[-1] == '"')
        or (s[0] == s[-1] == "'")
        or (s[0] == "«" and s[-1] == "»")
    ):
        s = s[1:-1].strip()
        if not s:
            return ""
    # Убрать разделители разрядов (пробел, NBSP, узкий NBSP и т.п.) и привести целое к тексту
    compact = _normalize_string_for_numeric_cell(s)
    if compact:
        try:
            num = float(compact)
            if abs(num - round(num)) < 1e-9:
                return str(int(round(num)))
        except (TypeError, ValueError):
            pass
    return s


def _normalize_merge_key_value(val: Any, key_compare: str = KEY_COMPARE_EXACT) -> Any:
    """
    Нормализация одной части ключа merge.

    exact — значение как есть (прежнее поведение, типы не трогаем).
    as_text — оба ключа (src и dst) приводятся к тексту и сравниваются как строки.
    """
    mode = _normalize_key_compare_mode(key_compare)
    if mode == KEY_COMPARE_EXACT:
        return val
    return _merge_key_value_as_text(val)


@debug_timed(hot=True, log_args_len=True)
def add_fields_to_sheet(df_base, df_ref, src_keys, dst_keys, columns, sheet_name, ref_sheet_name, mode="value",
                        multiply_rows=False, count_prefix="COUNT", count_aggregation="size", count_label=None,
                        source_rows_before_filter=None, applied_filters=None, key_compare=KEY_COMPARE_EXACT):
    """
    Добавляет к df_base поля из df_ref по ключам.
    Если mode == "value": подтягивает значения (первого найденного или всех при multiply_rows=True).
    Если mode == "count": добавляет количество по каждому ключу.
      count_aggregation: "size" — число строк, "nunique" — число уникальных значений (по первой колонке из columns).
      count_label: если задан, создаётся одна колонка с именем ref_sheet_name=>COUNT_{count_aggregation}_{count_label}.
    Если multiply_rows == True: при множественных совпадениях размножает строки в df_base.
    Если multiply_rows == False: берет первое найденное значение (по умолчанию); если у ключа в источнике
    несколько строк с разными значениями — WARNING в лог-файл (BUG-02).
    Если нужной колонки нет — создаёт её с дефолтными значениями "-".
    source_rows_before_filter / applied_filters: контекст, если df_ref пуст после фильтрации.
    key_compare: "exact" — строгое сравнение; "as_text" — оба ключа в текст, сравнение строк
    (алиас "number_as_text").
    """
    key_compare = _normalize_key_compare_mode(key_compare)
    logging.info(
        f"[MERGE] add_fields_to_sheet (лист: {sheet_name}, поля: {columns}, ключ: {dst_keys}->{src_keys}, "
        f"mode: {mode}, multiply: {multiply_rows}, key_compare: {key_compare})"
    )
    if isinstance(columns, str):
        columns = [columns]
    if isinstance(src_keys, str):
        src_keys = [src_keys]
    if isinstance(dst_keys, str):
        dst_keys = [dst_keys]

    def _fill_empty_result_columns() -> None:
        """Добавляет дефолтные колонки, когда источник пуст / None."""
        if mode == "count" and count_label is not None:
            new_col_name = f"{ref_sheet_name}=>COUNT_{count_aggregation}_{count_label}"
            if new_col_name not in df_base.columns:
                df_base[new_col_name] = 0
        else:
            for col in columns:
                new_col_name = (
                    f"{ref_sheet_name}=>{count_prefix}_{col}" if mode == "count" else f"{ref_sheet_name}=>{col}"
                )
                if new_col_name not in df_base.columns:
                    df_base[new_col_name] = 0 if mode == "count" else "-"

    # Лист-источник отсутствует
    if df_ref is None:
        logging.warning(
            f"[add_fields_to_sheet] Лист-источник «{ref_sheet_name}» отсутствует (None) — "
            f"поля для «{sheet_name}» не подтягиваем (ставим дефолт)."
        )
        _fill_empty_result_columns()
        return df_base

    # Пустой DataFrame: часто это 0 строк после status_filters, а не «лист пустой»
    if isinstance(df_ref, pd.DataFrame) and df_ref.empty:
        before = source_rows_before_filter
        filters_txt = applied_filters
        if before is not None and before > 0:
            # Ожидаемо (нет строк нужного статуса) — понятный текст только в лог-файл, не в консоль
            _log_info_file_only(
                f"[add_fields_to_sheet] Не ошибка: после фильтра на листе-источнике "
                f"«{ref_sheet_name}» осталось 0 строк из {before}. "
                f"Условие фильтра: {filters_txt}. "
                f"Сам лист «{ref_sheet_name}» не пуст — просто нет строк, подходящих под фильтр "
                f"(например, нет турниров со статусом из фильтра). "
                f"Для листа «{sheet_name}» по этому правилу ставим дефолт "
                f"(ожидаемо: нет строк с нужным статусом)."
            )
        elif before == 0:
            _log_info_file_only(
                f"[add_fields_to_sheet] Лист-источник «{ref_sheet_name}» содержит 0 строк — "
                f"поля для «{sheet_name}» не подтягиваем (дефолт)."
            )
        else:
            _log_info_file_only(
                f"[add_fields_to_sheet] В merge передан пустой набор из «{ref_sheet_name}» "
                f"(0 строк; контекст фильтра не передан). "
                f"Часто это результат status_filters без совпадений — сам CSV-лист при этом "
                f"может быть непустым. Поля для «{sheet_name}» — дефолт."
            )
        _fill_empty_result_columns()
        return df_base

    # Сопоставление имён колонок: регистр + суффикс после «=>» (calc_type, CONTEST-DATA=>calc_type)
    from src.csv_headers import align_dataframe_columns

    names_to_align = list(dict.fromkeys(list(columns) + list(src_keys)))
    df_ref, _missing_aligned, renames = align_dataframe_columns(df_ref, names_to_align)
    for old_n, new_n in renames:
        _log_info_file_only(
            f"[add_fields_to_sheet] Колонка «{old_n}» на листе «{ref_sheet_name}» "
            f"сопоставлена с «{new_n}» (без учёта регистра / суффикс после =>)"
        )
    if df_base is not None and isinstance(df_base, pd.DataFrame):
        df_base, _, dst_renames = align_dataframe_columns(df_base, list(dst_keys))
        for old_n, new_n in dst_renames:
            _log_info_file_only(
                f"[add_fields_to_sheet] Ключ «{old_n}» на листе «{sheet_name}» "
                f"сопоставлен с «{new_n}» (без учёта регистра)"
            )

    # Подстановка ключа/колонки источника во всех путях вызова (merge, SUMMARY) — source_aliases
    src_keys = apply_source_aliases(ref_sheet_name, df_ref, src_keys, columns, f"add_fields_to_sheet (→ {sheet_name})")

    if ref_sheet_name == "LIST-TOURNAMENT" and sheet_name == "TOURNAMENT-SCHEDULE":
        logging.info(f"[MERGE] add_fields_to_sheet LIST-TOURNAMENT -> TOURNAMENT-SCHEDULE: src_keys={src_keys}, dst_keys={dst_keys}, columns={columns}")
        logging.info(f"[MERGE] add_fields_to_sheet df_ref (LIST-TOURNAMENT) колонки: {list(df_ref.columns)}, shape={df_ref.shape}")
        if df_base is not None and isinstance(df_base, pd.DataFrame):
            logging.info(f"[MERGE] add_fields_to_sheet df_base (TOURNAMENT-SCHEDULE) колонок: {len(df_base.columns)}, есть TOURNAMENT_CODE: {'TOURNAMENT_CODE' in df_base.columns}")

    logging.debug(f"[add_fields_to_sheet] === НАЧАЛО === Лист: {sheet_name}, Источник: {ref_sheet_name}")
    logging.debug(f"[add_fields_to_sheet] df_base shape: {df_base.shape if df_base is not None and isinstance(df_base, pd.DataFrame) else "None или не DataFrame"}")
    logging.debug(f"[add_fields_to_sheet] df_ref shape: {df_ref.shape if df_ref is not None and isinstance(df_ref, pd.DataFrame) else "None или не DataFrame"}")
    logging.debug(f"[add_fields_to_sheet] Колонки для добавления: {columns}")
    logging.debug(f"[add_fields_to_sheet] Ключи: dst_keys={dst_keys}, src_keys={src_keys}")
    logging.debug(f"[add_fields_to_sheet] Режим: mode={mode}, multiply_rows={multiply_rows}")
    if df_base is not None and isinstance(df_base, pd.DataFrame) and len(df_base) > 0:
        if _debug_enabled():
            logging.debug(f"[add_fields_to_sheet] df_base колонки: {list(df_base.columns)}")
        if _debug_enabled():
            logging.debug(f"[add_fields_to_sheet] df_base первые 3 строки:\n{df_base.head(3).to_string()}")
    if df_ref is not None and isinstance(df_ref, pd.DataFrame) and len(df_ref) > 0:
        if _debug_enabled():
            logging.debug(f"[add_fields_to_sheet] df_ref колонки: {list(df_ref.columns)}")
        if _debug_enabled():
            logging.debug(f"[add_fields_to_sheet] df_ref первые 3 строки:\n{df_ref.head(3).to_string()}")





    # --- Добавлено: авто-дополнение отсутствующих колонок и ключей ---
    missing_cols = [col for col in columns if col not in df_ref.columns]
    missing_keys = [k for k in src_keys if k not in df_ref.columns]
    if missing_cols or missing_keys:
        # Источник мог прийти срезом после фильтра — дополняем свою копию, а не срез
        df_ref = df_ref.copy()
    for col in missing_cols:
        logging.warning(f"[add_fields_to_sheet] Колонка {col} не найдена в {ref_sheet_name}, создаём пустую.")
        df_ref[col] = "-"

    missing_keys = [k for k in src_keys if k not in df_ref.columns]
    for k in missing_keys:
        logging.warning(f"[add_fields_to_sheet] Ключевая колонка {k} не найдена в {ref_sheet_name}, создаём пустую.")
        df_ref[k] = "-"

    # ИСПРАВЛЕНИЕ: Проверка и создание отсутствующих ключевых колонок в df_base
    missing_dst_keys = [k for k in dst_keys if k not in df_base.columns]
    for k in missing_dst_keys:
        logging.warning(f"[add_fields_to_sheet] Ключевая колонка {k} не найдена в {sheet_name}, создаём пустую.")
        df_base[k] = "-"


    if mode == "count":
        # Ключи с учётом key_compare (as_text: оба ключа → текст)
        new_keys = _vectorized_tuple_key(df_base, dst_keys, key_compare=key_compare)
        df_ref_for_group = df_ref
        if key_compare == KEY_COMPARE_AS_TEXT:
            df_ref_for_group = df_ref.copy()
            for k in src_keys:
                df_ref_for_group[k] = df_ref_for_group[k].map(
                    lambda v: _normalize_merge_key_value(v, key_compare)
                )
        if count_aggregation == "nunique":
            col_to_count = columns[0] if columns else None
            if col_to_count and col_to_count in df_ref_for_group.columns:
                group_counts = df_ref_for_group.groupby(src_keys)[col_to_count].nunique()
            else:
                group_counts = df_ref_for_group.groupby(src_keys).size()
        else:
            group_counts = df_ref_for_group.groupby(src_keys).size()
        
        count_dict = {key_tuple: count for key_tuple, count in group_counts.items()}
        
        if count_label is not None:
            # Одна колонка с именем COUNT_{count_aggregation}_{count_label}
            count_col_name = f"{ref_sheet_name}=>COUNT_{count_aggregation}_{count_label}"
            if len(src_keys) == 1:
                new_keys_single = new_keys.apply(lambda x: x[0] if x and len(x) > 0 else None)
                df_base[count_col_name] = new_keys_single.map(group_counts).fillna(0).astype(int)
            else:
                df_base[count_col_name] = new_keys.map(count_dict).fillna(0).astype(int)
        else:
            for col in columns:
                count_col_name = f"{ref_sheet_name}=>{count_prefix}_{col}"
                if len(src_keys) == 1:
                    new_keys_single = new_keys.apply(lambda x: x[0] if x and len(x) > 0 else None)
                    df_base[count_col_name] = new_keys_single.map(group_counts).fillna(0).astype(int)
                else:
                    df_base[count_col_name] = new_keys.map(count_dict).fillna(0).astype(int)
        return df_base

    # Создаем ключи для df_ref
    # ОПТИМИЗАЦИЯ v5.0: Векторизованное создание ключей (3-5x быстрее)
    df_ref_keys = _vectorized_tuple_key(df_ref, src_keys, key_compare=key_compare)

    if not multiply_rows:
        new_keys = _vectorized_tuple_key(df_base, dst_keys, key_compare=key_compare)
        
        # BUG-02 (решение Q1): при нескольких строках источника с одним ключом берётся ПЕРВАЯ;
        # если значения поля у таких строк различаются — WARNING (детали в лог-файл, итог — в консоль)
        first_mask = ~df_ref_keys.duplicated(keep="first")
        dup_mask = df_ref_keys.duplicated(keep=False)
        keys_first = df_ref_keys[first_mask]
        # Оптимизация: собираем все новые колонки в словарь и добавляем их одним вызовом
        new_columns_dict = {}
        for col in columns:
            ref_map = dict(zip(keys_first, df_ref[col][first_mask]))
            new_col_name = f"{ref_sheet_name}=>{col}"
            new_columns_dict[new_col_name] = new_keys.map(ref_map).fillna("-")
            if dup_mask.any():
                _report_merge_key_conflicts(df_ref_keys[dup_mask], df_ref[col][dup_mask], ref_sheet_name, sheet_name, col)
        
        # Добавляем все колонки одним вызовом через pd.concat для избежания фрагментации
        if new_columns_dict:
            new_columns_df = pd.DataFrame(new_columns_dict, index=df_base.index)
            df_base = pd.concat([df_base, new_columns_df], axis=1)
            if ref_sheet_name == "LIST-TOURNAMENT" and sheet_name == "TOURNAMENT-SCHEDULE":
                for col in columns:
                    new_col_name = f"{ref_sheet_name}=>{col}"
                    if new_col_name in df_base.columns:
                        filled = (df_base[new_col_name] != "-").sum()
                        logging.info(f"[MERGE] add_fields_to_sheet результат LIST-TOURNAMENT -> TOURNAMENT-SCHEDULE: колонка '{new_col_name}', заполнено строк: {filled} из {len(df_base)}")
        
        # Специально для REWARD_LINK =>CONTEST_CODE: auto-rename, если создали с дефисом
        for col in columns:
            new_col_name = f"{ref_sheet_name}=>{col}"
            if new_col_name.replace("-", "_").replace(" ", "") == COL_REWARD_LINK_CONTEST_CODE.replace("-", "_").replace(" ", ""):
                candidates = [c for c in df_base.columns if
                              c.replace("-", "_").replace(" ", "") == COL_REWARD_LINK_CONTEST_CODE.replace("-", "_").replace(" ", "")]
                for cand in candidates:
                    if cand != COL_REWARD_LINK_CONTEST_CODE:
                        df_base = df_base.rename(columns={cand: COL_REWARD_LINK_CONTEST_CODE})
    else:
        # multiply_rows (BUG-08, решение Q7): строка приёмника повторяется для каждого совпадения в источнике
        # (в порядке строк источника); без совпадений — одна строка с «-». Через pd.merge по ключам
        # (с учётом key_compare) вместо iterrows + фильтра источника на каждую строку.
        logging.info(f"[MULTIPLY ROWS] {sheet_name}: начинаем размножение строк для поля {columns}")
        old_rows_count = len(df_base)
        base_keys = _vectorized_tuple_key(df_base, dst_keys, key_compare=key_compare)
        left = pd.DataFrame({"__k": base_keys.to_numpy(object), "__pos": np.arange(old_rows_count)})
        right = pd.DataFrame({"__k": df_ref_keys.to_numpy(object), "__rpos": np.arange(len(df_ref))})
        pairs = left.merge(right, on="__k", how="left", sort=False)
        pairs = pairs.sort_values(["__pos", "__rpos"], kind="stable", na_position="last")
        has_match = pairs["__rpos"].notna().to_numpy()
        rpos = pairs["__rpos"].fillna(0).astype(int).to_numpy()
        df_base = df_base.iloc[pairs["__pos"].to_numpy()].reset_index(drop=True)
        for col in columns:
            ref_values = df_ref[col].to_numpy(object)
            taken = ref_values[rpos] if len(ref_values) else np.full(len(rpos), "-", dtype=object)
            df_base[f"{ref_sheet_name}=>{col}"] = np.where(has_match, taken, "-")
        new_rows_count = len(df_base)
        multiply_factor = round(new_rows_count / old_rows_count, 2) if old_rows_count > 0 else 0
        logging.info(
            f"[MULTIPLY ROWS] {sheet_name}: {old_rows_count} строк -> {new_rows_count} строк (размножение: {multiply_factor}x)"
        )

        # Обработка специального случая для REWARD_LINK
        for col in columns:
            new_col_name = f"{ref_sheet_name}=>{col}"
            if new_col_name.replace("-", "_").replace(" ", "") == COL_REWARD_LINK_CONTEST_CODE.replace("-", "_").replace(" ", ""):
                candidates = [c for c in df_base.columns if
                              c.replace("-", "_").replace(" ", "") == COL_REWARD_LINK_CONTEST_CODE.replace("-", "_").replace(" ", "")]
                for cand in candidates:
                    if cand != COL_REWARD_LINK_CONTEST_CODE:
                        df_base = df_base.rename(columns={cand: COL_REWARD_LINK_CONTEST_CODE})


    return df_base


# BUG-02: дубли ключа с разными значениями в источнике merge (сбор за блок; merge идут и в потоках)
_merge_key_conflicts: List[Dict[str, Any]] = []
_merge_key_conflicts_lock = threading.Lock()


def _log_file_only(level: int, msg: str) -> None:
    """Запись только в файловые обработчики (без консоли) — для подробностей, итог выводится отдельно."""
    logger = logging.getLogger()
    emitted = False
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler) and handler.level <= level:
            record = logger.makeRecord(logger.name, level, __file__, 0, msg, (), None, func="add_fields_to_sheet")
            if all(f.filter(record) for f in handler.filters):
                handler.emit(record)
                emitted = True
    if not emitted:
        logging.log(level, msg)


def _report_merge_key_conflicts(keys: pd.Series, values: pd.Series, src: str, dst: str, col: str) -> None:
    """Ключи, у которых в источнике несколько строк с РАЗНЫМИ значениями поля col (взято первое)."""
    frame = pd.DataFrame({"k": keys.values, "v": values.astype(str).values})
    nunique = frame.groupby("k", sort=False)["v"].nunique()
    conflicts = nunique[nunique > 1]
    if conflicts.empty:
        return
    examples = ", ".join(
        "/".join(str(p) for p in (k if isinstance(k, tuple) else (k,))) for k in list(conflicts.index[:5])
    )
    _log_file_only(
        logging.WARNING,
        f"[MERGE] {src}→{dst}, поле {col}: {len(conflicts)} ключ(ей) с разными значениями в источнике, "
        f"взято первое (примеры ключей: {examples})",
    )
    with _merge_key_conflicts_lock:
        _merge_key_conflicts.append({"src": src, "dst": dst, "column": col, "keys": int(len(conflicts))})


def _report_merge_key_conflicts_summary() -> None:
    """Одна строка за блок (консоль + лог): сколько полей получили «первое из нескольких разных»."""
    with _merge_key_conflicts_lock:
        items = list(_merge_key_conflicts)
        _merge_key_conflicts.clear()
    if not items:
        return
    fields = sorted({f"{i['src']}→{i['dst']}:{i['column']}" for i in items})
    logging.warning(
        f"[MERGE] Дубли ключей с разными значениями в источнике: полей {len(fields)} "
        f"(взято первое значение; подробности — строки [MERGE] в лог-файле)"
    )


def _vectorized_tuple_key(df, keys, key_compare: str = KEY_COMPARE_EXACT):
    """
    Векторизованное создание кортежей ключей для всего DataFrame.

    Args:
        df: DataFrame
        keys: список ключей или один ключ
        key_compare: exact | as_text — режим сравнения частей ключа
        (as_text: оба ключа приводятся к тексту)

    Returns:
        pd.Series с кортежами ключей
    """
    mode = _normalize_key_compare_mode(key_compare)

    def _series_as_key_parts(col_name: str) -> pd.Series:
        ser = df[col_name]
        if mode == KEY_COMPARE_EXACT:
            return ser
        return ser.map(lambda v: _normalize_merge_key_value(v, mode))

    if isinstance(keys, (list, tuple)):
        if len(keys) == 1:
            return _series_as_key_parts(keys[0]).apply(lambda x: (x,))
        parts = [_series_as_key_parts(k) for k in keys]
        return pd.Series(list(zip(*parts)), index=df.index)
    return _series_as_key_parts(keys).apply(lambda x: (x,))


def _transform_key_value(val: Any, spec: Dict[str, Any]) -> str:
    """
    Применяет одно преобразование к значению ключа (для src_key_transform).
    Поддерживается type: "pad_left_zeros" с width: N — строка из N символов с лидирующими нулями.
    """
    if pd.isna(val) or val is None or val == "":
        return ""
    s = str(val).strip()
    t = (spec or {}).get("type")
    if t == "pad_left_zeros":
        width = int(spec.get("width", 20))
        if len(s) >= width:
            return s[:width]
        return s.zfill(width)
    return s


def _apply_src_key_transforms(
    df_src: pd.DataFrame,
    src_keys: List[str],
    src_key_transform: Optional[Dict[str, Dict[str, Any]]],
    sheet_src: str,
) -> tuple:
    """
    Применяет преобразования к ключевым колонкам источника (src_key_transform в правиле merge).
    Для каждой колонки из src_keys, указанной в src_key_transform, создаёт временную колонку
    с преобразованным значением; возвращает df_src и список эффективных ключей (исходные или временные).
    """
    if not src_key_transform or not src_keys:
        return df_src, src_keys
    effective_keys = []
    for k in src_keys:
        if k not in df_src.columns:
            effective_keys.append(k)
            continue
        spec = src_key_transform.get(k)
        if not spec:
            effective_keys.append(k)
            continue
        temp_col = f"_merge_key_{k}"
        df_src[temp_col] = df_src[k].apply(lambda v: _transform_key_value(v, spec))
        effective_keys.append(temp_col)
        logging.debug(f"[MERGE] src_key_transform: лист {sheet_src}, колонка '{k}' -> '{temp_col}' (type={spec.get('type')})")
    return df_src, effective_keys


@debug_timed(hot=True, log_args_len=True)
def _process_single_merge_rule(rule, sheets_data_copy, count_column_prefix="COUNT", merge_name="MERGE_FIELDS_ADVANCED"):
    """
    Обрабатывает одно правило merge_fields — и для группы из одного правила, и в потоке параллельной группы.
    Лист-приёмник и его params не меняются на месте: возвращаются новые (params копируются).
    merge_name: имя набора правил для логов (MERGE_FIELDS или MERGE_FIELDS_ADVANCED).
    
    Args:
        rule: Правило из merge_fields
        sheets_data_copy: Копия sheets_data для безопасной работы в потоке
        count_column_prefix: префикс для имён count-колонок (COUNT или COUNT_SELECT для MERGE_FIELDS_ADVANCED)
        
    Returns:
        tuple: (rule, updated_sheets_dict) где updated_sheets_dict содержит обновленные листы
    """
    sheet_src = rule["sheet_src"]
    sheet_dst = rule["sheet_dst"]
    src_keys = rule["src_key"] if isinstance(rule["src_key"], list) else [rule["src_key"]]
    dst_keys = rule["dst_key"] if isinstance(rule["dst_key"], list) else [rule["dst_key"]]
    col_names = rule["column"]
    mode = rule.get("mode", "value")
    multiply_rows = rule.get("multiply_rows", False)
    
    status_filters = rule.get("status_filters", None)
    custom_conditions = rule.get("custom_conditions", None)
    group_by = rule.get("group_by", None)
    aggregate = rule.get("aggregate", None)
    count_aggregation = rule.get("count_aggregation", "size")
    count_label = rule.get("count_label", None)
    key_compare = _normalize_key_compare_mode(rule.get("key_compare", KEY_COMPARE_EXACT))
    
    updated_sheets = {}
    logging.info(
        f"[MERGE] {merge_name} (_process_single_merge_rule) правило: {sheet_src} -> {sheet_dst}, "
        f"колонки: {col_names}, ключи: {dst_keys} <- {src_keys}, mode={mode}, key_compare={key_compare}"
    )
    if sheet_src in sheets_data_copy and sheets_data_copy[sheet_src] is not None:
        df_src_check = sheets_data_copy[sheet_src][0] if len(sheets_data_copy[sheet_src]) > 0 else None
        if df_src_check is not None and isinstance(df_src_check, pd.DataFrame):
            logging.debug(f"[_process_single_merge_rule] df_src ({sheet_src}): shape={df_src_check.shape}")
        else:
            logging.warning(f"[_process_single_merge_rule] [WARN] df_src ({sheet_src}) равен None!")
    if sheet_dst in sheets_data_copy and sheets_data_copy[sheet_dst] is not None:
        df_dst_check = sheets_data_copy[sheet_dst][0] if len(sheets_data_copy[sheet_dst]) > 0 else None
        if df_dst_check is not None and isinstance(df_dst_check, pd.DataFrame):
            logging.debug(f"[_process_single_merge_rule] df_dst ({sheet_dst}): shape={df_dst_check.shape}")
        else:
            logging.warning(f"[_process_single_merge_rule] [WARN] df_dst ({sheet_dst}) равен None!")

    
    # ОПТИМИЗАЦИЯ v5.0: Проверка на существование листов и None (правильный порядок)
    if (sheet_src not in sheets_data_copy or sheet_dst not in sheets_data_copy):
        logging.warning(f"[MERGE] {merge_name} ПРОПУСК: нет листа {sheet_src} или {sheet_dst}, колонки {col_names} не добавлены")
        return (rule, updated_sheets)
    
    if (sheets_data_copy[sheet_src] is None or sheets_data_copy[sheet_dst] is None or
        len(sheets_data_copy[sheet_src]) < 1 or len(sheets_data_copy[sheet_dst]) < 1 or
        sheets_data_copy[sheet_src][0] is None or sheets_data_copy[sheet_dst][0] is None):
        logging.warning(f"[MERGE] {merge_name} ПРОПУСК: лист {sheet_src} или {sheet_dst} содержит None, колонки {col_names} не добавлены")
        return (rule, updated_sheets)
    
    df_src = sheets_data_copy[sheet_src][0].copy()
    if _debug_enabled():
        logging.debug(f"[MERGE] {merge_name} df_src ({sheet_src}): shape={df_src.shape}, колонки: {list(df_src.columns)}")
    df_dst, params_dst = sheets_data_copy[sheet_dst]
    params_dst = params_dst.copy()  # Копируем параметры
    
    # Подстановка ключа/колонки источника (source_aliases)
    src_keys = apply_source_aliases(sheet_src, df_src, src_keys, col_names, merge_name)

    # Преобразование ключей источника (src_key_transform): например табельный к 20 знакам с лидирующими нулями
    src_key_transform = rule.get("src_key_transform")
    df_src, src_keys = _apply_src_key_transforms(df_src, src_keys, src_key_transform, sheet_src)

    # Применяем фильтрацию
    rows_before_filter = len(df_src)
    filter_ctx = status_filters if status_filters else custom_conditions
    df_src_filtered = apply_filters_to_dataframe(df_src, status_filters, custom_conditions, sheet_src)
    
    # Применяем группировку и агрегацию если необходимо
    if group_by or aggregate:
        df_src_filtered = apply_grouping_and_aggregation(df_src_filtered, group_by, aggregate, sheet_src)
    
    # Вызываем основную функцию добавления полей
    df_dst = add_fields_to_sheet(
        df_dst, df_src_filtered, src_keys, dst_keys, col_names, sheet_dst, sheet_src, mode=mode,
        multiply_rows=multiply_rows, count_prefix=count_column_prefix,
        count_aggregation=count_aggregation, count_label=count_label,
        source_rows_before_filter=rows_before_filter, applied_filters=filter_ctx,
        key_compare=key_compare,
    )
    
    # ИСПРАВЛЕНИЕ: Проверка на None после add_fields_to_sheet
    if df_dst is None or not isinstance(df_dst, pd.DataFrame):
        logging.error(f"[MERGE] {merge_name} add_fields_to_sheet вернул None для листа {sheet_dst}, используем исходный DataFrame")
        df_dst = sheets_data_copy[sheet_dst][0].copy() if sheets_data_copy[sheet_dst][0] is not None else pd.DataFrame()
    else:
        added_cols = [c for c in df_dst.columns if c.startswith(sheet_src + "=>")]
        logging.info(f"[MERGE] {merge_name} результат правила {sheet_src} -> {sheet_dst}: добавлены колонки: {added_cols}, всего колонок в {sheet_dst}: {len(df_dst.columns)}")

    # Сохраняем информацию о ширине колонок
    if "added_columns_width" not in params_dst:
        params_dst["added_columns_width"] = {}
    
    if mode == "count" and count_label is not None:
        new_col_name = f"{sheet_src}=>COUNT_{count_aggregation}_{count_label}"
        params_dst["added_columns_width"][new_col_name] = {
            "max_width": rule.get("col_max_width"),
            "width_mode": rule.get("col_width_mode", "AUTO"),
            "min_width": rule.get("col_min_width", 8)
        }
    else:
        for col in col_names:
            new_col_name = f"{sheet_src}=>{col}"
            if mode == "count":
                new_col_name = f"{sheet_src}=>{count_column_prefix}_{col}"
            params_dst["added_columns_width"][new_col_name] = {
                "max_width": rule.get("col_max_width"),
                "width_mode": rule.get("col_width_mode", "AUTO"),
                "min_width": rule.get("col_min_width", 8)
            }
    
    updated_sheets[sheet_dst] = (df_dst, params_dst)
    return (rule, updated_sheets)


def _group_independent_rules(merge_fields):
    """
    Группирует правила merge_fields (по порядку конфига) в группы, которые можно выполнять параллельно
    с тем же результатом, что и последовательно.

    Новое правило начинает новую группу, если (BUG-07, BUG-08):
      - его sheet_dst уже пишет правило текущей группы (два писателя одного листа);
      - его sheet_src пишет правило текущей группы: в группе все читают снимок «до группы», а при
        последовательном выполнении оно увидело бы уже добавленные колонки;
      - у него multiply_rows=true (меняется число строк листа) — такое правило всегда в отдельной группе.
    «Сначала читает, потом другое правило пишет этот лист» — не конфликт: при последовательном
    выполнении чтение тоже происходит до записи.

    Returns:
        list: Список групп правил, где каждая группа может быть обработана параллельно
    """
    if not merge_fields:
        return []

    groups = []
    current_group = []
    group_destinations = set()
    for rule in merge_fields:
        sheet_dst = rule["sheet_dst"]
        sheet_src = rule.get("sheet_src")
        multiply = bool(rule.get("multiply_rows", False))
        conflict = (
            sheet_dst in group_destinations
            or sheet_src in group_destinations
            or multiply
            or any(r.get("multiply_rows", False) for r in current_group)
        )
        if conflict and current_group:
            groups.append(current_group)
            current_group = []
            group_destinations = set()
        current_group.append(rule)
        group_destinations.add(sheet_dst)

    if current_group:
        groups.append(current_group)
    return groups


def _dump_sheets_data_for_baseline(sheets_data, max_rows: int = 3) -> dict:
    """
    Формирует снимок sheets_data для сохранения/сравнения baseline:
    для каждого листа — список колонок (порядок сохранён) и первые max_rows строк как список списков.
    Используется для верификации, что после объединения MERGE_FIELDS в MERGE_FIELDS_ADVANCED
    выходные колонки и фрагмент данных не изменились.
    """
    result = {}
    for sheet_name, sheet_data in sheets_data.items():
        if sheet_data is None or len(sheet_data) < 1:
            result[sheet_name] = {"columns": [], "sample_rows": []}
            continue
        df, _ = sheet_data
        if df is None or not isinstance(df, pd.DataFrame):
            result[sheet_name] = {"columns": [], "sample_rows": []}
            continue
        cols = list(df.columns)
        head = df.head(max_rows)
        # Преобразуем в список списков; NaN -> None для JSON
        sample_rows = []
        for _, row in head.iterrows():
            sample_rows.append([None if pd.isna(v) else v for v in row.tolist()])
        result[sheet_name] = {"columns": cols, "sample_rows": sample_rows}
    return result


def _compare_sheets_data_with_baseline(sheets_data, baseline_path: str, max_rows: int = 3) -> tuple[bool, list[str]]:
    """
    Сравнивает текущий sheets_data с сохранённым baseline (список колонок и сэмпл строк).
    Возвращает (True, []) при совпадении; (False, список сообщений об отличиях) при расхождении.
    """
    errors = []
    try:
        with open(baseline_path, "r", encoding="utf-8") as f:
            baseline = json.load(f)
    except Exception as e:
        return False, [f"Не удалось загрузить baseline {baseline_path}: {e}"]
    current = _dump_sheets_data_for_baseline(sheets_data, max_rows=max_rows)
    baseline_sheets = set(baseline.keys())
    current_sheets = set(current.keys())
    if baseline_sheets != current_sheets:
        only_baseline = baseline_sheets - current_sheets
        only_current = current_sheets - baseline_sheets
        if only_baseline:
            errors.append(f"В baseline есть листы, которых нет сейчас: {sorted(only_baseline)}")
        if only_current:
            errors.append(f"Сейчас есть листы, которых нет в baseline: {sorted(only_current)}")
    for sheet in sorted(baseline_sheets & current_sheets):
        bc = baseline[sheet].get("columns", [])
        cc = current[sheet].get("columns", [])
        if bc != cc:
            errors.append(f"Лист {sheet}: различаются колонки. Baseline: {bc[:15]}...; текущие: {cc[:15]}...")
        br = baseline[sheet].get("sample_rows", [])
        cr = current[sheet].get("sample_rows", [])
        if br != cr:
            errors.append(f"Лист {sheet}: различаются сэмпл-строки (первые {max_rows} строк)")
    return (len(errors) == 0, errors)


@debug_timed()
def merge_fields_across_sheets(sheets_data, merge_fields, count_column_prefix="COUNT", merge_name=""):
    """
    count_column_prefix: для режима count имя колонки будет {sheet_src}=>{count_column_prefix}_{col}.
    Для MERGE_FIELDS оставить "COUNT", для MERGE_FIELDS_ADVANCED передать "COUNT_SELECT".
    merge_name: имя набора правил для логов (например "MERGE_FIELDS" или "MERGE_FIELDS_ADVANCED").
    """
    name_tag = merge_name or "merge_fields"
    logging.info(f"[MERGE] ========== {name_tag}: НАЧАЛО ========== Правил: {len(merge_fields)}, листов в sheets_data: {list(sheets_data.keys())}")
    for idx, rule in enumerate(merge_fields):
        src = rule.get("sheet_src", "?")
        dst = rule.get("sheet_dst", "?")
        col = rule.get("column", [])
        sk = rule.get("src_key", [])
        dk = rule.get("dst_key", [])
        logging.info(f"[MERGE] {name_tag} правило {idx+1}/{len(merge_fields)}: {src} -> {dst}, колонки: {col}, ключи: {dk} <- {sk}")
    rule_groups = _group_independent_rules(merge_fields)
    logging.info(f"[MERGE] {name_tag}: сгруппировано в {len(rule_groups)} групп(ы) для обработки")
    for sheet_name, sheet_data in sheets_data.items():
        if sheet_data is not None and len(sheet_data) > 0:
            df, params = sheet_data
            if df is not None and isinstance(df, pd.DataFrame):
                logging.debug(f"[MERGE] {name_tag} лист {sheet_name}: shape={df.shape}, колонок: {len(df.columns)}")
            else:
                logging.debug(f"[MERGE] {name_tag} лист {sheet_name}: нет данных")

    """
    Универсально добавляет поля по правилам из merge_fields
    (source_df -> target_df), поддержка mode value / count, multiply_rows.
    
    НОВЫЕ ВОЗМОЖНОСТИ:
    - status_filters: фильтрация по статусам колонок
    - custom_conditions: пользовательские условия фильтрации
    - group_by: группировка данных перед добавлением
    - aggregate: подведение итогов (sum, count, avg, max, min)
    
    sheets_data: dict {sheet_name: (df, params)}
    merge_fields: список блоков с параметрами (см. выше)
    """
    lock = threading.Lock()  # Для безопасного доступа к sheets_data
    
    for group_idx, rule_group in enumerate(rule_groups):
        if len(rule_group) == 1:
            # Одно правило — в текущем потоке; та же функция, что и для параллельных групп (STR-03)
            rule = rule_group[0]
            logging.info(
                f"[MERGE] {name_tag} обработка правила (последовательно): {rule['sheet_src']} -> {rule['sheet_dst']}, "
                f"колонки: {rule['column']}"
            )
            _, updated_sheets = _process_single_merge_rule(rule, sheets_data, count_column_prefix, name_tag)
            sheets_data.update(updated_sheets)
            if updated_sheets:
                logging.info(f"[MERGE] {name_tag} правило завершено: {rule['sheet_src']} -> {rule['sheet_dst']}")
        else:
            # Несколько независимых правил - обрабатываем параллельно
            logging.info(f"[MERGE] {name_tag} обработка группы из {len(rule_group)} правил (параллельно)")
            
            with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(rule_group))) as executor:
                # Создаем копию sheets_data для каждого потока (безопасность)
                futures = {
                    executor.submit(_process_single_merge_rule, rule, sheets_data.copy(), count_column_prefix, name_tag): rule
                    for rule in rule_group
                }
                
                for future in as_completed(futures):
                    try:
                        rule, updated_sheets = future.result()
                        
                        # Обновляем sheets_data с блокировкой. Не перезаписываем лист целиком —
                        # дополняем колонки и params, чтобы теоретически не потерять данные от других правил.
                        with lock:
                            for sheet_name, data in updated_sheets.items():
                                if data is None or len(data) < 1 or data[0] is None:
                                    logging.warning(f"[PARALLEL MERGE] Пропущено обновление листа {sheet_name}: данные равны None")
                                    continue
                                new_df, new_params = data
                                if sheet_name in sheets_data and sheets_data[sheet_name] is not None:
                                    existing_df, existing_params = sheets_data[sheet_name]
                                    if existing_df is not None and isinstance(existing_df, pd.DataFrame):
                                        # Дополняем существующий df новыми колонками (pd.concat — без фрагментации)
                                        added_cols = [c for c in new_df.columns if c not in existing_df.columns]
                                        if added_cols:
                                            existing_df = pd.concat(
                                                [existing_df, new_df[added_cols].copy()], axis=1
                                            )
                                        else:
                                            existing_df = existing_df.copy()
                                        # Объединяем params (в т.ч. added_columns_width)
                                        merged_params = existing_params.copy() if isinstance(existing_params, dict) else {}
                                        new_added = new_params.get("added_columns_width", {}) if isinstance(new_params, dict) else {}
                                        merged_params["added_columns_width"] = {
                                            **merged_params.get("added_columns_width", {}),
                                            **new_added
                                        }
                                        if isinstance(new_params, dict):
                                            for k, v in new_params.items():
                                                if k != "added_columns_width" and k not in merged_params:
                                                    merged_params[k] = v
                                        sheets_data[sheet_name] = (existing_df, merged_params)
                                    else:
                                        sheets_data[sheet_name] = data
                                else:
                                    sheets_data[sheet_name] = data
                            
                            sheet_src = rule["sheet_src"]
                            sheet_dst = rule["sheet_dst"]
                            col_names = rule["column"]
                            logging.info(f"[MERGE] {name_tag} правило завершено (параллельно): {sheet_src} -> {sheet_dst}, колонки: {col_names}")
                    except Exception as e:
                        logging.exception(f"[PARALLEL MERGE ERROR] Ошибка обработки правила: {e}")
    
    logging.info(f"[MERGE] ========== {name_tag}: КОНЕЦ ========== Обработано групп: {len(rule_groups)}")
    return sheets_data


def apply_filters_to_dataframe(df, status_filters, custom_conditions, sheet_name):
    """
    Применяет фильтрацию к DataFrame на основе status_filters и custom_conditions.
    
    Args:
        df: исходный DataFrame
        status_filters: словарь с фильтрами по статусам {column: [allowed_values]}
        custom_conditions: словарь с пользовательскими условиями {column: condition}
        sheet_name: имя листа для логирования
        
    Returns:
        отфильтрованный DataFrame
    """
    if df.empty:
        return df
    
    # PERF-08: без копии до фильтра — булева маска и так создаёт новый объект, вызывающие передают свою копию
    df_filtered = df
    original_count = len(df_filtered)
    
    # Применяем фильтры по статусам
    if status_filters:
        from src.csv_headers import align_dataframe_columns

        filter_cols = list(status_filters.keys())
        df_filtered, missing_f, renames_f = align_dataframe_columns(df_filtered, filter_cols)
        for old_n, new_n in renames_f:
            logging.info(
                f"[FILTER] Колонка фильтра «{old_n}» сопоставлена с «{new_n}» (без учёта регистра)"
            )
        for column, allowed_values in status_filters.items():
            if column in df_filtered.columns:
                df_filtered = df_filtered[df_filtered[column].isin(allowed_values)]
                logging.info(f"[FILTER] Применен фильтр по статусу: {column}={allowed_values}, осталось строк: {len(df_filtered)}")
            else:
                logging.warning(f"[WARNING] Колонка для фильтрации по статусу не найдена: {column} в листе {sheet_name}")
    
    # Применяем пользовательские условия
    if custom_conditions:
        for column, condition in custom_conditions.items():
            if column in df_filtered.columns:
                if callable(condition):
                    # Лямбда-функция
                    df_filtered = df_filtered[df_filtered[column].apply(condition)]
                elif isinstance(condition, list):
                    # Список разрешенных значений
                    df_filtered = df_filtered[df_filtered[column].isin(condition)]
                else:
                    # Точное совпадение
                    df_filtered = df_filtered[df_filtered[column] == condition]
                
                logging.info(f"[FILTER] Применено пользовательское условие: {column}={condition}, осталось строк: {len(df_filtered)}")
            else:
                logging.warning(f"[WARNING] Колонка для пользовательского условия не найдена: {column} в листе {sheet_name}")
    
    filtered_count = len(df_filtered)
    if original_count != filtered_count:
        logging.info(f"[FILTER] Фильтрация завершена: {original_count} -> {filtered_count} строк в листе {sheet_name}")
    
    return df_filtered


def apply_grouping_and_aggregation(df, group_by, aggregate, sheet_name):
    """
    Применяет группировку и агрегацию к DataFrame.
    
    Args:
        df: исходный DataFrame
        group_by: список колонок для группировки
        aggregate: словарь с правилами агрегации {column: function}
        sheet_name: имя листа для логирования
        
    Returns:
        DataFrame с примененной группировкой и агрегацией
    """
    if df.empty:
        return df
    
    if not group_by and not aggregate:
        return df
    
    df_grouped = df.copy()
    original_count = len(df_grouped)
    
    try:
        if group_by:
            # Проверяем наличие колонок для группировки
            missing_group_cols = [col for col in group_by if col not in df_grouped.columns]
            if missing_group_cols:
                logging.warning(f"[WARNING] Колонки для группировки не найдены: {missing_group_cols} в листе {sheet_name}")
                return df_grouped
            
            # Применяем группировку
            if aggregate:
                # Группировка с агрегацией
                agg_dict = {}
                for col, func in aggregate.items():
                    if col in df_grouped.columns:
                        agg_dict[col] = func
                    else:
                        logging.warning(f"[WARNING] Колонка для агрегации не найдена: {col} в листе {sheet_name}")
                
                if agg_dict:
                    df_grouped = df_grouped.groupby(group_by).agg(agg_dict).reset_index()
                    # Убираем многоуровневые заголовки если они появились
                    if isinstance(df_grouped.columns, pd.MultiIndex):
                        df_grouped.columns = [col[0] if col[1] == '' else f"{col[0]}_{col[1]}" for col in df_grouped.columns]
            else:
                # Простая группировка (убираем дубликаты)
                df_grouped = df_grouped.groupby(group_by).first().reset_index()
        else:
            # Только агрегация без группировки
            agg_dict = {}
            for col, func in aggregate.items():
                if col in df_grouped.columns:
                    agg_dict[col] = func
                else:
                    logging.warning(f"[WARNING] Колонка для агрегации не найдена: {col} в листе {sheet_name}")
            
            if agg_dict:
                df_grouped = df_grouped.agg(agg_dict).to_frame().T
        
        grouped_count = len(df_grouped)
        logging.info(f"[GROUP] Группировка и агрегация завершены: {original_count} -> {grouped_count} строк в листе {sheet_name}")
        
    except Exception as e:
        logging.exception(f"[ERROR] Ошибка при группировке в листе {sheet_name}: {e}")
        return df
    
    return df_grouped



def detect_gender_by_patterns(value, patterns_male, patterns_female):
    """Определение пола по окончаниям в тексте"""
    if pd.isna(value) or not isinstance(value, str):
        return None

    value_lower = value.lower().strip()
    if not value_lower:
        return None

    # Проверяем мужские окончания
    for pattern in patterns_male:
        if value_lower.endswith(pattern.lower()):
            return 'М'

    # Проверяем женские окончания
    for pattern in patterns_female:
        if value_lower.endswith(pattern.lower()):
            return 'Ж'

    return None


def detect_gender_for_person(patronymic, first_name, surname, row_idx):
    """Определение пола для одного человека по приоритету: отчество -> имя -> фамилия"""

    # 1. Попытка определить по отчеству
    gender = detect_gender_by_patterns(
        patronymic,
        GENDER_PATTERNS['patronymic_male'],
        GENDER_PATTERNS['patronymic_female']
    )
    if gender:
        return gender

    # 2. Попытка определить по имени
    gender = detect_gender_by_patterns(
        first_name,
        GENDER_PATTERNS['name_male'],
        GENDER_PATTERNS['name_female']
    )
    if gender:
        return gender

    # 3. Попытка определить по фамилии
    gender = detect_gender_by_patterns(
        surname,
        GENDER_PATTERNS['surname_male'],
        GENDER_PATTERNS['surname_female']
    )
    if gender:
        return gender

    return '-'


@debug_timed()
def add_auto_gender_column(df, sheet_name):
    """Добавление колонки AUTO_GENDER к DataFrame с автоматическим определением пола"""

    # Проверяем наличие необходимых колонок
    required_columns = ['MIDDLE_NAME', 'FIRST_NAME', 'SURNAME']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logging.warning(f"[GENDER DETECTION] Пропущены колонки {missing_columns} в листе {sheet_name}")
        df['AUTO_GENDER'] = '-'
        return df

    total_rows = len(df)
    logging.info(f"[GENDER DETECTION] Начинаем определение пола для листа {sheet_name}, строк: {total_rows}")

    # Счетчики для статистики
    male_count = 0
    female_count = 0
    unknown_count = 0

    # Создаем новую колонку
    auto_gender = []

    for idx, row in df.iterrows():
        # Получаем значения полей
        patronymic = row.get('MIDDLE_NAME', '')
        first_name = row.get('FIRST_NAME', '')
        surname = row.get('SURNAME', '')

        # Определяем пол
        gender = detect_gender_for_person(patronymic, first_name, surname, idx)
        auto_gender.append(gender)

        # Обновляем статистику
        if gender == 'М':
            male_count += 1
        elif gender == 'Ж':
            female_count += 1
        else:
            unknown_count += 1

    # Добавляем колонку к DataFrame
    df['AUTO_GENDER'] = auto_gender

    # Логируем финальную статистику
    logging.info(f"[GENDER DETECTION] Статистика: М={male_count}, Ж={female_count}, неопределено={unknown_count} (всего: {total_rows})")
    logging.info(f"[GENDER DETECTION] Завершено для листа {sheet_name}")

    return df


@debug_timed()
def add_auto_gender_column_vectorized(df, sheet_name):
    """
    ОПТИМИЗИРОВАННАЯ ВЕРСИЯ: Векторизованное определение пола.
    
    Обрабатывает все строки одновременно используя строковые операции pandas
    вместо iterrows(). Ожидаемое ускорение: 100-200x.
    
    Args:
        df (pd.DataFrame): DataFrame для обработки
        sheet_name (str): Название листа

    Returns:
        pd.DataFrame: DataFrame с добавленной колонкой AUTO_GENDER
    """
    
    required_columns = ['MIDDLE_NAME', 'FIRST_NAME', 'SURNAME']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logging.warning(f"[GENDER DETECTION VECTORIZED] Пропущены колонки {missing_columns} в листе {sheet_name}")
        df['AUTO_GENDER'] = '-'
        return df
    
    total_rows = len(df)
    logging.info(f"[GENDER DETECTION VECTORIZED] Начинаем определение пола для листа {sheet_name}, строк: {total_rows}")
    
    # Инициализируем колонку с дефолтным значением
    gender = pd.Series('-', index=df.index)
    
    # Подготовка данных: приводим к нижнему регистру и заполняем пустые значения
    patronymic_lower = df['MIDDLE_NAME'].fillna('').astype(str).str.lower().str.strip()
    first_name_lower = df['FIRST_NAME'].fillna('').astype(str).str.lower().str.strip()
    surname_lower = df['SURNAME'].fillna('').astype(str).str.lower().str.strip()
    
    # 1. Определение по отчеству (приоритет 1)
    for pattern in GENDER_PATTERNS['patronymic_male']:
        mask = patronymic_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'М'
    
    for pattern in GENDER_PATTERNS['patronymic_female']:
        mask = patronymic_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'Ж'
    
    # 2. Определение по имени (приоритет 2)
    for pattern in GENDER_PATTERNS['name_male']:
        mask = first_name_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'М'
    
    for pattern in GENDER_PATTERNS['name_female']:
        mask = first_name_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'Ж'
    
    # 3. Определение по фамилии (приоритет 3)
    for pattern in GENDER_PATTERNS['surname_male']:
        mask = surname_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'М'
    
    for pattern in GENDER_PATTERNS['surname_female']:
        mask = surname_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'Ж'
    
    # Добавляем колонку к DataFrame
    df['AUTO_GENDER'] = gender
    
    # Статистика
    male_count = (gender == 'М').sum()
    female_count = (gender == 'Ж').sum()
    unknown_count = (gender == '-').sum()
    
    logging.info(f"[GENDER DETECTION VECTORIZED] Статистика: М={male_count}, Ж={female_count}, неопределено={unknown_count} (всего: {total_rows})")
    if unknown_count:
        # INFO-01: разбивка причин без персональных данных (логика определения не меняется)
        unknown = gender == '-'
        no_patronymic = int((unknown & (patronymic_lower == '')).sum())
        masked_surname = int((unknown & surname_lower.str.contains('…', regex=False)).sum())
        logging.info(
            f"[GENDER DETECTION VECTORIZED] Не определено {unknown_count}: пустое отчество — {no_patronymic}, "
            f"фамилия замаскирована «…» — {masked_surname}; остальные — имя/фамилия не подошли под шаблоны gender"
        )
    logging.info(f"[GENDER DETECTION VECTORIZED] Завершено для листа {sheet_name}")
    
    return df




@debug_timed()
def build_summary_sheet(dfs, params_summary, merge_fields):
    if _debug_enabled():
        logging.debug(f"[build_summary_sheet] === НАЧАЛО === Доступные листы в dfs: {list(dfs.keys())}")
    for sheet_name, df in dfs.items():
        if df is not None and isinstance(df, pd.DataFrame):
            if _debug_enabled():
                logging.debug(f"[build_summary_sheet] Лист {sheet_name}: shape={df.shape}, колонки={list(df.columns)[:10]}...")
        else:
            logging.debug(f"[build_summary_sheet] Лист {sheet_name}: DataFrame равен None")
    logging.debug(f"[build_summary_sheet] Правил merge_fields: {len(merge_fields)}")


    summary = collect_summary_keys(dfs)
    logging.debug(f"[build_summary_sheet] После collect_summary_keys: summary shape={summary.shape if summary is not None and isinstance(summary, pd.DataFrame) else "None"}")
    if summary is not None and isinstance(summary, pd.DataFrame) and len(summary) > 0:
        if _debug_enabled():
            logging.debug(f"[build_summary_sheet] summary колонки: {list(summary.columns)}")
        if _debug_enabled():
            logging.debug(f"[build_summary_sheet] summary первые 3 строки:\n{summary.head(3).to_string()}")

    
    # ОПТИМИЗАЦИЯ v5.0: Проверка на None
    if summary is None:
        logging.error("[build_summary_sheet] collect_summary_keys вернул None, создаем пустой DataFrame")
        summary = pd.DataFrame(columns=SUMMARY_KEY_COLUMNS)
    elif not isinstance(summary, pd.DataFrame):
        logging.error("[build_summary_sheet] collect_summary_keys вернул не DataFrame, создаем пустой DataFrame")
        summary = pd.DataFrame(columns=SUMMARY_KEY_COLUMNS)

    # Детальное логирование для отладки GROUP_VALUE
    DEBUG_CODES = []  # Отключено подробное логирование
    for debug_code in DEBUG_CODES:
        debug_rows = summary[summary["CONTEST_CODE"] == debug_code]
        if not debug_rows.empty:
            logging.debug(f"[SUMMARY] === После collect_summary_keys для CONTEST_CODE: {debug_code} ===")
            logging.debug(f"[SUMMARY] Всего строк: {len(debug_rows)}")
            logging.debug(f"[SUMMARY] Уникальные GROUP_CODE: {debug_rows['GROUP_CODE'].unique().tolist()}")
            logging.debug(f"[SUMMARY] Уникальные GROUP_VALUE: {debug_rows['GROUP_VALUE'].unique().tolist()}")
            logging.debug("[SUMMARY] Комбинации (GROUP_CODE, GROUP_VALUE):")
            for _, row in debug_rows.iterrows():
                logging.debug(
                    f"[SUMMARY]   CONTEST={row.get('CONTEST_CODE', '')}, GROUP_CODE={row.get('GROUP_CODE', '')}, GROUP_VALUE={row.get('GROUP_VALUE', '')}"
                )
            
            # Проверяем, что есть в таблице GROUP
            if "GROUP" in dfs and not dfs["GROUP"].empty:
                group_rows = dfs["GROUP"][dfs["GROUP"]["CONTEST_CODE"] == debug_code]
                if not group_rows.empty:
                    logging.debug(f"[SUMMARY] === Данные в таблице GROUP для CONTEST_CODE: {debug_code} ===")
                    logging.debug(f"[SUMMARY] Всего строк в GROUP: {len(group_rows)}")
                    logging.debug(
                        f"[SUMMARY] Строки GROUP:\n{group_rows[['CONTEST_CODE', 'GROUP_CODE', 'GROUP_VALUE']].to_string()}"
                    )

    logging.info(f"Summary: Каркас: {len(summary)} строк (реальные комбинации ключей)")
    if _debug_enabled():
        logging.debug(f"{params_summary['sheet']}: первые строки после разворачивания:\n{summary.head(5).to_string()}")

    # Универсально добавляем все поля по merge_fields
    # (как merge_fields_across_sheets: status_filters, count_label, count_aggregation)
    for field_idx, field in enumerate(merge_fields):
        col_names = field["column"]
        if isinstance(col_names, str):
            col_names = [col_names]
        sheet_src = field["sheet_src"]
        src_keys = field["src_key"] if isinstance(field["src_key"], list) else [field["src_key"]]
        dst_keys = field["dst_key"] if isinstance(field["dst_key"], list) else [field["dst_key"]]
        mode = field.get("mode", "value")
        status_filters = field.get("status_filters", None)
        custom_conditions = field.get("custom_conditions", None)
        group_by = field.get("group_by", None)
        aggregate = field.get("aggregate", None)
        count_aggregation = field.get("count_aggregation", "size")
        count_label = field.get("count_label", None)
        key_compare = _normalize_key_compare_mode(field.get("key_compare", KEY_COMPARE_EXACT))
        params_str = (
            f"(лист-источник: {sheet_src}, поля: {col_names}, ключ: {dst_keys}->{src_keys}, mode: {mode}"
            f", key_compare: {key_compare}"
        )
        if status_filters:
            params_str += f", status_filters: {status_filters}"
        if mode == "count" and count_label is not None:
            params_str += f", count_aggregation: {count_aggregation}, count_label: {count_label}"
        params_str += ")"

        logging.debug(f"[build_summary_sheet] === MERGE {field_idx+1}/{len(merge_fields)} ===")
        logging.debug(f"[build_summary_sheet] Правило: sheet_src={sheet_src}, sheet_dst={params_summary['sheet']}")
        logging.debug(f"[build_summary_sheet] Поля: {col_names}, ключи: {dst_keys}->{src_keys}, mode={mode}")
        logging.debug(
            f"[build_summary_sheet] summary ДО merge: shape="
            f"{summary.shape if summary is not None and isinstance(summary, pd.DataFrame) else 'None'}"
        )
        if summary is not None and isinstance(summary, pd.DataFrame) and len(summary) > 0:
            if _debug_enabled():
                logging.debug(f"[build_summary_sheet] summary ДО merge первые 3 строки:\n{summary.head(3).to_string()}")

        # Детальное логирование для merge_fields с GROUP
        if sheet_src == "GROUP":
            for debug_code in DEBUG_CODES:
                debug_rows_before = summary[summary["CONTEST_CODE"] == debug_code]
                if not debug_rows_before.empty:
                    logging.debug(f"[SUMMARY] === Перед merge_fields из GROUP для CONTEST_CODE: {debug_code} ===")
                    logging.debug(f"[SUMMARY] Строк в Summary: {len(debug_rows_before)}")
                    logging.debug(f"[SUMMARY] GROUP_CODE: {debug_rows_before['GROUP_CODE'].unique().tolist()}")
                    logging.debug(f"[SUMMARY] GROUP_VALUE: {debug_rows_before['GROUP_VALUE'].unique().tolist()}")

        ref_df = dfs.get(sheet_src)
        if ref_df is not None and isinstance(ref_df, pd.DataFrame):
            logging.debug(
                f"[build_summary_sheet] ref_df ({sheet_src}): shape={ref_df.shape}, "
                f"колонки={list(ref_df.columns)[:10]}..."
            )
        else:
            logging.warning(f"[build_summary_sheet] [WARN] ref_df ({sheet_src}) равен None!")
        if ref_df is None:
            logging.warning(f"Колонка {col_names} не добавлена: нет листа {sheet_src} или ключей {src_keys}")
            continue

        # Копия источника: фильтры и transform не должны менять dfs
        ref_df = ref_df.copy()

        # Подстановка ключа/колонки источника (source_aliases)
        src_keys = apply_source_aliases(sheet_src, ref_df, src_keys, col_names, "build_summary_sheet")

        src_key_transform = field.get("src_key_transform")
        ref_df, src_keys = _apply_src_key_transforms(ref_df, src_keys, src_key_transform, sheet_src)

        multiply_rows = field.get("multiply_rows", False)
        try:
            # Сохраняем исходный summary перед merge
            summary_before_merge = (
                summary.copy() if summary is not None and isinstance(summary, pd.DataFrame) else None
            )

            rows_before_filter = len(ref_df)
            filter_ctx = status_filters if status_filters else custom_conditions
            ref_df_filtered = apply_filters_to_dataframe(
                ref_df, status_filters, custom_conditions, sheet_src
            )
            if group_by or aggregate:
                ref_df_filtered = apply_grouping_and_aggregation(
                    ref_df_filtered, group_by, aggregate, sheet_src
                )

            summary = add_fields_to_sheet(
                summary,
                ref_df_filtered,
                src_keys,
                dst_keys,
                col_names,
                params_summary["sheet"],
                sheet_src,
                mode=mode,
                multiply_rows=multiply_rows,
                count_aggregation=count_aggregation,
                count_label=count_label,
                source_rows_before_filter=rows_before_filter,
                applied_filters=filter_ctx,
                key_compare=key_compare,
            )

            if summary is None:
                logging.error(
                    f"[build_summary_sheet] КРИТИЧЕСКАЯ ОШИБКА: summary стал None после merge "
                    f"{field_idx+1}/{len(merge_fields)} с {sheet_src}!"
                )
                logging.error(
                    f"[build_summary_sheet] Параметры merge: поля={col_names}, "
                    f"ключи={dst_keys}->{src_keys}, mode={mode}"
                )
                summary = (
                    summary_before_merge.copy()
                    if summary_before_merge is not None
                    else pd.DataFrame(columns=SUMMARY_KEY_COLUMNS)
                )

            logging.debug(
                f"[build_summary_sheet] summary ПОСЛЕ merge: shape="
                f"{summary.shape if summary is not None and isinstance(summary, pd.DataFrame) else 'None'}"
            )
            if summary is not None and isinstance(summary, pd.DataFrame) and len(summary) > 0:
                logging.debug(
                    f"[build_summary_sheet] summary ПОСЛЕ merge первые 3 строки:\n{summary.head(3).to_string()}"
                )
            else:
                logging.error(
                    "[build_summary_sheet] [ERR] КРИТИЧЕСКАЯ ОШИБКА: summary стал None или пустым после merge!"
                )
                logging.warning(
                    f"[build_summary_sheet] Восстановлен исходный summary ({len(summary)} строк) "
                    f"после None merge с {sheet_src}"
                )
        except Exception as e:
            logging.exception(f"[build_summary_sheet] ОШИБКА при merge с {sheet_src}: {e}")
            logging.error(
                f"[build_summary_sheet] Параметры: поля={col_names}, ключи={dst_keys}->{src_keys}, mode={mode}"
            )
            summary = (
                summary_before_merge.copy()
                if summary_before_merge is not None
                else pd.DataFrame(columns=SUMMARY_KEY_COLUMNS)
            )
            logging.warning(
                f"[build_summary_sheet] Восстановлен исходный summary ({len(summary)} строк) "
                f"после ошибки merge с {sheet_src}"
            )
            continue
        # Детальное логирование после merge_fields с GROUP
        if sheet_src == "GROUP":
            for debug_code in DEBUG_CODES:
                debug_rows_after = summary[summary["CONTEST_CODE"] == debug_code]
                if not debug_rows_after.empty:
                    logging.debug(f"[SUMMARY] === После merge_fields из GROUP для CONTEST_CODE: {debug_code} ===")
                    logging.debug(f"[SUMMARY] Строк в Summary: {len(debug_rows_after)}")
                    logging.debug(f"[SUMMARY] GROUP_CODE: {debug_rows_after['GROUP_CODE'].unique().tolist()}")
                    logging.debug(f"[SUMMARY] GROUP_VALUE: {debug_rows_after['GROUP_VALUE'].unique().tolist()}")
                    logging.debug("[SUMMARY] Комбинации (GROUP_CODE, GROUP_VALUE):")
                    for _, row in debug_rows_after.iterrows():
                        logging.debug(
                            f"[SUMMARY]   CONTEST={row.get('CONTEST_CODE', '')}, "
                            f"GROUP_CODE={row.get('GROUP_CODE', '')}, "
                            f"GROUP_VALUE={row.get('GROUP_VALUE', '')}"
                        )

    return summary


@debug_timed(log_args_len=True)
def process_single_file(file_conf):
    """
    Обрабатывает один CSV файл: поиск, чтение и разворачивание JSON полей.
    Используется для параллельной обработки файлов.
    
    Args:
        file_conf (dict): Конфигурация файла из INPUT_FILES
        
    Returns:
        tuple: (df, sheet_name, file_conf, df_raw, file_path) или (None, sheet_name, None, None, None) при ошибке
    """
    sheet_name = file_conf["sheet"]
    try:
        # Подкаталог (один уровень): если задан subdir — ищем в paths.input / subdir
        subdir = (file_conf.get("subdir") or "").strip()
        search_dir = os.path.join(DIR_INPUT, subdir) if subdir else DIR_INPUT
        file_path = _find_input_file(search_dir, sheet_name, file_conf["file"])
        # Проверяем, найден ли файл
        if file_path is None:
            th = threading.current_thread().name
            logging.error(f"Файл не найден: {file_conf['file']} в каталоге {DIR_INPUT} [поток: {th}]")
            return None, sheet_name, None, None, None
        
        th = threading.current_thread().name
        logging.info(f"Загрузка файла: {file_path} [поток: {th}]")
        # expected_columns из consistency_checks.csv_columns_count.sheets[sheet], иначе из file_conf (обратная совместимость); 0 = АВТО
        csv_cc = (CONSISTENCY_CHECKS or {}).get("csv_columns_count", {}).get("sheets", {})
        expected_columns = int(csv_cc.get(sheet_name, {}).get("expected_columns", file_conf.get("expected_columns", 0)))
        result = read_csv_file(file_path, expected_columns=expected_columns)
        if result is None:
            logging.error(f"Ошибка чтения файла: {file_path} [поток: {th}]")
            return None, sheet_name, None, None, None
        df, csv_issues = result
        # Копия ровно того, что в CSV (без разворота JSON и без доп. полей) — для выгрузки source
        df_raw_for_source = df.copy()
        if csv_issues:
            with _csv_mismatches_lock:
                for rec in csv_issues:
                    _csv_column_mismatches.append({
                        **rec,
                        "sheet": sheet_name,
                        "file": file_conf.get("file", ""),
                    })

        # Разворачиваем только нужные JSON-поля по строгому списку
        json_columns = JSON_COLUMNS.get(sheet_name, [])
        for json_conf in json_columns:
            col = json_conf["column"]
            prefix = json_conf.get("prefix", col)
            if col in df.columns:
                df = flatten_json_column_recursive(df, col, prefix=prefix, sheet=sheet_name)
                logging.info(f"[JSON FLATTEN] {sheet_name}: поле '{col}' развернуто с префиксом '{prefix}' [поток: {th}]")
            else:
                logging.warning(f"[JSON FLATTEN] {sheet_name}: поле '{col}' не найдено в колонках! [поток: {th}]")
        
        # Для дебага: логируем итоговый список колонок после всех разворотов
        if _debug_enabled():
            logging.debug(f"{sheet_name}: колонки после разворачивания: {', '.join(df.columns.tolist())} [поток: {th}]")

        logging.info(f"Файл успешно обработан: {sheet_name}, строк: {len(df)} [поток: {th}]")
        
        return df, sheet_name, file_conf, df_raw_for_source, file_path
        
    except Exception as e:
        logging.exception(
            f"Ошибка обработки файла {file_conf.get('file', 'unknown')}: {e} [поток: {threading.current_thread().name}]"
        )
        return None, sheet_name, None, None, None




@debug_timed()
def collect_duplicates_and_validation_report(sheets_data: Dict[str, Any]) -> tuple:
    """
    Собирает сводный отчёт по отклонениям длины полей (из правил consistency_checks) и расхождениям по числу полей в CSV.

    Returns:
        tuple: (validation_report, csv_mismatch_report)
            - validation_report: список dict с ключами sheet, result_column, n_violations, sample_values
            - csv_mismatch_report: список записей о строках CSV с числом полей != заголовку
    """
    validation_report: List[Dict[str, Any]] = []

    # --- Отклонения по длине полей (из правил consistency_checks type=field_length) ---
    _cc_rules = (CONSISTENCY_CHECKS or {}).get("rules") or []
    for rule in _cc_rules:
        if rule.get("type") != "field_length" or not rule.get("enabled", True):
            continue
        sheet_name = rule.get("sheet")
        result_column = rule.get("result_column") or "FIELD_LENGTH_CHECK"
        if not sheet_name or sheet_name not in sheets_data:
            continue
        sheet_item = sheets_data[sheet_name]
        if sheet_item is None:
            continue
        try:
            df, _ = sheet_item
            if df is None or not isinstance(df, pd.DataFrame):
                continue
        except (TypeError, ValueError):
            continue
        if result_column not in df.columns:
            continue
        violations_mask = (df[result_column].astype(str).str.strip() != "") & (df[result_column].astype(str).str.strip() != "-")
        n_violations = int(violations_mask.sum())
        if n_violations == 0:
            continue
        sample_values = df.loc[violations_mask, result_column].drop_duplicates().head(20).tolist()
        validation_report.append({
            "sheet": sheet_name,
            "result_column": result_column,
            "n_violations": n_violations,
            "sample_values": sample_values,
        })

    csv_mismatch_report = list(_csv_column_mismatches)
    return validation_report, csv_mismatch_report


@debug_timed()
def build_raw_sheets_data_for_consistency(
    raw_sheets: Dict[str, Any],
    sheets_data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Собирает словарь листов для проверок консистентности.

    База — raw_sheets (листы с include_in_source=true, сырой CSV).
    Листы с include_in_source=false (например LIST-REWARDS) в raw_sheets не попадают
    и иначе пропускались бы правилами consistency: их добавляем копией из sheets_data
    (уже после разворота JSON, до merge) — только для проверок, не для source Excel.
    """
    out: Dict[str, Any] = {
        s: (raw_sheets[s][0], raw_sheets[s][1]) for s in raw_sheets
    }
    for sheet_name, item in sheets_data.items():
        if sheet_name in out:
            continue
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        df, conf = item[0], item[1]
        if not isinstance(df, pd.DataFrame):
            continue
        out[sheet_name] = (df.copy(), conf)
        logging.info(
            "[CONSISTENCY] Лист «%s» добавлен в проверки "
            "(нет в source / include_in_source=false)",
            sheet_name,
        )
    return out


@debug_timed()
def copy_consistency_results_from_raw_to_processed(
    raw_sheets_data: Dict[str, Any],
    sheets_data: Dict[str, Any],
    summary_sheet_name: str,
) -> None:
    """
    Копирует результаты проверок консистентности с сырых листов на обработанные:
    колонки, добавленные проверками (ДУБЛЬ:…, ПРОВЕРКА:… и т.д.), и лист CONSISTENCY.
    Сырые и обработанные листы имеют одинаковый порядок строк (индексы совпадают).
    """
    for sheet_name in list(sheets_data.keys()):
        if sheet_name == summary_sheet_name:
            continue
        if sheet_name not in raw_sheets_data or raw_sheets_data[sheet_name] is None:
            continue
        raw_item = raw_sheets_data[sheet_name]
        proc_item = sheets_data[sheet_name]
        if not isinstance(raw_item, (list, tuple)) or len(raw_item) < 1 or not isinstance(proc_item, (list, tuple)) or len(proc_item) < 1:
            continue
        raw_df = raw_item[0]
        proc_df = proc_item[0]
        if not isinstance(raw_df, pd.DataFrame) or not isinstance(proc_df, pd.DataFrame):
            continue
        # Колонки, добавленные проверками на сырых данных (есть в raw, нет в processed)
        added_cols = [c for c in raw_df.columns if c not in proc_df.columns]
        if added_cols:
            # pd.concat вместо поочерёдного присваивания — избегаем PerformanceWarning «fragmented DataFrame»
            proc_df_new = pd.concat([proc_df, raw_df[added_cols].copy()], axis=1)
            sheets_data[sheet_name] = (proc_df_new, proc_item[1])
        logging.debug(f"[CONSISTENCY] Скопировано колонок проверок на лист {sheet_name}: {len(added_cols)}")
    if summary_sheet_name in raw_sheets_data and raw_sheets_data[summary_sheet_name] is not None:
        sheets_data[summary_sheet_name] = raw_sheets_data[summary_sheet_name]
        logging.debug(f"[CONSISTENCY] Лист {summary_sheet_name} скопирован с сырых данных")


@debug_timed()
def append_csv_mismatches_to_consistency(
    sheets_data: Dict[str, Any],
    csv_mismatch_report: List[Dict[str, Any]],
    summary_sheet_name: str = "CONSISTENCY",
    consistency_checks_config: Optional[Dict[str, Any]] = None,
    raw_sheets_data: Optional[Dict[str, Any]] = None,
    raw_counts: Optional[Dict[str, Dict[str, int]]] = None,
) -> None:
    """
    Дополняет сводный лист CONSISTENCY записью о проверке числа полей в CSV.
    Список листов, ожидаемое число полей (expected_columns: 0 = АВТО) и тексты для колонок
    берутся из consistency_checks.csv_columns_count (sheets + _default).
    Число колонок и строк должно браться из raw_counts (сырые данные до любых проверок);
    если raw_counts не передан — из raw_sheets_data (но там уже могут быть колонки проверок).
    """
    cc = consistency_checks_config if consistency_checks_config is not None else CONSISTENCY_CHECKS
    csv_cc = (cc or {}).get("csv_columns_count", {})
    sheets_cfg = csv_cc.get("sheets", {})
    if not sheets_cfg:
        return
    default_desc = (csv_cc.get("_default") or {}).copy()
    base_columns = [
        "check_id", "sheet", "name", "имя_колонки", "type", "total_rows", "violations", "sample"
    ]
    desc_columns = [
        "ТИП ПРОВЕРКИ", "Описание", "таблица источник", "поле источник",
        "таблица где проверяем", "поле для проверки", "параметр сравнения", "комментарий"
    ]
    by_sheet: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in csv_mismatch_report:
        key = r.get("sheet", "") or ""
        by_sheet[key].append(r)

    new_rows = []
    for sheet_name, sheet_cfg in sheets_cfg.items():
        if not sheet_name or sheet_name not in sheets_data or sheets_data[sheet_name] is None:
            continue
        item = sheets_data[sheet_name]
        if not isinstance(item, (list, tuple)) or len(item) < 1:
            continue
        df = item[0]
        if not isinstance(df, pd.DataFrame):
            continue
        # Число строк и колонок — только из сырых данных ДО проверок (raw_counts); иначе raw_sheets_data уже с колонками проверок даст неверный подсчёт
        if raw_counts and sheet_name in raw_counts:
            total_rows = raw_counts[sheet_name].get("nrows", len(df))
            actual_col_count = raw_counts[sheet_name].get("ncols", 0)
        elif raw_sheets_data and sheet_name in raw_sheets_data:
            raw_item = raw_sheets_data[sheet_name]
            if isinstance(raw_item, (list, tuple)) and len(raw_item) >= 1 and isinstance(raw_item[0], pd.DataFrame):
                raw_df = raw_item[0]
                total_rows = len(raw_df)
                actual_col_count = len(raw_df.columns)
            else:
                total_rows = len(df)
                actual_col_count = len(df.columns) if hasattr(df, "columns") else 0
        else:
            total_rows = len(df)
            actual_col_count = len(df.columns) if hasattr(df, "columns") else 0
        expected_cols = int(sheet_cfg.get("expected_columns", 0))
        expected_label = "АВТО (по заголовку)" if expected_cols == 0 else str(expected_cols)
        param_compare = expected_label
        if expected_cols == 0 and actual_col_count:
            param_compare = f"АВТО (по заголовку), колонок в файле: {actual_col_count}"
        recs = by_sheet.get(sheet_name, [])
        violations = len(recs)
        if violations == 0:
            result_text = "OK"
            # В sample заполняем только при наличии отклонений (строки с расхождением числа полей)
            sample_str = ""
        else:
            result_text = f"{violations} строк с расхождением"
            sample_parts = []
            max_csv_sample = 5
            for r in recs[:max_csv_sample]:
                exp = r.get("expected_cols", "")
                act = r.get("actual_cols", "")
                direction = r.get("direction", "")
                d_short = "+" if "больше" in str(direction) else "-" if "меньше" in str(direction) else ""
                sample_parts.append(f"[{r.get('row_index', '')}] | полей {act}/{exp} | {d_short}")
            if len(recs) > max_csv_sample:
                sample_parts.append(" ...")
            sample_str = "; ".join(sample_parts)
        name_text = (
            f"Проверка числа полей в CSV. Ожидалось: {expected_label} полей. Результат: {result_text}"
        )
        # Тексты для колонок листа CONSISTENCY: из sheet_cfg с подстановкой _default
        desc = {**default_desc, **{k: v for k, v in sheet_cfg.items() if k in desc_columns}}
        desc.setdefault("ТИП ПРОВЕРКИ", "число полей в CSV")
        desc.setdefault("Описание", "Проверка числа полей в CSV (ожидаемое из конфига или АВТО по заголовку)")
        desc.setdefault("таблица источник", sheet_name)
        desc.setdefault("поле источник", "все поля строки")
        desc.setdefault("таблица где проверяем", "")
        desc.setdefault("поле для проверки", "")
        desc.setdefault("комментарий", "")
        row = {
            "ТИП ПРОВЕРКИ": desc.get("ТИП ПРОВЕРКИ", ""),
            "Описание": desc.get("Описание", ""),
            "таблица источник": desc.get("таблица источник", sheet_name),
            "поле источник": desc.get("поле источник", ""),
            "таблица где проверяем": desc.get("таблица где проверяем", ""),
            "поле для проверки": desc.get("поле для проверки", ""),
            "параметр сравнения": param_compare,
            "комментарий": desc.get("комментарий", ""),
            "check_id": "csv_columns_count",
            "sheet": sheet_name,
            "name": name_text,
            "имя_колонки": "",
            "type": "csv_columns_count",
            "total_rows": total_rows,
            "violations": violations,
            "sample": sample_str,
        }
        new_rows.append(row)

    if not new_rows:
        return
    params = {"sheet": summary_sheet_name, "max_col_width": 80, "col_width_mode": "AUTO", "min_col_width": 10}
    out_columns = desc_columns + base_columns
    if summary_sheet_name in sheets_data:
        item = sheets_data[summary_sheet_name]
        if item and isinstance(item, (list, tuple)) and len(item) >= 1:
            df_summary, params = item[0], item[1]
            if isinstance(df_summary, pd.DataFrame):
                out_columns = df_summary.columns.tolist()
                # Строки new_rows уже содержат все колонки (описание + базовые)
                rows_for_df = [row for row in new_rows]
                extra_df = pd.DataFrame(rows_for_df, columns=out_columns)
                combined = pd.concat([df_summary, extra_df], axis=0, ignore_index=True)
                sheets_data[summary_sheet_name] = (combined, params)
                logging.info(f"[CONSISTENCY] Добавлено записей проверки числа полей CSV: {len(new_rows)}")
                return
    # Новый лист: создаём с полным набором колонок (описание + базовые), таблица не пустая
    extra_df = pd.DataFrame(new_rows, columns=out_columns)
    sheets_data[summary_sheet_name] = (extra_df, params)
    logging.info(f"[CONSISTENCY] Создан лист {summary_sheet_name} с записями проверки числа полей CSV: {len(new_rows)}")


def _union_columns_ordered(dfs: List[pd.DataFrame]) -> List[str]:
    """Объединение имён колонок с сохранением порядка первого появления (как concat по строкам)."""
    out: List[str] = []
    seen: Set[str] = set()
    for df in dfs:
        for c in df.columns:
            if c not in seen:
                seen.add(c)
                out.append(c)
    return out


def _sort_source_sheets_for_aggregate(
    source_sheets: List[str],
    sheet_order: List[str],
    input_files: List[Dict[str, Any]],
) -> List[str]:
    """Порядок склейки: сначала индекс в sheet_order, иначе порядок в input_files."""
    pos_in_input: Dict[str, int] = {}
    for i, fc in enumerate(input_files):
        sn = fc.get("sheet")
        if isinstance(sn, str) and sn not in pos_in_input:
            pos_in_input[sn] = i
    order_index: Dict[str, int] = {}
    for i, name in enumerate(sheet_order):
        order_index[name] = i

    def key(sn: str) -> Tuple[int, int]:
        return (order_index.get(sn, 10**9), pos_in_input.get(sn, 10**9))

    return sorted(source_sheets, key=key)


def apply_aggregate_sheets(
    sheets_data: Dict[str, Any],
    raw_sheets: Dict[str, Any],
    input_files: List[Dict[str, Any]],
    sheet_order: List[str],
    summary: List[str],
) -> None:
    """
    Дополняет sheets_data (и при наличии — raw_sheets) объединёнными листами.

    В записи input_files необязательный ключ aggregate_into_sheet: непустое имя целевого листа.
    Все файлы с одним и тем же значением дают вертикальное объединение строк (один заголовок,
    порядок блоков — по sheet_order / порядку в input_files). Исходные листы не удаляются.
    """
    groups: Dict[str, List[str]] = defaultdict(list)
    for fc in input_files:
        target = (fc.get("aggregate_into_sheet") or "").strip()
        if not target:
            continue
        sn = fc.get("sheet")
        if not isinstance(sn, str) or not sn:
            continue
        if sn == target:
            logging.warning(
                f"[aggregate_into_sheet] Пропуск: лист «{sn}» совпадает с целевым именем агрегата"
            )
            continue
        if sn not in groups[target]:
            groups[target].append(sn)

    for target, sources in groups.items():
        ordered = _sort_source_sheets_for_aggregate(sources, sheet_order, input_files)
        present = [s for s in ordered if s in sheets_data and sheets_data[s] is not None]
        if not present:
            logging.warning(f"[aggregate_into_sheet] Цель «{target}»: нет загруженных исходных листов")
            continue
        dfs: List[pd.DataFrame] = []
        first_conf: Optional[Dict[str, Any]] = None
        for s in present:
            pair = sheets_data[s]
            df_part = pair[0]
            if df_part is None or not isinstance(df_part, pd.DataFrame):
                continue
            dfs.append(df_part)
            if first_conf is None:
                first_conf = pair[1] if isinstance(pair[1], dict) else {}
        if not dfs:
            continue
        cols = _union_columns_ordered(dfs)
        aligned = [df.reindex(columns=cols) for df in dfs]
        merged = pd.concat(aligned, ignore_index=True)
        synth = copy.deepcopy(first_conf) if first_conf else {}
        synth["sheet"] = target
        synth["aggregate_into_sheet"] = ""
        synth["_aggregate_sources"] = present
        if target in sheets_data and sheets_data[target] is not None:
            logging.warning(
                f"[aggregate_into_sheet] Лист «{target}» уже существует — перезапись объединёнными данными"
            )
        sheets_data[target] = (merged, synth)
        summary.append(f"{target}: {len(merged)} строк (агрегат из {len(present)} листов)")
        logging.info(
            f"[aggregate_into_sheet] Лист «{target}»: {len(merged)} строк, источники: {', '.join(present)}"
        )

        raw_parts: List[pd.DataFrame] = []
        raw_first_conf: Optional[Dict[str, Any]] = None
        for s in present:
            if s not in raw_sheets:
                continue
            raw_pair = raw_sheets[s]
            rdf = raw_pair[0]
            if rdf is None or not isinstance(rdf, pd.DataFrame):
                continue
            raw_parts.append(rdf)
            if raw_first_conf is None and isinstance(raw_pair[1], dict):
                raw_first_conf = raw_pair[1]
        if raw_parts:
            rcols = _union_columns_ordered(raw_parts)
            raligned = [df.reindex(columns=rcols) for df in raw_parts]
            rmerged = pd.concat(raligned, ignore_index=True)
            rsynth = copy.deepcopy(raw_first_conf) if raw_first_conf else copy.deepcopy(synth)
            rsynth["sheet"] = target
            rsynth["aggregate_into_sheet"] = ""
            rsynth["_aggregate_sources"] = present
            raw_sheets[target] = (rmerged, rsynth)


@debug_timed()
def build_stat_file_sheet(
    input_files: List[Dict[str, Any]],
    sheets_data: Dict[str, Any],
    run_datetime: datetime,
) -> pd.DataFrame:
    """
    Формирует лист STAT_FILE со статистикой по исходным файлам: имя файла, лист, дата файла,
    дата обновления данных, количество записей и колонок, размер файла, статус.
    """
    rows = []
    for file_conf in input_files:
        file_name = file_conf.get("file", "")
        sheet_name = file_conf.get("sheet", "")
        subdir = (file_conf.get("subdir") or "").strip()
        search_dir = os.path.join(DIR_INPUT, subdir) if subdir else DIR_INPUT
        file_path = find_file_case_insensitive(search_dir, file_name, [".csv", ".CSV"])
        if file_path is None:
            file_date = ""
            file_size = 0
            status = "не найден"
            row_count = 0
            col_count = 0
        else:
            try:
                mtime = os.path.getmtime(file_path)
                file_date = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
                file_size = os.path.getsize(file_path)
            except OSError:
                file_date = ""
                file_size = 0
            status = "OK"
            if sheet_name in sheets_data and sheets_data[sheet_name] is not None:
                df_sheet = sheets_data[sheet_name][0]
                row_count = len(df_sheet) if df_sheet is not None else 0
                col_count = len(df_sheet.columns) if df_sheet is not None else 0
            else:
                row_count = 0
                col_count = 0
        data_update_date = run_datetime.strftime("%Y-%m-%d %H:%M:%S")
        rows.append({
            "FILE_NAME": file_name,
            "SHEET_NAME": sheet_name,
            "FILE_DATE": file_date,
            "DATA_UPDATE_DATE": data_update_date,
            "ROW_COUNT": row_count,
            "COL_COUNT": col_count,
            "FILE_SIZE_BYTES": file_size,
            "STATUS": status,
        })
    return pd.DataFrame(rows)


def print_final_report(
    validation_report: List[Dict[str, Any]],
    csv_mismatch_report: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Выводит итоговый отчёт по отклонениям длины полей и расхождениям по числу полей в CSV.
    Дубликаты отображаются в сводке консистентности (лист CONSISTENCY) и в логе проверок.
    """
    if csv_mismatch_report is None:
        csv_mismatch_report = []
    lines: List[str] = []
    lines.append("")
    lines.append("========== ИТОГОВАЯ СТАТИСТИКА: ОТКЛОНЕНИЯ ДЛИНЫ ПОЛЕЙ И РАСХОЖДЕНИЯ CSV ==========")

    if validation_report:
        lines.append("--- Отклонения по длине полей (проверки консистентности) ---")
        for r in validation_report:
            lines.append(f"  Лист: {r['sheet']}, колонка результата: {r['result_column']}")
            lines.append(f"  Количество строк с отклонениями: {r['n_violations']}")
            for i, sample in enumerate(r["sample_values"][:10], 1):
                lines.append(f"    Пример {i}: {sample}")
            if len(r["sample_values"]) > 10:
                lines.append(f"    ... и ещё {len(r['sample_values']) - 10} вариантов")
            lines.append("")
    else:
        lines.append("--- Отклонения по длине полей: не обнаружены ---")

    if csv_mismatch_report:
        lines.append("--- Расхождения по числу полей в CSV ---")
        for r in csv_mismatch_report:
            lines.append(
                f"  Файл: {r.get('file', '')}, лист: {r.get('sheet', '')}, "
                f"строка: {r.get('row_index', '')}, ожидалось полей: {r.get('expected_cols', '')}, "
                f"фактически: {r.get('actual_cols', '')}, направление: {r.get('direction', '')}"
            )
        lines.append("")
    else:
        lines.append("--- Расхождения по числу полей в CSV: не обнаружены ---")

    lines.append("===============================================================================")
    lines.append("")

    # Подробный многострочный отчёт — только в лог; консоль — print_validation_and_csv_compact
    for line in lines:
        logging.info(line.strip() if line.strip() else "")


def _console_footer(
    log_file: str,
    output_excel: Optional[str] = None,
    banner: str = "Готово",
    *,
    files_processed: Optional[int] = None,
    rows_total: Optional[int] = None,
    summary_parts: Optional[List[str]] = None,
) -> None:
    """Итоговая сводка в консоль: обработка, этапы, топ функций, пути, время."""
    summ = get_run_summary_for_console()
    if files_processed is not None and rows_total is not None:
        console_ui.print_data_processing_summary(files_processed, rows_total, summary_parts)
    console_ui.print_phases_table(summ["phases"])
    console_ui.print_top_functions(summ["top_functions"])
    console_ui.print_paths_and_total_time(output_excel, log_file, summ["total_sec"])
    console_ui.print_banner(banner)


def _write_stat_file_perf_excel(
    run_output_dir: str,
    start_time: datetime,
    run_mode_label: str,
) -> Optional[str]:
    """Отдельная книга STAT_FILE <таймштамп>.xlsx — только при stat_file_only в run_outputs."""
    if not RUN_WRITE_STAT_FILE:
        return None
    out_path = write_performance_statistics_excel(
        run_output_dir,
        program_started_at=start_time.strftime("%Y-%m-%d %H:%M:%S"),
        run_mode_label=run_mode_label,
    )
    if out_path:
        logging.info(f"[main] Статистика времени: {out_path}")
    return out_path


def _write_manager_stats_excel(
    sheets_data: Dict[str, Any],
    run_output_dir: str,
    timestamp: str,
) -> Optional[str]:
    """Сбор уникальных табельных и запись отдельной книги MANAGER_STATS."""
    if not RUN_WRITE_MANAGER_STATS:
        return None
    from src.manager_stats import build_manager_stats_workbook_data

    ms_data = build_manager_stats_workbook_data(
        sheets_data,
        INPUT_FILES,
        MANAGER_STATS,
        paths_cfg={"input": DIR_INPUT, "output": DIR_OUTPUT},
    )
    out_path = os.path.join(run_output_dir, f"{OUTPUT_FILENAME_MANAGER_STATS} {timestamp}.xlsx")
    with debug_phase("08_write_manager_stats_excel"):
        write_to_excel(ms_data, out_path, use_color_scheme=False)
    tab_sheet = (MANAGER_STATS or {}).get("output_sheet") or "TAB_NUMBERS"
    n_tabs = 0
    if tab_sheet in ms_data and ms_data[tab_sheet][0] is not None:
        n_tabs = len(ms_data[tab_sheet][0])
    logging.info(f"MANAGER_STATS записан: {n_tabs} уникальных табельных ({out_path})")
    console_ui.print_manager_stats_summary(n_tabs, out_path)
    from src.leaders_for_admin_auto_js import write_tournament_leaders_auto_js

    js_path = write_tournament_leaders_auto_js(
        run_output_dir,
        sheets_data=sheets_data,
        manager_stats_cfg=MANAGER_STATS,
        full_cfg={
            "run_outputs": RUN_OUTPUTS,
            "paths": {"input": DIR_INPUT, "output": DIR_OUTPUT},
            "input_files": INPUT_FILES,
        },
    )
    if js_path:
        logging.info(f"[main] leadersForAdmin JS: {js_path}")
    from src.profile_gp_auto_js import write_profile_gp_auto_js

    tab_sheet = (MANAGER_STATS or {}).get("output_sheet") or "TAB_NUMBERS"
    df_tabs_ms = None
    if tab_sheet in ms_data and ms_data[tab_sheet][0] is not None:
        df_tabs_ms = ms_data[tab_sheet][0]
    profile_js_path = write_profile_gp_auto_js(
        run_output_dir,
        df_tabs=df_tabs_ms,
        manager_stats_cfg={**(MANAGER_STATS or {}), "_paths": {"input": DIR_INPUT, "output": DIR_OUTPUT}},
        full_cfg={
            "run_outputs": RUN_OUTPUTS,
            "paths": {"input": DIR_INPUT, "output": DIR_OUTPUT},
            "input_files": INPUT_FILES,
        },
    )
    if profile_js_path:
        logging.info(f"[main] profile GP JS: {profile_js_path}")
    return out_path


def select_consistency_sheets(sheets_data: Dict[str, Any], summary_sheet_name: str) -> Dict[str, Any]:
    """
    Листы для книги консистентности: свод + листы, у которых в своде violations > 0 (STR-03, BUG-10).
    violations читается через to_numeric: пустые/текстовые значения считаются 0, а не роняют программу.
    """
    out_sheets = {summary_sheet_name}
    item = sheets_data.get(summary_sheet_name)
    if item is not None:
        df = item[0]
        if isinstance(df, pd.DataFrame) and "violations" in df.columns and "sheet" in df.columns:
            viol = pd.to_numeric(df["violations"], errors="coerce").fillna(0)
            for s in df.loc[viol > 0, "sheet"].dropna().astype(str).unique().tolist():
                if sheets_data.get(s) is not None:
                    out_sheets.add(s)
    return {k: v for k, v in sheets_data.items() if k in out_sheets}


class _LogLevelCounter(logging.Handler):
    """Считает записи WARNING/ERROR/CRITICAL за прогон: итог в консоль и код возврата (LOG-02)."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.counts: Dict[str, int] = defaultdict(int)

    def emit(self, record: logging.LogRecord) -> None:
        self.counts[record.levelname] += 1

    @property
    def warnings(self) -> int:
        return self.counts["WARNING"]

    @property
    def errors(self) -> int:
        return self.counts["ERROR"] + self.counts["CRITICAL"]


def _run_block_safely(block: str, log_file: str) -> int:
    """
    Один блок с перехватом ошибок: остальные блоки продолжают работу (LOG-02, BUG-06).
    Возвращает код: 0 — успех, 1 — ошибка обработки/записи, 2 — нет входных файлов.
    """
    try:
        _run_pipeline_for_block(block, log_file)
        return EXIT_OK
    except MissingInputFilesError as e:
        logging.error(str(e))
        console_ui.stderr_message(e.message_lines)
        return EXIT_MISSING_INPUT
    except OutputWriteError as e:
        # traceback уже записан в write_to_excel
        logging.error(f"[main] Блок {block}: {e}")
        console_ui.stderr_message([f"ОШИБКА (блок {block}): {e}"])
        return EXIT_PROCESSING_ERROR
    except Exception as e:
        logging.exception(f"[main] Блок {block} прерван ошибкой: {e}")
        console_ui.stderr_message([f"ОШИБКА (блок {block}): {e}", f"Подробности — в лог-файле: {log_file}"])
        return EXIT_PROCESSING_ERROR


def _parallel_block_worker(payload: Tuple[str, str]) -> Tuple[str, str, Optional[str], int, int, int]:
    """
    Воркер процесса: один блок целиком.
    Возвращает (block, console_text, error_or_None, код, предупреждений, ошибок).
    Вывод stdout/stderr буферизуется — родитель печатает пачками без перемешивания.
    """
    import contextlib
    import io
    import traceback

    config_path, block = payload
    buf = io.StringIO()
    err: Optional[str] = None
    code = EXIT_PROCESSING_ERROR
    counter = _LogLevelCounter()
    try:
        from src.config_holder import set_current_config
        from src.config_loader import Config

        cfg = Config(config_path)
        # В дочернем процессе — только этот блок
        cfg.run_blocks = [block]
        cfg.run_blocks_parallel = False
        set_current_config(cfg)
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            _load_config_globals()
            set_current_block(block)
            apply_run_block_context(block)
            log_file = setup_logger(block_suffix=block)
            # Счётчик — после setup_logger: иначе hasHandlers() и файл лога не создаётся
            logging.getLogger().addHandler(counter)
            code = _run_block_safely(block, log_file)
    except Exception:
        err = traceback.format_exc()
    return block, buf.getvalue(), err, code, counter.warnings, counter.errors


def main() -> int:
    """Запуск всех блоков из run_blocks. Возвращает код: 0 — успех, 1 — ошибки, 2 — нет входных файлов."""
    console_ui.configure_console_output()
    # Повторная загрузка глобалов при запуске (подхват внедрённого Config из config_holder)
    _load_config_globals()
    overall_start = datetime.now()
    log_file = setup_logger()
    counter = _LogLevelCounter()
    # Счётчик — после setup_logger: иначе hasHandlers() и файл лога не создаётся
    logging.getLogger().addHandler(counter)
    logging.info(f"[env] {environment_summary()}")
    _removed_logs = cleanup_old_logs(DIR_LOGS, LOG_BASE_NAME, LOG_RETENTION_DAYS)
    if _removed_logs:
        logging.info(f"[logs] Удалено старых лог-файлов (старше {LOG_RETENTION_DAYS} дн.): {_removed_logs}")
    blocks = list(RUN_BLOCKS) if RUN_BLOCKS else ["PROM"]
    parallel = bool(RUN_BLOCKS_PARALLEL) and len(blocks) > 1
    logging.info(
        f"=== Старт программы: {overall_start.strftime('%Y-%m-%d %H:%M:%S')}; "
        f"run_blocks={blocks}; parallel={parallel} ==="
    )
    console_ui.print_banner(
        f"SPOD — старт (блоки: {', '.join(blocks)}"
        + ("; параллельно" if parallel else "")
        + ")"
    )

    codes: List[int] = []
    warnings_total = 0
    errors_total = 0
    try:
        if parallel:
            # Отдельный процесс на блок — изоляция глобалов; консоль — пачками в порядке run_blocks
            cfg_path = CONFIG_PATH or default_config_path()
            payloads = [(cfg_path, b) for b in blocks]
            # max_workers <= число блоков; только stdlib
            from concurrent.futures import ProcessPoolExecutor, as_completed

            results_by_block: Dict[str, Tuple[str, Optional[str], int, int, int]] = {}
            with ProcessPoolExecutor(max_workers=len(blocks)) as pool:
                future_map = {pool.submit(_parallel_block_worker, p): p[1] for p in payloads}
                for fut in as_completed(future_map):
                    block_done, text, err, code, n_warn, n_err = fut.result()
                    results_by_block[block_done] = (text, err, code, n_warn, n_err)
            # Печать в порядке конфигурации — без перемешивания
            for b in blocks:
                text, err, code, n_warn, n_err = results_by_block.get(
                    b, ("", f"нет результата для блока {b}", EXIT_PROCESSING_ERROR, 0, 1)
                )
                header = f"===== вывод блока {b} ====="
                console_print_lines(
                    [header] + (text.rstrip("\n").split("\n") if text else ["(нет вывода)"])
                )
                if err:
                    console_print_lines([f"===== ошибка блока {b} =====", err])
                    logging.error(f"Блок {b} (параллельный прогон) завершился с ошибкой: {err}")
                codes.append(code)
                warnings_total += n_warn
                errors_total += n_err
        else:
            for block in blocks:
                apply_run_block_context(block)
                codes.append(_run_block_safely(block, log_file))
    finally:
        logging.getLogger().removeHandler(counter)

    warnings_total += counter.warnings
    errors_total += counter.errors
    exit_code = max(codes) if codes else EXIT_OK
    if exit_code == EXIT_OK and errors_total:
        exit_code = EXIT_PROCESSING_ERROR
    logging.info(
        f"=== Все блоки завершены ({', '.join(blocks)}). "
        f"Общее время: {datetime.now() - overall_start}; "
        f"предупреждений: {warnings_total}, ошибок: {errors_total}; код возврата: {exit_code} ==="
    )
    console_ui.print_run_result(warnings_total, errors_total, log_file, exit_code)
    return exit_code


def _run_pipeline_for_block(block: str, log_file: str) -> None:
    """Полный пайплайн обработки для одного блока (PROM / IFT / PSI)."""
    global _csv_column_mismatches
    set_current_block(block)
    _csv_column_mismatches.clear()
    with _merge_key_conflicts_lock:
        _merge_key_conflicts.clear()
    start_time = datetime.now()
    reset_run_timing()
    console_ui.reset_phase_counter()
    set_debug_phase_console_hooks(console_ui.on_phase_start, console_ui.on_phase_end)
    console_ui.print_banner(f"SPOD_{block} — старт")
    # До первой фазы — число шагов прогресс-бара от набора run_outputs (или устаревшего run_mode)
    console_ui.set_phase_progress_total(
        console_ui.expected_phases_for_run_flags(
            RUN_SOURCE_ONLY_EXIT,
            RUN_WRITE_SOURCE,
            RUN_WRITE_MAIN,
            RUN_WRITE_CONSISTENCY_FILE,
            RUN_CONSISTENCY_EARLY,
            RUN_WRITE_MANAGER_STATS,
            MANAGER_STATS_EARLY,
        )
    )
    logging.info(
        f"=== Старт блока {block}: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ==="
    )
    logging.debug(
        "[PERF] Логирование: файл — DEBUG/INFO; консоль — WARNING+ и краткие этапы (console_ui). "
        "Итоговая таблица [PERF] в лог-файле при завершении процесса."
    )

    # Excel-форма BADGE: export/import (изолировано от main_only)
    from src.contest_badge_form.runner import (
        only_form_tokens,
        run_contest_badge_form_modes,
    )

    _cfg_for_form = CFG_RAW if isinstance(CFG_RAW, dict) and CFG_RAW else {}
    if run_contest_badge_form_modes(
        PROJECT_BASE_DIR, block, list(RUN_OUTPUTS), _cfg_for_form
    ):
        if only_form_tokens(list(RUN_OUTPUTS)):
            logging.info(
                "[contest_badge_form] Только токены формы — выход из пайплайна блока %s",
                block,
            )
            return

    # BUG-06: fail-fast — до чтения CSV, разворота JSON и записи SQLite-архива
    _raise_if_input_files_missing()

    sheets_data = {}
    archive_payload: Dict[str, Any] = {}
    files_processed = 0
    rows_total = 0
    summary = []
    consistency_results: List[Dict[str, Any]] = []
    # Метаданные матрицы RATING×ITEM (подсветка после записи основного Excel)
    _rating_matrix_meta: Optional[Dict[str, Any]] = None
    # Ранняя запись книги консистентности (main + consistency_only), до merge/Summary
    consistency_written_early = False
    # 1. Параллельное чтение всех CSV и разворот ВСЕХ JSON‑полей на каждом листе
    logging.info(f"Начало параллельного чтения CSV файлов (потоков: {MAX_WORKERS_IO})")

    lock = threading.Lock()  # Для безопасного доступа к sheets_data

    with debug_phase("01_parallel_csv_read_and_json_flatten"):
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_IO) as executor:  # I/O операция
            futures = {executor.submit(process_single_file, file_conf): file_conf for file_conf in INPUT_FILES}

            raw_sheets = {}
            for future in as_completed(futures):
                df, sheet_name, file_conf, df_raw, resolved_path = future.result()
                if df is not None and file_conf is not None:
                    with lock:
                        sheets_data[sheet_name] = (df, file_conf)
                        files_processed += 1
                        rows_total += len(df)
                        summary.append(f"{sheet_name}: {len(df)} строк")
                        # PERF-08: df_raw — уже отдельная копия из process_single_file; архив только читает
                        # его и выполняется до проверок консистентности (они добавляют колонки в raw_sheets)
                        if file_conf.get("include_in_source", True):
                            raw_sheets[sheet_name] = (
                                df_raw if df_raw is not None else pd.DataFrame(),
                                file_conf,
                            )
                        archive_payload[sheet_name] = {
                            "df_raw": df_raw,
                            "file_conf": file_conf,
                            "file_path": resolved_path,
                        }
                elif sheet_name:
                    summary.append(f"{sheet_name}: {'файл не найден' if file_conf is None else 'ошибка'}")

    logging.info(f"Параллельное чтение CSV файлов завершено. Обработано файлов: {files_processed}")

    # Объединённые листы (aggregate_into_sheet в input_files): дополняют данные, исходные листы сохраняются
    apply_aggregate_sheets(sheets_data, raw_sheets, INPUT_FILES, SHEET_ORDER, summary)

    # Архив сырых CSV в SQLite (опционально, config input_archive_sqlite.enabled)
    if INPUT_ARCHIVE_SQLITE.get("enabled"):
        try:
            if INPUT_ARCHIVE_SQLITE.get("row_level_archive"):
                from src.input_archive_sqlite_v2 import run_input_archive_sqlite_v2

                run_input_archive_sqlite_v2(
                    PROJECT_BASE_DIR, INPUT_ARCHIVE_SQLITE, archive_payload
                )
            else:
                from src.input_archive_sqlite import run_input_archive_sqlite

                cfg_v1 = dict(INPUT_ARCHIVE_SQLITE)
                legacy_db = (cfg_v1.get("legacy_db_path") or "").strip()
                if legacy_db:
                    cfg_v1["db_path"] = legacy_db
                run_input_archive_sqlite(PROJECT_BASE_DIR, cfg_v1, archive_payload)
        except Exception:
            logging.exception(
                "[archive_sqlite] Ошибка записи архива во входной SQLite (продолжаем пайплайн)"
            )

    run_mode = int(RUN_MODE) if RUN_MODE is not None else 1
    _run_mode_label = (
        f"block={block}; run_outputs={RUN_OUTPUTS} (compat_mode={run_mode})"
    )
    logging.info(f"[main] Режим запуска: {_run_mode_label}")

    # Подкаталог вывода: OUT/<BLOCK>/YYYY/DD-MM
    run_output_dir = get_output_dir_for_run(DIR_OUTPUT, block=block)
    logging.info(f"[main] Выходной каталог блока {block}: {run_output_dir}")

    # Только source (в массиве ровно source_only) — проверка файлов, запись source, выход из блока
    if RUN_SOURCE_ONLY_EXIT:
        with debug_phase("mode2_source_only_excel"):
            write_source_excel(raw_sheets, run_output_dir)
        _write_stat_file_perf_excel(run_output_dir, start_time, _run_mode_label)
        logging.info(
            f"=== Блок {block}, режим 2 завершён. Выгружен только source. "
            f"Время: {datetime.now() - start_time} ==="
        )
        _console_footer(log_file, banner=f"Блок {block}: готово (только source)")
        return

    # 1.1. Выгрузка source Excel — если в run_outputs указан source_only (и это не «только source» с выходом выше).
    # Без source_only в массиве — как бывший main_only: без файла source.
    if RUN_WRITE_SOURCE:
        with debug_phase("full_mode_source_excel"):
            write_source_excel(raw_sheets, run_output_dir)

    # 5. Проверки консистентности на сырых данных (до EMPLOYEE, merge и т.д.); результаты потом попадут в конец листов
    summary_sheet_name = (CONSISTENCY_CHECKS or {}).get("summary_sheet_name", "CONSISTENCY")
    raw_sheets_data = build_raw_sheets_data_for_consistency(raw_sheets, sheets_data)
    # Число колонок и строк в сырых CSV — фиксируем до проверок (проверки добавляют колонки на листы)
    raw_counts = {}
    for s in raw_sheets_data:
        item = raw_sheets_data[s]
        if isinstance(item, (list, tuple)) and len(item) >= 1 and isinstance(item[0], pd.DataFrame):
            raw_counts[s] = {"ncols": len(item[0].columns), "nrows": len(item[0])}
    with debug_phase("02_consistency_pipeline_raw_and_csv_mismatch"):
        if CONSISTENCY_CHECKS and (CONSISTENCY_CHECKS.get("rules")):
            logging.info("[main] Запуск проверок консистентности на сырых данных (до обработки)")
            consistency_results = run_consistency_checks_and_attach_summary(
                raw_sheets_data,
                CONSISTENCY_CHECKS,
                current_block=block,
                max_workers=MAX_WORKERS,
            )
            copy_consistency_results_from_raw_to_processed(raw_sheets_data, sheets_data, summary_sheet_name)
            logging.info("[main] Проверки консистентности завершены, результаты скопированы на обработанные листы")
            console_ui.print_consistency_summary(
                consistency_results, rules=CONSISTENCY_CHECKS.get("rules"), block=block
            )
        else:
            # В логе правила не запускались; в консоли — кратко, чтобы итог был предсказуемым
            console_ui.print_consistency_summary(
                consistency_results, rules=CONSISTENCY_CHECKS.get("rules"), block=block
            )
        append_csv_mismatches_to_consistency(
            sheets_data, list(_csv_column_mismatches),
            summary_sheet_name=summary_sheet_name,
            consistency_checks_config=CONSISTENCY_CHECKS,
            raw_sheets_data=raw_sheets_data,
            raw_counts=raw_counts,
        )

        if RUN_WRITE_MAIN and RUN_WRITE_CONSISTENCY_FILE and not RUN_CONSISTENCY_EARLY:
            consistency_early_data = select_consistency_sheets(sheets_data, summary_sheet_name)
            ts_e = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            consistency_early_path = os.path.join(
                run_output_dir, f"{OUTPUT_FILENAME_CONSISTENCY} {ts_e}.xlsx"
            )
            write_to_excel(consistency_early_data, consistency_early_path, use_color_scheme=False)
            logging.info(f"Ранняя книга консистентности записана: {consistency_early_path}")
            consistency_written_early = True

    with debug_phase("03_gender_tournament_merge_reward_summary"):
        # 2. Добавление колонки AUTO_GENDER для листа EMPLOYEE (пропускаем в режиме consistency_only)
        if not RUN_CONSISTENCY_EARLY and "EMPLOYEE" in sheets_data:
            df_employee, conf_employee = sheets_data["EMPLOYEE"]
            # PERF-03: только векторизованная версия; совпадение с построчной проверяется в
            # src/Tests/test_auto_gender_equivalence.py (раньше обе считались в каждом прогоне)
            df_base = df_employee.drop(columns=["AUTO_GENDER"], errors="ignore")
            df_employee = add_auto_gender_column_vectorized(df_base.copy(), "EMPLOYEE")
            sheets_data["EMPLOYEE"] = (df_employee, conf_employee)

        # 3. Расчётный статус турнира для TOURNAMENT-SCHEDULE
        if not RUN_CONSISTENCY_EARLY and "TOURNAMENT-SCHEDULE" in sheets_data:
            df_tournament, conf_tournament = sheets_data["TOURNAMENT-SCHEDULE"]
            df_report = sheets_data.get("REPORT", (None, None))[0]
            df_tournament = calculate_tournament_status(df_tournament, df_report)
            sheets_data["TOURNAMENT-SCHEDULE"] = (df_tournament, conf_tournament)

        # 4. Merge fields и сводка REWARD getCondition
        if not RUN_CONSISTENCY_EARLY:
            merge_fields_across_sheets(
                sheets_data,
                [f for f in MERGE_FIELDS_ADVANCED if f.get("sheet_dst") != "SUMMARY"],
                count_column_prefix="COUNT",
                merge_name="MERGE_FIELDS_ADVANCED",
            )

            _rgs = REWARD_GETCONDITION_SUMMARY or {}
            if _rgs.get("enabled", True) and "REWARD" in sheets_data:
                from src.reward_getcondition_summary import add_reward_getcondition_summary_column

                _prefix = "ADD_DATA"
                _rc_list = JSON_COLUMNS.get("REWARD") or []
                if _rc_list and isinstance(_rc_list[0], dict):
                    _prefix = (_rc_list[0].get("prefix") or "ADD_DATA").strip() or "ADD_DATA"
                _col_name = _rgs.get("column_name") or "Сводка: nonRewards и rewards (getCondition)"
                _df_r, _conf_r = sheets_data["REWARD"]
                sheets_data["REWARD"] = (
                    add_reward_getcondition_summary_column(_df_r, prefix=_prefix, column_name=_col_name),
                    _conf_r,
                )

            # Матрица наград ITEM на листе RATING (после merge и сводки REWARD)
            if RUN_RATING_ITEM_MATRIX and RATING_ITEM_MATRIX.get("enabled", True):
                from src.rating_item_matrix import apply_rating_item_matrix_enrichment

                _rating_matrix_meta = apply_rating_item_matrix_enrichment(sheets_data, RATING_ITEM_MATRIX)
            elif not RUN_RATING_ITEM_MATRIX:
                logging.info(
                    "[rating_item_matrix] Пропуск — в run_outputs нет токена rating_item_matrix"
                )

            _sos = SEASON_ORDER_SUMMARY or {}
            if RUN_SEASON_ORDER_SUMMARY and _sos.get("enabled", True):
                from src.season_order_summary import apply_season_order_summary

                _sos_cfg = {"season_order_summary": _sos, "rating_item_matrix": RATING_ITEM_MATRIX}
                _summary_sheet = apply_season_order_summary(sheets_data, _sos_cfg)
                if _summary_sheet and _summary_sheet not in SHEET_ORDER:
                    SHEET_ORDER.append(_summary_sheet)
            elif not RUN_SEASON_ORDER_SUMMARY:
                logging.info(
                    "[season_order_summary] Пропуск — в run_outputs нет токена season_order_summary"
                )

    # Только статистика менеджеров без main (manager_stats_only без main_only)
    if MANAGER_STATS_EARLY:
        _report_merge_key_conflicts_summary()
        ts_ms = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        manager_stats_path = _write_manager_stats_excel(sheets_data, run_output_dir, ts_ms)
        _write_stat_file_perf_excel(run_output_dir, start_time, _run_mode_label)
        logging.info(
            f"=== Режим manager_stats_only завершён. Файл: {manager_stats_path}. "
            f"Время: {datetime.now() - start_time} ==="
        )
        _console_footer(
            log_file,
            output_excel=manager_stats_path or "",
            banner="Режим manager_stats_only: готово",
            files_processed=files_processed,
            rows_total=rows_total,
            summary_parts=summary,
        )
        return

    # Только отдельная книга консистентности без main (в массиве есть consistency_only, нет main_only)
    if RUN_CONSISTENCY_EARLY:
        consistency_data = select_consistency_sheets(sheets_data, summary_sheet_name)
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        consistency_path = os.path.join(run_output_dir, f"{OUTPUT_FILENAME_CONSISTENCY} {ts}.xlsx")
        with debug_phase("04_consistency_only_write_excel"):
            write_to_excel(consistency_data, consistency_path, use_color_scheme=False)
        _write_stat_file_perf_excel(run_output_dir, start_time, _run_mode_label)
        logging.info(f"=== Режим 4 завершён. Файл консистентности: {consistency_path}. Время: {datetime.now() - start_time} ===")
        _console_footer(
            log_file,
            output_excel=consistency_path,
            banner="Режим 4: готово (консистентность)",
            files_processed=files_processed,
            rows_total=rows_total,
            summary_parts=summary,
        )
        return

    # 6–8. Основная книга Excel — только если в run_outputs есть main_only
    output_excel = ""
    if RUN_WRITE_MAIN:
        with debug_phase("05_summary_stat_baseline"):
            dfs = {k: v[0] for k, v in sheets_data.items()}
            df_summary = build_summary_sheet(
                dfs,
                params_summary=SUMMARY_SHEET,
                merge_fields=[f for f in MERGE_FIELDS_ADVANCED if f.get("sheet_dst") == "SUMMARY"],
            )
            if df_summary is None or not isinstance(df_summary, pd.DataFrame):
                logging.error("[main] КРИТИЧЕСКАЯ ОШИБКА: df_summary равен None или не DataFrame после build_summary_sheet!")
                logging.error("[main] Создаем пустой DataFrame для SUMMARY")
                df_summary = pd.DataFrame(columns=SUMMARY_KEY_COLUMNS)
            elif len(df_summary) == 0:
                logging.warning("[main] df_summary пустой после build_summary_sheet, но продолжаем работу")
            else:
                logging.info(f"[main] df_summary успешно создан: {len(df_summary)} строк, {len(df_summary.columns)} колонок")

            sheets_data[SUMMARY_SHEET["sheet"]] = (df_summary, SUMMARY_SHEET)

            df_stat = build_stat_file_sheet(INPUT_FILES, sheets_data, start_time)
            stat_file_params = {
                "sheet": "STAT_FILE",
                "max_col_width": 80,
                "freeze": "A2",
                "col_width_mode": "AUTO",
                "min_col_width": 10,
            }
            sheets_data["STAT_FILE"] = (df_stat, stat_file_params)
            logging.info(f"[main] Лист STAT_FILE сформирован: {len(df_stat)} строк (статистика по файлам)")

            _baseline_path = os.path.join(run_output_dir, "merge_output_baseline.json")
            if os.environ.get("SAVE_MERGE_BASELINE") == "1":
                snapshot = _dump_sheets_data_for_baseline(sheets_data, max_rows=3)
                with open(_baseline_path, "w", encoding="utf-8") as f:
                    json.dump(snapshot, f, ensure_ascii=False, indent=2)
                logging.info(f"[MERGE] Baseline сохранён: {_baseline_path} (колонки и по 3 строки на лист)")
            elif os.path.isfile(_baseline_path):
                ok, diff_errors = _compare_sheets_data_with_baseline(sheets_data, _baseline_path, max_rows=3)
                if ok:
                    logging.info("[MERGE] Сравнение с baseline: колонки и сэмпл данных совпадают")
                else:
                    for msg in diff_errors:
                        logging.warning(f"[MERGE] Baseline расхождение: {msg}")

        output_excel = os.path.join(run_output_dir, get_output_filename())
        with debug_phase("06_write_main_excel"):
            write_to_excel(sheets_data, output_excel)
        _wt_main_elapsed = run_elapsed_sec()
        logging.info(f"Основная книга записана: {output_excel} (от старта прогона ~{_wt_main_elapsed:.2f} s)")

        if _rating_matrix_meta:
            from src.rating_item_matrix import apply_rating_item_matrix_colors

            apply_rating_item_matrix_colors(output_excel, _rating_matrix_meta, RATING_ITEM_MATRIX)

        # 8.1. Отдельный файл consistency — если в run_outputs указаны и main_only, и consistency_only
        if RUN_WRITE_CONSISTENCY_FILE and not consistency_written_early:
            consistency_data = select_consistency_sheets(sheets_data, summary_sheet_name)
            ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            consistency_path = os.path.join(run_output_dir, f"{OUTPUT_FILENAME_CONSISTENCY} {ts}.xlsx")
            with debug_phase("07_write_consistency_excel_full_mode"):
                write_to_excel(consistency_data, consistency_path, use_color_scheme=False)
            logging.info(f"Книга консистентности записана: ({consistency_path})")

    # 8.2. MANAGER_STATS — если токен в run_outputs (отдельно или вместе с main_only)
    manager_stats_path: Optional[str] = None
    if RUN_WRITE_MANAGER_STATS and not MANAGER_STATS_EARLY:
        ts_ms = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        manager_stats_path = _write_manager_stats_excel(sheets_data, run_output_dir, ts_ms)

    _write_stat_file_perf_excel(run_output_dir, start_time, _run_mode_label)

    _report_merge_key_conflicts_summary()

    # Итоговая статистика по отклонениям длины полей и расхождениям по числу полей в CSV (дубликаты — в сводке консистентности)
    validation_report, csv_mismatch_report = collect_duplicates_and_validation_report(sheets_data)
    print_final_report(validation_report, csv_mismatch_report)
    console_ui.print_validation_and_csv_compact(validation_report, csv_mismatch_report)

    time_elapsed = datetime.now() - start_time
    logging.info(
        f"=== Завершение блока {block}. Обработано файлов: {files_processed}, "
        f"строк всего: {rows_total}. Время выполнения: {time_elapsed} ==="
    )
    logging.info(f"Summary: {'; '.join(summary)}")
    if output_excel:
        logging.info(f"Excel file: {output_excel}")
    if manager_stats_path:
        logging.info(f"Manager stats file: {manager_stats_path}")
    logging.info(f"Log file: {log_file}")

    _console_footer(
        log_file,
        output_excel=output_excel or manager_stats_path or "",
        banner=f"Блок {block}: обработка завершена",
        files_processed=files_processed,
        rows_total=rows_total,
        summary_parts=summary,
    )


if __name__ == "__main__":
    sys.exit(main())
