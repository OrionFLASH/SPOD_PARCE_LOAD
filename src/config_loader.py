# -*- coding: utf-8 -*-
"""
Загрузка и хранение конфигурации из config.json.
Все параметры обработки доступны через атрибуты класса Config.
"""

import json
import os
from typing import Any, Dict, List, Optional, Set, Tuple

# Допустимые элементы массива run_outputs в config.json (какие выходные файлы формировать)
_RUN_OUTPUT_TOKENS: Set[str] = frozenset(
    {
        "source_only",
        "main_only",
        "consistency_only",
        "manager_stats_only",
        "stat_file_only",
        "rating_item_matrix",
        "season_order_summary",
    }
)

# Допустимые блоки входных SPOD-данных (среды PROM / IFT / PSI)
_RUN_BLOCK_TOKENS: Set[str] = frozenset({"PROM", "IFT", "PSI"})
_DEFAULT_RUN_BLOCKS: List[str] = ["PROM"]


def parse_run_blocks_config(cfg: Dict[str, Any]) -> List[str]:
    """
    Разбор run_blocks: какие блоки (PROM / IFT / PSI) обрабатывать в одном запуске.

    По умолчанию — только PROM. Порядок в массиве сохраняется (без дублей).
    """
    raw = cfg.get("run_blocks")
    if raw is None:
        return list(_DEFAULT_RUN_BLOCKS)
    if not isinstance(raw, list):
        raise ValueError(
            "run_blocks: ожидается массив строк (например [\"PROM\"] или [\"PROM\", \"IFT\", \"PSI\"])"
        )
    result: List[str] = []
    seen: Set[str] = set()
    for item in raw:
        if not isinstance(item, str):
            continue
        token = item.strip().upper()
        if not token:
            continue
        if token not in _RUN_BLOCK_TOKENS:
            raise ValueError(
                f"run_blocks: неизвестный блок «{item}». "
                f"Допустимо: {', '.join(sorted(_RUN_BLOCK_TOKENS))}"
            )
        if token in seen:
            continue
        seen.add(token)
        result.append(token)
    if not result:
        raise ValueError(
            "run_blocks: укажите хотя бы одно из значений: PROM, IFT, PSI "
            "(по умолчанию используется [\"PROM\"])"
        )
    return result


def parse_input_files_by_block(cfg: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Разбор input_files: объект с разделами PROM / IFT / PSI (полный список файлов в каждом).

    Обратная совместимость: если input_files — плоский список (старый формат),
    он считается списком блока PROM; записи с полем block раскладываются по разделам.
    """
    raw = cfg.get("input_files")
    result: Dict[str, List[Dict[str, Any]]] = {b: [] for b in sorted(_RUN_BLOCK_TOKENS)}

    if isinstance(raw, dict):
        for key, value in raw.items():
            if not isinstance(key, str):
                continue
            block = key.strip().upper()
            if block not in _RUN_BLOCK_TOKENS:
                raise ValueError(
                    f"input_files: неизвестный раздел «{key}». "
                    f"Допустимо: {', '.join(sorted(_RUN_BLOCK_TOKENS))}"
                )
            if value is None:
                result[block] = []
                continue
            if not isinstance(value, list):
                raise ValueError(
                    f"input_files.{block}: ожидается массив записей файлов, получено {type(value).__name__}"
                )
            result[block] = [dict(item) for item in value if isinstance(item, dict)]
        return result

    if isinstance(raw, list):
        # Старый плоский список: разложить по полю block или целиком в PROM
        for item in raw:
            if not isinstance(item, dict):
                continue
            rec = dict(item)
            raw_block = rec.pop("block", None)
            if raw_block is None or (isinstance(raw_block, str) and not str(raw_block).strip()):
                # без block — добавить во все разделы (общие FILE в старой схеме)
                for b in result:
                    result[b].append(dict(rec))
                continue
            token = str(raw_block).strip().upper()
            if token in ("*", "ALL", "SHARED"):
                for b in result:
                    result[b].append(dict(rec))
                continue
            if token not in _RUN_BLOCK_TOKENS:
                raise ValueError(
                    f"input_files[].block: неизвестный блок «{raw_block}». "
                    f"Допустимо: {', '.join(sorted(_RUN_BLOCK_TOKENS))}"
                )
            result[token].append(rec)
        return result

    if raw is None:
        return result

    raise ValueError(
        "input_files: ожидается объект {\"PROM\": [...], \"IFT\": [...], \"PSI\": [...]} "
        "или устаревший плоский массив"
    )


def get_input_files_for_block(
    input_files_by_block: Dict[str, List[Dict[str, Any]]], block: str
) -> List[Dict[str, Any]]:
    """Список input_files для одного блока (копия списка раздела)."""
    block_u = str(block).strip().upper()
    if block_u not in _RUN_BLOCK_TOKENS:
        raise ValueError(
            f"Неизвестный блок «{block}». Допустимо: {', '.join(sorted(_RUN_BLOCK_TOKENS))}"
        )
    files = input_files_by_block.get(block_u) or []
    return list(files)


def filter_input_files_for_block(
    input_files: Any, block: str
) -> List[Dict[str, Any]]:
    """
    Получить список файлов блока.

    - Если input_files — dict разделов PROM/IFT/PSI — вернуть раздел блока.
    - Если плоский list — устаревшая фильтрация по полю block (совместимость).
    """
    if isinstance(input_files, dict):
        return get_input_files_for_block(input_files, block)

    if not isinstance(input_files, list):
        return []

    block_u = str(block).strip().upper()
    selected: List[Dict[str, Any]] = []
    for file_conf in input_files:
        if not isinstance(file_conf, dict):
            continue
        raw_block = file_conf.get("block")
        if raw_block is None or (isinstance(raw_block, str) and not raw_block.strip()):
            selected.append(file_conf)
            continue
        token = str(raw_block).strip().upper()
        if token in ("*", "ALL", "SHARED"):
            selected.append(file_conf)
            continue
        if token == block_u:
            selected.append(file_conf)
    return selected


def resolve_output_filename_template(template: str, block: str) -> str:
    """
    Подставляет имя блока в шаблон output_filenames.

    Плейсхолдеры: {BLOCK} / {block}. Если их нет — замена устаревшего префикса SPOD_PROM
    на SPOD_<BLOCK> (обратная совместимость со старым конфигом).
    """
    block_u = str(block).strip().upper()
    tpl = str(template or "")
    if "{BLOCK}" in tpl or "{block}" in tpl:
        return tpl.replace("{BLOCK}", block_u).replace("{block}", block_u)
    if "SPOD_PROM" in tpl:
        return tpl.replace("SPOD_PROM", f"SPOD_{block_u}")
    return tpl


def parse_run_outputs_config(cfg: Dict[str, Any]) -> Tuple[List[str], bool, bool, bool, bool, bool, bool, bool, bool, int, bool, bool]:
    """
    Разбор режима запуска: приоритет у run_outputs (массив строк).
    Возвращает:
      run_outputs_sorted — нормализованный список для логов;
      source_only_exit — только source и выход (в массиве ровно source_only);
      write_source — создавать файл source Excel;
      write_main — полный main Excel (SUMMARY, merge, …);
      write_consistency_file — отдельная книга консистентности;
      consistency_early — ранний выход с одной книгой консистентности (без main), бывший режим 4;
      write_manager_stats — отдельная книга статистики менеджеров (табельные и др.);
      manager_stats_early — только manager_stats без main (выход после merge);
      write_stat_file — отдельный Excel STAT_FILE <таймштамп>.xlsx (время этапов и функций);
      run_rating_item_matrix — колонки ITEM на листе RATING (rating_item_matrix);
      run_season_order_summary — лист ORDER-SEASON-SUMMARY (season_order_summary);
      run_mode_compat — число 1–4 для обратной совместимости и логов.
    """
    ro_raw = cfg.get("run_outputs")
    tokens: Set[str] = set()

    if isinstance(ro_raw, list) and len(ro_raw) > 0:
        for item in ro_raw:
            if not isinstance(item, str):
                continue
            t = item.strip().lower().replace("-", "_")
            if t in _RUN_OUTPUT_TOKENS:
                tokens.add(t)
        if not tokens:
            raise ValueError(
                "run_outputs: укажите хотя бы одно из значений: "
                "source_only, main_only, consistency_only, manager_stats_only, "
                "stat_file_only, rating_item_matrix, season_order_summary"
            )
    else:
        # Обратная совместимость: run_mode full | source_only | main_only | consistency_only | 1–4
        _raw = cfg.get("run_mode", 1)
        _run_mode_map = {"full": 1, "source_only": 2, "main_only": 3, "consistency_only": 4}
        if isinstance(_raw, str):
            _mode_val = _run_mode_map.get(_raw.strip().lower(), 1)
        else:
            _mode_val = int(_raw)
        if _mode_val == 1:
            tokens = {"source_only", "main_only", "consistency_only"}
        elif _mode_val == 2:
            tokens = {"source_only"}
        elif _mode_val == 3:
            tokens = {"main_only"}
        elif _mode_val == 4:
            tokens = {"consistency_only"}
        else:
            tokens = {"source_only", "main_only", "consistency_only"}

    source_only_exit = tokens == {"source_only"}
    write_source = "source_only" in tokens
    write_main = "main_only" in tokens
    write_consistency_file = "consistency_only" in tokens
    write_manager_stats = "manager_stats_only" in tokens
    write_stat_file = "stat_file_only" in tokens
    run_rating_item_matrix = "rating_item_matrix" in tokens
    run_season_order_summary = "season_order_summary" in tokens
    # Ранний «только консистентность» без основной книги — как старый режим 4
    consistency_early = write_consistency_file and not write_main
    # Только статистика менеджеров без основной книги — выход после merge
    manager_stats_early = write_manager_stats and not write_main

    # Число 1–4 для логов и совместимости со старым кодом
    if source_only_exit:
        run_mode_compat = 2
    elif consistency_early:
        run_mode_compat = 4
    elif write_main and not write_source and not write_consistency_file:
        run_mode_compat = 3
    elif write_main and write_source and write_consistency_file:
        run_mode_compat = 1
    else:
        run_mode_compat = 1  # гибриды (например source+main без отдельного файла консистентности)

    run_outputs_sorted = sorted(tokens)
    return (
        run_outputs_sorted,
        source_only_exit,
        write_source,
        write_main,
        write_consistency_file,
        consistency_early,
        write_manager_stats,
        manager_stats_early,
        write_stat_file,
        run_mode_compat,
        run_rating_item_matrix,
        run_season_order_summary,
    )


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

        # Входные файлы по блокам: input_files = { "PROM": [...], "IFT": [...], "PSI": [...] }
        self.input_files_by_block: Dict[str, List[Dict[str, Any]]] = parse_input_files_by_block(
            self._cfg
        )
        self.run_blocks: List[str] = parse_run_blocks_config(self._cfg)
        # Для текущего/первого блока (совместимость со старым кодом, ожидающим список)
        _first_block = self.run_blocks[0] if self.run_blocks else "PROM"
        self.input_files: List[Dict[str, Any]] = get_input_files_for_block(
            self.input_files_by_block, _first_block
        )
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

        # Устаревший параметр: проверки длины полей перенесены в consistency_checks (type: field_length). Оставлен пустой dict для совместимости.
        self.field_length_validations: Dict[str, Any] = self._cfg.get("field_length_validations") or {}

        # Merge, цвета, форматы, дубликаты, JSON
        self.merge_fields_advanced: List[Dict[str, Any]] = self._cfg["merge_fields_advanced"]
        self.color_scheme: List[Dict[str, Any]] = self._cfg.get("color_scheme") or []
        self.column_formats: List[Dict[str, Any]] = self._cfg.get("column_formats") or []
        # Устаревший параметр: проверки дублей перенесены в consistency_checks (type: unique). Оставлен пустой список для совместимости с validation.check_duplicates_single_sheet.
        self.check_duplicates: List[Dict[str, Any]] = self._cfg.get("check_duplicates") or []
        # Проверки консистентности: единый конфиг правил (referential, unique, field_length и т.д.)
        _cc = self._cfg.get("consistency_checks") or {}
        # Базовые ключи + любые дополнительные из JSON (подсказки, расширения), чтобы не терять поля вроде spod_todo_config_guide
        self.consistency_checks = {
            "summary_sheet_name": _cc.get("summary_sheet_name", "CONSISTENCY"),
            "rules": _cc.get("rules") or [],
            "csv_columns_count": _cc.get("csv_columns_count") or {},
        }
        for _k, _v in _cc.items():
            if _k not in self.consistency_checks:
                self.consistency_checks[_k] = _v
        self.json_columns: Dict[str, List[Dict[str, Any]]] = self._cfg.get("json_columns") or {}
        self.derived_columns: List[Dict[str, Any]] = self._cfg.get("derived_columns") or []
        # Сводная колонка по getCondition на листе REWARD (см. reward_getcondition_summary.py)
        self.reward_getcondition_summary: Dict[str, Any] = self._cfg.get("reward_getcondition_summary") or {}
        # Матрица ITEM на листе RATING (счётчики по ORDER, подсветка; см. rating_item_matrix.py)
        self.rating_item_matrix: Dict[str, Any] = self._cfg.get("rating_item_matrix") or {}
        # Сводка заказов по группам SEASON (см. season_order_summary.py)
        self.season_order_summary: Dict[str, Any] = self._cfg.get("season_order_summary") or {}
        self.manager_stats: Dict[str, Any] = self._cfg.get("manager_stats") or {}

        # Параллелизм
        self.max_workers_io: int = self._cfg["performance"]["max_workers_io"]
        self.max_workers_cpu: int = self._cfg["performance"]["max_workers_cpu"]
        self.max_workers: int = self.max_workers_cpu

        # Выгрузка сырых данных (source): сортировка листов при записи в SPOD_PROM source *.xlsx
        _source = self._cfg.get("source_export") or {}
        self.source_export_sort: List[Dict[str, Any]] = _source.get("sort_rules") or []

        # Статусы турнира
        _default_statuses = [
            "НЕОПРЕДЕЛЕН", "АКТИВНЫЙ", "ЗАПЛАНИРОВАН",
            "ПОДВЕДЕНИЕ ИТОГОВ", "ПОДВЕДЕНИЕ ИТОГОВ", "ПОДВЕДЕНИЕ ИТОГОВ", "ЗАВЕРШЕН",
        ]
        self.tournament_status_choices: List[str] = (
            self._cfg.get("tournament_status_choices") or _default_statuses
        )

        # Режим запуска: массив run_outputs или устаревший run_mode (см. parse_run_outputs_config)
        (
            self.run_outputs,
            self.run_source_only_exit,
            self.run_write_source,
            self.run_write_main,
            self.run_write_consistency_file,
            self.run_consistency_early,
            self.run_write_manager_stats,
            self.run_manager_stats_early,
            self.run_write_stat_file,
            self.run_mode,
            self.run_rating_item_matrix,
            self.run_season_order_summary,
        ) = parse_run_outputs_config(self._cfg)

        # Применять ли сортировку (sort_columns из input_files): к source — да по умолчанию, к main — нет по умолчанию
        self.apply_sort_to_source: bool = self._cfg.get("apply_sort_to_source", True)
        self.apply_sort_to_main: bool = self._cfg.get("apply_sort_to_main", False)

        # Имена/шаблоны выходных файлов (без расширения и без timestamp; плейсхолдер {BLOCK})
        _of = self._cfg.get("output_filenames") or {}
        self.output_filename_main_template: str = _of.get("main", "SPOD_{BLOCK} main")
        self.output_filename_source_template: str = _of.get("source", "SPOD_{BLOCK} source")
        self.output_filename_consistency_template: str = _of.get(
            "consistency", "SPOD_{BLOCK} consistency"
        )
        self.output_filename_manager_stats_template: str = _of.get(
            "manager_stats", "SPOD_{BLOCK} MANAGER_STATS"
        )
        # Значения по умолчанию для первого блока (совместимость со старым кодом)
        _default_block = self.run_blocks[0] if self.run_blocks else "PROM"
        self.output_filename_main: str = resolve_output_filename_template(
            self.output_filename_main_template, _default_block
        )
        self.output_filename_source: str = resolve_output_filename_template(
            self.output_filename_source_template, _default_block
        )
        self.output_filename_consistency: str = resolve_output_filename_template(
            self.output_filename_consistency_template, _default_block
        )
        self.output_filename_manager_stats: str = resolve_output_filename_template(
            self.output_filename_manager_stats_template, _default_block
        )

        # Архив входных CSV в SQLite (см. src/input_archive_sqlite.py, Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md)
        from src.input_archive_sqlite_v2 import merge_archive_v2_config

        self.input_archive_sqlite: Dict[str, Any] = merge_archive_v2_config(
            self._cfg.get("input_archive_sqlite")
        )

    @property
    def base_dir(self) -> str:
        """Корень проекта (каталог с config.json)."""
        return self._base_dir

    def get_output_filename(self) -> str:
        """Имя выходного Excel с датой и временем (шаблон из output_filenames.main)."""
        from datetime import datetime
        return f"{self.output_filename_main}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
