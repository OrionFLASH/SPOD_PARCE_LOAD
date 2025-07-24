import os
import sys
import pandas as pd
import logging
from datetime import datetime
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font, PatternFill
from time import time
import json
import re

# === Глобальные константы и переменные ===
# Каталоги
DIR_INPUT = r'/Users/orionflash/Desktop/MyProject/SPOD_PROM/SPOD'
DIR_OUTPUT = r'/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT'
DIR_LOGS = r'/Users/orionflash/Desktop/MyProject/SPOD_PROM/LOGS'

# Входные файлы (имя без расширения)
# Соответствие: Имя листа, максимальная ширина колонки, закрепление
INPUT_FILES = [
    {
        "file": "CONTEST-DATA (PROM) 2025-07-14 v0",
        "sheet": "CONTEST-DATA",
        "max_col_width": 120,
        "freeze": "C2"
    },
    {
        "file": "GROUP (PROM) 2025-07-14 v0",
        "sheet": "GROUP",
        "max_col_width": 20,
        "freeze": "C2"
    },
    {
        "file": "INDICATOR (PROM) 2025-07-14 v0",
        "sheet": "INDICATOR",
        "max_col_width": 20,
        "freeze": "B2"
    },
    {
        "file": "REPORT (PROM-KMKKSB) 2025-07-23 v1",
        "sheet": "REPORT",
        "max_col_width": 25,
        "freeze": "D2"
    },
    {
        "file": "REWARD (PROM) 2025-07-23 v0",
        "sheet": "REWARD",
        "max_col_width": 140,
        "freeze": "B2"
    },
    {
        "file": "REWARD-LINK (PROM) 2025-07-14 v0",
        "sheet": "REWARD-LINK",
        "max_col_width": 30,
        "freeze": "A2"
    },
    {
        "file": "SVD_KB_DM_GAMIFICATION_ORG_UNIT_V20 2025_07_11 v1",
        "sheet": "ORG_UNIT_V20",
        "max_col_width": 60,
        "freeze": "A2"
    },
    {
        "file": "TOURNAMENT-SCHEDULE (PROM) 2025-07-23 v1",
        "sheet": "TOURNAMENT-SCHEDULE",
        "max_col_width": 120,
        "freeze": "B2"
    },
    {
        "file": "PROM_USER_ROLE 2025-07-21 v0",
        "sheet": "USER_ROLE",
        "max_col_width": 60,
        "freeze": "D2"
    },
    {
        "file": "PROM_USER_ROLE SB 2025-07-21 v0",
        "sheet": "USER_ROLE SB",
        "max_col_width": 60,
        "freeze": "D2"
    },
    {
        "file": "employee_PROM_final_5000",
        "sheet": "EMPLOYEE",
        "max_col_width": 70,
        "freeze": "F2"
    }
]

SUMMARY_SHEET = {
    "sheet": "SUMMARY",
    "max_col_width": 70,
    "freeze": "F2"
}

# Логирование: уровень, шаблоны, имена
LOG_LEVEL = "DEBUG"  # "INFO" или "DEBUG"
LOG_BASE_NAME = "LOGS2"
LOG_MESSAGES = {
    "start":                "=== Старт работы программы: {time} ===",
    "reading_file":         "Загрузка файла: {file_path}",
    "read_ok":              "Файл успешно загружен: {file_path}, строк: {rows}, колонок: {cols}",
    "read_fail":            "Ошибка загрузки файла: {file_path}. {error}",
    "sheet_written":        "Лист Excel сформирован: {sheet} (строк: {rows}, колонок: {cols})",
    "finish":               "=== Завершение работы. Обработано файлов: {files}, строк всего: {rows_total}. Время выполнения: {time_elapsed} ===",
    "summary":              "Summary: {summary}",
    "func_start":           "[START] {func} {params}",
    "func_end":             "[END] {func} {params} (время: {time:.3f}s)",
    "func_error":           "[ERROR] {func} {params} — {error}",
    "json_flatten_start":   "Разворачивание колонки {column} (строк: {rows})",
    "json_flatten_end":     "Развёрнуто {n_cols} колонок из {n_keys} ключей, ошибок JSON: {n_errors}, строк: {rows}, время: {time:.3f}s",
    "json_flatten_error":   "Ошибка разбора JSON (строка {row}): {error}",
    "debug_columns":        "[DEBUG] {sheet}: колонки после разворачивания: {columns}",
    "debug_head":           "[DEBUG] {sheet}: первые строки после разворачивания:\n{head}",
    "field_joined":         "Колонка {column} присоединена из {src_sheet} по ключу {dst_key} -> {src_key}",
    "field_missing":        "Колонка {column} не добавлена: нет листа {src_sheet} или ключей {src_key}",
    "fields_summary":       "Итоговая структура: {rows} строк, {cols} колонок",
    "duplicates_start":     "[START] Проверка дублей: {sheet}, ключ: {keys}",
    "duplicates_found":     "[INFO] Дублей найдено: {count} на листе {sheet} по ключу {keys}",
    "duplicates_error":     "[ERROR] Ошибка при поиске дублей: {sheet}, ключ: {keys}: {error}",
    "duplicates_end":       "[END] Проверка дублей: {sheet}, время: {time:.3f}s",
    "color_scheme_applied": "[INFO] Цветовая схема применена: лист {sheet}, колонка {col}, стиль {scope}, цвет {color}"
}

MERGE_FIELDS = [
    # REPORT: добавляем CONTEST_TYPE из CONTEST-DATA
    {
        "sheet_src": "CONTEST-DATA",
        "sheet_dst": "REPORT",
        "src_key": ["CONTEST_CODE"],
        "dst_key": ["CONTEST_CODE"],
        "column": ["CONTEST_TYPE"],
        "mode": "value"
    },
    # REPORT: добавляем даты из TOURNAMENT-SCHEDULE
    {
        "sheet_src": "TOURNAMENT-SCHEDULE",
        "sheet_dst": "REPORT",
        "src_key": ["TOURNAMENT_CODE"],
        "dst_key": ["TOURNAMENT_CODE"],
        "column": ["END_DT", "RESULT_DT"],
        "mode": "value"
    },
    # REWARD: добавляем CONTEST_CODE из REWARD-LINK по REWARD_CODE
    {
        "sheet_src": "REWARD-LINK",
        "sheet_dst": "REWARD",
        "src_key": ["REWARD_CODE"],
        "dst_key": ["REWARD_CODE"],
        "column": ["CONTEST_CODE"],
        "mode": "value"
    },
    # SUMMARY: из CONTEST-DATA по CONTEST_CODE — основные поля
    {
        "sheet_src": "CONTEST-DATA",
        "sheet_dst": "SUMMARY",
        "src_key": ["CONTEST_CODE"],
        "dst_key": ["CONTEST_CODE"],
        "column": [
            "FULL_NAME",
            "CONTEST_FEATURE => momentRewarding",
            "FACTOR_MATCH",
            "PLAN_MOD_VALUE",
            "BUSINESS_BLOCK",
            "CONTEST_FEATURE => tournamentStartMailing",
            "CONTEST_FEATURE => tournamentEndMailing",
            "CONTEST_FEATURE => tournamentRewardingMailing",
            "CONTEST_FEATURE => tournamentLikeMailing"
        ],
        "mode": "value"
    },
    # SUMMARY: из GROUP по составному ключу
    {
        "sheet_src": "GROUP",
        "sheet_dst": "SUMMARY",
        "src_key": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"],
        "dst_key": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"],
        "column": [
            "GET_CALC_CRITERION",
            "ADD_CALC_CRITERION",
            "ADD_CALC_CRITERION_2"
        ],
        "mode": "value"
    },
    # SUMMARY: из INDICATOR по CONTEST_CODE
    {
        "sheet_src": "INDICATOR",
        "sheet_dst": "SUMMARY",
        "src_key": ["CONTEST_CODE"],
        "dst_key": ["CONTEST_CODE"],
        "column": [
            "INDICATOR_MARK_TYPE",
            "INDICATOR_MATCH",
            "INDICATOR_VALUE"
        ],
        "mode": "value"
    },
    # SUMMARY: из TOURNAMENT-SCHEDULE по TOURNAMENT_CODE
    {
        "sheet_src": "TOURNAMENT-SCHEDULE",
        "sheet_dst": "SUMMARY",
        "src_key": ["TOURNAMENT_CODE"],
        "dst_key": ["TOURNAMENT_CODE"],
        "column": [
            "START_DT",
            "END_DT",
            "RESULT_DT",
            "TOURNAMENT_STATUS",
            "TARGET_TYPE"
        ],
        "mode": "value"
    },
    # SUMMARY: CONTEST_DATE из REPORT по TOURNAMENT_CODE
    {
        "sheet_src": "REPORT",
        "sheet_dst": "SUMMARY",
        "src_key": ["TOURNAMENT_CODE"],
        "dst_key": ["TOURNAMENT_CODE"],
        "column": [
            "CONTEST_DATE"
        ],
        "mode": "value"
    },
    # SUMMARY: сколько в REPORT строк по паре TOURNAMENT_CODE + CONTEST_CODE
    {
        "sheet_src": "REPORT",
        "sheet_dst": "SUMMARY",
        "src_key": ["TOURNAMENT_CODE", "CONTEST_CODE"],
        "dst_key": ["TOURNAMENT_CODE", "CONTEST_CODE"],
        "column": [
            "CONTEST_DATE"
        ],
        "mode": "count"
    },
    # SUMMARY: все нужные поля из REWARD по составному ключу
    {
        "sheet_src": "REWARD",
        "sheet_dst": "SUMMARY",
        "src_key": ["REWARD_LINK => CONTEST_CODE", "REWARD_CODE"],  # ПРОБЕЛ после =>
        "dst_key": ["CONTEST_CODE", "REWARD_CODE"],
        "column": [
            "ADD_DATA => rewardAgainGlobal",
            "ADD_DATA => rewardAgainTournament",
            "ADD_DATA => outstanding",
            "ADD_DATA => teamNews",
            "ADD_DATA => singleNews"
        ],
        "mode": "value"
    }
]



SUMMARY_KEY_DEFS = [
    {"sheet": "CONTEST-DATA",    "cols": ["CONTEST_CODE"]},
    {"sheet": "TOURNAMENT-SCHEDULE", "cols": ["TOURNAMENT_CODE", "CONTEST_CODE"]},
    {"sheet": "REWARD-LINK",     "cols": ["REWARD_CODE", "CONTEST_CODE"]},
    {"sheet": "GROUP",           "cols": ["GROUP_CODE", "CONTEST_CODE", "GROUP_VALUE"]},
    {"sheet": "REWARD",          "cols": ["REWARD_CODE"]},
]

# Построить упорядоченный список всех уникальных ключей
SUMMARY_KEY_COLUMNS = []
for entry in SUMMARY_KEY_DEFS:
    for col in entry["cols"]:
        if col not in SUMMARY_KEY_COLUMNS:
            SUMMARY_KEY_COLUMNS.append(col)


COLOR_SCHEME = [
    # --- ИСХОДНЫЕ ДАННЫЕ (загружаются из CSV) — светло-синий ---
    {
        "group": "Исходные данные",
        "header_bg": "D4FB79",  # светло-синий
        "header_fg": "000000",  # чёрный
        "column_bg": None,      # пока не используем, можно добавить позже
        "column_fg": None,
        "style_scope": "header",  # красим только заголовок
        "sheets": ["CONTEST-DATA", "GROUP", "INDICATOR", "REPORT", "REWARD", "REWARD-LINK", "TOURNAMENT-SCHEDULE", "ORG_UNIT_V20", "USER_ROLE", "USER_ROLE SB", "EMPLOYEE"],
        "columns": [],  # все колонки (если не указано — все)
        # #CCE5FF — светло-синий (header)
    },

    # --- ДАННЫЕ, развёрнутые из JSON — сама колонка (несильно светлый зелёный), развёрнутые — светло-зелёный ---
    {
        "group": "JSON source columns",
        "header_bg": "FFD479",  # средний зелёный (оригинал JSON)
        "header_fg": "000000",
        "column_bg": None,  # #D8FCD8 — светло-зелёный, если потребуется
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["CONTEST-DATA", "REWARD"],
        "columns": ["CONTEST_FEATURE", "REWARD_ADD_DATA", "ADD_DATA => getCondition", "ADD_DATA => getCondition => employeeRating"],
        # #85E085 — зелёный (header)
    },
    {
        "group": "JSON expanded",
        "header_bg": "D8FCD8",  # светло-зелёный (развёрнутые)
        "header_fg": "000000",
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["CONTEST-DATA", "REWARD"],
        "columns": [
            "CONTEST_FEATURE => momentRewarding", "CONTEST_FEATURE => tournamentStartMailing", "CONTEST_FEATURE => tournamentEndMailing",
            "CONTEST_FEATURE => tournamentRewardingMailing", "CONTEST_FEATURE => tournamentLikeMailing", "CONTEST_FEATURE => capacity",
            "CONTEST_FEATURE => tournamentListMailing", "CONTEST_FEATURE => vid", "CONTEST_FEATURE => tbVisible", "CONTEST_FEATURE => tbHidden",
            "CONTEST_FEATURE => persomanNumberVisible",	"CONTEST_FEATURE => typeRewarding",	"CONTEST_FEATURE => masking",
            "CONTEST_FEATURE => minNumber",	"CONTEST_FEATURE => businessBlock",	"CONTEST_FEATURE => accuracy", "CONTEST_FEATURE => gosbHidden",
            "CONTEST_FEATURE => preferences", "CONTEST_FEATURE => persomanNumberHidden",	"CONTEST_FEATURE => gosbVisible",	"CONTEST_FEATURE => feature",
            "ADD_DATA => getCondition => nonRewards", "ADD_DATA => refreshOldNews", "ADD_DATA => getCondition => rewards",
            "ADD_DATA => fileName",	"ADD_DATA => rewardRule",	"ADD_DATA => bookingRequired", "ADD_DATA => outstanding",
            "ADD_DATA => teamNews", "ADD_DATA => singleNews", "ADD_DATA => rewardAgainGlobal", "ADD_DATA => rewardAgainTournament",
            "ADD_DATA => isGrouping", "ADD_DATA => tagEndDT",	"ADD_DATA => itemAmount",	"ADD_DATA => isGroupingTitle",
            "ADD_DATA => itemLimitCount",	"ADD_DATA => recommendationLevel",	"ADD_DATA => isGroupingName",	"ADD_DATA => ignoreConditions",
            "ADD_DATA => masterBadge",	"ADD_DATA => priority",	"ADD_DATA => nftFlg",	"ADD_DATA => itemMinShow",	"ADD_DATA => itemFeature",
            "ADD_DATA => itemLimitPeriod",	"ADD_DATA => businessBlock",	"ADD_DATA => parentRewardCode",	"ADD_DATA => deliveryRequired",
            "ADD_DATA => feature", "ADD_DATA => itemGroupAmount", "ADD_DATA => seasonItem", "ADD_DATA => isGroupingTultip", "ADD_DATA => tagColor",
            "ADD_DATA => commingSoon", "ADD_DATA => tournamentTeam",	"ADD_DATA => hidden",
            "ADD_DATA => getCondition => employeeRating => minRatingTB",	"ADD_DATA => getCondition => employeeRating => minRatingGOSB",
            "ADD_DATA => getCondition => employeeRating => minRatingBANK",	"ADD_DATA => getCondition => employeeRating => seasonCode",
            "ADD_DATA => getCondition => employeeRating => minCrystalEarnedTotal"
        ],
        # #D8FCD8 — светло-зелёный (header)
    },

    # --- Дополнительные, добавляемые при обработке — светло-розовый ---
    {
        "group": "Process added fields",
        "header_bg": "FFD9E6",  # светло-розовый
        "header_fg": "000000",
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["REWARD", "REPORT"],  # например, поле "REWARD_LINK =>CONTEST_CODE"
        "columns": ["REWARD_LINK =>CONTEST_CODE", "CONTEST-DATA=>CONTEST_TYPE", "TOURNAMENT-SCHEDULE=>END_DT", "TOURNAMENT-SCHEDULE=>RESULT_DT"],
        # #FFD9E6 — светло-розовый (header)
    },

    # --- SUMMARY (ключевые поля) — светло-синий ---
    {
        "group": "SUMMARY KEYS",
        "header_bg": "CCE5FF",  # светло-синий
        "header_fg": "000000",
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": SUMMARY_KEY_COLUMNS,
        # #CCE5FF — светло-синий (header)
    },

    # --- SUMMARY: добавляемые поля с каждого листа (оттенки для каждого листа) ---
    {
        "group": "SUMMARY FIELDS: CONTEST-DATA",
        "header_bg": "B6E0FE",  # голубой (отдельно от исходного)
        "header_fg": "000000",
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": [
            "CONTEST-DATA=>FULL_NAME",
            "CONTEST-DATA=>CONTEST_FEATURE => momentRewarding",
            "CONTEST-DATA=>FACTOR_MATCH",
            "CONTEST-DATA=>PLAN_MOD_VALUE",
            "CONTEST-DATA=>BUSINESS_BLOCK",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentStartMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentEndMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentRewardingMailing",
            "CONTEST-DATA=>CONTEST_FEATURE => tournamentLikeMailing",
        ],
        # #B6E0FE — голубой (header)
    },
    {
        "group": "SUMMARY FIELDS: GROUP",
        "header_bg": "DAF7A6",  # светло-зеленоватый
        "header_fg": "000000",
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": ["GROUP=>GET_CALC_CRITERION", "GROUP=>ADD_CALC_CRITERION", "GROUP=>ADD_CALC_CRITERION_2"],
        # #DAF7A6 — светло-зеленый (header)
    },
    {
        "group": "SUMMARY FIELDS: INDICATOR",
        "header_bg": "FBE7B0",  # светло-жёлтый
        "header_fg": "000000",
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": ["INDICATOR=>INDICATOR_MARK_TYPE", "INDICATOR=>INDICATOR_MATCH", "INDICATOR=>INDICATOR_VALUE"],
        # #FBE7B0 — светло-жёлтый (header)
    },
    {
        "group": "SUMMARY FIELDS: TOURNAMENT-SCHEDULE",
        "header_bg": "C2F0FC",  # голубой
        "header_fg": "000000",
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": ["TOURNAMENT-SCHEDULE=>START_DT", "TOURNAMENT-SCHEDULE=>END_DT", "TOURNAMENT-SCHEDULE=>RESULT_DT", "TOURNAMENT-SCHEDULE=>TOURNAMENT_STATUS"],
        # #C2F0FC — голубой (header)
    },
    {
        "group": "SUMMARY FIELDS: REPORT",
        "header_bg": "D9F2E6",  # светло-зелёный
        "header_fg": "000000",
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": ["REPORT=>CONTEST_DATE", "REPORT=>COUNT"],
        # #D9F2E6 — светло-зелёный (header)
    },
    {
        "group": "SUMMARY FIELDS: REWARD",
        "header_bg": "FFF2CC",  # светло-оранжевый
        "header_fg": "000000",
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": [
            "REWARD=>ADD_DATA => rewardAgainGlobal",
            "REWARD=>ADD_DATA => rewardAgainTournament",
            "REWARD=>ADD_DATA => outstanding",
            "REWARD=>ADD_DATA => teamNews",
            "REWARD=>ADD_DATA => singleNews",
        ],
        # #FFF2CC — светло-оранжевый (header)
    },

    # --- Дубли в SUMMARY (если появятся) — светло-розовый ---
    {
        "group": "SUMMARY DUPLICATES",
        "header_bg": "FFD9E6",  # светло-розовый
        "header_fg": "000000",
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": ["SUMMARY"],
        "columns": ["ДУБЛЬ: CONTEST_CODE_TOURNAMENT_CODE_REWARD_CODE_GROUP_CODE"],
        # #FFD9E6 — светло-розовый (header)
    },
]

# Добавление секции для дублей по CHECK_DUPLICATES
CHECK_DUPLICATES = [
    {"sheet": "CONTEST-DATA", "key": ["CONTEST_CODE"]},
    {"sheet": "GROUP",        "key": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"]},
    {"sheet": "INDICATOR",    "key": ["CONTEST_CODE", "INDICATOR_ADD_CALC_TYPE"]},
    {"sheet": "REPORT",       "key": ["MANAGER_PERSON_NUMBER", "TOURNAMENT_CODE", "CONTEST_CODE"]},
    {"sheet": "REWARD",       "key": ["REWARD_CODE"]},
    {"sheet": "REWARD-LINK",  "key": ["CONTEST_CODE", "REWARD_CODE"]},
    {"sheet": "TOURNAMENT-SCHEDULE", "key": ["TOURNAMENT_CODE", "CONTEST_CODE"]},
    {"sheet": "ORG_UNIT_V20", "key": ["ORG_UNIT_CODE"]},
    {"sheet": "USER_ROLE", "key": ["RULE_NUM"]},
    {"sheet": "USER_ROLE SB", "key": ["RULE_NUM"]},
    {"sheet": "EMPLOYEE", "key": ["PERSON_NUMBER"]}
]

for check in CHECK_DUPLICATES:
    sheet = check["sheet"]
    keys = check["key"]
    col_name = "ДУБЛЬ: " + "_".join(keys)
    COLOR_SCHEME.append({
        "group": "DUPLICATES",
        "header_bg": "FFD9E6",  # светло-розовый (header)
        "header_fg": "000000",
        "column_bg": None,
        "column_fg": None,
        "style_scope": "header",
        "sheets": [sheet],
        "columns": [col_name],
        # #FFD9E6 — светло-розовый (header)
    })

# Какие поля разворачивать, в каком листе, с каким префиксом (строго регламентировано)
JSON_COLUMNS = {
    "CONTEST-DATA": [
        {"column": "CONTEST_FEATURE", "prefix": "CONTEST_FEATURE"},
    ],
    "REWARD": [
        {"column": "REWARD_ADD_DATA", "prefix": "REWARD_ADD_DATA"},
    ],
    # Если появятся другие листы — добавить по аналогии
}


# Выходной файл Excel
def get_output_filename():
    return f'SPOD_ALL_IN_ONE_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.xlsx'

# Лог-файл с учетом уровня
def get_log_filename():
    # Имя лог-файла по дате, например: LOGS_2025-07-23.log
    suffix = f"_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.log"
    return os.path.join(DIR_LOGS, LOG_BASE_NAME + suffix)


# === Логирование ===
def setup_logger():
    log_file = get_log_filename()
    # Если логгер уже инициализирован, не добавляем обработчики ещё раз
    if logging.getLogger().hasHandlers():
        return log_file
    logging.basicConfig(
        level=logging.DEBUG if LOG_LEVEL == "DEBUG" else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8", mode="a"),  # append mode
            logging.StreamHandler(sys.stdout)
        ]
    )
    return log_file


# === Чтение CSV ===
def read_csv_file(file_path):
    func_start = time()
    params = f"({file_path})"
    logging.info(LOG_MESSAGES["func_start"].format(func="read_csv_file", params=params))
    try:
        df = pd.read_csv(file_path, sep=";", header=0, dtype=str, quoting=3, encoding="utf-8", keep_default_na=False)
        # Добавь лог первых строк для всех JSON-полей
        for col in df.columns:
            if "FEATURE" in col or "ADD_DATA" in col:
                logging.debug(f"[CSV][{file_path}] Пример значений {col}: {df[col].dropna().head(2).to_list()}")
        logging.info(LOG_MESSAGES["read_ok"].format(file_path=file_path, rows=len(df), cols=len(df.columns)))
        func_time = time() - func_start
        logging.info(LOG_MESSAGES["func_end"].format(func="read_csv_file", params=params, time=func_time))
        return df
    except Exception as e:
        func_time = time() - func_start
        logging.error(LOG_MESSAGES["read_fail"].format(file_path=file_path, error=e))
        logging.error(LOG_MESSAGES["func_error"].format(func="read_csv_file", params=params, error=e))
        logging.info(LOG_MESSAGES["func_end"].format(func="read_csv_file", params=params, time=func_time))
        return None

# === Запись в Excel с форматированием ===
def write_to_excel(sheets_data, output_path):
    func_start = time()
    params = f"({output_path})"
    logging.info(LOG_MESSAGES["func_start"].format(func="write_to_excel", params=params))
    try:
        # SUMMARY первый, остальные — по алфавиту
        ordered_sheets = ["SUMMARY"] + [s for s in sheets_data if s != "SUMMARY"]
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for sheet_name in ordered_sheets:
                df, params_sheet = sheets_data[sheet_name]
                df.to_excel(writer, index=False, sheet_name=sheet_name)
                ws = writer.sheets[sheet_name]
                _format_sheet(ws, df, params_sheet)
                logging.info(LOG_MESSAGES["sheet_written"].format(sheet=sheet_name, rows=len(df), cols=len(df.columns)))
            # Делать SUMMARY активным
            writer.book.active = writer.book.sheetnames.index("SUMMARY")
            writer.book.save(output_path)
        func_time = time() - func_start
        logging.info(LOG_MESSAGES["func_end"].format(func="write_to_excel", params=params, time=func_time))
    except Exception as ex:
        func_time = time() - func_start
        logging.error(LOG_MESSAGES["func_error"].format(func="write_to_excel", params=params, error=ex))
        logging.info(LOG_MESSAGES["func_end"].format(func="write_to_excel", params=params, time=func_time))

# === Форматирование листа ===
def _format_sheet(ws, df, params):
    func_start = time()
    params_str = f"({ws.title})"
    logging.debug(LOG_MESSAGES["func_start"].format(func="_format_sheet", params=params_str))
    header_font = Font(bold=True)
    align_center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    align_data = Alignment(horizontal="left", vertical="center", wrap_text=True)
    max_col_width = params.get("max_col_width", 30)

    for col_num, cell in enumerate(ws[1], 1):
        cell.font = header_font
        cell.alignment = align_center
        col_letter = get_column_letter(col_num)
        max_width = min(
            max([len(str(cell.value)) for cell in ws[get_column_letter(col_num)] if cell.value] + [8]),
            max_col_width
        )
        ws.column_dimensions[col_letter].width = max_width
        apply_color_scheme(ws, ws.title)

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            cell.alignment = align_data

    # Закрепление строк и столбцов
    ws.freeze_panes = params.get("freeze", "A2")
    ws.auto_filter.ref = ws.dimensions

    func_time = time() - func_start
    logging.debug(LOG_MESSAGES["func_end"].format(func="_format_sheet", params=params_str, time=func_time))


    # Данные: перенос строк, выравнивание по левому краю, по вертикали по центру
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            cell.alignment = align_data

def safe_json_loads(s: str):
    """
    Преобразует строку в объект JSON. Возвращает dict/list или None, если не удается разобрать.
    Более толерантен к разным типам кавычек и пустым строкам.
    """
    if not isinstance(s, str):
        return s
    s = s.strip()
    if not s or s in {'-', 'None', 'null'}:
        return None
    try:
        return json.loads(s)
    except Exception as ex:
        # Пробуем заменить одинарные кавычки на двойные
        try:
            fixed = s.replace("'", '"')
            return json.loads(fixed)
        except Exception:
            logging.debug(f"[safe_json_loads] Ошибка: {ex} | Исходная строка: {repr(s)}")
            return None

def flatten_json_column_recursive(df, column, prefix=None, sheet=None, sep="; "):
    import time as tmod
    func_start = tmod.time()
    n_rows = len(df)
    n_errors = 0
    prefix = prefix if prefix is not None else column
    logging.info(LOG_MESSAGES["func_start"].format(func="flatten_json_column_recursive", params=f"(лист: {sheet}, колонка: {column})"))

    def extract(obj, current_prefix):
        fields = {}
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_prefix = f"{current_prefix} => {k}"
                fields.update(extract(v, new_prefix))
        elif isinstance(obj, list):
            if all(isinstance(x, (str, int, float, bool, type(None))) for x in obj):
                fields[current_prefix] = sep.join(str(x) for x in obj)
            else:
                for idx, x in enumerate(obj):
                    item_prefix = f"{current_prefix} => [{idx}]"
                    fields.update(extract(x, item_prefix))
        else:
            # Сюда могут попасть nan, пустые строки, и т.п.
            if obj not in (None, "", float('nan')):
                fields[current_prefix] = obj
        return fields

    new_cols = {}
    for idx, val in enumerate(df[column]):
        try:
            parsed = None
            # Строка — парсим JSON; dict/list — оставляем; иначе пропускаем
            if isinstance(val, str):
                val = val.strip()
                if val in {"", "-", "None", "null"}:
                    parsed = {}
                else:
                    parsed = safe_json_loads(val)
            elif isinstance(val, (dict, list)):
                parsed = val
            else:
                # Необрабатываемые типы (например float('nan'))
                parsed = {}
            flat = extract(parsed, prefix)
        except Exception as ex:
            logging.debug(LOG_MESSAGES["json_flatten_error"].format(row=idx, error=ex))
            n_errors += 1
            flat = {}
        for k, v in flat.items():
            if k not in new_cols:
                new_cols[k] = [None] * n_rows
            new_cols[k][idx] = v

    # Оставлять только реально созданные колонки (не пустые)
    for col_name, values in new_cols.items():
        if any(x is not None for x in values):
            df[col_name] = values

    logging.info(f"[JSON_FLATTEN] {column} → новых колонок: {len(new_cols)}")
    logging.info(f"Все новые колонки: {list(new_cols.keys())}")
    return df


def flatten_nested_json_column(df, source_col, prefix, subfield, sheet=None, sep="; "):
    """
    Разворачивает под-поле subfield внутри JSON-строк в колонке source_col.
    subfield — имя ключа внутри JSON (например, 'nonRewards')
    prefix — что добавить к колонкам (например, "ADD_DATA => getCondition => nonRewards => ")
    """
    func_start = time()
    params = f"(колонка: {source_col}, поле: {subfield}, префикс: {prefix})"
    n_rows = len(df)
    n_errors = 0
    logging.info(LOG_MESSAGES["func_start"].format(func="flatten_nested_json_column", params=params))

    all_keys = set()
    json_objs = []
    # 1. Получаем содержимое subfield для каждой строки (если возможно)
    for idx, val in enumerate(df[source_col]):
        try:
            # Если нет данных — пусто
            if not val or str(val).strip() in {"-", ""}:
                obj = {}
            else:
                # Преобразуем строку в JSON
                main_obj = safe_json_loads(val) if isinstance(val, str) else val
                sub_obj = main_obj.get(subfield, {})
                # если строка — тоже json
                if isinstance(sub_obj, str):
                    sub_obj = safe_json_loads(sub_obj)
                obj = sub_obj if isinstance(sub_obj, dict) else {}
        except Exception as ex:
            logging.debug(LOG_MESSAGES["json_flatten_error"].format(row=idx, error=ex))
            obj = {}
            n_errors += 1
        json_objs.append(obj)
        all_keys.update(obj.keys())

    # 2. Формируем новые колонки
    for key in all_keys:
        colname = f"{prefix}{key}"
        new_col = []
        for obj in json_objs:
            val = obj.get(key, "")
            if isinstance(val, list):
                val = sep.join([str(x) for x in val])
            elif isinstance(val, dict):
                val = json.dumps(val, ensure_ascii=False)
            new_col.append(val)
        df[colname] = new_col

    n_cols = len([c for c in df.columns if c.startswith(prefix)])
    func_time = time() - func_start

    logging.info(LOG_MESSAGES["json_flatten_end"].format(
        n_cols=n_cols, n_keys=len(all_keys), n_errors=n_errors, rows=n_rows, time=func_time
    ))
    logging.info(LOG_MESSAGES["func_end"].format(func="flatten_nested_json_column", params=params, time=func_time))
    if LOG_LEVEL == "DEBUG":
        if sheet:
            logging.debug(LOG_MESSAGES["debug_columns"].format(sheet=sheet, columns=', '.join(df.columns.tolist())))
            logging.debug(LOG_MESSAGES["debug_head"].format(sheet=sheet, head=df.head(3).to_string()))
    return df

def flatten_contest_feature_column(df, column='CONTEST_FEATURE', prefix="CONTEST_FEATURE => "):
    func_start = time()
    params = f"(колонка: {column})"
    n_rows = len(df)
    n_errors = 0

    logging.info(LOG_MESSAGES["func_start"].format(func="flatten_contest_feature_column", params=params))
    logging.info(LOG_MESSAGES["json_flatten_start"].format(column=column, rows=n_rows))

    all_keys = set()
    json_objs = []
    for idx, val in enumerate(df[column]):
        try:
            # Нормализация кавычек:
            norm_val = val.replace('"""', '"')
            obj = safe_json_loads(norm_val)
        except Exception as ex:
            logging.debug(LOG_MESSAGES["json_flatten_error"].format(row=idx, error=ex))
            obj = {}
            n_errors += 1
        json_objs.append(obj)
        all_keys.update(obj.keys())

    for key in all_keys:
        colname = f"{prefix}{key}"
        new_col = []
        for obj in json_objs:
            val = obj.get(key, "")
            if isinstance(val, list):
                val = ";".join([str(x) for x in val])
            new_col.append(val)
        df[colname] = new_col

    n_cols = len([c for c in df.columns if c.startswith(prefix)])
    func_time = time() - func_start

    logging.info(LOG_MESSAGES["json_flatten_end"].format(
        n_cols=n_cols, n_keys=len(all_keys), n_errors=n_errors, rows=n_rows, time=func_time
    ))
    logging.info(LOG_MESSAGES["func_end"].format(func="flatten_contest_feature_column", params=params, time=func_time))
    return df

def apply_color_scheme(ws, sheet_name):
    """
    Окрашивает заголовки и/или всю колонку на листе Excel по схеме COLOR_SCHEME.
    Все действия логируются через LOG_MESSAGES.
    """
    for color_conf in COLOR_SCHEME:
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
                    cell.fill = PatternFill(start_color=color_conf["header_bg"], end_color=color_conf["header_bg"], fill_type="solid")
                if color_conf.get("header_fg"):
                    cell.font = Font(color=color_conf["header_fg"])
                # Логирование
                logging.info(LOG_MESSAGES["color_scheme_applied"].format(
                    sheet=sheet_name,
                    col=colname,
                    scope="header",
                    color=color_conf.get("header_bg", "default")
                ))
            # Окраска всей колонки (если понадобится в будущем)
            elif style_scope == "all":
                for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=col_idx, max_col=col_idx):
                    for cell in row:
                        if cell.row == 1 and color_conf.get("header_bg"):
                            cell.fill = PatternFill(start_color=color_conf["header_bg"], end_color=color_conf["header_bg"], fill_type="solid")
                            if color_conf.get("header_fg"):
                                cell.font = Font(color=color_conf["header_fg"])
                        elif color_conf.get("column_bg"):
                            cell.fill = PatternFill(start_color=color_conf["column_bg"], end_color=color_conf["column_bg"], fill_type="solid")
                            if color_conf.get("column_fg"):
                                cell.font = Font(color=color_conf["column_fg"])
                logging.debug(LOG_MESSAGES["color_scheme_applied"].format(
                    sheet=sheet_name,
                    col=colname,
                    scope="all",
                    color=color_conf.get("column_bg", "default")
                ))


def flatten_json_column(df, column, prefix, sheet=None, sep="; "):
    func_start = time()
    params = f"(колонка: {column}, префикс: {prefix})"
    n_rows = len(df)
    n_errors = 0

    logging.info(LOG_MESSAGES["func_start"].format(func="flatten_json_column", params=params))
    logging.info(LOG_MESSAGES["json_flatten_start"].format(column=column, rows=n_rows))

    all_keys = set()
    json_objs = []
    for idx, val in enumerate(df[column]):
        try:
            # Нормализация кавычек: """ -> "
            norm_val = val.replace('"""', '"')
            obj = safe_json_loads(norm_val)
        except Exception as ex:
            logging.debug(LOG_MESSAGES["json_flatten_error"].format(row=idx, error=ex))
            obj = {}
            n_errors += 1
        json_objs.append(obj)
        all_keys.update(obj.keys())

    for key in all_keys:
        colname = f"{prefix}{key}"
        new_col = []
        for obj in json_objs:
            val = obj.get(key, "")
            if isinstance(val, list):
                val = sep.join([str(x) for x in val])
            elif isinstance(val, dict):
                # Словарь сериализуем одной строкой
                val = json.dumps(val, ensure_ascii=False)
            new_col.append(val)
        df[colname] = new_col

    n_cols = len([c for c in df.columns if c.startswith(prefix)])
    func_time = time() - func_start

    logging.info(LOG_MESSAGES["json_flatten_end"].format(
        n_cols=n_cols, n_keys=len(all_keys), n_errors=n_errors, rows=n_rows, time=func_time
    ))
    logging.info(LOG_MESSAGES["func_end"].format(func="flatten_json_column", params=params, time=func_time))
    if LOG_LEVEL == "DEBUG":
        if sheet:
            logging.debug(LOG_MESSAGES["debug_columns"].format(sheet=sheet, columns=', '.join(df.columns.tolist())))
            logging.debug(LOG_MESSAGES["debug_head"].format(sheet=sheet, head=df.head(3).to_string()))
    return df

def _merge_field(summary, ref_df, dst_keys, src_keys, col_name, sheet_src):
    """
    Присоединяет к summary новое поле по ключам (берет только первое совпадение).
    """
    def make_key(row, keys):
        return tuple(row[k] for k in keys)
    ref_map = dict(zip(
        ref_df.apply(lambda r: make_key(r, src_keys), axis=1),
        ref_df[col_name]
    ))
    summary_col_name = f"{sheet_src}=>{col_name}"
    summary[summary_col_name] = summary.apply(lambda r: ref_map.get(make_key(r, dst_keys), "-"), axis=1)
    return summary

def collect_summary_keys(dfs):
    """
    Собирает все реально существующие сочетания ключей,
    включая осиротевшие коды и сочетания с GROUP_VALUE.
    """
    all_rows = []

    rewards = dfs.get("REWARD-LINK", pd.DataFrame())
    tournaments = dfs.get("TOURNAMENT-SCHEDULE", pd.DataFrame())
    groups = dfs.get("GROUP", pd.DataFrame())

    all_contest_codes = set()
    all_tournament_codes = set()
    all_reward_codes = set()
    all_group_codes = set()
    all_group_values = set()

    if not rewards.empty:
        all_contest_codes.update(rewards["CONTEST_CODE"].dropna())
        all_reward_codes.update(rewards["REWARD_CODE"].dropna())
    if not tournaments.empty:
        all_contest_codes.update(tournaments["CONTEST_CODE"].dropna())
        all_tournament_codes.update(tournaments["TOURNAMENT_CODE"].dropna())
    if not groups.empty:
        all_contest_codes.update(groups["CONTEST_CODE"].dropna())
        all_group_codes.update(groups["GROUP_CODE"].dropna())
        all_group_values.update(groups["GROUP_VALUE"].dropna())

    # 1. Для каждого CONTEST_CODE
    for code in all_contest_codes:
        tourns = tournaments[tournaments["CONTEST_CODE"] == code]["TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else []
        rewards_ = rewards[rewards["CONTEST_CODE"] == code]["REWARD_CODE"].dropna().unique() if not rewards.empty else []
        groups_df = groups[groups["CONTEST_CODE"] == code] if not groups.empty else pd.DataFrame()
        groups_ = groups_df["GROUP_CODE"].dropna().unique() if not groups_df.empty else []
        group_values_ = groups_df["GROUP_VALUE"].dropna().unique() if not groups_df.empty else []

        tourns = tourns if len(tourns) else ["-"]
        rewards_ = rewards_ if len(rewards_) else ["-"]
        groups_ = groups_ if len(groups_) else ["-"]
        group_values_ = group_values_ if len(group_values_) else ["-"]

        for t in tourns:
            for r in rewards_:
                for g in groups_:
                    for gv in group_values_:
                        all_rows.append((str(code), str(t), str(r), str(g), str(gv)))

    # 2. Для каждого TOURNAMENT_CODE (даже если нет CONTEST_CODE)
    if not tournaments.empty:
        for t_code in tournaments["TOURNAMENT_CODE"].dropna().unique():
            code = tournaments[tournaments["TOURNAMENT_CODE"] == t_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
            rewards_ = rewards[rewards["CONTEST_CODE"] == code]["REWARD_CODE"].dropna().unique() if not rewards.empty else ["-"]
            groups_df = groups[groups["CONTEST_CODE"] == code] if not groups.empty else pd.DataFrame()
            groups_ = groups_df["GROUP_CODE"].dropna().unique() if not groups_df.empty else ["-"]
            group_values_ = groups_df["GROUP_VALUE"].dropna().unique() if not groups_df.empty else ["-"]
            rewards_ = rewards_ if len(rewards_) else ["-"]
            groups_ = groups_ if len(groups_) else ["-"]
            group_values_ = group_values_ if len(group_values_) else ["-"]
            for r in rewards_:
                for g in groups_:
                    for gv in group_values_:
                        all_rows.append((str(code), str(t_code), str(r), str(g), str(gv)))

    # 3. Для каждого REWARD_CODE (даже если нет CONTEST_CODE)
    if not rewards.empty:
        for r_code in rewards["REWARD_CODE"].dropna().unique():
            code = rewards[rewards["REWARD_CODE"] == r_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
            tourns = tournaments[tournaments["CONTEST_CODE"] == code]["TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else ["-"]
            groups_df = groups[groups["CONTEST_CODE"] == code] if not groups.empty else pd.DataFrame()
            groups_ = groups_df["GROUP_CODE"].dropna().unique() if not groups_df.empty else ["-"]
            group_values_ = groups_df["GROUP_VALUE"].dropna().unique() if not groups_df.empty else ["-"]
            tourns = tourns if len(tourns) else ["-"]
            groups_ = groups_ if len(groups_) else ["-"]
            group_values_ = group_values_ if len(group_values_) else ["-"]
            for t in tourns:
                for g in groups_:
                    for gv in group_values_:
                        all_rows.append((str(code), str(t), str(r_code), str(g), str(gv)))

    # 4. Для каждого GROUP_CODE (даже если нет CONTEST_CODE)
    if not groups.empty:
        for g_code in groups["GROUP_CODE"].dropna().unique():
            code = groups[groups["GROUP_CODE"] == g_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
            tourns = tournaments[tournaments["CONTEST_CODE"] == code]["TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else ["-"]
            rewards_ = rewards[rewards["CONTEST_CODE"] == code]["REWARD_CODE"].dropna().unique() if not rewards.empty else ["-"]
            group_values_ = groups[groups["GROUP_CODE"] == g_code]["GROUP_VALUE"].dropna().unique() if not groups.empty else ["-"]
            tourns = tourns if len(tourns) else ["-"]
            rewards_ = rewards_ if len(rewards_) else ["-"]
            group_values_ = group_values_ if len(group_values_) else ["-"]
            for t in tourns:
                for r in rewards_:
                    for gv in group_values_:
                        all_rows.append((str(code), str(t), str(r), str(g_code), str(gv)))

    # Удалить дубли
    summary_keys = pd.DataFrame(all_rows, columns=SUMMARY_KEY_COLUMNS).drop_duplicates().reset_index(drop=True)
    return summary_keys

def mark_duplicates(df, key_cols, sheet_name=None):
    """
    Добавляет колонку с пометкой о дублях по key_cols.
    Если строк по ключу больше одной — пишем xN, иначе пусто.
    """
    import time as tmod
    func_start = tmod.time()
    key_str = "_".join(key_cols)
    col_name = f"ДУБЛЬ: {key_str}"
    params = {"sheet": sheet_name, "keys": key_cols}

    logging.info(LOG_MESSAGES["duplicates_start"].format(sheet=sheet_name, keys=key_cols))
    try:
        dup_counts = df.groupby(key_cols)[key_cols[0]].transform('count')
        df[col_name] = dup_counts.apply(lambda x: f"x{x}" if x > 1 else "")
        n_duplicates = (df[col_name] != "").sum()
        func_time = tmod.time() - func_start
        logging.info(LOG_MESSAGES["duplicates_found"].format(count=n_duplicates, sheet=sheet_name, keys=key_cols))
        logging.info(LOG_MESSAGES["duplicates_end"].format(sheet=sheet_name, time=func_time))
    except Exception as ex:
        func_time = tmod.time() - func_start
        logging.error(LOG_MESSAGES["duplicates_error"].format(sheet=sheet_name, keys=key_cols, error=ex))
        logging.info(LOG_MESSAGES["duplicates_end"].format(sheet=sheet_name, time=func_time))
    return df

def add_fields_to_sheet(df_base, df_ref, src_keys, dst_keys, columns, sheet_name, ref_sheet_name, mode="value"):
    """
    Добавляет к df_base поля из df_ref по ключам.
    Если mode == "value": подтягивает значения первого найденного (основной режим).
    Если mode == "count": добавляет количество строк в df_ref по каждому ключу.
    Если нужной колонки нет — создаёт её с дефолтными значениями "-".
    """
    func_start = time()
    logging.info(LOG_MESSAGES["func_start"].format(
        func="add_fields_to_sheet",
        params=f"(лист: {sheet_name}, поля: {columns}, ключ: {dst_keys}->{src_keys}, mode: {mode})"
    ))
    if isinstance(columns, str):
        columns = [columns]

    def tuple_key(row, keys):
        if isinstance(keys, (list, tuple)):
            return tuple(row[k] for k in keys)
        return row[keys]

    new_keys = df_base.apply(lambda row: tuple_key(row, dst_keys), axis=1)

    # --- Добавлено: авто-дополнение отсутствующих колонок ---
    missing = [col for col in columns if col not in df_ref.columns]
    for col in missing:
        logging.warning(f"[add_fields_to_sheet] Колонка {col} не найдена в {ref_sheet_name}, создаём пустую.")
        df_ref[col] = "-"

    if mode == "count":
        group_counts = df_ref.groupby(src_keys).size().to_dict()
        for col in columns:
            count_col_name = f"{ref_sheet_name}=>COUNT_{col}"
            df_base[count_col_name] = new_keys.map(group_counts).fillna(0).astype(int)
        logging.info(LOG_MESSAGES["func_end"].format(
            func="add_fields_to_sheet",
            params=f"(лист: {sheet_name}, mode: count, ключ: {dst_keys}->{src_keys})",
            time=time() - func_start
        ))
        return df_base

    for col in columns:
        ref_map = dict(zip(
            df_ref.apply(lambda row: tuple_key(row, src_keys), axis=1),
            df_ref[col]
        ))
        new_col_name = f"{ref_sheet_name}=>{col}"
        df_base[new_col_name] = new_keys.map(ref_map).fillna("-")
        # Специально для REWARD_LINK =>CONTEST_CODE: auto-rename, если создали с дефисом
        if new_col_name.replace("-", "_").replace(" ", "") == "REWARD_LINK=>CONTEST_CODE".replace("-", "_").replace(" ", ""):
            candidates = [c for c in df_base.columns if c.replace("-", "_").replace(" ", "") == "REWARD_LINK=>CONTEST_CODE".replace("-", "_").replace(" ", "")]
            for cand in candidates:
                if cand != "REWARD_LINK =>CONTEST_CODE":
                    df_base = df_base.rename(columns={cand: "REWARD_LINK =>CONTEST_CODE"})

    logging.info(LOG_MESSAGES["func_end"].format(
        func="add_fields_to_sheet",
        params=f"(лист: {sheet_name}, поля: {columns}, ключ: {dst_keys}->{src_keys}, mode: {mode})",
        time=time() - func_start
    ))
    return df_base


def merge_fields_across_sheets(sheets_data, merge_fields):
    """
    Универсально добавляет поля по правилам из merge_fields
    (source_df -> target_df), поддержка mode value / count.
    sheets_data: dict {sheet_name: (df, params)}
    merge_fields: список блоков с параметрами (см. выше)
    """
    for rule in merge_fields:
        sheet_src = rule["sheet_src"]
        sheet_dst = rule["sheet_dst"]
        src_keys = rule["src_key"] if isinstance(rule["src_key"], list) else [rule["src_key"]]
        dst_keys = rule["dst_key"] if isinstance(rule["dst_key"], list) else [rule["dst_key"]]
        col_names = rule["column"]
        mode = rule.get("mode", "value")
        params_str = f"(src: {sheet_src} -> dst: {sheet_dst}, поля: {col_names}, ключ: {dst_keys}<-{src_keys}, mode: {mode})"

        if sheet_src not in sheets_data or sheet_dst not in sheets_data:
            logging.warning(LOG_MESSAGES.get("field_missing", LOG_MESSAGES["func_error"]).format(
                column=col_names, src_sheet=sheet_src, src_key=src_keys
            ))
            continue

        df_src = sheets_data[sheet_src][0]
        df_dst, params_dst = sheets_data[sheet_dst]

        logging.info(LOG_MESSAGES["func_start"].format(func="merge_fields_across_sheets", params=params_str))
        df_dst = add_fields_to_sheet(df_dst, df_src, src_keys, dst_keys, col_names, sheet_dst, sheet_src, mode=mode)
        sheets_data[sheet_dst] = (df_dst, params_dst)
        logging.info(LOG_MESSAGES["func_end"].format(func="merge_fields_across_sheets", params=params_str, time=0))
    return sheets_data

def build_summary_sheet(dfs, params_summary, merge_fields):
    func_start = time()
    params_log = f"(лист: {params_summary['sheet']})"
    logging.info(LOG_MESSAGES["func_start"].format(func="build_summary_sheet", params=params_log))

    summary = collect_summary_keys(dfs)

    logging.info(LOG_MESSAGES["summary"].format(summary=f"Каркас: {len(summary)} строк (реальные комбинации ключей)"))
    logging.debug(LOG_MESSAGES["debug_head"].format(sheet=params_summary["sheet"], head=summary.head(5).to_string()))

    # Универсально добавляем все поля по merge_fields
    for field in merge_fields:
        col_names = field["column"]
        if isinstance(col_names, str):
            col_names = [col_names]
        sheet_src = field["sheet_src"]
        src_keys = field["src_key"] if isinstance(field["src_key"], list) else [field["src_key"]]
        dst_keys = field["dst_key"] if isinstance(field["dst_key"], list) else [field["dst_key"]]
        mode = field.get("mode", "value")
        params_str = f"(лист-источник: {sheet_src}, поля: {col_names}, ключ: {dst_keys}->{src_keys}, mode: {mode})"
        logging.info(LOG_MESSAGES["func_start"].format(func="add_fields_to_sheet", params=params_str))
        ref_df = dfs.get(sheet_src)
        if ref_df is None:
            logging.warning(LOG_MESSAGES.get("field_missing", LOG_MESSAGES["func_error"]).format(
                column=col_names, src_sheet=sheet_src, src_key=src_keys
            ))
            continue

        summary = add_fields_to_sheet(summary, ref_df, src_keys, dst_keys, col_names, params_summary["sheet"], sheet_src, mode=mode)
        logging.info(LOG_MESSAGES["func_end"].format(func="add_fields_to_sheet", params=params_str, time=0))

    n_rows, n_cols = summary.shape
    func_time = time() - func_start
    logging.info(LOG_MESSAGES["fields_summary"].format(rows=n_rows, cols=n_cols))
    logging.info(LOG_MESSAGES["sheet_written"].format(sheet=params_summary['sheet'], rows=n_rows, cols=n_cols))
    logging.info(LOG_MESSAGES["func_end"].format(func="build_summary_sheet", params=params_log, time=func_time))
    logging.debug(LOG_MESSAGES["debug_columns"].format(sheet=params_summary["sheet"], columns=', '.join(summary.columns.tolist())))
    logging.debug(LOG_MESSAGES["debug_head"].format(sheet=params_summary["sheet"], head=summary.head(5).to_string()))
    return summary

def enrich_reward_with_contest_code(df_reward, df_link):
    """
    Добавляет или перезаписывает колонку 'REWARD_LINK => CONTEST_CODE' в df_reward
    по соответствию REWARD_CODE -> CONTEST_CODE из df_link.
    Старые или битые варианты колонки удаляются!
    """
    # Удаляем все возможные ошибочные варианты названия колонки
    for col in list(df_reward.columns):
        norm = col.replace(" ", "").replace("-", "_").upper()
        if norm == "REWARD_LINK=>CONTEST_CODE".replace(" ", "").replace("-", "_").upper():
            df_reward = df_reward.drop(columns=[col])
    # Создаём колонку по мапу
    reward2contest = dict(zip(df_link["REWARD_CODE"], df_link["CONTEST_CODE"]))
    df_reward["REWARD_LINK => CONTEST_CODE"] = df_reward["REWARD_CODE"].map(reward2contest).fillna("-")
    return df_reward

def main():
    start_time = datetime.now()
    log_file = setup_logger()
    logging.info(LOG_MESSAGES["start"].format(time=start_time.strftime("%Y-%m-%d %H:%M:%S")))

    sheets_data = {}
    files_processed = 0
    rows_total = 0
    summary = []

    # 1. Чтение всех CSV и разворот ВСЕХ JSON‑полей на каждом листе
    for file_conf in INPUT_FILES:
        file_path = os.path.join(DIR_INPUT, file_conf["file"] + ".CSV")
        sheet_name = file_conf["sheet"]
        logging.info(LOG_MESSAGES["reading_file"].format(file_path=file_path))
        df = read_csv_file(file_path)
        if df is not None:
            # --- Разворачиваем только нужные JSON-поля по строгому списку ---
            json_columns = JSON_COLUMNS.get(sheet_name, [])
            for json_conf in json_columns:
                col = json_conf["column"]
                prefix = json_conf.get("prefix", col)
                if col in df.columns:
                    df = flatten_json_column_recursive(df, col, prefix=prefix, sheet=sheet_name)
                    logging.info(f"[JSON FLATTEN] {sheet_name}: поле '{col}' развернуто с префиксом '{prefix}'")
                else:
                    logging.warning(f"[JSON FLATTEN] {sheet_name}: поле '{col}' не найдено в колонках!")

            # Для дебага: логируем итоговый список колонок после всех разворотов
            logging.debug(f"[COLUMNS][{sheet_name}] Итоговые колонки: {df.columns.tolist()}")
            sheets_data[sheet_name] = (df, file_conf)
            files_processed += 1
            rows_total += len(df)
            summary.append(f"{sheet_name}: {len(df)} строк")
        else:
            summary.append(f"{sheet_name}: ошибка")
    if "REWARD" in sheets_data and "REWARD-LINK" in sheets_data:
        df_reward, conf_reward = sheets_data["REWARD"]
        df_link, conf_link = sheets_data["REWARD-LINK"]
        # Всегда пересоздаём колонку с нужным именем (автоочистка битых вариантов)
        df_reward = enrich_reward_with_contest_code(df_reward, df_link)
        sheets_data["REWARD"] = (df_reward, conf_reward)

    # 3. Merge fields (только после полного разворота JSON)
    merge_fields_across_sheets(
        sheets_data,
        [f for f in MERGE_FIELDS if f.get("sheet_dst") != "SUMMARY"]
    )

    # 4. Проверка на дубли
    for sheet_name, (df, conf) in sheets_data.items():
        check_cfg = next((x for x in CHECK_DUPLICATES if x["sheet"] == sheet_name), None)
        if check_cfg:
            df = mark_duplicates(df, check_cfg["key"], sheet_name=sheet_name)
            sheets_data[sheet_name] = (df, conf)

    # 5. Формирование итогового Summary (build_summary_sheet)
    dfs = {k: v[0] for k, v in sheets_data.items()}
    df_summary = build_summary_sheet(
        dfs,
        params_summary=SUMMARY_SHEET,
        merge_fields=[f for f in MERGE_FIELDS if f.get("sheet_dst") == "SUMMARY"]
    )
    sheets_data[SUMMARY_SHEET["sheet"]] = (df_summary, SUMMARY_SHEET)

    # 6. Запись в Excel
    output_excel = os.path.join(DIR_OUTPUT, get_output_filename())
    logging.info(LOG_MESSAGES["func_start"].format(func="write_to_excel", params=f"({output_excel})"))
    write_to_excel(sheets_data, output_excel)
    logging.info(LOG_MESSAGES["func_end"].format(func="write_to_excel", params=f"({output_excel})", time=0))

    time_elapsed = datetime.now() - start_time
    logging.info(LOG_MESSAGES["finish"].format(
        files=files_processed,
        rows_total=rows_total,
        time_elapsed=str(time_elapsed)
    ))
    logging.info(LOG_MESSAGES["summary"].format(summary="; ".join(summary)))
    logging.info(f"Excel file: {output_excel}")
    logging.info(f"Log file: {log_file}")


if __name__ == "__main__":
    main()
