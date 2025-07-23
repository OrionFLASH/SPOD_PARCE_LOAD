import os
import sys
import pandas as pd
import logging
from datetime import datetime
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font
import json
from time import time

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

SUMMARY_MERGE_FIELDS = [
    {
        "sheet": "CONTEST-DATA",       # имя листа-источника
        "src_key": ["CONTEST_CODE"],   # ключ в источнике (может быть список)
        "dst_key": ["CONTEST_CODE"],   # ключ в summary (может быть список)
        "column": "FULL_NAME",         # какое поле подгружаем
    },
    {
        "sheet": "CONTEST-DATA",       # имя листа-источника
        "src_key": ["CONTEST_CODE"],   # ключ в источнике (может быть список)
        "dst_key": ["CONTEST_CODE"],   # ключ в summary (может быть список)
        "column": "CONTEST_FEATURE => momentRewarding",         # какое поле подгружаем
    },
    {
        "sheet": "CONTEST-DATA",       # имя листа-источника
        "src_key": ["CONTEST_CODE"],   # ключ в источнике (может быть список)
        "dst_key": ["CONTEST_CODE"],   # ключ в summary (может быть список)
        "column": "PLAN_MOD_VALUE",         # какое поле подгружаем
    },
    {
        "sheet": "CONTEST-DATA",       # имя листа-источника
        "src_key": ["CONTEST_CODE"],   # ключ в источнике (может быть список)
        "dst_key": ["CONTEST_CODE"],   # ключ в summary (может быть список)
        "column": "BUSINESS_BLOCK",         # какое поле подгружаем
    },
    {
        "sheet": "CONTEST-DATA",       # имя листа-источника
        "src_key": ["CONTEST_CODE"],   # ключ в источнике (может быть список)
        "dst_key": ["CONTEST_CODE"],   # ключ в summary (может быть список)
        "column": "CONTEST_FEATURE => tournamentStartMailing",         # какое поле подгружаем
    },
    {
        "sheet": "CONTEST-DATA",       # имя листа-источника
        "src_key": ["CONTEST_CODE"],   # ключ в источнике (может быть список)
        "dst_key": ["CONTEST_CODE"],   # ключ в summary (может быть список)
        "column": "CONTEST_FEATURE => tournamentEndMailing",         # какое поле подгружаем
    },
    {
        "sheet": "CONTEST-DATA",       # имя листа-источника
        "src_key": ["CONTEST_CODE"],   # ключ в источнике (может быть список)
        "dst_key": ["CONTEST_CODE"],   # ключ в summary (может быть список)
        "column": "CONTEST_FEATURE => tournamentRewardingMailing",         # какое поле подгружаем
    },
    {
        "sheet": "CONTEST-DATA",       # имя листа-источника
        "src_key": ["CONTEST_CODE"],   # ключ в источнике (может быть список)
        "dst_key": ["CONTEST_CODE"],   # ключ в summary (может быть список)
        "column": "CONTEST_FEATURE => tournamentLikeMailing",         # какое поле подгружаем
    },
    {
        "sheet": "GROUP",
        "src_key": ["CONTEST_CODE", "GROUP_CODE"],   # пример составного ключа
        "dst_key": ["CONTEST_CODE", "GROUP_CODE"],
        "column": "GET_CALC_CRITERION"
    },
    {
        "sheet": "GROUP",
        "src_key": ["CONTEST_CODE", "GROUP_CODE"],   # пример составного ключа
        "dst_key": ["CONTEST_CODE", "GROUP_CODE"],
        "column": "ADD_CALC_CRITERION"
    },
    {
        "sheet": "GROUP",
        "src_key": ["CONTEST_CODE", "GROUP_CODE"],   # пример составного ключа
        "dst_key": ["CONTEST_CODE", "GROUP_CODE"],
        "column": "ADD_CALC_CRITERION_2"
    },
    {
        "sheet": "INDICATOR",
        "src_key": ["CONTEST_CODE"],   # пример составного ключа
        "dst_key": ["CONTEST_CODE"],
        "column": "INDICATOR_MARK_TYPE"
    },
    {
        "sheet": "INDICATOR",
        "src_key": ["CONTEST_CODE"],   # пример составного ключа
        "dst_key": ["CONTEST_CODE"],
        "column": "INDICATOR_MATCH"
    },
    {
        "sheet": "INDICATOR",
        "src_key": ["CONTEST_CODE"],   # пример составного ключа
        "dst_key": ["CONTEST_CODE"],
        "column": "INDICATOR_VALUE"
    },
    {
        "sheet": "TOURNAMENT-SCHEDULE",
        "src_key": ["TOURNAMENT_CODE"],
        "dst_key": ["TOURNAMENT_CODE"],
        "column": "START_DT"
    },
    {
        "sheet": "TOURNAMENT-SCHEDULE",
        "src_key": ["TOURNAMENT_CODE"],
        "dst_key": ["TOURNAMENT_CODE"],
        "column": "END_DT"
    },
    {
        "sheet": "TOURNAMENT-SCHEDULE",
        "src_key": ["TOURNAMENT_CODE"],
        "dst_key": ["TOURNAMENT_CODE"],
        "column": "RESULT_DT"
    },
    {
        "sheet": "TOURNAMENT-SCHEDULE",
        "src_key": ["TOURNAMENT_CODE"],
        "dst_key": ["TOURNAMENT_CODE"],
        "column": "TOURNAMENT_STATUS"
    },
    {
        "sheet": "REPORT",
        "src_key": ["TOURNAMENT_CODE"],
        "dst_key": ["TOURNAMENT_CODE"],
        "column": "CONTEST_DATE"
    },
    {
        "sheet": "REWARD",
        "src_key": ["REWARD_LINK =>CONTEST_CODE", "REWARD_CODE"], # пример составного ключа
        "dst_key": ["CONTEST_CODE", "REWARD_CODE"],
        "column": "ADD_DATA => rewardAgainGlobal"
    },
    {
        "sheet": "REWARD",
        "src_key": ["REWARD_LINK =>CONTEST_CODE", "REWARD_CODE"], # пример составного ключа
        "dst_key": ["CONTEST_CODE", "REWARD_CODE"],
        "column": "ADD_DATA => rewardAgainTournament"
    },
    {
        "sheet": "REWARD",
        "src_key": ["REWARD_LINK =>CONTEST_CODE", "REWARD_CODE"], # пример составного ключа
        "dst_key": ["CONTEST_CODE", "REWARD_CODE"],
        "column": "ADD_DATA => outstanding"
    },
    {
        "sheet": "REWARD",
        "src_key": ["REWARD_LINK =>CONTEST_CODE", "REWARD_CODE"], # пример составного ключа
        "dst_key": ["CONTEST_CODE", "REWARD_CODE"],
        "column": "ADD_DATA => teamNews"
    },
    {
        "sheet": "REWARD",
        "src_key": ["REWARD_LINK =>CONTEST_CODE", "REWARD_CODE"], # пример составного ключа
        "dst_key": ["CONTEST_CODE", "REWARD_CODE"],
        "column": "ADD_DATA => singleNews"
    },
 ]

CHECK_DUPLICATES = [
    {"sheet": "CONTEST-DATA", "key": ["CONTEST_CODE"]},
    {"sheet": "GROUP",        "key": ["CONTEST_CODE", "GROUP_CODE"]},
    {"sheet": "INDICATOR",    "key": ["CONTEST_CODE"]},
    {"sheet": "REPORT",       "key": ["MANAGER_PERSON_NUMBER", "TOURNAMENT_CODE"]},
    {"sheet": "REWARD",       "key": ["REWARD_CODE"]},
    # ... добавьте нужные листы и ключи
]

CHECK_DUPLICATES = [
    {
        "sheet": "CONTEST-DATA",
        "key": ["CONTEST_CODE"]
    },
    {
        "sheet": "GROUP",
        "key": ["CONTEST_CODE", "GROUP_CODE"]
    },
    {
        "sheet": "REPORT",
        "key": ["MANAGER_PERSON_NUMBER", "TOURNAMENT_CODE"]
    },
]


# Логирование: уровень, шаблоны, имена
LOG_LEVEL = "DEBUG"  # или "DEBUG"
LOG_BASE_NAME = "LOGS"
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
    "duplicates_end":       "[END] Проверка дублей: {sheet}, время: {time:.3f}s"
}

# Выходной файл Excel
def get_output_filename():
    return f'SPOD_ALL_IN_ONE_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.xlsx'

# Лог-файл с учетом уровня
def get_log_filename():
    # Имя лог-файла по дате, например: LOGS_2025-07-23.log
    suffix = f"_{datetime.now().strftime('%Y-%m-%d')}.log"
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
                main_obj = json.loads(val) if isinstance(val, str) else val
                sub_obj = main_obj.get(subfield, {})
                # если строка — тоже json
                if isinstance(sub_obj, str):
                    sub_obj = json.loads(sub_obj)
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
            obj = json.loads(norm_val)
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
            obj = json.loads(norm_val)
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
    Собирает все реально существующие сочетания ключей CONTEST_CODE, TOURNAMENT_CODE, REWARD_CODE, GROUP_CODE,
    включая осиротевшие коды (например, TOURNAMENT_CODE без CONTEST_CODE).
    """
    key_cols = ["CONTEST_CODE", "TOURNAMENT_CODE", "REWARD_CODE", "GROUP_CODE"]
    all_rows = []

    # Получаем коды из всех файлов
    rewards = dfs.get("REWARD-LINK", pd.DataFrame())
    tournaments = dfs.get("TOURNAMENT-SCHEDULE", pd.DataFrame())
    groups = dfs.get("GROUP", pd.DataFrame())

    # Все уникальные коды
    all_contest_codes = set()
    all_tournament_codes = set()
    all_reward_codes = set()
    all_group_codes = set()

    if not rewards.empty:
        all_contest_codes.update(rewards["CONTEST_CODE"].dropna())
        all_reward_codes.update(rewards["REWARD_CODE"].dropna())
    if not tournaments.empty:
        all_contest_codes.update(tournaments["CONTEST_CODE"].dropna())
        all_tournament_codes.update(tournaments["TOURNAMENT_CODE"].dropna())
    if not groups.empty:
        all_contest_codes.update(groups["CONTEST_CODE"].dropna())
        all_group_codes.update(groups["GROUP_CODE"].dropna())

    # --- 1. Для каждого CONTEST_CODE — все комбинации с его TOURNAMENT_CODE, REWARD_CODE, GROUP_CODE
    for code in all_contest_codes:
        tourns = tournaments[tournaments["CONTEST_CODE"] == code]["TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else []
        rewards_ = rewards[rewards["CONTEST_CODE"] == code]["REWARD_CODE"].dropna().unique() if not rewards.empty else []
        groups_ = groups[groups["CONTEST_CODE"] == code]["GROUP_CODE"].dropna().unique() if not groups.empty else []

        tourns = tourns if len(tourns) else ["-"]
        rewards_ = rewards_ if len(rewards_) else ["-"]
        groups_ = groups_ if len(groups_) else ["-"]

        for t in tourns:
            for r in rewards_:
                for g in groups_:
                    all_rows.append((str(code), str(t), str(r), str(g)))

    # --- 2. Для каждого TOURNAMENT_CODE (даже если нет CONTEST_CODE)
    if not tournaments.empty:
        for t_code in tournaments["TOURNAMENT_CODE"].dropna().unique():
            code = tournaments[tournaments["TOURNAMENT_CODE"] == t_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
            rewards_ = rewards[rewards["CONTEST_CODE"] == code]["REWARD_CODE"].dropna().unique() if not rewards.empty else ["-"]
            groups_ = groups[groups["CONTEST_CODE"] == code]["GROUP_CODE"].dropna().unique() if not groups.empty else ["-"]
            rewards_ = rewards_ if len(rewards_) else ["-"]
            groups_ = groups_ if len(groups_) else ["-"]
            for r in rewards_:
                for g in groups_:
                    all_rows.append((str(code), str(t_code), str(r), str(g)))

    # --- 3. Для каждого REWARD_CODE (даже если нет CONTEST_CODE)
    if not rewards.empty:
        for r_code in rewards["REWARD_CODE"].dropna().unique():
            code = rewards[rewards["REWARD_CODE"] == r_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
            tourns = tournaments[tournaments["CONTEST_CODE"] == code]["TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else ["-"]
            groups_ = groups[groups["CONTEST_CODE"] == code]["GROUP_CODE"].dropna().unique() if not groups.empty else ["-"]
            tourns = tourns if len(tourns) else ["-"]
            groups_ = groups_ if len(groups_) else ["-"]
            for t in tourns:
                for g in groups_:
                    all_rows.append((str(code), str(t), str(r_code), str(g)))

    # --- 4. Для каждого GROUP_CODE (даже если нет CONTEST_CODE)
    if not groups.empty:
        for g_code in groups["GROUP_CODE"].dropna().unique():
            code = groups[groups["GROUP_CODE"] == g_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
            tourns = tournaments[tournaments["CONTEST_CODE"] == code]["TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else ["-"]
            rewards_ = rewards[rewards["CONTEST_CODE"] == code]["REWARD_CODE"].dropna().unique() if not rewards.empty else ["-"]
            tourns = tourns if len(tourns) else ["-"]
            rewards_ = rewards_ if len(rewards_) else ["-"]
            for t in tourns:
                for r in rewards_:
                    all_rows.append((str(code), str(t), str(r), str(g_code)))

    # --- Удалить дубли
    summary_keys = pd.DataFrame(all_rows, columns=key_cols).drop_duplicates().reset_index(drop=True)
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

def build_summary_sheet(dfs, params_summary, merge_fields):
    func_start = time()
    params_log = f"(лист: {params_summary['sheet']})"
    logging.info(LOG_MESSAGES["func_start"].format(func="build_summary_sheet", params=params_log))

    # Используем только реальные сочетания!
    summary = collect_summary_keys(dfs)

    logging.info(LOG_MESSAGES["summary"].format(summary=f"Каркас: {len(summary)} строк (реальные комбинации ключей)"))
    logging.debug(LOG_MESSAGES["debug_head"].format(sheet=params_summary["sheet"], head=summary.head(5).to_string()))

    # Универсально добавляем все поля по merge_fields — берем первое найденное
    for field in merge_fields:
        col_name = field["column"]
        sheet_src = field["sheet"]
        src_keys = field["src_key"] if isinstance(field["src_key"], list) else [field["src_key"]]
        dst_keys = field["dst_key"] if isinstance(field["dst_key"], list) else [field["dst_key"]]
        params_str = f"(лист-источник: {sheet_src}, поле: {col_name}, ключ: {dst_keys}->{src_keys})"
        logging.info(LOG_MESSAGES["func_start"].format(func="add_fields_to_sheet", params=params_str))
        ref_df = dfs.get(sheet_src)
        if ref_df is None:
            logging.warning(LOG_MESSAGES.get("field_missing", LOG_MESSAGES["func_error"]).format(
                column=col_name, src_sheet=sheet_src, src_key=src_keys
            ))
            continue

        # Вытаскиваем поле (или список)
        if isinstance(col_name, list):
            for single_col in col_name:
                summary = _merge_field(summary, ref_df, dst_keys, src_keys, single_col, sheet_src)
        else:
            summary = _merge_field(summary, ref_df, dst_keys, src_keys, col_name, sheet_src)
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
    func_start = time()
    logging.info(LOG_MESSAGES["func_start"].format(func="enrich_reward_with_contest_code", params="(REWARD)"))
    # строим map по REWARD_CODE -> CONTEST_CODE
    reward2contest = dict(zip(df_link["REWARD_CODE"], df_link["CONTEST_CODE"]))
    new_col = df_reward["REWARD_CODE"].map(reward2contest).fillna("-")
    df_reward["REWARD_LINK =>CONTEST_CODE"] = new_col
    func_time = time() - func_start
    logging.info(LOG_MESSAGES["func_end"].format(func="enrich_reward_with_contest_code", params="(REWARD)", time=func_time))
    return df_reward

def add_fields_to_sheet(df_base, df_ref, src_keys, dst_keys, columns, sheet_name, ref_sheet_name):
    func_start = time()
    logging.info(LOG_MESSAGES["func_start"].format(
        func="add_fields_to_sheet",
        params=f"(лист: {sheet_name}, поле: {columns}, ключ: {dst_keys}->{src_keys})"
    ))
    if isinstance(columns, str):
        columns = [columns]

    # Подготовим ключи (tuple если список, иначе str)
    def tuple_key(row, keys):
        if isinstance(keys, (list, tuple)):
            return tuple(row[k] for k in keys)
        return row[keys]

    # Строим справочник по ключу из df_ref
    key_col = (
        df_ref.apply(lambda row: tuple_key(row, src_keys), axis=1)
    )
    ref_map = {}
    for col in columns:
        ref_map[col] = dict(zip(key_col, df_ref[col]))

    # Подгружаем новые поля
    new_keys = df_base.apply(lambda row: tuple_key(row, dst_keys), axis=1)
    for col in columns:
        new_col_name = f"{ref_sheet_name}=>{col}"
        df_base[new_col_name] = new_keys.map(ref_map[col]).fillna("-")
    func_time = time() - func_start
    logging.info(LOG_MESSAGES["func_end"].format(
        func="add_fields_to_sheet",
        params=f"(лист: {sheet_name}, поле: {columns}, ключ: {dst_keys}->{src_keys})",
        time=func_time
    ))
    return df_base


def main():
    start_time = datetime.now()
    log_file = setup_logger()
    logging.info(LOG_MESSAGES["start"].format(time=start_time.strftime("%Y-%m-%d %H:%M:%S")))

    sheets_data = {}
    files_processed = 0
    rows_total = 0
    summary = []

    # 1. Чтение и обработка всех файлов
    for file_conf in INPUT_FILES:
        file_path = os.path.join(DIR_INPUT, file_conf["file"] + ".CSV")
        sheet_name = file_conf["sheet"]
        params = f"(файл: {file_path}, лист: {sheet_name})"
        logging.info(LOG_MESSAGES["reading_file"].format(file_path=file_path))
        df = read_csv_file(file_path)
        if df is not None:
            # CONTEST_FEATURE на CONTEST-DATA
            if sheet_name == "CONTEST-DATA" and "CONTEST_FEATURE" in df.columns:
                logging.info(LOG_MESSAGES["func_start"].format(func="flatten_json_column", params=f"(лист: {sheet_name})"))
                df = flatten_json_column(df, column='CONTEST_FEATURE', prefix="CONTEST_FEATURE => ", sheet=sheet_name)
                logging.info(LOG_MESSAGES["func_end"].format(func="flatten_json_column", params=f"(лист: {sheet_name})", time=0))
            # REWARD_ADD_DATA на REWARD с вложенным getCondition + вложенные nonRewards, employeeRating
            if sheet_name == "REWARD" and "REWARD_ADD_DATA" in df.columns:
                logging.info(LOG_MESSAGES["func_start"].format(func="flatten_json_column", params=f"(лист: {sheet_name})"))
                df = flatten_json_column(df, column='REWARD_ADD_DATA', prefix="ADD_DATA => ", sheet=sheet_name)
                logging.info(LOG_MESSAGES["func_end"].format(func="flatten_json_column", params=f"(лист: {sheet_name})", time=0))

                # --- Дополнительно разворачиваем ADD_DATA => getCondition ---
                getcond_col = "ADD_DATA => getCondition"
                if getcond_col in df.columns:
                    idx = df.columns.get_loc(getcond_col)
                    all_keys = set()
                    json_objs = []
                    for i, val in enumerate(df[getcond_col]):
                        try:
                            if pd.isnull(val) or val == "" or val == "-":
                                obj = {}
                            else:
                                obj = json.loads(val)
                        except Exception as ex:
                            logging.debug(f"Ошибка разбора JSON в getCondition (строка {i}): {ex}")
                            obj = {}
                        json_objs.append(obj)
                        all_keys.update(obj.keys())
                    for key in all_keys:
                        new_col = []
                        for obj in json_objs:
                            v = obj.get(key, "")
                            if isinstance(v, list):
                                v = ";".join(map(str, v))
                            elif isinstance(v, dict):
                                v = json.dumps(v, ensure_ascii=False)
                            new_col.append(v)
                        colname = f"ADD_DATA => getCondition => {key}"
                        df[colname] = new_col
                    cols = df.columns.tolist()
                    insert_at = cols.index(getcond_col) + 1
                    new_cols = [f"ADD_DATA => getCondition => {k}" for k in all_keys if f"ADD_DATA => getCondition => {k}" in cols]
                    for col in reversed(new_cols):
                        cols.remove(col)
                        cols.insert(insert_at, col)
                    df = df[cols]
                    for subfield, subprefix in [("nonRewards", "ADD_DATA => getCondition => nonRewards => "), ("employeeRating", "ADD_DATA => getCondition => employeeRating => ")]:
                        nested_json_objs = []
                        all_nested_keys = set()
                        for val in df[getcond_col]:
                            try:
                                if pd.isnull(val) or val == "" or val == "-":
                                    obj = {}
                                else:
                                    parent = json.loads(val)
                                    sub = parent.get(subfield, {})
                                    if isinstance(sub, str):
                                        sub = json.loads(sub)
                                    obj = sub if isinstance(sub, dict) else {}
                            except Exception:
                                obj = {}
                            nested_json_objs.append(obj)
                            all_nested_keys.update(obj.keys())
                        for key in all_nested_keys:
                            new_col = []
                            for obj in nested_json_objs:
                                v = obj.get(key, "")
                                if isinstance(v, list):
                                    v = ";".join(map(str, v))
                                elif isinstance(v, dict):
                                    v = json.dumps(v, ensure_ascii=False)
                                new_col.append(v)
                            colname = f"{subprefix}{key}"
                            df[colname] = new_col

            # === Проверка на дубли ===
            check_cfg = next((x for x in CHECK_DUPLICATES if x["sheet"] == sheet_name), None)
            if check_cfg:
                df = mark_duplicates(df, check_cfg["key"], sheet_name=sheet_name)
            sheets_data[sheet_name] = (df, file_conf)
            files_processed += 1
            rows_total += len(df)
            summary.append(f"{sheet_name}: {len(df)} строк")
        else:
            summary.append(f"{sheet_name}: ошибка")

    # 2. Добавляем REWARD_LINK =>CONTEST_CODE на REWARD
    dfs = {k: v[0] for k, v in sheets_data.items()}
    if "REWARD" in dfs and "REWARD-LINK" in dfs:
        df_reward = dfs["REWARD"]
        df_link = dfs["REWARD-LINK"]
        df_reward = enrich_reward_with_contest_code(df_reward, df_link)
        sheets_data["REWARD"] = (df_reward, sheets_data["REWARD"][1])

    # 4. Построение итоговой таблицы
    df_summary = build_summary_sheet(
        dfs,
        params_summary=SUMMARY_SHEET,
        merge_fields=SUMMARY_MERGE_FIELDS
    )
    sheets_data[SUMMARY_SHEET["sheet"]] = (df_summary, SUMMARY_SHEET)

    # 5. Запись всего в Excel
    output_excel = os.path.join(DIR_OUTPUT, get_output_filename())
    logging.info(LOG_MESSAGES["func_start"].format(func="write_to_excel", params=f"({output_excel})"))
    write_to_excel(sheets_data, output_excel)
    logging.info(LOG_MESSAGES["func_end"].format(func="write_to_excel", params=f"({output_excel})", time=0))

    # 6. Финальный лог (точное время до миллисекунд)
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
