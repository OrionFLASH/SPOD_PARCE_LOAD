import os
import sys
import pandas as pd
import logging
from datetime import datetime
from pandas import ExcelWriter
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font
from openpyxl.worksheet.dimensions import ColumnDimension, DimensionHolder
from openpyxl.worksheet.table import Table, TableStyleInfo
import json

# === Глобальные константы и переменные ===
# Каталоги
DIR_INPUT = '/Users/orionflash/Desktop/MyProject/SPOD_PROM/SPOD/'
DIR_OUTPUT = '/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT/'
DIR_LOGS = '/Users/orionflash/Desktop/MyProject/SPOD_PROM/LOGS/'

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
        "file": "GROUP (PROM) 2025-06-17 v1",
        "sheet": "GROUP",
        "max_col_width": 20,
        "freeze": "C2"
    },
    {
        "file": "INDICATOR (PROM) 2025-06-17 v1",
        "sheet": "INDICATOR",
        "max_col_width": 20,
        "freeze": "B2"
    },
    {
        "file": "REPORT (PROM-KMKKSB) 2025-06-17 v1",
        "sheet": "REPORT",
        "max_col_width": 25,
        "freeze": "D2"
    },
    {
        "file": "REWARD (PROM) 2025-07-21 v0",
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
        "file": "TOURNAMENT-SCHEDULE (PROM) 2025-07-21 v0",
        "sheet": "TOURNAMENT-SCHEDULE",
        "max_col_width": 120,
        "freeze": "B2"
    },
    {
        "file": "PROM_USER_ROLE 2025-05-30 v0",
        "sheet": "USER_ROLE",
        "max_col_width": 60,
        "freeze": "D2"
    },
    {
        "file": "PROM_USER_ROLE SB 2025-05-30 v1",
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
    "freeze": "D2"
}

SUMMARY_MERGE_FIELDS = [
    {
        "sheet": "CONTEST-DATA",       # имя листа-источника
        "src_key": ["CONTEST_CODE"],   # ключ в источнике (может быть список)
        "dst_key": ["CONTEST_CODE"],   # ключ в summary (может быть список)
        "column": "FULL_NAME",         # какое поле подгружаем
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
    # ...
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
    "fields_summary":       "Итоговая структура: {rows} строк, {cols} колонок"
}

# Выходной файл Excel
def get_output_filename():
    return f'SPOD_ALL_IN_ONE_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.xlsx'

# Лог-файл с учетом уровня
def get_log_filename():
    suffix = f"_{LOG_LEVEL.upper()}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    return os.path.join(DIR_LOGS, LOG_BASE_NAME + suffix)

# === Логирование ===
def setup_logger():
    log_file = get_log_filename()
    logging.basicConfig(
        level=logging.DEBUG if LOG_LEVEL == "DEBUG" else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return log_file

# === Чтение CSV ===
def read_csv_file(file_path):
    from time import time
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
    from time import time
    func_start = time()
    params = f"({output_path})"
    logging.info(LOG_MESSAGES["func_start"].format(func="write_to_excel", params=params))
    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for sheet_name, (df, params_sheet) in sheets_data.items():
                df.to_excel(writer, index=False, sheet_name=sheet_name)
                ws = writer.sheets[sheet_name]
                _format_sheet(ws, df, params_sheet)
                logging.info(LOG_MESSAGES["sheet_written"].format(sheet=sheet_name, rows=len(df), cols=len(df.columns)))
        func_time = time() - func_start
        logging.info(LOG_MESSAGES["func_end"].format(func="write_to_excel", params=params, time=func_time))
    except Exception as ex:
        func_time = time() - func_start
        logging.error(LOG_MESSAGES["func_error"].format(func="write_to_excel", params=params, error=ex))
        logging.info(LOG_MESSAGES["func_end"].format(func="write_to_excel", params=params, time=func_time))


# === Форматирование листа ===
def _format_sheet(ws, df, params):
    from time import time
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

    ws.freeze_panes = params.get("freeze", "A2")
    ws.auto_filter.ref = ws.dimensions

    func_time = time() - func_start
    logging.debug(LOG_MESSAGES["func_end"].format(func="_format_sheet", params=params_str, time=func_time))


    # Данные: перенос строк, выравнивание по левому краю, по вертикали по центру
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            cell.alignment = align_data

    # Закрепление строк и столбцов
    ws.freeze_panes = params.get("freeze", "A2")
    ws.auto_filter.ref = ws.dimensions

def flatten_contest_feature_column(df, column='CONTEST_FEATURE', prefix="CONTEST_FEATURE => "):
    from time import time
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
    from time import time
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

def build_summary_sheet(dfs, params_summary, merge_fields):
    from time import time
    func_start = time()
    params_log = f"(лист: {params_summary['sheet']})"
    logging.info(LOG_MESSAGES["func_start"].format(func="build_summary_sheet", params=params_log))

    # 1. Автоматически определяем все dst_key-комбинации, которые нужны для summary
    all_dst_keys = []
    for field in merge_fields:
        keys = field["dst_key"] if isinstance(field["dst_key"], list) else [field["dst_key"]]
        all_dst_keys.append(tuple(keys))
    # Берём самую длинную комбинацию dst_key как "базовую структуру"
    main_keys = max(all_dst_keys, key=len)
    main_keys = list(main_keys)

    # Определяем, с какого листа брать базу — находим первый merge_field с этими dst_key
    base_sheet = None
    for field in merge_fields:
        dst_keys = field["dst_key"] if isinstance(field["dst_key"], list) else [field["dst_key"]]
        if set(dst_keys) == set(main_keys):
            base_sheet = field["sheet"]
            break
    if not base_sheet:
        # Если нет — используем первый лист из merge_fields
        base_sheet = merge_fields[0]["sheet"]
    if base_sheet not in dfs:
        logging.error(LOG_MESSAGES["func_error"].format(func="build_summary_sheet", params=params_log, error=f"Нет листа {base_sheet}"))
        raise ValueError(f"Нет листа {base_sheet} для формирования структуры.")
    base_df = dfs[base_sheet]
    # Получаем уникальные комбинации по main_keys
    summary = base_df[main_keys].drop_duplicates().copy()
    n_init = len(summary)
    logging.info(LOG_MESSAGES["summary"].format(summary=f"Каркас: {n_init} строк (уникальные по {main_keys})"))
    logging.debug(LOG_MESSAGES["debug_head"].format(sheet=params_summary["sheet"], head=summary.head(5).to_string()))

    # 2. Универсально добавляем все поля по merge_fields
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
        # Строим справочник по src_key
        def make_key(row, keys):
            return tuple(row[k] for k in keys)
        ref_map = dict(zip(
            ref_df.apply(lambda r: make_key(r, src_keys), axis=1),
            ref_df[col_name]
        ))
        # Добавляем поле по dst_key
        summary_col_name = f"{sheet_src}=>{col_name}"
        summary[summary_col_name] = summary.apply(lambda r: ref_map.get(make_key(r, dst_keys), "-"), axis=1)
        logging.info(LOG_MESSAGES.get("field_joined", LOG_MESSAGES["summary"]).format(
            column=summary_col_name, src_sheet=sheet_src, dst_key=dst_keys, src_key=src_keys
        ))
        logging.debug(LOG_MESSAGES["debug_columns"].format(sheet=params_summary["sheet"], columns=', '.join(summary.columns.tolist())))
        logging.debug(LOG_MESSAGES["debug_head"].format(sheet=params_summary["sheet"], head=summary.head(3).to_string()))
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
    from time import time
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
    from time import time
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
            # REWARD_ADD_DATA на REWARD
            if sheet_name == "REWARD" and "REWARD_ADD_DATA" in df.columns:
                logging.info(LOG_MESSAGES["func_start"].format(func="flatten_json_column", params=f"(лист: {sheet_name})"))
                df = flatten_json_column(df, column='REWARD_ADD_DATA', prefix="ADD_DATA => ", sheet=sheet_name)
                logging.info(LOG_MESSAGES["func_end"].format(func="flatten_json_column", params=f"(лист: {sheet_name})", time=0))
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
