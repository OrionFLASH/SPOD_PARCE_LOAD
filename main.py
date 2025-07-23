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
    }
]


# Логирование: уровень, шаблоны, имена
LOG_LEVEL = "DEBUG"  # или "DEBUG"
LOG_BASE_NAME = "LOGS"
LOG_MESSAGES = {
    "start":            "=== Старт работы программы: {time} ===",
    "reading_file":     "Загрузка файла: {file_path}",
    "read_ok":          "Файл успешно загружен: {file_path}, строк: {rows}, колонок: {cols}",
    "read_fail":        "Ошибка загрузки файла: {file_path}. {error}",
    "sheet_written":    "Лист Excel сформирован: {sheet} (строк: {rows}, колонок: {cols})",
    "finish":           "=== Завершение работы. Обработано файлов: {files}, строк всего: {rows_total}. Время выполнения: {time_elapsed} ===",
    "summary":          "Summary: {summary}"
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
    try:
        df = pd.read_csv(file_path, sep=";", header=0, dtype=str, quoting=3, encoding="utf-8", keep_default_na=False)
        logging.info(LOG_MESSAGES["read_ok"].format(file_path=file_path, rows=len(df), cols=len(df.columns)))
        return df
    except Exception as e:
        logging.error(LOG_MESSAGES["read_fail"].format(file_path=file_path, error=str(e)))
        return None

# === Запись в Excel с форматированием ===
def write_to_excel(sheets_data, output_path):
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, (df, params) in sheets_data.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            ws = writer.sheets[sheet_name]
            _format_sheet(ws, df, params)
            logging.info(LOG_MESSAGES["sheet_written"].format(sheet=sheet_name, rows=len(df), cols=len(df.columns)))

# === Форматирование листа ===
def _format_sheet(ws, df, params):
    header_font = Font(bold=True)
    align_center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    align_data = Alignment(horizontal="left", vertical="center", wrap_text=True)
    max_col_width = params.get("max_col_width", 30)

    # Форматирование заголовков
    for col_num, cell in enumerate(ws[1], 1):
        cell.font = header_font
        cell.alignment = align_center
        col_letter = get_column_letter(col_num)
        # Ширина — под контент, но не больше max_col_width
        max_width = min(
            max([len(str(cell.value)) for cell in ws[get_column_letter(col_num)] if cell.value] + [8]),
            max_col_width
        )
        ws.column_dimensions[col_letter].width = max_width

    # Данные: перенос строк, выравнивание по левому краю, по вертикали по центру
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            cell.alignment = align_data

    # Закрепление строк и столбцов
    ws.freeze_panes = params.get("freeze", "A2")
    ws.auto_filter.ref = ws.dimensions

def flatten_contest_feature_column(df, column='CONTEST_FEATURE', prefix="CONTEST_FEATURE => "):
    # Сбор всех ключей по всем строкам
    all_keys = set()
    json_objs = []
    for val in df[column]:
        try:
            obj = json.loads(val)
        except Exception:
            obj = {}
        json_objs.append(obj)
        all_keys.update(obj.keys())

    # Создаём новые колонки, заполняем
    for key in all_keys:
        colname = f"{prefix}{key}"
        new_col = []
        for obj in json_objs:
            val = obj.get(key, "")
            # Если массив — объединяем через ;
            if isinstance(val, list):
                val = ";".join([str(x) for x in val])
            new_col.append(val)
        df[colname] = new_col
    return df

# === Основная логика ===
def main():
    start_time = datetime.now()
    log_file = setup_logger()
    logging.info(LOG_MESSAGES["start"].format(time=start_time.strftime("%Y-%m-%d %H:%M:%S")))

    sheets_data = {}
    files_processed = 0
    rows_total = 0
    summary = []

    for file_conf in INPUT_FILES:
        file_path = os.path.join(DIR_INPUT, file_conf["file"] + ".CSV")
        sheet_name = file_conf["sheet"]
        logging.info(LOG_MESSAGES["reading_file"].format(file_path=file_path))
        df = read_csv_file(file_path)
        if df is not None:
            # Разворачиваем JSON-колонку для нужного файла
            if sheet_name == "CONTEST-DATA (PROM) 2025-07-14 v0" and "CONTEST_FEATURE" in df.columns:
                logging.info("Разворачивание колонки CONTEST_FEATURE в файле %s", file_path)
                df = flatten_contest_feature_column(df, column='CONTEST_FEATURE', prefix="CONTEST_FEATURE => ")
                logging.info("Разворачивание завершено: добавлено %d колонок", len([c for c in df.columns if c.startswith("CONTEST_FEATURE => ")]))
            sheets_data[sheet_name] = (df, file_conf)
            files_processed += 1
            rows_total += len(df)
            summary.append(f"{sheet_name}: {len(df)} строк")
        else:
            summary.append(f"{sheet_name}: ошибка")

    output_excel = os.path.join(DIR_OUTPUT, get_output_filename())
    write_to_excel(sheets_data, output_excel)

    time_elapsed = datetime.now() - start_time
    logging.info(LOG_MESSAGES["finish"].format(
        files=files_processed,
        rows_total=rows_total,
        time_elapsed=str(time_elapsed).split('.')[0]
    ))
    logging.info(LOG_MESSAGES["summary"].format(summary="; ".join(summary)))
    logging.info(f"Excel file: {output_excel}")
    logging.info(f"Log file: {log_file}")



if __name__ == "__main__":
    main()
