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

# === Глобальные константы и переменные ===

# Каталоги
DIR_INPUT = '/Users/orionflash/Desktop/MyProject/SPOD_PROM/SPOD/'
DIR_OUTPUT = '/Users/orionflash/Desktop/MyProject/SPOD_PROM/OUT/'
DIR_LOGS = '/Users/orionflash/Desktop/MyProject/SPOD_PROM/LOGS/'

# Входные файлы (имя без расширения)
INPUT_FILES = [
    "CONTEST-DATA (PROM) 2025-07-14 v0",
    "GROUP (PROM) 2025-06-17 v1",
    "INDICATOR (PROM) 2025-06-17 v1",
    "REPORT (PROM-KMKKSB) 2025-06-17 v1",
    "REWARD (PROM) 2025-07-21 v0",
    "REWARD-LINK (PROM) 2025-07-14 v0",
    "SVD_KB_DM_GAMIFICATION_ORG_UNIT_V20 2025_07_11 v1",
    "TOURNAMENT-SCHEDULE (PROM) 2025-07-21 v0",
    "PROM_USER_ROLE 2025-05-30 v0",
    "PROM_USER_ROLE SB 2025-05-30 v1"
]

# Соответствие: Имя листа, максимальная ширина колонки, закрепление
SHEET_PARAMS = {
    "CONTEST-DATA (PROM) 2025-07-14 v0":         {"max_col_width": 40, "freeze": "A2"},
    "GROUP (PROM) 2025-06-17 v1":                {"max_col_width": 30, "freeze": "A2"},
    "INDICATOR (PROM) 2025-06-17 v1":            {"max_col_width": 25, "freeze": "A2"},
    "REPORT (PROM-KMKKSB) 2025-06-17 v1":        {"max_col_width": 40, "freeze": "A2"},
    "REWARD (PROM) 2025-07-21 v0":               {"max_col_width": 25, "freeze": "A2"},
    "REWARD-LINK (PROM) 2025-07-14 v0":          {"max_col_width": 30, "freeze": "A2"},
    "SVD_KB_DM_GAMIFICATION_ORG_UNIT_V20 2025_07_11 v1": {"max_col_width": 40, "freeze": "A2"},
    "TOURNAMENT-SCHEDULE (PROM) 2025-07-21 v0":  {"max_col_width": 25, "freeze": "A2"},
    "PROM_USER_ROLE 2025-05-30 v0":              {"max_col_width": 25, "freeze": "A2"},
    "PROM_USER_ROLE SB 2025-05-30 v1":           {"max_col_width": 25, "freeze": "A2"},
}

# Логирование: уровень, шаблоны, имена
LOG_LEVEL = "INFO"  # или "DEBUG"
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
        for sheet_name, df in sheets_data.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            ws = writer.sheets[sheet_name]
            params = SHEET_PARAMS.get(sheet_name, {"max_col_width": 30, "freeze": "A2"})
            # Форматирование: ширина, автофильтр, закрепление
            _format_sheet(ws, df, params)
            logging.info(LOG_MESSAGES["sheet_written"].format(sheet=sheet_name, rows=len(df), cols=len(df.columns)))

# === Форматирование листа ===
def _format_sheet(ws, df, params):
    # Заголовок жирный, перенос строк, центр по горизонтали и вертикали
    header_font = Font(bold=True)
    align_center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    align_data = Alignment(horizontal="left", vertical="center", wrap_text=True)
    max_col_width = params.get("max_col_width", 30)

    # Форматирование заголовков
    for col_num, cell in enumerate(ws[1], 1):
        cell.font = header_font
        cell.alignment = align_center
        col_letter = get_column_letter(col_num)
        max_width = min(max([len(str(cell.value)) for cell in ws[get_column_letter(col_num)] if cell.value] + [8]), max_col_width)
        ws.column_dimensions[col_letter].width = max_width

    # Форматирование данных (перенос, выравнивание по левому краю, по вертикали по центру)
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            cell.alignment = align_data

    # Закрепление строк и столбцов
    ws.freeze_panes = params.get("freeze", "A2")
    # Автофильтр
    ws.auto_filter.ref = ws.dimensions

# === Основная логика ===
def main():
    start_time = datetime.now()
    log_file = setup_logger()
    logging.info(LOG_MESSAGES["start"].format(time=start_time.strftime("%Y-%m-%d %H:%M:%S")))

    sheets_data = {}
    files_processed = 0
    rows_total = 0
    summary = []

    for file_name in INPUT_FILES:
        file_path = os.path.join(DIR_INPUT, file_name + ".CSV")
        logging.info(LOG_MESSAGES["reading_file"].format(file_path=file_path))
        df = read_csv_file(file_path)
        if df is not None:
            sheets_data[file_name] = df
            files_processed += 1
            rows_total += len(df)
            summary.append(f"{file_name}: {len(df)} строк")
        else:
            summary.append(f"{file_name}: ошибка")

    # Выходной Excel
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
