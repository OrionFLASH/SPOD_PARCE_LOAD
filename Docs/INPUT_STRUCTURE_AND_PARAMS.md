# Структура входных файлов, зависимости и параметры (main.py)

Документ описывает конфигурацию входных данных, связи между листами и все настраиваемые параметры по коду `main.py`.

---

## 1. Структура входных файлов и каталоги

- **Каталог входных файлов:** `DIR_INPUT` = `SPOD` (относительно корня скрипта).
- **Формат:** CSV с разделителем `;`, кодировка UTF-8, поиск по имени без учёта регистра, расширения: `.csv`, `.CSV`.
- **Поиск файла:** `find_file_case_insensitive(DIR_INPUT, file_conf["file"], [".csv", ".CSV"])` — имя из конфига без расширения.

### Список входных файлов (INPUT_FILES)

Каждый элемент задаёт один CSV и один лист в итоговом Excel:

| № | Параметр `file` (имя без расширения) | Лист `sheet` |
|---|--------------------------------------|--------------|
| 1 | CONTEST (PROM) 30-01 v2 | CONTEST-DATA |
| 2 | GROUP (PROM) 30-01 v1 | GROUP |
| 3 | INDICATOR (PROM) 30-01 v1 | INDICATOR |
| 4 | REPORT (PROM) 30-01 v2 | REPORT |
| 5 | REWARD (PROM) 30-01 v2 | REWARD |
| 6 | REWARD-LINK (PROM) 30-01 v1 | REWARD-LINK |
| 7 | SCHEDULE (PROM) 30-01 v2 | TOURNAMENT-SCHEDULE |
| 8 | SVD_KB_DM_GAMIFICATION_ORG_UNIT_V20 - 2025.08.28 | ORG_UNIT_V20 |
| 9 | USER_ROLE (PROM) 12-12 v0 | USER_ROLE |
| 10 | USER_ROLE_SB (PROM) 12-12 v0 | USER_ROLE SB |
| 11 | employee_PROM_final_5000_2025-07-26_00-09-03 | EMPLOYEE |
| 12 | gamification-employeeRewards-3 | LIST-REWARDS |
| 13 | gamification-statistics-4 | STATISTICS |
| 14 | gamification-tournamentList-2 | LIST-TOURNAMENT |

Перед запуском обработки вызывается `check_input_files_exist()`: проверяется наличие каждого файла из `INPUT_FILES` в каталоге `SPOD`. Если хотя бы один файл отсутствует, программа выводит список отсутствующих файлов и завершается (`sys.exit(1)`), без запасных имён.

---

## 2. Параметры одного входного файла (INPUT_FILES[*])

Для каждого элемента `INPUT_FILES` используются такие ключи (и дальше те же поля попадают в `params` листа при записи в Excel):

| Параметр | Тип | Назначение | Варианты / примеры |
|----------|-----|------------|---------------------|
| **file** | str | Имя файла без расширения | Любая строка (по нему ищется `file.csv` / `file.CSV` в `SPOD`) |
| **sheet** | str | Имя листа в итоговом Excel | Уникальное имя листа (CONTEST-DATA, GROUP, …) |
| **max_col_width** | int | Макс. ширина колонки (символов) | 20–200 по листам (например 120, 20, 100, 25, 200, …) |
| **freeze** | str | Закрепление области в Excel | Строка вида `"<колонка><строка>"`: A2, B2, C2, D2, F2, G2. Пример: `"C2"` — закреплены колонки A,B и строка 1 |
| **col_width_mode** | str \| int | Режим ширины колонок | **"AUTO"** — по содержимому (ограничено max_col_width); **число** — фиксированная ширина в символах |
| **min_col_width** | int | Минимальная ширина колонки | 8–15 в зависимости от листа |

Использование в коде:

- `process_single_file(file_conf)` берёт `file_conf["file"]`, `file_conf["sheet"]`.
- После загрузки в `sheets_data[sheet_name]` кладётся `(df, file_conf)` — то есть этот же dict используется как `params` при записи.
- В `_format_sheet(ws, df, params)` и `calculate_column_width(..., params, ...)` читаются:
  - `params.get("max_col_width", 30)`
  - `params.get("col_width_mode", "AUTO")`
  - `params.get("min_col_width", 8)`
  - `params.get("freeze", "A2")` → присваивается `ws.freeze_panes`.

---

## 3. Сводный лист (SUMMARY_SHEET)

Отдельный конфиг (не из INPUT_FILES):

- **sheet:** `"SUMMARY"` — лист создаётся программно, не из CSV.
- **max_col_width:** 150
- **freeze:** `"G2"`
- **col_width_mode:** `"AUTO"`
- **min_col_width:** 8

---

## 4. Зависимости между листами и порядок обработки

- Загружаются все CSV из `INPUT_FILES` параллельно; каждый CSV → один лист в `sheets_data` с именем `sheet` и параметрами из того же элемента `INPUT_FILES`.
- Далее по цепочке:
  1. **EMPLOYEE** — добавляется колонка **AUTO_GENDER** (по паттернам ФИО из `GENDER_PATTERNS`).
  2. **FIELD_LENGTH_VALIDATIONS** — проверка длины полей только для листов **ORG_UNIT_V20**, **EMPLOYEE**, **REPORT** (результат в колонку `FIELD_LENGTH_CHECK`).
  3. **TOURNAMENT-SCHEDULE** — добавляется **CALC_TOURNAMENT_STATUS**; для расчёта используются **REPORT** (CONTEST_DATE, TOURNAMENT_CODE). То есть TOURNAMENT-SCHEDULE зависит от REPORT.
  4. **MERGE_FIELDS_ADVANCED** — добавление полей между листами (источник → приёмник по ключам). От этого зависят состав и порядок колонок на многих листах.
  5. Проверка дубликатов по листам.
  6. **SUMMARY** строится из ключей и правил слияния (`SUMMARY_KEY_DEFS`, `build_summary_sheet` с правилами MERGE для `sheet_dst == "SUMMARY"`).

### Связи по ключам (SUMMARY_KEY_DEFS)

- CONTEST-DATA: CONTEST_CODE
- TOURNAMENT-SCHEDULE: TOURNAMENT_CODE, CONTEST_CODE
- REWARD-LINK: REWARD_CODE, CONTEST_CODE
- GROUP: GROUP_CODE, CONTEST_CODE, GROUP_VALUE
- REWARD: REWARD_CODE
- INDICATOR: INDICATOR_ADD_CALC_TYPE, CONTEST_CODE

Итог: входные файлы от которых что-то считается — в первую очередь **REPORT** (для TOURNAMENT-SCHEDULE и для SUMMARY). Остальные связи задаются правилами MERGE_FIELDS_ADVANCED (источник → приёмник по ключам).

---

## 5. JSON-поля (JSON_COLUMNS)

Разворачиваются только для листов, указанных в `JSON_COLUMNS`:

| Лист | Колонка в CSV | Префикс после разворота |
|------|----------------|---------------------------|
| CONTEST-DATA | CONTEST_FEATURE | CONTEST_FEATURE |
| REWARD | REWARD_ADD_DATA | ADD_DATA |

Структура элемента: `{"column": "<имя колонки>", "prefix": "<префикс>"}`. Остальные листы в `JSON_COLUMNS` не заданы — JSON там не разворачивается.

---

## 6. Валидация длины полей (FIELD_LENGTH_VALIDATIONS)

Тип поля: для каждого листа — словарь с ключами:

- **result_column** — имя колонки с результатом проверки (например `"FIELD_LENGTH_CHECK"`).
- **fields** — словарь `{имя_поля: {"limit": int, "operator": str}}`.

**operator** (используются в коде):

- **"<="** — длина должна быть ≤ limit
- **"="** — длина должна быть ровно limit
- **">="**, **"<"**, **">"** — аналогично по коду проверки

Примеры из кода:

- ORG_UNIT_V20: TB_FULL_NAME (≤100), GOSB_NAME (≤100), GOSB_SHORT_NAME (≤20).
- EMPLOYEE: PERSON_NUMBER (=20), PERSON_NUMBER_ADD (=20).
- REPORT: MANAGER_PERSON_NUMBER (=20).

Типы: `limit` — целое число; `operator` — одна из строк выше.

---

## 7. Правила слияния (MERGE_FIELDS_ADVANCED) — параметры правил

Для каждого правила используются (в коде и в конфиге):

- **sheet_src**, **sheet_dst** — строки (имена листов).
- **src_key**, **dst_key** — списки строк (имена колонок).
- **column** — список строк (поля для переноса или для count).
- **mode** — строка: **"value"** (перенос значений) или **"count"** (агрегат по ключу).
- **multiply_rows** — bool.
- **col_max_width**, **col_width_mode**, **col_min_width** — как у листов (для добавленных колонок).
- **status_filters** — dict или None: `{"ИМЯ_КОЛОНКИ": ["значение1", "значение2"]}`; оставляются только строки, где значение в этой колонке входит в список.
- **custom_conditions** — dict или None.
- **group_by**, **aggregate** — для группировки/агрегации или None.
- Для **mode == "count"**:
  - **count_aggregation** — **"size"** (число строк) или **"nunique"** (число уникальных значений).
  - **count_label** — строка или None (например "ACTIVE", "DELETED" — для имени колонки COUNT_...).

### Фиксированные значения статусов в коде

- **BUSINESS_STATUS** (фильтр в merge): `["АКТИВНЫЙ", "ПОДВЕДЕНИЕ ИТОГОВ"]`.
- **TOURNAMENT_STATUS** (в merge и в `calculate_tournament_status`):
  **"АКТИВНЫЙ"**, **"ЗАВЕРШЕН"**, **"ОТМЕНЕН"**, **"ПОДВЕДЕНИЕ ИТОГОВ"**, **"УДАЛЕН"**, **"ЗАПЛАНИРОВАН"**, **"НЕОПРЕДЕЛЕН"**.

---

## 8. Логирование и пути

- **LOG_LEVEL** — строка: **"DEBUG"** или **"INFO"** (в коде упоминается "INFO" для продакшена, "DEBUG" для отладки).
- **LOG_BASE_NAME** — строка (базовое имя лог-файла, например `"LOGS"`).
- **DIR_LOGS** — каталог логов (относительно скрипта).
- **DIR_OUTPUT** — каталог для выходного Excel (`OUT`).

Файл лога: `LOGS/LOGS_<LEVEL>_YYYYMMDD_HH_MM.log`.

---

## 9. Остальные параметры

- **GENDER_PATTERNS** — словарь списков строк для определения пола по отчеству/имени/фамилии (`patronymic_male/female`, `name_male/female`, `surname_male/female`). Варианты значений — текстовые окончания (строки).
- **GENDER_PROGRESS_STEP** — целое число (шаг вывода прогресса по строкам при расчёте пола).
- **MAX_WORKERS_IO**, **MAX_WORKERS_CPU**, **MAX_WORKERS** — числа потоков (вычисляются от `os.cpu_count()` с ограничениями 16 и 8).
- **COLOR_SCHEME** — список словарей (group, header_bg, header_fg, column_bg, column_fg, style_scope, sheets, columns). Цвета — строки HEX (например `"E6F3FF"`). **style_scope** — строка, в коде используется **"header"**.
- **SUMMARY_KEY_DEFS** — список `{"sheet": str, "cols": [str]}`; от него зависят ключи сводного листа и порядок колонок в SUMMARY.

---

## 10. Краткая схема «кто от кого зависит»

- **Вход:** 14 CSV в каталоге `SPOD`; имена и привязка к листам — в `INPUT_FILES`.
- **Один особый случай:** TOURNAMENT-SCHEDULE (статус) и SUMMARY (данные по турнирам) зависят от листа **REPORT**.
- **Остальные связи:** задаются только правилами **MERGE_FIELDS_ADVANCED** (источник → приёмник по ключам) и **SUMMARY_KEY_DEFS** для сводного листа.
- **Параметры форматирования** каждого листа (в т.ч. входного) задаются соответствующим элементом **INPUT_FILES** (или **SUMMARY_SHEET** для SUMMARY); они же через `params` попадают в `_format_sheet` и `calculate_column_width` (max_col_width, col_width_mode, min_col_width, freeze).
