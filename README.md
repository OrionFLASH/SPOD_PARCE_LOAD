# SPOD PROM - Система обработки и редактирования данных

## Содержание

1. [Общее описание](#общее-описание)
2. [Структура проекта](#структура-проекта)
3. [Навигация по документации](#навигация-по-документации)
4. [Модули src/ (описание и назначение)](#модули-src-описание-и-назначение)
5. [Конфигурация config.json](#конфигурация-configjson)
6. [Программа main.py - Обработка данных](#программа-mainpy---обработка-данных)
7. [Техническое задание](#техническое-задание)
8. [Анализ входных данных](#анализ-входных-данных)
9. [Установка и запуск](#установка-и-запуск)
10. [Логирование](#логирование)
11. [История версий](#история-версий)

> **Актуальность:** описание пайплайна, `config.json` и ведение вспомогательных файлов в **`Docs/`** синхронизированы; индекс: **`Docs/DOCS_INDEX.md`**. Краткий справочник входных данных и конфигурации: **`Docs/INPUT_DATA_AND_CONFIG_FULL.md`**.

---

## Общее описание

Основной компонент проекта — **`main.py`**: чтение CSV выгрузки SPOD, обработка, объединение по правилам конфигурации и запись результата в Excel (пайплайн в **`src/main_impl.py`**).

---

## Структура проекта

```
SPOD_PROM/
├── main.py                 # Точка входа: загрузка Config, запуск пайплайна (код в src/)
├── config.json             # Конфигурация (пути, файлы, правила merge, цвета, форматы и т.д.)
├── README.md               # Документация проекта (в т.ч. полное описание config.json)
├── src/                    # Исходный код обработки (модули и классы)
│   ├── __init__.py
│   ├── config_loader.py   # Класс Config — загрузка config.json
│   ├── config_holder.py    # Внедрение конфига для main_impl
│   ├── logging_setup.py   # Форматтер логов, настройка логгера
│   ├── debug_timing.py    # DEBUG [PERF]; отдельный xlsx STAT_FILE <таймштамп> (этапы и функции)
│   ├── console_ui.py      # Краткий вывод в консоль: этапы, прогресс, сводки (stdlib)
│   ├── json_utils.py      # Разбор и разворот JSON-полей
│   ├── json_spod_format_check.py  # Проверка формата SPOD-JSON в ячейках (consistency_checks: json_spod_format)
│   ├── archive_json_columns.py  # Архив SQLite: колонки JSON_* из CONTEST_FEATURE / REWARD_ADD_DATA
│   ├── input_archive_sqlite.py   # Архив сырых CSV в SQLite (снимки, дедуп, JSON_* для CONTEST/REWARD)
│   ├── reward_getcondition_summary.py  # Сводная колонка по getCondition на листе REWARD
│   ├── file_loader.py     # Класс FileLoader — поиск/чтение CSV, разворот JSON
│   ├── tournament.py      # Расчёт статуса турнира (CALC_TOURNAMENT_STATUS)
│   ├── validation.py     # Валидация длины полей, проверка дубликатов
│   ├── gender.py         # Определение пола (AUTO_GENDER)
│   └── main_impl.py      # Полный пайплайн: загрузка, merge, summary, Excel, отчёты
├── requirements.txt        # Зависимости (pandas, openpyxl и др.) для main.py
├── IN/                     # Корень входных данных (paths.input); внутри — subdir (SPOD, FILE и т.д.)
├── OUT/                    # Базовый каталог вывода (paths.output); файлы по дате: OUT/YYYY/DD-MM/; опционально OUT/DB/*.sqlite — архив входных CSV (input_archive_sqlite.db_path)
├── BACKUP/                 # Резервные копии
├── POST/                   # Не в Git (.gitignore). Снимок: python src/Tools/sync_post_txt.py — main.py.txt, config.json.txt, src/**/*.py.txt (без Tools/Tests); инструкция и bat из Docs/POST_SNAPSHOT/
├── LOGS/                   # Файлы логов (paths.logs); по дате: LOGS/YYYY/DD-MM/
├── Docs/                   # Дополнительная документация; каталог CSV/JSON — Docs/JSON/ (см. README внутри)
├── src/Tools/              # Утилиты: build_spod_input_catalog.py, export_spod_json_examples.py, sync_post_txt.py (заполнение POST/)
│   └── catalog_glossary/   # Фрагменты пояснений к JSON для каталога
└── venv/                  # Виртуальное окружение
```

---

## Навигация по документации

Для удобной ориентации по дополнительным материалам используйте единый индекс:

- `Docs/DOCS_INDEX.md` — карта документации и правила актуализации.

Ключевые документы по темам:

- `Docs/INPUT_DATA_AND_CONFIG_FULL.md` — структура входных данных и конфигурация.
- `Docs/CONSISTENCY_CHECKS_FORMAT.md` — формат правил `consistency_checks`.
- `Docs/SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.sql` — SQL-зеркало правил `referential` / `referential_composite` / `unique` / `field_length` для СУБД (подробная версия с комментариями). Результат: блок **SUMMARY** (`passed` 1/0, `violation_count`) и блок **DETAIL** (`detail_key`, `detail_message`) без колонок `check_id` / `check_type`; правила **`field_format`** в SQL не дублируются (только Python). CTE **`dim_*`** / **`base_schedule_ref`** снижают повторные сканы таблиц; в файле — глоссарий SQL и пояснения к коду на русском, связь с **`consistency_checks.py`**.
- `Docs/SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.md` — подробная документация по SQL-зеркалу: одна команда `WITH`…`SELECT`, все CTE и проверки, таблицы и поля витрины, что заменять под реальную БД, формат результата SUMMARY/DETAIL, плюс соглашение по двум версиям SQL-файла (подробная и `*_PLAIN.sql`).
- `Docs/CONSISTENCY_SAMPLE_FORMAT.md` — формат заполнения колонки `sample`.
- `Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md` — архив входных CSV в SQLite (без отдельного сервера); секция **`input_archive_sqlite`** в `config.json`, код **`src/input_archive_sqlite.py`**, разворот **`CONTEST_FEATURE`** / **`REWARD_ADD_DATA`** в колонки **`JSON_*`** — **`src/archive_json_columns.py`**.
- `Docs/АНАЛИЗ_ПРОВЕРОК_КОНСИСТЕНТНОСТИ.md` — аналитика покрытия и предложения по новым правилам.
- `Docs/PERFORMANCE_AND_PARALLELIZATION_HISTORY.md` — консолидированная история оптимизации и распараллеливания.
- `Docs/SUMMARY_GROUP_FIX_HISTORY.md` — история исправлений логики `SUMMARY` и связки `GROUP`.
- `Docs/JSON/` — **каталог входных данных и примеров JSON:** `SPOD_INPUT_DATA_CATALOG.md`, папка `examples/` с реальными JSON из `IN/SPOD`; см. `Docs/JSON/README.md`. Пересборка каталога: `python src/Tools/build_spod_input_catalog.py`; примеры JSON: `python src/Tools/export_spod_json_examples.py`.

---

## Модули src/ (описание и назначение)

Исходный код обработки данных вынесен в каталог **src/**; корневой **main.py** только загружает конфигурацию и запускает пайплайн.

| Модуль | Назначение | Основные сущности |
|--------|------------|-------------------|
| **config_loader.py** | Загрузка и хранение настроек из config.json | Класс `Config`: атрибуты `dir_input`, `dir_output`, `dir_logs`, `input_files`, `run_outputs`, `run_source_only_exit`, `run_write_source`, `run_write_main`, `run_write_consistency_file`, `run_consistency_early`, `run_mode` (число 1–4 для логов), **`parse_run_outputs_config`**, остальные поля как прежде; метод `get_output_filename()`. |
| **config_holder.py** | Внедрение текущего конфига для кода, работающего с глобальными переменными | `set_current_config(config)`, `get_current_config()`. |
| **logging_setup.py** | Настройка логирования | Класс `CallerFormatter` (добавляет имя функции в сообщение); функция `setup_logger(config)` — путь к лог-файлу; уровень файла из конфига (обычно DEBUG). В **`main_impl`** после настройки консольный поток поднимается до **WARNING**, чтобы **INFO** шёл в файл; краткий ход — **`console_ui`**. |
| **json_utils.py** | Разбор и разворот JSON-полей в DataFrame | `safe_json_loads(s)` — парсинг строки в JSON с поправкой типичных ошибок; `safe_json_loads_preserve_triple_quotes(s)`; `flatten_json_column_recursive(df, column, prefix=..., sheet=..., sep=..., max_workers_io=...)` — рекурсивный разворот колонки в несколько колонок, при большом объёме — параллельно. |
| **reward_getcondition_summary.py** | Сводный текст по кодам getCondition на листе REWARD | `add_reward_getcondition_summary_column(df_reward, prefix=..., column_name=...)` — после разворота JSON и merge; строки вида `[код] FULL_NAME {seasonItem}`. |
| **reward_item_catalog.py** | Каталог ITEM из **`REWARD_ADD_DATA`** и проверка доступности товара менеджеру | `build_item_catalog_from_reward_df`, `rules_for_matrix_column`, `item_accessible_for_manager` — для раскраски матрицы на **RATING**; учёт массива **`ignoreConditions`** (табельные «всегда доступно»). |
| **rating_item_matrix.py** | Колонки-счётчики по ORDER и подсветка доступности ITEM на **RATING** | `apply_rating_item_matrix_enrichment`, `apply_rating_item_matrix_colors` — светло-зелёный / светло-красный по полным критериям из JSON и листов **ORDER** / **LIST-REWARDS**; передача табельного в **`item_accessible_for_manager`**. |
| **json_spod_format_check.py** | Валидация SPOD-JSON: BOM/Unicode-пробелы вне **`"""…"""`**, симметрия внешних кавычек, рекурсивный разбор **со сбором всех** структурных ошибок в ячейке; **`""key""`**, JSON- и **`""значение""`** у строки, лишние **`{}`** в массиве; **`numeric_value_keys`**; нормализация и **json.loads**; **короткие** строки (путь + суть), лимиты **`_MAX_STRUCTURE_ERRORS`** / **`_MAX_CELL_ERROR_LEN`** | `validate_spod_json_cell`, `run_json_spod_format_check` — из **`consistency_checks`**, **`type: "json_spod_format"`**. |
| **file_loader.py** | Поиск и загрузка CSV, разворот JSON по конфигу | Класс `FileLoader(config)`: `find_file_case_insensitive(directory, base_name, extensions)`, `check_input_files_exist()`, `read_csv_file(file_path)`, `process_single_file(file_conf)` — возвращает `(df, sheet_name, file_conf)` или `(None, sheet_name, None)`. |
| **tournament.py** | Расчёт статуса турнира по датам | `calculate_tournament_status(config, df_tournament, df_report=None)` — добавляет колонку `CALC_TOURNAMENT_STATUS` по правилам из `config.tournament_status_choices`. |
| **validation.py** | Валидация длины полей и проверка дубликатов (устаревшие пути) | `validate_field_lengths(config, df, sheet_name)`, `validate_field_lengths_vectorized(config, df, sheet_name)`, `compare_validate_results`, `mark_duplicates`, `validate_single_sheet`, `check_duplicates_single_sheet`. Основной пайплайн больше не использует отдельные шаги проверки дубликатов и длины полей — всё выполняется в **consistency_checks**. |
| **consistency_checks.py** | Проверки консистентности (unique, field_length, field_format, referential, referential_composite с фильтрами строк, json_*, **json_spod_format**) | Выполняет правила из `consistency_checks.rules` **с параллелизацией** (ThreadPoolExecutor). **Фаза 1** — создаёт на листах колонки `unique`, `field_length`, `field_format`, json_field_*; **Фаза 2** — referential/referential_composite, **json_spod_format**, сбор результатов. Правила с **`enabled: false`** не выполняются, но строка в своде **CONSISTENCY** всё равно создаётся (**total_rows**, **violations=0**, текст в **sample**). Парсинг JSON в ячейках: **`_parse_add_data_cell`**, **`_parse_add_data_cell_with_normalized`**. Свод **CONSISTENCY** и **csv_columns_count** — как раньше. См. **`Docs/CONSISTENCY_CHECKS_FORMAT.md`** (п. 2.2 — фильтры **src_row_conditions** / **ref_row_conditions**, п. 2.8 — **json_spod_format**). |
| **gender.py** | Определение пола по отчеству, имени, фамилии | `add_auto_gender_column(config, df, sheet_name)`, `add_auto_gender_column_vectorized(config, df, sheet_name)`, `compare_gender_results(df_old, df_new)`. Внутри используются паттерны из `config.gender_patterns`. |
| **console_ui.py** | Краткий вывод в **stdout** при работе **main** | Этапы, сводки, **`print_consistency_summary`**, **`print_input_archive_sqlite_report`** (путь к БД **от корня проекта** без усечения «…», таблица по листам). Только stdlib. |
| **main_impl.py** | Полный пайплайн обработки | При импорте вызывается `_load_config_globals()`. Функция `main()`: хуки консоли и прогресс по этапам → параллельная загрузка CSV и разворот JSON → **выгрузка source** (`SPOD_PROM source …`) только в режимах **`full`** и отдельно в **`source_only`** (до выхода); в **`main_only`** и **`consistency_only`** source не создаётся → проверка наличия файлов → проверки консистентности на сырых данных и перенос на обработанные листы → добавление AUTO_GENDER (EMPLOYEE) → расчёт статуса турнира → merge (кроме SUMMARY) → **сводка getCondition на REWARD** (`reward_getcondition_summary`, если не `consistency_only`) → **проверки консистентности** (модуль `consistency_checks`) → формирование SUMMARY → лист STAT_FILE → запись основного Excel → **файл статистики времени** `STAT_FILE <таймштамп>.xlsx` (`write_performance_statistics_excel` из `debug_timing`) → итоговый отчёт по отклонениям длины полей и расхождениям CSV (**полный текст в лог**, в консоль — **`console_ui`**). Режим **`consistency_only`**: без merge, gender, турнира и основного Excel — только файл консистентности. Файл **source**: для всех ячеек включён перенос по словам (`write_source_excel`). Запись основного Excel: **`write_to_excel`**, подготовка типов **`apply_column_format_conversion`**, пост-оформление листа **`_format_sheet`** (ширины **`calculate_column_width`** с выборкой **`_AUTO_COLUMN_WIDTH_MAX_DATA_ROWS`**, цвета **`apply_color_scheme`**, выравнивание и форматы **`apply_column_formats`**, вспомогательно **`_column_indices_covered_by_column_formats`**). |

**Запуск:** из корня проекта выполняется `python main.py`. При этом создаётся `Config()` (путь к config.json — корень проекта), конфиг передаётся в `set_current_config(config)`, затем вызывается `main_impl.main()`. В начале `main_impl.main()` снова вызывается `_load_config_globals()`, поэтому все глобальные переменные в main_impl берутся из внедрённого конфига.

---

## Конфигурация config.json

Все параметры обработки данных задаются в файле **config.json** в корне проекта. Программа `main.py` при запуске загружает конфиг и использует его значения. Изменение настроек не требует правки кода.

### Полный перечень секций config.json

| Секция | Назначение |
|--------|------------|
| `run_outputs` | Массив строк: `source_only`, `main_only`, `consistency_only` — какие выходные файлы создавать (можно несколько). Эквивалент старого `full`: все три. Устаревшее поле **`run_mode`** (строка или 1–4) читается, если **`run_outputs`** отсутствует. |
| `output_filenames` | Имена выходных файлов без расширения: main, source, consistency. |
| `apply_sort_to_source` | Применять ли сортировку из `input_files.sort_columns` при записи source Excel. |
| `apply_sort_to_main` | Применять ли сортировку из `input_files.sort_columns` при записи основного Excel. |
| `paths` | Каталоги: вход (IN/SPOD), выход (OUT), логи (LOGS). Выходные файлы пишутся в подпапки по дате: OUT/YYYY/DD-MM. |
| `logging` | Уровень (INFO/DEBUG) и базовое имя файла логов. |
| `performance` | Количество потоков: max_workers_io, max_workers_cpu. |
| `tournament_status_choices` | Подписи статусов турнира (расчёт CALC_TOURNAMENT_STATUS). |
| `input_files` | Список CSV: file, sheet, expected_columns (0=АВТО), subdir, **`aggregate_into_sheet`** (опц.), **`archive_db_path`** (опц., отдельный файл архива SQLite), **`archive_to_db`**, sort_columns, ширина, freeze, include_in_source. |
| `summary_sheet` | Параметры сводного листа SUMMARY (ширина, закрепление). |
| `sheet_order` | Порядок листов в выходном Excel (если задан). |
| `summary_key_defs` | Ключевые колонки по листам для каркаса SUMMARY (в т.ч. INDICATOR: INDICATOR_CODE, INDICATOR_ADD_CALC_TYPE, CONTEST_CODE). |
| `gender` | Правила автоопределения пола (паттерны отчества/имени/фамилии). |
| `derived_columns` | Производные колонки (pad_left и т.д.). |
| `merge_fields_advanced` | Правила переноса/подсчёта полей между листами. |
| `color_scheme` | Цвета заголовков и ячеек по листам и колонкам. |
| `column_formats` | Формат ячеек: число, дата, выравнивание по листам и колонкам; режимы `columns` и `except_columns` (см. раздел **column_formats**). |
| `consistency_checks` | Единый конфиг проверок: **summary_sheet_name**, **rules**, **csv_columns_count**; опционально **`spod_todo_config_guide`** (текст-подсказка где искать правила SPOD-JSON и фильтры referential). Типы правил: **unique**, **field_length**, **field_format**, **referential**, **referential_composite**, **json_field_***, **json_priority_unique_per_contest_link**, **json_spod_format**. |
| `json_columns` | Колонки с JSON для разворота по листам (column, prefix). |
| `reward_getcondition_summary` | Сводная колонка на листе REWARD по кодам `getCondition` (nonRewards/rewards); `enabled`, `column_name`. |
| `rating_item_matrix` | Матрица ITEM на листе **RATING**: счётчики заказов по **ORDER** и подсветка доступности товара (зелёный/красный) по **`REWARD_ADD_DATA`**, **LIST-REWARDS**, кристаллам; см. раздел **rating_item_matrix**. |

### Общая структура файла

```json
{
  "run_outputs": ["source_only", "main_only", "consistency_only"],
  "_run_outputs_allowed": ["source_only", "main_only", "consistency_only"],
  "apply_sort_to_source": true,
  "apply_sort_to_main": false,
  "output_filenames": { "main": "SPOD_ALL_IN_ONE", "source": "SPOD_PROM source", "consistency": "SPOD_PROM CONSISTENCY" },
  "paths": { "input": "IN", "output": "OUT", "logs": "LOGS" },
  "logging": { ... },
  "performance": { ... },
  "tournament_status_choices": [ ... ],
  "input_files": [ ... ],
  "summary_sheet": { ... },
  "sheet_order": [ ... ],
  "summary_key_defs": [ ... ],
  "gender": { ... },
  "derived_columns": [ ... ],
  "merge_fields_advanced": [ ... ],
  "color_scheme": [ ... ],
  "column_formats": [ ... ],
  "consistency_checks": { "summary_sheet_name": "CONSISTENCY", "rules": [ ... ] },
  "json_columns": { ... },
  "reward_getcondition_summary": { "enabled": true, "column_name": "..." },
  "rating_item_matrix": { "enabled": true, "...": "..." }
}
```

Дополнительные секции (при наличии в файле): `derived_columns`; **`rating_item_matrix`** (матрица ITEM на RATING); опционально блок **`source_export`** с **`sort_rules`** для сортировки листов в source Excel — см. класс `Config` в **config_loader.py** и разделы ниже.

---

### run_outputs

**Назначение:** список выходных артефактов. Значения (строки, регистр не важен):

| Токен в массиве | Что создаётся |
|-----------------|---------------|
| `source_only` | Файл **source** Excel (если в массиве **только** этот элемент — запись source и **выход** без merge/main). |
| `main_only` | **Основной** Excel (SUMMARY, merge, STAT_FILE и т.д.). |
| `consistency_only` | Отдельная книга **консистентности**. Если **нет** `main_only`, но есть `consistency_only` — выполняется бывший режим «только консистентность» (без merge/gender). Если **есть** и `main_only`, и `consistency_only` — после основной книги дополнительно пишется файл consistency (как в старом `full`). |

**Примеры:**
```json
"run_outputs": ["main_only"]
```
```json
"run_outputs": ["source_only", "main_only", "consistency_only"]
```

**Обратная совместимость:** при отсутствии **`run_outputs`** используется **`run_mode`**: `full` → все три токена; `source_only` / `main_only` / `consistency_only` — один токен. Разбор в **`config_loader.parse_run_outputs_config`**. Файлы пишутся в подкаталог по дате. Для **source** включён перенос по словам.

---

### output_filenames

**Назначение:** имена выходных файлов без расширения. К имени добавляется метка времени и расширение .xlsx (кроме baseline).

| Ключ           | Тип    | Описание |
|----------------|--------|----------|
| `main`         | строка | Имя основного Excel (например `SPOD_ALL_IN_ONE_2026-03-17_12-00-00.xlsx`). |
| `source`       | строка | Имя файла сырых данных (например `SPOD_PROM source 2026-03-17_12-00-00.xlsx`). |
| `consistency`  | строка | Имя файла консистентности в режиме 4 (например `SPOD_PROM CONSISTENCY 2026-03-17_12-00-00.xlsx`). |

**Пример:**
```json
"output_filenames": {
  "main": "SPOD_ALL_IN_ONE",
  "source": "SPOD_PROM source",
  "consistency": "SPOD_PROM CONSISTENCY"
}
```

---

### apply_sort_to_source, apply_sort_to_main

**Назначение:** включать ли сортировку при записи в Excel. Правила сортировки задаются в каждом элементе `input_files` в поле `sort_columns`.

| Ключ                    | Тип  | По умолчанию | Описание |
|-------------------------|------|--------------|----------|
| `apply_sort_to_source`  | bool | `true`       | Применять сортировку из `input_files[].sort_columns` при записи файла сырых данных (source). |
| `apply_sort_to_main`    | bool | `false`      | Применять сортировку из `input_files[].sort_columns` при записи основного Excel. |

**Пример:**
```json
"apply_sort_to_source": true,
"apply_sort_to_main": false
```

**Логика:** для каждого листа берётся массив `sort_columns` из элемента `input_files` с соответствующим `sheet`. Колонки применяются последовательно (1 → 2 → 3). Если колонки нет на листе — она пропускается; если ни одной колонки нет — сортировка не выполняется.

---

### paths

**Назначение:** относительные имена каталогов для входных данных, выходного Excel и логов. Пути собираются программой как `(каталог_программы)/(значение)`.

| Ключ     | Тип   | Описание |
|----------|--------|----------|
| `input`  | строка | Корневой каталог с подкаталогами CSV (например `"IN"`). Файлы ищутся в `paths.input/subdir/` по элементу `input_files[].subdir`. |
| `output` | строка | Базовый каталог для сгенерированных xlsx (по умолчанию `"OUT"`). **Файлы пишутся в подпапки по дате формирования:** `OUT/YYYY/DD-MM/`, где YYYY — год (4 цифры), DD — день (2 цифры), MM — месяц (2 цифры). Например: 16 марта 2026 → `OUT/2026/16-03/`, 1 января → `OUT/2026/01-01/`. Каталоги создаются автоматически. |
| `logs`   | строка | Каталог для лог-файлов (по умолчанию `"LOGS"`). **Файлы пишутся в подпапки по дате:** `LOGS/YYYY/DD-MM/` (как для OUT). |

**Пример:**
```json
"paths": {
  "input": "IN",
  "output": "OUT",
  "logs": "LOGS"
}
```

**Логика:** при старте формируются `DIR_INPUT`, `DIR_OUTPUT`, `DIR_LOGS`. При записи любого выходного файла используется `get_output_dir_for_run(DIR_OUTPUT)` → `OUT/2026/17-03/` (текущая дата). Source, consistency и main Excel пишутся в этот подкаталог.

---

### logging

**Назначение:** настройки логирования.

| Ключ        | Тип   | Описание |
|-------------|--------|----------|
| `level`     | строка | Уровень логгера: `"INFO"` (обычная работа) или `"DEBUG"` (подробный вывод в файл). |
| `base_name` | строка | Базовое имя лог-файла; к нему добавляются уровень и дата/время. Файл создаётся в `LOGS/YYYY/DD-MM/` (например, `LOGS/2026/31-01/LOGS_DEBUG_20260204_22_30.log`). |

**Пример:**
```json
"logging": {
  "level": "INFO",
  "base_name": "LOGS"
}
```

**Логика:** имя файла лога: `{base_name}_{level}_{YYYYMMDD}_{HH_MM}.log` в каталоге `paths.logs`. Консоль получает только INFO и выше; в файл при `DEBUG` пишется всё.

---

### performance

**Назначение:** число потоков для параллельных операций.

| Ключ             | Тип    | Описание |
|------------------|--------|----------|
| `max_workers_io` | число  | Потоки для I/O: чтение CSV, подготовка к записи в Excel. Рекомендуется 8–16. |
| `max_workers_cpu` | число | Потоки для CPU: проверка длины полей, дубликатов и т.п. Обычно до числа ядер. |

**Пример:**
```json
"performance": {
  "max_workers_io": 16,
  "max_workers_cpu": 8
}
```

**Логика:** чтение файлов и разворот JSON идут в пуле с `max_workers_io`. Проверки консистентности выполняются **параллельно** в пуле с `max_workers_cpu` потоков (блокировка по листу при записи). Слишком большие значения могут замедлить из-за накладных расходов.

---

### Сортировка листов (source и main)

**Назначение:** правила сортировки задаются **в каждом элементе `input_files`** в поле **`sort_columns`** (см. выше). Отдельная секция `source_export.sort_rules` не используется. При формировании source-файла и основного Excel для каждого листа берутся колонки из `input_files[].sort_columns` для соответствующего `sheet`; применение управляется флагами `apply_sort_to_source` и `apply_sort_to_main`. На всех листах source по умолчанию включён автофильтр; ширины и закрепление — из `input_files`.

---

### tournament_status_choices

**Назначение:** подписи статусов турнира для расчётной колонки `CALC_TOURNAMENT_STATUS`. Порядок элементов строго соответствует условиям 0–6 в `calculate_tournament_status`.

| Индекс | Условие (кратко) | Типичное значение |
|--------|-------------------|-------------------|
| 0 | Нет ключевых дат | `"НЕОПРЕДЕЛЕН"` |
| 1 | Сегодня между START_DT и END_DT | `"АКТИВНЫЙ"` |
| 2 | Сегодня &lt; START_DT | `"ЗАПЛАНИРОВАН"` |
| 3–5 | Разные варианты после END_DT / RESULT_DT | `"ПОДВЕДЕНИЕ ИТОГОВ"` |
| 6 | Все конкурсы завершены | `"ЗАВЕРШЕН"` |

**Пример:**
```json
"tournament_status_choices": [
  "НЕОПРЕДЕЛЕН",
  "АКТИВНЫЙ",
  "ЗАПЛАНИРОВАН",
  "ПОДВЕДЕНИЕ ИТОГОВ",
  "ПОДВЕДЕНИЕ ИТОГОВ",
  "ПОДВЕДЕНИЕ ИТОГОВ",
  "ЗАВЕРШЕН"
]
```

**Логика:** по датам (START_DT, END_DT, RESULT_DT, MAX_CONTEST_DATE) выбирается одно из условий; в ячейку записывается строка из массива с тем же индексом. Смена формулировок (например, «ЗАВЕРШЕН» → «ЗАВЕРШЁН») делается только в конфиге.

---

### input_files

**Назначение:** список CSV-файлов и настроек листов Excel. Каждый элемент описывает один файл и один лист в итоговой книге.

| Ключ                | Тип    | Описание |
|---------------------|--------|----------|
| `file`              | строка | Имя CSV (с расширением или без). Поиск в каталоге `paths.input/subdir` без учёта регистра (.csv / .CSV). |
| `sheet`             | строка | Имя листа в выходном Excel, куда попадут данные этого файла. |
| `expected_columns`  | число  | Ожидаемое число полей в CSV. **0** — АВТО (берётся из заголовка). Если в строке CSV число полей отличается — фиксируется в отчёте и листе CONSISTENCY. |
| `subdir`            | строка | Подкаталог внутри `paths.input`, где искать файл (например `"SPOD"` или `"FILE"`). Итоговый путь: `paths.input/subdir/file`. |
| `sort_columns`      | массив | Правила сортировки листа при записи в source/main Excel (если включено `apply_sort_to_source` или `apply_sort_to_main`). Каждый элемент: `{"column": "ИмяКолонки", "order": "asc" \| "desc"}`. Применяются последовательно. Если колонки нет на листе — пропускается; если ни одной нет — сортировка не выполняется. |
| `max_col_width`     | число  | Максимальная ширина колонки (символов) при авто-ширине. |
| `freeze`            | строка | Закрепление областей, например `"C2"` — закрепить столбцы A–B и строку 1. |
| `col_width_mode`    | строка | Режим ширины: `"AUTO"` — по содержимому (ограничено max_col_width), либо число — фиксированная ширина. |
| `min_col_width`     | число  | Минимальная ширина колонки. |
| `include_in_source` | bool   | Включать ли лист в выгрузку сырых данных (файл «SPOD_PROM source …»). По умолчанию `true`. |

**Пример:**
```json
{
  "file": "CONTEST (PROM) 16-03 v0.csv",
  "sheet": "CONTEST-DATA",
  "expected_columns": 0,
  "subdir": "SPOD",
  "sort_columns": [{"column": "CONTEST_CODE", "order": "asc"}],
  "max_col_width": 120,
  "freeze": "C2",
  "col_width_mode": "AUTO",
  "min_col_width": 12,
  "include_in_source": true
}
```

**Логика:** программа перебирает `input_files`, для каждого ищет файл в `paths.input/subdir/` (например `IN/SPOD/`), читает CSV, проверяет число полей по `expected_columns` (0 = по заголовку), разворачивает JSON по `json_columns`, записывает лист с именем `sheet` и применяет ширины, закрепление и при необходимости сортировку из `sort_columns`. Результаты проверки числа полей CSV выводятся в лист CONSISTENCY.

---

### summary_sheet

**Назначение:** параметры сводного листа «SUMMARY» (имя листа, ширина колонок, закрепление).

| Ключ            | Тип    | Описание |
|-----------------|--------|----------|
| `sheet`         | строка | Имя листа (обычно `"SUMMARY"`). |
| `max_col_width` | число  | Максимальная ширина колонки. |
| `freeze`        | строка | Закрепление (например `"G2"`). |
| `col_width_mode`| строка | Режим ширины колонок. |
| `min_col_width` | число  | Минимальная ширина. |

**Логика:** сводный лист строится по правилам `merge_fields_advanced` (где `sheet_dst` = имя из `summary_sheet.sheet`), затем к нему применяются эти параметры форматирования.

---

### sheet_order

**Назначение:** порядок листов в выходном Excel. Если параметр задан (непустой массив), листы выводятся в указанном порядке; листы, не перечисленные в списке, добавляются следом в алфавитном порядке. Если параметр отсутствует или пуст — используется порядок по умолчанию: SUMMARY первым, остальные по алфавиту.

| Ключ | Тип | Описание |
|------|-----|----------|
| (массив) | строки | Имена листов в нужном порядке (например, `"SUMMARY"`, `"STAT_FILE"`, `"CONTEST-DATA"`, …). |

**Пример:**
```json
"sheet_order": ["SUMMARY", "STAT_FILE", "CONTEST-DATA", "GROUP", "INDICATOR", "REPORT", "REWARD", "REWARD-LINK", "TOURNAMENT-SCHEDULE"]
```

**Логика:** при записи в Excel сначала выводятся листы из `sheet_order`, присутствующие в `sheets_data`; затем все оставшиеся листы в отсортированном по имени порядке.

---

### summary_key_defs

**Назначение:** определение ключевых колонок по листам для построения «каркаса» сводного листа. Из этих определений программа собирает упорядоченный список всех уникальных ключей (`SUMMARY_KEY_COLUMNS`).

| Ключ   | Тип   | Описание |
|--------|--------|----------|
| `sheet`| строка | Имя листа (как в `input_files.sheet`). |
| `cols` | массив строк | Список колонок, образующих составной ключ для этого листа. |

**Пример:**
```json
{"sheet": "CONTEST-DATA", "cols": ["CONTEST_CODE"]},
{"sheet": "GROUP", "cols": ["GROUP_CODE", "CONTEST_CODE", "GROUP_VALUE"]}
```

**Логика:** из всех `cols` собирается уникальный упорядоченный список колонок. Сводный лист строится по комбинациям этих ключей, затем к строкам подтягиваются поля по правилам `merge_fields_advanced` с `sheet_dst: "SUMMARY"`.

---

### gender

**Назначение:** правила автоматического определения пола по отчеству, имени и фамилии (колонка `AUTO_GENDER` на листе EMPLOYEE).

| Ключ            | Тип  | Описание |
|-----------------|------|----------|
| `patterns`      | объект | Словарь списков окончаний для отчества, имени и фамилии (муж./жен.). |
| `progress_step` | число | Раз в сколько строк выводить прогресс в лог (например, 500). |

Ключи внутри `patterns`:

- `patronymic_male`, `patronymic_female` — окончания отчеств (например, «ович», «овна»).
- `name_male`, `name_female` — окончания имён.
- `surname_male`, `surname_female` — окончания фамилий.

**Пример (фрагмент):**
```json
"gender": {
  "patterns": {
    "patronymic_male": ["ович", "евич", "ич", "ыч", "оглы", "улы", "уулу", "заде"],
    "patronymic_female": ["овна", "евна", "инична", "ична", "на", "кызы"],
    "name_male": ["ий", "ей", "ай", "ой", "ый", "ев", "ов", "ин", "ан", "он", "ен", "ур", "ич", "ыч"],
    "name_female": ["а", "я", "ина", "ана", "ена", "ия", "ья", "на", "ла", "ра", "са", "та", "да", "ка", "га"],
    "surname_male": ["ов", "ев", "ин", "ын", "ский", "цкий", "ич", "енко", "ко", "як", "ук", "юк", "ич", "ыч"],
    "surname_female": ["ова", "ева", "ина", "ына", "ская", "цкая", "енко", "ко"]
  },
  "progress_step": 500
}
```

**Логика:** приоритет — отчество → имя → фамилия. Значение приводится к нижнему регистру и проверяется на окончание из соответствующего списка; при совпадении выставляется «М» или «Ж», иначе «-». Векторизованная версия использует те же списки.

---

### field_length_validations (удалено из config.json)

**Статус:** секция **удалена** из config.json. Проверка длины полей полностью перенесена в **consistency_checks**.

Правила задаются в `consistency_checks.rules` правилами с **`type: "field_length"`**. В каждом таком правиле указываются: `sheet`, `result_column`, **`fields`** (объект «имя поля» → `{ "limit": N, "operator": "=" | "<=" | ">=" }`), `output.column_on_sheet`. Модуль **consistency_checks** в фазе 1 создаёт на листе колонку результата (например FIELD_LENGTH_CHECK), заполняет её по тем же правилам (длина полей, оператор, лимит); в фазе 2 собирает результаты в свод CONSISTENCY. Итоговый отчёт по отклонениям длины полей в лог/консоль формируется из данных этих правил (см. раздел «Итоговая статистика»).

---

### derived_columns

**Назначение:** добавление на лист производной колонки с преобразованием (например, приведение табельного номера к 20 знакам с лидирующими нулями). Выполняется до merge, чтобы новую колонку можно было использовать в ключах правил merge_fields_advanced.

| Ключ             | Тип    | Описание |
|------------------|--------|----------|
| `sheet`          | строка | Имя листа. |
| `source_column`  | строка | Исходная колонка. |
| `target_column`  | строка | Имя новой колонки. |
| `transform`      | строка | Тип преобразования: `"pad_left"` — дополнение слева нулями до заданной длины. |
| `length`         | число  | Для pad_left — целевая длина (например, 20). |

**Пример:**
```json
"derived_columns": [
  {
    "sheet": "LIST-REWARDS",
    "source_column": "Табельный номер сотрудника",
    "target_column": "PERSON_NUMBER_20",
    "transform": "pad_left",
    "length": 20
  }
]
```

**Логика:** перед merge для каждого правила создаётся колонка `target_column` на листе `sheet`; значение берётся из `source_column` и преобразуется (pad_left — строка с лидирующими нулями). В merge в `src_key` можно указать `target_column`.

---

### merge_fields_advanced

**Назначение:** правила переноса и подсчёта полей между листами (объединение данных). Каждое правило задаёт источник, приёмник, ключи и список полей.

Основные поля одного правила:

| Ключ               | Тип    | Описание |
|--------------------|--------|----------|
| `sheet_src`        | строка | Лист-источник данных. |
| `sheet_dst`        | строка | Лист, куда добавляются поля. |
| `src_key`         | массив строк | Ключ(и) в источнике (например, `["CONTEST_CODE"]`). |
| `dst_key`         | массив строк | Ключ(и) в приёмнике (часто те же имена или составной ключ, например `["REWARD_LINK => CONTEST_CODE"]`). |
| `column`          | массив строк | Поля для переноса или имя поля для подсчёта (при `mode: "count"`). |
| `mode`            | строка | `"value"` — подтянуть значения; `"count"` — подсчитать количество совпадений. |
| `multiply_rows`   | bool   | При нескольких совпадениях по ключу: размножить строки в приёмнике (true) или взять одно значение (false). |
| `col_max_width`, `col_width_mode`, `col_min_width` | | Параметры ширины добавляемых колонок в Excel. |
| `status_filters`  | объект или null | Фильтр по полям в источнике (например, только `BUSINESS_STATUS` из списка). |
| `custom_conditions`, `group_by`, `aggregate` | | Доп. условия, группировка, агрегация (если используются). |
| `count_aggregation`| строка | Для `mode: "count"`: `"size"` или `"nunique"`. |
| `count_label`     | строка или null | Суффикс имени колонки счётчика (например, `"ACTIVE"` → колонка `COUNT_..._ACTIVE`). |
| `src_key_transforms` | объект или null | Преобразование ключей источника перед связью: колонка → строка `"pad_20"` (привести к 20 знакам с лидирующими нулями). |
| `dst_key_transforms` | объект или null | Преобразование ключей приёмника перед связью: колонка → `"pad_20"` (чтобы совпадать с форматом источника). |

**Пример (подтягивание значений):**
```json
{
  "sheet_src": "CONTEST-DATA",
  "sheet_dst": "REPORT",
  "src_key": ["CONTEST_CODE"],
  "dst_key": ["CONTEST_CODE"],
  "column": ["CONTEST_TYPE", "FULL_NAME", "BUSINESS_STATUS", "BUSINESS_BLOCK", "TARGET_TYPE", "CONTEST_FEATURE => vid"],
  "mode": "value",
  "multiply_rows": false,
  "status_filters": { "BUSINESS_STATUS": ["АКТИВНЫЙ", "ПОДВЕДЕНИЕ ИТОГОВ"] }
}
```

**Пример (подсчёт):**
```json
{
  "sheet_src": "REPORT",
  "sheet_dst": "SUMMARY",
  "src_key": ["TOURNAMENT_CODE"],
  "dst_key": ["TOURNAMENT_CODE"],
  "column": ["CONTEST_DATE"],
  "mode": "count",
  "count_aggregation": "size",
  "count_label": null
}
```

**Логика:** для каждой строки листа-приёмника по `dst_key` ищутся строки в источнике по `src_key`; при `mode: "value"` подставляются значения из `column` (при нескольких совпадениях поведение задаётся `multiply_rows`); при `mode: "count"` в приёмник записывается количество (size или nunique). Если задан `src_key_transform`, перед связью значения указанных колонок источника преобразуются (например, табельный номер — к 20 символам с лидирующими нулями), чтобы совпадать с форматом на приёмнике. Поля с префиксом вида `CONTEST_FEATURE => vid` — это развёрнутые JSON-поля (имя после `=>` — ключ внутри JSON).

**Пример (count по составному ключу с преобразованием табельного к 20 знакам):**
```json
{
  "sheet_src": "LIST-REWARDS",
  "sheet_dst": "REPORT",
  "src_key": ["Код турнира", "Табельный номер сотрудника"],
  "dst_key": ["TOURNAMENT_CODE", "MANAGER_PERSON_NUMBER"],
  "src_key_transform": {
    "Табельный номер сотрудника": { "type": "pad_left_zeros", "width": 20 }
  },
  "column": ["Код турнира"],
  "mode": "count",
  "count_aggregation": "size",
  "count_label": "REWARDS"
}
```

---

### color_scheme

**Назначение:** цветовое оформление заголовков (и при необходимости ячеек) листов Excel. Каждый элемент задаёт группу стилей, список листов и список колонок.

| Ключ         | Тип   | Описание |
|--------------|--------|----------|
| `group`      | строка | Название группы (для справки). |
| `header_bg`  | строка | Цвет фона заголовка (HEX без решётки, например `"E6F3FF"`). |
| `header_fg`  | строка | Цвет текста заголовка (HEX). |
| `column_bg`, `column_fg` | строка или null | Фон и текст для ячеек данных (если заданы). |
| `style_scope`| строка | `"header"` — стиль только для первой строки. |
| `sheets`     | массив строк | Имена листов, к которым применяется правило. |
| `columns`    | массив строк | Список колонок (заголовков). Пустой массив — все колонки листа. |

**Пример:**
```json
{
  "group": "Исходные данные",
  "header_bg": "E6F3FF",
  "header_fg": "2C3E50",
  "column_bg": null,
  "column_fg": null,
  "style_scope": "header",
  "sheets": ["CONTEST-DATA", "GROUP", "INDICATOR", "REPORT", "REWARD", "REWARD-LINK", "TOURNAMENT-SCHEDULE", "ORG_UNIT_V20", "USER_ROLE", "USER_ROLE SB", "EMPLOYEE"],
  "columns": []
}
```

**Логика:** при формировании Excel для каждого листа из `sheets` к заголовкам (и при необходимости к ячейкам) из `columns` применяется заливка и цвет шрифта. Порядок правил важен: последующие могут перекрывать предыдущие для тех же колонок.

---

### column_formats

**Назначение:** формат ячеек Excel по листам и колонкам (число, дата, выравнивание, перенос). Можно задать либо список колонок для применения формата (`columns`), либо список исключений — тогда формат применяется ко всем колонкам листа кроме указанных (`except_columns`).

Каждый элемент — объект:

| Ключ                 | Тип    | Описание |
|----------------------|--------|----------|
| `sheet`              | строка | Имя листа. |
| `columns`            | массив строк | Имена колонок, к которым применяется формат. Не задавать, если используется `except_columns`. |
| `except_columns`     | массив строк | Имена колонок-исключений: формат применяется ко всем колонкам листа, кроме перечисленных. Если задан непустой список — используется он вместо `columns`. |
| `data_type`          | строка | `"number"`, `"date"` или `"text"`. |
| `decimal_places`     | число  | Для числа — знаков после запятой (0 — целые). |
| `decimal_separator`  | строка | Разделитель дробной части: `","` или `"."`. |
| `thousands_separator`| bool   | Разделитель разрядов в целой части. |
| `date_format`        | строка | Для даты: например `"YYYY-MM-DD"` или `"DD/MM/YYYY"`. |
| `horizontal`, `vertical` | строка | Выравнивание: `"left"`/`"center"`/`"right"`, `"top"`/`"center"`/`"bottom"`. |
| `wrap_text`          | bool   | Перенос по словам. |

**Пример (формат к указанным колонкам):**
```json
{
  "sheet": "INDICATOR",
  "columns": ["N", "CALC_TYPE", "INDICATOR_WEIGHT", "INDICATOR_CALC_TYPE"],
  "data_type": "number",
  "decimal_places": 0,
  "decimal_separator": ",",
  "thousands_separator": false,
  "date_format": "YYYY-MM-DD",
  "horizontal": "center",
  "vertical": "center",
  "wrap_text": false
}
```

**Пример (формат ко всем колонкам кроме указанных):**
```json
{
  "sheet": "REPORT",
  "except_columns": ["CONTEST_CODE", "FULL_NAME"],
  "data_type": "number",
  "decimal_places": 0
}
```

**Логика:** при записи в Excel данные в выбранных колонках приводятся к числу или дате; в книге задаётся числовой/датовый формат и выравнивание. Для `decimal_places: 0` в ячейку записываются целые числа без дробной части. Для типа `date`: сначала выполняется разбор в указанном формате; для ячеек, которые не удалось распознать (NaT), выполняется повторная попытка без формата (в т.ч. даты вида 4000-01-01); значения, которые так и не удалось преобразовать в дату, остаются в виде исходной строки (текст в Excel). Таким образом любые распознаваемые даты преобразуются, нераспознанные — сохраняются как текст.

**Сопоставление имён с `except_columns` / `columns`:** имена из CSV и из конфига приводятся к одному виду (нормализация NFKC, снятие BOM `\ufeff` с первого заголовка, схлопывание пробелов). Иначе колонка могла не попасть в исключения и остаться строкой, либо наоборот.

**Числа из CSV:** после чтения все ячейки — строки; перед `to_numeric` удаляются разряды (обычный пробел, неразрывный пробел NBSP, узкий NBSP и др.), запятая в десятичной части заменяется на точку — иначе значения вида `1 234` или `1\u00a0234` не превращались бы в число и в Excel оставались бы с общим/текстовым видом.

Режим **`except_columns`** (когда формат ко всем колонкам кроме перечисленных): при ошибке преобразования типа для отдельной колонки или листа запись Excel не прерывается — лист сохраняется без преобразования проблемных ячеек (см. лог).

**Ширина колонок и пост-форматирование листа (после `to_excel`):** вызываются **`calculate_column_width`**, **`_format_sheet`**, **`apply_column_formats`**, **`apply_color_scheme`**. Параметры ширины задаются в **`input_files`**, **`summary_sheet`**, параметрах листа **STAT_FILE** и т.д.: `max_col_width`, `min_col_width`, `col_width_mode` (**`AUTO`** или **фиксированное число** / строка-число). При **AUTO** для оценки длины текста используются строка заголовка и первые **`_AUTO_COLUMN_WIDTH_MAX_DATA_ROWS`** строк данных (константа в **`src/main_impl.py`**, по умолчанию **500**); полный обход столбца не выполняется (ускорение на больших листах). Если очень длинное значение встречается только ниже этой зоны, задайте для колонки **фиксированную** ширину через `added_columns_width` / `col_width_mode`. Вспомогательная функция **`_column_indices_covered_by_column_formats`** совпадает по логике отбора столбцов с **`apply_column_formats`**: для этих столбцов общий проход выравнивания в **`_format_sheet`** не выполняется — перенос **`wrap_text`** и выравнивание берутся **только из правил** `column_formats`; для остальных столбцов сохраняется общий стиль данных с переносом по словам.

---

### reward_getcondition_summary

**Назначение:** после **merge_fields_advanced** на лист **REWARD** добавляется одна колонка со сводным текстом по всем `getCondition.nonRewards[i].nonRewardCode` и `getCondition.rewards[j].rewardCode` из развёрнутого JSON (префикс как в `json_columns`, обычно `ADD_DATA`). Для каждого непустого кода подставляются **FULL_NAME** и **seasonItem** той награды, у которой `REWARD_CODE` совпадает с кодом (справочник по всему листу REWARD; колонка `… => seasonItem` ищется с учётом пробелов вокруг `=>`). Строки в ячейке разделяются переводом строки `\\n`, формат как в Excel: `[код] наименование {сезон}`.

**Имя в Excel:** в файле и в заголовках листа **REWARD** используется значение **`column_name`** из конфига (например «Сводка: nonRewards и rewards (getCondition)»). Отдельного столбца с именем `reward_getcondition_summary` нет — это только имя модуля в коде.

**Поведение:** при `enabled: true` колонка **всегда** добавляется в конец набора столбцов. Если подходящие поля `getCondition` не распознаны (неверный префикс или нет разворота JSON), колонка будет пустой; в лог пишется предупреждение. Сопоставление имён полей допускает **гибкие пробелы** вокруг `=>` в заголовках.

| Ключ | Тип | Описание |
|------|-----|----------|
| `enabled` | bool | `false` — не добавлять колонку. |
| `column_name` | строка | Имя колонки в Excel (по умолчанию см. config.json). |

Реализация: **src/reward_getcondition_summary.py**.

---

### rating_item_matrix

**Назначение:** после **merge_fields_advanced** на лист **RATING** добавляются столбцы по каждой награде с **REWARD_TYPE = ITEM** (имя столбца — **`REWARD_CODE`**, при необходимости с **`seasonCode`**; уникализация имён при дубликатах). В ячейке — число заказов на объединённом листе **ORDER** с тем же табельным номером и кодом товара (аналог **СЧЁТЕСЛИМН**); нулевые значения в Excel не выводятся (**NaN**).

**Подсветка** выполняется **после** записи основного Excel (**openpyxl**): каждая ячейка матрицы получает **светло-зелёную** заливку, если товар **доступен** менеджеру по **всем** условиям из JSON **`REWARD_ADD_DATA`**, и **светло-красную**, если **хотя бы одно** условие не выполнено. Условия:

1. **`minRatingBANK` / `minRatingTB` / `minRatingGOSB`** из **`employeeRating`**: учитываются только если значение **> 0**; тогда соответствующее **место в рейтинге** на RATING («Место в рейтинге по стране» / ТБ / ГОСБ) должно быть **≤** порога (меньшее число места = лучше).
2. **`minCrystalEarnedTotal`**: если **> 0**, значение колонки **«Количество кристаллов»** на RATING должно быть **≥** порога; при **0** или отсутствии в JSON критерий не действует.
3. **`getCondition` → `rewards`**: список **`rewardCode`**; если не пуст, **все** перечисленные коды должны встречаться на листе **LIST-REWARDS** для того же табельного (**«Табельный номер сотрудника»** + **«Код награды»** — имена настраиваются).
4. **`getCondition` → `nonRewards`**: список **`nonRewardCode`**; если не пуст, **ни один** из кодов не должен встречаться в заказах сотрудника на **ORDER** (**«Табельный номер»** + **«Код товара»**).
5. **`ignoreConditions`** (корень **`REWARD_ADD_DATA`**, массив табельных номеров): если **табельный номер строки RATING** входит в этот список для данного кода товара (**ITEM**), ячейка матрицы считается **доступной** (**светло-зелёный**, **`fill_accessibility_ok`**) **независимо** от пунктов 1–4.

Каталог правил по коду товара строится модулем **`src/reward_item_catalog.py`** (разбор **`REWARD_ADD_DATA`** с нормализацией `"""` → `"` и снятием внешних кавычек). Если для кода нет записи в JSON, подставляются плоские пороги **`minRating*`** из развёрнутых колонок **REWARD** (как при сборе спецификаций матрицы). Трёхцветная подсветка только по **`minRating*`** (страна/ТБ/ГОСБ) **не используется**.

| Ключ | Тип | Описание |
|------|-----|----------|
| `enabled` | bool | `false` — шаг отключён. |
| `sheet_rating` / `sheet_order` / `sheet_reward` | строка | Листы RATING, ORDER, REWARD. |
| `order_employee_col` / `order_product_col` | строка или список | Табельный и код товара на ORDER (есть запасные имена в коде). |
| `rating_employee_col` | строка или список | Табельный на RATING. |
| `country_rank_col` / `tb_rank_col` / `gosb_rank_col` | строка или список | Места в рейтинге (страна, ТБ, ГОСБ). |
| `reward_type_col` / `reward_code_col` | строка или список | На REWARD: тип и код награды. |
| `col_season_code` / `col_min_rating_bank` / `col_min_rating_tb` / `col_min_rating_gosb` | строка | Точные имена развёрнутых колонок или поиск по фрагментам. |
| `reward_add_data_col` | строка | Имя сырой JSON-колонки (обычно **`REWARD_ADD_DATA`**). |
| `sheet_list_rewards` | строка | Лист **LIST-REWARDS** (награды сотрудника). |
| `list_rewards_employee_col` / `list_rewards_code_col` | строка или список | Табельный и код награды на LIST-REWARDS. |
| `crystals_col` | строка или список | Колонка кристаллов на RATING. |
| `fill_accessibility_ok` / `fill_accessibility_fail` | строка (ARGB без `#`) | Цвета заливки доступен / недоступен (по умолчанию **C6EFCE** / **FFC7CE**). |

Реализация: **`src/rating_item_matrix.py`** (**`apply_rating_item_matrix_enrichment`**, **`apply_rating_item_matrix_colors`**), **`src/reward_item_catalog.py`**.

---

### Каталог POST (перенос кода без репозитория)

**Назначение:** локальная папка **POST/** с копиями **`main.py`**, **`config.json`** и всех **`src/**/*.py`**, **кроме** **`src/Tools/`** и **`src/Tests/`**, чтобы перенести программу на ПК без Git. К имени каждого такого файла добавлен суффикс **`.txt`** (**`main.py.txt`**, **`config.json.txt`**, **`src/main_impl.py.txt`** и т.д.) — для обхода ограничений почты и вложений.

**Каталог POST/ целиком в `.gitignore`** — в репозиторий не попадает; на каждой машине разработчика содержимое создаётся скриптом.

**Обновление:** из корня проекта **`python src/Tools/sync_post_txt.py`**. Скрипт **полностью удаляет** прежний **POST/** и создаёт заново: копирует **`main.py`**, **`config.json`** и все **`src/**/*.py`** (кроме **`src/Tools/`**, **`src/Tests/`**) с суффиксом **`.txt`** в имени файла (**`main.py.txt`**, **`config.json.txt`**, **`src/…/модуль.py.txt`**), затем копирует из **`Docs/POST_SNAPSHOT/`** без переименования **`КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt`** и **`restore_names_from_txt.bat`**. Актуальный **перечень имён** файлов с **`.txt`** приведён в **`Docs/POST_SNAPSHOT/КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt`** (раздел «Перечень файлов программы»); при появлении новых модулей в **`src/`** пересоберите POST скриптом и при необходимости обновите этот раздел в репозитории.

**Не копируются:** **`README.md`**, **`requirements.txt`**, каталог **`Docs/`**. На целевом ПК **`requirements.txt`** нужно взять из клона репозитория или установить зависимости вручную по списку в этом README (**pandas**, **openpyxl** и др.).

**История:** ранее в POST входили также README и requirements; с версии **1.7.36** — только Python и **config.json** (плюс служебные файлы из **Docs/POST_SNAPSHOT/**).

---

### check_duplicates (удалено из config.json)

**Статус:** секция **удалена** из config.json. Проверка дубликатов полностью перенесена в **consistency_checks**.

Правила задаются в `consistency_checks.rules` правилами с **`type: "unique"`**. В каждом правиле: `sheet`, **`key_columns`** (массив колонок ключа), `output.column_on_sheet` (например «ДУБЛЬ: CONTEST_CODE_GROUP_CODE_REWARD_CODE»). Модуль **consistency_checks** в фазе 1 создаёт на листе колонку с именем из `output.column_on_sheet`, заполняет её признаками дублей (пусто или «xN»); в фазе 2 собирает результаты в свод CONSISTENCY. Итоговая статистика по дубликатам отображается в листе CONSISTENCY и в логе проверок консистентности.

---

### consistency_checks

**Назначение:** единый конфиг **всех** проверок консистентности данных. Выполнение — в модуле **src/consistency_checks.py** (после merge, до формирования SUMMARY). Секции `check_duplicates` и `field_length_validations` в config.json **удалены**: правила уникальности и проверки длины полей задаются только здесь.

| Ключ | Тип | Описание |
|------|-----|----------|
| `summary_sheet_name` | строка | Имя сводного листа (по умолчанию `"CONSISTENCY"`). |
| `rules` | массив объектов | Список правил; у каждого: **`id`** (смысловой идентификатор, например **`ref_group_contest_code_in_contest_data`**), `name`, `type`, `enabled`, `output`. Остальные поля зависят от типа. |
| `spod_todo_config_guide` | строка (опц.) | Человекочитаемая подсказка: где в **rules** искать **`json_spod_format`** и поля фильтра referential. |
| `csv_columns_count` | объект | Ожидаемое число полей CSV по листам и тексты колонок свода (как раньше). |

**Загрузка конфига:** в **`Config`** и в **`main_impl`** в объект **`consistency_checks`** попадают также **прочие** ключи из JSON (не только три перечисленных выше), чтобы не терять подсказки.

**Порядок выполнения:** **Фаза 1** — **`unique`**, **`field_length`**, **`field_format`**, **`json_field_equals_column`**, **`json_field_in_column`**, **`json_priority_unique_per_contest_link`** (создание колонок на листах). **Фаза 2** — **`referential`**, **`referential_composite`**, **`json_spod_format`**, сбор результатов фазы 1 в свод. Правила с **`enabled: false`** в фазах не участвуют; для них формируется только строка свода (**см. ниже**).

**Типы правил:**

- **referential** — внешний ключ в одну колонку: значения `column_src` на `sheet_src` должны присутствовать в `sheet_ref.column_ref`. Поля: `sheet_src`, `column_src`, `sheet_ref`, `column_ref`. Опционально **`src_row_conditions`** и **`ref_row_conditions`** (или **`sheet_src_row_conditions`** / **`sheet_ref_row_conditions`**): массив **`{ "column", "op", "value" }`**, **`op`** = **`=`** / **`==`** / **`eq`** или **`<>`** / **`!=`** / **`ne`**; условия объединяются по **И**. Строки источника вне фильтра получают в колонке результата **«—»** и не считаются нарушениями; множество допустимых значений справочника строится по строкам ref, прошедшим **`ref_row_conditions`**. Результат: «OK», «НЕТ в …» или «—».
- **referential_composite** — внешний ключ из нескольких колонок; те же опциональные **`src_row_conditions`** / **`ref_row_conditions`**, семантика как у **referential**.
- **unique** — уникальность комбинации колонок **`key_columns`** на листе **`sheet`**; колонка результата из **`output.column_on_sheet`** (значения **пусто** или **xN**). Поддерживаются **область строк** и **обязательная непустота** части колонок; подробный сценарий и шаблон конфига — в подразделе **«Правило unique»** ниже. В фазе 2 результат собирается в свод.
- **field_length** — проверка длины полей. В фазе 1 модуль **создаёт** колонку результата на листе по полям `sheet`, `result_column`, **`fields`** (объект: имя поля → `{ "limit": N, "operator": "=" | "<=" | ">=" }`), `output.column_on_sheet`. В ячейках: «-» или строка с описанием нарушений. В фазе 2 результат собирается в свод.
- **field_format** — проверка формата поля (дата, десятичное число с фиксированной дробной частью, строка из N цифр). В фазе 1 создаётся колонка результата на листе по полям `sheet`, `field`, **`format`** (type: date/decimal/fixed_length_digits + параметры). В фазе 2 результат собирается в свод.
- **json_field_equals_column** — значение ключа из JSON-колонки сравнивается с колонкой листа. Применяется к полям вроде REWARD_ADD_DATA (JSON с тройными кавычками). Поля: `sheet`, **`json_column`** (например REWARD_ADD_DATA), **`json_key`** (например parentRewardCode), **`column_compare`** (например REWARD_CODE). Опционально: **`filter_column`** + **`filter_value`** (только строки, где значение колонки совпадает, например REWARD_TYPE=BADGE), **`json_filter_key`** + **`json_filter_value`** (доп. условие по ключу в JSON, например masterBadge=Y). При **`must_not_equal`: true** требование инвертируется: значение из JSON **не должно** равняться колонке (для BADGE с masterBadge=N: parentRewardCode ≠ REWARD_CODE). Парсинг JSON: тройные кавычки `"""` заменяются на `"`, затем `json.loads`. В ячейке: OK / сообщение об ошибке / пусто для неприменимых строк.
- **json_field_in_column** — все уникальные значения ключа из JSON-колонки должны присутствовать в указанной колонке того же листа. Поля: `sheet`, **`json_column`**, **`json_key`**, **`column_in_sheet`** (например REWARD_CODE). Используется для проверки: все parentRewardCode из ADD_DATA должны быть в REWARD_CODE.
- **json_priority_unique_per_contest_link** — для каждого **`CONTEST_CODE`** на листе **`link_sheet`** (по умолчанию REWARD-LINK) берутся **уникальные `REWARD_CODE`** (**`GROUP_CODE` не учитывается**). На листе **`sheet`** (REWARD) в колонке **`json_column`** (REWARD_ADD_DATA) из JSON читается ключ **`json_key`** (по умолчанию `priority`); разбор тот же, что у **json_field_*** — через **`_parse_add_data_cell_with_normalized`**. В рамках одного конкурса: если у **всех** привязанных наград поле отсутствует — **не нарушение**; если у **всех** задано — значения должны быть **попарно различны**; если **часть с полем, часть без** — **нарушение** для всех строк этой группы. Результат пишется в колонку на **REWARD**. Поля: `sheet`, `reward_code_column`, `json_column`, `json_key`, `link_sheet`, `link_contest_column`, `link_reward_column`, `output`.
- **json_spod_format** — проверка ячейки как строки **SPOD-JSON**: предобработка (BOM, пробелы вне тройных строк), симметрия внешних кавычек; разбор структуры (ключи **`"""…"""`**, значения — **`"""…"""`** / **`{}`** / **`[]`** по правилам, **`numeric_value_keys`** без кавычек); явные подсказки по типичным ошибкам (**`""key""`**, значение `"…"` вместо **`"""…"""`**, **`[{"""…"""}]`** вместо **`["""…"""]`**); затем **`"""` → `"`**, **json.loads** и проверка типов числовых полей. Поля: **`sheet`**, **`json_column`**, **`json_required`**, **`numeric_value_keys`**, **`output`**. Реализация: **`src/json_spod_format_check.py`**. В **config.json** — правила **`spod_json_`** (CONTEST-DATA, REWARD, TOURNAMENT-SCHEDULE, INDICATOR). Подробности — **`Docs/CONSISTENCY_CHECKS_FORMAT.md`**, п. **2.8**.

**Правило с `enabled: false`:** проверка **не** выполняется, колонка на листе **не** заполняется; в своде **CONSISTENCY** при **`include_in_summary: true`** всё равно есть строка: **`total_rows`** по целевому листу, **`violations: 0`**, в **`sample`** — пояснение, что правило отключено.

### Правило `unique`: область строк, непустота и единый шаблон в config.json

**Обязательные поля правила:** `sheet`, `key_columns`, `output` (в т.ч. `column_on_sheet`).

**Рекомендуемый единый набор полей** (присутствует **во всех** правилах `unique` в проектном **config.json**): сразу после `key_columns` задаются:

| Поле | Назначение |
|------|------------|
| **`unique_scope_mode`** | **`all`** — условия области объединяются по **И**. Значения **`any`**, **`or`**, **`или`** — по **ИЛИ** (достаточно совпадения одной пары column/value). |
| **`unique_scope_conditions`** | Массив объектов **`{ "column": "…", "value": "…" }`**. Сравнение: нормализованная строка ячейки (`strip`) с **`str(value).strip()`**; для `NaN`/пустой ячейки слева получается пустая строка. **Пустой массив `[]`** — **нет фильтра по области**, участвуют все строки (с учётом `unique_require_non_empty`). |
| **`unique_scope_column`**, **`unique_scope_value`** | Устаревшая **одна** пара; учитывается только если **`unique_scope_conditions`** пуст или отсутствует (эквивалент одного элемента в массиве, режим **И**). В шаблоне проекта для новых правил обычно **`""`** / **`""`**. |
| **`unique_require_non_empty`** | Список имён колонок. Строка **исключается** из проверки, если **хотя бы одна** из перечисленных колонок считается пустой: `NaN`, пустая строка после `strip`, литералы **`"-"`**, **`"None"`**, **`"null"`** (как при отборе «пустых» в духе проверок длины). **Пустой массив `[]`** — ограничение не действует. |

**Как формируется результат по строкам (фаза 1, `consistency_checks`):**

1. Вычисляется маска **активных** строк: выполнена **область** (`unique_scope_*`) **и** для всех колонок из **`unique_require_non_empty`** значения непустые.
2. Для **неактивных** строк в колонке «ДУБЛЬ: …» остаётся **пусто** — это не «ошибка» и не дубль: правило к строке **не применялось**.
3. Только для **активных** строк выполняется **`groupby(key_columns)`** и подсчёт числа строк с одинаковым ключом; при числе **> 1** в ячейку пишется **`xN`**.
4. В сводном листе **CONSISTENCY** для этого правила поле **`total_rows`** — количество **активных** строк (а не всех строк листа).

**Пример смысловой:** правило **`unique_employee_kpk_gosb`** — уникальность тройки **POSITION_NAME, KPK_CODE, ORG_UNIT_CODE** только среди строк, где **POSITION_NAME** = **КПК**, и при **непустом** **KPK_CODE**; остальные строки EMPLOYEE в этой колонке проверки пустые.

**Реализация в коде** (`src/consistency_checks.py`): **`_normalize_unique_scope_conditions`**, **`_unique_scope_mode`**, **`_unique_scope_mask`**, **`_unique_require_non_empty_mask`**, **`_unique_active_row_mask`**, **`_unique_cell_is_empty`**, логика в **`_run_unique_check`** и **`collect_unique_result`**.

**Парсинг ADD_DATA (REWARD и др.):** в модуле **consistency_checks** функция **`_parse_add_data_cell(val)`** разбирает ячейку: `str(val).replace('"""', '"')`, затем `json.loads(normalized)`. Возвращает `dict` или `None` при ошибке. Так обрабатываются поля с тройными кавычками в CSV. Проверки **json_field_equals_column**, **json_field_in_column** и **json_priority_unique_per_contest_link** используют тот же разбор (**`_parse_add_data_cell_with_normalized`** / **`_parse_add_data_cell`**), дублирования логики парсинга нет.

**Проверки на сырых данных:** все правила консистентности (в т.ч. json_field_equals_column, json_field_in_column и json_priority_unique_per_contest_link) выполняются по **сырым** данным до обработки (до merge, до разворота JSON и т.д.); результаты затем копируются на обработанные листы. Число полей в CSV для отчёта берётся до добавления колонок проверок (см. **csv_columns_count**).

**csv_columns_count** (внутри `consistency_checks`): список листов и ожидаемое число полей в CSV (0 = АВТО по заголовку), плюс тексты для колонок листа CONSISTENCY (ТИП ПРОВЕРКИ, Описание, таблица источник, поле источник, параметр сравнения, комментарий). Секция **`_default`** задаёт подписи по умолчанию; **`sheets`** — объект «имя листа» → `{ "expected_columns": 0, опционально переопределение текстов }`. Записи по расхождениям числа полей добавляются в свод CONSISTENCY с заполненными колонками описания. Колонка **sample** на листе CONSISTENCY для этой проверки заполняется **только при наличии отклонений** (номера строк и ожид./факт. число полей); при отсутствии отклонений sample остаётся пустой.

**Вывод:** для каждого правила с **`include_in_summary: true`** — строка в сводном листе **CONSISTENCY**, **включая отключённые** (**`enabled: false`**): см. выше. Колонки свода — как раньше (**ТИП ПРОВЕРКИ**, …, **check_id**, **violations**, **sample**). В **лог** — подробный отчёт; в **консоль** — **`console_ui.print_consistency_summary`**.

**Идентификаторы базовых проверок** (вместо коротких **`1.1`**, **`2`**, …): **`ref_group_contest_code_in_contest_data`**, **`ref_indicator_contest_code_in_contest_data`**, **`ref_reward_link_contest_code_in_contest_data`**, **`ref_reward_link_reward_code_in_reward`**, **`ref_employee_org_unit_code_in_org_unit_v20`**, **`unique_group_contest_code_group_code_group_value`**, **`unique_reward_link_contest_code_group_code_reward_code`**, **`ref_composite_reward_link_pair_in_group`** — см. таблицу в **`Docs/CONSISTENCY_CHECKS_FORMAT.md`** (п. 6).

**Пример фрагмента rules (referential и unique):**
```json
{
  "id": "ref_group_contest_code_in_contest_data",
  "name": "CONTEST_CODE из GROUP в CONTEST-DATA",
  "type": "referential",
  "enabled": true,
  "sheet_src": "GROUP",
  "column_src": "CONTEST_CODE",
  "sheet_ref": "CONTEST-DATA",
  "column_ref": "CONTEST_CODE",
  "src_row_conditions": [],
  "ref_row_conditions": [],
  "output": { "column_on_sheet": "ПРОВЕРКА: CONTEST_CODE в CONTEST-DATA", "include_in_summary": true }
},
{
  "id": "unique_reward_link_contest_code_group_code_reward_code",
  "name": "Уникальность CONTEST_CODE+GROUP_CODE+REWARD_CODE в REWARD-LINK",
  "type": "unique",
  "enabled": true,
  "sheet": "REWARD-LINK",
  "key_columns": ["CONTEST_CODE", "GROUP_CODE", "REWARD_CODE"],
  "unique_scope_mode": "all",
  "unique_scope_conditions": [],
  "unique_scope_column": "",
  "unique_scope_value": "",
  "unique_require_non_empty": [],
  "output": { "column_on_sheet": "ДУБЛЬ: CONTEST_CODE_GROUP_CODE_REWARD_CODE", "include_in_summary": true }
}
```

Полное описание формата и соответствия пунктам ПРОВЕРКИ.txt — в **Docs/CONSISTENCY_CHECKS_FORMAT.md** (в т.ч. **2.7** — **json_priority_unique_per_contest_link**, **2.8** — **json_spod_format**, фильтры строк в **2.2**).

---

### json_columns

**Назначение:** какие колонки в каких листах считать JSON и разворачивать в отдельные колонки. Задаётся по имени листа.

Структура: объект, ключи — имена листов. Значение — массив объектов:

| Ключ    | Тип   | Описание |
|---------|--------|----------|
| `column`| строка | Имя колонки с JSON-строкой. |
| `prefix`| строка | Префикс для имён новых колонок (например, `"CONTEST_FEATURE"` или `"ADD_DATA"`). Вложенные ключи дают имена вида `prefix => key` или `prefix => key => nested`. |

**Пример:**
```json
"json_columns": {
  "CONTEST-DATA": [
    { "column": "CONTEST_FEATURE", "prefix": "CONTEST_FEATURE" }
  ],
  "REWARD": [
    { "column": "REWARD_ADD_DATA", "prefix": "ADD_DATA" }
  ]
}
```

**Логика:** при загрузке листа каждая указанная колонка парсится как JSON; ключи верхнего уровня становятся колонками `prefix => key`; вложенные объекты/массивы разворачиваются рекурсивно с тем же префиксом. Исходная колонка сохраняется; при необходимости тройные кавычки в исходных данных обрабатываются отдельно (CONTEST_FEATURE).

После разворота JSON и merge выполняется опциональная **сводка getCondition** (см. **reward_getcondition_summary**).

---

## Программа main.py - Обработка данных

### Назначение

Основная программа для обработки данных из CSV файлов системы SPOD. Состоит из двух частей:

1. **Корневой main.py** — точка входа: создаёт экземпляр `Config` (загрузка **config.json** из корня проекта), передаёт его в `config_holder`, затем вызывает `main_impl.main()`.
2. **src/main_impl.py** — полный пайплайн: при запуске подхватывает внедрённый конфиг (или при прямом запуске загружает config.json), настраивает логирование, читает CSV из `paths.input`, обрабатывает данные по правилам объединения (`merge_fields_advanced`), запускает **проверки консистентности** (модуль `consistency_checks`: создание колонок unique и field_length, referential/referential_composite, свод CONSISTENCY), формирует сводный лист SUMMARY и лист STAT_FILE, записывает итоговый Excel и формирует отчёт по отклонениям длины полей и расхождениям CSV. Отдельных шагов «проверка дубликатов» и «валидация длины полей» нет — всё в рамках consistency_checks. **Подробный** ход (**INFO/DEBUG**) — в **лог-файл**; в **консоль** — краткие этапы, сводки и таблицы через **`console_ui`** (см. раздел [Логирование](#логирование) и п. 8 ниже).

### Входные данные

Программа работает с CSV-файлами, перечисленными в `config.json` в секции **input_files** (имя файла и листа для каждого источника). Примеры листов:

1. **CONTEST-DATA** - Данные о конкурсах
2. **EMPLOYEE** - Данные о сотрудниках
3. **GROUP** - Группы
4. **INDICATOR** - Индикаторы
5. **ORG_UNIT_V20** - Организационные единицы
6. **REPORT** - Отчеты
7. **REWARD** - Награды
8. **REWARD-LINK** - Связи наград
9. **TOURNAMENT-SCHEDULE** - Расписание турниров
10. **TOURNAMENT-SCHEDULE-LINK** - Связи расписания
11. **TOURNAMENT-SCHEDULE-LINK_CONTEST** - Связи расписания с конкурсами

### Основная логика работы

#### 1. Инициализация и настройка

При запуске из корня (`python main.py`):

```python
# main.py (корень)
config = Config()                    # Загрузка config.json из корня проекта
set_current_config(config)           # Внедрение для main_impl
main_impl.main()                     # Запуск пайплайна
```

Внутри `main_impl.main()` сначала вызывается `_load_config_globals()` — глобальные переменные (DIR_INPUT, INPUT_FILES, MERGE_FIELDS_ADVANCED, SOURCE_EXPORT_SORT и т.д.) заполняются из внедрённого Config. Затем настраивается логирование (`setup_logger()` — использует `logging.level` и `logging.base_name` из конфига): в **файл** — уровень из конфига (как правило **DEBUG**); для **консоли** в **main_impl** уровень поднимается до **WARNING**, чтобы не дублировать длинный **INFO** в терминал — ход работы показывает **`console_ui`**.

#### 2. Чтение CSV файлов

Чтение выполняется в **main_impl**: поиск файла без учёта регистра (`find_file_case_insensitive`), чтение CSV с разделителем `;`, кодировка UTF-8, все ячейки как строки. Функция `read_csv_file(file_path)` возвращает `(DataFrame, список_расхождений)` или `None` при ошибке. Строки с числом полей, отличным от числа колонок в заголовке, нормализуются (дополняются пустыми значениями или обрезаются); при этом каждая такая строка фиксируется для итогового отчёта (номер строки в файле, ожидаемое/фактическое число полей, направление — «больше»/«меньше»). Аналогичная логика чтения и нормализации вынесена в класс **FileLoader** в `src/file_loader.py` (методы `read_csv_file`, `process_single_file` с разворотом JSON по `json_columns`).

**Особенности:**
- Поиск файла в каталоге `paths.input` по имени без учёта регистра (.csv / .CSV).
- Для LIST-TOURNAMENT при отсутствии файла с суффиксом `-2` используется альтернативное имя без суффикса.
- Нормализация длины строк к длине заголовка предотвращает ошибку загрузки при разном числе полей в строках CSV; расхождения выводятся в итоговой статистике.

#### 2.1. Выгрузка сырых данных (source Excel)

Сразу после загрузки CSV (до разворота JSON, валидации, merge и доп. колонок) формируется отдельный файл с именем из `output_filenames.source` и меткой времени (например **«SPOD_PROM source 2026-03-17_12-00-00.xlsx»**). Файл записывается в подкаталог по дате: **`paths.output/YYYY/DD-MM/`** (например `OUT/2026/17-03/`). В него попадают только листы с `include_in_source: true`. Данные записываются в виде CSV без разворота JSON. Перед записью к листам применяется сортировка по `input_files[].sort_columns`, если включено `apply_sort_to_source`. Ширина колонок и закрепление берутся из `input_files`; на всех листах source включён автофильтр. Проверка наличия обязательных файлов выполняется **после** выгрузки в режиме 2; в режимах 1 и 4 при отсутствии файлов программа завершается с ошибкой после записи source.

#### 3. Обработка данных

##### 3.1. Объединение данных (merge_fields_advanced)

Программа объединяет данные из разных файлов по правилам из **config.json** → `merge_fields_advanced`. Каждое правило задаёт источник, приёмник, ключи и поля (подробнее см. раздел [Конфигурация config.json](#конфигурация-configjson) → merge_fields_advanced).

```json
{
  "sheet_src": "SOURCE_SHEET",
  "sheet_dst": "DEST_SHEET",
  "src_key": ["SOURCE_KEY"],
  "dst_key": ["DEST_KEY"],
  "column": ["FIELD1", "FIELD2"],
  "mode": "value",
  "multiply_rows": false
}
```

**Режимы объединения:**
- `merge` - Объединение полей (добавление новых колонок)
- `count` - Подсчет количества записей
- `sum` - Суммирование значений

##### 3.2. Проверки консистентности (consistency_checks)

После merge выполняется модуль **consistency_checks** (с параллелизацией: пул потоков, блокировка по листу при записи). Создаются колонки **unique** / **field_length** / **field_format** / **json_field_***; выполняются **referential** / **referential_composite** (с опциональными фильтрами строк) и **json_spod_format**; свод **CONSISTENCY** включает и правила с **`enabled: false`** (без выполнения проверки). **Подробности** — в **лог**, **краткая сводка** — в **консоль** (`console_ui.print_consistency_summary`). Типы правил см. раздел **consistency_checks** выше. Секции `check_duplicates` и `field_length_validations` в config не используются.

##### 3.3. Обработка JSON полей

Некоторые поля содержат JSON данные:
- `CONTEST_FEATURE` (CONTEST-DATA)
- `CONTEST_PERIOD` (CONTEST-DATA)
- `BUSINESS_BLOCK` (CONTEST-DATA)
- `TARGET_TYPE` (TOURNAMENT-SCHEDULE)
- `FILTER_PERIOD_ARR` (TOURNAMENT-SCHEDULE)
- `INDICATOR_FILTER` (INDICATOR)
- `GROUP_VALUE` (GROUP)
- `REWARD_ADD_DATA` (REWARD)

Эти поля обрабатываются как строки и сохраняются в исходном виде.

#### 4. Запись в Excel

```python
def write_to_excel(sheets_data, output_path):
    # Создает Excel файл с несколькими листами
    # Сохраняет порядок листов
    # Применяет цветовую схему для дубликатов
```

**Структура Excel файла:**
- Каждый исходный файл → отдельный лист
- Листы упорядочены согласно `ordered_sheets`
- Применяется цветовая схема для дубликатов
- Форматирование листов (`_format_sheet`): при **AUTO**-ширине колонок учитываются заголовок и первые 500 строк данных (полный скан столбца отключён ради скорости); фиксированная ширина из конфига без изменений. Перенос по словам: заголовки и общий стиль данных с переносом сохраняются; столбцы из **column_formats** получают выравнивание и **wrap_text** только из правил.

#### 5. Итоговая статистика (отклонения длины полей, расхождения по числу полей в CSV)

После записи в Excel программа формирует отчёт (без прерывания работы):

- **В лог (INFO)** — полный многострочный блок: **отклонения по длине полей** (по правилам `consistency_checks` с типом `field_length`) — лист, колонка результата, число строк, примеры (до 10); **расхождения по числу полей в CSV** — файл, лист, номер строки, ожидаемое/фактическое число полей, направление.
- **В консоль** — компактно через **`console_ui.print_validation_and_csv_compact`** (без длинных примеров).

Дубликаты отображаются в сводном листе **CONSISTENCY** и в логе проверок консистентности (не в отдельном блоке итоговой статистики).

Функции: `collect_duplicates_and_validation_report(sheets_data)` — возвращает `(validation_report, csv_mismatch_report)`; `print_final_report(...)` — **только в лог**; консоль дополняется **`console_ui`**.

#### 6. Лист STAT_FILE (статистика по файлам)

После формирования SUMMARY программа создаёт лист **STAT_FILE** с общей статистикой по исходным CSV-файлам. Для каждого файла из `input_files` выводится: имя файла (`FILE_NAME`), имя листа (`SHEET_NAME`), дата изменения файла (`FILE_DATE`), дата обработки (`DATA_UPDATE_DATE`), количество записей (`ROW_COUNT`), количество колонок (`COL_COUNT`), размер файла в байтах (`FILE_SIZE_BYTES`), статус загрузки (`STATUS`: OK / не найден). Лист добавляется в `sheets_data` и выводится в Excel в порядке, заданном в `sheet_order` (по умолчанию — сразу после SUMMARY).

#### 7. Отдельный файл `STAT_FILE YYYY-MM-DD_HH-MM-SS.xlsx` (время этапов и функций)

В том же выходном каталоге по дате (`OUT/YYYY/DD-MM/`), что и основной Excel, создаётся **отдельная книга** с именем **`STAT_FILE <таймштамп>.xlsx`**: листы **«Сводка»** (режим, старт, общее время прогона `perf_counter`), **«Этапы»** (все блоки `debug_phase` с длительностью и смещением от начала прогона), **«Функции»** (агрегаты `@debug_timed`: вызовы, сумма/среднее/min/max, доля от общего времени, признак `hot`). Длительности в колонках для человека задаются в формате **`ХХ мин. YY сек ZZZ мс`**; рядом дублируются значения в секундах для сортировки и фильтров. Файл формируется в конце успешного прогона (в т.ч. в режимах **source_only** и **consistency_only**).

#### 8. Краткий вывод в консоль (`src/console_ui.py`)

Показывает, что программа **не зависла**: баннер старта; при входе/выходе из **`debug_phase`** — строки «… этап» и «`[NN] ✓` время краткое_имя»; опционально полоса **`[###-----] done/total`** (число шагов — **`expected_phases_for_run_flags`**, задаётся до первой фазы чтения CSV). Сводка **консистентности** (**`print_consistency_summary`**): оценка «правил в отчёте» и «листов с колонками проверок», явное сообщение **есть/нет проблем**, **таблица по типам** (тип, число правил, число листов, сумма нарушений, примечание OK/есть); при нарушениях — блок по правилам с нарушениями; компактно **длина полей и CSV**; в конце прогона — **таблица обработки данных**: строка «файлов / сумма строк», затем колонки **«Лист»** и **«Строки / примечание»** по **всем** листам из внутреннего `summary` **без усечения** текста (разбор строк вида `ИМЯ: 200 строк` — **`_split_sheet_summary_line`**); далее таблица **этапов по времени**, **топ функций** `@debug_timed`, пути к **Excel** и **логу**, **wall-clock**. Критические сообщения (нет файлов) — **`stderr_message`**. Реализация на стандартной библиотеке Python.

### Логирование

Программа разделяет потоки вывода **main**:

1. **Файл лога** (уровень из **config.json** → `logging.level`, обычно **DEBUG**):
   - Полный ход: чтение/запись, этапы, **INFO**-итоги, **DEBUG**-детали (в т.ч. `[PERF]`, прогресс длины полей).
   - Формат: `дата время - [DEBUG] - сообщение [class: ClassName | def: function_name]` (см. `logging_setup`).

2. **Консоль (stdout)** для **main_impl**:
   - Обработчик логгера — **WARNING** и выше (предупреждения и ошибки из `logging`).
   - Обычный **INFO** в терминал **не** дублируется; вместо этого — модуль **`console_ui`** (см. п. 8).

**Именование логов:**
- Формат: `LOGS_DEBUG_YYYYMMDD_HH_MM.log`
- Расположение: `LOGS/YYYY/DD-MM/` (подпапки по дате, как для OUT)

---

## Техническое задание

### Требования к main.py

1. **Чтение данных:**
   - Поддержка CSV с разделителем ";"
   - Кодировка UTF-8
   - Автоматическое определение файлов по дате

2. **Обработка данных:**
   - Объединение данных по правилам `MERGE_FIELDS`
   - Проверка дубликатов по правилам `CHECK_DUPLICATES`
   - Сохранение исходного порядка колонок

3. **Запись результатов:**
   - Excel файл с несколькими листами
   - Цветовая маркировка дубликатов
   - Сохранение всех исходных данных

4. **Логирование:**
   - DEBUG/INFO в файл (уровень из конфига)
   - Краткий ход в консоль через **console_ui**; сообщения **logging** в консоль — **WARNING** и выше
   - Формат с указанием функции в файле

---

## Анализ входных данных

### Структура файлов

Все файлы имеют структуру CSV с разделителем ";" и кодировкой UTF-8.

### JSON поля

В 5 файлах обнаружено 8 JSON полей:

1. **CONTEST-DATA:**
   - `CONTEST_FEATURE` - объект с признаками конкурса
   - `CONTEST_PERIOD` - массив периодов
   - `BUSINESS_BLOCK` - массив строк

2. **TOURNAMENT-SCHEDULE:**
   - `TARGET_TYPE` - объект с `seasonCode`
   - `FILTER_PERIOD_ARR` - массив объектов

3. **INDICATOR:**
   - `INDICATOR_FILTER` - массив объектов

4. **GROUP:**
   - `GROUP_VALUE` - массив чисел или строка

5. **REWARD:**
   - `REWARD_ADD_DATA` - объект (структура зависит от `REWARD_TYPE`)

**Каталог и примеры:** машинный разбор колонок и деревьев JSON — в **`Docs/JSON/SPOD_INPUT_DATA_CATALOG.md`**; JSON-выгрузка в соответствии с CSV (один файл CSV → один `.json`) — **`Docs/JSON/examples/`** (`python src/Tools/export_spod_json_examples.py`). Обзор — **`Docs/JSON/README.md`**.

### Зависимости между файлами

- **GROUP** → **CONTEST-DATA** (по `CONTEST_CODE`)
- **INDICATOR** → **CONTEST-DATA** (по `CONTEST_CODE`)
- **TOURNAMENT-SCHEDULE** → **CONTEST-DATA** (по `CONTEST_CODE`)
- **REPORT** → **TOURNAMENT-SCHEDULE** (по `TOURNAMENT_CODE`, `CONTEST_CODE`)
- **REWARD-LINK** → **REWARD** (по `REWARD_CODE`)
- **REWARD-LINK** → **CONTEST-DATA** (по `CONTEST_CODE`)
- **REWARD-LINK** → **GROUP** (по `CONTEST_CODE`, `GROUP_CODE`)

### Многострочные поля

- **REWARD-LINK.GROUP_CODE** - список значений через запятую

---

## Установка и запуск

### Требования

- Python 3.8+ (рекомендуется 3.10)
- pip (для базового Python) или conda (для Anaconda)
- Виртуальное окружение (рекомендуется)

### Зависимости main.py: базовый Python и Anaconda 3.10

Код использует только стандартную библиотеку Python (os, sys, json, re, csv, logging, datetime, typing, concurrent.futures, threading, inspect и др.) плюс **внешние пакеты**:

| Пакет     | Назначение                    | Базовый Python 3.10 | Anaconda 3.10      |
|-----------|-------------------------------|----------------------|--------------------|
| **pandas**| DataFrame, чтение/обработка CSV| ❌ ставить вручную   | ✅ входит в base   |
| **openpyxl** | Запись Excel, стили, форматирование | ❌ ставить вручную | ✅ обычно в base   |
| **numpy** | Векторизованный расчёт статуса турнира (`np.select`) | ❌ (ставится с pandas) | ✅ входит в base |

- **Базовый Python 3.10** (с python.org): внешних библиотек **нет** в поставке. Нужна установка: `pip install -r requirements.txt` (или минимум `pip install pandas openpyxl`).
- **Anaconda 3.10**: в базовое окружение уже входят numpy, pandas и, как правило, openpyxl. Код **работает без дополнительной установки**. Если чего-то не хватает: `conda install pandas openpyxl`.

В `tournament.py` и `main_impl.py` при отсутствии numpy используется запасной вариант на чистом pandas (медленнее, но без numpy).

### Установка и запуск main.py

Запуск выполняется **из корня проекта** (каталог, где лежат `main.py` и `config.json`). Конфигурация читается из `config.json` в корне; входные CSV — из каталога, заданного в `paths.input` (по умолчанию `SPOD/`).

```bash
# Создание виртуального окружения (рекомендуется)
python3 -m venv venv
source venv/bin/activate  # Linux/macOS
# или: venv\Scripts\activate  # Windows

# Установка зависимостей (из корня проекта)
pip install -r requirements.txt
# или вручную: pip install pandas openpyxl

# Запуск из корня проекта
python main.py
```

Выходной Excel создаётся в каталоге `paths.output` (по умолчанию `OUT/`), логи — в `paths.logs` (по умолчанию `LOGS/`).

---

## Логирование

### Формат логов

**DEBUG уровень (в файл)** — пример строки (формат задаётся в `logging_setup`):
```
2025-11-14 03:02:51,034 - [DEBUG] - Загрузка файла CONTEST-DATA [class: FileLoader | def: read_csv_file]
```

Для **`main.py` / main_impl** подробный **INFO/DEBUG** пишется **в файл**; в **терминал** обычный **INFO** из `logging` **не** дублируется (консольный handler — **WARNING+**, краткий ход пайплайна — **`console_ui`**).

### Именование файлов логов

- `main.py`: `LOGS_DEBUG_YYYYMMDD_HH_MM.log` (каталог и шаблон задаются в конфиге, см. **`paths.logs`**).

### Расположение

Все логи сохраняются в каталоге `LOGS/` в подпапках по дате: **`LOGS/YYYY/DD-MM/`** (год и день-месяц по 2 цифры, как для выходных файлов в OUT).

### Профилирование в DEBUG (`[PERF]`)

Модуль **`src/debug_timing.py`**: после старта пайплайна в лог-файл пишутся строки **`[PERF]`** — вход/выход отмеченных функций (время вызова, накопленная сумма по функции, номер вызова за прогон), крупные **фазы** пайплайна (`debug_phase`), при завершении процесса — **сводная таблица** по всем функциям (вызовы, сумма/среднее/min/max, пометка `hot` для частых вызовов). Краткий **топ** по времени и этапы дублируются в **консоли** через **`console_ui`** (итог прогона); на **stdout** обработчик логгера — только **WARNING** и выше, подробный **INFO/DEBUG** — в лог-файл. Дополнительно **`write_performance_statistics_excel`** записывает отдельный файл **`STAT_FILE <таймштамп>.xlsx`** (см. раздел 7 пайплайна).

---

## История версий

### Версия 1.7.41 — POST: перечень файлов в инструкции; уточнение README

- **`Docs/POST_SNAPSHOT/КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt`:** добавлен раздел с полным перечнем **`main.py.txt`**, **`config.json.txt`** и всех **`src/*.py.txt`** (без **Tools**/**Tests**), плюс служебные файлы POST. Раздел **«Каталог POST»** в **README** ссылается на этот перечень и явно описывает имена с **`.txt`**; исправлена устаревшая строка про **requirements.txt** в составе POST.

### Версия 1.7.40 — SPOD-JSON: все структурные ошибки в одной ячейке

- **`json_spod_format`** (**`src/json_spod_format_check.py`**): разбор этапа **(1)** проходит по всей структуре и **накапливает** все нарушения (не останавливается на первом ключе/значении); вывод **«разбор SPOD:»** и список с **•**, лимит **`_MAX_STRUCTURE_ERRORS`** (**80**); улучшен пропуск ошибочных значений вида **`""текст""`** при восстановлении позиции. Подробности — **`Docs/CONSISTENCY_CHECKS_FORMAT.md`**, п. **2.8**.

### Версия 1.7.39 — SPOD-JSON: устойчивость к NBSP/BOM, компактные сообщения на листе, типовые ошибки разметки

- **`json_spod_format`** (**`src/json_spod_format_check.py`**): пропуск **Unicode-пробелов** между токенами (`str.isspace()`), снятие **BOM**; удаление пробельных символов **вне** **`"""…"""`** до нормализации; **короткие** тексты в колонке проверки (путь к полю + суть, без длинных фрагментов и позиции в типовых случаях); явное распознавание **`""ключ""`** вместо **`"""ключ"""`**, значения в **одной паре** кавычек как в JSON (**`"1"`** вместо **`"""1"""`**), объект **`{"""текст"""}`** без пары ключ:значение (подсказка **`["""…"""]`** для массива строк). Ошибки **numeric_value_keys** и **JSON** после нормализации — в сжатом виде; в ячейке — до **12000** символов. Документация: **`Docs/CONSISTENCY_CHECKS_FORMAT.md`** (п. **2.8**), **`Docs/CONSISTENCY_SAMPLE_FORMAT.md`**.

### Версия 1.7.38 — Консистентность: SPOD-JSON, фильтры referential, свод для отключённых правил; архив SQLite в консоли/логе; RATING **`ignoreConditions`**; смысловые **`id`** правил

- **`json_spod_format`**: проверка колонок с JSON в нотации SPOD (**`src/json_spod_format_check.py`**), правила **`spod_json_*`**; компактные сообщения, все замечания разбора в одной ячейке — см. **1.7.40**; прочее — **1.7.39**.
- **referential** / **referential_composite**: опциональные **`src_row_conditions`** / **`ref_row_conditions`** (и алиасы **`sheet_*_row_conditions`**); строки вне фильтра — **«—»** в колонке результата. Пример с условиями — правило **`example_referential_row_filters`** (`enabled: false`) для копирования в боевые правила.
- **CONSISTENCY**: при **`enabled: false`** строка свода всё равно создаётся (**total_rows**, **violations=0**, текст в **sample**).
- **`consistency_checks`**: опциональный ключ **`spod_todo_config_guide`**; **`Config`** / **`main_impl`** сохраняют в объекте секции все дополнительные поля из JSON.
- **Архив SQLite** (**`input_archive_sqlite`**): в консоль и стартовую строку лога выводится **полный относительный** путь к файлу БД (без усечения); блок «По листам» — табличный вид (**`console_ui.print_input_archive_sqlite_report`**).
- **Матрица RATING**: массив **`ignoreConditions`** в **`REWARD_ADD_DATA`** для **ITEM** — перечисленные табельные номера получают ячейку **доступной** (**`fill_accessibility_ok`**) без проверки остальных критериев (**`reward_item_catalog`**, **`item_accessible_for_manager(..., manager_tab=)`**).
- Переименованы короткие **`id`** базовых правил (вместо **`1.1`**, **`2`**, …) на смысловые (**`ref_group_contest_code_in_contest_data`** и т.д.); обновлены **`Docs/SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.*`**, **`Docs/CONSISTENCY_CHECKS_FORMAT.md`**.

### Версия 1.7.33 — Архив SQLite: несколько файлов БД (`archive_db_path`)

- В **`input_files`** опционально **`archive_db_path`**: отдельный файл SQLite для выбранных CSV (иначе используется **`input_archive_sqlite.db_path`**). В одном програмном запуске архив пишется **последовательно** в каждую задействованную БД с той же схемой и логикой снимков/SHA.
- В **`config.json`**: для основных PROM-входов задан путь **`OUT/DB/spod_input_archive.sqlite`**; для файлов **`gamification-*.csv`** — **`OUT/DB/spod_gamification_archive.sqlite`** и **`archive_to_db`: true**.

### Версия 1.7.32 — Ранняя запись файла консистентности при `main_only` + `consistency_only`

- Если в **`run_outputs`** одновременно указаны **`main_only`** и **`consistency_only`**, отдельная книга **`SPOD_PROM CONSISTENCY`** создаётся **сразу после** проверок консистентности на сырых данных и сводки CSV (фаза 02), **до** merge, Summary и записи основного Excel — чтобы не ждать долгих стадий. Повторная запись того же файла в конце прогона **не** выполняется.

### Версия 1.7.37 — SQL-зеркало: 2 версии файла и упрощённый вывод

- Для SQL-проверок консистентности добавлены две версии: подробная **`Docs/SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.sql`** (с комментариями) и компактная **`Docs/SPOD_CONSISTENCY_CHECKS_SQL_MIRROR_PLAIN.sql`** (только код запроса).
- Из SQL-результата убраны **`check_id`** и **`check_type`**: в **SUMMARY** остаются `passed` и `violation_count`, в **DETAIL** — `detail_key` и `detail_message`.

### Версия 1.7.36 — POST: только **`.py`** + **`config.json`** с суффиксом **`.txt`**; **POST/** в **`.gitignore`**

- **`sync_post_txt.py`:** каталог **POST/** перед сборкой **полностью удаляется** и создаётся заново. Копируются только **`main.py`**, **`config.json`** и **`src/**/*.py`** (без **`Tools`** и **`Tests`**) с именами вида **`имя.py.txt`**, **`config.json.txt`**. Из **`Docs/POST_SNAPSHOT/`** в **POST/** копируются **`КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt`** и **`restore_names_from_txt.bat`** (отслеживаются в Git). **README.md**, **requirements.txt** и **Docs/** в POST не входят.
- Каталог **POST/** указан в **`.gitignore`**; ранее закоммиченные файлы под **POST/** следует убрать из индекса (**`git rm -r --cached POST/`**). Подробно: раздел **«Каталог POST»** в README, **`Docs/POST_SNAPSHOT/`**, **`Docs/DOCS_INDEX.md`**.

### Версия 1.7.35 — Матрица RATING: доступность ITEM по **`REWARD_ADD_DATA`**; подсветка зелёный / красный

- Каталог условий для строк **`REWARD_TYPE` = ITEM** строится из сырого JSON колонки **`REWARD_ADD_DATA`** (нормализация кавычек как в ТЗ), модуль **`src/reward_item_catalog.py`**; при отсутствии записи в каталоге подставляются плоские пороги из развёрнутых колонок **`employeeRating`** (как раньше в **`_collect_item_reward_specs`**).
- После записи основного Excel каждая ячейка матрицы (строка менеджера × столбец кода ITEM) получает **светло-зелёную** заливку, если товар **доступен** по **всем** критериям, и **светло-красную**, если **хотя бы один** не выполнен: пороги **`minRating*`** (учитываются только значения **> 0**; место в рейтинге на RATING должно быть **≤** порога), **`minCrystalEarnedTotal`** (если **> 0** — сравнение с колонкой **`Количество кристаллов`**, настраивается **`crystals_col`**), список **`rewardCode`** (все коды должны встречаться на листе **`LIST-REWARDS`** для того же табельного: **`list_rewards_employee_col`**, **`list_rewards_code_col`**), список **`nonRewardCode`** (ни один код не должен быть в заказах сотрудника на **ORDER**: табельный + **`Код товара`**).
- Трёхцветная подсветка только по **`minRating*** (жёлтый/голубой/зелёный по стране/ТБ/ГОСБ) **не применяется**. Цвета: **`fill_accessibility_ok`**, **`fill_accessibility_fail`** в **`rating_item_matrix`**.

### Версия 1.7.34 — Подсветка матрицы RATING: без требования счётчика; выбор цвета по max(minRating*) *(заменено 1.7.35)*

- Исторически: три цвета по max(**`minRating*`**). С версии **1.7.35** раскраска только по полной доступности (см. выше).

### Версия 1.7.31 — Матрица ITEM на листе RATING (`rating_item_matrix`)

- После **`merge_fields_advanced`** и сводки REWARD: на лист **`RATING`** добавляются колонки по строкам **`REWARD`** с **`REWARD_TYPE` = ITEM** (имена колонок — **`REWARD_CODE`**, при необходимости с сезоном; сортировка по коду и **`seasonCode`**). Значения — число строк на объединённом листе **`ORDER`**, где табельный номер и код товара совпадают с формулой СЧЁТЕСЛИМН; нули в ячейки не пишутся.
- Подсветка ячеек матрицы (после записи основного Excel): см. **версию 1.7.35** — **`src/rating_item_matrix.py`**, **`src/reward_item_catalog.py`**, секция **`rating_item_matrix`** в **`config.json`**.
- **Сопоставление столбцов:** для ORDER/RATING поддерживаются и русские заголовки из ТЗ, и типичные англ. имена (**`PERSON_NUMBER`**, **`REWARD_CODE`** и др.); пороги **`employeeRating`** ищутся также по фрагментам имени столбца, если путь отличается (например, индекс в **`getCondition`**). Если столбцы мест в рейтинге не найдены, матрица счётчиков всё равно строится; критерии **`minRating*`** с порогом **> 0** при отсутствии соответствующей колонки ранга дают **недоступность** (красная ячейка).
- **Подсветка матрицы:** заливка по полной проверке доступности (см. **1.7.35**), в том числе при пустом или нулевом счётчике в ячейке столбца ITEM.

### Версия 1.7.30 — Агрегированные листы ORDER / RATING (`aggregate_into_sheet`)

- В **`input_files`** необязательный ключ **`aggregate_into_sheet`**: непустое значение — имя дополнительного листа, куда вертикально склеиваются данные всех записей с тем же значением (порядок блоков по **`sheet_order`**, затем по порядку в **`input_files`**). Исходные листы сохраняются; объединённый лист **дополняет** набор.
- Пустая строка или отсутствие ключа — склейка для этого файла не выполняется.
- В **`config.json`**: gamification-файлы заказов/рейтингов с **`aggregate_into_sheet`**: **`ORDER`** / **`RATING`**; в **`sheet_order`** после **`STAT_FILE`** добавлены листы **`ORDER`** и **`RATING`**.

### Версия 1.7.29 — EMPLOYEE: корректное сравнение AUTO_GENDER и логи

- Лист **EMPLOYEE**: векторизованное и построчное определение пола считаются на одной базе (без колонки **`AUTO_GENDER`**), сравниваются два полных результата; при совпадении в выгрузку идёт векторизованная версия. Исправлены ложные предупреждения «различия — 0 из 0» из‑за сравнения кадра без **`AUTO_GENDER`** с кадром с колонкой.
- **`WARNING`** — только при реальных расхождениях или ошибке сравнения; **`[GENDER FALLBACK]`** поясняет, что в Excel ушёл построчный алгоритм; при полном совпадении — запись уровня **DEBUG**.

### Версия 1.7.28 — Архив SQLite: колонки JSON_* при пропуске ingest

- Если новый снимок в **`arch_*`** **не** создаётся (тот же CSV по SHA, дозапись хеша в inventory, синхронизация **`latest`**, реактивация historical по **`reuse_matching_historical_snapshot`**), недостающие колонки **`JSON_*`** всё равно добавляются (**`ALTER TABLE`**), а значения для строк актуального снимка обновляются **`UPDATE`** по **`__snapshot_id`** и **`__row_ix`** (данные из текущего чтения CSV). Так **`JSON_*`** заполняются и после обновления кода разворота без повторной смены входных файлов.
- **`src/archive_json_columns.py`**: **`update_json_flat_for_snapshot_rows`**; **`src/input_archive_sqlite.py`**: подготовка схемы и вызов синхронизации на соответствующих ветках. Подробно: **`Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md`** (версия документа **1.5**).

### Версия 1.7.27 — Архив SQLite: разворот CONTEST_FEATURE и REWARD_ADD_DATA в колонки JSON_*

- Для листов **CONTEST-DATA** и **REWARD** в таблицы **`arch_*`** в **конец** добавляются колонки **`JSON_…`** с листьями разобранного JSON (нормализация тройных кавычек и внешних кавычек, затем **`safe_json_loads`**). Исходные поля **`CONTEST_FEATURE`** / **`REWARD_ADD_DATA`** сохраняются без удаления.
- Модуль **`src/archive_json_columns.py`**; описание и смысл полей — **`Docs/JSON/SPOD_INPUT_DATA_CATALOG.md`**, **`Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md`**.

### Версия 1.7.26 — Архив SQLite: без дублей при возврате к старому содержимому файла

- **`reuse_matching_historical_snapshot`** (по умолчанию **true**): при совпадении SHA-256 с **любым** прошлым снимком не создаётся новый набор строк в `arch_*`, а релевантный снимок снова становится **`latest`** (прежний `latest` — в **`historical`**). Иначе откат файла к прежнему содержимому после правки давал бы лишний полный снимок.

### Версия 1.7.25 — Архив SQLite: отчёт в консоль и лог по уровням

- **`input_archive_sqlite.reporting`**: **`console`** (`off` / `summary` / `normal` / `verbose`) и **`log`** (`minimal` / `normal` / `verbose`) — отдельно задают подробность вывода в stdout и в лог-файл. Итог архивации всегда одной строкой **INFO** в лог; построчные решения (новый снимок / без изменений / дозапись SHA / пропуски / ошибки) — по выбранному режиму **`log`**.
- **`src/input_archive_sqlite.py`**: сбор событий и вызов **`console_ui.print_input_archive_sqlite_report`**.

### Версия 1.7.24 — Архив SQLite: явный признак архива у каждого входного файла

- В **`config.json`**: у **каждой** записи **`input_files`** задано **`archive_to_db`: true** (после **`subdir`**); **`input_archive_sqlite.default_archive_to_db`**: **`false`** — новые файлы без ключа в архив **не** попадут, пока не добавят **`archive_to_db`**. При **`input_archive_sqlite.enabled`**: **`true`** реплики пишутся в **`OUT/DB/*.sqlite`** для всех файлов с **`archive_to_db`: true**.

### Версия 1.7.23 — Архив SQLite: файл БД в OUT/DB/

- Дефолтный **`input_archive_sqlite.db_path`**: **`OUT/DB/spod_input_archive.sqlite`** (рядом с выходными артефактами; каталог **`OUT/`** уже в **`.gitignore`**). Ранее по умолчанию использовался **`archive/spod_input_archive.sqlite`**.
- **`config.json`**, **`src/input_archive_sqlite.py`** (дефолты и запасной путь), **`Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md`**, **`.gitignore`** (комментарий): согласованы с новым расположением.

### Версия 1.7.22 — Архив SQLite: сводная таблица и дедуп по SHA-256

- **`archive_file_inventory`**: служебная сводка по каждому логическому входному файлу (размер, строки, колонки, mtime, SHA-256, даты проверки/загрузки, счётчики пропусков и ingest).
- **`use_sha256_for_identity`** (по умолчанию true): при неизменных размере/числе строк и колонок сравнение с **`last_content_sha256`** — без повторной записи строк при тех же CSV (в т.ч. после смены только кода программы). Хеш не считается заранее, если метаданные уже отличаются.
- Миграция: колонка **`source_col_count`** в `archive_file_snapshot`; обратная заливка inventory из существующих `latest`-снимков.

### Версия 1.7.21 — Опциональный архив входных CSV в SQLite

- **`config.json` → `input_archive_sqlite`**: путь к файлу БД, включение записи, дефолты для листов, опционально SHA-256 и имена системных колонок.
- **`input_files[].archive_to_db`**: признак «грузить этот файл в архив» (при отсутствии ключа — `default_archive_to_db`).
- **`src/input_archive_sqlite.py`**: мета-таблица снимков (`latest` / `historical`), таблица данных на лист; сравнение mtime, размера, числа строк; без FK между листами.
- **`src/main_impl.py`**: после параллельного чтения CSV — вызов архива; в результат `process_single_file` добавлен путь к найденному файлу.
- **`Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md`**, **`Docs/DOCS_INDEX.md`**, **`.gitignore`**: локальная БД вне репозитория (изначально каталог **`archive/`** для файла БД; с **1.7.23** — **`OUT/DB/`**, см. выше).

### Версия 1.7.20 — Удаление веб-админ-панели из репозитория

- Удалены каталог **`admin_panel/`**, скрипты **`start_admin.sh`**, **`test_admin_panel.py`**, **`test_interface.py`**, документы **`Docs/ADMIN_PANEL_GUIDE.md`**, **`Docs/TZ_ADMIN_PANEL.md`**. Пайплайн **`main.py` / `src/main_impl.py`** не изменялся.
- **README**, **`Docs/DOCS_INDEX.md`**: убраны разделы, ссылки и структура каталогов, относящиеся к админ-панели.

### Версия 1.7.19 — Документация SQL-зеркала отдельным файлом

- **`Docs/SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.md`**: подробное описание единого запроса (`WITH` → `dim_*` / `base_schedule_ref` → `v_*` → `chk_summary` / `chk_detail` → итоговый `SELECT`); перечень всех проверок по типам; таблицы и поля витрины; замена схемы и имён таблиц; формат результата SUMMARY/DETAIL; исключения (field_format, json, csv_columns_count); диалект Hive/Spark и замечания по переносу.
- **`Docs/DOCS_INDEX.md`**, **README**: ссылка на новый документ в каталоге документации и в списке ключевых документов.

### Версия 1.7.18 — SQL-зеркало: оптимизация и комментарии в коде

- **`Docs/SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.sql`**: CTE **`dim_*`**, **`base_schedule_ref`** для переиспользования справочников и одного прохода по расписанию для **scenario_1/16/20**; расширенные комментарии на русском (глоссарий SQL, пояснения к типовым **`SELECT` / `LEFT JOIN` / `WHERE`**, разделы unique / field_length и итоговый **SELECT**); без зеркалирования **field_format**.
- **`Docs/DOCS_INDEX.md`**, **README**: уточнено описание SQL-файла (оптимизация и документирование внутри скрипта).

### Версия 1.7.17 — Документация SQL-зеркала проверок консистентности

- **`Docs/SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.sql`**: единый запрос со сводкой (**SUMMARY**) и деталями (**DETAIL**); комментарии — соответствие **`config.json` → consistency_checks.rules** (**id**, **name**, **type**) и модулю **`src/consistency_checks.py`**; описание работы CTE и ограничений (без **json_***, отключённых referential и **csv_columns_count**).
- **`Docs/DOCS_INDEX.md`**: расширено описание SQL-зеркала; в правилах актуализации — напоминание синхронизировать SQL при изменении перечисленных типов правил.
- **`Docs/CONSISTENCY_CHECKS_FORMAT.md`**: ссылка на SQL-зеркало и **DOCS_INDEX**.

### Версия 1.7.16 — run_outputs, pandas concat, переносы в консоли

- **`config.json`**: вместо одного **`run_mode`/`full`** — массив **`run_outputs`**: `source_only`, `main_only`, `consistency_only` (несколько значений = какие файлы создавать). Старый **`run_mode`** читается, если **`run_outputs`** нет.
- **`copy_consistency_results_from_raw_to_processed`**: копирование колонок проверок через **`pd.concat`**, без предупреждения **PerformanceWarning** о фрагментированном DataFrame.
- **`console_ui`**: **`print_wrapped`** + **textwrap** для сводки консистентности (без усечения «…»); **`expected_phases_for_run_flags`** для прогресс-бара.
- **Документация**: **`Docs/INPUT_DATA_AND_CONFIG_FULL.md`**, **`Docs/DOCS_INDEX.md`** — описание **`run_outputs`**; снимок **POST/** (не в Git): **`python src/Tools/sync_post_txt.py`**, раздел README **«Каталог POST»**, **`Docs/POST_SNAPSHOT/`**.

### Версия 1.7.15 — Сводка консистентности в консоли: обзор и таблица по типам

- **`console_ui.print_consistency_summary`**: явные строки «правил в отчёте / листов», «проблем не обнаружено» или «выявлено нарушений …»; **таблица** по каждому **типу проверки** (число правил, уникальных листов, сумма нарушений, примечание); при нарушениях сохранена **детализация** по правилам.
- **README**: обновлены п. **8**, таблица модулей и блок про вывод CONSISTENCY.

### Версия 1.7.14 — Документация и таблица «лист / строки» в консоли

- **`console_ui.print_data_processing_summary`**: сводка по листам — **двухколоночная таблица** («Лист» \| «Строки / примечание»), **все** записи из `summary`, **без усечения** текста; разбор строк — **`_split_sheet_summary_line`**.
- **README**: в структуре проекта и таблице модулей добавлен **`console_ui.py`**; уточнены **логирование** (файл vs консоль **WARNING+**), п. **5** итоговой статистики (лог полный / консоль компактно), п. **3.2** консистентности, ТЗ и раздел **«Логирование»**; новый п. **8** — описание **`console_ui`**.

### Версия 1.7.13 — Консоль: краткий ход работы и сводки

- Модуль **`src/console_ui.py`**: баннер старта, строки **старт/завершение** этапов (`debug_phase` через хуки в **`debug_timing`**), **прогресс-бар по этапам** (`done/total`, ASCII `#`/`-`, stdlib) после каждого завершённого верхнего этапа; число шагов задаётся **`expected_phases_for_run_mode`** (синхронно с **`main_impl`**). Усечение длинных строк там, где по-прежнему одна строка (заголовки, пути); сводка **консистентности**, компактно **длина полей / CSV**, таблицы **этапов** и **топ функций**, пути к **Excel/лог** и **wall-clock**.
- **`debug_timing`**: в хуки консоли передаётся **глубина** фазы (**`depth`**); вложенные фазы не дублируют строки прогресса.
- **`main_impl`**: после **`setup_logger`** консольный **`StreamHandler`** — уровень **WARNING** (подробный ход — в файл); **`RUN_MODE`** и **`set_phase_progress_total`** до первой фазы **01**; итог прогона — **`_console_footer`**; многострочный **`print_final_report`** только в лог; прогресс **FIELD LENGTH** — в **DEBUG**.
- **`run_consistency_checks_and_attach_summary`** возвращает список результатов для **`print_consistency_summary`**; при отсутствии правил в конфиге в консоль выводится краткая строка «правила не выполнялись».

### Версия 1.7.12 — Область и непустые колонки для правил `unique`

- **Логика:** проверка уникальности **`key_columns`** выполняется только среди **активных** строк. Активность = попадание в **область** (`unique_scope_*`) и **непустота** всех колонок из **`unique_require_non_empty`**. Для остальных строк колонка «ДУБЛЬ» пустая; в своде **CONSISTENCY** **`total_rows`** считается по активным строкам.
- **`unique_scope_conditions`**: массив пар **`column` / `value`**; режим **`unique_scope_mode`**: **`all`** (логическое **И** по всем парам) или **`any`** / **`or`** / **`или`** (**ИЛИ** — достаточно одной пары). Пустой массив условий — без ограничения по области (все строки листа, если не отсеяны по непустоте).
- **Устаревшая пара** **`unique_scope_column`** / **`unique_scope_value`**: одно условие, если массив **`unique_scope_conditions`** не используется.
- **`unique_require_non_empty`**: строка не участвует, если любая из перечисленных колонок пуста (в т.ч. `NaN`, `""`, `"-"`, `"None"`, `"null"` после нормализации, в духе проверок длины).
- **`config.json`:** у **каждого** из **18** правил **`type`: `unique`** явно заданы **`unique_scope_mode`**, **`unique_scope_conditions`**, **`unique_scope_column`**, **`unique_scope_value`**, **`unique_require_non_empty`**; для «глобальной» уникальности по листу — **`[]`**, **`""`**, **`""`**, **`[]`**.
- **Пример выборочной проверки:** **`unique_employee_kpk_gosb`** — только **POSITION_NAME = КПК** и непустой **KPK_CODE**.
- **Документация:** в **README** добавлен подраздел **«Правило unique»** (таблица полей, алгоритм, ссылка на функции в **`consistency_checks.py`**); в **Docs/CONSISTENCY_CHECKS_FORMAT.md** расширен п. **2.4** (пошаговая логика, примеры **И/ИЛИ**, строка в таблице листов для EMPLOYEE КПК); обновлён пример JSON в README.

### Версия 1.7.11 — Проверка priority по CONTEST_CODE (REWARD-LINK)

- Новый тип правила **`json_priority_unique_per_contest_link`** в **`consistency_checks`**: для каждого **`CONTEST_CODE`** на **REWARD-LINK** среди уникальных **`REWARD_CODE`** (без учёта **GROUP_CODE**) в **REWARD_ADD_DATA** проверяется ключ **`priority`**: либо отсутствует у всех, либо задан у всех с **уникальными** значениями; смешанное заполнение — нарушение. Парсинг JSON — **`_parse_add_data_cell_with_normalized`** (как у **json_field_***).
- В **`config.json`** добавлено правило **`reward_priority_unique_per_contest`**.
- В **`Docs/CONSISTENCY_CHECKS_FORMAT.md`** добавлен раздел **2.7** с полями правила и примером JSON.

### Версия 1.7.9 — Excel `STAT_FILE <таймштамп>.xlsx` со временем этапов и функций

- В **`debug_timing`**: снова накапливаются завершённые фазы **`debug_phase`**; функции **`format_duration_ru`**, **`write_performance_statistics_excel`** формируют книгу с листами «Сводка», «Этапы», «Функции» в каталоге вывода по дате.
- В **`main`**: вызов после режимов **2** и **4** и в конце основного прогона (**1** / **3**), после записи Excel, чтобы в этапы попадала и запись книг.

### Версия 1.7.10 — Ускорение форматирования Excel

- **`calculate_column_width`**: режим **AUTO** — оценка ширины по заголовку и первым **500** строкам данных (`_AUTO_COLUMN_WIDTH_MAX_DATA_ROWS`); фиксированная ширина (число в `col_width_mode` / `width_mode`) как прежде.
- **`_format_sheet`**: для столбцов из **COLUMN_FORMATS** общий проход с `wrap_text` не дублируется — выравнивание и перенос задаёт только **`apply_column_formats`** по конфигу; остальные столбцы — прежний общий стиль с переносом.

### Версия 1.7.7 — Детальное DEBUG-логирование производительности (`debug_timing`)

- Добавлен **`src/debug_timing.py`**: декоратор **`@debug_timed`**, контекст **`debug_phase`**, **`reset_run_timing`**, итоговая сводка через **`atexit`**.
- Ключевые функции **`main_impl`**, **`consistency_checks.run_consistency_checks_and_attach_summary`**, **`json_utils.flatten_json_column_recursive`**, **`reward_getcondition_summary`** помечены для учёта времени; «горячие» функции (частые вызовы) — режим **`hot=True`** (агрегат без лога на каждый вызов).

### Версия 1.7.5 — Режимы `*_only` и перенос текста в source

- **`consistency_only`:** больше не создаётся файл **SPOD_PROM source** — только отчёт консистентности. Выгрузка **source** выполняется только в режиме **`full`** (и отдельно при **`source_only`** до выхода).
- **`write_source_excel`:** для всех ячеек листов файла source включены **перенос по словам** и выравнивание по верху.

### Версия 1.7.6 — POST: состав снимка *(устарело; актуально 1.7.36)*

- Исторически в **POST/** входили также **README** и **requirements**. Текущий состав и полная очистка каталога — см. **версию 1.7.36** и раздел **«Каталог POST»**.

### Версия 1.7.4 — Каталог POST (ранее с Docs/)

- Введены **`sync_post_txt.py`**, bat для снятия **.txt** и расширенный состав POST. Актуальный состав и шаблоны инструкций — **версия 1.7.36**, **`sync_post_txt.py`**, **`Docs/POST_SNAPSHOT/`** (в Git); сгенерированный **POST/** — только локально (**`.gitignore`**).

### Версия 1.7.3 — Полнота каталога JSON и CSV

- **`SPOD_INPUT_DATA_CATALOG.md`:** для файлов `REWARD (PROM) 23-03 v3.csv` и `CONTEST (PROM) 23-03 v3.csv` включены таблицы «Краткое назначение колонок» (раньше подсказки были только у старых имён файлов).
- Добавлен машинный разбор **JSON** для колонок **GROUP** (`GROUP_VALUE`), **SCHEDULE** (`TARGET_TYPE`, `FILTER_PERIOD_ARR`), **USER_ROLE** (массивы в `PERSON_NUMBER_ARR`, `STAGE_ETALONE_CODE_ARR`, `POST_ETALONE_CODE_ARR`, `DIV_CODE_ARR`, `EXCLUDE_DIV_CODE_ARR`) — в соответствии со структурой в **`Docs/JSON/examples`**.
- Обновлены **`Docs/JSON/README.md`**, **`src/Tools/build_spod_input_catalog.py`**.

### Версия 1.7.2 — Каталог входных данных в `Docs/JSON/`, примеры JSON

- Файл **`SPOD_INPUT_DATA_CATALOG.md`** перенесён в **`Docs/JSON/`**; добавлены **`Docs/JSON/README.md`** и каталог **`Docs/JSON/examples/`**: **один CSV в `IN/SPOD` → один `.json`** с тем же именем; внутри один JSON с массивом `rows` (строки с разобранным `REWARD_ADD_DATA` / `CONTEST_FEATURE`).
- Скрипт **`src/Tools/export_spod_json_examples.py`** — генерация примеров; **`build_spod_input_catalog.py`** пишет каталог в `Docs/JSON/`, в шапке — ссылка на примеры.
- В **`build_spod_input_catalog.py`** имена CSV для REWARD/CONTEST приведены к актуальным выгрузкам (`* 23-03 v3.csv`).
- Обновлены ссылки в **README**, **DOCS_INDEX**, **INPUT_DATA_AND_CONFIG_FULL**, **catalog_glossary/README**.

### Версия 1.7.1 — Синхронизация документации

- В **README.md** обновлены: таблица секций `config.json` (добавлены `reward_getcondition_summary`, уточнён `column_formats`), пример JSON-структуры, описание пайплайна **main_impl** (порядок: консистентность на сырых данных, merge, сводка REWARD, SUMMARY, STAT_FILE).
- **`Docs/INPUT_DATA_AND_CONFIG_FULL.md`**: расширен §3 (блоки `column_formats`, `json_columns`, `reward_getcondition_summary`).
- **`Docs/DOCS_INDEX.md`**: правила актуализации — приоритет README, согласование с INPUT_DATA_AND_CONFIG_FULL.
- **`ToDo SPOD.txt`**: актуализированы статусы задач по `except_columns`/STATISTICS и сводке getCondition.

### Версия 1.7 — Логи по дате, формат sample, DEBUG по JSON, режимы, файл consistency в full

**Логи:**
- Файлы логов пишутся в подкаталоги по дате **`LOGS/YYYY/DD-MM/`** (как для OUT). Функции `get_log_dir_for_run()` (main_impl), `_get_log_dir_for_run()` (logging_setup).

**Формат записей в колонке sample (лист CONSISTENCY):**
- Номера строк в формате **`[N]`** или **`[N, M, K]`**. При нескольких нарушениях — префикс «[37], [47], … => » и детали.
- **referential** / **referential_composite**: только `[N] значение` (без имени колонки и «∉таблица»).
- **json_field_equals_column**: `[N] значение_из_JSON ≠ ожидаемое`; ошибка парсинга: `[N] | json_бит`.
- **unique**: `[N, M] {ключ} ×k` (дубль×k).
- **field_format** (fixed_length_digits): `[N] значение = факт < ожид` или `факт > ожид`; проверка — ровно N цифр (и меньше, и больше считаются ошибкой).
- **csv_columns_count**: `[N] | полей факт/ожид | +/−`.

**DEBUG при ошибках проверок JSON (поле в JSON равно колонке / поле в JSON в колонке):**
- Отдельные строки лога: исходное значение колонки (целиком), после преобразований, JSON в виде дерева (`json.dumps(..., indent=2)`), поля с ошибкой и сравниваемые значения.

**safe_json_loads:** при ошибке разбора в DEBUG выводится понятное объяснение (например: «Строка не распознана как JSON: в начале ожидалось… Возможно, в ячейке обычный текст»).

**Режим consistency_only (4):** к листам не применяются **merge_fields_advanced**, **AUTO_GENDER** (EMPLOYEE) и расчёт статуса турнира (TOURNAMENT-SCHEDULE). Выгружается только файл consistency.

**Определение пола (gender):** в лог пишутся только старт, сводка (М / Ж / неопределено, всего) и завершение; построчные DEBUG по каждой строке убраны.

**Режим full (1):** дополнительно к основному Excel создаётся **отдельный файл consistency** (лист CONSISTENCY + листы с нарушениями) в том же каталоге `OUT/YYYY/DD-MM/`, имя `{output_filenames.consistency} YYYY-MM-DD_HH-MM-SS.xlsx`.

**Документация:** обновлён README (логи, sample, режимы); Docs/CONSISTENCY_SAMPLE_FORMAT.md — актуальные форматы и реализация.

**Сводка getCondition (лист REWARD):** сопоставление колонок `nonRewards` / `rewards` с учётом гибких пробелов вокруг `=>`; колонка из `reward_getcondition_summary.column_name` добавляется всегда при `enabled: true` (в т.ч. пустая, если поля не найдены), чтобы в Excel был виден заголовок.

**column_formats / `except_columns` (в т.ч. лист STATISTICS):** нормализация имён заголовков (BOM, NFKC, пробелы); разбор чисел с разрядами (пробел/NBSP); обход столбцов по индексу при применении формата в Excel — чтобы числовой формат и тип ячейки совпадали с правилом «все колонки кроме перечисленных».

---

### Документация — каталог входных CSV `IN/SPOD`

- **`Docs/JSON/`** — `SPOD_INPUT_DATA_CATALOG.md` (оглавление по CSV, разбор JSON), **`examples/`** (по одному JSON-файлу на каждый CSV выгрузок REWARD/CONTEST), **`README.md`**. Сборка каталога: **`src/Tools/build_spod_input_catalog.py`**; экспорт примеров: **`src/Tools/export_spod_json_examples.py`**; глоссарии JSON: **`src/Tools/catalog_glossary/`**.
- В **`Docs/INPUT_DATA_AND_CONFIG_FULL.md`** добавлена ссылка на этот каталог (раздел «Входные данные»).

---

### Версия 1.6.1 — Проверка числа полей CSV: sample только при отклонениях

- Для строки проверки **csv_columns_count** на листе CONSISTENCY колонка **sample** больше не заполняется текстом «Проверка пройдена…» при нуле нарушений; при успехе sample **пустая**. В sample выводятся только примеры строк с расхождением числа полей.

---

### Версия 1.6 — Проверки по JSON (ADD_DATA), склейка полей CSV, правила REWARD

**Новые типы правил консистентности:**
- **json_field_equals_column** — сравнение значения ключа из JSON-колонки с колонкой листа. Поддержка фильтров: `filter_column`/`filter_value` (только строки с заданным значением колонки), `json_filter_key`/`json_filter_value` (условие по ключу в JSON). Параметр **`must_not_equal`: true** инвертирует проверку (значение из JSON не должно равняться колонке). Используется для REWARD: BADGE с masterBadge=Y — parentRewardCode должен равняться REWARD_CODE; BADGE с masterBadge=N — parentRewardCode не должен равняться REWARD_CODE; LABEL — parentRewardCode = REWARD_CODE.
- **json_field_in_column** — все уникальные значения ключа из JSON-колонки должны присутствовать в указанной колонке того же листа. Используется для проверки: все parentRewardCode из ADD_DATA на листе REWARD должны быть в REWARD_CODE.

**Парсинг ADD_DATA:** в **consistency_checks.py** добавлена **`_parse_add_data_cell(val)`**: замена `"""` на `"` в строке ячейки, затем `json.loads(normalized)`. Ошибки парсинга возвращают `None`; проверки обрабатывают только успешно разобранный JSON.

**Правила в config.json (REWARD):**
- **reward_add_data_badge** — REWARD_TYPE=BADGE, в ADD_DATA masterBadge=Y → parentRewardCode должен равняться REWARD_CODE.
- **reward_add_data_label** — REWARD_TYPE=LABEL → parentRewardCode = REWARD_CODE.
- **reward_add_data_badge_n** — REWARD_TYPE=BADGE, masterBadge=N → parentRewardCode не должен равняться REWARD_CODE (must_not_equal: true).
- **reward_parent_in_reward_code** — тип json_field_in_column: все parentRewardCode из ADD_DATA должны быть в колонке REWARD_CODE.

**Чтение CSV (main_impl):** при **фактическом числе полей в строке больше ожидаемого** («хвост» из-за точки с запятой внутри JSON) последние поля склеиваются в одну колонку: `";".join(row[n-1:])`, чтобы JSON в последнем поле не обрезался. Ожидаемое число берётся из `expected_columns` (0 = по заголовку).

**csv_columns_count в consistency_checks:** секция в конфиге задаёт ожидаемое число полей по листам и тексты для колонок свода CONSISTENCY (ТИП ПРОВЕРКИ, Описание и т.д.). Записи о расхождениях числа полей в CSV добавляются в лист CONSISTENCY с заполненными описаниями.

**Документация:** в README добавлено описание типов json_field_equals_column (включая must_not_equal) и json_field_in_column, парсинга ADD_DATA, csv_columns_count; обновлено описание модуля consistency_checks.py; в истории версий — запись 1.6.

---

### Версия 1.5 — Режимы запуска, выход по дате, ожидаемые колонки, сортировка в input_files, SUMMARY

**Режимы запуска (run_mode):**
- В конфиг добавлен **`run_mode`**: `"full"` (1), `"source_only"` (2), `"main_only"` (3), `"consistency_only"` (4). Режим задаётся строкой или числом.
- **output_filenames**: имена выходных файлов (main, source, consistency) задаются в конфиге.
- **apply_sort_to_source**, **apply_sort_to_main**: флаги применения сортировки при записи source и основного Excel.

**Выходные файлы по дате:**
- Все сгенерированные файлы (source, consistency, main) пишутся в подкаталоги по дате формирования: **`paths.output/YYYY/DD-MM/`** (например `OUT/2026/17-03/` для 17 марта 2026). День и месяц — всегда по 2 цифры (01-01 для 1 января). Каталоги создаются автоматически (`get_output_dir_for_run`).

**input_files — новые поля:**
- **expected_columns**: ожидаемое число полей в CSV; **0** — АВТО (по заголовку). При расхождении данные фиксируются и выводятся в лист CONSISTENCY.
- **subdir**: подкаталог внутри `paths.input` для поиска файла (например `"SPOD"`, `"FILE"`). Итоговый путь: `paths.input/subdir/file`.
- **sort_columns**: правила сортировки листа при записи (массив `{"column": "Имя", "order": "asc"|"desc"}`). Сортировка применяется последовательно; при отсутствии колонки на листе — пропуск; при отсутствии листа SUMMARY в режиме 4 активным делается первый лист.

**Сортировка:**
- Секция **source_export.sort_rules** не используется. Правила сортировки задаются в **input_files[].sort_columns** для каждого листа; применение управляется **apply_sort_to_source** и **apply_sort_to_main**.

**SUMMARY и INDICATOR:**
- В **summary_key_defs** для листа INDICATOR добавлен ключ **INDICATOR_CODE** (cols: INDICATOR_CODE, INDICATOR_ADD_CALC_TYPE, CONTEST_CODE). Свод ключей SUMMARY формируется с 7 колонками; в блоках 1–4 при формировании строк подставляется INDICATOR_CODE из листа INDICATOR (при однозначном совпадении по CONTEST_CODE и INDICATOR_ADD_CALC_TYPE), чтобы не появлялись строки с пустым INDICATOR_CODE и дубликаты. Строка-заглушка (все ключи "-" и пустые индикаторы) отфильтровывается перед построением DataFrame.

**Исправления:**
- В режиме **consistency_only** лист SUMMARY не записывается — при установке активного листа книги проверяется наличие "SUMMARY", иначе активным делается первый лист.
- Документация обновлена: все поля конфигурации описаны с примерами; добавлены run_mode, output_filenames, apply_sort_to_source/main, paths (подпапки по дате), input_files (expected_columns, subdir, sort_columns).

**Папка POST:** копии **main.py**, **config.json** и **`src/**/*.py`** (кроме **Tools** и **Tests**) с суффиксом **.txt** в имени файла; **requirements.txt** в POST не входит. Документация в POST не кладётся; перечень файлов — **`Docs/POST_SNAPSHOT/КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt`**.

---

### Версия 1.4 — Единая точка проверок консистентности (удаление check_duplicates и field_length_validations)

**Цель:** все проверки уникальности (дубликаты) и длины полей выполняются только в модуле **consistency_checks**; отдельные секции конфига и отдельные шаги пайплайна удалены.

**Удалено из config.json:**
- Секция **`check_duplicates`** — правила перенесены в `consistency_checks.rules` с типом **`unique`** (поля `sheet`, `key_columns`, `output.column_on_sheet`). Модуль consistency_checks в фазе 1 создаёт колонки «ДУБЛЬ: …» на листах.
- Секция **`field_length_validations`** — правила перенесены в `consistency_checks.rules` с типом **`field_length`** (поля `sheet`, `result_column`, **`fields`** с limit/operator по каждому полю, `output.column_on_sheet`). Модуль в фазе 1 создаёт колонки проверки длины полей (FIELD_LENGTH_CHECK и т.д.).
- В **color_scheme** удалена группа **DUPLICATES** (оформление колонок «ДУБЛЬ: …» при необходимости задаётся общими правилами color_scheme).

**Изменения в коде:**
- **consistency_checks.py**: добавлены **`_run_unique_check`** (создание колонок «ДУБЛЬ: …») и **`_run_field_length_check`** (создание колонок проверки длины полей по полю `fields` правила). В **Фазе 1** выполняются оба типа; в Фазе 2 — referential/referential_composite и сбор результатов unique/field_length.
- **main_impl.py**: удалён шаг «Параллельная проверка длины полей» (ThreadPoolExecutor с validate_single_sheet). Проверки консистентности вызываются **после merge, до формирования SUMMARY**, чтобы колонки unique и field_length уже были на листах при построении SUMMARY. Итоговый отчёт: **`collect_duplicates_and_validation_report`** возвращает только `(validation_report, csv_mismatch_report)`; блок «Дубликаты» в консольном выводе убран — дубликаты отображаются в листе CONSISTENCY и в логе проверок. Отклонения по длине полей собираются по правилам `consistency_checks.rules` (type=field_length), а не по `FIELD_LENGTH_VALIDATIONS`.
- **config_loader.py**: атрибуты **`check_duplicates`** и **`field_length_validations`** оставлены для совместимости (пустой список и пустой dict при отсутствии в config), чтобы код в validation.py не падал при обращении к ним.

**Итог:** один конфиг правил (`consistency_checks.rules`), один модуль выполнения (consistency_checks), один порядок шагов; дублирование конфигурации и логики убрано.

**Параллелизация проверок консистентности (дополнение к v1.4):** фазы 1 и 2 в `run_all_consistency_checks` выполняются в **ThreadPoolExecutor** (число потоков из `max_workers`, по умолчанию из `performance.max_workers_cpu`). Запись в один и тот же лист защищена блокировкой по имени листа (`threading.Lock`), чтобы правила, пишущие в разные листы, шли параллельно без гонок. В лог (DEBUG) выводится сообщение о числе потоков и правил.

**Дополнение: новые правила консистентности (по таблице «Проверки-Tаблица 1.csv»):** в `consistency_checks.rules` добавлены 8 правил referential/referential_composite и 11 правил **field_format** (проверка формата даты, числа 0.00000, 20 цифр с лидирующими нулями). Итого правил: 46 (13 referential, 3 referential_composite, 16 unique, 3 field_length, 11 field_format). Лист **CONSISTENCY** формируется с колонками-описаниями (ТИП ПРОВЕРКИ, Описание, таблица источник, поле источник, таблица где проверяем, поле для проверки, параметр сравнения, комментарий) по образцу таблицы проверок; при формировании сводки в модуль передаётся секция конфига с ключом `rules`, чтобы эти колонки заполнялись.

**Исправление отображения колонок на листе CONSISTENCY:** в `run_consistency_checks_and_attach_summary` в main передаётся секция конфига (summary_sheet_name + rules), а не весь config.json. Правила для заполнения колонок-описаний берутся из `config.get("rules")`, а не из `config.get("consistency_checks", {}).get("rules")`, чтобы на листе CONSISTENCY отображались колонки ТИП ПРОВЕРКИ, Описание, таблица источник и т.д.

---

### Версия 1.3 — Выгрузка source Excel, форматы колонок (except_columns), даты и include_in_source

**Выгрузка сырых данных (source Excel):**
- Отдельный файл «SPOD_PROM source YYYY-MM-DD_HH-MM-SS.xlsx» формируется сразу после загрузки CSV (до разворота JSON и любых доп. колонок). В него записываются только данные из CSV, без разворота JSON.
- В `input_files` добавлен параметр `include_in_source` (по умолчанию `true`): при `false` лист не включается в source-выгрузку; основная загрузка и основной Excel не зависят от этого параметра.
- Секция конфига `source_export.sort_rules`: задаётся сортировка листов при записи в source-файл (лист, колонки, порядок asc/desc).
- Для каждого листа в source применяются свои параметры из `input_files` (max_col_width, freeze, col_width_mode, min_col_width). На всех листах source по умолчанию включён автофильтр.
- Проверка наличия файлов выполняется после записи source-файла.

**Форматы колонок (column_formats):**
- Добавлен режим `except_columns`: можно задать список колонок-исключений — формат применяется ко всем колонкам листа кроме указанных. Если задан непустой `except_columns`, он используется вместо `columns`.

**Обработка дат (data_type: date):**
- Преобразуются любые распознаваемые даты (в т.ч. 4000-01-01): сначала разбор в указанном формате, затем для NaT — повторная попытка без формата. Значения, которые не удалось преобразовать в дату, остаются в виде исходной строки (текст в Excel).

**Изменения в коде:**
- `process_single_file` возвращает четвёртый элемент — копию DataFrame до разворота JSON (`df_raw_for_source`); в `main()` в source попадают только листы с `include_in_source: true`.
- `write_source_excel`: дополнение пустыми листами только для листов с `include_in_source: true`, сортировка по `source_export.sort_rules`, применение параметров листов из конфига, автофильтр на каждом листе.
- `apply_column_format_conversion` и `apply_column_formats`: поддержка `except_columns`; для дат — двухэтапный разбор и сохранение нераспознанных как текст.
- `Config`: атрибут `source_export_sort` из `config.source_export.sort_rules`.

---

### Версия 1.2 — Расхождения по числу полей в CSV в итоговой статистике

**Функциональность:**
- При чтении CSV (`read_csv_file` в main_impl) строки с числом полей, отличным от числа колонок в заголовке, по-прежнему нормализуются (дополняются пустыми значениями или обрезаются), но каждая такая строка фиксируется: номер строки в файле, ожидаемое/фактическое число полей, направление («больше»/«меньше»).
- Список расхождений накапливается при параллельной загрузке файлов (потокобезопасно) и в конце работы выводится в том же блоке итоговой статистики, что дубликаты и отклонения по длине полей.
- В отчёте для каждого расхождения выводятся: файл, лист, номер строки с ошибкой, ожидаемое и фактическое число полей, направление.

**Изменения в коде:**
- `read_csv_file` возвращает `(df, issues)` или `None`; при нормализации строки в `issues` добавляется запись с `row_index`, `expected_cols`, `actual_cols`, `direction`.
- Глобальный список `_csv_column_mismatches` и блокировка для потоков; в `process_single_file` при непустом списке расхождений записи дополняются полями `sheet` и `file` и добавляются в общий список.
- `collect_duplicates_and_validation_report` возвращает третьим элементом `csv_mismatch_report`.
- `print_final_report` принимает третий аргумент `csv_mismatch_report` и выводит блок «Расхождения по числу полей в CSV».

---

### Версия 1.1 — Модульная структура и классы (обновление документации)

**Документация:**
- В содержание добавлен раздел «Модули src/ (описание и назначение)» с таблицей модулей, их назначением и перечнем основных классов и функций.
- Раздел «Программа main.py» обновлён: описание точки входа (корневой main.py → Config → main_impl.main()), инициализации и чтения CSV.
- Раздел «Установка и запуск» уточнён: запуск из корня проекта, использование `requirements.txt`, каталоги OUT/ и LOGS/.
- В структуру проекта добавлен файл `requirements.txt`.

**Изменения кода (v1.1):**
- **Точка входа**: корневой `main.py` только загружает конфигурацию (`Config`), внедряет её в контекст и запускает пайплайн из `src.main_impl`.
- **Каталог src/**: весь рабочий код вынесен в модули:
  - `config_loader.py` — класс `Config` (загрузка config.json, атрибуты для путей, листов, правил merge, валидации и т.д.).
  - `config_holder.py` — внедрение текущего конфига для совместимости с `main_impl`.
  - `logging_setup.py` — форматтер логов с именем функции, настройка логгера по конфигу.
  - `json_utils.py` — разбор и разворот JSON-полей (`safe_json_loads`, `flatten_json_column_recursive`).
  - `file_loader.py` — класс `FileLoader`: поиск файлов без учёта регистра, чтение CSV, разворот JSON по конфигу.
  - `tournament.py` — расчёт статуса турнира (`calculate_tournament_status` по датам и config).
  - `validation.py` — валидация длины полей и проверка дубликатов (в т.ч. векторизованные и параллельные варианты).
  - `gender.py` — определение пола по отчеству/имени/фамилии (паттерны из config).
  - `main_impl.py` — полный пайплайн (загрузка CSV, merge, summary, запись Excel, отчёты); при запуске из корня использует внедрённый `Config`.
- Логика работы программы сохранена; запуск из корня: `python main.py`.

### Версия 1.0

**Основные изменения:**
- Реализована основная программа обработки данных (`main.py`)
- Вся конфигурация вынесена в **config.json** (пути, input_files, summary_sheet, sheet_order, summary_key_defs, gender, field_length_validations, derived_columns, merge_fields_advanced, color_scheme, column_formats, check_duplicates, json_columns)
- **sheet_order** в config — задаётся порядок листов в выходном Excel; листы не из списка идут следом по алфавиту
- Лист **STAT_FILE** — сводная статистика по исходным файлам (имя файла, лист, дата файла, дата обработки, строки, колонки, размер, статус)
- Настроено логирование с двумя уровнями
- Реализована проверка дубликатов по правилам из config
- Итоговая статистика по дубликатам и отклонениям длины полей в лог и консоль в конце работы (без прерывания)
- Добавлена поддержка JSON полей (разворот по json_columns)
- **derived_columns** — производные колонки на листе (например, табельный в 20 знаков с лидирующими нулями на LIST-REWARDS); **src_key_transforms** / **dst_key_transforms** в merge — преобразование ключей при связке (pad_20 и т.д.)
- Документация — README.md и Docs/; снимок для переноса без Git — каталог POST/ (не в репозитории), см. раздел **«Каталог POST»** и **`sync_post_txt.py`**.

**Исправленные проблемы:**
- Исправлены отступы в main.py (basedpyright): find_file_case_insensitive, safe_json_loads, generate_dynamic_color_scheme_from_merge_fields, merge_fields_across_sheets
- Добавлено детальное логирование ошибок
- Исправлена обработка JSON полей
- Улучшена валидация данных
- Исправлена работа с зависимостями между файлами

**Оптимизации:**
- Удалены неиспользуемые функции создания SUMMARY листов
- Улучшена производительность чтения CSV
- Оптимизирована работа с большими файлами

---

## Контакты и поддержка

Для вопросов и предложений обращайтесь к разработчику проекта.

---

*Документация обновлена: 2026-03-28*

---

### Версия 1.2 — Проверки консистентности и папка POST (историческая)

**Проверки консистентности (актуально с v1.4 см. выше):**
- Добавлен модуль **src/consistency_checks.py**. С версии 1.4 модуль **сам создаёт** колонки unique и field_length на листах; секции `check_duplicates` и `field_length_validations` в config удалены.
- В **config.json** секция **consistency_checks** (summary_sheet_name, rules) — единственное место задания правил unique и field_length.
- В **config_loader** — атрибут `consistency_checks`; в **main_impl** — вызов `run_consistency_checks_and_attach_summary` после merge, до SUMMARY.

**Папка POST (историческая формулировка):**
- Актуальное поведение см. **версию 1.7.36** и раздел **«Каталог POST»**: в **POST/** только **main.py**, **config.json** и **src/**/*.py** (без Tools/Tests) с суффиксом **.txt** в имени файла; каталог не версионируется (**`.gitignore`**).
