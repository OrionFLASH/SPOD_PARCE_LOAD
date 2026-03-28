# SPOD PROM - Система обработки и редактирования данных

## Содержание

1. [Общее описание](#общее-описание)
2. [Структура проекта](#структура-проекта)
3. [Навигация по документации](#навигация-по-документации)
4. [Модули src/ (описание и назначение)](#модули-src-описание-и-назначение)
5. [Конфигурация config.json](#конфигурация-configjson)
6. [Программа main.py - Обработка данных](#программа-mainpy---обработка-данных)
7. [Админ-панель - Редактирование данных](#админ-панель---редактирование-данных)
8. [Техническое задание](#техническое-задание)
9. [Анализ входных данных](#анализ-входных-данных)
10. [Установка и запуск](#установка-и-запуск)
11. [Логирование](#логирование)
12. [История версий](#история-версий)

> **Актуальность:** описание пайплайна, `config.json` и ведение вспомогательных файлов в **`Docs/`** синхронизированы; индекс: **`Docs/DOCS_INDEX.md`**. Краткий справочник входных данных и конфигурации: **`Docs/INPUT_DATA_AND_CONFIG_FULL.md`**.

---

## Общее описание

Проект состоит из двух основных компонентов:

1. **main.py** - Основная программа обработки данных из CSV файлов SPOD системы
2. **admin_panel** - Веб-интерфейс для редактирования данных через браузер

Обе программы работают с одними и теми же исходными данными, но выполняют разные задачи:
- `main.py` - читает, обрабатывает, объединяет и записывает данные в Excel
- `admin_panel` - позволяет редактировать данные через удобный веб-интерфейс

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
│   ├── json_utils.py      # Разбор и разворот JSON-полей
│   ├── reward_getcondition_summary.py  # Сводная колонка по getCondition на листе REWARD
│   ├── file_loader.py     # Класс FileLoader — поиск/чтение CSV, разворот JSON
│   ├── tournament.py      # Расчёт статуса турнира (CALC_TOURNAMENT_STATUS)
│   ├── validation.py     # Валидация длины полей, проверка дубликатов
│   ├── gender.py         # Определение пола (AUTO_GENDER)
│   └── main_impl.py      # Полный пайплайн: загрузка, merge, summary, Excel, отчёты
├── requirements.txt        # Зависимости (pandas, openpyxl и др.) для main.py
├── IN/                     # Корень входных данных (paths.input); внутри — subdir (SPOD, FILE и т.д.)
├── OUT/                    # Базовый каталог вывода (paths.output); файлы по дате: OUT/YYYY/DD-MM/
├── EDIT/                   # Копии файлов для редактирования (сессии админ-панели)
├── BACKUP/                 # Резервные копии
├── POST/                   # Снимок для переноса без Git: python src/Tools/sync_post_txt.py — main.py, requirements.txt, config.json, src/**/*.py кроме src/Tools и src/Tests; см. POST/КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt
├── LOGS/                   # Файлы логов (paths.logs); по дате: LOGS/YYYY/DD-MM/
├── Docs/                   # Дополнительная документация; каталог CSV/JSON — Docs/JSON/ (см. README внутри)
├── src/Tools/              # Утилиты: build_spod_input_catalog.py, export_spod_json_examples.py, sync_post_txt.py (заполнение POST/)
│   └── catalog_glossary/   # Фрагменты пояснений к JSON для каталога
├── admin_panel/            # Админ-панель
│   ├── app.py             # Flask приложение
│   ├── config.py          # Конфигурация
│   ├── templates/         # HTML шаблоны
│   ├── static/            # CSS, JS, изображения
│   └── utils/             # Утилиты
│       ├── file_manager.py
│       ├── data_manager.py
│       └── json_editor.py
└── venv/                  # Виртуальное окружение
```

---

## Навигация по документации

Для удобной ориентации по дополнительным материалам используйте единый индекс:

- `Docs/DOCS_INDEX.md` — карта документации и правила актуализации.

Ключевые документы по темам:

- `Docs/INPUT_DATA_AND_CONFIG_FULL.md` — структура входных данных и конфигурация.
- `Docs/CONSISTENCY_CHECKS_FORMAT.md` — формат правил `consistency_checks`.
- `Docs/CONSISTENCY_SAMPLE_FORMAT.md` — формат заполнения колонки `sample`.
- `Docs/АНАЛИЗ_ПРОВЕРОК_КОНСИСТЕНТНОСТИ.md` — аналитика покрытия и предложения по новым правилам.
- `Docs/PERFORMANCE_AND_PARALLELIZATION_HISTORY.md` — консолидированная история оптимизации и распараллеливания.
- `Docs/SUMMARY_GROUP_FIX_HISTORY.md` — история исправлений логики `SUMMARY` и связки `GROUP`.
- `Docs/ADMIN_PANEL_GUIDE.md` — краткий гид по админ-панели.
- `Docs/JSON/` — **каталог входных данных и примеров JSON:** `SPOD_INPUT_DATA_CATALOG.md`, папка `examples/` с реальными JSON из `IN/SPOD`; см. `Docs/JSON/README.md`. Пересборка каталога: `python src/Tools/build_spod_input_catalog.py`; примеры JSON: `python src/Tools/export_spod_json_examples.py`.

---

## Модули src/ (описание и назначение)

Исходный код обработки данных вынесен в каталог **src/**; корневой **main.py** только загружает конфигурацию и запускает пайплайн.

| Модуль | Назначение | Основные сущности |
|--------|------------|-------------------|
| **config_loader.py** | Загрузка и хранение настроек из config.json | Класс `Config`: атрибуты `dir_input`, `dir_output`, `dir_logs`, `input_files`, `summary_sheet`, `sheet_order`, `summary_key_defs`, `summary_key_columns`, `gender_patterns`, `gender_progress_step`, `field_length_validations` (устаревший, пустой dict), `merge_fields_advanced`, `color_scheme`, `column_formats`, `check_duplicates` (устаревший, пустой список), `consistency_checks`, `json_columns`, `reward_getcondition_summary`, `source_export_sort`, `max_workers_io`, `max_workers_cpu`, `tournament_status_choices`; метод `get_output_filename()`. |
| **config_holder.py** | Внедрение текущего конфига для кода, работающего с глобальными переменными | `set_current_config(config)`, `get_current_config()`. |
| **logging_setup.py** | Настройка логирования | Класс `CallerFormatter` (добавляет имя функции в сообщение); функция `setup_logger(config)` — возвращает путь к лог-файлу, настраивает вывод в файл (DEBUG) и консоль (INFO). |
| **json_utils.py** | Разбор и разворот JSON-полей в DataFrame | `safe_json_loads(s)` — парсинг строки в JSON с поправкой типичных ошибок; `safe_json_loads_preserve_triple_quotes(s)`; `flatten_json_column_recursive(df, column, prefix=..., sheet=..., sep=..., max_workers_io=...)` — рекурсивный разворот колонки в несколько колонок, при большом объёме — параллельно. |
| **reward_getcondition_summary.py** | Сводный текст по кодам getCondition на листе REWARD | `add_reward_getcondition_summary_column(df_reward, prefix=..., column_name=...)` — после разворота JSON и merge; строки вида `[код] FULL_NAME {seasonItem}`. |
| **file_loader.py** | Поиск и загрузка CSV, разворот JSON по конфигу | Класс `FileLoader(config)`: `find_file_case_insensitive(directory, base_name, extensions)`, `check_input_files_exist()`, `read_csv_file(file_path)`, `process_single_file(file_conf)` — возвращает `(df, sheet_name, file_conf)` или `(None, sheet_name, None)`. |
| **tournament.py** | Расчёт статуса турнира по датам | `calculate_tournament_status(config, df_tournament, df_report=None)` — добавляет колонку `CALC_TOURNAMENT_STATUS` по правилам из `config.tournament_status_choices`. |
| **validation.py** | Валидация длины полей и проверка дубликатов (устаревшие пути) | `validate_field_lengths(config, df, sheet_name)`, `validate_field_lengths_vectorized(config, df, sheet_name)`, `compare_validate_results`, `mark_duplicates`, `validate_single_sheet`, `check_duplicates_single_sheet`. Основной пайплайн больше не использует отдельные шаги проверки дубликатов и длины полей — всё выполняется в **consistency_checks**. |
| **consistency_checks.py** | Проверки консистентности (unique, field_length, field_format, referential, json_field_equals_column, json_field_in_column) | Выполняет правила из `consistency_checks.rules` **с параллелизацией** (ThreadPoolExecutor). **Фаза 1** — создаёт на листах колонки `unique` («ДУБЛЬ: …»), `field_length`, `field_format`; **Фаза 2** — referential/referential_composite, **json_field_equals_column**, **json_field_in_column** и сбор результатов. Парсинг JSON в ячейках (ADD_DATA и т.п.): **`_parse_add_data_cell(val)`** — замена `"""` на `"`, затем `json.loads`. Сводный лист CONSISTENCY формируется с колонками-описаниями (ТИП ПРОВЕРКИ, Описание, таблица источник, поле источник и т.д.); расхождения по числу полей CSV (csv_columns_count) также попадают в свод. Функции: `_run_unique_check`, `_run_field_length_check`, `_run_field_format_check`, `run_referential`, `run_referential_composite`, `_run_json_field_equals_column`, `_run_json_field_in_column`, `collect_*`, `run_all_consistency_checks`, `run_consistency_checks_and_attach_summary`, `build_consistency_summary_df(results, rules)`. |
| **gender.py** | Определение пола по отчеству, имени, фамилии | `add_auto_gender_column(config, df, sheet_name)`, `add_auto_gender_column_vectorized(config, df, sheet_name)`, `compare_gender_results(df_old, df_new)`. Внутри используются паттерны из `config.gender_patterns`. |
| **main_impl.py** | Полный пайплайн обработки | При импорте вызывается `_load_config_globals()`. Функция `main()`: параллельная загрузка CSV и разворот JSON → **выгрузка source** (`SPOD_PROM source …`) только в режимах **`full`** и отдельно в **`source_only`** (до выхода); в **`main_only`** и **`consistency_only`** source не создаётся → проверка наличия файлов → проверки консистентности на сырых данных и перенос на обработанные листы → добавление AUTO_GENDER (EMPLOYEE) → расчёт статуса турнира → merge (кроме SUMMARY) → **сводка getCondition на REWARD** (`reward_getcondition_summary`, если не `consistency_only`) → **проверки консистентности** (модуль `consistency_checks`) → формирование SUMMARY → лист STAT_FILE → запись основного Excel → итоговый отчёт по отклонениям длины полей и расхождениям CSV. Режим **`consistency_only`**: без merge, gender, турнира и основного Excel — только файл консистентности. Файл **source**: для всех ячеек включён перенос по словам (`write_source_excel`). |

**Запуск:** из корня проекта выполняется `python main.py`. При этом создаётся `Config()` (путь к config.json — корень проекта), конфиг передаётся в `set_current_config(config)`, затем вызывается `main_impl.main()`. В начале `main_impl.main()` снова вызывается `_load_config_globals()`, поэтому все глобальные переменные в main_impl берутся из внедрённого конфига.

---

## Конфигурация config.json

Все параметры обработки данных задаются в файле **config.json** в корне проекта. Программа `main.py` при запуске загружает конфиг и использует его значения. Изменение настроек не требует правки кода.

### Полный перечень секций config.json

| Секция | Назначение |
|--------|------------|
| `run_mode` | Режим запуска: `full`, `source_only`, `main_only`, `consistency_only` (или числа 1–4). |
| `output_filenames` | Имена выходных файлов без расширения: main, source, consistency. |
| `apply_sort_to_source` | Применять ли сортировку из `input_files.sort_columns` при записи source Excel. |
| `apply_sort_to_main` | Применять ли сортировку из `input_files.sort_columns` при записи основного Excel. |
| `paths` | Каталоги: вход (IN/SPOD), выход (OUT), логи (LOGS). Выходные файлы пишутся в подпапки по дате: OUT/YYYY/DD-MM. |
| `logging` | Уровень (INFO/DEBUG) и базовое имя файла логов. |
| `performance` | Количество потоков: max_workers_io, max_workers_cpu. |
| `tournament_status_choices` | Подписи статусов турнира (расчёт CALC_TOURNAMENT_STATUS). |
| `input_files` | Список CSV: file, sheet, expected_columns (0=АВТО), subdir, sort_columns, ширина, freeze, include_in_source. |
| `summary_sheet` | Параметры сводного листа SUMMARY (ширина, закрепление). |
| `sheet_order` | Порядок листов в выходном Excel (если задан). |
| `summary_key_defs` | Ключевые колонки по листам для каркаса SUMMARY (в т.ч. INDICATOR: INDICATOR_CODE, INDICATOR_ADD_CALC_TYPE, CONTEST_CODE). |
| `gender` | Правила автоопределения пола (паттерны отчества/имени/фамилии). |
| `derived_columns` | Производные колонки (pad_left и т.д.). |
| `merge_fields_advanced` | Правила переноса/подсчёта полей между листами. |
| `color_scheme` | Цвета заголовков и ячеек по листам и колонкам. |
| `column_formats` | Формат ячеек: число, дата, выравнивание по листам и колонкам; режимы `columns` и `except_columns` (см. раздел **column_formats**). |
| `consistency_checks` | Единый конфиг проверок консистентности (summary_sheet_name, rules: referential, unique, field_length, field_format). |
| `json_columns` | Колонки с JSON для разворота по листам (column, prefix). |
| `reward_getcondition_summary` | Сводная колонка на листе REWARD по кодам `getCondition` (nonRewards/rewards); `enabled`, `column_name`. |

### Общая структура файла

```json
{
  "run_mode": "full",
  "_run_mode_options": ["full", "source_only", "main_only", "consistency_only"],
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
  "reward_getcondition_summary": { "enabled": true, "column_name": "..." }
}
```

Дополнительные секции (при наличии в файле): `derived_columns`; опционально блок **`source_export`** с **`sort_rules`** для сортировки листов в source Excel — см. класс `Config` в **config_loader.py** и разделы ниже.

---

### run_mode

**Назначение:** режим запуска пайплайна. Задаётся строкой или числом.

| Значение (строка)   | Число | Описание |
|---------------------|-------|----------|
| `"full"`            | 1     | Source Excel + основной Excel (merge, SUMMARY, STAT_FILE) + отдельный файл consistency (CONSISTENCY + листы с нарушениями). |
| `"source_only"`     | 2     | Только source Excel (сырые листы) и выход; остальной пайплайн не выполняется. |
| `"main_only"`       | 3     | Только основной Excel (без source и без отдельного файла consistency). |
| `"consistency_only"`| 4     | Только файл консистентности (CONSISTENCY + листы с нарушениями); **source не создаётся**. |

**Пример:**
```json
"run_mode": "full",
"_run_mode_options": ["full", "source_only", "main_only", "consistency_only"]
```

**Логика:** при старте значение читается из конфига; если указана строка из списка — используется соответствующий код; если число 1–4 — режим по коду. Создаётся **ровно один целевой результат** для режимов `*_only` (быстрый прогон). Файлы пишутся в подкаталог по дате (см. `paths.output`). Для файла **source** во всех ячейках включён **перенос по словам** (openpyxl `wrap_text`).

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

### check_duplicates (удалено из config.json)

**Статус:** секция **удалена** из config.json. Проверка дубликатов полностью перенесена в **consistency_checks**.

Правила задаются в `consistency_checks.rules` правилами с **`type: "unique"`**. В каждом правиле: `sheet`, **`key_columns`** (массив колонок ключа), `output.column_on_sheet` (например «ДУБЛЬ: CONTEST_CODE_GROUP_CODE_REWARD_CODE»). Модуль **consistency_checks** в фазе 1 создаёт на листе колонку с именем из `output.column_on_sheet`, заполняет её признаками дублей (пусто или «xN»); в фазе 2 собирает результаты в свод CONSISTENCY. Итоговая статистика по дубликатам отображается в листе CONSISTENCY и в логе проверок консистентности.

---

### consistency_checks

**Назначение:** единый конфиг **всех** проверок консистентности данных. Выполнение — в модуле **src/consistency_checks.py** (после merge, до формирования SUMMARY). Секции `check_duplicates` и `field_length_validations` в config.json **удалены**: правила уникальности и проверки длины полей задаются только здесь.

| Ключ | Тип | Описание |
|------|-----|----------|
| `summary_sheet_name` | строка | Имя сводного листа (по умолчанию `"CONSISTENCY"`). |
| `rules` | массив объектов | Список правил; у каждого: `id`, `name` (опционально), `type`, `enabled`, `output` (column_on_sheet, include_in_summary). Остальные поля зависят от типа. |

**Порядок выполнения:** **Фаза 1** — для правил с `type: "unique"` и `type: "field_length"` модуль **создаёт** на листах соответствующие колонки («ДУБЛЬ: …» и FIELD_LENGTH_CHECK и т.д.). **Фаза 2** — выполняются referential/referential_composite и **сбор** результатов unique/field_length в свод.

**Типы правил:**

- **referential** — внешний ключ в одну колонку: значения `column_src` на `sheet_src` должны присутствовать в `sheet_ref.column_ref`. Поля: `sheet_src`, `column_src`, `sheet_ref`, `column_ref`. Результат записывается в колонку на листе-источнике («OK» или «НЕТ в &lt;sheet_ref&gt;»).
- **referential_composite** — внешний ключ из нескольких колонок: комбинация `columns_src` на `sheet_src` должна встречаться в `sheet_ref` по `columns_ref`. Поля: `sheet_src`, `columns_src`, `sheet_ref`, `columns_ref`.
- **unique** — уникальность комбинации колонок. В фазе 1 модуль **создаёт** колонку «ДУБЛЬ: …» на листе по полям `sheet`, `key_columns`, `output.column_on_sheet` (например «ДУБЛЬ: CONTEST_CODE_GROUP_CODE_REWARD_CODE»). В ячейках: пусто или «xN». В фазе 2 результат собирается в свод.
- **field_length** — проверка длины полей. В фазе 1 модуль **создаёт** колонку результата на листе по полям `sheet`, `result_column`, **`fields`** (объект: имя поля → `{ "limit": N, "operator": "=" | "<=" | ">=" }`), `output.column_on_sheet`. В ячейках: «-» или строка с описанием нарушений. В фазе 2 результат собирается в свод.
- **field_format** — проверка формата поля (дата, десятичное число с фиксированной дробной частью, строка из N цифр). В фазе 1 создаётся колонка результата на листе по полям `sheet`, `field`, **`format`** (type: date/decimal/fixed_length_digits + параметры). В фазе 2 результат собирается в свод.
- **json_field_equals_column** — значение ключа из JSON-колонки сравнивается с колонкой листа. Применяется к полям вроде REWARD_ADD_DATA (JSON с тройными кавычками). Поля: `sheet`, **`json_column`** (например REWARD_ADD_DATA), **`json_key`** (например parentRewardCode), **`column_compare`** (например REWARD_CODE). Опционально: **`filter_column`** + **`filter_value`** (только строки, где значение колонки совпадает, например REWARD_TYPE=BADGE), **`json_filter_key`** + **`json_filter_value`** (доп. условие по ключу в JSON, например masterBadge=Y). При **`must_not_equal`: true** требование инвертируется: значение из JSON **не должно** равняться колонке (для BADGE с masterBadge=N: parentRewardCode ≠ REWARD_CODE). Парсинг JSON: тройные кавычки `"""` заменяются на `"`, затем `json.loads`. В ячейке: OK / сообщение об ошибке / пусто для неприменимых строк.
- **json_field_in_column** — все уникальные значения ключа из JSON-колонки должны присутствовать в указанной колонке того же листа. Поля: `sheet`, **`json_column`**, **`json_key`**, **`column_in_sheet`** (например REWARD_CODE). Используется для проверки: все parentRewardCode из ADD_DATA должны быть в REWARD_CODE.

**Парсинг ADD_DATA (REWARD и др.):** в модуле **consistency_checks** функция **`_parse_add_data_cell(val)`** разбирает ячейку: `str(val).replace('"""', '"')`, затем `json.loads(normalized)`. Возвращает `dict` или `None` при ошибке. Так обрабатываются поля с тройными кавычками в CSV.

**Проверки на сырых данных:** все правила консистентности (в т.ч. json_field_equals_column и json_field_in_column) выполняются по **сырым** данным до обработки (до merge, до разворота JSON и т.д.); результаты затем копируются на обработанные листы. Число полей в CSV для отчёта берётся до добавления колонок проверок (см. **csv_columns_count**).

**csv_columns_count** (внутри `consistency_checks`): список листов и ожидаемое число полей в CSV (0 = АВТО по заголовку), плюс тексты для колонок листа CONSISTENCY (ТИП ПРОВЕРКИ, Описание, таблица источник, поле источник, параметр сравнения, комментарий). Секция **`_default`** задаёт подписи по умолчанию; **`sheets`** — объект «имя листа» → `{ "expected_columns": 0, опционально переопределение текстов }`. Записи по расхождениям числа полей добавляются в свод CONSISTENCY с заполненными колонками описания. Колонка **sample** на листе CONSISTENCY для этой проверки заполняется **только при наличии отклонений** (номера строк и ожид./факт. число полей); при отсутствии отклонений sample остаётся пустой.

**Вывод:** для каждого правила с `include_in_summary: true` — строка в сводном листе **CONSISTENCY**. На листе CONSISTENCY сначала идут колонки по образцу таблицы проверок: **ТИП ПРОВЕРКИ**, **Описание**, **таблица источник**, **поле источник**, **таблица где проверяем**, **поле для проверки**, **параметр сравнения**, **комментарий** (заполняются из правил конфига), затем колонки результата: check_id, sheet, name, имя_колонки, type, total_rows, violations, sample. В лог INFO — кратко «нарушений не найдено» или «найдено нарушений: …»; в консоль выводится тот же итог.

**Пример фрагмента rules (referential и unique):**
```json
{
  "id": "1.1",
  "name": "CONTEST_CODE из GROUP в CONTEST-DATA",
  "type": "referential",
  "enabled": true,
  "sheet_src": "GROUP",
  "column_src": "CONTEST_CODE",
  "sheet_ref": "CONTEST-DATA",
  "column_ref": "CONTEST_CODE",
  "output": { "column_on_sheet": "ПРОВЕРКА: CONTEST_CODE в CONTEST-DATA", "include_in_summary": true }
},
{
  "id": "4",
  "name": "Уникальность CONTEST_CODE+GROUP_CODE+REWARD_CODE в REWARD-LINK",
  "type": "unique",
  "enabled": true,
  "sheet": "REWARD-LINK",
  "key_columns": ["CONTEST_CODE", "GROUP_CODE", "REWARD_CODE"],
  "output": { "column_on_sheet": "ДУБЛЬ: CONTEST_CODE_GROUP_CODE_REWARD_CODE", "include_in_summary": true }
}
```

Полное описание формата и соответствия пунктам ПРОВЕРКИ.txt — в **Docs/CONSISTENCY_CHECKS_FORMAT.md**.

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
2. **src/main_impl.py** — полный пайплайн: при запуске подхватывает внедрённый конфиг (или при прямом запуске загружает config.json), настраивает логирование, читает CSV из `paths.input`, обрабатывает данные по правилам объединения (`merge_fields_advanced`), запускает **проверки консистентности** (модуль `consistency_checks`: создание колонок unique и field_length, referential/referential_composite, свод CONSISTENCY), формирует сводный лист SUMMARY и лист STAT_FILE, записывает итоговый Excel и выводит отчёт по отклонениям длины полей и расхождениям CSV. Отдельных шагов «проверка дубликатов» и «валидация длины полей» нет — всё в рамках consistency_checks. Логирование ведётся в файл (DEBUG) и в консоль (INFO).

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

Внутри `main_impl.main()` сначала вызывается `_load_config_globals()` — глобальные переменные (DIR_INPUT, INPUT_FILES, MERGE_FIELDS_ADVANCED, SOURCE_EXPORT_SORT и т.д.) заполняются из внедрённого Config. Затем настраивается логирование (`setup_logger()` — использует `logging.level` и `logging.base_name` из конфига): DEBUG в файл, INFO в консоль.

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

После merge выполняется модуль **consistency_checks** (с параллелизацией: пул потоков, блокировка по листу при записи). Создаются колонки «ДУБЛЬ: …» (unique), проверки длины полей (field_length) и формата полей (field_format) на листах, выполняются referential/referential_composite, результаты собираются в сводный лист **CONSISTENCY** и выводятся в лог и консоль. На листе CONSISTENCY выводятся колонки-описания проверок (ТИП ПРОВЕРКИ, Описание, таблица источник, поле источник, таблица где проверяем, поле для проверки, параметр сравнения, комментарий) и колонки результата (check_id, sheet, name, total_rows, violations, sample). Правила задаются в **config.json** в секции `consistency_checks.rules` (типы `unique`, `field_length`, `field_format`, `referential`, `referential_composite`). Секции `check_duplicates` и `field_length_validations` в config больше не используются.

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

#### 5. Итоговая статистика (отклонения длины полей, расхождения по числу полей в CSV)

После записи в Excel программа формирует отчёт и выводит его **в лог и в консоль** (без прерывания работы):

- **Отклонения по длине полей** (по правилам `consistency_checks` с типом `field_length`): для каждого листа с нарушениями — имя листа, колонка результата, число строк с отклонениями, примеры текста нарушений (до 10).
- **Расхождения по числу полей в CSV**: для каждой строки CSV, в которой число полей не совпадает с числом колонок в заголовке — файл, лист, номер строки в файле, ожидаемое и фактическое число полей, направление («больше» или «меньше»). Если таких строк нет — выводится «не обнаружены».

Дубликаты отображаются в сводном листе **CONSISTENCY** и в логе проверок консистентности (не в отдельном блоке итоговой статистики).

Функции: `collect_duplicates_and_validation_report(sheets_data)` — возвращает `(validation_report, csv_mismatch_report)`; `print_final_report(validation_report, csv_mismatch_report)` — вывод обоих блоков в лог и консоль.

#### 6. Лист STAT_FILE (статистика по файлам)

После формирования SUMMARY программа создаёт лист **STAT_FILE** с общей статистикой по исходным CSV-файлам. Для каждого файла из `input_files` выводится: имя файла (`FILE_NAME`), имя листа (`SHEET_NAME`), дата изменения файла (`FILE_DATE`), дата обработки (`DATA_UPDATE_DATE`), количество записей (`ROW_COUNT`), количество колонок (`COL_COUNT`), размер файла в байтах (`FILE_SIZE_BYTES`), статус загрузки (`STATUS`: OK / не найден). Лист добавляется в `sheets_data` и выводится в Excel в порядке, заданном в `sheet_order` (по умолчанию — сразу после SUMMARY).

#### 7. Отдельный файл `STAT_FILE YYYY-MM-DD_HH-MM-SS.xlsx` (время этапов и функций)

В том же выходном каталоге по дате (`OUT/YYYY/DD-MM/`), что и основной Excel, создаётся **отдельная книга** с именем **`STAT_FILE <таймштамп>.xlsx`**: листы **«Сводка»** (режим, старт, общее время прогона `perf_counter`), **«Этапы»** (все блоки `debug_phase` с длительностью и смещением от начала прогона), **«Функции»** (агрегаты `@debug_timed`: вызовы, сумма/среднее/min/max, доля от общего времени, признак `hot`). Длительности в колонках для человека задаются в формате **`ХХ мин. YY сек ZZZ мс`**; рядом дублируются значения в секундах для сортировки и фильтров. Файл формируется в конце успешного прогона (в т.ч. в режимах **source_only** и **consistency_only**).

### Логирование

Программа ведет два уровня логирования:

1. **DEBUG** (в файл):
   - Все операции чтения/записи
   - Детали обработки данных
   - Ошибки с полным stack trace
   - Формат: `дата время - [DEBUG] - сообщение [class: ClassName | def: function_name]`

2. **INFO** (в консоль):
   - Основные этапы выполнения
   - Критические ошибки
   - Итоговая статистика

**Именование логов:**
- Формат: `LOGS_DEBUG_YYYYMMDD_HH_MM.log`
- Расположение: `LOGS/YYYY/DD-MM/` (подпапки по дате, как для OUT)

---

## Админ-панель - Редактирование данных

### Назначение

Веб-интерфейс для редактирования данных из CSV файлов через браузер. Позволяет:
- Просматривать данные всех файлов
- Редактировать записи
- Создавать новые записи
- Удалять записи
- Редактировать JSON поля
- Работать с несколькими сессиями редактирования

### Архитектура

#### Backend (Flask)

**app.py** - Основное Flask приложение:
- REST API для работы с данными
- Управление сессиями редактирования
- Обработка CRUD операций

**Основные endpoints:**
```
GET  /api/sessions              # Список сессий
POST /api/session/new           # Создание новой сессии
POST /api/session/<name>        # Переключение сессии
DELETE /api/session/<name>      # Удаление сессии
GET  /api/files                  # Список файлов
GET  /api/files/<key>/records   # Записи файла
GET  /api/files/<key>/records/<id>  # Одна запись
POST /api/files/<key>/records   # Создание записи
PUT  /api/files/<key>/records/<id>   # Обновление записи
DELETE /api/files/<key>/records/<id> # Удаление записи
```

**config.py** - Конфигурация:
- Динамическое чтение `INPUT_FILES` из `main.py`
- Определение JSON полей
- Зависимости между файлами
- Многострочные поля

**utils/file_manager.py** - Управление файлами:
- Чтение/запись CSV
- Управление сессиями (создание, удаление)
- Резервное копирование

**utils/data_manager.py** - Управление данными:
- CRUD операции
- Валидация данных
- Обработка JSON полей
- Проверка зависимостей

#### Frontend (HTML/CSS/JavaScript)

**templates/index.html** - Главная страница:
- Интерфейс с вкладками для файлов
- Формы редактирования
- Модальные окна для JSON редактора

**static/js/app.js** - Клиентская логика:
- Загрузка и отображение данных
- Управление сессиями
- CRUD операции через API
- JSON редактор

**static/css/style.css** - Стили:
- Современный дизайн
- Адаптивная верстка
- Цветовая схема

### Логика работы админ-панели

#### 1. Инициализация

При загрузке страницы:
1. Загружается список сессий из `EDIT/`
2. Выбирается последняя сессия (или создается новая)
3. Загружается список файлов из `main.py`
4. Создаются вкладки для каждого файла
5. Загружаются записи первого файла

#### 2. Управление сессиями

**Создание сессии:**
1. Создается каталог `EDIT/YYYYMMDD_HHMM/`
2. Копируются все файлы из `SPOD/`
3. Сессия добавляется в список
4. Автоматически выбирается как текущая

**Переключение сессии:**
1. Меняется `current_edit_dir` в `FileManager`
2. Перезагружаются данные
3. Обновляется интерфейс

**Удаление сессии:**
1. Проверяется что сессия не текущая
2. Удаляется каталог со всеми файлами
3. Обновляется список сессий

#### 3. Работа с данными

**Чтение записей:**
- Пагинация (по умолчанию 50 записей на страницу)
- Поиск по всем полям или по конкретному полю (`field:value`)
- Сортировка по колонкам
- Сохранение порядка колонок из исходного файла

**Редактирование записи:**
1. Загружается запись по ID
2. Отображается форма с полями
3. JSON поля отображаются в специальном редакторе
4. При сохранении проверяются зависимости
5. Обновляется файл в текущей сессии

**Создание записи:**
1. Отображается пустая форма
2. Заполняются обязательные поля
3. Проверяются зависимости
4. Добавляется запись в конец файла

**Удаление записи:**
1. Проверяются зависимости (если запись используется в других файлах)
2. Показывается предупреждение
3. Удаляется запись из файла

#### 4. Обработка JSON полей

**Определение JSON полей:**
```python
JSON_FIELDS = {
    "CONTEST-DATA": ["CONTEST_FEATURE", "CONTEST_PERIOD", "BUSINESS_BLOCK"],
    "TOURNAMENT-SCHEDULE": ["TARGET_TYPE", "FILTER_PERIOD_ARR"],
    "INDICATOR": ["INDICATOR_FILTER"],
    "GROUP": ["GROUP_VALUE"],
    "REWARD": ["REWARD_ADD_DATA"]
}
```

**Редактирование JSON:**
- Отдельный редактор для JSON полей
- Валидация JSON структуры
- Поддержка зависимостей (например, структура `REWARD_ADD_DATA` зависит от `REWARD_TYPE`)
- Автодополнение полей и значений

#### 5. Зависимости между файлами

```python
FILE_DEPENDENCIES = {
    "GROUP": {"parent": "CONTEST-DATA", "parent_key": "CONTEST_CODE", "child_key": "CONTEST_CODE"},
    "INDICATOR": {"parent": "CONTEST-DATA", "parent_key": "CONTEST_CODE", "child_key": "CONTEST_CODE"},
    "REWARD-LINK": {"parent": "REWARD", "parent_key": "REWARD_CODE", "child_key": "REWARD_CODE"},
    # ...
}
```

При удалении записи проверяется:
- Используется ли она в дочерних файлах
- Показывается список зависимых записей
- Предлагается удалить зависимые записи

### Логирование админ-панели

**Серверные логи:**
- Формат: `LOGS_DEBUG_admin_panel_YYYYMMDD_HHMM.log`
- Расположение: `LOGS/`
- Уровни: DEBUG (файл), INFO (консоль)

**Клиентские логи:**
- Консоль браузера (F12)
- Логирование всех API запросов
- Детали ошибок

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
   - DEBUG уровень в файл
   - INFO уровень в консоль
   - Формат с указанием функции

### Требования к админ-панели

1. **Функциональность:**
   - Просмотр всех файлов через вкладки
   - CRUD операции для всех записей
   - Редактирование JSON полей
   - Управление сессиями редактирования

2. **Интерфейс:**
   - Современный дизайн
   - Адаптивная верстка
   - Удобная навигация

3. **Валидация:**
   - Проверка обязательных полей
   - Проверка зависимостей
   - Валидация JSON

4. **Безопасность:**
   - Работа только с копиями файлов
   - Резервное копирование
   - Изоляция сессий

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

### Установка админ-панели

```bash
# Переход в каталог админ-панели
cd admin_panel

# Создание виртуального окружения (если нужно)
python3 -m venv venv
source venv/bin/activate

# Установка зависимостей
pip install -r requirements.txt

# Запуск
python app.py
# или
./start.sh
```

Админ-панель будет доступна по адресу: `http://localhost:5001`

---

## Логирование

### Формат логов

**DEBUG уровень (в файл):**
```
2025-11-14 03:02:51,034 | DEBUG | __main__ | <module> | Запуск сервера админ-панели... [class: None | def: <module>]
```

**INFO уровень (в консоль):**
```
2025-11-14 03:02:51,034 | INFO | __main__ | <module> | Запуск сервера админ-панели...
```

### Именование файлов логов

- `main.py`: `LOGS_DEBUG_YYYYMMDD_HH_MM.log`
- `admin_panel`: `LOGS_DEBUG_admin_panel_YYYYMMDD_HHMM.log`

### Расположение

Все логи сохраняются в каталоге `LOGS/` в подпапках по дате: **`LOGS/YYYY/DD-MM/`** (год и день-месяц по 2 цифры, как для выходных файлов в OUT).

### Профилирование в DEBUG (`[PERF]`)

Модуль **`src/debug_timing.py`**: после старта пайплайна в лог-файл пишутся строки **`[PERF]`** — вход/выход отмеченных функций (время вызова, накопленная сумма по функции, номер вызова за прогон), крупные **фазы** пайплайна (`debug_phase`), при завершении процесса — **сводная таблица** по всем функциям (вызовы, сумма/среднее/min/max, пометка `hot` для частых вызовов). На консоль (INFO) выводится краткий **топ** по суммарному времени; полная таблица — только в файле (DEBUG). Дополнительно **`write_performance_statistics_excel`** записывает отдельный файл **`STAT_FILE <таймштамп>.xlsx`** (см. раздел 7 пайплайна).

---

## История версий

### Версия 1.7.9 — Excel `STAT_FILE <таймштамп>.xlsx` со временем этапов и функций

- В **`debug_timing`**: снова накапливаются завершённые фазы **`debug_phase`**; функции **`format_duration_ru`**, **`write_performance_statistics_excel`** формируют книгу с листами «Сводка», «Этапы», «Функции» в каталоге вывода по дате.
- В **`main`**: вызов после режимов **2** и **4** и в конце основного прогона (**1** / **3**), после записи Excel, чтобы в этапы попадала и запись книг.

### Версия 1.7.7 — Детальное DEBUG-логирование производительности (`debug_timing`)

- Добавлен **`src/debug_timing.py`**: декоратор **`@debug_timed`**, контекст **`debug_phase`**, **`reset_run_timing`**, итоговая сводка через **`atexit`**.
- Ключевые функции **`main_impl`**, **`consistency_checks.run_consistency_checks_and_attach_summary`**, **`json_utils.flatten_json_column_recursive`**, **`reward_getcondition_summary`** помечены для учёта времени; «горячие» функции (частые вызовы) — режим **`hot=True`** (агрегат без лога на каждый вызов).

### Версия 1.7.5 — Режимы `*_only` и перенос текста в source

- **`consistency_only`:** больше не создаётся файл **SPOD_PROM source** — только отчёт консистентности. Выгрузка **source** выполняется только в режиме **`full`** (и отдельно при **`source_only`** до выхода).
- **`write_source_excel`:** для всех ячеек листов файла source включены **перенос по словам** и выравнивание по верху.

### Версия 1.7.6 — POST: только программа и config.json

- **`sync_post_txt.py`:** в **POST/** — **`main.py`**, **`requirements.txt`**, **`config.json`** и **`src/**/*.py`**, **исключая** **`src/Tools/`** и **`src/Tests/`**; суффикс **.txt** в имени. **Docs/** и **README.md** не копируются; перед синхронизацией POST очищается (кроме `КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt` и `restore_names_from_txt.bat*`).

### Версия 1.7.4 — Каталог POST (ранее с Docs/)

- Введены **`sync_post_txt.py`**, bat для снятия **.txt** и расширенный состав POST; с **1.7.6** в POST только код и **config.json** (см. выше).

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

**Папка POST:** копии **основной программы и config.json** с суффиксом **.txt** (main.py, requirements.txt, config.json, `src/**/*.py` кроме **Tools** и **Tests**); документация в POST не кладётся.

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
- Создана админ-панель для редактирования данных
- Настроено логирование с двумя уровнями
- Реализована проверка дубликатов по правилам из config
- Итоговая статистика по дубликатам и отклонениям длины полей в лог и консоль в конце работы (без прерывания)
- Добавлена поддержка JSON полей (разворот по json_columns)
- Реализовано управление сессиями редактирования
- **derived_columns** — производные колонки на листе (например, табельный в 20 знаков с лидирующими нулями на LIST-REWARDS); **src_key_transforms** / **dst_key_transforms** в merge — преобразование ключей при связке (pad_20 и т.д.)
- Документация — в README.md и Docs/; в POST/ синхронизируются только код и config.json (см. `sync_post_txt.py`).

**Исправленные проблемы:**
- Исправлены отступы в main.py (basedpyright): find_file_case_insensitive, safe_json_loads, generate_dynamic_color_scheme_from_merge_fields, merge_fields_across_sheets
- Исправлен порядок колонок в админ-панели
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

*Документация обновлена: 2026-03-17*

---

### Версия 1.2 — Проверки консистентности и папка POST (историческая)

**Проверки консистентности (актуально с v1.4 см. выше):**
- Добавлен модуль **src/consistency_checks.py**. С версии 1.4 модуль **сам создаёт** колонки unique и field_length на листах; секции `check_duplicates` и `field_length_validations` в config удалены.
- В **config.json** секция **consistency_checks** (summary_sheet_name, rules) — единственное место задания правил unique и field_length.
- В **config_loader** — атрибут `consistency_checks`; в **main_impl** — вызов `run_consistency_checks_and_attach_summary` после merge, до SUMMARY.

**Папка POST:**
- В **POST/** складываются копии **всех файлов программы** с добавлением к расширению суффикса **.txt** (main.py → main.py.txt, config.json → config.json.txt, README.md → README.md.txt, requirements.txt → requirements.txt.txt, все файлы из src/ → src/имя_файла.py.txt) для переноса программы без репозитория. Существующие файлы в POST заменяются новыми версиями при обновлении.
