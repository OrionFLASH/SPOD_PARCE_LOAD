# Каталог документации SPOD

Документ фиксирует актуальную структуру `Docs/` после консолидации и удаления устаревших отчетов.

## Основные документы

- `INPUT_DATA_AND_CONFIG_FULL.md` — единый справочник по входным данным, структуре листов и параметрам конфигурации.
- `CONSISTENCY_CHECKS_FORMAT.md` — формат правил `consistency_checks` и структура сводного листа `CONSISTENCY`.
- `SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.sql` — зеркало части `consistency_checks.rules` в одном запросе (Hive/Spark: `WITH`, `RLIKE`, `CONCAT_WS`), подробная версия с комментариями. Не вызывается из Python. Результат: блок **SUMMARY** (`passed`, `violation_count`) и блок **DETAIL** (`detail_key`, `detail_message`) без `check_id` / `check_type`. В начале запроса — CTE **`dim_*`** и **`base_schedule_ref`** (меньше повторных чтений одних и тех же таблиц). Таблицы — плейсхолдеры `spod_dq.t_*` (заменить под витрину). В SQL нет правил **`field_format`** (format_*), **`json_*`**, **`json_spod_format`**, отключённых `ref_contest_data_indicator` / `ref_group_indicator` и **`csv_columns_count`** — см. хвост файла.
- `SPOD_CONSISTENCY_CHECKS_SQL_MIRROR_PLAIN.sql` — та же логика SQL-зеркала, но без подробных комментариев (только исполняемый код).
- `SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.md` — отдельное подробное описание скрипта: структура CTE, полный перечень проверок и таблиц/полей, замены схемы и имён, формат вывода, исключения (field_format, json, csv).
- `CONSISTENCY_SAMPLE_FORMAT.md` — актуальный формат поля `sample` по всем типам проверок.
- `INPUT_ARCHIVE_SQLITE_DESIGN.md` — архив **v1** (снимки целого файла): `src/input_archive_sqlite.py`, **`JSON_*`** через `src/archive_json_columns.py`, БД **`OUT/DB/spod_input_archive.sqlite`**, отчёт **`print_input_archive_sqlite_report`**.
- `INPUT_ARCHIVE_ROW_LEVEL.md` — архив **v2 (построчно)**, реализован: `row_level_archive`, `src/input_archive_sqlite_v2.py`, `input_archive_row_hash.py`, `input_archive_row_parallel.py`, **`src/csv_headers.py`** (BOM/сопоставление заголовков), БД **`OUT/DB/spod_input_archive_v2.sqlite`**, отчёт **`print_input_archive_row_report`**.
- `INPUT_ARCHIVE_ROW_LEVEL_PLAN.md` — план и таблица **`row_key_columns`** по листам (справочник).
- `RATING_MATRIX_COLORS_AND_LOGIC.md` — лист RATING: подсчёт, доступность, раскраска матрицы ITEM (в т.ч. itemAmount и шапка колонки).
- `SEASON_ORDER_SUMMARY.md` — лист **ORDER-SEASON-SUMMARY**: сводка заказов по группам **SEASON_*** (`item_order_groups`), остаток склада, счётчики КМ.
- `АНАЛИЗ_ПРОВЕРОК_КОНСИСТЕНТНОСТИ.md` — аналитика покрытия проверок и предложения по расширению.

## Консолидированные исторические документы

- `PERFORMANCE_AND_PARALLELIZATION_HISTORY.md` — единая история оптимизаций/параллелизации и сравнения производительности (вместо множества версионных отчетов).
- `SUMMARY_GROUP_FIX_HISTORY.md` — история исправлений логики формирования `SUMMARY` и связки `GROUP_CODE`/`GROUP_VALUE`.

## Каталог CSV и JSON `IN/SPOD` — папка `Docs/JSON/`

- **`Docs/JSON/README.md`** — назначение каталога, список команд пересборки.
- **`Docs/JSON/SPOD_INPUT_DATA_CATALOG.md`** — единый документ: оглавление по файлам; для каждого CSV — назначение колонок, статистика значений; для **REWARD** и **CONTEST** — разбор JSON (`REWARD_TYPE` / `CONTEST_TYPE`) и встроенные пояснительные справочники полей.
- **`Docs/JSON/examples/`** — по одному JSON на каждый соответствующий CSV выгрузки в `IN/SPOD` (те же базовые имена файлов).

Пересборка каталога: `python src/Tools/build_spod_input_catalog.py`. Обновление примеров JSON: `python src/Tools/export_spod_json_examples.py`. Тексты глоссариев: `src/Tools/catalog_glossary/`.

## Специализированные материалы

- `EXCEL_FEATURES_EXAMPLES.md` — примеры работы с валидациями/формулами в Excel.

## Снимок POST для переноса без Git (`Docs/POST_SNAPSHOT/`)

- Шаблоны **`КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt`** и **`restore_names_from_txt.bat`** копируются скриптом **`python src/Tools/sync_post_txt.py`** в корень локального **`POST/`** (каталог **`POST/`** в **`.gitignore`**).
- В **`POST/`** после синхронизации: **`main.py.txt`**, **`config.json.txt`**, **`README.md.txt`**, **`requirements.txt.txt`**, **`src/**/*.py.txt`** (без **Tools**/**Tests**), дерево **`POST/Docs/**`** (копия **`Docs/`** без **`Docs/POST_SNAPSHOT/`**, у каждого файла суффикс **`.txt`**). Подробности — **`КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt`**, **`README.md`** (раздел **«Каталог POST»**).

## Правила актуализации

- **Источник истины по продукту:** корневой **`README.md`** (ТЗ, пайплайн, `config.json`, логирование, история версий). Разделы **`column_formats`** (в т.ч. `except_columns`, лист **STATISTICS**), **`reward_getcondition_summary`**, **`rating_item_matrix`** (в т.ч. **`ignoreConditions`**), **`season_order_summary`** (лист **ORDER-SEASON-SUMMARY**), **`consistency_checks`** (в т.ч. **`json_spod_format`**, фильтры **referential**, **`enabled: false`**, смысловые **`id`**) и **«Каталог POST»** описывают актуальное поведение Excel, листов REWARD/RATING и снимка для переноса без Git.
- После обновления CSV в `IN/SPOD/` пересобрать **`Docs/JSON/SPOD_INPUT_DATA_CATALOG.md`**: `python src/Tools/build_spod_input_catalog.py`; обновить примеры в **`Docs/JSON/examples/`**: `python src/Tools/export_spod_json_examples.py`; при смене схемы JSON править **`src/Tools/catalog_glossary/`**.
- Справочник **`INPUT_DATA_AND_CONFIG_FULL.md`** держать согласованным с `README.md` по ключевым блокам конфигурации (п. 3), в т.ч. **`run_outputs`** (массив `source_only` / `main_only` / `consistency_only`) и при необходимости устаревший **`run_mode`**.
- Новые изменения по консистентности вносить сначала в `README.md`, затем синхронно в `CONSISTENCY_CHECKS_FORMAT.md`, `CONSISTENCY_SAMPLE_FORMAT.md` и при необходимости **`INPUT_DATA_AND_CONFIG_FULL.md`**. Для **`json_spod_format`** детали поведения и формата сообщений на листе — п. **2.8** в `CONSISTENCY_CHECKS_FORMAT.md`. При добавлении или переименовании правил с типами `referential`, `referential_composite`, `unique`, `field_length` по возможности обновлять комментарии и логику в `SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.sql` (соответствие `rules[].id` / `name`). Правила **`field_format`**, **`json_*`**, **`json_spod_format`** в SQL-зеркале не ведутся — только в Python и конфиге.
- Снимок для переноса без Git: `python src/Tools/sync_post_txt.py` — полная пересборка **POST/**: код, **config.json**, **README.md**, **requirements.txt**, дерево **Docs/** (без **Docs/POST_SNAPSHOT** внутри копии), суффикс `.txt` к именам; из **`Docs/POST_SNAPSHOT/`** в корень **POST/** — инструкция и **bat**; **POST/** в **`.gitignore`**. Подробно — **`README.md`**, раздел **«Каталог POST»**.
- Для крупных блоков изменений использовать консолидированные документы, а не создавать новые `*_V2`, `*_FINAL`, `*_FULL` файлы.
- Исторические документы с пересекающимся содержимым объединять и удалять дубли.
