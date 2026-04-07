# Каталог документации SPOD

Документ фиксирует актуальную структуру `Docs/` после консолидации и удаления устаревших отчетов.

## Основные документы

- `INPUT_DATA_AND_CONFIG_FULL.md` — единый справочник по входным данным, структуре листов и параметрам конфигурации.
- `CONSISTENCY_CHECKS_FORMAT.md` — формат правил `consistency_checks` и структура сводного листа `CONSISTENCY`.
- `SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.sql` — зеркало части `consistency_checks.rules` в одном запросе (Hive/Spark: `WITH`, `RLIKE`, `CONCAT_WS`), подробная версия с комментариями. Не вызывается из Python. Результат: блок **SUMMARY** (`passed`, `violation_count`) и блок **DETAIL** (`detail_key`, `detail_message`) без `check_id` / `check_type`. В начале запроса — CTE **`dim_*`** и **`base_schedule_ref`** (меньше повторных чтений одних и тех же таблиц). Таблицы — плейсхолдеры `spod_dq.t_*` (заменить под витрину). В SQL нет правил **`field_format`** (format_*), `json_*`, отключённых `ref_contest_data_indicator` / `ref_group_indicator` и `csv_columns_count` — см. хвост файла.
- `SPOD_CONSISTENCY_CHECKS_SQL_MIRROR_PLAIN.sql` — та же логика SQL-зеркала, но без подробных комментариев (только исполняемый код).
- `SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.md` — отдельное подробное описание скрипта: структура CTE, полный перечень проверок и таблиц/полей, замены схемы и имён, формат вывода, исключения (field_format, json, csv).
- `CONSISTENCY_SAMPLE_FORMAT.md` — актуальный формат поля `sample` по всем типам проверок.
- `INPUT_ARCHIVE_SQLITE_DESIGN.md` — опциональный архив сырых входных CSV в SQLite (`config.input_archive_sqlite`, модули `src/input_archive_sqlite.py`, `src/archive_json_columns.py` — колонки **`JSON_*`** для **CONTEST-DATA** / **REWARD**, в т.ч. **`UPDATE`** при отсутствии нового снимка). Файл БД по умолчанию: **`OUT/DB/spod_input_archive.sqlite`** (см. **`db_path`** в `config.json`).
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

- Шаблоны **`КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt`** и **`restore_names_from_txt.bat`** копируются скриптом **`python src/Tools/sync_post_txt.py`** в локальный каталог **`POST/`** (каталог **`POST/`** в **`.gitignore`**, в репозиторий не коммитится).
- В **`POST/`** после синхронизации: **`main.py.txt`**, **`config.json.txt`**, **`src/**/*.py.txt`** (без **Tools** и **Tests**). **README**, **requirements**, **Docs/** в POST не входят — см. **`README.md`**, раздел **«Каталог POST»**.

## Правила актуализации

- **Источник истины по продукту:** корневой **`README.md`** (ТЗ, пайплайн, `config.json`, логирование, история версий). Разделы **`column_formats`** (в т.ч. `except_columns`, лист **STATISTICS**), **`reward_getcondition_summary`**, **`rating_item_matrix`** и **«Каталог POST»** описывают актуальное поведение Excel, листов REWARD/RATING и снимка для переноса без Git.
- После обновления CSV в `IN/SPOD/` пересобрать **`Docs/JSON/SPOD_INPUT_DATA_CATALOG.md`**: `python src/Tools/build_spod_input_catalog.py`; обновить примеры в **`Docs/JSON/examples/`**: `python src/Tools/export_spod_json_examples.py`; при смене схемы JSON править **`src/Tools/catalog_glossary/`**.
- Справочник **`INPUT_DATA_AND_CONFIG_FULL.md`** держать согласованным с `README.md` по ключевым блокам конфигурации (п. 3), в т.ч. **`run_outputs`** (массив `source_only` / `main_only` / `consistency_only`) и при необходимости устаревший **`run_mode`**.
- Новые изменения по консистентности вносить сначала в `README.md`, затем синхронно в `CONSISTENCY_CHECKS_FORMAT.md` и `CONSISTENCY_SAMPLE_FORMAT.md`. При добавлении или переименовании правил с типами `referential`, `referential_composite`, `unique`, `field_length` по возможности обновлять комментарии и логику в `SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.sql` (соответствие `rules[].id` / `name`). Правила **`field_format`** в SQL-зеркале не ведутся — только в Python и конфиге.
- Снимок для переноса без Git: `python src/Tools/sync_post_txt.py` — полная пересборка локального **POST/**: `main.py`, `config.json` и `src/**/*.py` (без Tools/Tests) с суффиксом `.txt` в имени файла; из **`Docs/POST_SNAPSHOT/`** — инструкция и **bat**; **POST/** в **`.gitignore`**. Подробно — **`README.md`** (раздел **«Каталог POST»**).
- Для крупных блоков изменений использовать консолидированные документы, а не создавать новые `*_V2`, `*_FINAL`, `*_FULL` файлы.
- Исторические документы с пересекающимся содержимым объединять и удалять дубли.
