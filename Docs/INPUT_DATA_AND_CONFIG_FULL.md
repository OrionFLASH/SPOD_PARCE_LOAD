# Входные данные и конфигурация SPOD (актуализировано)

Единый справочник по структуре входных файлов, связям и ключевым параметрам конфигурации.

## 1. Входные данные

- Источник: CSV-файлы из каталога входных данных (путь задается в конфигурации).
- Файлы загружаются в листы Excel согласно сопоставлению `file -> sheet`.
- Перед запуском проверяется наличие обязательных файлов.
- **Детальный каталог полей и значений** по CSV в `IN/SPOD/` (все колонки, варианты, JSON `REWARD_ADD_DATA` / `CONTEST_FEATURE`): **`Docs/JSON/SPOD_INPUT_DATA_CATALOG.md`**, примеры JSON — **`Docs/JSON/examples/`** (см. **`Docs/JSON/README.md`**). Пересборка: `python src/Tools/build_spod_input_catalog.py`; обновление примеров: `python src/Tools/export_spod_json_examples.py`. Пояснения к ключам JSON — `src/Tools/catalog_glossary/`.

## 2. Базовые листы и связи

- `CONTEST-DATA` — базовый справочник конкурсов (`CONTEST_CODE`).
- `GROUP` — группы конкурса (связь по `CONTEST_CODE`).
- `INDICATOR` — индикаторы конкурса (связь по `CONTEST_CODE`).
- `TOURNAMENT-SCHEDULE` — турниры конкурса (связь по `CONTEST_CODE`).
- `REPORT` — отчетные строки, связь с конкурсом/турниром.
- `REWARD` — справочник наград (`REWARD_CODE`).
- `REWARD-LINK` — связь конкурс/группа/награда.

## 3. Конфигурация (ключевые блоки)

- `paths` — директории входа/выхода/логов.
- **`input_archive_sqlite`** — опциональная запись сырых входных CSV в локальный файл SQLite после чтения (без отдельного сервера). Ключи: **`enabled`**, **`db_path`** (по умолчанию относительно корня проекта **`OUT/DB/spod_input_archive.sqlite`** — каталог **`OUT/DB`** создаётся при необходимости; **`OUT/`** в репозиторий не коммитится), **`default_archive_to_db`**, **`input_files[].archive_to_db`**, **`reuse_matching_historical_snapshot`** (при SHA — без лишних снимков при возврате к старому содержимому файла), **`reporting.console`** / **`reporting.log`**, **`use_sha256_for_identity`**, сводка **`archive_file_inventory`** и снимки **`archive_file_snapshot`**. Колонки **`JSON_*`** для **CONTEST-DATA** / **REWARD** синхронизируются (**`ALTER`** / **`UPDATE`**) и при пропуске ingest — см. **`README.md` 1.7.28**. Подробно: **`Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md`**, **`Docs/JSON/SPOD_INPUT_DATA_CATALOG.md`** (JSON колонок) и **`README.md`** (история версий **1.7.21**–**1.7.28**).
- **`run_outputs`** — массив строк: `source_only`, `main_only`, `consistency_only`. Перечисляются **какие выходные файлы** создавать (можно сочетать). Эквивалент бывшего **full**: все три значения в массиве. Если в массиве **только** `source_only` — выгрузка source и выход без основного пайплайна. **Только** `main_only` — основной Excel без source и без отдельного файла consistency. **Только** `consistency_only` — отдельная книга консистентности без merge (как старый режим 4). Сочетание `main_only` + `consistency_only` — основной Excel и затем отдельный файл consistency. При отсутствии **`run_outputs`** читается устаревший **`run_mode`** (`full` / `source_only` / … или число 1–4); разбор — **`config_loader.parse_run_outputs_config`**. Файл source: перенос по словам (`write_source_excel`).
- `consistency_checks` — правила проверок консистентности и сводный лист.
- `merge_fields_advanced` — правила переноса/объединения полей между листами.
- `summary_sheet` / `summary_key_defs` — формирование листа `SUMMARY`.
- `column_formats` — форматы ячеек Excel по листам: списки `columns` или режим **`except_columns`** (формат ко всем колонкам кроме перечисленных). Для чисел после чтения CSV применяются нормализация имён заголовков (BOM, NFKC) и разбор чисел с разрядами (пробел/NBSP). Подробно: **README.md**, раздел **column_formats**.
- `json_columns` — какие колонки с JSON разворачивать в плоские поля по листам (`column`, `prefix`).
- `reward_getcondition_summary` — опциональная сводная колонка на листе **REWARD** по кодам из `getCondition` (аналог СЦЕПИТЬ/ВПР); ключи `enabled`, `column_name`. Подробно: **README.md**, раздел **reward_getcondition_summary**.
- `logging` — уровни, имя логов и формат.

## 4. JSON-поля

В проекте есть JSON-поля, часть из них разворачивается в колонки, часть валидируется правилами консистентности. Для проверки актуального набора ориентироваться на:

- `config.json` — какие колонки считаются JSON и как проверяются;
- `src/json_utils.py` и `src/consistency_checks.py` — фактическая логика парсинга/валидации.

## 5. Что считается источником истины

- Для структуры и параметров: `config.json`.
- Для поведения: код в `src/`.
- Для пользовательского описания: `README.md`.

## 6. Примечание о прошлых версиях документации

Ранее информация была разнесена между несколькими файлами и частично содержала устаревшие параметры. После консолидации этот файл заменяет старые разрозненные описания и должен поддерживаться синхронно с `README.md` и `config.json`.
