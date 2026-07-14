# Входные данные и конфигурация SPOD (актуализировано)

Единый справочник по структуре входных файлов, связям и ключевым параметрам конфигурации.

## 1. Входные данные

- Источник: CSV-файлы из каталога входных данных (путь задается в конфигурации).
- Файлы загружаются в листы Excel согласно сопоставлению `file -> sheet`.
- Перед запуском проверяется наличие обязательных файлов.
- **Детальный каталог полей и значений** по CSV в `IN/SPOD/<BLOCK>/` (блоки **PROM** / **IFT** / **PSI**; все колонки, варианты, JSON `REWARD_ADD_DATA` / `CONTEST_FEATURE`): **`Docs/JSON/SPOD_INPUT_DATA_CATALOG.md`**, примеры JSON — **`Docs/JSON/examples/`** (см. **`Docs/JSON/README.md`**). Пересборка: `python src/Tools/build_spod_input_catalog.py`; обновление примеров: `python src/Tools/export_spod_json_examples.py`. Пояснения к ключам JSON — `src/Tools/catalog_glossary/`.

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
- **`input_archive_sqlite`** — опциональная запись сырых входных CSV в локальный файл SQLite после чтения (без отдельного сервера). Ключи: **`enabled`**, **`db_path`**, **`input_files[].archive_db_path`**, **`default_archive_to_db`**, **`input_files[].archive_to_db`**, **`reuse_matching_historical_snapshot`**, **`reporting`**, **`use_sha256_for_identity`**, мета-таблицы. В **консоли** и **стартовой строке лога** путь к БД выводится **полностью**, относительно корня проекта (без усечения «…»); блок по листам — в виде таблицы (**`console_ui.print_input_archive_sqlite_report`**). Подробно: **`Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md`**, **`README.md`** (**1.7.38**, **1.7.28**, **1.7.33**).
- **`run_outputs`** — массив строк: `source_only`, `main_only`, `consistency_only`, **`manager_stats_only`**, **`stat_file_only`**, **`rating_item_matrix`**, **`season_order_summary`**. Перечисляются **какие выходные артефакты и шаги** выполнять (можно сочетать). Эквивалент бывшего **full**: `source_only` + `main_only` + `consistency_only`. **Матрица ITEM** на RATING и лист **ORDER-SEASON-SUMMARY** — только при токенах **`rating_item_matrix`** / **`season_order_summary`** (дополнительно к `enabled` в секциях конфига). **Только** `manager_stats_only` — ранний выход после merge. Сочетание `main_only` + `consistency_only` — основной Excel и отдельный файл consistency. При отсутствии **`run_outputs`** читается устаревший **`run_mode`**; разбор — **`config_loader.parse_run_outputs_config`**. Файл source: перенос по словам (`write_source_excel`).
- **`run_blocks`** — массив блоков **`PROM`** / **`IFT`** / **`PSI`**: какие наборы SPOD-CSV обрабатывать (по умолчанию `["PROM"]`). Вход: **`IN/SPOD/<BLOCK>/`**; выход: **`OUT/<BLOCK>/YYYY/DD-MM/`**, имена **`SPOD_<BLOCK> …`**. У записей **`input_files`** поле **`block`**; без него файл общий (обычно `FILE`). Разбор — **`parse_run_blocks_config`**.
- **`consistency_checks`** — **`summary_sheet_name`**, **`rules`**, **`csv_columns_count`**; опционально **`spod_todo_config_guide`**. Типы правил: **unique**, **field_length**, **field_format**, **referential** / **referential_composite** (с **`src_row_conditions`** / **`ref_row_conditions`**), **json_field_***, **json_priority_unique_per_contest_link**, **json_spod_format** (**`src/json_spod_format_check.py`**: SPOD-разбор, BOM/NBSP, короткие сообщения в колонке проверки — см. **`Docs/CONSISTENCY_CHECKS_FORMAT.md`**, п. **2.8**). Правила с **`enabled: false`** попадают в свод **CONSISTENCY** без выполнения проверки. Смысловые **`id`** базовых правил — см. **`Docs/CONSISTENCY_CHECKS_FORMAT.md`**, п. 6. Полное описание — **`README.md`**, раздел **consistency_checks**.
- `merge_fields_advanced` — правила переноса/объединения полей между листами.
- `summary_sheet` / `summary_key_defs` — формирование листа `SUMMARY`.
- `column_formats` — форматы ячеек Excel по листам: списки `columns` или режим **`except_columns`** (формат ко всем колонкам кроме перечисленных). Для чисел после чтения CSV применяются нормализация имён заголовков (BOM, NFKC) и разбор чисел с разрядами (пробел/NBSP). Подробно: **README.md**, раздел **column_formats**.
- `json_columns` — какие колонки с JSON разворачивать в плоские поля по листам (`column`, `prefix`).
- `reward_getcondition_summary` — опциональная сводная колонка на листе **REWARD** по кодам из `getCondition` (аналог СЦЕПИТЬ/ВПР); ключи `enabled`, `column_name`. Подробно: **README.md**, раздел **reward_getcondition_summary**.
- **`rating_item_matrix`** — матрица **ITEM** на **RATING**: счётчики по **ORDER**, подсветка по критериям из **`REWARD_ADD_DATA`** и листов **LIST-REWARDS** / **ORDER**; шаг выполняется только при токене **`rating_item_matrix`** в **`run_outputs`**. Реализация: **`src/rating_item_matrix.py`**, **`src/reward_item_catalog.py`**. Поля конфига — **README.md**, раздел **rating_item_matrix**.
- **`season_order_summary`** — лист **ORDER-SEASON-SUMMARY** по **`item_order_groups`**; только при токене **`season_order_summary`** в **`run_outputs`**. Реализация: **`src/season_order_summary.py`**. Подробно: **`Docs/SEASON_ORDER_SUMMARY.md`**.
- `logging` — уровни, имя логов и формат.

### 3.1. Локальный снимок POST (без коммита в Git)

- Каталог **`POST/`** в **`.gitignore`**. Команда **`python src/Tools/sync_post_txt.py`** (из корня репозитория) полностью пересоздаёт **POST/**: копии **`main.py`**, **`config.json`** и **`src/**/*.py`** (без **`Tools`** и **`Tests`**) с добавлением **`.txt`** к имени файла (**`main.py.txt`**, **`src/main_impl.py.txt`** и т.д.); из **`Docs/POST_SNAPSHOT/`** — **`КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt`** и **`restore_names_from_txt.bat`**. **README**, **requirements**, **Docs/** в снимок не входят. Подробно: **README.md**, раздел **«Каталог POST»**, **`Docs/DOCS_INDEX.md`** (§ POST_SNAPSHOT).

## 4. JSON-поля

В проекте есть JSON-поля, часть из них разворачивается в колонки, часть валидируется правилами консистентности. Для проверки актуального набора ориентироваться на:

- `config.json` — какие колонки считаются JSON и как проверяются;
- **`src/json_utils.py`**, **`src/consistency_checks.py`**, **`src/json_spod_format_check.py`** — парсинг и валидация JSON / SPOD-JSON.

## 5. Что считается источником истины

- Для структуры и параметров: `config.json`.
- Для поведения: код в `src/`.
- Для пользовательского описания: `README.md`.

## 6. Примечание о прошлых версиях документации

Ранее информация была разнесена между несколькими файлами и частично содержала устаревшие параметры. После консолидации этот файл заменяет старые разрозненные описания и должен поддерживаться синхронно с `README.md` и `config.json`.
