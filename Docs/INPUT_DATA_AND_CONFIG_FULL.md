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
- **`input_archive_sqlite`** — опциональная запись сырых входных CSV в локальный файл SQLite после чтения (без отдельного сервера). Ключи: **`enabled`**, **`db_path`**, **`input_files[].archive_db_path`** (отдельный файл БД для части входов), **`default_archive_to_db`**, **`input_files[].archive_to_db`**, **`reuse_matching_historical_snapshot`**, **`reporting`**, **`use_sha256_for_identity`**, мета-таблицы. Подробно: **`Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md`**, **`README.md`** (в т.ч. **1.7.28**, **1.7.33**).
- **`run_outputs`** — массив строк: `source_only`, `main_only`, `consistency_only`. Перечисляются **какие выходные файлы** создавать (можно сочетать). Эквивалент бывшего **full**: все три значения в массиве. Если в массиве **только** `source_only` — выгрузка source и выход без основного пайплайна. **Только** `main_only` — основной Excel без source и без отдельного файла consistency. **Только** `consistency_only` — отдельная книга консистентности без merge (как старый режим 4). Сочетание `main_only` + `consistency_only` — основной Excel и затем отдельный файл consistency. При отсутствии **`run_outputs`** читается устаревший **`run_mode`** (`full` / `source_only` / … или число 1–4); разбор — **`config_loader.parse_run_outputs_config`**. Файл source: перенос по словам (`write_source_excel`).
- `consistency_checks` — правила проверок консистентности и сводный лист.
- `merge_fields_advanced` — правила переноса/объединения полей между листами.
- `summary_sheet` / `summary_key_defs` — формирование листа `SUMMARY`.
- `column_formats` — форматы ячеек Excel по листам: списки `columns` или режим **`except_columns`** (формат ко всем колонкам кроме перечисленных). Для чисел после чтения CSV применяются нормализация имён заголовков (BOM, NFKC) и разбор чисел с разрядами (пробел/NBSP). Подробно: **README.md**, раздел **column_formats**.
- `json_columns` — какие колонки с JSON разворачивать в плоские поля по листам (`column`, `prefix`).
- `reward_getcondition_summary` — опциональная сводная колонка на листе **REWARD** по кодам из `getCondition` (аналог СЦЕПИТЬ/ВПР); ключи `enabled`, `column_name`. Подробно: **README.md**, раздел **reward_getcondition_summary**.
- **`rating_item_matrix`** — матрица наград **ITEM** на листе **RATING**: столбцы со счётчиками заказов (**ORDER**) и подсветка ячеек (**светло-зелёный** / **светло-красный**) по доступности товара менеджеру из JSON **`REWARD_ADD_DATA`** (пороги **`minRating*`**, **`minCrystalEarnedTotal`**, коды **`rewardCode`** / **`nonRewardCode`** с листов **LIST-REWARDS** и **ORDER**). Ключи: **`enabled`**, имена листов и столбцов (**`sheet_rating`**, **`order_employee_col`**, **`crystals_col`**, **`sheet_list_rewards`**, **`list_rewards_*`**, **`fill_accessibility_*`**, **`reward_add_data_col`** и др.). Реализация: **`src/rating_item_matrix.py`**, **`src/reward_item_catalog.py`**. Полная таблица полей — **README.md**, раздел **rating_item_matrix**.
- `logging` — уровни, имя логов и формат.

### 3.1. Локальный снимок POST (без коммита в Git)

- Каталог **`POST/`** в **`.gitignore`**. Команда **`python src/Tools/sync_post_txt.py`** (из корня репозитория) полностью пересоздаёт **POST/**: копии **`main.py`**, **`config.json`** и **`src/**/*.py`** (без **`Tools`** и **`Tests`**) с добавлением **`.txt`** к имени файла (**`main.py.txt`**, **`src/main_impl.py.txt`** и т.д.); из **`Docs/POST_SNAPSHOT/`** — **`КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt`** и **`restore_names_from_txt.bat`**. **README**, **requirements**, **Docs/** в снимок не входят. Подробно: **README.md**, раздел **«Каталог POST»**, **`Docs/DOCS_INDEX.md`** (§ POST_SNAPSHOT).

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
