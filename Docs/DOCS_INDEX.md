# Каталог документации SPOD

Актуальная карта `Docs/` после ревизии (2026-07-15). Устаревшие планы, дубли и черновики удалены.

## Источник истины

| Тема | Где смотреть |
|------|----------------|
| Продукт, пайплайн, changelog | корневой **`README.md`** |
| Раскладка и параметры `config/` | **`CONFIG_FILES.md`** |
| ToDo / статусы работ | **`ROADMAP.md`** (корень) |
| Утилита сбора REPORT из `IN/REPORT` | **`folder_parce.py`** + **`config_folder_parce.json`** (описание — README, раздел «Утилита folder_parce.py») |

## Конфигурация и данные

- `CONFIG_FILES.md` — каталог `config/`, `$include`, все `CONFIG_*.json`, примеры.
- `BLOCKS_MIGRATION.md` — шпаргалка переноса `IN/<BLOCK>/…` и SQLite по блокам.
- `IN_OUT_DATA_POLICY.md` — политика: не удалять `IN/`/`OUT/` без явного разрешения.
- `JSON/README.md` + `JSON/SPOD_INPUT_DATA_CATALOG.md` — каталог полей входных CSV/JSON (пересборка Tools).
- `params_catalog/README.md` + `params_catalog/SPOD_PARAMS_CATALOG_LEAF_v3.xlsx` — Excel-перечень колонок и конечных JSON-ключей + оформление из `CONFIG_FORMATS` (v2 сохранён).
- `CONTEST_BADGE_FORM.md` — Excel-форма конкурса BADGE (export/import через `run_outputs`).
- `CONTEST_BADGE_FORM_FILLING.md` — **заполнение шаблона**: цвета, порядок полей, типичные ошибки, цикл export→import.
- `CONTEST_BADGE_FORM_PARAM_REVIEW.md` — stub: актуальный MD-снимок в **`common/param_catalog_review/`**.
- Редактор описаний: **`common/web-edit/`** (данные **`common/param_catalog_review/catalog.json`**). Сборка: `python src/Tools/build_param_review_editor.py`. Длинные списки (≥16 вариантов, в т.ч. `INDICATOR_CODE`) — combobox сверху карточки. Метки **ПКАП / ФАБРИКА** — поле `marks[]` в каталоге (после `allow_empty`), UI в web-edit / web-edit-full.
- Полный каталог (скан PROM SPOD): **`common/web-edit-full/`** — `README.md`, `game_edit_catalog.json`; пересборка `python src/Tools/build_web_edit_full_catalog.py`.
- Заполнение параметров SPOD: **`common/web-fill/`** (однофайловый HTML) и **`common/web-fill-full/`** (html + css + js + catalog). С 16.16 UX дорабатывается только в fill-full.
- Примеры снимков JSON для импорта: **`common/examples/`** (`README.md`, подпапки `web-fill/{curated,badges,contests}/`). Каталоги UI и «Сохранить JSON» остаются в папках приложений.
- `PLAN_CONTEST_BADGE_FORM.md` — согласованный план формы BADGE.
- `PLAN_WEB_FILL.md` — план fill / fill-full. Пункт 16 выполнен (16.16–16.18 — только fill-full). Снимки `common/examples/web-fill/` из CSV `CONFIG_RUN_INPUT.json`.
- `TODO_WEB_FILL.md` — чеклист к пункту 16 ROADMAP.
- `PLAN_WEB_FILL_FULL.md` — план волны fill-full (выбор выгрузки, бизнес-блок, ITEM/`r_`, полный код в JSON, разбиение JS). Пункт **18** выполнен в fill-full.
- `TODO_WEB_FILL_FULL.md` — чеклист к пункту 18. Пожелания: `common/ToDo FILL EDIT.txt`.
- `PLAN_WEB_FILL_STANDS.md` — стенды PROM/PSI, merge, фильтр стенда. Пункт **19** (не перезаписывает 18).
- `TODO_WEB_FILL_STANDS.md` — чеклист к пункту 19.
- `PLAN_WEB_FILL_EDIT_WAVE20.md` — волна 20: тексты каталога, JSON-пусто, зависимости ключей (fill-full + edit-full).
- `TODO_WEB_FILL_EDIT_WAVE20.md` — чеклист к пункту 20. Пожелания: `common/ToDo FILL EDIT.txt`.
- Шаблон BLANK: **`common/templates/CONTEST_BADGE_FORM/CONTEST_BADGE_FORM_BLANK.xlsx`** (stub в `Docs/templates/…`).

## Консистентность

- `CONSISTENCY_CHECKS_FORMAT.md` — типы правил, поля, id, лист CONSISTENCY (п. 2.8: обёртка `"`, `array_value_keys`).
- `CONSISTENCY_SAMPLE_FORMAT.md` — формат колонки `sample`.
- `SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.md` + `.sql` (+ `_PLAIN.sql`) — SQL-зеркало части правил (не из Python).

## Архив SQLite

- `INPUT_ARCHIVE_ROW_LEVEL.md` — **v2** построчно (основной режим), таблица `row_key_columns`.
- `INPUT_ARCHIVE_SQLITE_DESIGN.md` — **v1** снимки файла (legacy).

## RATING / ORDER / MANAGER_STATS

- `RATING_MATRIX_COLORS_AND_LOGIC.md` — матрица ITEM, цвета, itemAmount.
- `SEASON_ORDER_SUMMARY.md` — обзор листа ORDER-SEASON-SUMMARY.
- `SEASON_ORDER_SUMMARY_KM_LOGIC.md` — колонки «КМ:».
- `MANAGER_STATS.md` — отдельная книга табельных / enrich / JS.

## POST / перенос

- `POST_ENCRYPTED_TRANSFER.md` — шифрованный bundle для почты.
- `POST_SNAPSHOT/` — шаблоны `КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt`, `restore_names_from_txt.bat` (копируются в `POST/`).
- **WEB-снимок:** `python src/Tools/sync_post_web_bundle.py` → **`POST/SPOD_PROM/`** (common web-edit/fill, примеры JSON, Docs WEB, Tools экспорта). Состав — **`POST/SPOD_PROM/СОСТАВ_ПАКЕТА.txt`**.

## Прочее (живой / генерируемый)

- `PERFORMANCE_OPTIMIZATION_PROPOSALS.md` — бэклог ускорения (часть пунктов ещё открыта).
- `PERFORMANCE_AND_PARALLELIZATION_HISTORY.md` — краткая история уже сделанных оптимизаций.
- `CODEBASE_ANALYTICS.md` — снимок метрик кода (`build_codebase_analytics.py`).

## Правила актуализации

1. Поведение Excel / пайплайна — сначала **`README.md`**, затем узкий Docs по теме.
2. Формат `consistency_checks` — **`CONSISTENCY_CHECKS_FORMAT.md`** (+ sample); SQL-зеркало обновлять при новых referential/unique/field_length.
3. Конфиг — править файлы в **`config/`**, описание — **`CONFIG_FILES.md`**.
4. После смены CSV в `IN/` — пересобрать `Docs/JSON/SPOD_INPUT_DATA_CATALOG.md`.
5. Не плодить `*_V2` / `*_FINAL`; историю багов — в changelog README, не отдельными файлами.
