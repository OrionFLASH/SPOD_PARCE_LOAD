# Каталог документации SPOD

Актуальная карта `Docs/` после ревизии (2026-07-15). Устаревшие планы, дубли и черновики удалены.

## Источник истины

| Тема | Где смотреть |
|------|----------------|
| Продукт, пайплайн, changelog | корневой **`README.md`** |
| Раскладка и параметры `config/` | **`CONFIG_FILES.md`** |
| ToDo / статусы работ | **`ROADMAP.md`** (корень) |

## Конфигурация и данные

- `CONFIG_FILES.md` — каталог `config/`, `$include`, все `CONFIG_*.json`, примеры.
- `BLOCKS_MIGRATION.md` — шпаргалка переноса `IN/<BLOCK>/…` и SQLite по блокам.
- `IN_OUT_DATA_POLICY.md` — политика: не удалять `IN/`/`OUT/` без явного разрешения.
- `JSON/README.md` + `JSON/SPOD_INPUT_DATA_CATALOG.md` — каталог полей входных CSV/JSON (пересборка Tools).

## Консистентность

- `CONSISTENCY_CHECKS_FORMAT.md` — типы правил, поля, id, лист CONSISTENCY.
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
