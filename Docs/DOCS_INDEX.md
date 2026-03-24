# Каталог документации SPOD

Документ фиксирует актуальную структуру `Docs/` после консолидации и удаления устаревших отчетов.

## Основные документы

- `INPUT_DATA_AND_CONFIG_FULL.md` — единый справочник по входным данным, структуре листов и параметрам конфигурации.
- `CONSISTENCY_CHECKS_FORMAT.md` — формат правил `consistency_checks` и структура сводного листа `CONSISTENCY`.
- `CONSISTENCY_SAMPLE_FORMAT.md` — актуальный формат поля `sample` по всем типам проверок.
- `АНАЛИЗ_ПРОВЕРОК_КОНСИСТЕНТНОСТИ.md` — аналитика покрытия проверок и предложения по расширению.

## Консолидированные исторические документы

- `PERFORMANCE_AND_PARALLELIZATION_HISTORY.md` — единая история оптимизаций/параллелизации и сравнения производительности (вместо множества версионных отчетов).
- `SUMMARY_GROUP_FIX_HISTORY.md` — история исправлений логики формирования `SUMMARY` и связки `GROUP_CODE`/`GROUP_VALUE`.
- `ADMIN_PANEL_GUIDE.md` — объединенный набор материалов по админ-панели (ТЗ, краткий запуск, статус реализации).

## Каталог CSV и JSON `IN/SPOD` — папка `Docs/JSON/`

- **`Docs/JSON/README.md`** — назначение каталога, список команд пересборки.
- **`Docs/JSON/SPOD_INPUT_DATA_CATALOG.md`** — единый документ: оглавление по файлам; для каждого CSV — назначение колонок, статистика значений; для **REWARD** и **CONTEST** — разбор JSON (`REWARD_TYPE` / `CONTEST_TYPE`) и встроенные пояснительные справочники полей.
- **`Docs/JSON/examples/`** — по одному JSON на каждый соответствующий CSV выгрузки в `IN/SPOD` (те же базовые имена файлов).

Пересборка каталога: `python src/Tools/build_spod_input_catalog.py`. Обновление примеров JSON: `python src/Tools/export_spod_json_examples.py`. Тексты глоссариев: `src/Tools/catalog_glossary/`.

## Специализированные материалы

- `EXCEL_FEATURES_EXAMPLES.md` — примеры работы с валидациями/формулами в Excel.
- `TZ_ADMIN_PANEL.md` — исходное детализированное ТЗ (оставлен как первоисточник требований).

## Правила актуализации

- **Источник истины по продукту:** корневой **`README.md`** (ТЗ, пайплайн, `config.json`, логирование, история версий). Разделы **`column_formats`** (в т.ч. `except_columns`, лист **STATISTICS**) и **`reward_getcondition_summary`** описывают актуальное поведение Excel и листа REWARD.
- После обновления CSV в `IN/SPOD/` пересобрать **`Docs/JSON/SPOD_INPUT_DATA_CATALOG.md`**: `python src/Tools/build_spod_input_catalog.py`; обновить примеры в **`Docs/JSON/examples/`**: `python src/Tools/export_spod_json_examples.py`; при смене схемы JSON править **`src/Tools/catalog_glossary/`**.
- Справочник **`INPUT_DATA_AND_CONFIG_FULL.md`** держать согласованным с `README.md` по ключевым блокам конфигурации (п. 3), в т.ч. **`run_mode`**: какие выходные файлы создаются в `full` и в каждом из `*_only`.
- Новые изменения по консистентности вносить сначала в `README.md`, затем синхронно в `CONSISTENCY_CHECKS_FORMAT.md` и `CONSISTENCY_SAMPLE_FORMAT.md`.
- Для крупных блоков изменений использовать консолидированные документы, а не создавать новые `*_V2`, `*_FINAL`, `*_FULL` файлы.
- Исторические документы с пересекающимся содержимым объединять и удалять дубли.
