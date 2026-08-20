# ROADMAP — ToDo SPOD

Статусы: `[v]` сделано · `[w]` в работе · `[ ]` не сделано · `[x]` отменено

Согласование: пункты **2**, **3**, **6**, **7** — реализованы (см. планы в `Docs/`). Пункт **8** — анализ разбиения конфига (реализация после выбора варианта). Пункт **16** — fill/fill-full (`Docs/PLAN_WEB_FILL.md`); **16.1 сделано** — сверка JSON=CSV и пересборка примеров fill из файлов `CONFIG_RUN_INPUT.json`. Пункт **17** — примеры JSON в `common/examples/`. Пункт **18** — доработки fill-full (`Docs/PLAN_WEB_FILL_FULL.md`); **18.1–18.5 сделаны** в `web-fill-full`. Пункт **19** — стенды PROM/PSI и UX фильтров (`Docs/PLAN_WEB_FILL_STANDS.md`); код восстановлен поверх пункта 18. Пункт **20** — волна ToDo FILL EDIT (`Docs/PLAN_WEB_FILL_EDIT_WAVE20.md`): fill-full + edit-full.

---

## Пункт 1 — Проверка консистентности IN + NOT NULL

| # | Задача | Статус |
|---|--------|--------|
| 1.1 | Тип правила `field_in_values` в `src/consistency_checks.py` (колонка и JSON-ключ) | [v] |
| 1.2 | Правило `in_schedule_tournament_status` в `config.json` (TOURNAMENT-SCHEDULE / TOURNAMENT_STATUS) | [v] |
| 1.3 | Документация `Docs/CONSISTENCY_CHECKS_FORMAT.md` (п. 2.6.1) | [v] |

Подробности: `allowed_values` = УДАЛЕН, ЗАВЕРШЕН, АКТИВНЫЙ, ПОДВЕДЕНИЕ ИТОГОВ, ОТМЕНЕН; `allow_empty: false` (NOT NULL).

---

## Пункт 2 — RATING: заказы, доступность, лимиты групп, itemAmount

**Документ:** `Docs/RATING_MATRIX_COLORS_AND_LOGIC.md`. Решения по вопросам — в конфиге `rating_item_matrix` и коде (план выполнен, 2026-05).

| # | Задача | Статус |
|---|--------|--------|
| 2.0 | Утверждение плана и ROADMAP заказчиком | [v] |
| 2.1 | Фильтр ORDER: исключить «Отклонён», «Отменён» по колонке статуса | [v] |
| 2.2 | Четыре состояния ячейки: значение (число / Y / N) + 4 цвета | [v] |
| 2.3 | Лимит заказов по группам сезона (SEASON_m_2025_2, SEASON_2025_2, max=2) | [v] |
| 2.4 | Красная шапка колонки при превышении itemAmount (по менеджеру и коду) | [v] |
| 2.5 | Секция `rating_item_matrix` в config.json, тесты | [v] |
| 2.6 | Лист `ORDER-SEASON-SUMMARY` (сводка SEASON, заказано/остаток, счётчики КМ) | [v] |

Документация: `Docs/SEASON_ORDER_SUMMARY.md`.

---

## Пункт 3 — Архив SQLite: история по строкам, не по файлу

**Документ:** `Docs/INPUT_ARCHIVE_ROW_LEVEL.md` (таблица `row_key_columns`). Код: **`src/input_archive_sqlite_v2.py`**, **`src/input_archive_row_hash.py`**, **`src/input_archive_row_parallel.py`**. БД: **`OUT/DB/{BLOCK}/spod_input_archive_{BLOCK}_v2.sqlite`** при **`row_level_archive`: true**.

| # | Задача | Статус |
|---|--------|--------|
| 3.0 | Утверждение плана, `row_key_columns`, режима ingest | [v] |
| 3.1 | Новая БД + схема: ключ строки, row_hash, row_status, метаданные загрузки | [v] |
| 3.2 | `row_key_columns` в `input_files` / конфиге архива (`default_row_key_by_sheet`) | [v] |
| 3.6 | Параллелизация: `parallel_row_processing` в config, хеши и сравнение по процессам (п. 11 плана) | [v] |
| 3.3 | Ingest: upsert / inactive + интеграция параллельных фаз, batch-запись SQLite | [v] |
| 3.4 | Повторная загрузка того же содержимого из другого файла — только source_file / loaded_at | [v] |
| 3.7 | Замеры производительности (фазы hash / compare / db), DEBUG-лог | [v] |
| 3.5 | Отчёт в консоль (`print_input_archive_row_report`), README/ROADMAP | [v] |
| 3.8 | BOM в заголовках gamification-CSV, ключ STATISTICS без «Период» (`csv_headers.py`) | [v] |

Подробности параллелизации и заголовков CSV: **`Docs/INPUT_ARCHIVE_ROW_LEVEL.md`**.

---

## Пункт 4 — PerformanceWarning: фрагментация DataFrame при развороте JSON

**Документ:** `Docs/PERFORMANCE_AND_PARALLELIZATION_HISTORY.md` (§4). Версия **1.7.48**.

| # | Задача | Статус |
|---|--------|--------|
| 4.1 | Пакетный `pd.concat` в `flatten_json_column_recursive` (этап `01_parallel_csv_read_and_json_flatten`) | [v] |
| 4.2 | Пакетный `pd.concat` в параллельном `merge_fields_across_sheets` | [v] |
| 4.3 | Тест `src/Tests/test_flatten_json_batch.py` | [v] |
| 4.4 | Документация: README, ROADMAP, `PERFORMANCE_AND_PARALLELIZATION_HISTORY.md` | [v] |
| 4.5 | Полный прогон `main.py` с замером времени этапа 01 (baseline до/после) | [ ] |

---

## Пункт 5 — Самодостаточный дешифровщик для POST-пакета

| # | Задача | Статус |
|---|--------|--------|
| 5.1 | Перенести крипто-логику в `decrypt_post_program.py` без импорта из `src` | [v] |
| 5.2 | Сохранить совместимость с ранее зашифрованными пакетами (`SPODENC1`) | [v] |
| 5.3 | Уменьшить эвристические признаки «trojan/script» (нейтральные имена, проверка путей) | [v] |
| 5.4 | Проверить тесты/запуск и обновить статус до `[v]` | [v] |

---

## Пункт 6 — Блоки входных данных PROM / IFT / PSI

Отдельные наборы CSV по средам. Переключатель `run_blocks`. Выход: `OUT/<BLOCK>/YYYY/DD-MM/`, имена `SPOD_<BLOCK> …`.

**Структура `input_files`:** объект с разделами `PROM` / `IFT` / `PSI`, в каждом — свой полный список файлов (не плоский список с полем `block`). Составы могут различаться (нет ORDER/RATING — блок обрабатывается без них). Сейчас все три раздела заполнены одинаково.

| # | Задача | Статус |
|---|--------|--------|
| 6.1 | `run_blocks` в config + разбор в `config_loader` (по умолчанию `["PROM"]`) | [v] |
| 6.2 | `input_files` как разделы PROM/IFT/PSI; полный набор в каждом | [v] |
| 6.3 | Цикл расчёта: брать список файлов из раздела блока | [v] |
| 6.4 | Каталог `OUT/<BLOCK>/YYYY/DD-MM` и шаблоны `SPOD_{BLOCK} …` | [v] |
| 6.5 | Тесты, README / Docs | [v] |

## Пункт 7 — Доработки блоков PROM / IFT / PSI (пожелания)

Пожелания после PR #10. Реализовано: run_outputs по блокам; IN/<BLOCK>/{SPOD,FILE,…}; SQLite на блок; метки в консоли/логах; опциональный параллельный прогон (run_blocks_parallel).

### 7.1. `run_outputs` отдельно для каждого блока

Сейчас один глобальный `run_outputs` на весь запуск. Нужно: **для каждого выбранного `run_blocks` — свой набор `run_outputs`**.

Идея конфига (эскиз): вложенная структура по блокам — у каждого блока свой список токенов обработки (`main_only`, `source_only`, `consistency_only`, `manager_stats_only` и т.д.). Блоки из `run_blocks` обрабатываются независимо, каждый своим набором выходов.

| # | Задача | Статус |
|---|--------|--------|
| 7.1.1 | Конфиг: `run_outputs` (или аналог) вложенно по блокам PROM/IFT/PSI | [v] |
| 7.1.2 | Пайплайн: для каждого блока из `run_blocks` применять только его `run_outputs` | [v] |
| 7.1.3 | Обратная совместимость: плоский глобальный `run_outputs` → одинаково для всех выбранных блоков | [v] |
| 7.1.4 | Документация + тесты комбинаций | [v] |

### 7.2. Структура каталогов `IN/`: блок → тип данных

Сейчас: `IN/SPOD/<BLOCK>/`, `IN/FILE/`, …  
Нужно: **сначала блок среды**, внутри — привычные подкаталоги данных.

Целевая схема:

```text
IN/
  PROM/
    SPOD/
    FILE/
    POST/
    JS/
  IFT/
    SPOD/
    FILE/
    POST/
    JS/
  PSI/
    SPOD/
    FILE/
    POST/
    JS/
```

`subdir` в `input_files` и пути POST/JS — под новую раскладку. Документация и миграция существующих файлов.

| # | Задача | Статус |
|---|--------|--------|
| 7.2.1 | Новая раскладка `IN/<BLOCK>/{SPOD,FILE,POST,JS}` | [v] |
| 7.2.2 | Обновить `subdir` / резолв путей в конфиге и коде | [v] |
| 7.2.3 | README / Docs / политика IN-OUT; заметка по миграции данных | [v] |

### 7.3. Консоль и логи: пометка блока

Любой вывод, относящийся к конкретному блоку (в т.ч. **сводка консистентности**), должен явно указывать блок (`PROM` / `IFT` / `PSI`). То же для записей в лог-файл: сообщения по блоку — с меткой блока, чтобы при нескольких `run_blocks` в одном прогоне не смешивать контекст.

| # | Задача | Статус |
|---|--------|--------|
| 7.3.1 | Консоль: префикс/метка блока в сводке консистентности и прочих блок-зависимых сообщениях | [v] |
| 7.3.2 | Лог-файл: та же пометка блока в соответствующих записях | [v] |
| 7.3.3 | Проверить баннеры этапов / итоги / ошибки отсутствия файлов — везде, где вывод про блок | [v] |

### 7.4. Параллельная обработка нескольких блоков

Если в `run_blocks` указано **два и более** блока — проработать возможность считать их **параллельно** (сейчас цикл последовательный).

**Критичное условие:** вывод в консоль (и лог) не должен перемешиваться между блоками. Нужен корректный, **непересекающийся** вывод: либо буферизация по блоку с печатью цельными пачками, либо выделенный канал/префикс с сериализацией записи в консоль, либо иной способ, при котором пользователь однозначно видит, к какому блоку относится каждое сообщение.

Параллель имеет смысл только если удаётся обеспечить такую изоляцию вывода (п. 7.3 — база; 7.4 — усиление под concurrent-режим). Общие ресурсы (SQLite-архив, лог-файл) — отдельно оценить на гонки и блокировки.

| # | Задача | Статус |
|---|--------|--------|
| 7.4.1 | Оценить техническую возможность параллельного прогона блоков (CPU/IO, логи; архив SQLite — см. п. 7.5 по блокам) | [v] |
| 7.4.2 | Релиз параллели только при гарантии непересекающегося вывода в консоль (буфер/лок/секции по блоку) | [v] |
| 7.4.3 | Конфиг-переключатель: последовательный (по умолчанию) / параллельный режим при нескольких `run_blocks` | [v] |
| 7.4.4 | Тесты на изоляцию консольного вывода и отсутствие гонок при записи OUT/логов/архива | [v] |

### 7.5. SQLite-архив: отдельная БД на каждый блок

Сейчас путь к архиву общий (`OUT/DB/spod_input_archive*.sqlite`). Нужно: **у каждого блока своя SQLite-БД**.

- Каталог — подпапка с именем блока (например `OUT/DB/PROM/`, `OUT/DB/IFT/`, `OUT/DB/PSI/` либо согласованный аналог в конфиге).
- Имя файла БД тоже содержит блок (например `spod_input_archive_PROM_v2.sqlite`).
- Пути `db_path` / `archive_db_path` в конфиге — шаблоны с `{BLOCK}` или разделы по блокам; без пересечения данных между средами.

| # | Задача | Статус |
|---|--------|--------|
| 7.5.1 | Разнести archive SQLite по каталогам блоков; имя файла с кодом блока | [v] |
| 7.5.2 | Конфиг: шаблоны/разделы `db_path` и `archive_db_path` под блоки | [v] |
| 7.5.3 | Документация + миграция/пояснение для уже существующей общей БД | [v] |

---

## Пункт 8 — Разбиение `config.json` на доменные `CONFIG_*.json`

**Мотив:** монолит ~200 KB / ~6900 строк неудобен для правок; риск смешения доменов (INPUT / CHECKS / FORMAT / MERGE / RATING / ORDER).

**Документ:** [`Docs/CONFIG_FILES.md`](Docs/CONFIG_FILES.md) (реализация) + ROADMAP ниже.

**Имена:** `CONFIG_<бизнес_смысл>.json` (например `CONFIG_CHECKS.json`, `CONFIG_RATING.json`).  
**Совместимость:** `config/config.json` + `$include` → в памяти единый dict; API `Config` без ломающих изменений для остального пайплайна. `_base_dir` = корень репозитория.

**Утверждено (2026-07-15):** вариант **B** + каталог **`config/`**; детали и параметры — [`Docs/CONFIG_FILES.md`](Docs/CONFIG_FILES.md).

| Файл | Смысл |
|------|--------|
| `config/config.json` | Точка входа, `$include`, оверрайды запуска |
| `config/CONFIG_RUN_INPUT.json` | `run_*`, paths, logging, performance, `input_files`, `input_archive_sqlite` |
| `config/CONFIG_CHECKS.json` | `consistency_checks` и связанное |
| `config/CONFIG_FORMATS.json` | `color_scheme`, `column_formats` |
| `config/CONFIG_MERGE.json` | merge / SUMMARY / sheet_order / … |
| `config/CONFIG_RATING.json` | `rating_item_matrix` |
| `config/CONFIG_ORDER.json` | `season_order_summary` |
| `config/CONFIG_MANAGER.json` | `manager_stats` |

| # | Задача | Статус |
|---|--------|--------|
| 8.0 | Анализ и утверждение варианта B + `config/` (зафиксировано в CONFIG_FILES / ROADMAP) | [v] |
| 8.1 | Согласование: вариант B + `config/` + ответы диалога (§9 анализа) | [v] |
| 8.2 | `config_loader`: путь `config/config.json`, `$include`, deep-merge, запрет дублей, тесты | [v] |
| 8.3 | Вынос доменов в `CONFIG_*.json`; удаление корневого монолита | [v] |
| 8.4 | README / `Docs/CONFIG_FILES.md` / DOCS_INDEX | [v] |
| 8.5 | POST/sync: весь каталог `config/` как есть | [v] |
| 8.6 | Проверка загрузки Config + тесты include/blocks | [v] |

---

## Пункт 9 — Область применения правил консистентности по блокам

Цель: в `consistency_checks.rules[]` добавить параметр блоков, чтобы конкретные проверки
применялись только к указанным блокам (`PROM` / `IFT` / `PSI`).  
По умолчанию (если параметр не задан) — правило работает для всех блоков.

| # | Задача | Статус |
|---|--------|--------|
| 9.1 | Реализовать фильтрацию правил консистентности по блоку запуска с дефолтом «все блоки» | [v] |
| 9.2 | Обновить конфиг-пример и документацию нового параметра | [v] |
| 9.3 | Добавить/обновить тесты на поведение по блокам и обратную совместимость | [w] |

---

## Пункт 10 — Понятность WARNING merge / даты / регистр колонок

| # | Задача | Статус |
|---|--------|--------|
| 10.1 | Понятный INFO при пустом результате фильтра (не «лист пустой») | [v] |
| 10.2 | Case-insensitive сопоставление колонок в `add_fields_to_sheet` (`calc_type` ↔ `CALC_TYPE`) | [v] |
| 10.3 | Парсинг дат турнира без шумного UserWarning в консоли; при необходимости — запись в лог | [v] |
| 10.4 | Доработка: суффикс `=>calc_type`, не дублировать INFO в консоль, ясный текст фильтра | [v] |

---

## Пункт 11 — Ускорение Excel: skip Alignment на тяжёлых листах

Цель: убрать массовый `Alignment` на ячейках данных для LIST-REWARDS / STATISTICS / RATING / ORDER (агрегаты и отдельные), управляемо через config.

| # | Задача | Статус |
|---|--------|--------|
| 11.1 | `performance.skip_data_alignment_sheets` + fnmatch; дефолт в конфиге и коде | [v] |
| 11.2 | `_format_sheet` / `apply_column_formats`: пропуск Alignment данных; заголовок без изменений | [v] |
| 11.3 | Документация (README, CONFIG_FILES, PERFORMANCE) + тесты | [v] |
| 11.4 | Контрольный полный прогон main и сравнение этапов до/после | [v] |

---

## Пункт 12 — SUMMARY: status_filters / count_label / count_aggregation

Цель: правила `merge_fields_advanced` с `sheet_dst: SUMMARY` обрабатывают фильтры и именованные счётчики так же, как обычные листы (`merge_fields_across_sheets`).

| # | Задача | Статус |
|---|--------|--------|
| 12.1 | `build_summary_sheet`: применять `status_filters` / `custom_conditions` / `group_by` / `aggregate`, передавать `count_aggregation` и `count_label` | [v] |
| 12.2 | 5 правил count по статусам турнира на SUMMARY в `CONFIG_MERGE.json` | [v] |
| 12.3 | Тест: на SUMMARY появляются `COUNT_nunique_ACTIVE` … `DELETED` | [v] |
| 12.4 | Добить недостающие правила из пользовательского конфига: status-count на TOURNAMENT-SCHEDULE, count без фильтра, даты LIST-TOURNAMENT | [v] |

---

## Пункт 13 — Утилита folder_parce (REPORT из папки)

Цель: отдельный скрипт + `config_folder_parce.json` — сбор строк REPORT по `TOURNAMENT_CODE` из `IN/REPORT` (рекурсивно) с выбором файла по max `CONTEST_DATE` / mtime.

| # | Задача | Статус |
|---|--------|--------|
| 13.1 | `config_folder_parce.json` | [v] |
| 13.2 | `folder_parce.py`: Pass1/Pass2, ThreadPool, Excel, консоль | [v] |
| 13.3 | README + ROADMAP | [v] |
| 13.4 | Smoke-прогон на `IN/REPORT` | [v] |
| 13.5 | Ненайденные коды: строка с `НЕ ОБНАРУЖЕН REPORT` и `-` в прочих полях | [v] |
| 13.6 | Оформление Excel: заголовок, freeze, auto_filter, ширины/центрирование как REPORT | [v] |
| 13.7 | Колонки `PARCE_FILES_LIST` / `PARCE_FILES_MAX_DATE_LIST` (списки файлов, wrap left) | [v] |

---

## Пункт 14 — Excel-каталог всех настраиваемых параметров PROM/SPOD

Цель: по полному разбору всех строк выгрузок (CONTEST-DATA, REWARD, REWARD-LINK, GROUP, INDICATOR, TOURNAMENT-SCHEDULE, REPORT, ORG_UNIT_V20, EMPLOYEE, USER_ROLE) собрать Excel с перечнем колонок и JSON-ключей: типы, пути, зависимости, описания, id, дубли, примеры, правила консистентности.

| # | Задача | Статус |
|---|--------|--------|
| 14.1 | Roadmap и ветка `orionflash/spod-params-excel-catalog-7a68` | [v] |
| 14.2 | Утилита `src/Tools/build_spod_params_excel.py` (полный обход CSV + JSON) | [v] |
| 14.3 | Связка с глоссариями, COLUMN_HINTS и `CONFIG_CHECKS.json` | [v] |
| 14.4 | Excel-артефакт + краткая документация | [v] |
| 14.5 | Прогон на выгрузках 05-08 / 02-07 / 27-07 / 04-06 | [v] |
| 14.6 | Доработка Excel: зависимости построчно, полный JSON-путь, имя ≠ `[]`, центрирование, статистика заполненности | [v] |
| 14.7 | Каталог только колонки + конечные JSON-ключи; колонка CSV-источник; без контейнеров/«элемент массива» | [v] |

---

## Пункт 15 — Excel-форма конкурса BADGE (export / import)

**Документы:** `Docs/CONTEST_BADGE_FORM.md`, заполнение — `Docs/CONTEST_BADGE_FORM_FILLING.md` (план: `Docs/PLAN_CONTEST_BADGE_FORM.md`).

| # | Задача | Статус |
|---|--------|--------|
| 15.1 | Пакет `src/contest_badge_form/` + `config/CONFIG_CONTEST_BADGE_FORM.json` | [v] |
| 15.2 | Токены `contest_badge_form_export` / `contest_badge_form_import` / `blank` в `run_outputs` | [v] |
| 15.3 | Лимиты BADGE по `CONTEST_TYPE`; не-BADGE → лог | [v] |
| 15.4 | Round-trip тест `src/Tests/test_contest_badge_form.py` | [v] |
| 15.5 | Docs / README / ROADMAP | [v] |
| 15.6 | xlsxwriter, dropdowns, цвета типов ввода, инструкция заполнения | [v] |
| 15.7 | HTML-редактор каталога полей (`common/web-edit/` + `common/param_catalog_review/`) | [v] |
| 15.8 | Blank Excel из `catalog.json` (web); apply → `field_meta.py` (полная синхронизация кода) | [w] |
| 15.8a | Blank формируется из `common/param_catalog_review/catalog.json` → `common/templates/…` | [v] |
| 15.9 | HTML-заполнение SPOD: `common/web-fill/` (шаги/сетка, тип→BADGE, `dropdown_custom`, JSON save/load, PROM, CSV `;` + UTF-8 BOM) | [v] |
| 15.10 | Тип ввода `dropdown_custom` (список + свой вариант): edit + fill; `SHOW_INDICATOR` | [v] |
| 15.11 | Подписи вариантов (`variant_labels`): в edit — два блока; в fill — текст на чипе + код; CSV — исходное значение; Y/N → Да/Нет | [v] |
| 15.12 | Архив web-fill: удалённые конкурсы/части, фильтр, восстановление и purge | [v] |
| 15.13 | SPOD-JSON массивы/объекты: `CONTEST_PERIOD`, `FILTER_PERIOD_ARR`, `INDICATOR_FILTER`, `TARGET_TYPE`(schedule/`seasonCode`) — разделы edit + UI/экспорт fill | [v] |
| 15.14 | JSON: `json_required` (ключ обязателен / может отсутствовать) + колонки-оболочки `kind: json` в TABLE для `allow_empty` ячейки | [v] |
| 15.15 | Fill: наборы JSON-массивов (`CONTEST_PERIOD` / `FILTER_PERIOD_ARR` / `INDICATOR_FILTER`) — UI список+редактор; примеры снимков со структурами | [v] |
| 15.16 | Fill: подписи/описания только из каталога 2124 (без override layout); пилюля `json_required` у JSON-ключей | [v] |
| 15.17 | Каталог 2304; UX fill: dirty↔baseline, P×N/F×N/SC, sch 2 ряда, Импорт JSON, раскладки, список+ | [v] |
| 15.18 | `web-edit-full` / `web-fill-full`: полный каталог из PROM SPOD CSV; fill с отдельными css/js/catalog | [v] |
| 15.19 | Edit: длинные списки (`INDICATOR_CODE`) — combobox и блок дефолта на всю ширину сверху карточки | [v] |

---

## Пункт 16 — Fill / fill-full: JSON без выдумок, панели, фильтры, каталог

**Документы:** [`Docs/PLAN_WEB_FILL.md`](Docs/PLAN_WEB_FILL.md) (план), [`Docs/TODO_WEB_FILL.md`](Docs/TODO_WEB_FILL.md) (чеклист).  
**Страницы:** `common/web-fill/`, `common/web-fill-full/`. С **16.16** UX — только fill-full. Каталог — через `web-edit` + `sync_web_fill_catalog.py`.  
**Статус:** пункт **16** выполнен, включая пересборку `common/examples/web-fill/**/*.json` и полный снимок всех конкурсов PROM SPOD (`contests/spod_fill_all_contests.json`) из файлов `config/CONFIG_RUN_INPUT.json` (листы каталога fill).

### Суть

1. Примеры JSON из PROM-SPOD не содержат строк, которых нет в CSV (фантом `t_CONTEST_00` / пустой INDICATOR).
2. Скрытие левой панели не прячет верхний и нижний колонтитулы.
3. Правая панель «Поиск и фильтры» (поиск + Турнир/Награда/Архив + ПРОМ/ТЕСТ, статус, дата), скрытие независимое.
4. Легенда цветов вкладок внизу слева.
5. Списки и короткие подписи: `INDICATOR_CODE`, `PLAN_METHOD_CODE`, `PLAN_MOD_METOD`, методы конкурса, `FACT_POST_PROCESSING`, агрегации.

| # | Задача | Статус |
|---|--------|--------|
| 16.0 | План + ToDo + этот пункт ROADMAP | [v] |
| 16.1 | Экспортер JSON: без заглушек; сверка CSV; пересборка примеров fill | [v] |
| 16.2 | Импорт fill: пустые массивы не порождают `t_/r_` и `emptyScheduleRow` | [v] |
| 16.3 | Скрытие левой панели: колонтитулы остаются | [v] |
| 16.4 | Правая панель поиска/фильтров, независимое скрытие | [v] |
| 16.5 | Фильтры ПРОМ/ТЕСТ, `TOURNAMENT_STATUS`, дата в `[START_DT, END_DT]` | [v] |
| 16.6 | Легенда цветов (столбик приоритета, не матрица) | [v] |
| 16.7 | Каталог edit+fill: списки/подписи + combobox `INDICATOR_CODE` | [v] |
| 16.8 | Sync каталога, README fill, приёмка fill ≡ fill-full | [v] |
| 16.9 | Fill: поле `REWARD_TYPE` в блоке «Связи + награды» | [v] |
| 16.10 | Fill: фильтр списка по `REWARD_TYPE` | [v] |
| 16.11 | Fill: в фильтрах значение «пусто / нет поля» (нет турниров в SCHEDULE) | [v] |
| 16.12 | Fill: кнопки «Сбросить все» / «Установить все» (кроме даты) | [v] |
| 16.13 | Fill: ITEM в списке слева под конкурсом; в шапке только выбранный товар | [v] |
| 16.14 | Fill: фильтр списка по бизнес-блоку | [v] |
| 16.15 | Fill: режим поиска «начинается с» / «содержит» / «равно» | [v] |
| 16.16 | Fill-full: шире правая панель фильтров, без длинных подсказок, чипы по ширине текста | [v] |
| 16.17 | Fill-full: поиск и фильтры — отдельные видимые мини-блоки | [v] |
| 16.18 | Fill-full: коды r_/t_ только если есть префикс; иначе поле целиком. ITEM в списке: код, имя, Ct | [v] |

16.1: экспортер читает CSV из `CONFIG_RUN_INPUT.json` (листы каталога fill); сверка JSON=CSV по каждому `CONTEST_CODE`; полный снимок `common/examples/web-fill/contests/spod_fill_all_contests.json`. Пересборка: `python3 src/Tools/export_web_fill_examples_from_spod.py`.  
16.9: в fill у пары «Связь + награда» видно поле `REWARD_TYPE`; в каталоге код `CRYSTAL` (как в CSV).  
16.10: фильтр списка конкурсов по `REWARD_TYPE` (чипы на правой панели).  
16.11: в фильтрах статуса и типа награды — значение «пусто / нет поля» (чип «Нет турниров» / «Пусто»).  
16.12: внизу правой панели — сброс всех чипов и включение всех кроме даты.  
16.13: награды `ITEM` в списке слева под конкурсом (со смещением); в шапке только выбранный товар, конкурс/группы/индикаторы общие.  
16.14: фильтр бизнес-блока (KMMMB / KMKKSB / AKMKKSB / CSM / остальные / пусто) по `BUSINESS_BLOCK`, `FEATURE.businessBlock`, `ADD.businessBlock`.  
16.15: поиск — режимы «начинается с» / «содержит» / «равно» (сравнение с отдельным полем).
16.16: только **web-fill-full** — правая панель ~400px; пояснения фильтров в тултипе заголовка; чипы не сжимают подпись.
16.17: fill-full — поиск, каждый фильтр и «все фильтры» в отдельных карточках.
16.18: fill-full — если в коде нет `r_/t_`+CONTEST_CODE, поле правится целиком; новые конкурсы всегда с префиксом. ITEM в списке: REWARD_CODE, FULL_NAME, Ct из itemAmount.

---

## Пункт 17 — Примеры JSON в `common/examples/`

Рабочие каталоги и сохранения UI (**edit/fill**: `catalog.json`, `game_edit_catalog.json`, датированные снимки каталога, `spod_fill_YYYYMMDD*.json` из «Сохранить JSON») **не** переносятся. Выгрузки CSV→JSON для документации остаются в `Docs/JSON/examples/`.

| # | Задача | Статус |
|---|--------|--------|
| 17.1 | Собрать примеры JSON из `common` в `common/examples/` по задаче и смыслу | [v] |
| 17.2 | Экспортер и документация пишут/ссылаются на новую раскладку | [v] |

---

## Пункт 18 — Fill-full: выбор выгрузки, бизнес-блок, коды ITEM, полный код в JSON, разбиение JS

**Документы:** [`Docs/PLAN_WEB_FILL_FULL.md`](Docs/PLAN_WEB_FILL_FULL.md) (план), [`Docs/TODO_WEB_FILL_FULL.md`](Docs/TODO_WEB_FILL_FULL.md) (чеклист).  
**Пожелания:** [`common/ToDo FILL EDIT.txt`](common/ToDo%20FILL%20EDIT.txt).  
**Страницы:** `common/web-fill-full/` (основное). Однофайловый fill не зеркалить, пока не попросят. Разбиение JS edit-full не делали.  
**Статус:** пункт **18** выполнен в fill-full (18.1–18.5). Однофайловый fill не зеркалился. Разбиение JS edit-full не делали.

### Суть

1. Выгрузка JSON/CSV только выбранных конкурсов (режим «Выбрать» + отметки; без новых кнопок скачивания).
2. Бизнес-блок правится на конкурсе; в FEATURE и REWARD — тот же текст без смены.
3. Награды ITEM: префикс `ITEM_` + окончание; остальные — `r_` + код конкурса (как 16.18).
4. В JSON `TOURNAMENT_CODE` / `REWARD_CODE` — полный код; окончание в `*_ENDING`.
5. Разбить монолитный JS fill-full (и при успехе — edit-full).

| # | Задача | Статус |
|---|--------|--------|
| 18.0 | План + ToDo + этот пункт ROADMAP | [v] |
| 18.1 | Выбор конкурсов: отметки, все/снять, подписи JSON/CSV | [v] |
| 18.2 | Бизнес-блок: мастер CONTEST, показ в FEATURE/REWARD | [v] |
| 18.3 | Код награды: `ITEM_` vs `r_` + CONTEST_CODE | [v] |
| 18.4 | Полный код в JSON + `*_ENDING`; экспортер, примеры, тесты | [v] |
| 18.5 | Разбиение JS fill-full (edit-full не делали) | [v] |

18.1: фильтры не сбрасывают выбор; в файл — объединение отмеченных; «отметить все» = видимые, «снять все» = вся сессия.  
18.2: в CSV/JSON зависимые поля по-прежнему заполнены.  
18.3: без `ITEM_` в загруженном коде — поле целиком.  
18.4: старые снимки с суффиксом в штатном поле остаются читаемыми.  
18.5: без бандлера, `<script src>` по порядку; поведение не менять.

---

## Пункт 19 — Стенды PROM/PSI, фильтры «все/снять», ПКАП после «Можно пусто»

**Документы:** [`Docs/PLAN_WEB_FILL_STANDS.md`](Docs/PLAN_WEB_FILL_STANDS.md), [`Docs/TODO_WEB_FILL_STANDS.md`](Docs/TODO_WEB_FILL_STANDS.md).  
**Страницы:** `common/web-fill-full/` (стенды, фильтры). Edit: `web-edit` / `web-edit-full`.  
**Правило:** код вшивать в текущие 6 JS-файлов fill-full. Пункт **18** (выбор JSON, бизнес-блок, `ITEM_`, `*_ENDING`, split) не откатывать.

### Суть

1. Метки стенда `stands[]` (PROM / PSI; IFT зарезервирован). Три JSON: PROM, PSI, merged. CSV по умолчанию — строки с PROM.
2. Фильтр «Стенд» под «Средой»; «Среда» под датой; у каждого фильтра мини-кнопки все/снять.
3. Edit: ПКАП / ФАБРИКА справа после «Можно пусто»; можно отжать обе.
4. Ночной UX (18 авг ~1:00): `CONTEST_PERIOD` внизу карточки конкурса; статус турнира и тип награды в две строки («Нет награды»); кнопки сворачивания панелей на стыке колонок, стрелки над и под текстом.
5. Метки стенда PROM/PSI явно цветом: включено — заливка, выключено — серые.

| # | Задача | Статус |
|---|--------|--------|
| 19.0 | План + ToDo + этот пункт ROADMAP | [v] |
| 19.A | Фильтры: все/снять на блоке, Среда под датой | [v] |
| 19.B | Edit: ПКАП после «Можно пусто», `marks: []` | [v] |
| 19.C | Merge Python + три JSON + `*_ENDING` | [v] |
| 19.D | UI стенда в fill-full | [v] |
| 19.E | Ночной UX 18 авг: CONTEST_PERIOD на карточке, две строки статусов/наград, edge-кнопки панелей | [v] |
| 19.F | Цвет меток стенда: вкл PROM синий / PSI фиолетовый, выкл серый | [v] |
| 19.G | Фильтр стенда: «только PROM» / «только PSI» (ровно один признак) | [v] |

---

## Пункт 20 — Fill-full / edit-full: тексты, JSON-пусто, зависимости ключей

**Документы:** [`Docs/PLAN_WEB_FILL_EDIT_WAVE20.md`](Docs/PLAN_WEB_FILL_EDIT_WAVE20.md), [`Docs/TODO_WEB_FILL_EDIT_WAVE20.md`](Docs/TODO_WEB_FILL_EDIT_WAVE20.md).  
**Страницы:** `common/web-fill-full/`, `common/web-edit-full/`. Однофайловые не зеркалим.  
**Правило:** пункты **18** и **19** не откатывать. Пустые JSON-колонки — метаданные в edit, fill только по каталогу.

### Суть

1. Каталог: опечатки, MKKMMB, SEASON_mkk, fileName allow_empty, nftFlg.
2. Fill UX: массивы по `\n`, GROUP layout, активная вкладка, nav ТУРНИР/НАГРАДА + CODE.
3. Обязательные ключи: маркер без автовыбора.
4. `empty_json_mode` / `json_wrap_quotes`; без фантомов CONTEST_PERIOD.
5. list→array; `omit_when_empty` + `depends_on`; пересборка examples.

| # | Задача | Статус |
|---|--------|--------|
| 20.0 | План + ToDo + этот пункт ROADMAP | [v] |
| 20.A | Тексты и справочники каталога | [v] |
| 20.B | UX fill: массивы, GROUP, вкладка, nav | [v] |
| 20.C | Маркеры обязательности без автовыбора | [v] |
| 20.D | empty JSON meta + pack без фантомов | [v] |
| 20.E | list / omit_when_empty / depends_on | [v] |
| 20.F | Пересборка examples | [v] |
| 20.G | Приёмка по ToDo FILL EDIT 1–16 | [v] |

---

## Пункт 21 — Consistency: обёртка JSON `"` и массивы helpCodeList / seasonItem

**Документы:** [`Docs/PLAN_CONSISTENCY_JSON_WRAP_ARRAYS.md`](Docs/PLAN_CONSISTENCY_JSON_WRAP_ARRAYS.md), [`Docs/TODO_CONSISTENCY_JSON_WRAP_ARRAYS.md`](Docs/TODO_CONSISTENCY_JSON_WRAP_ARRAYS.md).  
**Связь:** волна 20 (web-fill pack) → те же правила в основном пайплайне (`main_only` / `consistency_only` / full).

| # | Задача | Статус |
|---|--------|--------|
| 21.0 | `json_spod_format`: запрет внешней `'…'`; только `"` | [v] |
| 21.A | `array_value_keys`: helpCodeList, seasonItem → `[]` | [v] |
| 21.B | CONFIG_CHECKS + Docs + тесты | [v] |

