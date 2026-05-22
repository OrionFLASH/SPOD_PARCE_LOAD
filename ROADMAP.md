# ROADMAP — ToDo SPOD

Статусы: `[v]` сделано · `[w]` в работе · `[ ]` не сделано · `[x]` отменено

Согласование: пункты **2** и **3** — реализованы (см. планы в `Docs/`).

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

**Документ плана:** `Docs/RATING_MATRIX_ENRICHMENT_PLAN.md` (решения по вопросам — п. 9 документа, 2026-05-22)

| # | Задача | Статус |
|---|--------|--------|
| 2.0 | Утверждение плана и ROADMAP заказчиком | [v] |
| 2.1 | Фильтр ORDER: исключить «Отклонён», «Отменён» по колонке статуса | [v] |
| 2.2 | Четыре состояния ячейки: значение (число / Y / N) + 4 цвета | [v] |
| 2.3 | Лимит заказов по группам сезона (SEASON_m_2025_2, SEASON_2025_2, max=2) | [v] |
| 2.4 | Красная шапка колонки при превышении itemAmount (по менеджеру и коду) | [v] |
| 2.5 | Секция `rating_item_matrix` в config.json, тесты | [v] |

---

## Пункт 3 — Архив SQLite: история по строкам, не по файлу

**Документ плана:** `Docs/INPUT_ARCHIVE_ROW_LEVEL_PLAN.md`. Код: **`src/input_archive_sqlite_v2.py`**, **`src/input_archive_row_hash.py`**, **`src/input_archive_row_parallel.py`**. БД: **`OUT/DB/spod_input_archive_v2.sqlite`** при **`row_level_archive`: true**.

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

Подробности параллелизации: **`Docs/INPUT_ARCHIVE_ROW_LEVEL_PLAN.md`**, раздел **11**.
