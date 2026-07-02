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
| 2.6 | Лист `ORDER-SEASON-SUMMARY` (сводка SEASON, заказано/остаток, счётчики КМ) | [v] |

Документация: `Docs/SEASON_ORDER_SUMMARY.md`.

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
| 3.8 | BOM в заголовках gamification-CSV, ключ STATISTICS без «Период» (`csv_headers.py`) | [v] |

Подробности параллелизации: **`Docs/INPUT_ARCHIVE_ROW_LEVEL_PLAN.md`**, раздел **11**. Заголовки CSV: **`Docs/INPUT_ARCHIVE_ROW_LEVEL.md`**, п. **5.0**.

---

## Пункт 4 — PerformanceWarning: фрагментация DataFrame при развороте JSON

**Документ:** `TODO_dataframe_fragmentation_roadmap.md` (корень проекта). Версия **1.7.48**.

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
