# План: архив SQLite — актуальность и история по строкам (ToDo п. 3)

Документ для согласования. Текущая реализация: `src/input_archive_sqlite.py`, `Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md` — версионирование **целого файла** (снимок `latest` / `historical`).

---

## 1. Цель

- При изменении CSV на диске **не дублировать** все строки, а писать только **изменённые** и **новые** по ключу строки.
- Вести **историю** версий строки (хеш или сравнение полей).
- Строки, **отсутствующие** в текущей загрузке, помечать как неактуальные (не удалять историю).
- Повторная загрузка **того же содержимого** из **другого файла** — обновить только `source_file` / `last_loaded_at` у актуальных строк без новой версии данных.
- Новая БД; старую оставить как архив (путь в config).

---

## 2. Ограничения текущей схемы

| Сейчас | Проблема для ToDo |
|--------|-------------------|
| `archive_file_snapshot` + полный `INSERT` в `arch_*` | Любое изменение файла → новый снимок, все строки снова |
| Идентичность файла | SHA-256 **файла**, не строки |
| `row_status` на снимке | Нет статуса **отдельной бизнес-строки** |

---

## 3. Целевая модель данных

### 3.1. Файл БД

- **`db_path`:** например `OUT/DB/spod_input_archive_v2.sqlite`
- Старый: `spod_input_archive.sqlite` — только чтение / архив, `enabled` указывает на v2.

### 3.2. Конфиг на входной файл (`input_files[]`)

```json
{
  "sheet": "GROUP",
  "file": "GROUP (PROM).csv",
  "archive_to_db": true,
  "row_key_columns": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"]
}
```

Глобально в `input_archive_sqlite`:

- `default_row_key_columns` по sheet (если не задано в entry).
- `row_hash_columns`: `null` = все колонки CSV после чтения; или явный список для хеша.

### 3.3. Таблицы (предложение)

**`archive_row_current`** — одна актуальная версия на бизнес-ключ:

| Поле | Тип | Назначение |
|------|-----|------------|
| sheet_name | TEXT | Лист |
| row_key_hash | TEXT | SHA-256 канонического JSON ключа |
| row_key_json | TEXT | Ключ для отладки |
| row_hash | TEXT | SHA-256 содержимого строки (канон. поля) |
| row_status | TEXT | `active` / `inactive` / `superseded` (см. п. 9) |
| source_file | TEXT | Имя файла последней загрузки |
| source_path | TEXT | Полный путь |
| first_seen_at | TEXT | UTC |
| last_loaded_at | TEXT | UTC |
| payload_id | INTEGER | FK на снимок полей |

**`archive_row_payload`** — тело строки (версии):

| Поле | Тип |
|------|-----|
| id | INTEGER PK |
| sheet_name | TEXT |
| row_key_hash | TEXT |
| row_hash | TEXT |
| loaded_at | TEXT |
| source_file | TEXT |
| + все колонки CSV как TEXT |

При **том же** `row_key_hash` + **том же** `row_hash` — **не** вставлять новый payload; только `UPDATE archive_row_current SET last_loaded_at, source_file`.

При **том же** ключе, **другом** `row_hash` — новый payload, старый current → `superseded` или ссылка `previous_payload_id`.

**`archive_ingest_run`** — журнал прогонов (опционально): run_id, started_at, file_sha256, counts inserted/updated/skipped/missing.

**`arch_*` с `__snapshot_id`** — можно оставить для обратной совместимости или не переносить в v2 (только row-модель).

---

## 4. Алгоритм ingest одного файла

```
1. Прочитать CSV → DataFrame (как сейчас).
2. Построить row_key_hash, row_hash для каждой строки.
3. Множество K = ключи текущего файла.
4. Для каждой строки:
   a. Нет в archive_row_current → INSERT payload + current (active).
   b. Есть, row_hash совпадает → UPDATE current (last_loaded_at, source_file).
   c. Есть, row_hash другой → INSERT payload + UPDATE current (active, новый payload_id).
5. Для current WHERE sheet=X AND row_status=active AND row_key_hash NOT IN K:
   → row_status = inactive, inactive_since = now.
6. (Опционально) если файл byte-identical предыдущему ingest — только шаг 4b для метаданных.
```

**Канонизация ключа и хеша:** сортировка имён колонок, `str(value).strip()`, JSON `sort_keys=True`, UTF-8, SHA-256.

---

## 5. Связь с JSON_* колонками

Разворот `JSON_*` для CONTEST-DATA / REWARD:

- Включать в `row_hash` только при стабильной нормализации; иначе хеш только по сырым колонкам CSV.
- При upsert без смены row_hash — обновлять `JSON_*` как сейчас для latest-снимка (см. `archive_json_columns.py`).

---

## 6. Изменения в коде

| Компонент | Действие |
|-----------|----------|
| `input_archive_sqlite.py` | Режим `schema_version: 2` или отдельный модуль `input_archive_sqlite_v2.py` |
| `config_loader` / `Config` | Чтение `row_key_columns` из input_files |
| `main_impl.py` | Вызов ingest v2 при `enabled` + `row_level: true` |
| `console_ui.py` | Отчёт: new / updated / unchanged / marked_missing |
| `INPUT_ARCHIVE_SQLITE_DESIGN.md` | Слить описание v2 в README |

**Миграция:** скрипт в `src/Tools/` — опционально перенос последнего `latest` снимка в row_current (одноразово).

---

## 7. Порядок работ (после согласования)

| Шаг | ROADMAP | Содержание |
|-----|---------|------------|
| 1 | 3.1 | Схема SQL + новый db_path, флаг enabled |
| 2 | 3.2 | row_key в config для всех archive_to_db файлов |
| 3 | 3.3 | ingest upsert + mark missing |
| 4 | 3.4 | Ветка «тот же хеш, другой файл» |
| 5 | 3.5 | Отчёты, README, ROADMAP [v] |

---

## 8. Риски и решения

| Риск | Митигация |
|------|-----------|
| Нет уникального ключа на листе | Обязательный `row_key_columns` в config; валидация дублей ключа при ingest |
| Дубликаты ключа в одном CSV | WARNING + последняя строка wins или ошибка ingest |
| Рост payload | Периодическая чистка `superseded` / TTL в отдельной задаче |
| Два режима БД | Флаг `row_level_archive`; v1 и v2 не писать в один файл |

---

## 9. Решения по согласованию (2026-05-22)

| Вопрос | Решение |
|--------|---------|
| История версий строки | **Полная:** каждая смена `row_hash` → новая строка в `archive_row_payload`; в `archive_row_current` — ссылка на актуальный `payload_id`. Старые payload не удалять. |
| Статус строки, отсутствующей в текущем CSV | **`inactive`** (не `missing` / `deleted`). Поле метки времени: `inactive_since`. |
| Ingest: один на прогон или по файлам? | См. п. 9.1 ниже — **рекомендация: по файлу**, как сейчас. |

### 9.1. Пояснение: «один ingest на прогон» vs «на каждый файл»

Имелось в виду **гранулярность одного запуска `main.py`**:

| Вариант | Что это | Плюсы / минусы |
|---------|---------|----------------|
| **По файлу** (как сейчас в v1) | После чтения каждого CSV из `input_files` вызывается отдельная процедура архивации для пары `(sheet, file, subdir)`. Сравнение с inventory, commit, отчёт в консоли — **на файл**. | Листы и пути независимы; сбой одного файла не откатывает остальные; разные `archive_db_path` (SPOD vs gamification) естественно разделены. |
| **На весь прогон** | Один общий `ingest_run_id` на запуск, все файлы в одной большой транзакции SQLite, один итоговый отчёт. | Проще «срез на момент времени», но дольше блокировка БД и сложнее частичный откат при ошибке на одном CSV. |

**Согласованная рекомендация для v2:** оставить **обработку по каждому файлу** (как сейчас), плюс ввести опциональный **`ingest_run_id`** в `archive_ingest_run`: один id на запуск `main.py`, к нему привязаны все пофайловые ingest с счётчиками (new / updated / unchanged / inactive). Так сохраняется привычная модель и появляется свод «за прогон».

### 9.2. Предлагаемые `row_key_columns` по листам

Основа — **включённые** правила `type: unique` в `consistency_checks.rules` (`key_columns`). Для листов без unique — ключи из `merge_fields` / фактических заголовков gamification; помечены как **уточнить по CSV**.

| Лист (`sheet`) | `row_key_columns` (предложение) | Источник / правило unique (`id`) | Примечание |
|----------------|----------------------------------|-----------------------------------|------------|
| CONTEST-DATA | `CONTEST_CODE` | `unique_contest_data` | |
| GROUP | `CONTEST_CODE`, `GROUP_CODE`, `GROUP_VALUE` | `unique_group_contest_code_group_code_group_value` | |
| INDICATOR | `CONTEST_CODE`, `INDICATOR_ADD_CALC_TYPE`, `INDICATOR_CODE` | `unique_indicator_1` | Отдельно есть unique по `N` (`unique_indicator_n`) — для архива берём **бизнес-ключ**, не суррогат `N`. |
| REPORT | `MANAGER_PERSON_NUMBER`, `TOURNAMENT_CODE`, `CONTEST_CODE` | `unique_report` | |
| REWARD | `REWARD_CODE` | `unique_reward` | |
| REWARD-LINK | `CONTEST_CODE`, `GROUP_CODE`, `REWARD_CODE` | `unique_reward_link_contest_code_group_code_reward_code` | Правило `unique_reward_link_reward` (только `REWARD_CODE`) **выключено** — составной ключ предпочтительнее. |
| TOURNAMENT-SCHEDULE | `TOURNAMENT_CODE` | `unique_schedule_1` | Правило по паре `(TOURNAMENT_CODE, CONTEST_CODE)` **выключено** — в файле одна строка на турнир. |
| ORG_UNIT_V20 | `ORG_UNIT_CODE` | `unique_org_unit` | Есть также unique `TB_CODE`+`GOSB_CODE` — для строки справочника ГОСБ достаточно `ORG_UNIT_CODE`. |
| USER_ROLE | `RULE_NUM` | `unique_user_role` | |
| USER_ROLE SB | `RULE_NUM` | `unique_user_role_sb` | |
| EMPLOYEE | `PERSON_NUMBER` | `unique_employee_person` | Unique по `PERSON_NUMBER_ADD` и по `(POSITION_NAME, KPK_CODE, ORG_UNIT_CODE)` с областью КПК — для архива сотрудника достаточно **табельного** `PERSON_NUMBER`. |
| LIST-REWARDS | `Табельный номер сотрудника`, `Код награды`, `Код турнира` | — (нет unique) | Согласовано с `rating_item_matrix` / merge (`LIST-REWARDS` → REPORT). **Уточнить:** если одна награда без привязки к турниру — убрать `Код турнира` из ключа. |
| LIST-TOURNAMENT | `Код турнира` | — | Ключ merge в REPORT (`Код турнира` → `TOURNAMENT_CODE`). |
| STATISTICS | `Табельный номер`, `Наименование Роли`, `Период` | — | Идентификаторы в `column_formats.except_columns`; **уточнить** по заголовкам CSV. |
| RATING_* (все листы `RATING_…` в `input_files`) | `Табельный номер`, `Наименование Роли`, `Период` | — | Одна строка рейтинга = менеджер в срезе роль+период (согласовано с п. 2 RATING). Имена колонок — как в выгрузке. |
| ORDER_* (все листы `ORDER_…`) | `Табельный номер`, `Код товара`, `Дата заказа` | — | **Уточнить по CSV:** при наличии стабильного `Номер заказа` / `orderId` — ключ **`[Номер заказа]`** вместо даты. Несколько заказов одного товара в один день тогда не сольются. |
| ORDER_ALL (MNS) | то же, что ORDER_* | — | Отдельный файл «все сезоны» — тот же шаблон ключа. |
| YEAR_STATA | `Табельный номер`, `Период`, `Версия записи` | — | В форматах есть «Версия записи»; **уточнить**, если период не уникален без версии. |

**Дубликаты ключа в одном CSV:** при ingest — WARNING в лог, политика **последняя строка файла** перезаписывает предыдущую с тем же `row_key_hash` (зафиксировать в реализации).

**Шаблон в `input_files`:**

```json
"row_key_columns": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"]
```

Глобальный fallback в `input_archive_sqlite.default_row_key_by_sheet` — дублировать таблицу выше для листов без ключа в entry.

---

## 10. Оценка

| Блок | Оценка |
|------|--------|
| Схема + ingest | 2–3 дн |
| Конфиг ключей по листам | 0.5–1 дн |
| Отчёты + документация | 1 дн |

**Итого:** ~4–5 дней после утверждения.
