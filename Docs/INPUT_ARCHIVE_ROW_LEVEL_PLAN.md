# План: архив SQLite — актуальность и история по строкам (ToDo п. 3)

**Статус: реализовано (2026-05-22).** Код: `src/input_archive_sqlite_v2.py`, `src/input_archive_row_hash.py`, `src/input_archive_row_parallel.py`.  
**Документация реализации:** `Docs/INPUT_ARCHIVE_ROW_LEVEL.md`.  
Режим v1 (снимки файла): `src/input_archive_sqlite.py`, `Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md`.

---

## 1. Цель

- При изменении CSV на диске **не дублировать** все строки, а писать только **изменённые** и **новые** по ключу строки.
- Вести **историю** версий строки (хеш или сравнение полей).
- Строки, **отсутствующие** в текущей загрузке, помечать как неактуальные (не удалять историю).
- Повторная загрузка **того же содержимого** из **другого файла** — обновить только `source_file` / `last_loaded_at` у актуальных строк без новой версии данных.
- Новая БД; старую оставить как архив (путь в config).
- **Ускорение ingest:** по возможности **распараллелить по процессам** расчёт хешей строк и сравнение с уже известными ключами (см. **п. 11**).

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
- **`parallel_row_processing`** — блок настроек параллелизации (п. 11): `enabled`, `max_workers`, `chunk_size`.

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
2. [CPU, параллельно] Построить row_key_hash, row_hash для каждой строки (пул процессов, чанками).
3. Множество K = ключи текущего файла.
4. [I/O] Одним запросом загрузить из БД существующие (row_key_hash → row_hash, payload_id) для sheet.
5. [CPU, параллельно] Классифицировать строки файла: new / unchanged / changed (без записи в SQLite).
6. [I/O, один поток] Транзакция SQLite: batch INSERT payload, UPSERT current, UPDATE inactive.
7. (Опционально) если файл byte-identical предыдущему ingest — пропуск шагов 2–6 для данных, только метаданные inventory.
```

**Канонизация ключа и хеша:** сортировка имён колонок, `str(value).strip()`, JSON `sort_keys=True`, UTF-8, SHA-256.

Шаги **2** и **5** — основной выигрыш по времени при десятках/сотнях тысяч строк; шаг **6** остаётся последовательным из‑за одного писателя SQLite.

---

## 5. Связь с JSON_* колонками

Разворот `JSON_*` для CONTEST-DATA / REWARD:

- Включать в `row_hash` только при стабильной нормализации; иначе хеш только по сырым колонкам CSV.
- При upsert без смены row_hash — обновлять `JSON_*` как сейчас для latest-снимка (см. `archive_json_columns.py`).

---

## 6. Изменения в коде (выполнено)

| Компонент | Статус |
|-----------|--------|
| `input_archive_sqlite_v2.py` | [v] Схема, ingest, inventory по SHA файла |
| `input_archive_row_parallel.py` | [v] Пул процессов: хеши и классификация |
| `input_archive_row_hash.py` | [v] Канонизация и SHA-256 |
| `config_loader` / `Config` | [v] `merge_archive_v2_config` |
| `main_impl.py` | [v] Ветка v2 при `row_level_archive` |
| `console_ui.py` | [v] `print_input_archive_row_report` |
| `Docs/INPUT_ARCHIVE_ROW_LEVEL.md` | [v] Подробная документация |
| `INPUT_ARCHIVE_SQLITE_DESIGN.md` | [v] Сводка v2 + ссылка |

**Миграция v1→v2:** скрипт в `src/Tools/` — [ ] опционально, не реализован.

---

## 7. Порядок работ (после согласования)

| Шаг | ROADMAP | Содержание |
|-----|---------|------------|
| 1 | 3.1 | Схема SQL + новый db_path, флаг enabled |
| 2 | 3.2 | row_key в config для всех archive_to_db файлов |
| 3 | 3.6 | Проектирование параллельных фаз (хеши, сравнение), параметры в config |
| 4 | 3.3 | ingest upsert + mark inactive (с параллельными фазами 2 и 5) |
| 5 | 3.4 | Ветка «тот же хеш, другой файл» |
| 6 | 3.7 | Замеры: ingest до/после, лог длительности фаз |
| 7 | 3.5 | Отчёты, README, ROADMAP [v] |

---

## 8. Риски и решения

| Риск | Митигация |
|------|-----------|
| Нет уникального ключа на листе | Обязательный `row_key_columns` в config; валидация дублей ключа при ingest |
| Дубликаты ключа в одном CSV | WARNING + последняя строка wins или ошибка ingest |
| Рост payload | Периодическая чистка `superseded` / TTL в отдельной задаче |
| Два режима БД | Флаг `row_level_archive`; v1 и v2 не писать в один файл |
| Параллельные процессы + SQLite | Только **вычисления** в процессах; **запись** в одной транзакции одним соединением |
| Oversubscription на малой машине | `max_workers` из config, по умолчанию `min(8, cpu_count-1)` |

---

## 11. Параллелизация и ускорение (процессы)

### 11.1. Зачем

При построчном архиве на **каждую строку** CSV дважды считается SHA-256 (ключ + тело) и выполняется сравнение с множеством уже известных `row_key_hash` / `row_hash`. На файлах **ORDER**, **RATING**, **LIST-REWARDS** и др. это **сотни тысяч** операций — в одном потоке ingest становится узким местом. Задачи **CPU-bound** (хеширование, канонизация полей), поэтому предпочтительны **процессы** (`multiprocessing` / `ProcessPoolExecutor`), а не потоки.

### 11.2. Что распараллеливать

| Фаза | Параллельно? | Примечание |
|------|----------------|------------|
| Чтение CSV → DataFrame | Нет (уже быстро) | Как в текущем пайплайне |
| Расчёт `row_key_hash` + `row_hash` по строкам | **Да, процессы** | Чанки по `chunk_size` строк; функция воркера без доступа к SQLite |
| Загрузка справочника из БД `(key_hash → row_hash, …)` | Нет | Один `SELECT` по `sheet_name` |
| Классификация: new / unchanged / changed | **Да, процессы** | Вход: чанк хешей + общий dict из БД (read-only, picklable) |
| `INSERT` / `UPDATE` в SQLite | **Нет** | Один writer, `executemany` / batch в транзакции |
| Пометка `inactive` для ключей ∉ K | Нет | Один `UPDATE … WHERE row_key_hash NOT IN (...)` или временная таблица ключей |
| Несколько **файлов** `input_files` подряд | Опционально | Разные `archive_db_path` — можно обрабатывать файлы параллельно **разных** БД; одна БД — **последовательно** по файлам |

### 11.3. Конфиг `parallel_row_processing`

```json
"parallel_row_processing": {
  "enabled": true,
  "max_workers": 0,
  "chunk_size": 2000,
  "min_rows_for_parallel": 500
}
```

| Поле | Описание |
|------|----------|
| `enabled` | Включить пул процессов для фаз хеширования и сравнения. |
| `max_workers` | Число процессов; **`0`** — авто: `min(8, os.cpu_count() - 1)` (как в `main_impl` для I/O). |
| `chunk_size` | Строк в одной задаче воркера (баланс накладных расходов / IPC). |
| `min_rows_for_parallel` | Ниже порога — один процесс (не поднимать pool на малых листах). |

### 11.4. Эскиз реализации

1. **`compute_row_hashes_chunk(rows, key_cols, hash_cols) -> List[RowHashRecord]`** — чистая функция top-level (picklable) для `ProcessPoolExecutor`.
2. **`classify_chunk(records, existing_map) -> ClassifiedChunk`** — new / unchanged / changed списки id.
3. Главный процесс: `df` → список dict по строкам (или индекс + срезы) → `executor.map` по чанкам → merge результатов.
4. Сбор batch для SQLite: только changed/new → INSERT payload; unchanged → только UPDATE метаданных current.
5. В DEBUG-лог: `hash_phase_sec`, `compare_phase_sec`, `db_write_sec`, `workers`, `rows`.

### 11.5. Ограничения

- На **Windows** и macOS обязателен guard `if __name__ == "__main__"` при spawn (вызов ingest только из `main`, не из интерактивного REPL в том же модуле).
- Большой `existing_map` для листа целиком держится в памяти главного процесса (один раз из БД); при экстремальных объёмах — рассмотреть SQLite `ATTACH` + сравнение через временную таблицу (фаза 2, не MVP).
- Параллельная запись в **одну** SQLite-БД **не** используется (конфликты блокировок).

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
| LIST-REWARDS | `Уникальный идентификатор записи` | — (нет unique) | Согласовано с заказчиком (2026-05-22): один суррогатный ключ строки в выгрузке gamification. |
| LIST-TOURNAMENT | `Код турнира` | — | Ключ merge в REPORT (`Код турнира` → `TOURNAMENT_CODE`). |
| STATISTICS | `Табельный номер`, `Код роли`, `Дата вступления в роль` | — | В выгрузке **нет** колонки **Период** (2026-05-22); ключ без неё. |
| RATING_* (все листы `RATING_…` в `input_files`) | `Табельный номер`, `Наименование Роли`, `Период` | — | Одна строка рейтинга = менеджер в срезе роль+период (согласовано с п. 2 RATING). Имена колонок — как в выгрузке. |
| ORDER_* (все листы `ORDER_…`) | `Уникальный идентификатор транзакции` | — | Согласовано с заказчиком: одна строка = одна транзакция заказа. |
| ORDER_ALL (MNS) | `Уникальный идентификатор транзакции` | — | Тот же ключ, что у остальных листов ORDER_*. |
| YEAR_STATA | `Уникальный идентификатор записи` | — | Согласовано с заказчиком (2026-05-22). |

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
| Схема + ingest (базовый, однопоточный каркас) | 2–3 дн |
| Параллельные фазы (хеши + сравнение, config, замеры) | 1–1.5 дн |
| Конфиг ключей по листам | 0.5–1 дн |
| Отчёты + документация | 1 дн |

**Итого:** ~5–6 дней после утверждения (с параллелизацией).
