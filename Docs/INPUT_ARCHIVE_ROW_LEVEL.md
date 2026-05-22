# Построчный архив входных CSV в SQLite (v2)

Документ описывает **реализованный** режим **`row_level_archive`** в секции **`input_archive_sqlite`** (`config.json`). План согласования: **`Docs/INPUT_ARCHIVE_ROW_LEVEL_PLAN.md`**. Режим **v1** (снимки целого файла) — **`Docs/INPUT_ARCHIVE_SQLITE_DESIGN.md`**.

---

## 1. Зачем v2

| v1 (`input_archive_sqlite.py`) | v2 (`input_archive_sqlite_v2.py`) |
|--------------------------------|-----------------------------------|
| Любое изменение CSV → новый снимок, **все строки** снова в `arch_*` | Меняются только **новые** и **изменённые** бизнес-строки |
| Идентичность **файла** (SHA байтов) | Идентичность **строки** (`row_key_hash` + `row_hash`) |
| Нет статуса отдельной строки | `active` / `inactive` для строк, пропавших из текущего CSV |
| История = набор снимков файла | История = цепочка `archive_row_payload` на один `row_key_hash` |

Старая БД **`OUT/DB/spod_input_archive.sqlite`** не перезаписывается: v2 пишет в **`OUT/DB/spod_input_archive_v2.sqlite`** (путь в **`db_path`** при включённом построчном режиме).

---

## 2. Включение и переключение режимов

В **`config.json` → `input_archive_sqlite`**:

| Параметр | Значение (поставка) | Назначение |
|----------|---------------------|------------|
| **`enabled`** | `true` | Мастер-переключатель архива |
| **`row_level_archive`** | `true` | `true` → v2; `false` → v1 (снимки) |
| **`db_path`** | `OUT/DB/spod_input_archive_v2.sqlite` | Файл БД для активного режима |
| **`legacy_db_path`** | `OUT/DB/spod_input_archive.sqlite` | При **`row_level_archive: false`** main подставляет этот путь для v1 |
| **`schema_version`** | `2` | Метка схемы в конфиге |

Точка вызова: **`src/main_impl.py`** — сразу после параллельного чтения CSV и **`apply_aggregate_sheets`**, до проверок консистентности.

```text
enabled && row_level_archive  →  run_input_archive_sqlite_v2(...)
enabled && !row_level_archive →  run_input_archive_sqlite(..., db_path=legacy_db_path)
```

---

## 3. Модули кода

| Модуль | Назначение |
|--------|------------|
| **`src/input_archive_sqlite_v2.py`** | Схема БД, ingest по файлу, журнал прогонов, отчёт |
| **`src/input_archive_row_hash.py`** | Канонизация полей, `row_key_hash`, `row_hash` (SHA-256) |
| **`src/input_archive_row_parallel.py`** | `ProcessPoolExecutor`: расчёт хешей и классификация new/changed/unchanged |
| **`src/console_ui.py`** | **`print_input_archive_row_report`** — сводка v2 в stdout |
| **`src/config_loader.py`** | **`merge_archive_v2_config`** — слияние дефолтов v1 и v2 |
| **`src/Tests/test_input_archive_row_hash.py`** | Unit-тесты канонизации и dedupe last-wins |

---

## 4. Схема данных

### 4.1. `archive_row_current`

Одна актуальная запись на бизнес-ключ в рамках **(лист, имя файла из config, subdir)**:

| Поле | Тип | Назначение |
|------|-----|------------|
| `sheet_name`, `file_name`, `subdir` | TEXT | Логический вход из `input_files` |
| `row_key_hash` | TEXT | SHA-256 канонического JSON ключа |
| `row_key_json` | TEXT | Ключ в читаемом виде (отладка) |
| `row_hash` | TEXT | SHA-256 тела строки |
| `row_status` | TEXT | `active` или `inactive` |
| `source_file`, `source_path` | TEXT | Последняя загрузка |
| `first_seen_at`, `last_loaded_at` | TEXT | UTC ISO8601 |
| `inactive_since` | TEXT | Когда строка стала неактуальной |
| `payload_id` | INTEGER | FK на актуальный снимок полей |

**PRIMARY KEY:** `(sheet_name, file_name, subdir, row_key_hash)`.

### 4.2. `archive_row_payload`

История версий тела строки (при смене `row_hash` — **новая** строка, старые **не удаляются**):

| Поле | Назначение |
|------|------------|
| `id` | PK |
| `sheet_name`, `file_name`, `subdir`, `row_key_hash`, `row_hash` | Привязка |
| `loaded_at`, `source_file` | Метаданные загрузки |
| `payload_json` | Все поля строки CSV как JSON-объект (ключ → строка после нормализации) |

### 4.3. `archive_file_row_inventory`

Сводка по файлу для **пропуска ingest**, если байты CSV не изменились:

| Поле | Назначение |
|------|------------|
| `last_content_sha256` | SHA-256 файла на диске |
| `last_source_row_count` | Число строк при последней проверке |
| `last_checked_at` | UTC |

### 4.4. `archive_ingest_run`

Журнал пофайловых ingest в рамках одного запуска **`main.py`**:

| Поле | Назначение |
|------|------------|
| `ingest_run_id` | UUID на весь прогон |
| `count_new`, `count_changed`, `count_unchanged`, `count_inactive`, `count_key_errors` | Счётчики |
| `hash_phase_sec`, `compare_phase_sec`, `db_write_sec` | Длительность фаз (DEBUG в логе по строке файла) |

---

## 5. Ключ и хеш строки

### 5.0. Заголовки CSV (BOM и сопоставление)

Выгрузки gamification часто с **UTF-8 BOM** в первой колонке (`\ufeffТабельный номер` вместо `Табельный номер`). Чтение: **`utf-8-sig`** и нормализация заголовков (`src/csv_headers.py`). Архив v2 сопоставляет имена из config с фактическими столбцами без учёта BOM и лишних пробелов.

**STATISTICS:** в текущем CSV нет колонки **Период** — ключ строки: `Табельный номер`, `Код роли`, `Дата вступления в роль` (см. `default_row_key_by_sheet`).

### 5.1. `row_key_columns`

Список колонок CSV, однозначно идентифицирующих бизнес-строку. Источники (по приоритету):

1. **`input_files[].row_key_columns`** — для конкретного файла;
2. **`input_archive_sqlite.default_row_key_by_sheet[sheet]`** — глобальная таблица в config;
3. Шаблоны: листы **`RATING_*`** → ключ **`RATING_*`**; **`ORDER_*`** → **`ORDER_*`**.

Полная таблица ключей по листам — **`Docs/INPUT_ARCHIVE_ROW_LEVEL_PLAN.md`**, п. 9.2.

### 5.2. Канонизация

1. Значение ячейки: `str(value).strip()`, `NaN`/пусто → `""`.
2. Для ключа: JSON подмножества полей с **сортировкой имён ключей**, `sort_keys=True`, UTF-8.
3. **`row_key_hash`** = SHA-256 этой JSON-строки.
4. **`row_hash`**: то же для **всех** колонок строки (или списка **`row_hash_columns`**, если задан в config).

Реализация: **`src/input_archive_row_hash.py`**.

### 5.3. Дубликаты ключа в одном CSV

Если в файле несколько строк с одним `row_key_hash` — в лог **WARNING**, политика **последняя строка файла побеждает** (`dedupe_by_key_last_wins`).

---

## 6. Алгоритм ingest одного файла

```text
1. Прочитать CSV → DataFrame (уже в main, до архива).
2. SHA-256 файла → сравнить с archive_file_row_inventory;
   при совпадении и skip_ingest_if_file_unchanged=true — пропуск шагов 3–7.
3. [CPU, параллельно] row_key_hash + row_hash по строкам (чанки).
4. Dedupe по row_key_hash (last-wins).
5. [I/O] SELECT существующих (row_key_hash → row_hash, payload_id) из archive_row_current.
6. [CPU, параллельно] классификация: new / unchanged / changed.
7. [I/O, один поток, транзакция]:
   - new/changed → INSERT archive_row_payload, UPSERT archive_row_current (active);
   - unchanged → UPDATE last_loaded_at, source_file, source_path (без нового payload);
   - ключи active, отсутствующие в текущем файле → inactive + inactive_since.
8. INSERT archive_ingest_run; UPDATE inventory SHA.
```

**Тот же `row_key_hash` и тот же `row_hash`**, но другой путь/имя файла на диске: **новый payload не создаётся** — только обновление метаданных в `archive_row_current` (п. 3.4 плана).

**Тот же ключ, другой `row_hash`:** новая строка в `archive_row_payload`, в `archive_row_current` обновляются `row_hash` и `payload_id` (история сохраняется).

**Строка пропала из CSV:** `row_status = inactive`, `inactive_since = now` (история payload не удаляется).

---

## 7. Параллелизация (`parallel_row_processing`)

| Фаза | Параллельно? |
|------|----------------|
| Чтение CSV | Нет (уже в main) |
| Расчёт хешей | **Да**, `ProcessPoolExecutor` |
| Загрузка карты из БД | Нет, один SELECT |
| Классификация new/changed/unchanged | **Да** |
| INSERT/UPDATE SQLite | **Нет**, один writer, WAL |

Параметры в config:

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
| `max_workers` | `0` = авто `min(8, cpu_count - 1)` |
| `chunk_size` | Строк в задаче воркера |
| `min_rows_for_parallel` | Ниже порога — один процесс без pool |

На macOS/Windows ingest вызывается только из **`main`** (spawn-safe).

---

## 8. Конфигурация (дополнение к v1)

Секция **`input_archive_sqlite`** наследует параметры v1 (**`reporting`**, **`default_archive_to_db`**, **`archive_to_db`** в **`input_files`**, **`archive_db_path`** для отдельной БД gamification).

Специфичные для v2:

| Параметр | Описание |
|----------|----------|
| **`row_level_archive`** | Включить построчный режим |
| **`row_hash_columns`** | `null` — все колонки; или явный список для `row_hash` |
| **`skip_ingest_if_file_unchanged`** | Пропуск пересчёта строк при неизменном SHA файла |
| **`default_row_key_by_sheet`** | Объект «лист → массив имён колонок» |
| **`parallel_row_processing`** | См. п. 7 |
| **`legacy_db_path`** | Путь БД v1 при отключённом построчном режиме |

Пример ключа для листа GROUP в **`default_row_key_by_sheet`**:

```json
"GROUP": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"]
```

---

## 9. Отчёт в консоль и лог

**Консоль** (`reporting.console`, как у v1): **`print_input_archive_row_report`**

- Заголовок «SQLite v2, построчно», полный путь к БД;
- Итог: **новых**, **изменённых**, **без изменений**, **inactive**, пропуск файла по SHA, ошибки ключа;
- Режим **`normal`**: таблица по листам; **`verbose`**: тайминги фаз `hash=… cmp=… db=…`.

**Лог:**

- Старт/итог — **INFO** (`[archive_v2]`);
- Дубликаты ключа — **WARNING**;
- Тайминги фаз — **DEBUG**;
- Ошибки отсутствия колонок ключа — **ERROR**.

---

## 10. Примеры SQL

**Актуальные строки листа GROUP (файл из config):**

```sql
SELECT row_key_hash, row_hash, source_file, last_loaded_at, payload_id
FROM archive_row_current
WHERE sheet_name = 'GROUP'
  AND file_name = 'GROUP (PROM) 21-05 v0.csv'
  AND subdir = 'SPOD'
  AND row_status = 'active';
```

**Тело актуальной строки:**

```sql
SELECT p.payload_json, p.loaded_at
FROM archive_row_current c
JOIN archive_row_payload p ON p.id = c.payload_id
WHERE c.sheet_name = 'GROUP'
  AND c.row_key_hash = '…';
```

**История версий одного ключа:**

```sql
SELECT id, row_hash, loaded_at, source_file
FROM archive_row_payload
WHERE sheet_name = 'GROUP'
  AND row_key_hash = '…'
ORDER BY loaded_at;
```

**Неактуальные строки (пропали из последнего CSV):**

```sql
SELECT row_key_hash, inactive_since, last_loaded_at
FROM archive_row_current
WHERE sheet_name = 'ORDER_2025_2 (MNS)'
  AND row_status = 'inactive';
```

**Сводка последнего прогона main:**

```sql
SELECT sheet_name, file_name,
       count_new, count_changed, count_unchanged, count_inactive,
       hash_phase_sec, compare_phase_sec, db_write_sec
FROM archive_ingest_run
ORDER BY id DESC
LIMIT 20;
```

---

## 11. Ограничения и отличия от v1

- **JSON_* колонки** (`CONTEST_FEATURE` / `REWARD_ADD_DATA`): в v2 тело хранится в **`payload_json`**; отдельный разворот **`JSON_*`** в таблицах payload **пока не выполняется** (как в плане — при стабильной нормализации можно добавить).
- **Миграция v1 → v2:** одноразовый скрипт в **`src/Tools/`** не входит в текущую поставку; v1-БД остаётся архивом для чтения.
- **Несколько `archive_db_path`:** как в v1, файлы группируются по пути БД и обрабатываются **последовательно** (один writer на файл `.sqlite`).
- Рост **`archive_row_payload`:** при частых правках CSV история накапливается; очистка `superseded`/TTL — отдельная задача.

---

## 12. История документа

| Версия | Дата | Изменения |
|--------|------|-----------|
| 1.0 | 2026-05-22 | Первая версия после реализации: схема, ingest, config, SQL, модули |
