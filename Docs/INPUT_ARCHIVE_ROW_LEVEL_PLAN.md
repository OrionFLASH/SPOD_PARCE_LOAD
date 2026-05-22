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
| row_status | TEXT | `active` / `missing` / `superseded` |
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
   → row_status = missing, missing_since = now.
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

## 9. Открытые вопросы

1. Список **row_key_columns** для каждого листа из `input_files` — подготовить таблицу с заказчиком.
2. Нужна ли полная история всех версий строки в SQL или достаточно current + последний payload?
3. Имя статуса неактуальной строки: `missing`, `inactive`, `deleted`?
4. Один ingest на весь прогон или на каждый файл отдельно (как сейчас)?

---

## 10. Оценка

| Блок | Оценка |
|------|--------|
| Схема + ingest | 2–3 дн |
| Конфиг ключей по листам | 0.5–1 дн |
| Отчёты + документация | 1 дн |

**Итого:** ~4–5 дней после утверждения.
