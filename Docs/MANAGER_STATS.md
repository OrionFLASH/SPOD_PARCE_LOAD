# MANAGER_STATS: табельные и обогащение колонок

Отдельная книга Excel со списком уникальных табельных номеров и дополнительными полями из листов PROM/FILE.

## Включение

```json
"run_outputs": ["manager_stats_only"]
```

Комбинации с другими токенами (выполняются **все** перечисленные):

| Токен | Поведение |
|-------|-----------|
| `manager_stats_only` | Книга **`SPOD_PROM MANAGER_STATS <timestamp>.xlsx`** |
| `manager_stats_only` без `main_only` | Ранний выход после merge (фаза 03), без SUMMARY и основного Excel |
| `manager_stats_only` + `main_only` | Полный прогон: основная книга + MANAGER_STATS в конце |
| `stat_file_only` | Отдельный **`STAT_FILE <timestamp>.xlsx`** (время этапов) |

Имя файла: **`output_filenames.manager_stats`** (по умолчанию `SPOD_PROM MANAGER_STATS`).

## Секция `manager_stats` в config.json

| Ключ | Назначение |
|------|------------|
| `output_sheet` | Лист с табельными (по умолчанию `TAB_NUMBERS`) |
| `summary_sheet` | Сводка по правилам сбора (`MANAGER_STATS_SUMMARY`) |
| `normalize_pad_width` | Длина табельного с ведущими нулями (20) |
| `freeze` | Закрепление областей Excel (`E2` — строка 1 и колонки A–D) |
| `enrich_default` | Значение по умолчанию, если данные не найдены (`-`) |
| `enrich_parallel` | Потоки для enrich: `enabled`, `max_workers` (0=авто), `min_tabs_for_parallel`, `chunk_size` |
| `column_widths` | Ширина колонок листа TAB_NUMBERS (`added_columns_width`) |
| `sources` | Откуда собирать **уникальные** табельные |
| `enrich_columns` | Дополнительные колонки с приоритетным забором значений |

### `sources[]`

- `sheet` или `sheet_pattern` (glob, напр. `RATING_*`)
- `tab_column` — колонка табельного на листе
- `where_in` / `where_not_in` — фильтры строк (логическое И между колонками)
- `enabled: false` — пропуск правила

#### Фильтры EMPLOYEE при сборе табельных (`employee_person`, `employee_person_add`)

В `where_not_in` заданы исключения по строкам листа **EMPLOYEE** (логическое **И** между колонками):

| Колонка | Исключаемые значения |
|---------|----------------------|
| `SURNAME` | `…`, `...` (заглушки без фамилии) |
| `POSITION_NAME` | `КПК`, `ГОСБ`, `Управление МБ`, `ОСБ`, `ТБ`, `Центральный аппарат`, `Сбер Факторинг`, `Сбербанк Лизинг` |

**Два этапа применения:**

1. **Правила `employee_person` / `employee_person_add`** — табельные с отфильтрованных строк не добавляются при заборе с EMPLOYEE.
2. **`employee_placeholder_exclusion`** (в коде, после объединения всех `sources`) — те же критерии `where_not_in` из правил EMPLOYEE используются для **удаления табельных из итогового списка**, если они попали с других листов (REPORT, RATING, STATISTICS и т.д.).

На этапе **`enrich_columns`** эти фильтры **не** применяются: обогащение работает с уже сформированным списком уникальных табельных.

Пример фрагмента `config.json`:

```json
{
  "id": "employee_person",
  "sheet": "EMPLOYEE",
  "tab_column": "PERSON_NUMBER",
  "where_not_in": {
    "SURNAME": ["…", "..."],
    "POSITION_NAME": ["КПК", "ГОСБ", "Управление МБ", "ОСБ", "ТБ", "Центральный аппарат", "Сбер Факторинг", "Сбербанк Лизинг"]
  }
}
```

В сводке **`MANAGER_STATS_SUMMARY`** появляется строка `employee_placeholder_exclusion` с числом табельных, убранных на финальном шаге.

### `enrich_columns[]`

| Параметр | Описание |
|----------|----------|
| `output_column` | Имя колонки в TAB_NUMBERS |
| `lookup_row_key` | Составной ключ из уже заполненных колонок строки (напр. `["ТБ","ГОСБ"]`) |
| `mode` | `value`, `sum`, `count`, `exists` |
| `present_value` | Для `exists`: что писать при наличии строк (напр. `ДА`) |
| `multi_row` | Только `value`: `first` или `join` |
| `join_separator` | Разделитель для `join` (`;`) |
| `default` | Если нигде не найдено |
| `sources[]` | Цепочка по `priority` (меньше = выше) |

**Поведение поиска:**

- `value` + `first`, `sum`, `count` — **первый** источник с данными, дальше не ищем
- `value` + `join` — уникальные значения со **всех** источников через `join_separator`
- `sum` / `count` — только по первому источнику с подходящими строками

**Источник enrich (`sources[]` внутри колонки):**

- `tab_column` — сопоставление по табельному (нормализация 20 знаков)
- `key_columns` — сопоставление по составному ключу на листе (напр. `TB_CODE`, `GOSB_CODE`)
- `value_column` — откуда брать значение (не нужен для `count` / `exists`)
- `where_in` / `where_not_in` — фильтры; для булевых полей: `true` / `false` / `1` / `да`

### Примеры в config

**Фамилия** (цепочка RATING → STATISTICS → EMPLOYEE):

1. RATING, `Период` = «Сезон 2026»
2. RATING без фильтра
3. STATISTICS, `Текущая роль` = true
4. EMPLOYEE по `PERSON_NUMBER` → `SURNAME`
5. EMPLOYEE по `PERSON_NUMBER_ADD` → `SURNAME`

**TB_FULL_NAME / GOSB_NAME** — по `lookup_row_key` `["ТБ","ГОСБ"]` из листа `ORG_UNIT_V20` (`key_columns`: `TB_CODE`, `GOSB_CODE`).

**есть в текущем рейтинге** — `mode: exists`, `present_value: ДА`, лист RATING с фильтрами роли КМККСБ и периода «Сезон 2026».

### Ширина колонок

```json
"column_widths": {
  "Табельный номер": { "width_mode": 24 },
  "Источники": { "width_mode": "AUTO", "min_width": 50, "max_width": 80 }
}
```

## Код

| Модуль | Назначение |
|--------|------------|
| `src/manager_stats.py` | Сбор табельных, индексы enrich, параллельный lookup |
| `src/main_impl.py` | `_write_manager_stats_excel`, ветвления `run_outputs` |
| `src/config_loader.py` | `parse_run_outputs_config` |
| `src/console_ui.py` | `print_manager_stats_summary`, фазы прогресса |
| `src/Tests/test_manager_stats.py` | Тесты enrich и sources |
| `src/Tests/test_run_outputs.py` | Тесты комбинаций `run_outputs` |

## Производительность enrich

Раньше для каждого табельного выполнялся полный проход по листам. Сейчас один раз строятся индексы `tab → значение`, lookup — O(1); при большом числе табельных — `ThreadPoolExecutor` (см. `enrich_parallel`).
