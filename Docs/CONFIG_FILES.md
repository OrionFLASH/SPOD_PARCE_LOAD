# Конфигурация SPOD: каталог `config/`

С версии **1.7.53** монолитный корневой `config.json` заменён набораом файлов в каталоге **`config/`**.  
В памяти программа по-прежнему работает с **одним** словарём настроек (API класса `Config` не ломался для пайплайна).

Связанные документы:

- справочник каталога конфигурации: этот файл;
- входные CSV/JSON-каталог: [`JSON/SPOD_INPUT_DATA_CATALOG.md`](JSON/SPOD_INPUT_DATA_CATALOG.md);
- ROADMAP п. **8** (решение: вариант **B**, каталог `config/`, без fallback монолита).

**Утверждённые решения по раскладке (2026-07-15):** вход `config/config.json`; домены `CONFIG_RUN_INPUT` / `CHECKS` / `FORMATS` / `MERGE` / `RATING` / `ORDER` / `MANAGER`; архив SQLite внутри RUN_INPUT; POST копирует весь `config/`; корневого `config.json` нет.

---

## 1. Структура каталога

```text
config/
  config.json                 ← точка входа ($include + опциональные оверрайды)
  CONFIG_RUN_INPUT.json       ← запуск, пути, input_files, архив SQLite
  CONFIG_CHECKS.json          ← consistency_checks и связанное
  CONFIG_FORMATS.json         ← color_scheme, column_formats
  CONFIG_MERGE.json           ← merge, SUMMARY, sheet_order, gender, json_columns
  CONFIG_RATING.json          ← rating_item_matrix
  CONFIG_ORDER.json           ← season_order_summary
  CONFIG_MANAGER.json         ← manager_stats
```

В **корне репозитория** файла `config.json` **нет**.  
Пути **`IN/`**, **`OUT/`**, **`LOGS/`** считаются от **корня репозитория** (родитель `config/`), не от самого каталога конфигов.

Загрузка: `src/config_loader.py` → `load_config_dict()` / `Config()`.

---

## 2. Точка входа `config/config.json`

### Параметры

| Ключ | Тип | Обязателен | Назначение |
|------|-----|------------|------------|
| `$include` | массив строк | да (для split) | Список JSON-файлов в том же каталоге, порядок = порядок слияния |
| любые другие ключи | object / list / scalar | нет | **Оверрайды**: перекрывают значения из include (deep-merge для объектов) |
| `_config_layout_note` | строка | нет | Комментарий для людей (программой не используется) |

### Правила `$include`

1. Пути **только относительные** к каталогу `config/`, без `..`.  
2. Каждый include — JSON-**объект**.  
3. **Вложенные** `$include` внутри доменных файлов **запрещены**.  
4. Один и тот же **top-level ключ** не может встречаться в двух include → ошибка при старте.  
5. Списки на верхнем уровне при оверрайде **заменяются целиком** (не склеиваются).  
6. Вложенные объекты при оверрайде сливаются (**deep-merge**).

### Пример 1 — минимальный вход (как в репозитории)

```json
{
  "$include": [
    "CONFIG_RUN_INPUT.json",
    "CONFIG_CHECKS.json",
    "CONFIG_FORMATS.json",
    "CONFIG_MERGE.json",
    "CONFIG_RATING.json",
    "CONFIG_ORDER.json",
    "CONFIG_MANAGER.json"
  ]
}
```

### Пример 2 — оверрайд только запуска (не трогая доменные файлы)

```json
{
  "$include": [
    "CONFIG_RUN_INPUT.json",
    "CONFIG_CHECKS.json",
    "CONFIG_FORMATS.json",
    "CONFIG_MERGE.json",
    "CONFIG_RATING.json",
    "CONFIG_ORDER.json",
    "CONFIG_MANAGER.json"
  ],
  "run_blocks": ["PROM", "IFT"],
  "run_outputs": {
    "PROM": ["main_only", "consistency_only"],
    "IFT": ["source_only", "main_only"],
    "PSI": ["main_only"]
  },
  "run_blocks_parallel": true
}
```

Здесь `run_blocks` / `run_outputs` из entry **перекрывают** одноимённые ключи из `CONFIG_RUN_INPUT.json`.

### Пример 3 — ошибка (два файла объявляют один ключ)

`A.json`: `{ "paths": {…} }`  
`B.json`: `{ "paths": {…} }`  
→ `ValueError: Дублирующий ключ конфигурации «paths»`.

---

## 3. `CONFIG_RUN_INPUT.json` — запуск, пути, входы, архив

### 3.1. `run_blocks`

| | |
|--|--|
| **Тип** | массив строк |
| **Значения** | `PROM`, `IFT`, `PSI` |
| **По умолчанию** | `["PROM"]` |
| **Смысл** | Какие среды/блоки обработать в одном запуске |

**Пример:**

```json
"run_blocks": ["PROM"]
```

Несколько блоков:

```json
"run_blocks": ["PROM", "IFT", "PSI"]
```

### 3.2. `run_blocks_parallel`

| | |
|--|--|
| **Тип** | boolean |
| **По умолчанию** | `false` |
| **Смысл** | При ≥2 блоках в `run_blocks` — параллельный прогон (отдельные процессы); вывод в консоль пачками по блоку |

```json
"run_blocks_parallel": false
```

### 3.3. `run_outputs`

| | |
|--|--|
| **Тип** | массив **или** объект по блокам |
| **Токены** | см. таблицу ниже |

**Токены (все допустимые):**

| Токен | Что делает |
|-------|------------|
| `source_only` | Excel с сырыми листами после загрузки |
| `main_only` | Основная книга (merge, SUMMARY, …) |
| `consistency_only` | Книга / ранний выход с проверками консистентности |
| `manager_stats_only` | Книга MANAGER_STATS |
| `stat_file_only` | `STAT_FILE <timestamp>.xlsx` с таймингами |
| `rating_item_matrix` | Колонки ITEM на листе RATING (нужен ещё main/merge) |
| `season_order_summary` | Лист ORDER-SEASON-SUMMARY |

**Вариант A — одинаково для всех блоков:**

```json
"run_outputs": ["main_only"]
```

**Вариант B — свой набор на блок (рекомендуется):**

```json
"run_outputs": {
  "PROM": ["main_only"],
  "IFT": ["main_only", "source_only"],
  "PSI": ["consistency_only"]
}
```

Ключ `default` (или `*`, `ALL`) — запасной набор, если для блока нет раздела.

### 3.4. `paths`

| Ключ | Пример | Смысл |
|------|--------|--------|
| `input` | `"IN"` | Корень входа → `IN/<BLOCK>/{SPOD,FILE,…}` |
| `output` | `"OUT"` | Корень выхода → `OUT/<BLOCK>/YYYY/DD-MM/` |
| `logs` | `"LOGS"` | Логи → `LOGS/YYYY/DD-MM/` |

```json
"paths": {
  "input": "IN",
  "output": "OUT",
  "logs": "LOGS"
}
```

### 3.5. `output_filenames`

Шаблоны имён без расширения и timestamp. Плейсхолдер **`{BLOCK}`** → `PROM` / `IFT` / `PSI`.

```json
"output_filenames": {
  "main": "SPOD_{BLOCK} main",
  "source": "SPOD_{BLOCK} source",
  "consistency": "SPOD_{BLOCK} consistency",
  "manager_stats": "SPOD_{BLOCK} MANAGER_STATS"
}
```

### 3.6. `logging` / `performance`

```json
"logging": {
  "level": "INFO",
  "base_name": "LOGS"
},
"performance": {
  "max_workers_io": 8,
  "max_workers_cpu": 4
}
```

| Параметр | Варианты / смысл |
|----------|------------------|
| `logging.level` | `DEBUG`, `INFO`, `WARNING`, … — минимальный уровень **в файл** лога |
| `logging.base_name` | Префикс имени лог-файла |
| `max_workers_io` | Параллельное чтение CSV |
| `max_workers_cpu` | CPU-этапы (merge и др.) |

### 3.7. `apply_sort_to_source` / `apply_sort_to_main`

```json
"apply_sort_to_source": true,
"apply_sort_to_main": false
```

Включают применение `sort_columns` из записей `input_files` к книгам source / main.

### 3.8. `input_files`

Объект с разделами **`PROM`**, **`IFT`**, **`PSI`**. В каждом — полный список файлов блока.

**Поля записи (основные):**

| Поле | Тип | Пример | Смысл |
|------|-----|--------|--------|
| `file` | string | `"CONTEST (PROM) 03-07 v0.csv"` | Имя файла |
| `sheet` | string | `"CONTEST-DATA"` | Имя листа Excel |
| `subdir` | string | `"PROM/SPOD"` | Каталог относительно `paths.input` |
| `aggregate_into_sheet` | string | `"RATING"` / `"ORDER"` / `""` | Склейка в агрегат |
| `archive_to_db` | bool | `true` | Писать в SQLite-архив |
| `archive_db_path` | string | `"OUT/DB/{BLOCK}/spod_input_archive_{BLOCK}.sqlite"` | Путь БД (`{BLOCK}` подставляется) |
| `include_in_source` | bool | `true`/`false` | Лист в source Excel |
| `sort_columns` | array | `[{ "column": "…", "ascending": true }]` | Сортировка |
| `freeze`, `col_width_mode`, … | | | Оформление листа |

**Пример фрагмента:**

```json
"input_files": {
  "PROM": [
    {
      "file": "CONTEST (PROM) 03-07 v0.csv",
      "sheet": "CONTEST-DATA",
      "subdir": "PROM/SPOD",
      "archive_to_db": true,
      "include_in_source": true,
      "archive_db_path": "OUT/DB/{BLOCK}/spod_input_archive_{BLOCK}.sqlite"
    },
    {
      "file": "PROM_ALPHA_gamification-statistics.csv",
      "sheet": "STATISTICS",
      "subdir": "PROM/FILE",
      "aggregate_into_sheet": "",
      "archive_to_db": true,
      "include_in_source": false,
      "archive_db_path": "OUT/DB/{BLOCK}/spod_gamification_archive_{BLOCK}.sqlite"
    }
  ],
  "IFT": [ … ],
  "PSI": [ … ]
}
```

Итоговый путь файла:  
`<корень>/IN` + `/` + `subdir` + `/` + `file`  
→ например `…/IN/PROM/SPOD/CONTEST (PROM) 03-07 v0.csv`.

### 3.9. `input_archive_sqlite`

| Поле | Пример | Смысл |
|------|--------|--------|
| `enabled` | `true` | Включить архив |
| `row_level_archive` | `true` | Схема v2 (по строкам) |
| `db_path` | `"OUT/DB/{BLOCK}/spod_input_archive_{BLOCK}_v2.sqlite"` | Основная БД блока |
| `legacy_db_path` | `"OUT/DB/{BLOCK}/spod_input_archive_{BLOCK}.sqlite"` | Путь v1 |
| `default_archive_to_db` | `false` | Если у файла нет `archive_to_db` — не архивировать |
| `parallel_row_processing` | object | Параллельный hash/compare (см. Docs архива) |
| `default_row_key_by_sheet` | object | Ключи строк по листам |

Подробно: `Docs/INPUT_ARCHIVE_ROW_LEVEL.md`.

---

## 4. `CONFIG_CHECKS.json` — консистентность

### Ключи

| Ключ | Смысл |
|------|--------|
| `consistency_checks` | Основной объект: `summary_sheet_name`, `rules`, `csv_columns_count`, подсказки |
| `tournament_status_choices` | Список статусов турнира (справочник для UI/проверок) |

### `consistency_checks.rules[]` — типы (кратко)

| `type` | Назначение | Ключевые поля |
|--------|------------|---------------|
| `unique` | Уникальность ключа | `sheet`, `key_columns`, `unique_*` |
| `field_length` | Длина поля | `sheet`, `column`, `max_length` |
| `referential` / `referential_composite` | Ссылки между листами | `sheet`/`sheet_src`, ключи, фильтры |
| `field_in_values` | Значение ∈ списку | `allowed_values`, `allow_empty` |
| `field_format` | Формат | шаблоны дат/чисел |
| `json_*` / `json_spod_format` | JSON в ячейках | `json_column`, … |

**Пример правила:**

```json
{
  "id": "in_schedule_tournament_status",
  "name": "TOURNAMENT_STATUS из допустимого списка",
  "type": "field_in_values",
  "sheet": "TOURNAMENT-SCHEDULE",
  "column": "TOURNAMENT_STATUS",
  "allowed_values": ["УДАЛЕН", "ЗАВЕРШЕН", "АКТИВНЫЙ", "ПОДВЕДЕНИЕ ИТОГОВ", "ОТМЕНЕН"],
  "allow_empty": false,
  "enabled": true
}
```

Полный формат: `Docs/CONSISTENCY_CHECKS_FORMAT.md`.

### `csv_columns_count`

Контроль числа колонок по имени листа (`expected_columns: 0` = проверка отключена).

---

## 5. `CONFIG_FORMATS.json` — цвета и форматы ячеек

### `color_scheme[]`

Группы оформления заголовков/колонок (фон, шрифт, область применения).

```json
{
  "group": "SPOD_KEYS",
  "header_bg": "1F4E79",
  "header_fg": "FFFFFF",
  "column_bg": "D6EAF8",
  "column_fg": "000000",
  "style_scope": "header_and_columns",
  "sheets": ["REPORT", "REWARD"],
  "columns": ["CONTEST_CODE", "TOURNAMENT_CODE"]
}
```

### `column_formats[]`

Типы данных Excel (число, дата, текст), выравнивание, перенос.

```json
{
  "sheet": "RATING",
  "columns": ["Количество кристаллов"],
  "data_type": "number",
  "decimal_places": 0,
  "thousands_separator": true,
  "horizontal": "center",
  "vertical": "center",
  "wrap_text": false
}
```

| `data_type` | Смысл |
|-------------|--------|
| `number` | Число |
| `date` | Дата (`date_format`) |
| `text` / др. | Текст |

Форматы книги **MANAGER_STATS** живут внутри `CONFIG_MANAGER.json` → `manager_stats.column_formats`, не здесь.

---

## 6. `CONFIG_MERGE.json` — куда что добавляется

| Ключ | Смысл |
|------|--------|
| `merge_fields_advanced` | Правила: `sheet_src` → `sheet_dst`, ключи, колонки, фильтры |
| `summary_sheet` | Параметры листа SUMMARY |
| `summary_key_defs` | Какие ключевые колонки тянуть в SUMMARY с каких листов |
| `sheet_order` | Порядок вкладок в Excel |
| `gender` | Паттерны определения пола по ФИО |
| `json_columns` | Какие колонки разворачивать из JSON при загрузке |
| `reward_getcondition_summary` | Сводная колонка getCondition на REWARD |
| `derived_columns` / `source_export` | Опционально |

### Пример правила merge

```json
{
  "sheet_src": "CONTEST-DATA",
  "sheet_dst": "REPORT",
  "src_key": ["CONTEST_CODE"],
  "dst_key": ["CONTEST_CODE"],
  "column": ["CONTEST_TYPE", "FULL_NAME", "BUSINESS_STATUS"],
  "mode": "value",
  "multiply_rows": false,
  "status_filters": {
    "BUSINESS_STATUS": ["АКТИВНЫЙ", "ПОДВЕДЕНИЕ ИТОГОВ"]
  }
}
```

| `mode` | Смысл |
|--------|--------|
| `value` | Подставить значение(я) |
| другие | см. код `main_impl` / README → merge_fields_advanced |

### Пример `summary_key_defs`

```json
"summary_key_defs": [
  { "sheet": "CONTEST-DATA", "cols": ["CONTEST_CODE"] },
  { "sheet": "TOURNAMENT-SCHEDULE", "cols": ["TOURNAMENT_CODE", "CONTEST_CODE"] }
]
```

---

## 7. `CONFIG_RATING.json` — матрица ITEM на RATING

Секция **`rating_item_matrix`**.

| Поле | Пример | Смысл |
|------|--------|--------|
| `enabled` | `true` | Глобальный тумблер (токен `rating_item_matrix` в `run_outputs` тоже нужен) |
| `sheet_rating` / `sheet_order` / `sheet_reward` | `"RATING"`, `"ORDER"`, `"REWARD"` | Листы |
| `order_status_exclude` | `["Отклонён", "Отменён"]` | Фильтр заказов |
| `item_order_groups` | группы SEASON / max | Лимиты заказов по группе |
| пороги мест / itemAmount | | См. `Docs/RATING_MATRIX_COLORS_AND_LOGIC.md` |

```json
"rating_item_matrix": {
  "enabled": true,
  "sheet_rating": "RATING",
  "sheet_order": "ORDER",
  "sheet_reward": "REWARD",
  "order_status_exclude": ["Отклонён", "Отменён"]
}
```

Токен в `run_outputs`: `"rating_item_matrix"`.

---

## 8. `CONFIG_ORDER.json` — сводка заказов сезона

Секция **`season_order_summary`**.

```json
"season_order_summary": {
  "enabled": true,
  "sheet_name": "ORDER-SEASON-SUMMARY"
}
```

Токен в `run_outputs`: `"season_order_summary"`.  
Логика и колонки «КМ:» — `Docs/SEASON_ORDER_SUMMARY.md`, `SEASON_ORDER_SUMMARY_KM_LOGIC.md`.  
Группы заказов часто берутся из `rating_item_matrix.item_order_groups`.

---

## 9. `CONFIG_MANAGER.json` — книга MANAGER_STATS

Секция **`manager_stats`** (большая): источники табельных, enrich, JS AutoRun, `column_formats` этой книги, лист PROM_TOURNAMENTS и т.д.

Минимальный каркас:

```json
"manager_stats": {
  "output_sheet": "TAB_NUMBERS",
  "summary_sheet": "MANAGER_STATS_SUMMARY",
  "sources": [ … ],
  "enrich_columns": [ … ]
}
```

Полное описание: **`Docs/MANAGER_STATS.md`**.  
Токен: `manager_stats_only` в `run_outputs`.

---

## 10. Как программно читать конфиг

```python
from src.config_loader import Config, load_config_dict, default_config_path

# Как main.py
config = Config()
print(config.dir_input)          # …/IN
print(config.run_blocks)         # ['PROM']
print(config.input_files)        # список файлов текущего/первого блока

# Только dict
cfg = load_config_dict(default_config_path())
print(cfg["paths"]["output"])
```

Явный путь (тесты / другой стенд):

```python
Config("/abs/path/to/config/config.json")
```

---

## 11. POST / sync

Снимок программы копирует **весь каталог `config/`** с сохранением структуры:

```text
POST/config/config.json.txt
POST/config/CONFIG_RUN_INPUT.json.txt
…
```

Инструменты: `src/Tools/sync_post_txt.py`, `pack_post_encrypted_program.py`.  
На целевом ПК — снять суффикс `.txt` (или `restore_names_from_txt.bat`) и положить каталог `config/` рядом с `main.py`.

---

## 12. Частые сценарии правок

| Задача | Куда править |
|--------|----------------|
| Включить IFT / PSI | `CONFIG_RUN_INPUT.json` → `run_blocks` (или оверрайд в `config/config.json`) |
| Только source Excel | `run_outputs`: `["source_only"]` |
| Новый CSV для PROM | `CONFIG_RUN_INPUT.json` → `input_files.PROM[]` + файл в `IN/PROM/…` |
| Новое правило проверки | `CONFIG_CHECKS.json` → `consistency_checks.rules` |
| Цвет колонки | `CONFIG_FORMATS.json` → `color_scheme` / `column_formats` |
| Добавить колонку из CONTEST в REPORT | `CONFIG_MERGE.json` → `merge_fields_advanced` |
| Настройка матрицы RATING | `CONFIG_RATING.json` |
| Сводка ORDER | `CONFIG_ORDER.json` |
| Enrich менеджеров | `CONFIG_MANAGER.json` |

---

## 13. Миграция со старого корневого `config.json`

1. Каталог `config/` уже разбит по доменам.  
2. Корневой монолит **удалён** (fallback нет).  
3. Обновите скрипты/документацию: путь по умолчанию — `config/config.json`.  
4. Старая общая БД `OUT/DB/spod_input_archive_v2.sqlite` не удаляется автоматически; новые пути — `OUT/DB/<BLOCK>/…` (см. `Docs/BLOCKS_MIGRATION.md`).
