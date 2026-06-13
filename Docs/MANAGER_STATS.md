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
| `summary_sheet` | Сводка по правилам сбора и обогащению (`MANAGER_STATS_SUMMARY`) |
| `normalize_pad_width` | Длина табельного с ведущими нулями (20) |
| `freeze` | Закрепление областей Excel (`E2` — строка 1 и колонки A–D) |
| `enrich_default` | Значение по умолчанию, если данные не найдены (`-`) |
| `enrich_parallel` | Потоки для enrich: `enabled`, `max_workers` (0=авто), `min_tabs_for_parallel`, **`min_fields_for_parallel`** (параллель enrich tab-колонок), `chunk_size` |
| `column_widths` | Ширина колонок листа TAB_NUMBERS (`added_columns_width`) |
| `sources` | Откуда собирать **уникальные** табельные |
| `enrich_columns` | Дополнительные колонки с приоритетным забором значений |
| `prom_tournament_catalog` | Каталог турниров/наград ПРОМ и динамические колонки на TAB_NUMBERS (см. ниже) |

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

### Лист MANAGER_STATS_SUMMARY

Единая таблица документации того, что попадает на **TAB_NUMBERS**:

| Колонка сводки | Содержание |
|----------------|------------|
| `Раздел` | `Сбор табельных` / `Обогащение` / **`PROM колонки`** / `Формат Excel` |
| `Колонка TAB_NUMBERS` | Имя добавляемой колонки (для sources — `Табельный номер`) |
| `ID` | `id` правила sources или enrich |
| `Приоритет` | Порядок источника enrich (меньше — раньше) |
| `Лист` | Лист или `pattern:RATING_*` |
| `Сопоставление` | Колонка табельного, составной ключ или `lookup_row_key` |
| `Колонка значения` | Откуда брать значение на листе |
| `Режим` | `табельный номер` / `value` / `sum` / `count` / `exists` / `number` |
| `Логика` | Как объединяются источники (`value+first`, `value+join`, …) |
| `Фильтры` | `where_in` / `where_not_in` |
| `Примечание` | Статистика sources, `default`, `lookup_row_key` |

Между блоками — пустая строка-разделитель.

### `enrich_columns[]`

| Параметр | Описание |
|----------|----------|
| `output_column` | Имя колонки в TAB_NUMBERS |
| `lookup_row_key` | Составной ключ из уже заполненных колонок строки (напр. `["ТБ","ГОСБ"]`) |
| `mode` | `value`, `sum`, `count`, `exists` |
| `present_value` | Для `exists`: что писать при наличии строк (на уровне `sources[]`, напр. `КМ`, `AKM`) |
| `multi_row` | `first` или `join`; для `value` — одна строка или уникальные со всех источников; для `exists` + `join` — коды всех подходящих ролей через `join_separator` |
| `join_separator` | Разделитель для `join` (по умолчанию `;`, в config для рейтинга — `; `) |
| `default` | Если нигде не найдено |
| `sources[]` | Цепочка по `priority` (меньше = выше) |

**Поведение поиска:**

- `value` + `first`, `sum`, `count` — **первый** источник с данными, дальше не ищем
- `value` + `join` — уникальные значения со **всех** источников через `join_separator`
- `exists` + `first` — «есть строка» по первому источнику с совпадением
- `exists` + `join` — один проход по листу: для каждой строки RATING проверяются **все** источники-роли; уникальные коды `present_value` объединяются через `join_separator` (порядок по `priority`)
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

**есть в текущем рейтинге** — `mode: exists`, лист **RATING**, период «Сезон 2026»; вместо «ДА» — код роли (`present_value` у каждого источника):

| Наименование Роли | Значение |
|-------------------|----------|
| Клиентский менеджер крупнейшего, крупного и среднего бизнеса | КМ |
| Ассистент клиентского менеджера ККСБ | AKM |
| Менеджер нефинансовых сервисов | МНС |
| Руководитель проектов по технологическому развитию клиентов | CSM |

Проверка **всех** строк листа RATING с `Период` = «Сезон 2026»; для каждой подходящей роли — свой код (`present_value` у источника). Режим `multi_row: join`: если у табельного несколько ролей — коды через `; ` (например `КМ; CSM`); если ни одна роль не найдена — `-`. В логе enrich для этой колонки строится **один** комбинированный индекс на лист.

**Email Sigma / Email Alpha** — `value` + `first`:

1. STATISTICS → `Почта Сигма` / `Почта Альфа`
2. ORDER → `Email в домене Sigma` / `Email в домене Alpha` (без отклонённых и отменённых заказов)

**Активность в приложении (STATISTICS)** — `sum` по табельному; при нескольких строках STATISTICS значения **суммируются**:

| Источник (STATISTICS) | Колонка TAB_NUMBERS |
|------------------------|---------------------|
| Октябрь 2025 дней | `10_2025 (дней)` |
| Ноябрь 2025 дней | `11_2025 (дней)` |
| Декабрь 2025 дней | `12_2025 (дней)` |
| Январь 2026 дней | `01_2026 (дней)` |
| … | … |
| Июнь 2026 дней | `06_2026 (дней)` |
| Октябрь 2025 входов | `10_2025 (входы)` |
| … | … |
| Июнь 2026 входов | `06_2026 (входы)` |

Период: октябрь 2025 — июнь 2026 (9 месяцев × 2 метрики = 18 колонок). Если данных нет — `-`. Числовой формат Excel — в `column_formats.columns`.

**RATING по группам роль + период** (роль: КМ ККСБ) — для каждой пары `Наименование Роли` + `Период` четыре колонки с суффиксом `| <период>`:

| Период | Колонки |
|--------|---------|
| 1 полугодие 2025 | `Количество кристаллов \| …`, `Место в рейтинге по стране \| …`, `Место в рейтинге ТБ \| …`, `Место в рейтинге ГОСБ \| …` |
| 2 полугодие 2025 | то же |
| За всё время | то же |
| Сезон 2024 | то же |
| Сезон 2026 | то же |

Источник: лист **RATING**, `where_in` по роли и периоду; если строки нет — `-`. При записи в Excel колонки с префиксами `Количество кристаллов |`, `Место в рейтинге … |` получают **числовой формат** (см. `manager_stats.column_formats`).

### Ширина и формат колонок TAB_NUMBERS

`column_widths` — ширина отдельных колонок.

`column_formats` — числовой/датовый формат при записи Excel (префиксы имён колонок RATING-групп):

```json
"column_formats": [
  {
    "column_prefixes": [
      "Количество кристаллов |",
      "Место в рейтинге по стране |",
      "Место в рейтинге ТБ |",
      "Место в рейтинге ГОСБ |"
    ],
    "data_type": "number",
    "decimal_places": 0,
    "thousands_separator": true
  },
  {
    "columns": ["ТБ", "ГОСБ"],
    "data_type": "number",
    "decimal_places": 0,
    "thousands_separator": false
  }
]
```

Пример `column_widths`:

```json
"column_widths": {
  "Табельный номер": { "width_mode": 24 },
  "Источники": { "width_mode": "AUTO", "min_width": 50, "max_width": 80 }
}
```

## Лист PROM_TOURNAMENTS (`prom_tournament_catalog`)

Третий лист книги MANAGER_STATS — каталог турниров, конкурсов и наград **vid = ПРОМ** за 2026 год.

### Включение

```json
"prom_tournament_catalog": {
  "enabled": true,
  "sheet_name": "PROM_TOURNAMENTS"
}
```

### Источники данных (объединение без дублей)

| № | Источник | Условие отбора |
|---|----------|----------------|
| 1 | **TOURNAMENT-SCHEDULE** + **REWARD-LINK** | Статус `АКТИВНЫЙ` или `ПОДВЕДЕНИЕ ИТОГОВ`, **или** `START_DT` / `END_DT` содержат `date_year` (2026) |
| 2 | **LIST-REWARDS** + **TOURNAMENT-SCHEDULE** | `Дата создания` содержит `date_year`; join по `Код турнира` = `TOURNAMENT_CODE` |

В обоих случаях остаются только конкурсы из **CONTEST-DATA** с `CONTEST_FEATURE.vid = ПРОМ` (`contest_vid`).

Дедупликация строк: `(TOURNAMENT_CODE, CONTEST_CODE, REWARD_CODE)`.

### Колонки листа PROM_TOURNAMENTS

| Колонка | Источник |
|---------|----------|
| `№` | Порядковый номер после сортировки |
| `TOURNAMENT_CODE`, `PERIOD_TYPE`, `START_DT`, `END_DT`, `TOURNAMENT_STATUS` | TOURNAMENT-SCHEDULE |
| `CONTEST_CODE` | TOURNAMENT-SCHEDULE / LIST-REWARDS |
| `CONTEST_TYPE` | CONTEST-DATA → метка: `ТУРНИРНЫЙ` → **ТУРНИР**, `ИНДИВИДУАЛЬНЫЙ` / `ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ` → **НАГРАДА** |
| `PRODUCT`, `PRODUCT_GROUP` | CONTEST-DATA по `CONTEST_CODE` |
| `REWARD_CODE` | REWARD-LINK или LIST-REWARDS |
| `FULL_NAME` | Наименование конкурса (CONTEST-DATA) |
| `REWARD_FULL_NAME` | Наименование награды (REWARD) |
| `получено наград` | Число **всех** строк LIST-REWARDS по паре `TOURNAMENT_CODE` + `REWARD_CODE` (без фильтра по году) |

`START_DT` / `END_DT` — формат даты **YYYY-MM-DD** в Excel.

### Сортировка PROM_TOURNAMENTS

Многоуровневая (как диалог Excel), по возрастанию:

1. `START_DT`
2. `REWARD_CODE`
3. `PRODUCT`
4. `PRODUCT_GROUP`
5. `CONTEST_TYPE`

### Параметры `prom_tournament_catalog`

| Ключ | По умолчанию | Назначение |
|------|--------------|------------|
| `schedule_sheet` | `TOURNAMENT-SCHEDULE` | Расписание турниров |
| `reward_link_sheet` | `REWARD-LINK` | Связь конкурс ↔ награда |
| `contest_sheet` | `CONTEST-DATA` | Конкурсы |
| `reward_sheet` | `REWARD` | Справочник наград |
| `list_rewards_sheet` | `LIST-REWARDS` | Выдачи наград |
| `active_statuses` | `АКТИВНЫЙ`, `ПОДВЕДЕНИЕ ИТОГОВ` | Статусы для отбора по расписанию |
| `date_year` | `2026` | Год в датах / `Дата создания` |
| `contest_vid` | `ПРОМ` | Фильтр vid в CONTEST_FEATURE |
| `rewards_received_column` | `получено наград` | Имя колонки счётчика на PROM_TOURNAMENTS |

## Динамические колонки PROM на TAB_NUMBERS

При `tab_columns_enabled: true` (по умолчанию) на лист **TAB_NUMBERS** добавляются колонки-счётчики **по каждому табельному**.

### Что считается

Для каждой колонки — число строк **LIST-REWARDS**, где:

- `Табельный номер сотрудника` = табельный из строки TAB_NUMBERS;
- `Код турнира` + `Код награды` = пара из каталога PROM_TOURNAMENTS;
- `Дата создания` содержит `date_year` (2026).

Если выдач нет — `tab_columns_default` (по умолчанию **0**).

### Имена колонок

| Тип | Шаблон | Пример |
|-----|--------|--------|
| **НАГРАДА** | `НАГРАДА {REWARD_FULL_NAME} ({START_DT}) [{PRODUCT}]` | `НАГРАДА Награда B1 (2026-03-01) [Эффективность КМ]` |
| **ТУРНИР** | `ТУРНИР {FULL_NAME} ({START_DT}) [{PRODUCT}]` | `ТУРНИР Конкурс A (2025-01-01) [Рейтинг]` |

`FULL_NAME` — наименование конкурса; `REWARD_FULL_NAME` — наименование награды. Если пусто — подставляется код.

### Итоговые колонки по табельному

| Колонка | Позиция | Значение |
|---------|---------|----------|
| `НАГРАДА всего` (`tab_columns_total_nagrada`) | Первая в блоке НАГРАДА | Сумма всех колонок НАГРАДА по табельному |
| `ТУРНИР всего` (`tab_columns_total_tournament`) | Первая в блоке ТУРНИР | Сумма всех колонок ТУРНИР по табельному |

### Порядок колонок на TAB_NUMBERS

```
№ | Табельный номер | … enrich_columns … |
  НАГРАДА всего | НАГРАДА … | НАГРАДА … |
  ТУРНИР всего | ТУРНИР … | ТУРНИР … |
  Источники | Число источников
```

Порядок детальных PROM-колонок (внутри блока НАГРАДА / ТУРНИР):

1. `PRODUCT_GROUP`
2. `PRODUCT`
3. `CONTEST_CODE` (сортировка)
4. внутри конкурса — по `START_DT` (для НАГРАДА — `REWARD_CODE`, для ТУРНИР — `TOURNAMENT_CODE` в заголовке)

### Ширина и формат PROM-колонок

| Параметр | Значение |
|----------|----------|
| `tab_columns_width` | Фиксированная ширина (по умолчанию **7**) |
| `tab_columns_format` | Число по центру (`horizontal` / `vertical`: `center`) |

Колонки с префиксами `НАГРАДА ` и `ТУРНИР ` попадают под `tab_columns_format` через `column_prefixes`.

### Параметры TAB_NUMBERS (дополнение к `prom_tournament_catalog`)

| Ключ | По умолчанию |
|------|--------------|
| `tab_columns_enabled` | `true` |
| `tab_columns_default` | `0` |
| `tab_columns_width` | `7` |
| `tab_columns_total_nagrada` | `НАГРАДА всего` |
| `tab_columns_total_tournament` | `ТУРНИР всего` |

## Код

| Модуль | Назначение |
|--------|------------|
| `src/manager_stats.py` | Сбор табельных, индексы enrich, каталог PROM, динамические колонки TAB_NUMBERS |
| `src/main_impl.py` | `_write_manager_stats_excel`, ветвления `run_outputs` |
| `src/config_loader.py` | `parse_run_outputs_config` |
| `src/console_ui.py` | `print_manager_stats_summary`, фазы прогресса |
| `src/Tests/test_manager_stats.py` | Тесты enrich и sources |
| `src/Tests/test_run_outputs.py` | Тесты комбинаций `run_outputs` |

## Производительность

### Enrich (`enrich_parallel`)

Раньше для каждого табельного выполнялся полный проход по листам. Сейчас один раз строятся индексы `tab → значение`, lookup — O(1).

| Механизм | Условие | Эффект |
|----------|---------|--------|
| Параллель lookup по табельным | `min_tabs_for_parallel` (50+) | `ThreadPoolExecutor`, чанки `chunk_size` |
| Параллель enrich tab-колонок | `min_fields_for_parallel` (3+) | Несколько `enrich_columns` без `lookup_row_key` считаются параллельно |
| Колонки с `lookup_row_key` | — | Последовательно (нужны уже заполненные колонки строки) |

### PROM и LIST-REWARDS

| Оптимизация | Описание |
|-------------|----------|
| Кэш каталога | `build_prom_tournament_catalog_dataframe` вызывается **один раз** на прогон; результат переиспользуется для TAB_NUMBERS, сводки и листа PROM_TOURNAMENTS |
| Векторизация PROM-колонок | `groupby` + `merge` + `pivot` по длинному индексу LIST-REWARDS вместо вложенных циклов табельный × колонка × пара |
| `pd.concat` | PROM-колонки добавляются одним блоком (без фрагментации DataFrame) |

Основное время полного прогона часто занимает запись Excel (`08_write_manager_stats_excel`) — см. **`Docs/PERFORMANCE_OPTIMIZATION_PROPOSALS.md`**.
