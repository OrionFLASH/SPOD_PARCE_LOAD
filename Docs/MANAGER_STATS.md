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
| `profile_gp_load` | Дозаполнение из JSON профилей и автовыгрузка `Profile_GP_LOAD_AutoRun.js` (см. ниже) |

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
| `leaders_for_admin_column` | `запрос leadersForAdmin` | Признак турниров для скрипта `OUT/Tournament_LeadersForAdmin_AutoRun.js` |
| `leaders_for_admin_value_yes` | `ДА` | Значение, если турнир попадает в выгрузку leadersForAdmin |
| `leaders_for_admin_contest_type` | `ТУРНИРНЫЙ` | Фильтр CONTEST-DATA по `CONTEST_CODE` (вместе со статусом расписания) |

### Колонка «запрос leadersForAdmin»

`ДА` — `TOURNAMENT_CODE` входит в список автовыгрузки leadersForAdmin:

1. `TOURNAMENT_STATUS` ∈ `active_statuses` (АКТИВНЫЙ / ПОДВЕДЕНИЕ ИТОГОВ);
2. связанный `CONTEST_CODE` в CONTEST-DATA с `vid = ПРОМ` и `CONTEST_TYPE = ТУРНИРНЫЙ`.

Иначе — `-`. Список кодов для браузерного скрипта собирается той же логикой:

```bash
python src/Tools/build_tournament_leaders_auto_js.py
```

## Порядок обогащения TAB_NUMBERS (enrich pipeline)

Колонки на листе **TAB_NUMBERS** заполняются **строго в три этапа**. Это важно для полей **ТБ** / **ГОСБ** и последующего lookup в **ORG_UNIT_V20**.

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. CSV-enrich (enrich_columns без lookup_row_key)                       │
│    RATING, STATISTICS, EMPLOYEE, ORDER, … по табельному номеру        │
└───────────────────────────────┬─────────────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. JSON профилей (profile_gp_load.json_file / json_files)               │
│    Дозаполнение пустых / «-» из IN/JS/profiles_*.json                   │
│    Сопоставление: employeeNumber ↔ Табельный номер (20 знаков)         │
└───────────────────────────────┬─────────────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. Составной lookup (enrich_columns с lookup_row_key)                   │
│    TB_FULL_NAME, GOSB_NAME ← ORG_UNIT_V20 по ключу ТБ + ГОСБ строки    │
│    (коды ТБ/ГОСБ могли появиться только на шаге 2)                      │
└───────────────────────────────┬─────────────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 4. PROM-колонки (НАГРАДА / ТУРНИР / претендент из leadersForAdmin JSON) │
└─────────────────────────────────────────────────────────────────────────┘
```

Реализация: `enrich_tab_dataframe()` в `src/manager_stats.py`; JSON — `apply_profile_gp_json_enrich()` в `src/profile_gp_json.py`.

В логе прогона:

```
[manager_stats] profile GP JSON enrich: N ячеек, M табельных
[manager_stats] Profile AutoRun: K табельных с пустыми полями после CSV+JSON enrich
```

---

## Браузерные скрипты AutoRun (OUT/YYYY/DD-MM)

При `run_outputs` с **`manager_stats_only`** рядом с Excel в каталоге прогона создаются два JS-файла (вставить целиком в DevTools → Console на странице стенда).

| Файл | Назначение | Эталон |
|------|------------|--------|
| `Tournament_LeadersForAdmin_AutoRun.js` | GET leadersForAdmin по кодам турниров из PROM_TOURNAMENTS | `IN/JS/Tournament_LeadersForAdmin.js` |
| `Profile_GP_LOAD_AutoRun.js` | POST профиля по табельным, у которых после CSV+JSON остались дыры | `IN/JS/Profile_GP_LOAD_file.js` |

Оба скрипта запускаются **сразу** после вставки (без панели). Подробный журнал — в **Console** (запрос N/M, размеры ответов в bytes, итог).

CLI пересборки (без полного прогона `main.py`):

```bash
python src/Tools/build_tournament_leaders_auto_js.py
python src/Tools/build_profile_gp_auto_js.py
```

---

## Tournament_LeadersForAdmin AutoRun

### Отбор кодов турниров

Список `TOURNAMENT_CODES` в JS совпадает с колонкой **`запрос leadersForAdmin = ДА`** на листе PROM_TOURNAMENTS:

1. `TOURNAMENT_STATUS` ∈ `active_statuses` (АКТИВНЫЙ / ПОДВЕДЕНИЕ ИТОГОВ);
2. связанный `CONTEST_CODE` в CONTEST-DATA с `vid = ПРОМ` и `CONTEST_TYPE = ТУРНИРНЫЙ`.

### JSON претендентов на TAB_NUMBERS

Файл **`IN/JS/<leaders_for_admin_json_file>`** — ответ leadersForAdmin. Для турниров с `запрос leadersForAdmin = ДА`:

1. Обычные колонки **`ТУРНИР …`** (count LIST-REWARDS) **не создаются**.
2. В конце листа — колонки **`ТУРНИР (претендент) FULL_NAME (START_DT) [PRODUCT]`**.
3. Значение — сумма по табельному, если в JSON у `employeeNumber` в `divisionRatings` есть `ratingCategoryName` из списка претендента.

| Категория | Смысл |
|-----------|--------|
| Серебро | претендент на серебро |
| Бронза | претендент на золото |
| Вы в лидерах | претендент на золото |

Уровни группировки в JSON: `BANK`, `TB`, `GOSB`, `GROUPING` — учитываются все блоки `divisionRatings`.

| Ключ config | По умолчанию |
|-------------|--------------|
| `leaders_for_admin_js_enabled` | `true` |
| `leaders_for_admin_js_file` | `Tournament_LeadersForAdmin_AutoRun.js` |
| `leaders_for_admin_json_enabled` | `true` |
| `leaders_for_admin_json_file` | имя файла в `IN/JS/` |
| `leaders_for_admin_json_subdir` | `JS` |
| `leaders_for_admin_pretender_categories` | Серебро, Бронза, Вы в лидерах |
| `tab_columns_pretender_prefix` | `ТУРНИР (претендент)` |
| `tab_columns_total_pretender` | `ТУРНИР (претендент) всего` |

При вставке `Tournament_LeadersForAdmin_AutoRun.js` в Console: `Запрос N/M — GET leadersForAdmin`, HTTP-статус, размер ответа/записи в bytes, leaders/employeeNumber, итог с размером скачанного JSON.

---

## Profile GP: JSON-дозаполнение и AutoRun

Двухэтапная схема для полей, которые не находятся в CSV-листах на первом проходе enrich.

### Этап A — выгрузка профилей в браузере (один раз или по мере появления дыр)

1. Прогон `main.py` с `manager_stats_only` → в `OUT/YYYY/DD-MM/` появляются Excel и **`Profile_GP_LOAD_AutoRun.js`**.
2. Открыть стенд (omega / salesheroes), DevTools → Console, вставить весь JS, Enter.
3. Скрипт вызывает `runCollectProfiles` без панели; JSON скачивается батчами: `profiles_<STAND>_<CONTOUR>_part<N>_<timestamp>.json`.
4. Положить файлы в **`IN/JS/`**, указать имя в `profile_gp_load.json_file` или список в `json_files`.

### Этап B — подстановка из JSON при следующем прогоне

Модуль `src/profile_gp_json.py` читает массив записей из JSON (формат выгрузки Profile_GP_LOAD):

```json
{
  "tn": "00007713",
  "processed": {
    "success": true,
    "body": {
      "employeeNumber": "00007713",
      "lastName": "Радыгина",
      "firstName": "Светлана",
      "tbCode": "38",
      "gosbCode": "0"
    }
  }
}
```

| Колонка TAB_NUMBERS | Поле в `processed.body` | Примечание |
|---------------------|-------------------------|------------|
| Фамилия | `lastName` | |
| Имя | `firstName` | |
| ТБ | `tbCode` | После подстановки участвует в lookup ORG_UNIT |
| ГОСБ | `gosbCode` | После подстановки участвует в lookup ORG_UNIT |
| Код роли | `roleCode` | В текущих выгрузках SIGMA поле часто отсутствует → остаётся `-` |

Правила подстановки:

- Сопоставление по `employeeNumber` / `tn` с нормализацией до `normalize_pad_width` (20 знаков).
- Заполняется **только** если после CSV-enrich значение пусто или равно `enrich_default` (`-`).
- Если в JSON поля нет или запись с ошибкой (`error`, `success: false`) — значение не меняется.
- Несколько part-файлов (`json_files`): индекс строится по всем файлам; при дубликате табельного побеждает последний файл в списке.

### Этап C — формирование списка для Profile AutoRun JS

**Критично:** список табельных в `Profile_GP_LOAD_AutoRun.js` формируется **после** CSV-enrich **и** JSON-дозаполнения.

В `write_profile_gp_auto_js()` вызывается `prepare_tabs_for_profile_js()`:

1. Повторно применяет JSON к DataFrame (идемпотентно — только пустые ячейки).
2. Отбирает табельные, у которых **хотя бы одно** из пяти полей профиля **отсутствует**.

#### Повод включить табельный в AutoRun JS

Только если после CSV + JSON **отсутствует** (хотя бы одно):

| Поле |
|------|
| Фамилия |
| Имя |
| ТБ |
| ГОСБ |
| Код роли |

**Отсутствие** = значение **пустое**, **NULL/NaN** (в т.ч. пустая ячейка Excel) или строка **`"-"`**.

#### Не повод для AutoRun JS

Следующие поля **не учитываются** при формировании списка табельных, даже если они пустые или `-`:

| Поле | Источник |
|------|----------|
| Email Sigma | STATISTICS / ORDER |
| Email Alpha | STATISTICS / ORDER |
| Наименование Роли | STATISTICS |

Проверка реализована через **`js_missing_columns`** (только 5 полей профиля). Ключ `missing_columns` в config — справочный, **на отбор JS не влияет**.

| Список config | Назначение |
|---------------|------------|
| `js_missing_columns` | **Единственный критерий AutoRun JS** — Фамилия, Имя, ТБ, ГОСБ, Код роли |
| `missing_columns` | Справочный расширенный список; **не используется** для отбора в JS |

### Секция `profile_gp_load` в config.json

```json
"profile_gp_load": {
  "js_enabled": true,
  "js_file": "Profile_GP_LOAD_AutoRun.js",
  "js_template": "Profile_GP_LOAD_file.js",
  "js_template_subdir": "JS",
  "json_enabled": true,
  "json_subdir": "JS",
  "json_file": "profiles_PROM_SIGMA_part1_20260614_204856.json",
  "json_files": [],
  "json_field_map": {
    "Фамилия": "lastName",
    "Имя": "firstName",
    "ТБ": "tbCode",
    "ГОСБ": "gosbCode",
    "Код роли": "roleCode"
  },
  "js_missing_columns": [
    "Фамилия", "Имя", "ТБ", "ГОСБ", "Код роли"
  ],
  "missing_columns": [
    "Фамилия", "Имя", "ТБ", "ГОСБ", "Код роли",
    "Наименование Роли", "Email Sigma", "Email Alpha"
  ],
  "request_delay_ms": 2,
  "enable_retry": true,
  "max_retries": 1,
  "retry_delay_on_error_ms": 1500,
  "output_base_name": "profiles",
  "batch_size": 12000,
  "enable_photo_download": false,
  "enable_photo_strip": true
}
```

| Ключ | По умолчанию | Назначение |
|------|--------------|------------|
| `js_enabled` | `true` | Генерировать `Profile_GP_LOAD_AutoRun.js` |
| `js_file` | `Profile_GP_LOAD_AutoRun.js` | Имя файла в OUT |
| `js_template` | `Profile_GP_LOAD_file.js` | Эталон в `IN/JS/` |
| `json_enabled` | `true` | Читать JSON при enrich |
| `json_file` | — | Один файл в `IN/JS/` |
| `json_files` | `[]` | Несколько part-файлов (приоритет над `json_file`, если непустой) |
| `json_field_map` | lastName, firstName, … | Маппинг колонка → поле body |
| `js_missing_columns` | 5 полей профиля | Единственный критерий отбора ТН для AutoRun |
| `missing_columns` | справочно | **Не** влияет на AutoRun JS |
| `request_delay_ms` | `2` | Пауза между POST в JS |
| `batch_size` | `12000` | Размер батча при сохранении JSON в браузере |
| `enable_photo_strip` | `true` | Удалять base64 фото из JSON (меньше размер файла) |

### Console при работе Profile AutoRun

В DevTools выводится журнал (как «Журнал работы» эталона):

- `——— Старт сбора ———`, стенд/контур, URL, параметры
- `Запрос N/M — ТН …`
- `OK | size before: … bytes | size after: … bytes` (или ERROR с кодом)
- сохранение батчей с размером файла
- `==== ИТОГ ====` с суммарными размерами

---

## Колонки (претендент) на TAB_NUMBERS — сводка

(См. также раздел **Tournament_LeadersForAdmin AutoRun** выше.)

Для турниров с `запрос leadersForAdmin = ДА` обычные колонки `ТУРНИР …` не создаются; вместо них — `ТУРНИР (претендент) …` из JSON leadersForAdmin.

---

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
| `src/manager_stats.py` | Сбор табельных, enrich pipeline (CSV → JSON → ORG_UNIT), каталог PROM, динамические колонки TAB_NUMBERS |
| `src/profile_gp_json.py` | Парсинг `profiles_*.json`, `apply_profile_gp_json_enrich()` |
| `src/profile_gp_auto_js.py` | `Profile_GP_LOAD_AutoRun.js`: `prepare_tabs_for_profile_js()`, отбор по `js_missing_columns` |
| `src/leaders_for_admin_json.py` | Парсинг leadersForAdmin JSON, колонки «претендент» |
| `src/leaders_for_admin_auto_js.py` | `Tournament_LeadersForAdmin_AutoRun.js` |
| `src/main_impl.py` | `_write_manager_stats_excel`, запись обоих AutoRun JS |
| `src/Tools/build_profile_gp_auto_js.py` | CLI пересборки Profile AutoRun |
| `src/Tools/build_tournament_leaders_auto_js.py` | CLI пересборки leadersForAdmin AutoRun |
| `src/config_loader.py` | `parse_run_outputs_config` |
| `src/console_ui.py` | `print_manager_stats_summary`, фазы прогресса |
| `src/Tests/test_manager_stats.py` | Тесты enrich, profile JSON, profile JS, pretender |
| `src/Tests/test_run_outputs.py` | Тесты комбинаций `run_outputs` |
| `IN/JS/Profile_GP_LOAD_file.js` | Эталон панели загрузки профилей |
| `IN/JS/Tournament_LeadersForAdmin.js` | Эталон панели leadersForAdmin |

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
