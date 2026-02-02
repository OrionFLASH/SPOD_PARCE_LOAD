# Входные данные и конфигурация SPOD — полный справочник

Документ объединяет: (1) структуру и содержимое входных файлов, связи по кодам конкурса/турнира/награды, типы полей и варианты значений; (2) конфигурацию программы (INPUT_FILES, параметры листов, валидации, merge, логирование).

**Источники:** ANALYSIS_INPUT_DATA.md (анализ данных 2025-11-14), main.py, Docs/INPUT_STRUCTURE_AND_PARAMS.md.

---

# ЧАСТЬ I. ВХОДНЫЕ ФАЙЛЫ И ДАННЫЕ

## 1. Общие сведения

- **Каталог:** `SPOD` (относительно корня скрипта). CSV с разделителем `;`, кодировка UTF-8.
- **Проверка при старте:** все файлы из `INPUT_FILES` должны существовать в `SPOD` (по имени без расширения, расширения `.csv`/`.CSV`). При отсутствии любого файла программа выводит список отсутствующих и завершается (`sys.exit(1)`).

---

## 2. Список входных файлов (имя файла → лист)

| № | Имя файла (без расширения) | Лист в Excel |
|---|----------------------------|--------------|
| 1 | CONTEST (PROM) 30-01 v2 | CONTEST-DATA |
| 2 | GROUP (PROM) 30-01 v1 | GROUP |
| 3 | INDICATOR (PROM) 30-01 v1 | INDICATOR |
| 4 | REPORT (PROM) 30-01 v2 | REPORT |
| 5 | REWARD (PROM) 30-01 v2 | REWARD |
| 6 | REWARD-LINK (PROM) 30-01 v1 | REWARD-LINK |
| 7 | SCHEDULE (PROM) 30-01 v2 | TOURNAMENT-SCHEDULE |
| 8 | SVD_KB_DM_GAMIFICATION_ORG_UNIT_V20 - 2025.08.28 | ORG_UNIT_V20 |
| 9 | USER_ROLE (PROM) 12-12 v0 | USER_ROLE |
| 10 | USER_ROLE_SB (PROM) 12-12 v0 | USER_ROLE SB |
| 11 | employee_PROM_final_5000_2025-07-26_00-09-03 | EMPLOYEE |
| 12 | gamification-employeeRewards-3 | LIST-REWARDS |
| 13 | gamification-statistics-4 | STATISTICS |
| 14 | gamification-tournamentList-2 | LIST-TOURNAMENT |

---

## 3. Связи между файлами по кодам

**Модель:** один конкурс (CONTEST_CODE) связан с несколькими турнирами (TOURNAMENT_CODE) и несколькими наградами (REWARD_CODE). Связь конкурс–награда задаётся через REWARD-LINK (конкурс + группа + награда).

```
CONTEST-DATA (один конкурс — строка с CONTEST_CODE)
    ├── GROUP (CONTEST_CODE) — группы по конкурсу; ключ (CONTEST_CODE, GROUP_CODE, GROUP_VALUE)
    │   └── REWARD-LINK (CONTEST_CODE, GROUP_CODE, REWARD_CODE) — какая награда в какой группе конкурса
    ├── INDICATOR (CONTEST_CODE) — индикаторы конкурса; ключ (CONTEST_CODE, INDICATOR_ADD_CALC_TYPE)
    ├── TOURNAMENT-SCHEDULE (CONTEST_CODE) — турниры конкурса; ключ TOURNAMENT_CODE, есть CONTEST_CODE
    │   └── REPORT (TOURNAMENT_CODE, CONTEST_CODE) — отчёты по турниру/конкурсу
    └── REWARD-LINK (CONTEST_CODE, REWARD_CODE) — связь конкурса с наградами
        └── REWARD (REWARD_CODE) — справочник наград
```

**Зависимые списки (FK):**
- GROUP.CONTEST_CODE → CONTEST-DATA.CONTEST_CODE
- INDICATOR.CONTEST_CODE → CONTEST-DATA.CONTEST_CODE
- TOURNAMENT-SCHEDULE.CONTEST_CODE → CONTEST-DATA.CONTEST_CODE
- REPORT.CONTEST_CODE → CONTEST-DATA.CONTEST_CODE
- REPORT.TOURNAMENT_CODE → TOURNAMENT-SCHEDULE.TOURNAMENT_CODE (в контексте конкурса)
- REWARD-LINK.CONTEST_CODE → CONTEST-DATA.CONTEST_CODE
- REWARD-LINK.GROUP_CODE → GROUP.GROUP_CODE (в контексте CONTEST_CODE)
- REWARD-LINK.REWARD_CODE → REWARD.REWARD_CODE

---

## 4. Структура файлов: колонки и типы полей

### 4.1. CONTEST-DATA

| Колонка | Тип / роль | Варианты значений (если не текст) |
|---------|------------|-----------------------------------|
| CONTEST_CODE | ключ, уникальный | — |
| FULL_NAME, CONTEST_DESCRIPTION, PRODUCT_GROUP, PRODUCT, CONTEST_SUBJECT, BUSINESS_BLOCK, SOURCE_UPD_FREQUENCY, FACT_POST_PROCESSING, PLAN_MOD_METOD, PLAN_MOD_VALUE, FACTOR_MATCH, CONTEST_PERIOD | текст/число | — |
| CREATE_DT, CLOSE_DT | даты | — |
| BUSINESS_STATUS | перечисление | **АКТИВНЫЙ**, **АРХИВНЫЙ** |
| CONTEST_TYPE | перечисление | **ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ**, **ТУРНИРНЫЙ**, **ИНДИВИДУАЛЬНЫЙ** |
| TARGET_TYPE | перечисление | **ПРОМ**, **ТЕСТ** |
| CALC_TYPE | перечисление | **1**, **0** |
| CONTEST_FEATURE | JSON | разворачивается с префиксом CONTEST_FEATURE => |
| SHOW_INDICATOR, FACTOR_MARK_TYPE, CONTEST_INDICATOR_METHOD, CONTEST_FACTOR_METHOD, PLAN_METHOD_CODE | текст/число | — |

### 4.2. GROUP

| Колонка | Тип / роль | Варианты значений (если не текст) |
|---------|------------|-----------------------------------|
| CONTEST_CODE | FK → CONTEST-DATA | — |
| GROUP_CODE, GROUP_VALUE | часть ключа | GROUP_VALUE — JSON-массив или строка (напр. `"*"`) |
| GET_CALC_METHOD | перечисление | **1**, **2**, **3** |
| GET_CALC_CRITERION, ADD_CALC_CRITERION, ADD_CALC_CRITERION_2, BASE_CALC_CODE | текст | — |

### 4.3. INDICATOR

| Колонка | Тип / роль | Варианты значений (если не текст) |
|---------|------------|-----------------------------------|
| CONTEST_CODE, INDICATOR_ADD_CALC_TYPE | ключ | — |
| INDICATOR_CALC_TYPE, FULL_NAME, INDICATOR_CODE, INDICATOR_AGG_FUNCTION, INDICATOR_WEIGHT, INDICATOR_OBJECT, INDICATOR_MARK_TYPE, INDICATOR_MATCH, INDICATOR_VALUE, CONTEST_CRITERION, CONTESTANT_SELECTION, CALC_TYPE, N | текст/число | — |
| INDICATOR_FILTER | JSON | массив объектов (filtered_attribute_code, filtered_attribute_type, filtered_attribute_value) |

### 4.4. REWARD

| Колонка | Тип / роль | Варианты значений (если не текст) |
|---------|------------|-----------------------------------|
| REWARD_CODE | ключ, уникальный | — |
| REWARD_TYPE | перечисление | **ITEM**, **BADGE**, **LABEL**, **CRYSTAL** |
| FULL_NAME, REWARD_DESCRIPTION, REWARD_CONDITION, REWARD_COST | текст | — |
| REWARD_ADD_DATA | JSON | разворачивается с префиксом ADD_DATA => |

### 4.5. TOURNAMENT-SCHEDULE

| Колонка | Тип / роль | Варианты значений (если не текст) |
|---------|------------|-----------------------------------|
| TOURNAMENT_CODE | ключ | — |
| CONTEST_CODE | FK → CONTEST-DATA | — |
| TOURNAMENT_STATUS | перечисление | **УДАЛЕН**, **ЗАВЕРШЕН**, **АКТИВНЫЙ**, **ОТМЕНЕН**, **ПОДВЕДЕНИЕ ИТОГОВ** |
| CALC_TYPE | перечисление | **1**, **3**, **0** |
| START_DT, END_DT, RESULT_DT, PLAN_PERIOD_START_DT, PLAN_PERIOD_END_DT | даты | — |
| PERIOD_TYPE, CRITERION_MARK_TYPE, CRITERION_MARK_VALUE, FILTER_PERIOD_ARR, TARGET_TYPE, TRN_INDICATOR_FILTER | текст/JSON | — |

### 4.6. REPORT

| Колонка | Тип / роль | Варианты значений (если не текст) |
|---------|------------|-----------------------------------|
| MANAGER_PERSON_NUMBER | FK → EMPLOYEE (по коду) | длина 20 (проверка в коде) |
| CONTEST_CODE | FK → CONTEST-DATA | — |
| TOURNAMENT_CODE | FK → TOURNAMENT-SCHEDULE | — |
| CONTEST_DATE | дата | — |
| PLAN_VALUE, FACT_VALUE, priority_type | число/текст | — |

### 4.7. REWARD-LINK

| Колонка | Тип / роль |
|---------|------------|
| CONTEST_CODE | FK → CONTEST-DATA |
| GROUP_CODE | FK → GROUP (в контексте конкурса) |
| REWARD_CODE | FK → REWARD |

### 4.8. Остальные листы (по коду, без полного анализа данных)

- **ORG_UNIT_V20:** ключи TB_CODE, GOSB_CODE, ORG_UNIT_CODE; поля TB_FULL_NAME (≤100 симв.), GOSB_NAME (≤100), GOSB_SHORT_NAME (≤20). Связь с EMPLOYEE по (TB_CODE, GOSB_CODE) и по ORG_UNIT_CODE.
- **EMPLOYEE:** PERSON_NUMBER, PERSON_NUMBER_ADD (длина 20); связь с REPORT по MANAGER_PERSON_NUMBER; подтягиваются ORG_UNIT_CODE, GOSB_SHORT_NAME и др. из ORG_UNIT_V20.
- **LIST-TOURNAMENT:** колонки «Код турнира» (или TOURNAMENT_CODE), «Бизнес-статус турнира» (или «Бизнес-статус»). Справочник турниров по коду.
- **LIST-REWARDS:** колонка «Код награды». Справочник наград по коду.
- **STATISTICS, USER_ROLE, USER_ROLE SB:** в программе используются как входные листы; детальная структура колонок в этом справочнике не разбиралась.

---

## 5. Фиксированные списки значений (перечисления)

| Поле | Лист | Допустимые значения |
|------|------|---------------------|
| BUSINESS_STATUS | CONTEST-DATA | АКТИВНЫЙ, АРХИВНЫЙ |
| CONTEST_TYPE | CONTEST-DATA | ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ, ТУРНИРНЫЙ, ИНДИВИДУАЛЬНЫЙ |
| TARGET_TYPE | CONTEST-DATA | ПРОМ, ТЕСТ |
| CALC_TYPE | CONTEST-DATA | 1, 0 |
| GET_CALC_METHOD | GROUP | 1, 2, 3 |
| REWARD_TYPE | REWARD | ITEM, BADGE, LABEL, CRYSTAL |
| TOURNAMENT_STATUS | TOURNAMENT-SCHEDULE | УДАЛЕН, ЗАВЕРШЕН, АКТИВНЫЙ, ОТМЕНЕН, ПОДВЕДЕНИЕ ИТОГОВ |
| CALC_TYPE | TOURNAMENT-SCHEDULE | 1, 3, 0 |

**В коде (расчёт/merge):** дополнительно используются статусы ЗАПЛАНИРОВАН, НЕОПРЕДЕЛЕН; фильтр по BUSINESS_STATUS в merge: АКТИВНЫЙ, ПОДВЕДЕНИЕ ИТОГОВ.

---

## 6. JSON-поля

| Лист | Колонка | Описание |
|------|---------|----------|
| CONTEST-DATA | CONTEST_FEATURE | JSON-объект; разворачивается с префиксом CONTEST_FEATURE => |
| CONTEST-DATA | CONTEST_PERIOD | JSON-массив (может быть пустым или содержать объекты) |
| CONTEST-DATA | BUSINESS_BLOCK | JSON-массив строк |
| REWARD | REWARD_ADD_DATA | JSON-объект; разворачивается с префиксом ADD_DATA => |
| TOURNAMENT-SCHEDULE | TARGET_TYPE | может быть JSON-подобной строкой (напр. seasonCode) |
| TOURNAMENT-SCHEDULE | FILTER_PERIOD_ARR | JSON-массив объектов (period_code, criterion_mark_type, criterion_mark_value, start_dt, end_dt) |
| INDICATOR | INDICATOR_FILTER | JSON-массив объектов |
| GROUP | GROUP_VALUE | JSON-массив чисел или строка (напр. `[38]`, `"*"`) |

В программе разворачиваются только: CONTEST-DATA.CONTEST_FEATURE, REWARD.REWARD_ADD_DATA (см. JSON_COLUMNS в части II).

---

## 7. Уникальность и ключи

- **CONTEST-DATA:** CONTEST_CODE — уникальный.
- **REWARD:** REWARD_CODE — уникальный.
- **GROUP:** (CONTEST_CODE, GROUP_CODE, GROUP_VALUE) — уникальная комбинация.
- **INDICATOR:** (CONTEST_CODE, INDICATOR_ADD_CALC_TYPE) — уникальная комбинация.
- **REPORT:** (MANAGER_PERSON_NUMBER, TOURNAMENT_CODE, CONTEST_CODE) — уникальная комбинация.

---

# ЧАСТЬ II. КОНФИГУРАЦИЯ ПРОГРАММЫ (main.py)

## 8. Параметры конфигурации входного файла (INPUT_FILES[*])

Для каждого элемента списка INPUT_FILES задаются:

| Параметр | Тип | Назначение | Варианты |
|----------|-----|------------|----------|
| file | str | Имя файла без расширения | ищется в SPOD с расширением .csv/.CSV |
| sheet | str | Имя листа в итоговом Excel | уникальное имя листа |
| max_col_width | int | Макс. ширина колонки (символов) | 20–200 по листам |
| freeze | str | Закрепление области в Excel | строка вида "КолонкаСтрока", напр. "C2", "G2" |
| col_width_mode | str \| int | Режим ширины колонок | "AUTO" или число (фиксированная ширина) |
| min_col_width | int | Минимальная ширина колонки | 8–15 по листам |

Используются при записи в Excel: _format_sheet, calculate_column_width читают эти поля из params листа.

---

## 9. Сводный лист (SUMMARY_SHEET)

- **sheet:** SUMMARY (создаётся программно, не из CSV).
- **max_col_width:** 150, **freeze:** "G2", **col_width_mode:** "AUTO", **min_col_width:** 8.

---

## 10. Порядок обработки и зависимости по коду

1. Загрузка всех CSV из INPUT_FILES (параллельно).
2. EMPLOYEE: добавление колонки AUTO_GENDER (по GENDER_PATTERNS).
3. Валидация длины полей (FIELD_LENGTH_VALIDATIONS) для ORG_UNIT_V20, EMPLOYEE, REPORT.
4. TOURNAMENT-SCHEDULE: расчёт CALC_TOURNAMENT_STATUS с использованием REPORT (CONTEST_DATE, TOURNAMENT_CODE).
5. MERGE_FIELDS_ADVANCED: добавление полей между листами (источник → приёмник по ключам).
6. Проверка дубликатов по листам.
7. Формирование SUMMARY по SUMMARY_KEY_DEFS и правилам merge для sheet_dst == "SUMMARY".

**SUMMARY_KEY_DEFS (ключи для сводного листа):**
- CONTEST-DATA: CONTEST_CODE
- TOURNAMENT-SCHEDULE: TOURNAMENT_CODE, CONTEST_CODE
- REWARD-LINK: REWARD_CODE, CONTEST_CODE
- GROUP: GROUP_CODE, CONTEST_CODE, GROUP_VALUE
- REWARD: REWARD_CODE
- INDICATOR: INDICATOR_ADD_CALC_TYPE, CONTEST_CODE

---

## 11. JSON-поля, разворачиваемые программой (JSON_COLUMNS)

| Лист | Колонка в CSV | Префикс после разворота |
|------|----------------|--------------------------|
| CONTEST-DATA | CONTEST_FEATURE | CONTEST_FEATURE |
| REWARD | REWARD_ADD_DATA | ADD_DATA |

Остальные JSON-колонки в других листах в программе не разворачиваются.

---

## 12. Валидация длины полей (FIELD_LENGTH_VALIDATIONS)

- **result_column:** имя колонки с результатом проверки (FIELD_LENGTH_CHECK).
- **fields:** словарь {имя_поля: {"limit": int, "operator": str}}.

**operator:** "<=", "=", ">=", "<", ">".

| Лист | Поле | Условие |
|------|------|---------|
| ORG_UNIT_V20 | TB_FULL_NAME | длина ≤ 100 |
| ORG_UNIT_V20 | GOSB_NAME | длина ≤ 100 |
| ORG_UNIT_V20 | GOSB_SHORT_NAME | длина ≤ 20 |
| EMPLOYEE | PERSON_NUMBER | длина = 20 |
| EMPLOYEE | PERSON_NUMBER_ADD | длина = 20 |
| REPORT | MANAGER_PERSON_NUMBER | длина = 20 |

---

## 13. Правила слияния (MERGE_FIELDS_ADVANCED) — параметры правила

- **sheet_src, sheet_dst** — имена листов.
- **src_key, dst_key** — списки колонок-ключей.
- **column** — список полей для переноса или для агрегата.
- **mode** — "value" (перенос значений) или "count" (агрегат по ключу).
- **multiply_rows** — bool.
- **col_max_width, col_width_mode, col_min_width** — для добавляемых колонок.
- **status_filters** — dict или None: {"колонка": ["значение1", "значение2"]}.
- **custom_conditions, group_by, aggregate** — доп. условия и агрегация.
- Для mode == "count": **count_aggregation** ("size" или "nunique"), **count_label** (строка или None).

---

## 14. Логирование и пути

- **LOG_LEVEL:** "DEBUG" или "INFO".
- **LOG_BASE_NAME:** базовое имя лог-файла (напр. "LOGS").
- **DIR_LOGS:** каталог логов. **DIR_OUTPUT:** каталог для выходного Excel (OUT).
- Имя файла лога: `LOGS/LOGS_<LEVEL>_YYYYMMDD_HH_MM.log`.

---

## 15. Прочие параметры

- **GENDER_PATTERNS:** словарь списков окончаний для определения пола (patronymic_male/female, name_male/female, surname_male/female).
- **MAX_WORKERS_IO, MAX_WORKERS_CPU, MAX_WORKERS:** число потоков (от os.cpu_count() с ограничениями).
- **COLOR_SCHEME:** цветовое оформление листов (group, header_bg, header_fg, sheets, columns, style_scope).

---

**Версия документа:** 1.0  
**Дата:** объединение ANALYSIS_INPUT_DATA.md, INPUT_STRUCTURE_AND_PARAMS.md и сведений из main.py.
