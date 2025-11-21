# АНАЛИЗ ВХОДНЫХ ДАННЫХ SPOD
## Без добавленных колонок (только исходные поля)

**Дата анализа:** 2025-11-14

---

## 1. СТРУКТУРА ФАЙЛОВ

### 1.1. CONTEST-DATA
**Колонки (25):**
- CONTEST_CODE (ключ, уникальный)
- FULL_NAME
- CREATE_DT
- CLOSE_DT
- BUSINESS_STATUS (список: АКТИВНЫЙ, АРХИВНЫЙ)
- CONTEST_TYPE (список: ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ, ТУРНИРНЫЙ, ИНДИВИДУАЛЬНЫЙ)
- CONTEST_DESCRIPTION
- **CONTEST_FEATURE (JSON)**
- SHOW_INDICATOR
- PRODUCT_GROUP
- PRODUCT
- CONTEST_SUBJECT
- FACTOR_MARK_TYPE
- CONTEST_INDICATOR_METHOD
- CONTEST_FACTOR_METHOD
- PLAN_METHOD_CODE
- PLAN_MOD_METOD
- PLAN_MOD_VALUE
- FACTOR_MATCH
- CONTEST_PERIOD
- TARGET_TYPE (список: ПРОМ, ТЕСТ)
- SOURCE_UPD_FREQUENCY
- CALC_TYPE (список: 1, 0)
- BUSINESS_BLOCK
- FACT_POST_PROCESSING

### 1.2. GROUP
**Колонки (8):**
- CONTEST_CODE (FK → CONTEST-DATA)
- GROUP_CODE
- GROUP_VALUE
- GET_CALC_METHOD (список: 1, 2, 3)
- GET_CALC_CRITERION
- ADD_CALC_CRITERION
- ADD_CALC_CRITERION_2
- BASE_CALC_CODE

### 1.3. INDICATOR
**Колонки (16):**
- CONTEST_CODE (FK → CONTEST-DATA)
- INDICATOR_CALC_TYPE
- INDICATOR_ADD_CALC_TYPE
- FULL_NAME
- INDICATOR_CODE
- INDICATOR_AGG_FUNCTION
- INDICATOR_WEIGHT
- INDICATOR_OBJECT
- INDICATOR_MARK_TYPE
- INDICATOR_MATCH
- INDICATOR_VALUE
- CONTEST_CRITERION
- INDICATOR_FILTER
- CONTESTANT_SELECTION
- CALC_TYPE
- N

### 1.4. REWARD
**Колонки (7):**
- REWARD_CODE (ключ, уникальный)
- REWARD_TYPE (список: ITEM, BADGE, LABEL, CRYSTAL)
- FULL_NAME
- REWARD_DESCRIPTION
- REWARD_CONDITION
- REWARD_COST
- **REWARD_ADD_DATA (JSON)**

### 1.5. TOURNAMENT-SCHEDULE
**Колонки (15):**
- TOURNAMENT_CODE (ключ)
- PERIOD_TYPE
- START_DT
- END_DT
- RESULT_DT
- PLAN_PERIOD_START_DT
- PLAN_PERIOD_END_DT
- CRITERION_MARK_TYPE
- CRITERION_MARK_VALUE
- FILTER_PERIOD_ARR
- TOURNAMENT_STATUS (список: УДАЛЕН, ЗАВЕРШЕН, АКТИВНЫЙ, ОТМЕНЕН, ПОДВЕДЕНИЕ ИТОГОВ)
- CONTEST_CODE (FK → CONTEST-DATA)
- TARGET_TYPE (может быть JSON-подобной строкой)
- CALC_TYPE (список: 1, 3, 0)
- TRN_INDICATOR_FILTER

### 1.6. REPORT
**Колонки (7):**
- MANAGER_PERSON_NUMBER
- CONTEST_CODE (FK → CONTEST-DATA)
- TOURNAMENT_CODE (FK → TOURNAMENT-SCHEDULE)
- CONTEST_DATE
- PLAN_VALUE
- FACT_VALUE
- priority_type

### 1.7. REWARD-LINK
**Колонки (3):**
- CONTEST_CODE (FK → CONTEST-DATA)
- GROUP_CODE (FK → GROUP)
- REWARD_CODE (FK → REWARD)

---

## 2. СВЯЗИ МЕЖДУ ФАЙЛАМИ

```
CONTEST-DATA (основной)
    ├── GROUP (CONTEST_CODE)
    │   └── REWARD-LINK (CONTEST_CODE, GROUP_CODE)
    ├── INDICATOR (CONTEST_CODE)
    ├── TOURNAMENT-SCHEDULE (CONTEST_CODE)
    │   └── REPORT (TOURNAMENT_CODE, CONTEST_CODE)
    └── REWARD-LINK (CONTEST_CODE)
        └── REWARD (REWARD_CODE)
```

---

## 3. JSON ПОЛЯ

### 3.1. CONTEST-DATA
- **CONTEST_FEATURE** - JSON объект с произвольными ключами
- **CONTEST_PERIOD** - JSON массив (может быть пустым или содержать объекты)
- **BUSINESS_BLOCK** - JSON массив строк

### 3.2. TOURNAMENT-SCHEDULE
- **TARGET_TYPE** - JSON объект с ключом `seasonCode`
- **FILTER_PERIOD_ARR** - JSON массив объектов с полями: period_code, criterion_mark_type, criterion_mark_value, start_dt, end_dt

### 3.3. INDICATOR
- **INDICATOR_FILTER** - JSON массив объектов с полями: filtered_attribute_code, filtered_attribute_type, filtered_attribute_value

### 3.4. GROUP
- **GROUP_VALUE** - JSON массив чисел или строка (может быть `[38]` или `"*"`)

### 3.5. REWARD
- **REWARD_ADD_DATA** - JSON объект с произвольными ключами

---

## 4. СПИСКИ ЗНАЧЕНИЙ

### 4.1. Фиксированные списки
- BUSINESS_STATUS: АКТИВНЫЙ, АРХИВНЫЙ
- CONTEST_TYPE: ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ, ТУРНИРНЫЙ, ИНДИВИДУАЛЬНЫЙ
- TARGET_TYPE (CONTEST-DATA): ПРОМ, ТЕСТ
- CALC_TYPE (CONTEST-DATA): 1, 0
- REWARD_TYPE: ITEM, BADGE, LABEL, CRYSTAL
- TOURNAMENT_STATUS: УДАЛЕН, ЗАВЕРШЕН, АКТИВНЫЙ, ОТМЕНЕН, ПОДВЕДЕНИЕ ИТОГОВ
- CALC_TYPE (TOURNAMENT-SCHEDULE): 1, 3, 0
- GET_CALC_METHOD: 1, 2, 3

### 4.2. Зависимые списки
- GROUP.CONTEST_CODE → CONTEST-DATA.CONTEST_CODE
- INDICATOR.CONTEST_CODE → CONTEST-DATA.CONTEST_CODE
- TOURNAMENT-SCHEDULE.CONTEST_CODE → CONTEST-DATA.CONTEST_CODE
- REPORT.CONTEST_CODE → CONTEST-DATA.CONTEST_CODE
- REPORT.TOURNAMENT_CODE → TOURNAMENT-SCHEDULE.TOURNAMENT_CODE (фильтр по CONTEST_CODE)
- REWARD-LINK.CONTEST_CODE → CONTEST-DATA.CONTEST_CODE
- REWARD-LINK.GROUP_CODE → GROUP.GROUP_CODE (фильтр по CONTEST_CODE)
- REWARD-LINK.REWARD_CODE → REWARD.REWARD_CODE

---

## 5. ПРОВЕРКИ УНИКАЛЬНОСТИ

- CONTEST-DATA.CONTEST_CODE - уникальный
- REWARD.REWARD_CODE - уникальный
- GROUP: CONTEST_CODE + GROUP_CODE + GROUP_VALUE - уникальная комбинация
- INDICATOR: CONTEST_CODE + INDICATOR_ADD_CALC_TYPE - уникальная комбинация
- REPORT: MANAGER_PERSON_NUMBER + TOURNAMENT_CODE + CONTEST_CODE - уникальная комбинация

---

## 6. РЕКОМЕНДАЦИИ ДЛЯ АДМИНКИ

1. **Интерфейс с вкладками** - каждая вкладка для одного файла
2. **Выпадающие списки** - для полей с фиксированными значениями
3. **Зависимые списки** - динамическая загрузка с фильтрацией
4. **JSON редактор** - отдельная форма для редактирования JSON полей
5. **Валидация** - проверка уникальности и существования внешних ключей
6. **Каскадные операции** - при удалении/изменении ключевых полей
7. **Синхронизация** - автоматическое обновление зависимых записей

---

**Версия:** 1.0  
**Дата:** 2025-11-14
