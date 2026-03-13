# Формат работы с проверками на консистентность

Документ описывает полный формат конфигурации и вывода проверок на консистентность данных.

**Актуально с версии 1.4:** колонки **unique** («ДУБЛЬ: …») и **field_length** (FIELD_LENGTH_CHECK и т.д.) **создаёт сам модуль consistency_checks** по правилам из `consistency_checks.rules`. Секции **check_duplicates** и **field_length_validations** в config.json **удалены** — все правила задаются только в `consistency_checks.rules` (типы `unique` и `field_length` с полем `fields` для ограничений по длине).

---

## 1. Разбор формулировок из ПРОВЕРКИ.txt (пункты 1.1–5)

Соответствие «таблиц» из текста и листов проекта:

| В тексте | В проекте | Пояснение |
|----------|-----------|-----------|
| таблица **contest** | лист **CONTEST-DATA** | справочник конкурсов |
| **contest_group** | лист **GROUP** | группы по конкурсу |
| **contest_indicator** | лист **INDICATOR** | индикаторы по конкурсу |
| **contest_reward_link** | лист **REWARD-LINK** | связи наград с конкурсами/группами |
| **contest_reward** | лист **REWARD** | справочник наград |

Перевод проверок в привязку к листам и колонкам:

- **1.1** — каждое значение колонки **CONTEST_CODE** на листе **GROUP** должно присутствовать в **CONTEST-DATA** (колонка CONTEST_CODE).
- **1.2** — каждое значение **CONTEST_CODE** на листе **INDICATOR** — в **CONTEST-DATA.CONTEST_CODE**.
- **1.3** — каждое значение **CONTEST_CODE** на листе **REWARD-LINK** — в **CONTEST-DATA.CONTEST_CODE**.
- **2** — каждое значение **REWARD_CODE** на листе **REWARD-LINK** должно присутствовать в **REWARD.REWARD_CODE**.
- **3** — комбинация **(CONTEST_CODE, GROUP_CODE, GROUP_VALUE)** на листе **GROUP** должна быть **уникальной** (нет дублей).
- **4** — комбинация **(CONTEST_CODE, GROUP_CODE, REWARD_CODE)** на листе **REWARD-LINK** должна быть **уникальной** (в тексте опечатка «contest_group» — по смыслу речь о REWARD-LINK).
- **5** — каждая пара **(CONTEST_CODE, GROUP_CODE)** из **REWARD-LINK** должна существовать в **GROUP** (та же пара CONTEST_CODE + GROUP_CODE).

---

## 2. Общий формат: конфиг проверок и вывод результата

Вся настройка выносится в конфиг; исходный (source) Excel не изменяется.

### 2.1. Секция конфига `consistency_checks`

Один элемент массива правил — одна проверка. Общие поля:

- **id** — короткий идентификатор (для логов и сводки).
- **name** — человекочитаемое название (по желанию).
- **type** — тип: `"referential"` | `"unique"` | `"referential_composite"` | `"field_length"` | `"field_format"`.
- **enabled** — выполнять ли проверку (true/false).
- **output** — куда и как выводить результат (см. ниже).

Остальные параметры зависят от типа.

---

### 2.2. Тип `referential` (внешний ключ в одну колонку)

Значения колонки на листе A должны присутствовать в справочнике (лист B, колонка).

```json
{
  "id": "group_contest_code",
  "name": "CONTEST_CODE из GROUP есть в CONTEST-DATA",
  "type": "referential",
  "enabled": true,
  "sheet_src": "GROUP",
  "column_src": "CONTEST_CODE",
  "sheet_ref": "CONTEST-DATA",
  "column_ref": "CONTEST_CODE",
  "output": {
    "column_on_sheet": "ПРОВЕРКА: CONTEST_CODE в CONTEST-DATA",
    "include_in_summary": true
  }
}
```

- **sheet_src** — лист, где проверяем (GROUP, REWARD-LINK и т.д.).
- **column_src** — проверяемая колонка.
- **sheet_ref** — лист-справочник (CONTEST-DATA, REWARD и т.д.).
- **column_ref** — колонка справочника.
- **output.column_on_sheet** — имя колонки на **sheet_src**, куда пишем результат по строкам (например «OK» / «НЕТ в CONTEST-DATA»).
- **output.include_in_summary** — включать ли эту проверку в сводный лист.

---

### 2.3. Тип `referential_composite` (внешний ключ из нескольких колонок)

Например проверка 5: пара (CONTEST_CODE, GROUP_CODE) из REWARD-LINK должна существовать в GROUP.

```json
{
  "id": "reward_link_group_match",
  "name": "Пара CONTEST_CODE+GROUP_CODE из REWARD-LINK есть в GROUP",
  "type": "referential_composite",
  "enabled": true,
  "sheet_src": "REWARD-LINK",
  "columns_src": ["CONTEST_CODE", "GROUP_CODE"],
  "sheet_ref": "GROUP",
  "columns_ref": ["CONTEST_CODE", "GROUP_CODE"],
  "output": {
    "column_on_sheet": "ПРОВЕРКА: пара в GROUP",
    "include_in_summary": true
  }
}
```

- **columns_src** / **columns_ref** — списки колонок в одном порядке (конкатенация или построчное сравнение по позициям).

---

### 2.4. Тип `unique` (уникальность комбинации колонок)

Комбинация полей на листе должна быть уникальной. Модуль **создаёт** колонку на листе по полям правила (`sheet`, `key_columns`, `output.column_on_sheet`); в ячейках — пусто или «xN» (N — число строк с данным ключом).

**Правила в едином формате задаются в `consistency_checks.rules` как тип `unique` (секция check_duplicates в config удалена). Примеры соответствия лист/ключ → имя колонки:**

| № | Лист | Ключ (key_columns) | Имя колонки вывода (текущее) |
|---|------|---------------------|------------------------------|
| 1 | CONTEST-DATA | CONTEST_CODE | ДУБЛЬ: CONTEST_CODE |
| 2 | GROUP | CONTEST_CODE, GROUP_CODE, GROUP_VALUE | ДУБЛЬ: CONTEST_CODE_GROUP_CODE_GROUP_VALUE |
| 3 | INDICATOR | CONTEST_CODE, INDICATOR_ADD_CALC_TYPE, INDICATOR_CODE | ДУБЛЬ: CONTEST_CODE_INDICATOR_ADD_CALC_TYPE_INDICATOR_CODE |
| 4 | INDICATOR | N | ДУБЛЬ: N |
| 5 | REPORT | MANAGER_PERSON_NUMBER, TOURNAMENT_CODE, CONTEST_CODE | ДУБЛЬ: MANAGER_PERSON_NUMBER_TOURNAMENT_CODE_CONTEST_CODE |
| 6 | REWARD | REWARD_CODE | ДУБЛЬ: REWARD_CODE |
| 7 | REWARD-LINK | CONTEST_CODE, REWARD_CODE | ДУБЛЬ: CONTEST_CODE_REWARD_CODE |
| 8 | REWARD-LINK | REWARD_CODE | ДУБЛЬ: REWARD_CODE |
| 9 | TOURNAMENT-SCHEDULE | TOURNAMENT_CODE, CONTEST_CODE | ДУБЛЬ: TOURNAMENT_CODE_CONTEST_CODE |
| 10 | TOURNAMENT-SCHEDULE | TOURNAMENT_CODE | ДУБЛЬ: TOURNAMENT_CODE |
| 11 | ORG_UNIT_V20 | ORG_UNIT_CODE | ДУБЛЬ: ORG_UNIT_CODE |
| 12 | USER_ROLE | RULE_NUM | ДУБЛЬ: RULE_NUM |
| 13 | USER_ROLE SB | RULE_NUM | ДУБЛЬ: RULE_NUM |
| 14 | EMPLOYEE | PERSON_NUMBER | ДУБЛЬ: PERSON_NUMBER |
| 15 | EMPLOYEE | PERSON_NUMBER_ADD | ДУБЛЬ: PERSON_NUMBER_ADD |

Пример правила в едином формате `consistency_checks`:

```json
{
  "id": "group_unique",
  "name": "Уникальность CONTEST_CODE+GROUP_CODE+GROUP_VALUE в GROUP",
  "type": "unique",
  "enabled": true,
  "sheet": "GROUP",
  "key_columns": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"],
  "output": {
    "column_on_sheet": "ДУБЛЬ: CONTEST_CODE_GROUP_CODE_GROUP_VALUE",
    "include_in_summary": true
  }
}
```

- **sheet** — лист, на котором проверяем.
- **key_columns** — колонки, образующие ключ уникальности.
- В **column_on_sheet** по строкам: пусто при уникальности, при дубле — «xN» (N — количество строк с этим ключом). Имя колонки может совпадать с текущим форматом «ДУБЛЬ: key1_key2_...» для совместимости.

---

### 2.5. Тип `field_length` (проверка длины полей)

Проверка того, что длина значений в указанных полях удовлетворяет заданным ограничениям (оператор + лимит). Задаётся **в правилах consistency_checks** (секция field_length_validations в config **удалена**). В каждом правиле с `type: "field_length"` указываются **result_column** и **fields** (поле → limit + operator). Модуль **создаёт** колонку результата на листе в фазе 1.

Формат результата в ячейке: «-» если всё ок; иначе строка вида «поле1 = длина оператор лимит; поле2 = ...».

Пример правила на лист в `consistency_checks.rules`:

```json
{
  "id": "org_unit_v20_length",
  "name": "Длина полей в ORG_UNIT_V20",
  "type": "field_length",
  "enabled": true,
  "sheet": "ORG_UNIT_V20",
  "result_column": "FIELD_LENGTH_CHECK",
  "fields": {
    "TB_FULL_NAME": { "limit": 100, "operator": "<=" },
    "GOSB_NAME": { "limit": 100, "operator": "<=" },
    "GOSB_SHORT_NAME": { "limit": 20, "operator": "<=" }
  },
  "output": {
    "column_on_sheet": "FIELD_LENGTH_CHECK",
    "include_in_summary": true
  }
}
```

Поддерживаемые операторы: `"<="`, `"="`, `">="`, `"<"`, `">"`. Пустые/пропущенные значения считаются проходящими проверку.

---

### 2.6. Тип `field_format` (проверка формата поля)

Проверка формата значения в одном поле: дата, десятичное число с фиксированной дробной частью или строка из N цифр (с лидирующими нулями). Задаётся в правиле: **sheet**, **field**, **format** (объект с полем **type** и параметрами). Модуль создаёт колонку результата на листе в фазе 1; в ячейках: «OK» или текст ошибки.

**format.type** и параметры:

| type | Параметры | Описание |
|------|-----------|----------|
| **date** | date_format ("YYYY-MM-DD" → разбор %Y-%m-%d), allow_empty (bool), special_values (массив строк — допустимые значения, напр. ["4000-01-01"]) | Дата в заданном формате; пустое допустимо при allow_empty: true |
| **decimal** | decimal_places (число знаков после точки, напр. 5), allow_empty | Число вида целая_часть.дробная_часть (дробная фиксированной длины); допускаются и строка "1.50000", и число 1.5 |
| **fixed_length_digits** | length (число цифр, напр. 20), allow_empty | Строка из ровно N цифр (лидирующие нули допустимы); короткое число дополняется нулями слева при проверке |

Примеры правил:

```json
{
  "id": "format_report_contest_date",
  "name": "Формат поля CONTEST_DATE в REPORT: YYYY-MM-DD",
  "type": "field_format",
  "enabled": true,
  "sheet": "REPORT",
  "field": "CONTEST_DATE",
  "format": { "type": "date", "date_format": "YYYY-MM-DD", "allow_empty": false },
  "output": { "column_on_sheet": "ПРОВЕРКА ФОРМАТ: CONTEST_DATE", "include_in_summary": true }
},
{
  "id": "format_contest_data_close_dt",
  "name": "Формат CLOSE_DT: YYYY-MM-DD (учесть 4000-01-01)",
  "type": "field_format",
  "enabled": true,
  "sheet": "CONTEST-DATA",
  "field": "CLOSE_DT",
  "format": { "type": "date", "date_format": "YYYY-MM-DD", "allow_empty": false, "special_values": ["4000-01-01"] },
  "output": { "column_on_sheet": "ПРОВЕРКА ФОРМАТ: CLOSE_DT", "include_in_summary": true }
},
{
  "id": "format_report_plan_value",
  "name": "Формат PLAN_VALUE: 0.00000",
  "type": "field_format",
  "enabled": true,
  "sheet": "REPORT",
  "field": "PLAN_VALUE",
  "format": { "type": "decimal", "decimal_places": 5, "allow_empty": false },
  "output": { "column_on_sheet": "ПРОВЕРКА ФОРМАТ: PLAN_VALUE", "include_in_summary": true }
},
{
  "id": "format_employee_person_number",
  "name": "Формат PERSON_NUMBER: 20 цифр с лидирующими нулями",
  "type": "field_format",
  "enabled": true,
  "sheet": "EMPLOYEE",
  "field": "PERSON_NUMBER",
  "format": { "type": "fixed_length_digits", "length": 20, "allow_empty": false },
  "output": { "column_on_sheet": "ПРОВЕРКА ФОРМАТ: PERSON_NUMBER", "include_in_summary": true }
}
```

В сводке и в ячейках для **field_format**: «OK» или описание нарушения (например «Не дата формата %Y-%m-%d», «Ожидается 20 цифр, получено 5»).

---

## 3. Куда выводить информацию

### 3.1. На загружаемых листах (основной Excel)

- В **output** у каждой проверки задаётся **column_on_sheet** (имя колонки).
- Для листа, к которому привязана проверка (**sheet_src** или **sheet**), добавляется колонка с этим именем.
- В ячейках по строкам:
  - **referential**: «OK» или короткий текст ошибки (например «НЕТ в CONTEST-DATA»);
  - **referential_composite**: «OK» или «НЕТ в GROUP»;
  - **unique**: пусто или «xN»;
  - **field_length**: «-» или строка с описанием нарушений;
  - **field_format**: «OK» или описание нарушения формата.
- При необходимости для колонок проверок можно задать цвет/формат в **color_scheme** (как для «ДУБЛЬ: …»), чтобы нарушения были заметны.

### 3.2. Сводный лист по проверкам

- Отдельный лист, например **CONSISTENCY** или **ПРОВЕРКИ_КОНСИСТЕНТНОСТИ**.
- Создаётся только если в конфиге есть включённые проверки и у части из них **output.include_in_summary: true**.
- Колонки формируются по образцу таблицы проверок (Проверки-Tаблица 1.csv). Сначала идут колонки-описания (заполняются из правил конфига):
  - **ТИП ПРОВЕРКИ** (внешний ключ в одну колонку, уникальность, длина полей, формат поля и т.д.),
  - **Описание** (поле name правила),
  - **таблица источник**, **поле источник**, **таблица где проверяем**, **поле для проверки**, **параметр сравнения**, **комментарий**.
- Затем колонки результата:
  - **check_id**, **sheet**, **name**, **имя_колонки**, **type**, **total_rows**, **violations**, **sample**.
- При вызове `run_consistency_checks_and_attach_summary` передаётся секция конфига (summary_sheet_name + rules); правила берутся из `config.get("rules")`, чтобы колонки-описания заполнялись.
- Порядок листа в книге задаётся в **sheet_order** (например CONSISTENCY после STAT_FILE).

### 3.3. Лог и консоль

- После выполнения проверок — блок в итоговой статистике (аналогично дубликатам и расхождениям по полям): список проверок, лист, число нарушений, примеры. Либо кратко: «Проверки консистентности: N нарушений по проверке X на листе Y».

---

## 4. Полная структура конфига (пример)

```json
"consistency_checks": {
  "summary_sheet_name": "CONSISTENCY",
  "rules": [
    {
      "id": "1.1",
      "name": "CONTEST_CODE из GROUP в CONTEST-DATA",
      "type": "referential",
      "enabled": true,
      "sheet_src": "GROUP",
      "column_src": "CONTEST_CODE",
      "sheet_ref": "CONTEST-DATA",
      "column_ref": "CONTEST_CODE",
      "output": { "column_on_sheet": "ПРОВЕРКА: CONTEST_CODE", "include_in_summary": true }
    },
    {
      "id": "3",
      "name": "Уникальность CONTEST_CODE+GROUP_CODE+GROUP_VALUE в GROUP",
      "type": "unique",
      "enabled": true,
      "sheet": "GROUP",
      "key_columns": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"],
      "output": { "column_on_sheet": "ДУБЛЬ: CONTEST_CODE_GROUP_CODE_GROUP_VALUE", "include_in_summary": true }
    },
    {
      "id": "5",
      "name": "Пара CONTEST_CODE+GROUP_CODE из REWARD-LINK в GROUP",
      "type": "referential_composite",
      "enabled": true,
      "sheet_src": "REWARD-LINK",
      "columns_src": ["CONTEST_CODE", "GROUP_CODE"],
      "sheet_ref": "GROUP",
      "columns_ref": ["CONTEST_CODE", "GROUP_CODE"],
      "output": { "column_on_sheet": "ПРОВЕРКА: пара в GROUP", "include_in_summary": true }
    },
    {
      "id": "org_unit_v20_length",
      "name": "Длина полей ORG_UNIT_V20",
      "type": "field_length",
      "enabled": true,
      "sheet": "ORG_UNIT_V20",
      "result_column": "FIELD_LENGTH_CHECK",
      "fields": {
        "TB_FULL_NAME": { "limit": 100, "operator": "<=" },
        "GOSB_NAME": { "limit": 100, "operator": "<=" },
        "GOSB_SHORT_NAME": { "limit": 20, "operator": "<=" }
      },
      "output": { "column_on_sheet": "FIELD_LENGTH_CHECK", "include_in_summary": true }
    }
  ]
}
```

- **summary_sheet_name** — имя листа со сводкой.
- **rules** — массив правил; по **enabled** решается, какие выполнять.

---

## 5. Порядок выполнения в пайплайне

- Проверки выполняются после загрузки листов и merge, при необходимости после текущих проверок дубликатов и длины полей.
- Перед финальной записью основного Excel:
  1. Загрузить правила из **consistency_checks.rules** (только **enabled: true**).
  2. Для каждого правила по **type** вызвать соответствующую функцию (referential / referential_composite / unique / field_length).
  3. Записать в соответствующий лист колонку **output.column_on_sheet**.
  4. Собрать по правилам с **include_in_summary** статистику (лист, количество нарушений, примеры).
  5. Сформировать DataFrame для **summary_sheet_name** и добавить его в **sheets_data**.
  6. Запись основного Excel выполняется как обычно (source не трогаем).

---

## 6. Сводная таблица по пунктам ПРОВЕРКИ.txt (1.1–5)

| Пункт | Тип | Лист-источник | Что проверяем | С чем / что делаем |
|-------|-----|----------------|---------------|---------------------|
| 1.1 | referential | GROUP | CONTEST_CODE | все значения есть в CONTEST-DATA.CONTEST_CODE |
| 1.2 | referential | INDICATOR | CONTEST_CODE | все значения есть в CONTEST-DATA.CONTEST_CODE |
| 1.3 | referential | REWARD-LINK | CONTEST_CODE | все значения есть в CONTEST-DATA.CONTEST_CODE |
| 2 | referential | REWARD-LINK | REWARD_CODE | все значения есть в REWARD.REWARD_CODE |
| 3 | unique | GROUP | (CONTEST_CODE, GROUP_CODE, GROUP_VALUE) | комбинация уникальна |
| 4 | unique | REWARD-LINK | (CONTEST_CODE, GROUP_CODE, REWARD_CODE) | комбинация уникальна |
| 5 | referential_composite | REWARD-LINK | (CONTEST_CODE, GROUP_CODE) | пара есть в GROUP |

---

## 7. Миграция текущих конфигов (при реализации)

- **check_duplicates** — каждую запись вида `{ "sheet": "...", "key": [...] }` можно преобразовать в правило **type: "unique"** с **key_columns** = **key**, **column_on_sheet** = «ДУБЛЬ: » + «_».join(key).
- **field_length_validations** — каждый лист с **result_column** и **fields** преобразовать в правило **type: "field_length"** с теми же полями и **output.column_on_sheet** = **result_column**.

После ввода единого формата **consistency_checks** старые секции **check_duplicates** и **field_length_validations** можно либо удалить, либо оставить для обратной совместимости (чтение при отсутствии правил в **consistency_checks**).
