# Формат работы с проверками на консистентность

Документ описывает полный формат конфигурации и вывода проверок на консистентность данных.

**Актуально с версии 1.4:** колонки **unique** («ДУБЛЬ: …») и **field_length** (FIELD_LENGTH_CHECK и т.д.) **создаёт сам модуль consistency_checks** по правилам из `consistency_checks.rules`. Секции **check_duplicates** и **field_length_validations** в config.json **удалены** — все правила задаются только в `consistency_checks.rules` (типы `unique` и `field_length` с полем `fields` для ограничений по длине).

**Версия 1.7.11:** в перечень типов правил добавлен **`json_priority_unique_per_contest_link`** (см. п. 2.7).

**Версия 1.7.12:** для типа **`unique`** — поля **`unique_scope_conditions`**, **`unique_scope_mode`**, **`unique_require_non_empty`** и устаревшая пара **`unique_scope_column`** / **`unique_scope_value`** (см. п. 2.4). В **config.json** проекта все правила **`unique`** содержат полный набор этих ключей (пустые значения = проверка по всем строкам листа); подробнее — в **README.md**, раздел **consistency_checks → Правило unique**.

**Дополнение:** тип **`json_spod_format`**; для **`referential`** / **`referential_composite`** — опциональные **`src_row_conditions`** / **`ref_row_conditions`**; правила с **`enabled: false`** всё равно попадают в свод **CONSISTENCY** (см. п. 2.2, 2.8, 3.2).

**Идентификаторы правил (`id`):** для базовых проверок из ПРОВЕРКИ.txt в **`config.json`** используются **смысловые** id (например **`ref_group_contest_code_in_contest_data`** вместо **`1.1`**); см. таблицу в п. 6 и **`Docs/SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.md`**.

**SQL-зеркало (витрина / Hive / Spark):** файл **`Docs/SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.sql`** дублирует в СУБД логику типов **`referential`**, **`referential_composite`**, **`unique`**, **`field_length`** (один запрос со сводкой и деталями); идентификаторы проверок совпадают с **`rules[].id`** для этих правил. Тип **`field_format`** в SQL не зеркалируется — только **`consistency_checks.py`**. Внутри запроса — CTE **`dim_*`** для снижения повторных чтений таблиц и комментарии на русском к конструкциям SQL и проверкам. Полное описание назначения и ограничений — в шапке этого SQL, в **`Docs/DOCS_INDEX.md`** и в отдельном справочнике **`Docs/SPOD_CONSISTENCY_CHECKS_SQL_MIRROR.md`** (все CTE, проверки, таблицы/поля, замены под витрину).

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
- **type** — тип: `"referential"` | `"unique"` | `"referential_composite"` | `"field_length"` | `"field_format"` | `"json_field_equals_column"` | `"json_field_in_column"` | `"json_priority_unique_per_contest_link"` | `"json_spod_format"`.
- **enabled** — выполнять ли проверку (true/false). При **false** строка в своде **CONSISTENCY** всё равно создаётся: **total_rows** по целевому листу, **violations = 0**, в **sample** — пометка об отключённом правиле; колонка проверки на листе не заполняется.
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

**Опционально — ограничение строк:**

- **`src_row_conditions`** (или устаревшее **`sheet_src_row_conditions`**) — массив объектов `{ "column": "ИМЯ", "op": "=", "value": "..." }`. **op**: `=`, `==`, `eq`, `<>`, `!=`, `ne`. Условия объединяются по **И**. Строки источника, не удовлетворяющие фильтру, получают в колонке результата **«—»** и **не** учитываются в **violations**.
- **`ref_row_conditions`** (**`sheet_ref_row_conditions`**) — то же для листа-справочника: во множество допустимых значений попадают только строки, прошедшие фильтр.

Те же ключи поддерживаются для типа **`referential_composite`**.

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
| 16 | EMPLOYEE | POSITION_NAME, KPK_CODE, ORG_UNIT_CODE (область: POSITION_NAME=КПК, непустой KPK_CODE) | ДУБЛЬ: POSITION_NAME_KPK_CODE_ORG_UNIT_CODE |

Пример правила в едином формате `consistency_checks`:

```json
{
  "id": "group_unique",
  "name": "Уникальность CONTEST_CODE+GROUP_CODE+GROUP_VALUE в GROUP",
  "type": "unique",
  "enabled": true,
  "sheet": "GROUP",
  "key_columns": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"],
  "unique_scope_mode": "all",
  "unique_scope_conditions": [],
  "unique_scope_column": "",
  "unique_scope_value": "",
  "unique_require_non_empty": [],
  "output": {
    "column_on_sheet": "ДУБЛЬ: CONTEST_CODE_GROUP_CODE_GROUP_VALUE",
    "include_in_summary": true
  }
}
```

Рекомендуется указывать пять опциональных полей **во всех** правилах `unique`: при пустых `conditions`, пустых строках legacy и пустом `unique_require_non_empty` проверка выполняется по **всем** строкам (как раньше).

- **sheet** — лист, на котором проверяем.
- **key_columns** — колонки, образующие ключ уникальности.
- В **column_on_sheet** по строкам: пусто при уникальности, при дубле — «xN» (N — количество строк с этим ключом). Имя колонки может совпадать с текущим форматом «ДУБЛЬ: key1_key2_...» для совместимости.

**Дополнительно (версия 1.7.12):**

| Поле | Описание |
|------|-----------|
| **unique_scope_conditions** | Массив объектов `{ "column": "…", "value": "…" }`. Сравнение: `str(ячейки).strip() == str(value).strip()`, пустые/NaN дают пустую строку слева. |
| **unique_scope_mode** | **`all`** (по умолчанию) — строка в проверке, только если **все** условия выполнены (**И**). **`any`**, **`or`** или **`или`** — достаточно **любого** условия (**ИЛИ**). |
| **unique_scope_column** / **unique_scope_value** | Устаревший вариант одной пары; используется, если **unique_scope_conditions** не задан или пуст. |
| **unique_require_non_empty** | Массив имён колонок. Строка **не участвует** в проверке уникальности, если хотя бы одна из них «пустая» (как в проверках длины: пропуск, `NaN`, строки `-`, `None`, `null`). |

Строки вне области и с пустыми обязательными полями: в колонке «ДУБЛЬ: …» для этого правила — **пусто** (проверка не применялась). В своде CONSISTENCY поле **total_rows** для `unique` — число строк, попавших в проверку.

#### Пошаговая логика (модуль `consistency_checks`)

1. Строится маска **области**: если **`unique_scope_conditions`** пуст и устаревшие **`unique_scope_column`** / **`value`** не задают условия — область = **все строки**. Иначе для каждой пары вычисляется совпадение ячейки с ожидаемым значением; результаты объединяются по **`unique_scope_mode`** (**`all`** → логическое произведение масок, **`any`** / **`or`** / **`или`** — дизъюнкция).
2. Строится маска **непустоты** по **`unique_require_non_empty`**: строка допускается только если **каждая** перечисленная колонка не считается пустой (см. таблицу выше). Пустой массив — маска «все true».
3. **Активная строка** = область **И** непустота.
4. **`groupby(key_columns)`** и подсчёт кратности ключа выполняются **только на подмножестве активных строк**; полученные метки «xN» или пусто записываются в колонку результата; для неактивных строк ячейка остаётся пустой.
5. Отсутствующая на листе колонка из условия области даёт предупреждение в лог и для этой пары маска совпадений = false для всех строк; отсутствующая колонка из **`unique_require_non_empty`** — все строки становятся неактивными по этому правилу.

Реализация: `src/consistency_checks.py` — функции **`_normalize_unique_scope_conditions`**, **`_unique_scope_mode`**, **`_unique_scope_mask`**, **`_unique_require_non_empty_mask`**, **`_unique_active_row_mask`**, **`_run_unique_check`**, **`collect_unique_result`**.

#### Примеры режимов И и ИЛИ

- **И (`unique_scope_mode`: `all`)** — две пары в **`unique_scope_conditions`**: строка проверяется только если **одновременно** `COL_A == "X"` **и** `COL_B == "Y"`.
- **ИЛИ (`unique_scope_mode`: `any` или `or`)** — те же две пары: строка проверяется, если **хотя бы одно** условие выполнено (в том числе оба).

#### Пример (EMPLOYEE: только КПК и непустой KPK_CODE)

Полный шаблон с пустыми legacy-полями (как в проектном **config.json**):

```json
{
  "id": "unique_employee_kpk_gosb",
  "type": "unique",
  "enabled": true,
  "sheet": "EMPLOYEE",
  "key_columns": ["POSITION_NAME", "KPK_CODE", "ORG_UNIT_CODE"],
  "unique_scope_mode": "all",
  "unique_scope_conditions": [
    { "column": "POSITION_NAME", "value": "КПК" }
  ],
  "unique_scope_column": "",
  "unique_scope_value": "",
  "unique_require_non_empty": ["KPK_CODE"],
  "output": { "column_on_sheet": "ДУБЛЬ: POSITION_NAME_KPK_CODE_ORG_UNIT_CODE", "include_in_summary": true }
}
```

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
| **fixed_length_digits** | length (число цифр, напр. 20), allow_empty | Строка строго из ровно N цифр (лидирующие нули допустимы). Короткие и длинные значения считаются ошибкой; недопустимы буквы/знаки. |

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

В сводке и в ячейках для **field_format**: «OK» или описание нарушения (например «Не дата формата %Y-%m-%d», «18 < 20», «21 > 20», «Ожидаются только цифры»).

---

### 2.7. Тип `json_priority_unique_per_contest_link` (уникальность поля JSON по конкурсу)

Проверка на листе **REWARD**: для каждого **CONTEST_CODE** на листе **REWARD-LINK** собираются **уникальные REWARD_CODE** (колонка **GROUP_CODE** не участвует в группировке). В колонке **json_column** (например **REWARD_ADD_DATA**) разбирается JSON тем же способом, что и в **json_field_equals_column** (замена `"""` на `"`, затем `json.loads`).

**Логика в рамках одного CONTEST_CODE:**

- у **всех** привязанных наград ключ **json_key** (по умолчанию `priority`) **отсутствует или пуст** — нарушения нет (в колонке результата для этих строк — пусто);
- у **всех** задан — значения должны быть **попарно различны**;
- **смешанно** (часть с полем, часть без) — нарушение; сообщение выставляется **всем** строкам группы на REWARD;
- ошибка разбора JSON — только для соответствующей строки («Ошибка разбора ADD_DATA»).

**Поля правила:**

| Поле | Описание |
|------|-----------|
| `sheet` | Лист с данными наград (**REWARD**), куда пишется колонка результата. |
| `reward_code_column` | Колонка кода награды (обычно **REWARD_CODE**). |
| `json_column` | Колонка с JSON (**REWARD_ADD_DATA**). |
| `json_key` | Ключ в объекте JSON (по умолчанию **priority**). |
| `link_sheet` | Лист связей (**REWARD-LINK**). |
| `link_contest_column` | Колонка конкурса (**CONTEST_CODE**). |
| `link_reward_column` | Колонка кода награды на листе связей (**REWARD_CODE**). |
| `output.column_on_sheet` | Имя колонки на **sheet** (REWARD). |

**Пример:**

```json
{
  "id": "reward_priority_unique_per_contest",
  "name": "REWARD: priority по REWARD-LINK уникален в рамках CONTEST_CODE",
  "type": "json_priority_unique_per_contest_link",
  "enabled": true,
  "sheet": "REWARD",
  "reward_code_column": "REWARD_CODE",
  "json_column": "REWARD_ADD_DATA",
  "json_key": "priority",
  "link_sheet": "REWARD-LINK",
  "link_contest_column": "CONTEST_CODE",
  "link_reward_column": "REWARD_CODE",
  "output": {
    "column_on_sheet": "ПРОВЕРКА: priority уникален по CONTEST (REWARD-LINK)",
    "include_in_summary": true
  }
}
```

Подробнее см. **README.md** (секция **consistency_checks**, тип **json_priority_unique_per_contest_link**, история версий **1.7.11**).

---

### 2.8. Тип `json_spod_format` (JSON в нотации SPOD с тройными кавычками)

Проверка ячейки как строки **SPOD-JSON** выполняется в два этапа. **(1) Сырая строка:** симметрия внешней обёртки двойной кавычкой (если ячейка начинается с `"`, должна заканчиваться на `"`, и наоборот; либо без внешних кавычек); снятие **BOM** (U+FEFF); удаление **Unicode-пробелов** вне блоков **`"""…"""`** (в т.ч. неразрывный пробел U+00A0 из Excel/CSV), чтобы после нормализации не ломался **JSON**; корень — объект **`{`** или массив **`[`**; **каждый ключ** объекта — строго **`"""имя"""`** (имя: латиница, цифры, `_`); **значение** при ключе **не** из **`numeric_value_keys`** — либо строка **`"""…"""`**, либо вложенный объект, либо массив; **массив** в кавычки не берётся, **строковые элементы** массива — в **`"""…"""`**, числа и `true`/`false`/`null` в элементах — **без** кавычек; для ключей из **`numeric_value_keys`** значение — **только** число или `true`/`false`/`null` **без** кавычек. **(2)** замена **`"""` → `"`**, снятие одной внешней пары кавычек при наличии, **`json.loads`**; затем контроль типов для **`numeric_value_keys`** (после разбора — число, не строка; собираются **все** нарушения по дереву).

**Особые случаи (явно распознаются и кратко описываются в колонке проверки):**

- **`""ключ""` вместо `"""ключ"""`** — подсказка с именем ключа и правильным оформлением.
- **Значение в одной паре кавычек как в JSON** (`"""amount""":"1"` вместо `"""amount""":"""1"""`) — указывается путь к полю и литерал вроде `"1"`.
- **Лишние `{}` вокруг одной строки в массиве** (`[{"""текст"""}]` вместо `["""текст"""]`) — в объекте не может быть только строка без пары ключ:значение; сообщение подсказывает форму с **`["""…"""]`**.

**Текст в колонке результата:** короткие формулировки с **путём к месту** (например `getCondition.rewards[0].amount`, `объект «[0]»`, `объект «filtered_attribute_condition[3]»`); без длинных фрагментов исходной строки и без номера позиции в типовых ошибках SPOD. На этапе **(1)** разбор **не останавливается на первой ошибке**: в одной ячейке перечисляются все обнаруженные структурные нарушения (префикс **«разбор SPOD:»**, далее маркеры **•**); длина списка ограничена **`_MAX_STRUCTURE_ERRORS`** (по умолчанию **80**), остаток — строка **«… и ещё N ошибок(ок)»**. Длинные сообщения целиком обрезаются при записи в ячейку (**`_MAX_CELL_ERROR_LEN`**, по умолчанию **12000** символов).

| Поле | Описание |
|------|----------|
| `sheet` | Лист с колонкой JSON. |
| `json_column` | Имя колонки. |
| `json_required` | Если **true**, пустая ячейка — нарушение; если **false**, пустые пропускаются. |
| `numeric_value_keys` | Список имён ключей, у которых значение — число **без** `"""…"""`. |
| `output.column_on_sheet` | Колонка с результатом (**OK** или текст ошибки). |

Реализация: **`src/json_spod_format_check.py`**. Примеры правил — в **`config.json`** (идентификаторы **`spod_json_*`**).

---

## 3. Куда выводить информацию

### 3.1. На загружаемых листах (основной Excel)

- В **output** у каждой проверки задаётся **column_on_sheet** (имя колонки).
- Для листа, к которому привязана проверка (**sheet_src** или **sheet**), добавляется колонка с этим именем.
- В ячейках по строкам:
  - **referential**: «OK» или короткий текст ошибки (например «НЕТ в CONTEST-DATA»); при фильтре строк источника — **«—»** вне области;
  - **referential_composite**: «OK» или «НЕТ в GROUP»; при фильтре — **«—»** вне области;
  - **unique**: пусто (уникально или проверка к строке не применялась — вне области / пустые обязательные колонки) или «xN»;
  - **field_length**: «-» или строка с описанием нарушений;
  - **field_format**: «OK» или описание нарушения формата;
  - **json_field_equals_column** / **json_field_in_column**: «OK», пусто (не применимо) или текст ошибки;
  - **json_priority_unique_per_contest_link**: «OK», пусто (не в группе по ссылке или в группе все без json_key) или текст нарушения / «Ошибка разбора ADD_DATA»;
  - **json_spod_format**: «OK» или текст ошибки: при неверной разметке SPOD — **все** найденные замечания (**«разбор SPOD:»** и перечень с **•**, до **`_MAX_STRUCTURE_ERRORS`**); иначе короткая одна строка (после **json.loads** / **numeric_value_keys**).
- При необходимости для колонок проверок можно задать цвет/формат в **color_scheme** (как для «ДУБЛЬ: …»), чтобы нарушения были заметны.

### 3.2. Сводный лист по проверкам

- Отдельный лист, например **CONSISTENCY** или **ПРОВЕРКИ_КОНСИСТЕНТНОСТИ**.
- Создаётся при наличии правил в конфиге; в свод попадают записи с **output.include_in_summary: true**, в том числе для правил с **enabled: false** (без выполнения проверки, см. п. 2.1).
- Колонки формируются по образцу таблицы проверок (Проверки-Tаблица 1.csv). Сначала идут колонки-описания (заполняются из правил конфига):
  - **ТИП ПРОВЕРКИ** (внешний ключ в одну колонку, уникальность, длина полей, формат поля и т.д.),
  - **Описание** (поле name правила),
  - **таблица источник**, **поле источник**, **таблица где проверяем**, **поле для проверки**, **параметр сравнения**, **комментарий**.
- Затем колонки результата:
  - **check_id**, **sheet**, **name**, **имя_колонки**, **type**, **total_rows**, **violations**, **sample**.
- При вызове `run_consistency_checks_and_attach_summary` передаётся секция конфига (summary_sheet_name + rules); правила берутся из `config.get("rules")`, чтобы колонки-описания заполнялись.
- Порядок листа в книге задаётся в **sheet_order** (например CONSISTENCY после STAT_FILE).

### 3.3. Лог и консоль

- **Лог-файл** — полный отчёт: по каждому правилу (или группе) — `check_id`, лист, число нарушений, примеры значений (**DEBUG** для развёрнутых строк). Итоговые сообщения уровня **INFO** при необходимости дублируют сводку.
- **Консоль (терминал)** при запуске **`main.py`**: подробный **INFO** из логгера на **stdout** не выводится (уровень консоли — **WARNING** и выше). Краткая сводка для пользователя формируется модулем **`src/console_ui.py`**, функция **`print_consistency_summary(results)`**, где **results** — список словарей, возвращаемый **`run_consistency_checks_and_attach_summary`** (те же поля, что на листе свода: `type`, `sheet`, `violations`, `check_id`, `include_in_summary` и т.д.).
- Содержимое консольной сводки:
  1. Заголовок блока **«Консистентность (сводка)»**.
  2. **Оценка:** число правил в отчёте (записи с `include_in_summary: true`) и число **уникальных листов** по полю `sheet` в этих записях.
  3. **Итог:** либо явное сообщение, что **проблем не обнаружено** (сумма `violations` по всем таким правилам равна 0), либо **суммарное число нарушений** и число **правил, у которых violations > 0**.
  4. **Таблица по типу проверки** (`unique`, `field_length`, `referential`, …): для каждого типа — сколько правил, сколько уникальных листов, сумма `violations`, примечание **OK** / **есть (N)**.
  5. Если есть нарушения — дополнительный блок **по правилам с нарушениями** (листы, до нескольких строк на правило). Длинные **sample** в консоль не выводятся — только в лог (**DEBUG**).
- Если правила в конфиге не заданы или проверки не запускались, в консоль выводится соответствующее короткое сообщение.

Подробнее поведение консоли и история изменений — в корневом **`README.md`** (раздел про **`console_ui`**, п. 8 программы **main**, история версий **1.7.13–1.7.15**).

---

## 4. Полная структура конфига (пример)

```json
"consistency_checks": {
  "summary_sheet_name": "CONSISTENCY",
  "rules": [
    {
      "id": "ref_group_contest_code_in_contest_data",
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
      "id": "unique_group_contest_code_group_code_group_value",
      "name": "Уникальность CONTEST_CODE+GROUP_CODE+GROUP_VALUE в GROUP",
      "type": "unique",
      "enabled": true,
      "sheet": "GROUP",
      "key_columns": ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE"],
      "output": { "column_on_sheet": "ДУБЛЬ: CONTEST_CODE_GROUP_CODE_GROUP_VALUE", "include_in_summary": true }
    },
    {
      "id": "ref_composite_reward_link_pair_in_group",
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
  2. Для каждого правила по **type** вызвать соответствующую функцию модуля **consistency_checks** (в т.ч. referential, referential_composite, unique, field_length, field_format, json_field_equals_column, json_field_in_column, json_priority_unique_per_contest_link, json_spod_format).
  3. Записать в соответствующий лист колонку **output.column_on_sheet**.
  4. Собрать по правилам с **include_in_summary** статистику (лист, количество нарушений, примеры).
  5. Сформировать DataFrame для **summary_sheet_name** и добавить его в **sheets_data**.
  6. Запись основного Excel выполняется как обычно (source не трогаем).

---

## 6. Сводная таблица по пунктам ПРОВЕРКИ.txt (1.1–5)

| Пункт текста | `id` в config.json | Тип | Лист-источник | Что проверяем | С чем / что делаем |
|--------------|-------------------|-----|----------------|---------------|---------------------|
| 1.1 | `ref_group_contest_code_in_contest_data` | referential | GROUP | CONTEST_CODE | все значения есть в CONTEST-DATA.CONTEST_CODE |
| 1.2 | `ref_indicator_contest_code_in_contest_data` | referential | INDICATOR | CONTEST_CODE | все значения есть в CONTEST-DATA.CONTEST_CODE |
| 1.3 | `ref_reward_link_contest_code_in_contest_data` | referential | REWARD-LINK | CONTEST_CODE | все значения есть в CONTEST-DATA.CONTEST_CODE |
| 2 | `ref_reward_link_reward_code_in_reward` | referential | REWARD-LINK | REWARD_CODE | все значения есть в REWARD.REWARD_CODE |
| 3 | `unique_group_contest_code_group_code_group_value` | unique | GROUP | (CONTEST_CODE, GROUP_CODE, GROUP_VALUE) | комбинация уникальна |
| 4 | `unique_reward_link_contest_code_group_code_reward_code` | unique | REWARD-LINK | (CONTEST_CODE, GROUP_CODE, REWARD_CODE) | комбинация уникальна |
| 5 | `ref_composite_reward_link_pair_in_group` | referential_composite | REWARD-LINK | (CONTEST_CODE, GROUP_CODE) | пара есть в GROUP |

Отдельное правило в **`config.json`**: **`ref_employee_org_unit_code_in_org_unit_v20`** — EMPLOYEE.ORG_UNIT_CODE ∈ ORG_UNIT_V20 (раньше id **`9`**).

---

## 7. Миграция текущих конфигов (при реализации)

- **Переименование `id`:** если у вас остались старые короткие идентификаторы (**`1.1`**, **`2`**, **`3`**, **`4`**, **`5`**, **`9`**), замените их на смысловые из п. 6 (колонка **`id` в config.json**), чтобы совпадать с проектным **`config.json`** и SQL-зеркалом в комментариях.
- **check_duplicates** — каждую запись вида `{ "sheet": "...", "key": [...] }` можно преобразовать в правило **type: "unique"** с **key_columns** = **key**, **column_on_sheet** = «ДУБЛЬ: » + «_».join(key).
- **field_length_validations** — каждый лист с **result_column** и **fields** преобразовать в правило **type: "field_length"** с теми же полями и **output.column_on_sheet** = **result_column**.

После ввода единого формата **consistency_checks** старые секции **check_duplicates** и **field_length_validations** можно либо удалить, либо оставить для обратной совместимости (чтение при отсутствии правил в **consistency_checks**).
