### Пояснительный справочник полей JSON `REWARD_ADD_DATA`

_Смысловые трактовки — рабочие гипотезы по именам и выборке._


Документ описывает структуру и смысл полей объекта JSON, который хранится в CSV в колонке **`REWARD_ADD_DATA`** (файл выборки: `IN/SPOD/REWARD (PROM) 20-03 v0.csv`, 598 строк, разбор JSON после замены `"""` → `"` — без ошибок).

**Корень:** один JSON-объект (ниже пути указаны относительно этого объекта, без префикса `REWARD_ADD_DATA.`).

**Связь с `REWARD_TYPE`:** набор полей и их наполнение **зависят от типа награды**. Ниже для каждого поля указано, в каких типах оно встречается.

---


#### 1. Типы наград и охват данных

| REWARD_TYPE | Строк в выборке | Примечание |
|-------------|-----------------|------------|
| **BADGE**   | 529             | Значки, турниры, новости, приоритеты |
| **ITEM**    | 61              | Предметы, условия выдачи, группировки |
| **LABEL**   | 6               | Метки, цвет, дата окончания |
| **CRYSTAL** | 2               | Минимальный набор полей |

Число **уникальных путей** в данных по типу: ITEM — 51, BADGE — 29, LABEL — 16, CRYSTAL — 11.

---

#### 2. Матрица: поле ↔ REWARD_TYPE

Легенда: **+** — поле присутствует во всех строках этого типа в выборке; **±** — только в части строк; **—** — нет в данных этого типа.

| Поле (ключ верхнего уровня) | BADGE | ITEM | LABEL | CRYSTAL |
|----------------------------|:-----:|:----:|:-----:|:-------:|
| `bookingRequired`          | —     | +    | —     | —       |
| `businessBlock`            | +     | +    | —     | —       |
| `commingSoon`              | —     | +    | —     | —       |
| `deliveryRequired`         | —     | +    | —     | —       |
| `feature`                  | +     | +    | +     | +       |
| `fileName`                 | +     | +    | +     | +       |
| `getCondition`             | —     | +    | —     | —       |
| `hidden`                   | +     | +    | +     | +       |
| `hiddenRewardList`         | ±     | ±    | —     | —       |
| `ignoreConditions`         | —     | +    | —     | —       |
| `isGrouping`               | —     | +    | —     | —       |
| `isGroupingName`           | —     | +    | —     | —       |
| `isGroupingTitle`          | —     | +    | —     | —       |
| `isGroupingTultip`         | —     | +    | —     | —       |
| `itemAmount`               | —     | +    | —     | —       |
| `itemFeature`              | —     | ±    | —     | —       |
| `itemGroupAmount`          | —     | +    | —     | —       |
| `itemLimitCount`           | —     | +    | —     | —       |
| `itemLimitPeriod`          | —     | +    | —     | —       |
| `itemMinShow`              | —     | +    | —     | —       |
| `helpCodeList`             | ±     | —    | —     | —       |
| `masterBadge`              | +     | —    | —     | —       |
| `newsType`                 | ±     | —    | —     | —       |
| `nftFlg`                   | +     | +    | +     | +       |
| `outstanding`              | +     | +    | +     | +       |
| `parentRewardCode`         | +     | —    | +     | —       |
| `persomanNumberVisible`    | —     | ±    | —     | —       |
| `preferences`              | ±     | —    | —     | —       |
| `priority`                 | +     | —    | —     | —       |
| `recommendationLevel`      | +     | —    | —     | —       |
| `refreshOldNews`           | ±     | +    | +     | —       |
| `rewardAgainGlobal`        | +     | +    | +     | +       |
| `rewardAgainTournament`    | +     | +    | +     | +       |
| `rewardRule`               | +     | +    | +     | +       |
| `seasonItem`               | ±     | ±    | —     | —       |
| `singleNews`               | +     | +    | +     | +       |
| `tagColor`                 | —     | —    | +     | —       |
| `tagEndDT`                 | —     | —    | +     | —       |
| `teamNews`                 | +     | +    | +     | +       |
| `tournamentTeam`           | ±     | —    | —     | —       |
| `winCriterion`             | ±     | —    | —     | —       |

---

#### 3. Дерево полей (объединённое)

Отражает все ключи, встречавшиеся в выборке по любому `REWARD_TYPE`.

```
├── bookingRequired          [ITEM]
├── businessBlock[]          [BADGE, ITEM]
├── commingSoon              [ITEM]
├── deliveryRequired         [ITEM]
├── feature[]                [BADGE, ITEM, LABEL, CRYSTAL]
├── fileName                 [все]
├── getCondition             [ITEM]
│   ├── employeeRating
│   │   ├── minCrystalEarnedTotal
│   │   ├── minRatingBANK
│   │   ├── minRatingGOSB
│   │   ├── minRatingTB
│   │   └── seasonCode
│   ├── nonRewards[]
│   │   └── nonRewardCode
│   └── rewards[]
│       ├── amount
│       └── rewardCode
├── hidden                   [все]
├── hiddenRewardList         [BADGE, ITEM]
├── ignoreConditions[]       [ITEM]
├── isGrouping               [ITEM]
├── isGroupingName           [ITEM]
├── isGroupingTitle          [ITEM]
├── isGroupingTultip         [ITEM]
├── itemAmount               [ITEM]
├── itemFeature[]            [ITEM]
├── itemGroupAmount[]
│   ├── itemParam            [ITEM]
│   └── itemParamAmount      [ITEM]
├── itemLimitCount           [ITEM]
├── itemLimitPeriod          [ITEM]
├── itemMinShow              [ITEM]
├── helpCodeList[]           [BADGE]
├── masterBadge              [BADGE]
├── newsType                 [BADGE]
├── nftFlg                   [все]
├── outstanding              [все]
├── parentRewardCode         [BADGE, LABEL]
├── persomanNumberVisible[]  [ITEM]
├── preferences[]            [BADGE]
├── priority                 [BADGE]
├── recommendationLevel      [BADGE]
├── refreshOldNews           [BADGE, ITEM, LABEL]
├── rewardAgainGlobal        [все]
├── rewardAgainTournament    [все]
├── rewardRule               [все]
├── seasonItem[]             [BADGE, ITEM]
├── singleNews               [все]
├── tagColor                 [LABEL]
├── tagEndDT                 [LABEL]
├── teamNews                 [все]
├── tournamentTeam           [BADGE]
└── winCriterion             [BADGE]
```

---

#### 4. Справочник полей по алфавиту

Формат записи: **назначение (гипотеза по имени и данным)**, тип, типы наград, домен значений, частота/особенности в выборке.

##### `bookingRequired`

- **Тип:** строка (флаг).
- **REWARD_TYPE:** только ITEM; во всех 61 строке — **`Y`**.
- **Смысл (гипотеза):** требуется бронирование для получения предмета.

##### `businessBlock`

- **Тип:** массив строк (коды блока/подразделения).
- **REWARD_TYPE:** BADGE, ITEM.
- **Размер массива в выборке:** всегда ровно один элемент.
- **Значения элемента:**  
  - BADGE: `MNS`, `IMUB`, `RNUB`, `RSB1`, `KMSB1`, `KMKKSB`, `SERVICEMEN`, `KMFACTORING`;  
  - ITEM: `MNS`, `KMKKSB`.

##### `commingSoon`

- **Тип:** строка (опечатка в имени поля: ожидаемо *comingSoon*).
- **REWARD_TYPE:** ITEM; везде **`N`**.

##### `deliveryRequired`

- **Тип:** строка (флаг).
- **REWARD_TYPE:** ITEM; везде **`Y`**.

##### `feature`

- **Тип:** массив строк.
- **REWARD_TYPE:** все типы.
- **Длина:** BADGE 0–5 элементов; ITEM 0–6; LABEL 0–1; CRYSTAL в выборке всегда пустой массив.
- **Элементы:** в BADGE/ITEM — в основном **длинные текстовые описания** (подсказки/условия); в LABEL — короткие фразы про метку; в CRYSTAL — нет заполненных элементов в выборке.

##### `fileName`

- **Тип:** строка (идентификатор файла/кампании/турнира).
- **REWARD_TYPE:** все.
- **Домен:**  
  - BADGE — много кодов (`GURU`, `VASCODAGAMA`, турниры, комиссии и т.д.) и пустая строка;  
  - ITEM, LABEL, CRYSTAL — в выборке преимущественно **`""`**.

##### `hidden`

- **Тип:** строка-флаг **`Y` / `N`**.
- **REWARD_TYPE:** все.
- **Смысл (гипотеза):** скрытая награда в интерфейсе.

##### `hiddenRewardList`

- **Тип:** строка **`Y` / `N`** (не массив, несмотря на имя).
- **REWARD_TYPE:** BADGE (часть строк), ITEM (часть строк).
- **Смысл (гипотеза):** участие в списке скрытых наград.

##### `helpCodeList`

- **Тип:** массив строк.
- **REWARD_TYPE:** только BADGE; в 30 из 529 строках; длина 1–2.
- **Значения элементов:** **`AST_1`**, **`NFT_1`**.

##### `ignoreConditions`

- **Тип:** массив (в выборке всегда пустой).
- **REWARD_TYPE:** ITEM.

##### `isGrouping` / `isGroupingName` / `isGroupingTitle` / `isGroupingTultip`

- **Тип:** строки.
- **REWARD_TYPE:** только ITEM.
- **`isGrouping`:** **`Y` / `N`**.
- **`isGroupingName`:** человекочитаемое название группы (обед, гаджет, SberShop, стажировки и т.д.).
- **`isGroupingTitle` / `isGroupingTultip`:** заголовок и текст подсказки; часто **`""`**; в данных имя **`isGroupingTultip`** вероятно означает *tooltip* (опечатка).

##### `itemAmount`

- **Тип:** строка с числом (количество).
- **REWARD_TYPE:** ITEM.
- **Примеры значений:** пусто, `1`, `2`, … до `110` и др.

##### `itemFeature`

- **Тип:** массив строк.
- **REWARD_TYPE:** ITEM (60 из 61); элементы — **длинный текст** (описания признаков предмета).

##### `itemGroupAmount`

- **Тип:** массив объектов `{ itemParam, itemParamAmount }`.
- **REWARD_TYPE:** ITEM; вложенные объекты редки (2 строки с непустым наполнением).
- **`itemParam`:** название месяца (январь–декабрь).
- **`itemParamAmount`:** в выборке везде **`15`**.

##### `itemLimitCount`

- **Тип:** строка.
- **REWARD_TYPE:** ITEM; в выборке везде **`1`**.

##### `itemLimitPeriod`

- **Тип:** строка.
- **REWARD_TYPE:** ITEM; в выборке везде **`once`**.

##### `itemMinShow`

- **Тип:** строка (число).
- **REWARD_TYPE:** ITEM.
- **Значения:** `1`, `2`, `3`, `4`, `5`, `10`.

##### `masterBadge`

- **Тип:** строка **`Y` / `N`**.
- **REWARD_TYPE:** BADGE; во всех строках типа.

##### `newsType`

- **Тип:** строка.
- **REWARD_TYPE:** BADGE; не во всех строках.
- **Значения в выборке:** **`AIPROMPT`**, **`TEMPLATE`**.

##### `nftFlg`

- **Тип:** строка **`Y` / `N`**.
- **REWARD_TYPE:** все; у ITEM в выборке только **`N`**.

##### `outstanding`

- **Тип:** строка **`Y` / `N`**.
- **REWARD_TYPE:** все; у ITEM/LABEL/CRYSTAL в выборке **`N`**.

##### `parentRewardCode`

- **Тип:** строка (код родительской награды).
- **REWARD_TYPE:** BADGE (`REWARD_*`), LABEL (`LABEL_*`).

##### `persomanNumberVisible`

- **Тип:** массив (в выборке пустой, 20 строк ITEM).
- **Смысл по имени:** видимость «персонального номера» (возможно, *person* / *persona*).

##### `preferences`

- **Тип:** массив строк.
- **REWARD_TYPE:** BADGE; редко (9 строк); элементы — **длинный текст** (до ~127 символов).

##### `priority`

- **Тип:** строка.
- **REWARD_TYPE:** BADGE.
- **Значения:** **`1`**, **`2`**, **`3`**.

##### `recommendationLevel`

- **Тип:** строка (уровень/канал рекомендаций).
- **REWARD_TYPE:** BADGE.
- **Значения:** **`TB`**, **`NON`**, **`BANK`**, **`GOSB`**.

##### `refreshOldNews`

- **Тип:** строка **`Y` / `N`**.
- **REWARD_TYPE:** BADGE (не во всех), ITEM, LABEL; у CRYSTAL поля нет в данных.

##### `rewardAgainGlobal` / `rewardAgainTournament`

- **Тип:** строка **`Y` / `N`** (в выборке у многих типов только **`N`**).
- **REWARD_TYPE:** все, где поле есть.

##### `rewardRule`

- **Тип:** строка.
- **REWARD_TYPE:** все.
- **Домен:** **длинный текст** (правила/условия); у LABEL/CRYSTAL часто пусто.

##### `seasonItem`

- **Тип:** массив строк (коды сезона).
- **REWARD_TYPE:** BADGE (45 строк с непустым), ITEM (59 из 61 с элементом).
- **Примеры:** `SEASON_2025_1`, `SEASON_m_2025_1`, `SEASON_f_2025`, коды по блокам (`SEASON_imub_2025` и т.д.).

##### `singleNews` / `teamNews`

- **Тип:** строка.
- **REWARD_TYPE:** все.
- **Домен:** у BADGE — **длинный текст** (новости); у ITEM/LABEL/CRYSTAL в выборке часто **`""`**.

##### `tagColor`

- **Тип:** строка (дизайн-токен цвета).
- **REWARD_TYPE:** LABEL.
- **Значения:** `pink-80`, `green-70`, `orange-80`, `purple-80`, `light-blue-80`.

##### `tagEndDT`

- **Тип:** строка (дата окончания действия метки, формат даты).
- **REWARD_TYPE:** LABEL.
- **Значения:** `2024-12-31`, `2026-06-30`, `4000-01-01` (условная «бесконечность»).

##### `tournamentTeam`

- **Тип:** строка.
- **REWARD_TYPE:** BADGE; 21 строка; в выборке только **`Y`**.

##### `winCriterion`

- **Тип:** строка.
- **REWARD_TYPE:** BADGE; 37 строк.
- **Домен:** **длинный текст** (критерий победы в турнире).

---

#### 5. Блок `getCondition` (только ITEM)

Объект задаёт условия получения предмета.

| Подполе | Тип | Описание и домен (по выборке) |
|---------|-----|-------------------------------|
| `employeeRating` | объект | Есть в 59 из 61 строк; вложенные пороги рейтинга и сезон. |
| `employeeRating.minCrystalEarnedTotal` | строка | Число как строка или `""`; примеры: `3`, `10`. |
| `employeeRating.minRatingBANK` | строка | `""`, `1`, `3`, … до `100`. |
| `employeeRating.minRatingGOSB` | строка | `""`, `1`. |
| `employeeRating.minRatingTB` | строка | `""`, `1`, `2`, `3`, `5`. |
| `employeeRating.seasonCode` | строка | Код сезона: `SEASON_2024`, `SEASON_2025_1`, `SEASON_m_*` и т.д. |
| `nonRewards` | массив объектов | 0–8 элементов; объект: `{ "nonRewardCode": "..." }`. |
| `nonRewards[].nonRewardCode` | строка | Коды вида `ITEM_*`, `ITEM_m_*`, составные коды с сезонами. |
| `rewards` | массив объектов | 0–1 объект в выборке. |
| `rewards[].amount` | строка | В выборке **`1`**. |
| `rewards[].rewardCode` | строка | Код награды, например `REWARD_23_04` или `r_01_2025-2_03-5_1`. |

---

#### 6. Версия документа

| Версия | Дата | Изменения |
|--------|------|-----------|
| 1.0 | 2026-01-31 | Первичная выгрузка справочника по полям на основе анализа файла `REWARD (PROM) 20-03 v0.csv` (598 строк). Описания «смысла» полей — рабочие гипотезы по именам и значениям, не официальная ТЗ СПОД. |
