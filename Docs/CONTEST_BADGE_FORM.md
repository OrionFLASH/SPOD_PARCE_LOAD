# Excel-форма конкурса BADGE

Статус: **реализовано** (форма v6: xlsxwriter, dropdowns, цвета типов ввода).  
План: [`PLAN_CONTEST_BADGE_FORM.md`](PLAN_CONTEST_BADGE_FORM.md).  
**Заполнение шаблона (для пользователя):** [`CONTEST_BADGE_FORM_FILLING.md`](CONTEST_BADGE_FORM_FILLING.md).

## Назначение

Отдельные режимы (не ломают `main_only`):

| Токен `run_outputs` | Действие |
|---------------------|----------|
| `contest_badge_form_blank` | Пустая форма + пример → `OUT/{BLOCK}/CONTEST_BADGE_FORM/` |
| `contest_badge_form_export` | CSV `IN/{BLOCK}/SPOD` → Excel-форма (листы `1`, `2`, `3`…) |
| `contest_badge_form_import` | Excel-форма → Excel листов SPOD + CSV (SPOD-JSON с `"""`) |

Если в `run_outputs` блока **только** эти токены — после формы пайплайн блока завершается (без чтения всех CSV и main).

## Конфиг

Файл: `config/CONFIG_CONTEST_BADGE_FORM.json` (подключён в `config/config.json` через `$include`).

| Ключ | Смысл |
|------|--------|
| `contest_badge_form.block` | Блок (PROM / IFT / PSI) |
| `contest_badge_form.contest_codes` | Список `CONTEST_CODE` для export |
| `contest_badge_form.export_path` | Опционально: путь к xlsx export |
| `contest_badge_form.import_form_path` | Путь к форме для import (обязателен для import) |
| `contest_badge_form.import_output_dir` | Опционально: каталог результата import |
| `contest_badge_form.blank_path` | Пустой шаблон (по умолчанию `OUT/{BLOCK}/CONTEST_BADGE_FORM/CONTEST_BADGE_FORM_BLANK.xlsx`) |
| `contest_badge_form.blank_sheet_count` | Число листов `1..N` в пустой форме |
| `contest_badge_form.blank_contest_type` | Тип по умолчанию (слоты BADGE: турнир=3, индивид.=1) |
| `contest_badge_form.example_path` | Пример с заполненными конкурсами |
| `contest_badge_form.example_contest_codes` | Коды для примера (ОСВ накопительные + турниры) |
| `contest_badge_form.dropdowns` | Опциональный оверрайд выпадающих списков |

## Пустой шаблон и пример

Каталог: **`OUT/PROM/CONTEST_BADGE_FORM/`**

| Файл | Содержимое |
|------|------------|
| `CONTEST_BADGE_FORM_BLANK.xlsx` | Пустой лист `1`, тип `ТУРНИРНЫЙ`, 3 слота BADGE |
| `CONTEST_BADGE_FORM_EXAMPLE.xlsx` | 6 листов: 4× ОСВ накопительные + 2× «Зарплатный рывок» |

Пример (листы):

1. `09_2026-0_23-1_2` — Передача ОСВ из 1С (до 5 задач)  
2. `09_2026-0_23-1_3` — … (6-10 задач)  
3. `09_2026-0_23-1_4` — … (11-15 задач)  
4. `09_2026-0_23-1_5` — … (более 15 задач)  
5. `01_2026-1_05-3_1` — ФОТ. Зарплатный рывок  
6. `10_2026-0_05-3_1` — Зарплатный рывок с операторами СМЗ  

Колонки формы:

| Столбец | Назначение |
|---------|------------|
| A | Ключ поля (не менять) |
| B | Краткая подпись |
| C | **Значение** (цвет = тип ввода; см. легенду на листе) |
| D | Описание поля и допустимые значения |

### Цвета значений (легенда `#META:LEGEND`)

| Цвет | Тип | Как заполнять |
|------|-----|----------------|
| Зелёный `#C6EFCE` | Выбор из списка | Выпадающий список (Y/N, ПРОМ/ТЕСТ, статусы…) |
| Жёлтый `#FFF2CC` | Свободный ввод | Текст / число вручную |
| Персик `#FCE4D6` | Несколько через `;` | Массив → при импорте JSON-массив |
| Розовый `#F5B7B1` | JSON | Как в SPOD (`INDICATOR_FILTER`, `FILTER_PERIOD_ARR`, `GROUP_VALUE`…) |
| Голубой `#DDEBF7` | Дата | `YYYY-MM-DD` |

Подробный порядок заполнения и типичные ошибки — в **`CONTEST_BADGE_FORM_FILLING.md`**.

Списки значений — `src/contest_badge_form/field_meta.py`. Скрытый лист `Lists` (в конце книги) — длинные списки и значения с запятыми.

Пересоздать оба файла: токен `contest_badge_form_blank` в `run_outputs`, затем `python main.py`
(пишет blank + example в `OUT/PROM/CONTEST_BADGE_FORM/`).

## Запуск

**Пустая форма** — `"PROM": ["contest_badge_form_blank"]`, затем `python main.py`.

**Export из CSV:**

1. В `CONFIG_RUN_INPUT.json` для нужного блока:
   ```json
   "PROM": ["contest_badge_form_export"]
   ```
2. Заполнить `contest_codes` в `CONFIG_CONTEST_BADGE_FORM.json`.
3. `python main.py` → файл вида  
   `OUT/{BLOCK}/CONTEST_BADGE_FORM/CONTEST_BADGE_FORM_EXPORT_{BLOCK}_{ts}.xlsx`.

**Import:**

1. Указать `import_form_path` на заполненную форму.
2. `"PROM": ["contest_badge_form_import"]`.
3. Результат: `OUT/{BLOCK}/CONTEST_BADGE_FORM_IMPORT_{ts}/` (xlsx + csv)  
   либо каталог из `import_output_dir`.

## Правила BADGE

- **ТУРНИРНЫЙ** — до 3 наград `REWARD_TYPE=BADGE`.
- **ИНДИВИДУАЛЬНЫЙ** / **ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ** — ровно 1 BADGE.
- Не-BADGE при export — только предупреждение в лог.

## Форма (лист `N`)

Секции `#SECTION:CONTEST`, `#SECTION:BADGE:1..`, таблицы `#TABLE:…` (+ строка `#HINT`).  
Столбцы: **A** ключ · **B** подпись · **C** значение (цвет по типу) · **D** описание. Массивы — через `;`.

Запись книги: **xlsxwriter** (`src/contest_badge_form/xlsx_write.py`) — shared strings + data validation без порчи OOXML.

## Код

Пакет `src/contest_badge_form/`: `schema`, `field_meta`, `spod_json`, `csv_load`, `form_io`, `xlsx_write`, `export_form`, `import_form`, `runner`.

Тест round-trip: `src/Tests/test_contest_badge_form.py`.
