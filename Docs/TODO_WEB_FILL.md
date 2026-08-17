# ToDo — fill / fill-full (волна фильтров и каталога)

Статусы: `[v]` сделано · `[w]` в работе · `[ ]` не сделано · `[x]` отменено  
План: [`PLAN_WEB_FILL.md`](PLAN_WEB_FILL.md) · ROADMAP: пункт **16**.

**Сейчас код не пишем** — только план.

---

## 16.0 Документы

- [v] План `Docs/PLAN_WEB_FILL.md`
- [v] Этот ToDo
- [v] Пункт 16 в `ROADMAP.md`
- [ ] После реализации: README fill / fill-full, `Docs/DOCS_INDEX.md`

---

## 16.1 JSON без выдуманных блоков

- [ ] Убрать заглушки в `export_web_fill_examples_from_spod.py` (schedule / indicator / group / badges)
- [ ] Пустой лист CSV → в снимке `[]`, не объект с пустыми полями
- [ ] Скрипт/тест сверки: по каждому `CONTEST_CODE` число строк JSON = CSV (CONTEST, GROUP, INDICATOR, SCHEDULE, REWARD, REWARD-LINK)
- [ ] Прогнать все конкурсы PROM-SPOD, в т.ч. `CONTEST_00`
- [ ] Пересобрать `common/web-fill/examples/*.json`

---

## 16.2 Импорт fill без фантомов

- [ ] Не вызывать `emptyScheduleRow()` / пустой индикатор при загрузке снимка с `[]`
- [ ] Пустой `TOURNAMENT_CODE` в импорте не превращать в `t_<CONTEST_CODE>`
- [ ] То же для пустого `REWARD_CODE` → не делать `r_<CONTEST_CODE>` из заглушки
- [ ] Заглушки только по кнопке «Добавить» (новый конкурс / новый период)
- [ ] Одинаково в `web-fill` и `web-fill-full`

---

## 16.3 Левая панель и колонтитулы

- [ ] Скрытие слева не трогает `.chrome-top` / `.chrome-foot`
- [ ] Подсказка кнопки «Конкурсы» без «и колонтитулы»
- [ ] Fill + fill-full

---

## 16.4 Правая панель

- [ ] Сетка: левая | workspace | правая
- [ ] Заголовок «Поиск и фильтры», своё скрытие (`is-filters-collapsed`)
- [ ] Перенести поиск
- [ ] Перенести «Турниры / Награды / Архив»
- [ ] Слева: список + действия + легенда
- [ ] Узкий экран: панели не прячут колонтитулы

---

## 16.5 Фильтры списка

- [ ] ПРОМ / ТЕСТ по `vid` ∪ `TARGET_TYPE` (оба = не режем; один = подстрока в любом поле)
- [ ] Статусы SCHEDULE: 5 чипов; дефолт АКТИВНЫЙ + ПОДВЕДЕНИЕ ИТОГОВ + ЗАВЕРШЕН
- [ ] Показ, если ≥1 период с выбранным статусом
- [ ] Дата: date-picker; попадание в `[START_DT, END_DT]`
- [ ] Группы фильтров через AND
- [ ] Fill + fill-full

---

## 16.6 Легенда

- [ ] Столбик цветов внизу слева (не матрица)
- [ ] Подписи: Активный / Подведение / Завершён / Отменён·удалён / Нет турниров
- [ ] Тултип с приоритетом «самый живой»

---

## 16.7 Каталог (edit + fill)

Правки в web-edit / web-edit-full, затем sync.

### INDICATOR_CODE

- [ ] `kind: dropdown` (без своего варианта)
- [ ] Дефолт `WAIT`
- [ ] Все коды из пожелания (WAIT … WD)
- [ ] Combobox: поиск + группировка по префиксу

### Списки и подписи

- [ ] `PLAN_METHOD_CODE`: NOT_USED, PRESET_VALUE (дефолт), DEPENDS_PREVIOUS_PERIOD
- [ ] `PLAN_MOD_METOD`: MULTIPLIER (дефолт), APPEND
- [ ] `CONTEST_INDICATOR_METHOD`: «Интегральный» / «Отношение агрегатов»
- [ ] `CONTEST_FACTOR_METHOD`: оставить FACT + недостающие Run rate; короткие подписи
- [ ] `FACT_POST_PROCESSING`: 6 кодов, короткие подписи, пусто можно
- [ ] `INDICATOR_ADD_CALC_TYPE`: Числитель / Знаменатель (сверка)
- [ ] `INDICATOR_AGG_FUNCTION`: MIN, AVG, COUNT, LAST_VALUE + поправить подписи unique

### Sync

- [ ] `sync_web_fill_catalog.py` → fill, fill-full, param_catalog_review
- [ ] web-edit-full каталог, если полный контур тоже показывает эти поля

---

## 16.8 Приёмка

- [ ] `CONTEST_00`: нет `t_CONTEST_00`, нет пустого индикатора, если их нет в CSV
- [ ] Колонтитулы при скрытой левой панели
- [ ] Независимое скрытие правой панели
- [ ] Фильтры на полном снимке BADGE
- [ ] Легенда = цвет вкладки
- [ ] INDICATOR_CODE: нельзя свой код
- [ ] Поведение fill ≡ fill-full
