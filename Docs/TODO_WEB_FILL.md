# ToDo — fill / fill-full (волна фильтров и каталога)

Статусы: `[v]` сделано · `[w]` в работе · `[ ]` не сделано · `[x]` отменено  
План: [`PLAN_WEB_FILL.md`](PLAN_WEB_FILL.md) · ROADMAP: пункт **16**.

---

## 16.0 Документы

- [v] План `Docs/PLAN_WEB_FILL.md`
- [v] Этот ToDo
- [v] Пункт 16 в `ROADMAP.md`
- [v] README fill / fill-full, `Docs/DOCS_INDEX.md`

---

## 16.1 JSON без выдуманных блоков

- [v] Убрать заглушки в `export_web_fill_examples_from_spod.py` (schedule / indicator / group / badges)
- [v] Пустой лист CSV → в снимке `[]`, не объект с пустыми полями
- [v] Тест `test_build_contest_data_no_stub_rows` (CONTEST_00 без строк → пустые массивы)
- [ ] Скрипт/тест сверки по каждому `CONTEST_CODE` JSON = CSV — когда будут исходные CSV
- [ ] Прогнать все конкурсы PROM-SPOD, в т.ч. `CONTEST_00` — локально
- [ ] Пересобрать `common/web-fill/examples/*.json` — **отложено**: в репозитории нет исходных CSV

---

## 16.2 Импорт fill без фантомов

- [v] Не вызывать `emptyScheduleRow()` / пустой индикатор при загрузке снимка с `[]`
- [v] Заглушки-строки (пусто кроме `CONTEST_CODE`) отсекаются `pruneImportedEmptyRows` до expand
- [v] Реальная строка с пустым окончанием по-прежнему разворачивается в `t_CODE` / `r_CODE`
- [v] Заглушки только по кнопке «Добавить» (новый конкурс / новый период)
- [v] Одинаково в `web-fill` и `web-fill-full`

---

## 16.3 Левая панель и колонтитулы

- [v] Скрытие слева не трогает `.chrome-top` / `.chrome-foot`
- [v] Подсказка кнопки «Конкурсы» без «и колонтитулы»
- [v] Fill + fill-full

---

## 16.4 Правая панель

- [v] Сетка: левая | workspace | правая
- [v] Заголовок «Поиск и фильтры», своё скрытие (`is-filters-collapsed`)
- [v] Перенести поиск
- [v] Перенести «Турниры / Награды / Архив»
- [v] Слева: список + действия + легенда
- [v] Узкий экран: панели не прячут колонтитулы

---

## 16.5 Фильтры списка

- [v] ПРОМ / ТЕСТ по `vid` ∪ `TARGET_TYPE` (оба одинаковы = не режем; один = подстрока в любом поле)
- [v] Статусы SCHEDULE: 5 чипов; дефолт АКТИВНЫЙ + ПОДВЕДЕНИЕ ИТОГОВ + ЗАВЕРШЕН
- [v] Показ, если ≥1 период с выбранным статусом
- [v] Дата: date-picker; попадание в `[START_DT, END_DT]`
- [v] Группы фильтров через AND
- [v] Fill + fill-full

---

## 16.6 Легенда

- [v] Столбик цветов внизу слева (не матрица)
- [v] Подписи: Активный / Подведение / Завершён / Отменён·удалён / Нет турниров
- [v] Тултип с приоритетом «самый живой»

---

## 16.7 Каталог (edit + fill)

Правки в web-edit / web-edit-full, затем sync.

### INDICATOR_CODE

- [v] `kind: dropdown` (без своего варианта)
- [v] Дефолт `WAIT`
- [v] Все коды из пожелания (WAIT … WD)
- [v] Combobox: поиск + группировка по префиксу

### Списки и подписи

- [v] `PLAN_METHOD_CODE`: NOT_USED, PRESET_VALUE (дефолт), DEPENDS_PREVIOUS_PERIOD
- [v] `PLAN_MOD_METOD`: MULTIPLIER (дефолт), APPEND
- [v] `CONTEST_INDICATOR_METHOD`: «Интегральный» / «Отношение агрегатов»
- [v] `CONTEST_FACTOR_METHOD`: оставить FACT + недостающие Run rate; короткие подписи
- [v] `FACT_POST_PROCESSING`: 6 кодов, короткие подписи, пусто можно
- [v] `INDICATOR_ADD_CALC_TYPE`: Числитель / Знаменатель (сверка)
- [v] `INDICATOR_AGG_FUNCTION`: MIN, AVG, COUNT, LAST_VALUE + поправить подписи unique

### Sync

- [v] `sync_web_fill_catalog.py` → fill, fill-full, param_catalog_review
- [v] web-edit-full каталог (те же списки)

---

## 16.8 Приёмка

- [v] Экспортер: нет строк CSV → `[]` (тест CONTEST_00)
- [ ] Примеры `examples/*.json` без фантомов — после локальной пересборки из CSV
- [v] Колонтитулы при скрытой левой панели
- [v] Независимое скрытие правой панели
- [v] Фильтры ПРОМ/ТЕСТ, статус, дата (логика в fill / fill-full)
- [v] Легенда = цвет вкладки
- [v] INDICATOR_CODE: нельзя свой код (dropdown + combobox)
- [v] Поведение fill ≡ fill-full (сборка `sync_web_fill_singlefile.py`)
