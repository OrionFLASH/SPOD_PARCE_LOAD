# Заполнение SPOD (web-fill)

**Один файл:** `index.html` (HTML + CSS + JS + встроенный каталог).  
Типы полей, варианты и дефолты правятся **только в этом HTML** (блок `EMBEDDED_CATALOG`). Загрузка каталога/настроек из JSON отключена.

## Как открыть

Откройте `index.html` в браузере (`file://` или Live Server) — внешние `catalog.json` / `catalog.js` не нужны.

## Возможности

- Каталог встроен в HTML; черновик значений — в `localStorage`
- Подсказки: описание, формат, **обязательно / можно пусто**
- Даты: календарь + чипы «Начало / Конец года / Бесконечный»
- Dropdown / list — чипы; **number** — поле ввода числа (`REWARD_COST` по умолчанию `5`)
- Подсветка изменённых полей
- Несколько конкурсов вкладками; экспорт 6 CSV (`;`, UTF-8 BOM), блок **PROM**
- «Сохранить снимок JSON» — только выгрузка значений (обратная загрузка отключена)

## Цепочка шагов

Конкурс → Особенности → Группы → Связи → Награды → Индикаторы → Расписание

## 6 CSV

1. `CONTEST (PROM) FORM_FILL.csv`
2. `REWARD (PROM) FORM_FILL.csv`
3. `REWARD-LINK (PROM) FORM_FILL.csv`
4. `GROUP (PROM) FORM_FILL.csv`
5. `INDICATOR (PROM) FORM_FILL.csv`
6. `SCHEDULE (PROM) FORM_FILL.csv`

`CONTEST_FEATURE` / `REWARD_ADD_DATA` / массивы — SPOD-JSON (`"""…"""`).

## Синхронизация каталога (опционально)

Файлы `catalog.json` / `catalog.js` рядом — зеркало для разработки; рабочий fill их не читает.  
Чтобы обновить встроенный блок после правок в web-edit:

```bash
python src/Tools/sync_web_fill_catalog.py
# затем встроить JSON в index.html (маркеры EMBEDDED_CATALOG_*)
```
