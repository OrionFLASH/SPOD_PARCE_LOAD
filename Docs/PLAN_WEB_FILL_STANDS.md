# План: метки стенда PROM / PSI / IFT в web-fill-full

**Статус:** восстановление поверх пункта 18 (2026-08-18 вечер)  
**Связано:** ROADMAP п. **19** (не путать с п. **18** — выбор JSON / бизнес-блок / `ITEM_` / `*_ENDING` / split JS).  
Fill-full: `common/web-fill-full/` (`game_fill_{core,model,filters,ui,io,boot}.js`). Экспортер `src/Tools/export_web_fill_examples_from_spod.py` — **добавлять** merge, не откатывать `*_ENDING`.

---

## 1. Цель

1. Собрать JSON снимок **только из PSI CSV** (аналог `spod_fill_all_contests.json`).
2. Собрать **объединённый** JSON PROM+PSI с метками принадлежности к стенду.
3. В fill-full: пиктограммы стенда, фильтр, редактирование меток (только теги в v1).
4. **Не ломать** текущий PROM-workflow; при конфликте данных — **приоритет PROM**.
5. Задел на третий стенд **IFT** (кода/файлов пока нет).

---

## 2. Термины (не путать)

| Понятие | Где живёт | Значение |
|---------|-----------|----------|
| **Стенд** | `stands: string[]` | Источник выгрузки SPOD: `PROM`, `PSI`, позже `IFT` |
| **Среда** | фильтр «ПРОМ / ТЕСТ» | Поля `feature.vid`, `TARGET_TYPE` — **не** стенд |
| **block** в JSON | корень снимка | Блок сборки: `PROM`, `PSI`, `MERGED` |

---

## 3. Входные файлы PSI (согласовано)

Каталог: `IN/PSI/SPOD/`

| Лист fill | Файл |
|-----------|------|
| CONTEST-DATA | `contest_data_IFT_2026-07-14.csv` ← **эталон** (новее) |
| CONTEST-GROUP | `contest_group_IFT_2026-07-09.csv` |
| CONTEST-INDICATOR | `contest_indicator_V20_IFT_2026-07-09.csv` |
| CONTEST-REWARD | `contest_reward_IFT_2026-07-09.csv` |
| REWARD-LINK | `contest_reward_link_IFT_2026-07-09.csv` |
| TOURNAMENT-SCHEDULE | `tournament_schedule_IFT_2026-07-09.csv` |

> Имена `*_IFT_*` в PSI-папке — это **стенд PSI**, не будущий стенд IFT.

### Статистика пересечения (на 2026-08-18)

| Метрика | Значение |
|---------|----------|
| Конкурсов PROM | 382 |
| Конкурсов PSI (07-14) | 391 |
| В обоих стендах | 377 |
| Только PSI | 14 |
| Только PROM | 5 |
| Конкурсы с разным набором турниров | 99 (пример: `CONTEST_05` — 18 PROM / 5 PSI, 4 общих) |

---

## 4. Решения заказчика (зафиксировано 2026-08-18)

| # | Вопрос | Ответ |
|---|--------|-------|
| Q1 | Какой `contest_data` PSI? | **`contest_data_IFT_2026-07-14.csv`** |
| Q2 | Стратегия файлов | **Три файла**: PROM, PSI, merged; по умолчанию работа с **merged**, можно открыть чистый PROM |
| Q3 | CSV-экспорт из merged | **По умолчанию только строки с `stands ∋ PROM`** (как сейчас); отдельный режим PSI — позже |
| Q4 | Редактирование стенда в UI v1 | **Только массив `stands`**; переключение источника данных PROM↔PSI — отдельный этап |
| Q5 | Ключ строки INDICATOR при merge | **`CONTEST_CODE` + `INDICATOR_ADD_CALC_TYPE` + `INDICATOR_CODE`** (как в CONFIG_RUN_INPUT) |
| Q6 | Ключ REWARD-LINK при merge | **`CONTEST_CODE` + `GROUP_CODE` + `REWARD_CODE`** |
| Q7 | Merge таблицы REWARD (награды) | **A:** в `badges[]` только награды с link этого конкурса; если `REWARD_CODE` в обоих стендах — поля награды из **PROM**; `stands` — по link. Если код только в PSI — поля из PSI (link-only). |
| Q8 | Имя merged-файла | **`spod_fill_all_contests_merged.json`** |
| Q9 | Обогащение старого PROM JSON | **Да** — `stands: ["PROM"]` на конкурс и все строки при пересборке |
| Q10 | Фильтр стенда по умолчанию | **Только PROM** (чип PSI выключен при открытии) |
| Q11 | `contests[].stands` vs дочерние | **Union** по дочерним строкам; ручная правка на конкурсе — override |

### Открытые вопросы

Нет — Q1–Q11 согласованы (2026-08-18).

---

## 5. Модель данных JSON

### 5.1 Версия и корень

```json
{
  "version": 5,
  "block": "MERGED",
  "standsManifest": ["PROM", "PSI"],
  "title": "...",
  "source": "...",
  "contests": [ ... ]
}
```

- `version: 5` — расширение v4 fill-full (конкурсы + archive); v2 снимки экспортера мигрируют при импорте.
- `standsManifest` — зарезервировано под IFT без смены схемы.

### 5.2 Конкурс

```json
{
  "id": "ex_CONTEST_05",
  "name": "...",
  "stands": ["PROM", "PSI"],
  "data": {
    "contest": { "CONTEST_CODE": "...", "FULL_NAME": "...", "stands": ["PROM"] },
    "feature": { ... },
    "contestPeriod": [],
    "group": [ { "GROUP_CODE": "...", "stands": ["PROM"] } ],
    "indicator": [ { "INDICATOR_CODE": "...", "N": "1", "stands": ["PSI"] } ],
    "schedule": [ { "TOURNAMENT_CODE": "...", "stands": ["PROM", "PSI"] } ],
    "badges": [ { "flat": {...}, "add": {...}, "link": {...}, "stands": ["PROM"] } ],
    "reward_link": []
  }
}
```

**Правила:**

1. **`contests[].stands`** — в каких стендах **есть** этот `CONTEST_CODE` (объединение).
2. **`data.contest.stands`** — откуда взяты **поля карточки** (при overlap → PROM, массив может быть `["PROM","PSI"]`).
3. **Каждая строка** массивов `group`, `indicator`, `schedule`, `badges` — своё поле **`stands: string[]`**.
4. Поле **`stands` не уходит в CSV** — только метаданные fill; при экспорте CSV отфильтровывается по стенду.
5. Новый конкурс в UI: **`stands: ["PROM"]`** на конкурсе и всех новых строках.

### 5.3 Алгоритм merge (приоритет PROM)

Для каждого `CONTEST_CODE` из **объединения** множеств PROM и PSI:

| Сущность | Ключ строки | Если ключ в обоих | Только PROM | Только PSI |
|----------|-------------|-------------------|-------------|------------|
| contest | `CONTEST_CODE` | данные **PROM**, `stands: ["PROM","PSI"]` | `["PROM"]` | `["PSI"]` |
| group | `CONTEST_CODE` + `GROUP_CODE` + `GROUP_VALUE` | данные **PROM**, `stands: ["PROM","PSI"]` | `["PROM"]` | `["PSI"]` |
| indicator | `CONTEST_CODE` + `INDICATOR_ADD_CALC_TYPE` + `INDICATOR_CODE` | данные **PROM**, `stands: ["PROM","PSI"]` | `["PROM"]` | `["PSI"]` |
| schedule | `TOURNAMENT_CODE` | данные **PROM**, `stands: ["PROM","PSI"]` | `["PROM"]` | `["PSI"]` |
| reward-link | `CONTEST_CODE` + `GROUP_CODE` + `REWARD_CODE` | link **PROM**, `stands: ["PROM","PSI"]` | `["PROM"]` | `["PSI"]` |
| badge (REWARD) | `REWARD_CODE` (через link) | flat/add **PROM** если код в обоих; `stands` по link | flat из PROM | flat из PSI |

**Union строк:** в merged попадают **все** ключи из обоих стендов (пример: 5 турниров = 2 PROM-only + 3 PSI-only + общие с победой PROM).

**REWARD (Q7):** таблица наград **глобальная** (не привязана к конкурсу напрямую). В снимок конкурса попадают только `REWARD_CODE`, на которые есть **REWARD-LINK** этого конкурса. При merge: если один код есть в PROM и PSI — поля награды (`FULL_NAME`, `REWARD_TYPE`, …) из **PROM**; метка `stands` — по link. Link-only PSI → поля награды из PSI.

---

## 6. Артефакты (три файла)

| Файл | block | Назначение |
|------|-------|------------|
| `common/examples/web-fill/contests/spod_fill_all_contests.json` | PROM | как сейчас + `stands: ["PROM"]` |
| `common/examples/web-fill/contests/spod_fill_all_contests_PSI.json` | PSI | новый снимок PSI |
| `common/examples/web-fill/contests/spod_fill_all_contests_merged.json` | MERGED | основной для работы |

---

## 7. ROADMAP — пункт 19 (декомпозиция)

Статусы: `[v]` `[w]` `[ ]` `[x]`

### Фаза A — Экспорт и merge (Python)

| # | Задача | Статус |
|---|--------|--------|
| 19.1 | Манифест PSI CSV в `config` или аргументы CLI экспортера | [w] |
| 19.2 | `load_spod_tables(block)` — обобщить с PROM на PSI (не только `load_prom_spod_tables`) | [w] |
| 19.3 | Экспорт `spod_fill_all_contests_PSI.json` + сверка JSON=CSV | [w] |
| 19.4 | Модуль `merge_spod_fill_projects(prom, psi) -> merged` с правилами §5.3 | [w] |
| 19.5 | CLI: `--block PSI`, `--merge`, пересборка всех трёх файлов одной командой | [w] |
| 19.6 | Обогащение PROM-снимка полем `stands: ["PROM"]` при пересборке | [w] |
| 19.7 | Тесты merge: overlap contest, mixed schedule, PSI-only, PROM-only, reward через link | [w] |

**Команда (целевая):**

```bash
python3 src/Tools/export_web_fill_examples_from_spod.py --all-contests --block PSI
python3 src/Tools/export_web_fill_examples_from_spod.py --merge-stands
```

### Фаза B — Импорт и миграция (fill-full JS)

| # | Задача | Статус |
|---|--------|--------|
| 19.8 | Константа `STANDS = ["PROM","PSI","IFT"]`; `IFT` в UI disabled/hidden | [w] |
| 19.9 | `normalizeContest()`: дефолт `stands: ["PROM"]` если нет поля | [w] |
| 19.10 | Импорт v2/v4 → v5: проставить `stands` из `block` или `["PROM"]` | [w] |
| 19.11 | `buildProjectObject`: сохранять `stands` на конкурсе и строках; `version: 5` | [w] |
| 19.12 | Новый конкурс / новая строка таблицы → `stands: ["PROM"]` | [w] |

### Фаза C — UI: пиктограммы и фильтр

Код был сделан, затем пропал из рабочей копии; восстанавливаем.

| # | Задача | Статус |
|---|--------|--------|
| 19.13 | CSS: чипы/иконки стенда (PROM, PSI, IFT-placeholder) | [w] |
| 19.14 | Список конкурсов слева: пиктogramma `stands` конкурса | [w] |
| 19.15 | Шапка каждой страницы редактирования: метка конкурса + метка **текущей сущности** (турнир, группа, …) | [w] |
| 19.16 | Nav-шаги: мини-метка если у шага несколько строк с разными stands (напр. schedule) | [w] |
| 19.17 | Фильтр **«Стенд»** на правой панели **под блоком «Среда»** (не путать!) | [w] |
| 19.18 | Логика фильтра: конкурс виден, если `contests[].stands` пересекается с выбранными чипами; **по умолчанию только PROM** | [w] |
| 19.19 | Фильтр строк внутри конкурса (schedule list): опционально v1.1 — скрывать PSI-строки при фильтре только PROM | [w] |

### Фаза D — Редактирование меток (v1)

| # | Задача | Статус |
|---|--------|--------|
| 19.20 | Редактор `stands` на странице «Конкурс» (multi-toggle PROM / PSI) | [w] |
| 19.21 | Редактор `stands` на строках group / indicator / schedule / badge | [w] |
| 19.22 | Валидация: массив не пустой; значения только из `standsManifest` | [w] |
| 19.23 | Синхронизация: смена `contests[].stands` = union дочерних (или явное override — TBD) | [w] |

### Фаза E — CSV-экспорт (минимум v1)

| # | Задача | Статус |
|---|--------|--------|
| 19.24 | Экспорт CSV: только строки с `"PROM" in stands` (поведение как сейчас для merged) | [w] |
| 19.25 | Кнопка/режим «Экспорт PSI» — отложено (отдельный подпункт после v1) | [x] |

### Фаза F — Документация и приёмка

| # | Задача | Статус |
|---|--------|--------|
| 19.26 | Обновить `Docs/DOCS_INDEX.md`, `common/examples/README.md`, README changelog | [w] |
| 19.27 | ROADMAP.md п.19 + ссылка на этот план | [w] |
| 19.28 | Приёмка: открыть merged, фильтр PSI/PROM, правка меток, save/load, CSV PROM-only | [w] |

### Фаза G — Будущее (не v1)

| # | Задача | Статус |
|---|--------|--------|
| 19.G1 | Стенд **IFT**: файлы, merge, чип в фильтре | [ ] |
| 19.G2 | Переключение источника данных строки PROM↔PSI (`_alt` снимок) | [ ] |
| 19.G3 | Двойной CSV-экспорт split по stands | [ ] |

---

## 8. Изменения по файлам (прогноз)

| Компонент | Файлы |
|-----------|-------|
| Экспорт / merge | `src/Tools/export_web_fill_examples_from_spod.py`, новый `src/Tools/merge_spod_fill_stands.py` |
| Конфиг PSI | `config/CONFIG_RUN_INPUT.json` (секция PSI) или `config/PSI_SPOD_MANIFEST.json` |
| Примеры | `common/examples/web-fill/contests/*.json` (3 файла) |
| UI | `common/web-fill-full/game_fill_{core,model,filters,ui,io,boot}.js`, `game_fill_styles.css`, `game_fill_settings.html` |
| Тесты | `src/Tests/test_merge_spod_fill_stands.py` |
| Доки | `Docs/PLAN_WEB_FILL_STANDS.md`, `ROADMAP.md` |

---

## 9. Риски и митигация

| Риск | Митигация |
|------|-----------|
| Старый JSON без `stands` ломает фильтр | Миграция при импорте → `["PROM"]` |
| Путаница «Среда» vs «Стенд» | Разные подписи, стенд **ниже** среды; в коде `standTags` vs `envFilter` |
| Дубли REWARD_CODE между стендами | Merge по link; глобальный reward не смешивать между конкурсами |
| Большой merged JSON (~2×) | Lazy load не нужен; один файл как сейчас |
| IFT в именах PSI-файлов | В коде block=`PSI`, не `IFT` |

---

## 10. Критерии готовности v1

- [ ] Три JSON пересобираются одной командой без расхождений сверки CSV.
- [ ] merged открывается в fill-full; PROM-only JSON работает как раньше.
- [ ] У конкурса `CONTEST_05` в schedule видны турниры обоих стендов с корректными `stands`.
- [ ] При overlap contest card — поля из PROM, `stands: ["PROM","PSI"]`.
- [ ] Фильтр стенда под «Средой»; пиктogramмы на списке и страницах.
- [ ] Ручная смена `stands` сохраняется в JSON v5.
- [ ] CSV-экспорт по умолчанию = только PROM-строки.

---

## 11. Порядок реализации (после согласования)

1. **19.1–19.7** — Python: PSI export + merge + тесты + три JSON.
2. **19.8–19.12** — импорт/экспорт JSON v5 без UI.
3. **19.13–19.19** — пиктogramмы и фильтр.
4. **19.20–19.23** — редактирование меток.
5. **19.24** — CSV PROM-only filter.
6. **19.26–19.28** — доки и приёмка.

**Правило:** восстанавливать UI в текущих 6 JS-файлах, не собирая монолит (кроме миграции импорта, если нужна для проверки JSON).

---

## 12. Ночной UX 18 авг (восстановлено поверх 18 и 19)

Та же ночная сессия, что стенды, дополнительно (после 1:00):

1. Редактор `CONTEST_PERIOD` внизу карточки «Конкурс» (несколько наборов). Отдельной страницы нет. Чип **P×N** в шапке открывает карточку и скроллит к блоку.
2. Фильтр статуса турнира: вторая строка — Отменён / Удалён / Нет турниров.
3. Фильтр типа награды: строка 1 — Награда / Товар / Метка; строка 2 — Кристалл / **Нет награды**.
4. Кнопки сворачивания панелей на **стыке колонок**, стрелка над и под подписью; левая панель развёрнута ← / свёрнута →, правая наоборот.
5. Метки стенда **PROM** (синий) / **PSI** (фиолетовый): включено — сплошная заливка и белый текст; выключено — серые. То же в фильтре, на карточке/строках и в списке конкурсов.
