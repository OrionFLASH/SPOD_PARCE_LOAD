# Заполнение SPOD — полный контур (web-fill-full)

Копия `web-fill` с **разделением на файлы** (не всё в одном HTML). Общая логика fill совпадает; с 16.16 UX (фильтры, коды r_/t_, список ITEM) дорабатывается **здесь**, однофайловый fill не зеркалится автоматически.

Каталог параметров синхронизируется из `web-edit` скриптом `sync_web_fill_catalog.py`. Примеры снимков JSON — в **`../examples/web-fill/`** (см. **`../examples/README.md`**).

## Файлы

| Файл | Назначение |
|------|------------|
| `game_fill_settings.html` | разметка |
| `game_fill_styles.css` | стили |
| `game_fill_core.js` | константы, состояние, раскладки |
| `game_fill_model.js` | данные, коды r_/t_/ITEM_, архив |
| `game_fill_filters.js` | поля, фильтры, выбор конкурсов |
| `game_fill_ui.js` | экраны секций |
| `game_fill_io.js` | JSON/CSV, localStorage |
| `game_fill_boot.js` | привязка кнопок и старт |
| `catalog.json` | каталог полей |
| `catalog.js` | зеркало для fallback (`window.PARAM_REVIEW_CATALOG`) |

## Открытие

Нужен HTTP (fetch `catalog.json`):

```bash
cd common/web-fill-full && python3 -m http.server 8766
```

Открыть `http://127.0.0.1:8766/game_fill_settings.html`.

Черновик в `localStorage` ключ: `spod_web_fill_full_project_v2` (отдельно от обычного fill).

## Интерфейс

Как в `../web-fill/README.md`, плюс fill-full:

- Правая панель шире; каждый фильтр — отдельная карточка.
- Коды `REWARD_CODE` / `TOURNAMENT_CODE`: если есть `r_`/`t_` + CONTEST_CODE — окончание; иначе поле целиком (старый формат). Новые конкурсы всегда с префиксом. ITEM: `ITEM_` + окончание товара.
- В JSON полный код + `REWARD_CODE_ENDING` / `TOURNAMENT_CODE_ENDING`. CSV — только штатные колонки с полным кодом.
- Режим «Выбрать»: выгрузка JSON/CSV только отмеченных конкурсов. Бизнес-блок задаётся на листе конкурса.
- Фильтры: у каждого блока мини-кнопки все/снять; **Среда** под датой; фильтр **Стенд** PROM/PSI (по умолчанию только PROM) и **только PROM / только PSI**. CSV по умолчанию — строки с меткой PROM.
- Метки стенда: **вкл** — PROM синий / PSI фиолетовый (белый текст); **выкл** — серые. На карточке, строках, в фильтре и в списке слева.
- `CONTEST_PERIOD` — блок внизу карточки конкурса; чип P×N в шапке скроллит к нему.
- Тип награды: Награда / Товар / Метка, затем Кристалл / «Нет награды». Статус: живые сверху, Отменён / Удалён / Нет турниров снизу.
- Кнопки сворачивания панелей на стыке колонок, стрелка над и под текстом.
- ITEM в списке слева: `REWARD_CODE`, ниже `FULL_NAME`, справа от кода `Ct:` из `itemAmount`.

Пустые массивы GROUP / INDICATOR / SCHEDULE / пары не дополняются заглушками при импорте — только кнопка «Добавить».

Следующая волна (выбор конкурсов для выгрузки, мастер бизнес-блока, `ITEM_`, полный код в JSON, разбиение JS) — план [`Docs/PLAN_WEB_FILL_FULL.md`](../../Docs/PLAN_WEB_FILL_FULL.md), пункт ROADMAP **18** (сделано в fill-full). Стенды PROM/PSI — [`Docs/PLAN_WEB_FILL_STANDS.md`](../../Docs/PLAN_WEB_FILL_STANDS.md), пункт **19**.

## Синхронизация

Из web-edit:

```bash
python3 src/Tools/sync_web_fill_catalog.py
```

Зеркало в однофайловый fill — только если явно нужно выровнять оба UI:

```bash
python3 src/Tools/sync_web_fill_singlefile.py
```

Пересборка примеров из CSV PROM SPOD в `CONFIG_RUN_INPUT.json` (листы каталога fill): `python3 src/Tools/export_web_fill_examples_from_spod.py` → `common/examples/web-fill/`. Полный снимок всех конкурсов — `contests/spod_fill_all_contests.json`.
