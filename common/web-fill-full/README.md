# Заполнение SPOD — полный контур (web-fill-full)

Копия `web-fill` с **разделением на файлы** (не всё в одном HTML). Общая логика fill совпадает; с 16.16 UX (фильтры, коды r_/t_, список ITEM) дорабатывается **здесь**, однофайловый fill не зеркалится автоматически.

Каталог параметров синхронизируется из `web-edit` скриптом `sync_web_fill_catalog.py`. Примеры снимков JSON — в **`../examples/web-fill/`** (см. **`../examples/README.md`**).

## Файлы

| Файл | Назначение |
|------|------------|
| `game_fill_settings.html` | разметка |
| `game_fill_styles.css` | стили |
| `game_fill_app.js` | логика UI / CSV |
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
- Коды `REWARD_CODE` / `TOURNAMENT_CODE`: если есть `r_`/`t_` + CONTEST_CODE — окончание; иначе поле целиком (старый формат). Новые конкурсы всегда с префиксом.
- ITEM в списке слева: `REWARD_CODE`, ниже `FULL_NAME`, справа от кода `Ct:` из `itemAmount`.

Пустые массивы GROUP / INDICATOR / SCHEDULE / пары не дополняются заглушками при импорте — только кнопка «Добавить».

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
