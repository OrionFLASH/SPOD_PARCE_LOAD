# Заполнение SPOD — полный контур (web-fill-full)

Копия `web-fill` с **разделением на файлы** (не всё в одном HTML). Поведение совпадает с однофайловым fill.

Каталог параметров синхронизируется из `web-edit` скриптом `sync_web_fill_catalog.py`. Примеры снимков JSON — в **`../web-fill/examples/`**.

## Файлы

| Файл | Назначение |
|------|------------|
| `game_fill_settings.html` | разметка |
| `game_fill_styles.css` | стили |
| `game_fill_app.js` | логика UI / CSV |
| `catalog.json` | каталог полей |
| `catalog.js` | зеркало для fallback (`window.PARAM_REVIEW_CATALOG`) |
| `examples/` | примеры снимков JSON — см. **`../web-fill/examples/`** |

## Открытие

Нужен HTTP (fetch `catalog.json`):

```bash
cd common/web-fill-full && python3 -m http.server 8766
```

Открыть `http://127.0.0.1:8766/game_fill_settings.html`.

Черновик в `localStorage` ключ: `spod_web_fill_full_project_v2` (отдельно от обычного fill).

## Интерфейс

Как в `../web-fill/README.md`: слева список + легенда, справа «Поиск и фильтры» (тип, ПРОМ/ТЕСТ, статус, дата). Скрытие панелей независимое; колонтитулы остаются.

Пустые массивы GROUP / INDICATOR / SCHEDULE / пары не дополняются заглушками при импорте — только кнопка «Добавить».

## Синхронизация

Из web-edit:

```bash
python3 src/Tools/sync_web_fill_catalog.py
```

После правок css/html/js здесь — зеркало в однофайловый fill:

```bash
python3 src/Tools/sync_web_fill_singlefile.py
```

Пересборка `examples/*.json` из CSV PROM-SPOD — локально (`export_web_fill_examples_from_spod.py`); в репозитории исходных CSV нет.
