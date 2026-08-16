# Заполнение SPOD — полный контур (web-fill-full)

Копия `web-fill` с **разделением на файлы** (не всё в одном HTML).

Пока каталог параметров — тот же, что в `web-fill` (синхрон с текущим edit). Полный перечень из `web-edit-full` подключим отдельно.

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

## Синхронизация каталога (пока с web-edit)

Пока вручную копируйте `catalog.json` / `catalog.js` из `web-fill` после `sync_web_fill_catalog.py`, либо укажите путь на каталог из `web-edit-full`, когда будете переключать fill-full на полный список.
