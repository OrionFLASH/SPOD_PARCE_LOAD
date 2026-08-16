# Полный каталог параметров (web-edit-full)

Копия UI `web-edit` + каталог, собранный **полным сканом** `IN/PROM/SPOD` (все CSV CONTEST / REWARD / REWARD-LINK / GROUP / INDICATOR / SCHEDULE).

## Отличие от `web-edit`

| | web-edit | web-edit-full |
|--|----------|---------------|
| Каталог | ручной / BADGE-форма | baseline + **все** колонки и JSON-ключи из PROM |
| Новые поля | — | `status: "[ ]"` (не готово), `note: auto: PROM SPOD scan` |
| Где известно | — | заполнены label / description / kind / variants |

## Файлы

- `game_edit_parameters.html` — редактор
- `game_edit_app.js`, `game_edit_styles.css`
- `game_edit_catalog.json` — актуальный полный каталог
- `game_edit_catalog_YYYYMMDD_HHMM.json` — снимок

## Пересборка каталога

```bash
python src/Tools/build_web_edit_full_catalog.py
```

Скрипт читает baseline `common/web-edit/game_edit_catalog_20260816_2304.json` и все CSV в `IN/PROM/SPOD`.

## Как открыть

Live Server / `python -m http.server` из `common/web-edit-full/` → открыть `game_edit_parameters.html` → выбрать `game_edit_catalog.json`.
