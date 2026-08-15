# Редактор описания параметров BADGE (web-edit)

HTML-страница в стиле **Apple Liquid Glass**. Данные каталога — в соседнем каталоге:

`../param_catalog_review/catalog.json` (+ зеркало `catalog.js` для `file://`).

## Цикл

1. Открыть `index.html` (Live Server / HTTP) или импортировать JSON.
2. Правки в UI → метка «отредактировано».
3. **Сохранить JSON** → `catalog_ГГГГММДД_ЧЧММ.json` (положить/заменить в `param_catalog_review/`).
4. Сборка Excel-шаблона: токен `contest_badge_form_blank` читает `catalog.json` и пишет в `common/templates/CONTEST_BADGE_FORM/`.

## Пересборка каталога из кода (опционально)

```bash
./venv/bin/python src/Tools/build_param_review_editor.py
```

Пишет в `common/param_catalog_review/catalog.json` + `catalog.js`.

## Файлы

| Файл | Назначение |
|------|------------|
| `index.html` / `app.js` / `styles.css` | UI |
| `../param_catalog_review/catalog.json` | Источник данных |
| `../param_catalog_review/catalog.js` | Зеркало для `file://` |
