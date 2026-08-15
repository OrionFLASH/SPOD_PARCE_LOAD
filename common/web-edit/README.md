# Редактор описания параметров BADGE (web-edit)

HTML-страница в стиле **Apple Liquid Glass**.

## Данные

- При **HTTP / Live Server** страница сама читает `catalog.json` рядом с собой.
- Через **`file://`** автозагрузка недоступна: в левой панели до первого открытия файла — карточка «Откройте catalog.json».

Копия для blank-генератора: `../param_catalog_review/catalog.json`.

## Цикл

1. Откройте `index.html` (лучше через Live Server) или выберите `catalog.json` в сайдбаре.
2. Правки → «Сохранить JSON» → заменить файл в `web-edit/` (и при необходимости в `param_catalog_review/`).
3. Токен `contest_badge_form_blank` читает `param_catalog_review/catalog.json`.

## Пересборка каталога из схемы

```bash
python src/Tools/build_param_review_editor.py
```

## Связка с заполнением SPOD

После правок каталога синхронизируйте форму заполнения:

```bash
python src/Tools/sync_web_fill_catalog.py
```

Страница: `../web-fill/index.html`.
