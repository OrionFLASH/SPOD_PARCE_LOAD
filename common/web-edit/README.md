# Редактор описания параметров BADGE (web-edit)

HTML-страница в стиле **Apple Liquid Glass**.

## Данные

Рядом со страницей (для автозагрузки):

- `catalog.json`
- `catalog.js` (зеркало для `file://`)

Та же копия для blank-генератора: `../param_catalog_review/`.

## Цикл

1. Открыть `index.html` (Live Server / HTTP) или импортировать JSON.
2. Правки → «Сохранить JSON» → положить/заменить `catalog.json` **и** в `web-edit/`, и в `param_catalog_review/`.
3. Токен `contest_badge_form_blank` читает `param_catalog_review/catalog.json` и пишет Excel в `../templates/`.

## Пересборка из кода

```bash
./venv/bin/python src/Tools/build_param_review_editor.py
```

Пишет `catalog.json` + `catalog.js` в оба каталога: `web-edit/` и `param_catalog_review/`.
