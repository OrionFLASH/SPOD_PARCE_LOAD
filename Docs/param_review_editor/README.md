# Редактор каталога параметров BADGE-формы

Удобная HTML-страница в стиле **Apple Liquid Glass**: стекло, iOS-toggle, сегменты статусов, иконки.

## Как открыть

1. Пересобрать данные (из схемы + `field_meta` + MD-снимка):

```bash
./venv/bin/python src/Tools/build_param_review_editor.py
```

2. Открыть в браузере:

**`Docs/param_review_editor/index.html`**

(двойной клик или Live Server; данные в `catalog.js`, fetch не нужен).

## Меню (как в SPOD)

| Пункт | Тип | Смысл |
|-------|-----|--------|
| CONTEST | TABLE | Лист CONTEST-DATA |
| ↳ CONTEST_FEATURE | JSON | Колонка JSON внутри CONTEST |
| REWARD | TABLE | Лист REWARD (в форме — слоты BADGE) |
| ↳ REWARD_ADD_DATA | JSON | Колонка JSON внутри REWARD |
| REWARD-LINK / GROUP / INDICATOR / SCHEDULE | TABLE | Соответствующие листы |

## Что умеет

- Карточка поля: подпись, описание, тип, **значение по умолчанию**, «можно пусто», JSON-цель, заметка
- **Варианты** — по одному в строке; активны только для `dropdown` / `list`
- Статусы `[ ]` / `[w]` / `[v]`
- Поиск и фильтр «только не готовые»
- Автосохранение в `localStorage` (ключ v2)
- **Экспорт:** JSON / CSV / MD
- **Импорт** JSON; «Сброс» — снова из `catalog.js`

## Как передать правки в чат

1. Нажмите **JSON** (предпочтительно).
2. Положите файл в проект или приложите в чат.
3. Напишите: **«примени каталог»**.

## Файлы

| Файл | Назначение |
|------|------------|
| `index.html` | Страница |
| `styles.css` | Liquid Glass |
| `app.js` | Логика |
| `catalog.js` / `catalog.json` | Данные (генерируются) |
| `src/Tools/build_param_review_editor.py` | Пересборка каталога |
| `Docs/CONTEST_BADGE_FORM_PARAM_REVIEW.md` | MD-снимок (архив) |
