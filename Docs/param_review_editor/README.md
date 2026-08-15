# Редактор каталога параметров BADGE-формы

Удобная HTML-страница в стиле **Apple Liquid Glass**. Источник: **`catalog.json`** (+ зеркало **`catalog.js`** для открытия без HTTP).

## Цикл работы

1. **Загрузка** `catalog.json` (Live Server) / зеркало `catalog.js` (`file://`) или **Импорт JSON**.
2. Правки → метка **«отредактировано»** (до импорта нового исходника; экспорт не снимает).
3. **Сохранить JSON** → `catalog_ГГГГММДД_ЧЧММ.json`.
4. В чат: **«примени каталог»** (+ приложите JSON).

## Как открыть

```bash
./venv/bin/python src/Tools/build_param_review_editor.py
```

Открыть **`Docs/param_review_editor/index.html`**:

- **Live Server / HTTP** — читается актуальный `catalog.json`.
- **Двойной клик (`file://`)** — `fetch` JSON часто блокируется; подхватывается `catalog.js`.

После замены только `catalog.json` без пересборки: Live Server или **Импорт JSON**.

## Меню (как в SPOD)

| Пункт | Тип | Смысл |
|-------|-----|--------|
| CONTEST | TABLE | Лист CONTEST-DATA |
| ↳ CONTEST_FEATURE | JSON | Колонка JSON внутри CONTEST |
| REWARD | TABLE | Лист REWARD (в форме — слоты BADGE) |
| ↳ REWARD_ADD_DATA | JSON | Колонка JSON внутри REWARD |
| REWARD-LINK / GROUP / INDICATOR / SCHEDULE | TABLE | Соответствующие листы |

## Что умеет

- Типы: **Выбор из списка** / **Свободный текст** / **Число** / **Массив значений** / **JSON формат {[ ]}** / **Дата (YYYY-MM-DD)**
- Дефолт: для dropdown — select из вариантов; для list — галочки; иначе текст
- Варианты — каждое значение с новой строки (dropdown / list)
- Статусы `[ ]` / `[w]` / `[v]` и **«Можно пусто»** в шапке карточки
- Поиск по всем разделам; фильтры «не готовые» / «отредактированные»
- Черновик в `localStorage`

## Файлы

| Файл | Назначение |
|------|------------|
| `index.html` | Страница |
| `styles.css` | Liquid Glass |
| `app.js` | Логика |
| `catalog.json` | Источник данных (HTTP / импорт) |
| `catalog.js` | Зеркало для `file://` |
| `src/Tools/build_param_review_editor.py` | Пересборка `catalog.json` + `catalog.js` |
| `Docs/CONTEST_BADGE_FORM_PARAM_REVIEW.md` | MD-снимок (архив) |
