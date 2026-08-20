# План: волна 20 — fill-full / edit-full (ToDo FILL EDIT)

**Статус:** сделано  
**Связано:** ROADMAP п. **20**. Не откатывать пункты **18** и **19**.  
**Дополнение (пайплайн PY):** ROADMAP п. **21** — `json_spod_format` в `main_only` / consistency: обёртка `"`, `array_value_keys` для helpCodeList / seasonItem.  
**Исходник:** [`common/ToDo FILL EDIT.txt`](../common/ToDo%20FILL%20EDIT.txt)  
**Страницы:** `common/web-fill-full/`, `common/web-edit-full/`. Однофайловые web-fill / web-edit **не зеркалим**.

ToDo: [`TODO_WEB_FILL_EDIT_WAVE20.md`](TODO_WEB_FILL_EDIT_WAVE20.md).

---

## Цель

1. Тексты и справочники каталога (опечатки, MKKMMB, SEASON_mkk, fileName allow_empty, nftFlg).
2. UX fill: массивы по переводам строк, раскладка GROUP, активная вкладка, шапка ТУРНИР/НАГРАДА + CONTEST_CODE.
3. Обязательные ключи без автовыбора — красный маркер.
4. Пустые JSON-колонки и кавычки — метаданные в edit-full, fill только по каталогу.
5. `kind=list` → массив в SPOD-JSON; `omit_when_empty` + `depends_on`.
6. Пересборка examples из новых CSV PROM (+ PSI/merged).

---

## Согласованные решения

| Тема | Решение |
|------|---------|
| Страницы | fill-full + edit-full |
| Пустой JSON-массив колонки | `empty_json_mode`: `empty` \| `brackets` \| `brackets_quoted`; fill не угадывает |
| Обёртка непустого массива | `json_wrap_quotes` (двойные кавычки CSV; не `'`) |
| KMMMB → MKKMMB | каталог + миграция при импорте fill |
| SEASON_mmb_2026 → SEASON_mkk_2026 | каталог + миграция при импорте |
| Массивы list (ручной ввод) | одна строка = один элемент (`\n`; `;` при загрузке — совместимость) |
| Зависимые ключи | `omit_when_empty` + `depends_on` (1–3, AND) |

---

## Модель каталога

```text
empty_json_mode: "empty" | "brackets" | "brackets_quoted"
json_wrap_quotes: true | false
omit_when_empty: true | false
depends_on: [ { field, json_path?, equals }, ... ]
```

Источник sync fill: `common/web-edit-full/game_edit_catalog.json` → `web-fill-full/catalog.json` (+ catalog.js).

---

## Порядок

| # | Блок |
|---|------|
| 20.0 | Документы |
| 20.A | Тексты и справочники |
| 20.B | UX fill |
| 20.C | Маркеры обязательности |
| 20.D | empty JSON meta + без фантомов |
| 20.E | list / omit / depends |
| 20.F | Пересборка examples |
| 20.G | Приёмка |
