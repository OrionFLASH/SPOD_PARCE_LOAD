# ToDo — волна 20 fill-full / edit-full

Статусы: `[v]` сделано · `[w]` в работе · `[ ]` не сделано · `[x]` отменено  
План: [`PLAN_WEB_FILL_EDIT_WAVE20.md`](PLAN_WEB_FILL_EDIT_WAVE20.md) · ROADMAP: пункт **20**.  
Пожелания: `common/ToDo FILL EDIT.txt`. Пункты **18** и **19** не откатывать.

---

## 20.0 Документы

- [v] План + этот ToDo + ROADMAP п.20 + DOCS_INDEX + changelog

---

## 20.A Тексты и справочники

- [v] masking / tournamentRewardingMailing / nftFlg descriptions
- [v] fileName `allow_empty: true`
- [v] BUSINESS_BLOCK: MKKMMB / «МКК ММБ»
- [v] SEASON_mkk_2026
- [v] Sync каталога в fill-full; миграция кодов при импорте

---

## 20.B UX fill

- [v] List: ввод по `\n`
- [v] GROUP_LAYOUT: GET_CALC_METHOD; затем три критерия
- [v] Активная вкладка конкурса явнее
- [v] Nav: ТУРНИР/НАГРАДА + CONTEST_CODE на кнопке

---

## 20.C Обязательные без автовыбора

- [v] Красный маркер / предупреждение
- [v] Чипы «надо выбрать» без подстановки значения
- [v] При сохранении обязательный ключ появляется, значение может быть пустым

---

## 20.D JSON-колонки

- [v] Edit-full: empty_json_mode + json_wrap_quotes
- [v] Fill: без фантома CONTEST_PERIOD; dump по каталогу

---

## 20.E list / omit / depends

- [v] list всегда массив в SPOD-JSON
- [v] omit_when_empty + depends_on (seasonItem и др.)

---

## 20.F Examples

- [v] Пересборка curated / badges / contests PROM + PSI + merged

---

## 20.G Приёмка

- [v] Пункты 1–16 ToDo FILL EDIT

---

## Связанное (пайплайн PY)

- [v] ROADMAP **21**: `json_spod_format` — обёртка `"`, `array_value_keys` для helpCodeList / seasonItem  
  → [`PLAN_CONSISTENCY_JSON_WRAP_ARRAYS.md`](PLAN_CONSISTENCY_JSON_WRAP_ARRAYS.md), [`TODO_CONSISTENCY_JSON_WRAP_ARRAYS.md`](TODO_CONSISTENCY_JSON_WRAP_ARRAYS.md)
