# План: Excel-форма конкурса (REWARD_TYPE = BADGE)

Статус: **реализовано** (v1). Документация: [`Docs/CONTEST_BADGE_FORM.md`](CONTEST_BADGE_FORM.md).

## Решения (ответы)

| # | Решение |
|---|---------|
| 1 | Полный набор полей из каталога, **но только связанные с сценарием BADGE** (не ITEM/LABEL/CRYSTAL-only; плоские колонки + JSON-ключи, которые реально используются с BADGE / турнирным конкурсом) |
| 2 | Лимит наград на конкурс: **ТУРНИРНЫЙ → до 3 BADGE**; **ИНДИВИДУАЛЬНЫЙ** / **ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ → ровно 1 BADGE** |
| 3 | Экспорт из **текущих CSV** `IN/{BLOCK}/SPOD` |
| 4 | Листы формы только **`1`, `2`, `3`…**; `CONTEST_CODE` — поле на листе |
| 5 | Не-BADGE при экспорте — **только лог / предупреждение** |
| 6 | Запуск **только токены `run_outputs`** (без отдельного CLI в первой версии) |
| 7 | Критерий готовности — **round-trip** (export → import ≈ исходные CSV по выбранным кодам) |

---

## Цель

1. Excel-форма «как UI»: CONTEST / REWARD(BADGE) / REWARD-LINK / GROUP / INDICATOR / SCHEDULE.
2. Несколько конкурсов = листы `1`, `2`, `3`…
3. **Импорт:** форма → Excel листов исходников + CSV (все колонки, SPOD-JSON с `"""`).
4. **Экспорт:** список `CONTEST_CODE` → форма (только BADGE и связи конкурса).
5. Режимы **изолированы** от `main_only`.

---

## Архитектура

```text
EXPORT:  CONTEST_CODE[] + IN/{BLOCK}/SPOD CSV  →  Excel-форма (листы 1..N)
IMPORT:  Excel-форма                           →  Excel SPOD-листы + CSV pull
```

| Компонент | Роль |
|-----------|------|
| `src/contest_badge_form/` | schema, form IO, assemble SPOD-JSON, export, import |
| `config/CONFIG_CONTEST_BADGE_FORM.json` | пути, enum/dropdown, `contest_codes`, блок |
| Токены | `contest_badge_form_export`, `contest_badge_form_import` в `run_outputs` (аддитивно; early path без полного main, если нет `main_only`) |

Не менять логику основного Excel / consistency / manager_stats — только вызов по флагу.

---

## Форма (лист `N`)

Секции на одном листе:

1. **Конкурс** — плоские CONTEST-DATA + листья `CONTEST_FEATURE`, релевантные BADGE-сценарию.
2. **Награды BADGE** — слоты по `CONTEST_TYPE`:
   - ТУРНИРНЫЙ: слоты 1..3 (лишние пустые игнорируются при импорте; >3 — ошибка/предупреждение);
   - ИНДИВИДУАЛЬНЫЙ / ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ: слот 1.
3. **REWARD-LINK**, **GROUP**, **INDICATOR**, **TOURNAMENT-SCHEDULE** — таблицы строк данного конкурса.

Data validation из конфига. Массивы (`businessBlock`, `feature`, …) — ввод через `;` или мини-таблицу → сборка SPOD-массива.

**Whitelist ADD_DATA (BADGE):** `masterBadge`, `priority`, `recommendationLevel`, `parentRewardCode`, `businessBlock`, `feature`, `helpCodeList`, `newsType`, `preferences`, `tournamentTeam`, `winCriterion`, `hidden`, `hiddenRewardList`, `nftFlg`, `outstanding`, `refreshOldNews`, `rewardAgainGlobal`/`Tournament`, `rewardRule`, `seasonItem`, `singleNews`, `teamNews`, `fileName` — уточнить по каталогу при реализации, без ITEM/LABEL-only.

---

## Импорт / экспорт

**Экспорт:** CSV блока → фильтр по `contest_codes` → только BADGE в наградах → листы `1..N`; не-BADGE в лог.

**Импорт:** листы `^\d+$` → проверка лимита наград по `CONTEST_TYPE` → SPOD-JSON → `OUT/.../CONTEST_BADGE_FORM_IMPORT_<ts>/` (xlsx + csv).

**Round-trip тест:** export выбранных кодов → import → сравнение ключевых колонок/JSON с исходными CSV (нормализация `"""`).

---

## Этапы реализации

1. Schema JSON (whitelist полей BADGE-сценария + лимиты по `CONTEST_TYPE`).
2. Генератор пустой формы + data validation.
3. Export из IN CSV.
4. Import → Excel + CSV.
5. Токены в `config_loader` / `main_impl` (изолированная ветка).
6. Тест round-trip + Docs (`Docs/CONTEST_BADGE_FORM.md`, пункт в README/ROADMAP).

---

## Вне скоупа v1

- CLI  
- SQLite как источник export  
- Листы с именем `CONTEST_CODE`  
- Экспорт не-BADGE на отдельный лист  
