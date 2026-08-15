# Каталог параметров Excel-формы BADGE

JSON, экспортируемый из веб-редактора [`../web-edit/`](../web-edit/), — источник подписей, описаний, типов ввода, вариантов и дефолтов для пустого шаблона Excel.

| Файл | Назначение |
|------|------------|
| `catalog.json` | Рабочий каталог (читает blank-генератор и web-edit) |
| `catalog.js` | Зеркало для открытия редактора через `file://` |
| `CONTEST_BADGE_FORM_PARAM_REVIEW.md` | MD-снимок (архив / дифф) |

Шаблон Excel: [`../templates/CONTEST_BADGE_FORM/CONTEST_BADGE_FORM_BLANK.xlsx`](../templates/CONTEST_BADGE_FORM/CONTEST_BADGE_FORM_BLANK.xlsx).

Путь в конфиге: `contest_badge_form.catalog_path` → `common/param_catalog_review/catalog.json`.
