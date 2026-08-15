# Каталог параметров Excel-формы BADGE

JSON, экспортируемый из веб-редактора [`../web-edit/`](../web-edit/), — источник подписей, описаний, типов ввода, вариантов (`variants`), опциональных подписей кнопок (`variant_labels`) и дефолтов для пустого шаблона Excel.

| Файл | Назначение |
|------|------------|
| `catalog.json` | Рабочий каталог (blank + копия в `../web-edit/` для UI) |
| `catalog.js` | Зеркало для `file://` |
| `CONTEST_BADGE_FORM_PARAM_REVIEW.md` | MD-снимок (архив / дифф) |

После правок в web-edit сохраните JSON и скопируйте в этот каталог (или запустите `build_param_review_editor.py`). Blank читает **этот** `catalog.json`. Для fill: `python src/Tools/sync_web_fill_catalog.py`.

`variant_labels` — тот же порядок, что у `variants`; в CSV уходит значение, на чипах fill — подпись (если задана).
