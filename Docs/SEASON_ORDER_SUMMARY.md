# Лист ORDER-SEASON-SUMMARY

Сводка по кодам ITEM из `rating_item_matrix.item_order_groups` (группы `SEASON_*`).

## Колонки

| Колонка | Источник |
|---------|----------|
| Код награды, Наименование товара | REWARD (ITEM), каталог ADD_DATA |
| Группа сезона | `id` группы из `item_order_groups` |
| Всего товаров | `itemAmount` |
| Заказано | сумма строк агрегата ORDER по коду (статус ≠ Отменён / Отклонён) |
| Остаток | Всего − Заказано |
| Статус наличия | `ЗАКОНЧИЛСЯ`, если заказано ≥ itemAmount |
| Мин. рейтинг BANK/TB/GOSB, Мин. кристаллов | `employeeRating` в ADD_DATA |
| Ограничение rewardCode / nonRewardCode | списки из getCondition |
| ignoreConditions (кол-во), (табельные) | ADD_DATA |
| КМ: … (4 колонки) | См. **`Docs/SEASON_ORDER_SUMMARY_KM_LOGIC.md`** — подробная логика, формулы в коде, расхождения с ТЗ |

Ниже кодов SEASON — строка-разделитель и **все остальные ITEM** из REWARD (**Группа сезона** пустая).

**Excel:** «Всего», «Заказано», «Остаток», мин. рейтинги, мин. кристаллы, «ignoreConditions (кол-во)», «КМ: …» — целое число, выравнивание по центру (`column_formats` → **ORDER-SEASON-SUMMARY**).

## Конфиг

```json
"season_order_summary": {
  "enabled": true,
  "sheet_name": "ORDER-SEASON-SUMMARY",
  "include_other_items": true,
  "section_other_label": "— Прочие товары (вне групп SEASON) —"
}
```

Колонки ORDER/RATING/группы берутся из `rating_item_matrix`. Модуль: `src/season_order_summary.py`, вызов после обогащения RATING в `main_impl.py` — **только** если в **`run_outputs`** указан токен **`season_order_summary`** (дополнительно к `enabled` в конфиге).

## Логика колонок «КМ:»

Полное описание (кого считаем, `cond_ok`, лимит 2 в группе, четыре формулы, почему цифры могут не сходиться, чеклист для правки):

→ **[`Docs/SEASON_ORDER_SUMMARY_KM_LOGIC.md`](SEASON_ORDER_SUMMARY_KM_LOGIC.md)**

Кратко: счётчик идёт по **строкам RATING** (не по уникальным табельным); **«КМ: без 2 заказов в группе»** и **«КМ: все ограничения кроме исчерпания»** в текущем коде **совпадают**.

---

## Связанные документы

- `Docs/RATING_MATRIX_COLORS_AND_LOGIC.md` — матрица на RATING (доступность, itemAmount, группы).
- `README.md` — раздел **season_order_summary**.
