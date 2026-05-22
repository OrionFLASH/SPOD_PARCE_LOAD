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
| КМ: условия выполнены | RATING + LIST-REWARDS + ORDER, без лимита 2 в группе |
| КМ: без 2 заказов в группе | условия + сумма заказов в группе &lt; max_orders |
| КМ: не закончился и не 2 в группе | + глобальный остаток по itemAmount |
| КМ: все ограничения кроме исчерпания | условия + лимит 2 в группе (остаток склада не учитывается) |

## Конфиг

```json
"season_order_summary": {
  "enabled": true,
  "sheet_name": "ORDER-SEASON-SUMMARY"
}
```

Колонки ORDER/RATING/группы берутся из `rating_item_matrix`. Модуль: `src/season_order_summary.py`, вызов после обогащения RATING в `main_impl.py`.

Связанные документы: `Docs/RATING_MATRIX_COLORS_AND_LOGIC.md` (логика доступности и itemAmount на RATING), `README.md` (раздел **season_order_summary**, версия **1.7.41**).
