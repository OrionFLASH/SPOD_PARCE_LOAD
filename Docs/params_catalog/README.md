# Excel-каталог параметров PROM/SPOD

## Назначение

Полный перечень настраиваемых параметров по выгрузкам:

`CONTEST-DATA`, `REWARD`, `REWARD-LINK`, `GROUP`, `INDICATOR`, `TOURNAMENT-SCHEDULE`, `REPORT`, `ORG_UNIT_V20`, `EMPLOYEE`, `USER_ROLE` (+ `USER_ROLE SB` при наличии файла).

Артефакт: **`SPOD_PARAMS_CATALOG.xlsx`**.

## Сборка

```bash
# CSV положить в IN/SPOD_UPLOAD/ (или указать другой каталог)
python src/Tools/build_spod_params_excel.py
python src/Tools/build_spod_params_excel.py \
  --input-dir IN/SPOD_UPLOAD \
  --out Docs/params_catalog/SPOD_PARAMS_CATALOG.xlsx
```

Скрипт обходит **все строки** каждого CSV, нормализует SPOD-JSON (`"""` → `"`), разворачивает ключи, подтягивает описания из `src/Tools/catalog_glossary/` и правила из `config/CONFIG_CHECKS.json`.

## Колонки листа PARAMETERS

| Колонка | Смысл |
|---------|--------|
| Название таблицы | Лист SPOD |
| ТИП колонки | `JSON` или `-` |
| Тип данных | строка / число / дата / массив со строками и т.д. |
| Путь до ключа JSON | `-` для не-JSON и для ключей в корне JSON; иначе путь родителя |
| Наименование | имя колонки или ключа |
| Зависимости | по `REWARD_TYPE` / `CONTEST_TYPE` и др. |
| Описание | глоссарий или гипотеза |
| Идентификатор (EN) | уникальный id |
| Признак дублей | одноимённые поля в других таблицах |
| Примеры 1–3 | разные значения из данных |
| Условия консистентности | все правила из CONFIG_CHECKS, где поле участвует |

Доп. листы: `META`, `TABLES`.
