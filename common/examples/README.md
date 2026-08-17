# Примеры JSON (`common/examples/`)

Сюда кладутся **готовые примеры** для импорта, демо и сверки — не рабочие каталоги и не сохранения из UI.

## Куда что

| Папка | Смысл |
|-------|--------|
| `web-fill/curated/` | Короткие снимки fill: награды, турниры, смесь, JSON-массивы |
| `web-fill/badges/` | Полные снимки конкурсов с `REWARD_TYPE=BADGE` (все / с периодом 2026) |
| `web-fill/contests/` | Полный снимок **всех** конкурсов PROM SPOD |

Новый пример JSON в `common/` — в подпапку по задаче (fill / edit / …) и смыслу (curated / полный дамп / тема). Имена файлов — понятные (`spod_fill_…`, не `data.json`).

## Сюда не класть

Это **не** примеры, они остаются рядом с приложениями:

- каталоги edit/fill: `catalog.json`, `game_edit_catalog.json`, `param_catalog_review/catalog.json`;
- датированные снимки каталога (`game_edit_catalog_YYYYMMDD_HHMM.json`);
- файлы кнопки **«Сохранить JSON»** в edit и fill (`spod_fill_YYYYMMDDHHMM.json` и аналоги).

Выгрузки «один CSV → один JSON» для документации полей — отдельно: **`Docs/JSON/examples/`**.

## Fill: импорт

В fill / fill-full: **«Импорт JSON»** → выбрать файл из папки ниже.

| Файл | Содержимое |
|------|------------|
| `web-fill/curated/spod_fill_example_rewards.json` | 4 индивидуальных накопительных |
| `web-fill/curated/spod_fill_example_tournaments.json` | 4 турнира |
| `web-fill/curated/spod_fill_example_mixed.json` | Те же 8 конкурсов в одном снимке |
| `web-fill/curated/spod_fill_example_json_arrays.json` | Шаблоны `CONTEST_PERIOD` / `FILTER_PERIOD_ARR` / `INDICATOR_FILTER` |
| `web-fill/badges/spod_fill_all_badges.json` | Все конкурсы со связью на BADGE |
| `web-fill/badges/spod_fill_badges_schedule_2026.json` | BADGE и период SCHEDULE с `START_DT`, содержащим `2026` |
| `web-fill/contests/spod_fill_all_contests.json` | Все конкурсы PROM SPOD из файлов `CONFIG_RUN_INPUT.json` |

Пересборка из CSV PROM SPOD:

```bash
python src/Tools/export_web_fill_examples_from_spod.py
```
