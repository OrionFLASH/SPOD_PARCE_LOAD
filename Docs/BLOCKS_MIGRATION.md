# Миграция каталогов блоков (PROM / IFT / PSI)

После версии **1.7.52** раскладка входа и архива такая:

## Вход

Было: `IN/SPOD/<BLOCK>/`, `IN/FILE/`, `IN/JS/`  
Стало:

```text
IN/
  PROM/
    SPOD/
    FILE/
    POST/
    JS/
  IFT/
    …
  PSI/
    …
```

Перенос (пример для PROM):

```bash
mkdir -p IN/PROM/SPOD IN/PROM/FILE IN/PROM/JS IN/PROM/POST
# CSV справочников SPOD:
mv IN/SPOD/PROM/* IN/PROM/SPOD/   # или из IN/SPOD/*, если ещё плоский каталог
# Gamification / FILE:
cp -a IN/FILE/. IN/PROM/FILE/     # при необходимости повторить для IFT/PSI
# JS шаблоны:
cp -a IN/JS/. IN/PROM/JS/
```

То же для `IFT` и `PSI` при использовании этих блоков.

## Архив SQLite

Было: `OUT/DB/spod_input_archive_v2.sqlite` (общий)  
Стало: `OUT/DB/<BLOCK>/spod_input_archive_<BLOCK>_v2.sqlite`

Старую общую БД можно оставить как справочный снимок; новый ingest пишет уже в каталог блока. При необходимости перенесите файл вручную и переименуйте.

## Конфиг

- `run_outputs`: объект `{ "PROM": [...], "IFT": [...], "PSI": [...] }` (или плоский список на все блоки).
- `run_blocks_parallel`: `false` по умолчанию; `true` — параллельный прогон.
- `subdir` в `input_files.<BLOCK>[]`: `PROM/SPOD`, `PROM/FILE`, …
- `archive_db_path` / `db_path`: шаблоны с `{BLOCK}`.
