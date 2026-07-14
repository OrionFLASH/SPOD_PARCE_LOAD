# Политика каталогов IN/ и OUT/ (входные и выходные данные)

## Инцидент 2026-07-02

**Причина удаления `IN/` и `OUT/`:** при тестировании POST bundle вручную/агентом выполнялась shell-команда:

```bash
rm -rf IN OUT && mkdir -p IN/POST && cp POST/* IN/POST/
```

Она **не входит в основной пайплайн** `main.py`, но уничтожила:

| Каталог | Что потеряно |
|---------|----------------|
| `IN/PROM|IFT|PSI/{SPOD,FILE,JS,…}/` | исходные CSV, JSON, JS по средам |
| `OUT/<BLOCK>/YYYY/DD-MM/` | Excel, JS AutoRun, STAT_FILE, CONSISTENCY |
| `OUT/DB/<BLOCK>/` | SQLite-архив блока (`spod_input_archive_<BLOCK>_v2.sqlite` и др.) |

**Когда:** ~23:43 02.07.2026.  
**Последний полный прогон до инцидента:** `LOGS/2026/02-07/LOGS_INFO_20260702_00_27.log` (02.07.2026 00:27).

Каталоги **`IN/`** и **`OUT/`** в **`.gitignore`** — Git не хранит эти данные.

---

## Правило проекта (обязательное)

1. **Никогда** не очищать и не удалять деревья **`IN/`** и **`OUT/`** из кода, тестов и скриптов без **явного распоряжения пользователя**.
2. Тесты и вспомогательные сценарии — только в **`.work/`** (копии, не боевые каталоги).
3. **Запрещено** в shell/CI/агенте: `rm -rf IN`, `rm -rf OUT`, `rm -rf IN/*`, `rm -rf OUT/*`.
4. Резервные копии данных — отдельно (`BACKUP/`, архив, почта).

---

## Аудит кода: где есть удаление файлов/каталогов

### A. Затрагивает IN/ или OUT/ — изменено или требует вашего решения

| Место | Что делает | Было | Сейчас / рекомендация |
|-------|------------|------|------------------------|
| **Shell (агент, вне Git)** `rm -rf IN OUT` | Удаляет всё | **Причина инцидента** | **Запрещено.** Только `.work/` |
| `decrypt_post_program.py` | Перед расшифровкой очищал `--output` | Очищал `OUT/POST` целиком | **Исправлено:** только перезапись файлов из манифеста, без удаления соседних |
| `src/Tools/safe_post_decrypt_test.py` | Тест pack→decrypt | Использовал `IN/POST`, `OUT/POST` | **Исправлено:** только `.work/post_decrypt_test/` |
| `src/path_data_guard.py` | — | не было | **Добавлено:** проверка защищённых путей |

### B. Не трогает IN/OUT — можно оставить

| Место | Что делает | Зачем |
|-------|------------|-------|
| `src/Tools/pack_post_encrypted_program.py` | Обновление **POST/** | ~~`shutil.rmtree(POST)`~~ | **Исправлено:** перезапись + удаление только устаревших `.txt` в корне POST |
| `src/Tools/sync_post_txt.py` | Обновление **POST/** | ~~`shutil.rmtree(POST)`~~ в полном/program режимах | **Исправлено:** инкрементально; устаревшие — по манифесту `.sync_manifest.json` |
| `src/main_impl.py` | `os.makedirs(run_output_dir)` | **Создаёт** `OUT/<BLOCK>/YYYY/DD-MM/` (BLOCK из `run_blocks`), **не удаляет** старые |
| `src/input_archive_sqlite*.py` | Пишет в `OUT/DB/*.sqlite` | Дополняет БД, не чистит OUT |
| `src/Tests/*.py` | Нет rmtree IN/OUT | — |

### C. Основной пайплайн и OUT

`main.py` / `main_impl.py` **никогда не удаляют** `OUT/` — только добавляют файлы в `OUT/YYYY/DD-MM/`. Потеря OUT произошла **только** из-за `rm -rf OUT` вне программы.

---

## Восстановление данных

### IN/

| Каталог | Статус | Источник |
|---------|--------|----------|
| `IN/SPOD/` | восстановлено (11 CSV) | `Downloads/Attachments_…_2026-07-01_17-19-55.zip` |
| `IN/JS/` (эталоны) | восстановлено | `Generate_Script_manual/Script/` |
| `IN/FILE/` (26 CSV) | **нет копии на диске** | повторный экспорт gamification |
| `IN/JS/` (JSON) | **нет копии** | повторная выгрузка profiles / leadersForAdmin |

### OUT/

Локальных копий **xlsx/sqlite/js** из `OUT/` **не найдено** (поиск по диску, Trash, Time Machine).

**Что было в OUT (по логам):** подкаталоги `OUT/2026/02-07`, `10-04`, `12-06`, `13-05`, `13-06`, `14-06`, `18-04`, `22-04`, `22-05`, `27-04`, `29-06`, `31-05` — Excel `SPOD_PROM main_*.xlsx`, `MANAGER_STATS *.xlsx`, JS AutoRun, `OUT/DB/spod_input_archive_v2.sqlite`.

**Частичное восстановление:** после заполнения `IN/FILE/` запустить `python main.py` — создаст новый `OUT/YYYY/DD-MM/` (не восстановит старые даты автоматически).

Последний известный main-файл:  
`OUT/2026/02-07/SPOD_PROM main_2026-07-02_00-27-19.xlsx`

---

## Безопасный тест POST

```bash
python src/Tools/safe_post_decrypt_test.py
```

Рабочие каталоги: `.work/post_decrypt_test/IN_POST` и `.work/post_decrypt_test/OUT_POST`.

---

## См. также

- `Docs/POST_ENCRYPTED_TRANSFER.md` — пересылка POST
- `src/path_data_guard.py` — API защиты путей
