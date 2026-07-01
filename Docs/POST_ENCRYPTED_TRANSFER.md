# Зашифрованный снимок POST для пересылки по почте

Каталог **POST/** не версионируется (`.gitignore`). Для пересылки программы по почте с минимальным шифрованием содержимого используется отдельный пайплайн — в отличие от открытого снимка **`sync_post_txt.py`**.

## Отправитель

Из корня проекта:

```bash
python src/Tools/pack_post_encrypted_program.py
```

**Результат в `POST/`** (все файлы **в корне**, без подкаталогов):

| Файл | Назначение |
|------|------------|
| `*.txt` (зашифрованные) | Все `.py` (корень + `src/`), `config.json`, `README.md` |
| `bundle_manifest.txt` | Зашифрованный манифест «имя в POST → путь в проекте» |
| `pack_post_encrypted_program.py.txt` | Утилита упаковки (открытый текст) |
| `decrypt_post_program.py.txt` | Утилита расшифровки (открытый текст) |
| `КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt` | Карта: файл в POST → путь в OUT/POST |

**Имена в POST:** плоский список в корне **POST/**; суффикс **`.txt`**; путь кодируется через **`__`** (например `src/Tools/sync_post_txt.py` → `src__Tools__sync_post_txt.py.txt`); из имён убраны фрагменты вроде `_auto_js`. Структура подкаталогов восстанавливается только в **OUT/POST** при расшифровке.

**Шифрование:** `SPODENC1` + PBKDF2-SHA256 + XOR-поток (только stdlib). Идентификатор пароля: `SPOD_post_program_bundle_v1` (встроен в pack/decrypt).

Перешлите каталог **POST/** целиком по почте.

## Получатель

1. Создайте **`IN/POST/`** и положите туда **все** файлы из пересланного **POST/** (тоже плоским списком, без подкаталогов).
2. Убедитесь, что в корне проекта есть **`decrypt_post_program.py`** (из Git или переименуйте **`decrypt_post_program.py.txt`**, убрав `.txt`).
3. Из корня:

```bash
python decrypt_post_program.py
```

Опции: `--input IN/POST`, `--output OUT/POST`.

**Результат:** **`OUT/POST/`** — восстановленная структура (`main.py`, `config.json`, `README.md`, `src/...`) с исходными именами файлов.

## Связанные файлы в репозитории

- `src/Tools/post_transfer_crypto.py` — крипто и санитизация имён
- `src/Tools/pack_post_encrypted_program.py` — упаковка
- `decrypt_post_program.py` — расшифровка (корень проекта)
- `src/Tests/test_post_transfer_crypto.py` — тесты

## Открытый снимок (без шифрования)

Для полного снимка с **Docs/** и **requirements.txt** по-прежнему:

```bash
python src/Tools/sync_post_txt.py
```

См. **`Docs/POST_SNAPSHOT/`** и раздел **«Каталог POST»** в **README.md**.
