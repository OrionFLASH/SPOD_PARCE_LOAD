# План: пункт 21 — consistency SPOD-JSON (обёртка `"` и массивы)

**Статус:** сделано  
**Связано:** ROADMAP п. **21**; продолжение волны **20** (web-fill pack → те же правила в `main.py`).  
**Режимы:** все, где выполняется `consistency_checks` (`main_only`, `consistency_only`, полный `run_outputs`).

## Цель

1. Внешняя обёртка JSON-ячейки — только двойные кавычки `"…"`, не одинарные `'…'`.
2. Ключи `helpCodeList` и `seasonItem` при наличии — всегда массив `[]` (не скаляр).

## Реализация

| Что | Где |
|-----|-----|
| Проверка обёртки + `array_value_keys` | `src/json_spod_format_check.py` |
| Правила | `config/CONFIG_CHECKS.json` → `spod_json_contest_data`, `spod_json_reward_add_data` |
| Формат | `Docs/CONSISTENCY_CHECKS_FORMAT.md` п. **2.8** |
| Тесты | `src/Tests/test_json_spod_array_and_wrap.py` |

ToDo: [`TODO_CONSISTENCY_JSON_WRAP_ARRAYS.md`](TODO_CONSISTENCY_JSON_WRAP_ARRAYS.md).
