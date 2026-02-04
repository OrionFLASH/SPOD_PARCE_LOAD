# Исправление проблемы с GROUP_CODE и GROUP_VALUE в Summary

## Дата исправления
2025-12-25

## Проблема (найдена в логе)

Для CONTEST_CODE `01_2025-0_03-2-3`:
- В таблице GROUP есть только 2 строки: (BANK, *) и (TB, *)
- В Summary попадало 22 строки с неправильными GROUP_VALUE: [38], [40], [44] и т.д.
- GROUP_CODE BANK не попадал в Summary

## Причина проблемы

**В этапе 4 (по GROUP_CODE) была неправильная логика:**

```python
# СТАРАЯ (неправильная) логика:
code = groups[groups["GROUP_CODE"] == g_code]["CONTEST_CODE"].dropna().unique()
code = code[0]  # Берет ПЕРВЫЙ CONTEST_CODE для GROUP_CODE

# Потом берет ВСЕ GROUP_VALUE для этого GROUP_CODE из ВСЕХ CONTEST_CODE:
group_values_df = groups[groups["GROUP_CODE"] == g_code]  # ❌ БЕЗ фильтрации по CONTEST_CODE!
group_values_ = group_values_df["GROUP_VALUE"].dropna().unique()
```

**Проблема:** Для GROUP_CODE=TB находился первый CONTEST_CODE (например, `01_2025-0_03-2-3`), но затем брались ВСЕ GROUP_VALUE для TB из ВСЕХ CONTEST_CODE в таблице (85 строк!), включая значения [38], [40] и т.д. из других конкурсов.

## Решение

**Исправлена логика этапа 4:**

```python
# НОВАЯ (правильная) логика:
# Находим ВСЕ CONTEST_CODE для данного GROUP_CODE
group_contest_codes = groups[groups["GROUP_CODE"] == g_code]["CONTEST_CODE"].dropna().unique()

# Обрабатываем каждый CONTEST_CODE отдельно
for group_contest_code in group_contest_codes:
    # Берем GROUP_VALUE только для конкретного CONTEST_CODE и GROUP_CODE
    group_values_df = groups[(groups["GROUP_CODE"] == g_code) & 
                              (groups["CONTEST_CODE"] == actual_code)]  # ✅ С фильтрацией!
    group_values_ = group_values_df["GROUP_VALUE"].dropna().unique()
```

**Теперь:** Для каждого CONTEST_CODE берутся только те GROUP_VALUE, которые действительно существуют для этого CONTEST_CODE и GROUP_CODE.

## Результат исправления

### До исправления:
- Для GROUP_CODE=TB находился первый CONTEST_CODE
- Брались ВСЕ GROUP_VALUE для TB из всех CONTEST_CODE (85 строк)
- Создавались неправильные комбинации

### После исправления:
- Для GROUP_CODE=TB находятся ВСЕ CONTEST_CODE
- Для каждого CONTEST_CODE берутся только его GROUP_VALUE
- Создаются только правильные комбинации

## Пример

### Таблица GROUP:
```
CONTEST_CODE | GROUP_CODE | GROUP_VALUE
01_2025-0_03-2-3 | BANK | *
01_2025-0_03-2-3 | TB   | *
OTHER_CONTEST | TB | [38]
OTHER_CONTEST | TB | [40]
```

### До исправления (этап 4):
- Для GROUP_CODE=TB находился CONTEST_CODE=01_2025-0_03-2-3
- Брались GROUP_VALUE: [*, [38], [40]] ❌ (из всех CONTEST_CODE)
- Создавались неправильные строки

### После исправления (этап 4):
- Для GROUP_CODE=TB находятся CONTEST_CODE: [01_2025-0_03-2-3, OTHER_CONTEST]
- Для 01_2025-0_03-2-3: GROUP_VALUE = [*] ✅
- Для OTHER_CONTEST: GROUP_VALUE = [[38], [40]] ✅
- Создаются только правильные строки

## Проверка

После исправления в логах должно быть:
- Для GROUP_CODE=TB и CONTEST_CODE=01_2025-0_03-2-3: только GROUP_VALUE=*
- Не должно быть GROUP_VALUE=[38], [40] и т.д. для этого CONTEST_CODE
- GROUP_CODE=BANK должен присутствовать в Summary
