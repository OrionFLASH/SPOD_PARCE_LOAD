# Исправление логики формирования GROUP_CODE и GROUP_VALUE в Summary

## Дата исправления
2025-12-25

## Проблема

Для кодов `t_01_2025-0_03-2-3_1001` и `01_2025-0_03-2-3`:
- В таблице GROUP было 2 строки
- В Summary попадало больше строк и не те значения
- GROUP_CODE имел 2 значения (bank и tb), но в Summary попадал только tb

## Причина проблемы

**Неправильная логика декартова произведения:**

В этапе 1 (для каждого CONTEST_CODE) использовалась неправильная логика:
```python
groups_ = groups_df["GROUP_CODE"].dropna().unique()  # ['bank', 'tb']
group_values_ = groups_df["GROUP_VALUE"].dropna().unique()  # ['V1', 'V2']

# Создавалось декартово произведение:
for g in groups_:  # bank, tb
    for gv in group_values_:  # V1, V2
        # Создавались ВСЕ комбинации:
        # (bank, V1) ✅ правильно
        # (bank, V2) ❌ неправильно!
        # (tb, V1)   ❌ неправильно!
        # (tb, V2)   ✅ правильно
```

**Проблема:** GROUP_VALUE должен быть связан с конкретным GROUP_CODE, а не со всеми GROUP_CODE.

## Решение

**Использование реальных пар (GROUP_CODE, GROUP_VALUE) из таблицы:**

```python
# Создаем список уникальных пар (GROUP_CODE, GROUP_VALUE) из реальных строк
group_code_value_pairs = []
if not groups_df.empty:
    for _, row in groups_df.iterrows():
        g_code = row.get("GROUP_CODE", "")
        g_value = row.get("GROUP_VALUE", "")
        if pd.notna(g_code) and pd.notna(g_value):
            pair = (str(g_code), str(g_value))
            if pair not in group_code_value_pairs:
                group_code_value_pairs.append(pair)

# Используем только реальные пары
for g_code, g_value in group_code_value_pairs:
    # Создаются только правильные комбинации
```

## Внесенные изменения

### 1. Этап 1: По CONTEST_CODE
- ✅ Исправлена логика: используются пары (GROUP_CODE, GROUP_VALUE) из реальных строк
- ✅ Добавлено подробное логирование для отладки

### 2. Этап 2: По TOURNAMENT_CODE
- ✅ Исправлена логика: используются пары (GROUP_CODE, GROUP_VALUE)

### 3. Этап 3: По REWARD_CODE
- ✅ Исправлена логика: используются пары (GROUP_CODE, GROUP_VALUE)

### 4. Этап 4: По GROUP_CODE
- ✅ Логика уже была правильной (берет GROUP_VALUE для конкретного GROUP_CODE)
- ✅ Добавлено подробное логирование для отладки

### 5. Этап 5: По INDICATOR_ADD_CALC_TYPE
- ✅ Исправлена логика: используются пары (GROUP_CODE, GROUP_VALUE)

## Добавленное логирование

Для кодов `t_01_2025-0_03-2-3_1001` и `01_2025-0_03-2-3` добавлено подробное логирование:

1. **На этапе обработки CONTEST_CODE:**
   - Количество найденных строк в GROUP
   - Содержимое строк GROUP (GROUP_CODE, GROUP_VALUE, CONTEST_CODE)
   - Уникальные пары (GROUP_CODE, GROUP_VALUE)
   - Уникальные GROUP_CODE и GROUP_VALUE отдельно
   - Количество создаваемых комбинаций
   - Каждая создаваемая строка

2. **На этапе обработки GROUP_CODE:**
   - Количество найденных строк в GROUP для конкретного GROUP_CODE
   - Содержимое строк GROUP
   - Уникальные GROUP_VALUE для GROUP_CODE
   - Количество создаваемых комбинаций
   - Каждая создаваемая строка

3. **В конце функции:**
   - Итоговые строки в Summary для указанных CONTEST_CODE
   - Уникальные GROUP_CODE и GROUP_VALUE
   - Все комбинации (GROUP_CODE, GROUP_VALUE)

## Пример исправления

### До исправления:

**Таблица GROUP:**
```
CONTEST_CODE | GROUP_CODE | GROUP_VALUE
C1           | bank       | V1
C1           | tb         | V2
```

**Результат в Summary (неправильно):**
- (C1, T1, R1, bank, V1) ✅
- (C1, T1, R1, bank, V2) ❌ (неправильная комбинация)
- (C1, T1, R1, tb, V1)   ❌ (неправильная комбинация)
- (C1, T1, R1, tb, V2)   ✅

### После исправления:

**Таблица GROUP:**
```
CONTEST_CODE | GROUP_CODE | GROUP_VALUE
C1           | bank       | V1
C1           | tb         | V2
```

**Результат в Summary (правильно):**
- (C1, T1, R1, bank, V1) ✅
- (C1, T1, R1, tb, V2)   ✅

## Тестирование

При запуске программы для указанных кодов в логе будет подробная информация:
- `[DEBUG GROUP]` - все этапы обработки GROUP
- Детальная информация о найденных строках
- Все создаваемые комбинации
- Итоговые строки в Summary

Рекомендуется:
1. Запустить программу
2. Проверить логи с меткой `[DEBUG GROUP]`
3. Убедиться, что в Summary попадают только правильные комбинации (GROUP_CODE, GROUP_VALUE)
4. Проверить, что все GROUP_CODE (bank и tb) присутствуют в Summary
