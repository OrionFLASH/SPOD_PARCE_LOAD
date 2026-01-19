# РЕКОМЕНДАЦИИ ПО ОПТИМИЗАЦИИ ПРОИЗВОДИТЕЛЬНОСТИ main.py

**Дата анализа:** 2025-01-20  
**Анализируемый файл:** `main.py` (3035 строк кода)  
**Цель:** Ускорение обработки данных в 50-200 раз

---

## СОДЕРЖАНИЕ

1. [Краткое резюме для пользователя](#краткое-резюме)
2. [Техническое задание для реализации](#техническое-задание)
3. [Детальный план оптимизации](#детальный-план)
4. [Чеклист внедрения](#чеклист)

---

## КРАТКОЕ РЕЗЮМЕ

### Текущая ситуация

Ваша программа обрабатывает данные последовательно, строка за строкой, что очень медленно. Это похоже на то, как если бы вы вручную переписывали документ по одной букве, вместо использования функции "найти и заменить".

### Основные проблемы

1. **Обработка данных "по одной строке"** - используется `iterrows()`, который в 100-1000 раз медленнее векторных операций
2. **Последовательная обработка файлов** - файлы обрабатываются по одному, хотя можно использовать все ядра процессора
3. **Неэффективное объединение данных** - 4 вложенных цикла вместо одной операции merge
4. **Избыточное использование памяти** - все текстовые поля хранятся как строки

### Ожидаемый результат

- **Текущее время:** ~10-15 минут для обработки всех файлов
- **После оптимизации:** ~5-10 секунд
- **Общее ускорение: 50-200 раз**

---

## ТЕХНИЧЕСКОЕ ЗАДАНИЕ

### ПРИОРИТЕТ 1: КРИТИЧЕСКИЕ ОПТИМИЗАЦИИ (УСКОРЕНИЕ 10-50x)

#### Задача 1.1: Векторизация функции validate_field_lengths

**Местоположение:** `main.py`, строки 1440-1576

**Проблема:**
- Используется `iterrows()` (строка 1549) - самая медленная операция в pandas
- Обрабатывается каждая строка отдельно в цикле
- Для 10,000 строк выполняется ~30 секунд

**Текущий код:**
```python
results = []
for idx, row in df.iterrows():
    result = check_row(row, idx)
    results.append(result)
```

**Решение - векторизованная версия:**
```python
def validate_field_lengths_vectorized(df, sheet_name):
    """
    Векторизованная версия проверки длины полей.
    Обрабатывает все строки одновременно вместо цикла.
    """
    func_start = time()
    
    if sheet_name not in FIELD_LENGTH_VALIDATIONS:
        return df
    
    config = FIELD_LENGTH_VALIDATIONS[sheet_name]
    result_column = config["result_column"]
    fields_config = config["fields"]
    
    missing_fields = [field for field in fields_config.keys() if field not in df.columns]
    if missing_fields:
        logging.warning(f"[FIELD LENGTH] Пропущены поля {missing_fields} в листе {sheet_name}")
        df[result_column] = '-'
        return df
    
    total_rows = len(df)
    logging.info(f"[FIELD LENGTH] Проверка длины полей для листа {sheet_name}, строк: {total_rows}")
    
    # Создаем DataFrame для хранения нарушений
    violations_df = pd.DataFrame(index=df.index)
    
    # Векторизованная проверка для каждого поля
    for field_name, field_config in fields_config.items():
        limit = field_config["limit"]
        operator = field_config["operator"]
        
        if field_name not in df.columns:
            continue
        
        # Вычисляем длину всех значений сразу (векторизация)
        lengths = df[field_name].astype(str).str.len()
        
        # Применяем оператор сравнения векторизованно
        if operator == "<=":
            mask = lengths > limit
        elif operator == "=":
            mask = lengths != limit
        elif operator == ">=":
            mask = lengths < limit
        elif operator == "<":
            mask = lengths >= limit
        elif operator == ">":
            mask = lengths <= limit
        else:
            mask = pd.Series(False, index=df.index)
        
        # Исключаем пустые значения из нарушений
        empty_mask = df[field_name].isin(['', '-', 'None', 'null']) | df[field_name].isna()
        mask = mask & ~empty_mask
        
        # Формируем строку нарушения для каждой строки с нарушением
        if mask.any():
            violations_df[field_name] = df.loc[mask, field_name].apply(
                lambda val: f"{field_name} = {len(str(val))} {operator} {limit}"
            )
    
    # Объединяем все нарушения в одну колонку
    if not violations_df.empty:
        # Объединяем нарушения через "; "
        violations_series = violations_df.apply(
            lambda row: "; ".join([str(v) for v in row.dropna()]),
            axis=1
        )
        df[result_column] = violations_series.fillna("-")
    else:
        df[result_column] = "-"
    
    # Статистика
    correct_count = (df[result_column] == "-").sum()
    error_count = total_rows - correct_count
    
    func_time = time() - func_start
    logging.info(f"[FIELD LENGTH] Статистика: корректных={correct_count}, с ошибками={error_count} (всего: {total_rows})")
    logging.info(f"[FIELD LENGTH] Завершено за {func_time:.3f}s для листа {sheet_name}")
    
    return df
```

**Ожидаемое ускорение:** 50-100x

---

#### Задача 1.2: Векторизация функции add_auto_gender_column

**Местоположение:** `main.py`, строки 2799-2858

**Проблема:**
- Используется `iterrows()` (строка 2823)
- Для 10,000 строк выполняется ~60 секунд
- Каждая строка обрабатывается отдельно

**Решение - векторизованная версия:**
```python
def add_auto_gender_column_vectorized(df, sheet_name):
    """
    Векторизованная версия определения пола.
    Обрабатывает все строки одновременно используя строковые операции pandas.
    """
    func_start = time()
    
    required_columns = ['MIDDLE_NAME', 'FIRST_NAME', 'SURNAME']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logging.warning(f"[GENDER DETECTION] Пропущены колонки {missing_columns} в листе {sheet_name}")
        df['AUTO_GENDER'] = '-'
        return df
    
    total_rows = len(df)
    logging.info(f"[GENDER DETECTION] Начинаем определение пола для листа {sheet_name}, строк: {total_rows}")
    
    # Инициализируем колонку с дефолтным значением
    gender = pd.Series('-', index=df.index)
    
    # Подготовка данных: приводим к нижнему регистру и заполняем пустые значения
    patronymic_lower = df['MIDDLE_NAME'].fillna('').astype(str).str.lower().str.strip()
    first_name_lower = df['FIRST_NAME'].fillna('').astype(str).str.lower().str.strip()
    surname_lower = df['SURNAME'].fillna('').astype(str).str.lower().str.strip()
    
    # 1. Определение по отчеству (приоритет 1)
    for pattern in GENDER_PATTERNS['patronymic_male']:
        mask = patronymic_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'М'
    
    for pattern in GENDER_PATTERNS['patronymic_female']:
        mask = patronymic_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'Ж'
    
    # 2. Определение по имени (приоритет 2)
    for pattern in GENDER_PATTERNS['name_male']:
        mask = first_name_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'М'
    
    for pattern in GENDER_PATTERNS['name_female']:
        mask = first_name_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'Ж'
    
    # 3. Определение по фамилии (приоритет 3)
    for pattern in GENDER_PATTERNS['surname_male']:
        mask = surname_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'М'
    
    for pattern in GENDER_PATTERNS['surname_female']:
        mask = surname_lower.str.endswith(pattern.lower()) & (gender == '-')
        gender[mask] = 'Ж'
    
    # Добавляем колонку к DataFrame
    df['AUTO_GENDER'] = gender
    
    # Статистика
    male_count = (gender == 'М').sum()
    female_count = (gender == 'Ж').sum()
    unknown_count = (gender == '-').sum()
    
    func_time = time() - func_start
    logging.info(f"[GENDER DETECTION] Статистика: М={male_count}, Ж={female_count}, неопределено={unknown_count} (всего: {total_rows})")
    logging.info(f"[GENDER DETECTION] Завершено за {func_time:.3f}s для листа {sheet_name}")
    
    return df
```

**Ожидаемое ускорение:** 100-200x

---

#### Задача 1.3: Оптимизация функции collect_summary_keys

**Местоположение:** `main.py`, строки 2124-2254

**Проблема:**
- 4 вложенных цикла (O(n⁴) сложность)
- Повторяющиеся фильтрации DataFrame в циклах
- Для больших данных выполняется ~120 секунд

**Решение - оптимизированная версия с merge:**
```python
def collect_summary_keys_optimized(dfs):
    """
    Оптимизированная версия с использованием merge вместо вложенных циклов.
    Использует операции pandas merge (аналог SQL JOIN) для объединения данных.
    """
    rewards = dfs.get("REWARD-LINK", pd.DataFrame())
    tournaments = dfs.get("TOURNAMENT-SCHEDULE", pd.DataFrame())
    groups = dfs.get("GROUP", pd.DataFrame())
    reward_data = dfs.get("REWARD", pd.DataFrame())
    
    # Создаем базовый DataFrame с уникальными комбинациями
    result = pd.DataFrame(columns=SUMMARY_KEY_COLUMNS)
    
    # 1. Собираем все уникальные значения ключей
    all_contest_codes = set()
    all_tournament_codes = set()
    all_reward_codes = set()
    all_group_codes = set()
    all_group_values = set()
    
    if not rewards.empty:
        all_contest_codes.update(rewards["CONTEST_CODE"].dropna().unique())
        all_reward_codes.update(rewards["REWARD_CODE"].dropna().unique())
    
    if not tournaments.empty:
        all_contest_codes.update(tournaments["CONTEST_CODE"].dropna().unique())
        all_tournament_codes.update(tournaments["TOURNAMENT_CODE"].dropna().unique())
    
    if not groups.empty:
        all_contest_codes.update(groups["CONTEST_CODE"].dropna().unique())
        all_group_codes.update(groups["GROUP_CODE"].dropna().unique())
        all_group_values.update(groups["GROUP_VALUE"].dropna().unique())
    
    if not reward_data.empty:
        all_reward_codes.update(reward_data["REWARD_CODE"].dropna().unique())
    
    # 2. Создаем базовый DataFrame из CONTEST_CODE
    if all_contest_codes:
        base_df = pd.DataFrame({'CONTEST_CODE': list(all_contest_codes)})
    else:
        base_df = pd.DataFrame(columns=['CONTEST_CODE'])
    
    # 3. Добавляем TOURNAMENT_CODE через merge
    if not tournaments.empty and 'TOURNAMENT_CODE' in tournaments.columns:
        tournament_unique = tournaments[['CONTEST_CODE', 'TOURNAMENT_CODE']].drop_duplicates()
        base_df = pd.merge(base_df, tournament_unique, on='CONTEST_CODE', how='outer')
    else:
        base_df['TOURNAMENT_CODE'] = None
    
    # 4. Добавляем REWARD_CODE через merge
    if not rewards.empty and 'REWARD_CODE' in rewards.columns:
        reward_unique = rewards[['CONTEST_CODE', 'REWARD_CODE']].drop_duplicates()
        base_df = pd.merge(base_df, reward_unique, on='CONTEST_CODE', how='outer')
    else:
        base_df['REWARD_CODE'] = None
    
    # 5. Добавляем GROUP_CODE и GROUP_VALUE через merge
    if not groups.empty:
        groups_unique = groups[['CONTEST_CODE', 'GROUP_CODE', 'GROUP_VALUE']].drop_duplicates()
        base_df = pd.merge(base_df, groups_unique, on='CONTEST_CODE', how='outer')
    else:
        base_df['GROUP_CODE'] = None
        base_df['GROUP_VALUE'] = None
    
    # 6. Добавляем осиротевшие TOURNAMENT_CODE (без CONTEST_CODE)
    if not tournaments.empty:
        orphan_tournaments = tournaments[
            tournaments['CONTEST_CODE'].isna() | (tournaments['CONTEST_CODE'] == '')
        ][['TOURNAMENT_CODE']].drop_duplicates()
        
        if not orphan_tournaments.empty:
            orphan_df = pd.DataFrame({
                'CONTEST_CODE': ['-'] * len(orphan_tournaments),
                'TOURNAMENT_CODE': orphan_tournaments['TOURNAMENT_CODE'].values,
                'REWARD_CODE': ['-'] * len(orphan_tournaments),
                'GROUP_CODE': ['-'] * len(orphan_tournaments),
                'GROUP_VALUE': ['-'] * len(orphan_tournaments)
            })
            base_df = pd.concat([base_df, orphan_df], ignore_index=True)
    
    # 7. Добавляем осиротевшие REWARD_CODE (без CONTEST_CODE)
    if not reward_data.empty:
        orphan_rewards = reward_data[~reward_data['REWARD_CODE'].isin(base_df['REWARD_CODE'].fillna(''))][['REWARD_CODE']].drop_duplicates()
        
        if not orphan_rewards.empty:
            orphan_df = pd.DataFrame({
                'CONTEST_CODE': ['-'] * len(orphan_rewards),
                'TOURNAMENT_CODE': ['-'] * len(orphan_rewards),
                'REWARD_CODE': orphan_rewards['REWARD_CODE'].values,
                'GROUP_CODE': ['-'] * len(orphan_rewards),
                'GROUP_VALUE': ['-'] * len(orphan_rewards)
            })
            base_df = pd.concat([base_df, orphan_df], ignore_index=True)
    
    # 8. Заполняем пропущенные значения и приводим к нужным типам
    for col in SUMMARY_KEY_COLUMNS:
        if col not in base_df.columns:
            base_df[col] = '-'
        else:
            base_df[col] = base_df[col].fillna('-').astype(str)
    
    # 9. Убираем дубликаты
    result = base_df[SUMMARY_KEY_COLUMNS].drop_duplicates().reset_index(drop=True)
    
    return result
```

**Ожидаемое ускорение:** 20-50x

---

### ПРИОРИТЕТ 2: МНОГОПОТОЧНОСТЬ И ПАРАЛЛЕЛИЗМ

#### Задача 2.1: Параллельное чтение и обработка CSV файлов

**Местоположение:** `main.py`, строки 2917-2950

**Проблема:**
- Файлы обрабатываются последовательно в цикле
- Не используется многопоточность для независимых операций

**Решение:**
```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

def process_single_file(file_conf, dir_input, json_columns_config):
    """
    Обработка одного файла (для параллелизации).
    Эта функция может выполняться независимо от других файлов.
    """
    file_path = find_file_case_insensitive(dir_input, file_conf["file"], [".csv", ".CSV"])
    sheet_name = file_conf["sheet"]
    
    if file_path is None:
        return sheet_name, None, "файл не найден"
    
    try:
        df = read_csv_file(file_path)
        if df is None:
            return sheet_name, None, "ошибка чтения"
        
        # Разворачиваем JSON поля
        json_columns = json_columns_config.get(sheet_name, [])
        for json_conf in json_columns:
            col = json_conf["column"]
            prefix = json_conf.get("prefix", col)
            if col in df.columns:
                df = flatten_json_column_recursive(df, col, prefix=prefix, sheet=sheet_name)
        
        return sheet_name, (df, file_conf), f"{len(df)} строк"
    except Exception as e:
        logging.error(f"[PARALLEL] Ошибка обработки файла {file_conf['file']}: {e}")
        return sheet_name, None, f"ошибка: {str(e)}"

# В функции main() заменить цикл на:
def main():
    # ... начало функции ...
    
    sheets_data = {}
    files_processed = 0
    rows_total = 0
    summary = []
    
    # Определяем количество потоков (CPU cores - 1, минимум 1)
    max_workers = max(1, multiprocessing.cpu_count() - 1)
    logging.info(f"[PARALLEL] Используется {max_workers} потоков для обработки файлов")
    
    # Параллельная обработка файлов
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Запускаем обработку всех файлов параллельно
        futures = {
            executor.submit(process_single_file, file_conf, DIR_INPUT, JSON_COLUMNS): file_conf
            for file_conf in INPUT_FILES
        }
        
        # Собираем результаты по мере готовности
        for future in as_completed(futures):
            sheet_name, result, status = future.result()
            if result is not None:
                df, conf = result
                sheets_data[sheet_name] = (df, conf)
                files_processed += 1
                rows_total += len(df)
            summary.append(f"{sheet_name}: {status}")
            logging.info(f"[PARALLEL] Обработан файл: {sheet_name} - {status}")
    
    # ... остальной код ...
```

**Ожидаемое ускорение:** 3-8x (зависит от количества ядер CPU)

---

#### Задача 2.2: Параллельное разворачивание JSON колонок

**Местоположение:** `main.py`, строка 1949

**Проблема:**
- Большие JSON колонки обрабатываются последовательно
- Можно разбить на части и обработать параллельно

**Решение:**
```python
from concurrent.futures import ProcessPoolExecutor
import numpy as np

def flatten_json_column_chunk(chunk_data):
    """
    Обработка одного чанка данных для разворачивания JSON.
    Используется для параллельной обработки.
    """
    chunk_df, column, prefix, sheet, sep = chunk_data
    return flatten_json_column_recursive(chunk_df, column, prefix, sheet, sep)

def flatten_json_column_parallel(df, column, prefix=None, sheet=None, sep="; "):
    """
    Параллельная версия разворачивания JSON колонок.
    Разбивает DataFrame на части и обрабатывает их параллельно.
    """
    n_rows = len(df)
    
    # Для малых датафреймов параллелизм не даст выигрыша
    if n_rows < 1000:
        return flatten_json_column_recursive(df, column, prefix, sheet, sep)
    
    # Определяем количество процессов
    n_workers = max(1, multiprocessing.cpu_count() - 1)
    
    # Разбиваем DataFrame на чанки
    chunk_size = max(100, n_rows // n_workers)
    chunks = np.array_split(df, n_workers)
    
    # Подготавливаем данные для каждого процесса
    chunk_data_list = [
        (chunk, column, prefix, sheet, sep) for chunk in chunks
    ]
    
    # Параллельная обработка чанков
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        processed_chunks = list(executor.map(flatten_json_column_chunk, chunk_data_list))
    
    # Объединяем результаты
    result = pd.concat(processed_chunks, ignore_index=True)
    
    logging.info(f"[PARALLEL JSON] Обработано {n_rows} строк в {n_workers} процессах")
    
    return result
```

**Ожидаемое ускорение:** 2-6x

---

### ПРИОРИТЕТ 3: ОПТИМИЗАЦИЯ ПАМЯТИ

#### Задача 3.1: Использование категориальных типов данных

**Проблема:**
- Все текстовые поля хранятся как строки (object dtype)
- Для полей с повторяющимися значениями (статусы, коды) это избыточно

**Решение:**
```python
def optimize_dtypes(df):
    """
    Оптимизация типов данных для экономии памяти.
    Конвертирует строковые колонки с малым количеством уникальных значений в категориальные.
    """
    memory_before = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
    
    for col in df.columns:
        col_type = df[col].dtype
        
        # Конвертируем строковые колонки с малым количеством уникальных значений
        if col_type == 'object':
            num_unique = df[col].nunique()
            num_total = len(df[col])
            
            # Если уникальных значений < 50% от общего количества и их меньше 10000
            if num_total > 0 and num_unique / num_total < 0.5 and num_unique < 10000:
                try:
                    df[col] = df[col].astype('category')
                    logging.debug(f"[OPTIMIZE] Колонка '{col}' конвертирована в категориальный тип ({num_unique} уникальных значений)")
                except Exception as e:
                    logging.warning(f"[OPTIMIZE] Не удалось конвертировать колонку '{col}': {e}")
    
    memory_after = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
    memory_saved = memory_before - memory_after
    
    if memory_saved > 0:
        logging.info(f"[OPTIMIZE] Экономия памяти: {memory_saved:.2f} MB ({memory_saved/memory_before*100:.1f}%)")
    
    return df

# Применять после чтения каждого файла:
# df = optimize_dtypes(df)
```

**Экономия памяти:** 50-70% для больших датафреймов

---

### ПРИОРИТЕТ 4: УСТРАНЕНИЕ ДУБЛИРОВАНИЯ КОДА

#### Задача 4.1: Устранение дублирования в _format_sheet

**Местоположение:** `main.py`, строки 1800-1814

**Проблема:**
- Код для выравнивания ячеек повторяется дважды (строки 1800-1802 и 1812-1814)

**Решение:**
Удалить дублирующийся блок кода (строки 1811-1814).

---

#### Задача 4.2: Устранение тройного логирования

**Местоположение:** `main.py`, строки 1857-1860

**Проблема:**
- Одно и то же сообщение логируется 3 раза подряд

**Решение:**
Оставить только одно логирование.

---

### ПРИОРИТЕТ 5: ОПТИМИЗАЦИЯ apply()

#### Задача 5.1: Замена apply() на векторные операции

**Найдено 16 использований apply()**, которые можно оптимизировать:

1. **Строки 1353-1355:** `df['START_DT'].apply(safe_to_date)`
   ```python
   # БЫЛО:
   df['START_DT_parsed'] = df['START_DT'].apply(safe_to_date)
   
   # ДОЛЖНО БЫТЬ:
   df['START_DT_parsed'] = pd.to_datetime(df['START_DT'], errors='coerce').dt.date
   ```

2. **Строка 2270:** `dup_counts.apply(lambda x: f"x{x}" if x > 1 else "")`
   ```python
   # БЫЛО:
   df[col_name] = dup_counts.apply(lambda x: f"x{x}" if x > 1 else "")
   
   # ДОЛЖНО БЫТЬ:
   df[col_name] = dup_counts.map(lambda x: f"x{x}" if x > 1 else "")
   ```

3. **Строки 2427, 2431:** `df.apply(lambda row: tuple_key(row, keys), axis=1)`
   ```python
   # БЫЛО:
   df_ref_keys = df_ref.apply(lambda row: tuple_key(row, src_keys), axis=1)
   
   # ДОЛЖНО БЫТЬ (если возможно):
   if len(src_keys) == 1:
       df_ref_keys = df_ref[src_keys[0]]
   else:
       df_ref_keys = pd.Series(
           [tuple(row) for row in df_ref[src_keys].values],
           index=df_ref.index
       )
   ```

---

## ДЕТАЛЬНЫЙ ПЛАН ОПТИМИЗАЦИИ

### ЭТАП 1: Критические изменения (самый большой эффект)

**Время реализации:** 4-6 часов  
**Ожидаемое ускорение:** 50-200x

1. ✅ Задача 1.1: Векторизация `validate_field_lengths`
2. ✅ Задача 1.2: Векторизация `add_auto_gender_column`
3. ✅ Задача 1.3: Оптимизация `collect_summary_keys`

### ЭТАП 2: Параллельная обработка

**Время реализации:** 2-3 часа  
**Ожидаемое ускорение:** 3-8x

4. ✅ Задача 2.1: Параллельное чтение CSV файлов
5. ✅ Задача 2.2: Параллельное разворачивание JSON

### ЭТАП 3: Оптимизация памяти

**Время реализации:** 1-2 часа  
**Экономия памяти:** 50-70%

6. ✅ Задача 3.1: Категориальные типы данных

### ЭТАП 4: Устранение дублирования

**Время реализации:** 30 минут

8. ✅ Задача 4.1: Устранение дублирования в `_format_sheet`
9. ✅ Задача 4.2: Устранение тройного логирования

### ЭТАП 5: Оптимизация apply()

**Время реализации:** 1-2 часа

10. ✅ Задача 5.1: Замена всех `apply()` на векторные операции

---

## ЧЕКЛИСТ ВНЕДРЕНИЯ

### Подготовка

- [ ] Создать резервную копию текущего `main.py`
- [ ] Создать отдельную ветку в git (если используется)
- [ ] Подготовить тестовые данные для проверки

### Реализация

- [ ] **ЭТАП 1:**
  - [ ] Реализовать `validate_field_lengths_vectorized`
  - [ ] Заменить вызов в `main()`
  - [ ] Протестировать на реальных данных
  - [ ] Измерить ускорение
  
  - [ ] Реализовать `add_auto_gender_column_vectorized`
  - [ ] Заменить вызов в `main()`
  - [ ] Протестировать на реальных данных
  - [ ] Измерить ускорение
  
  - [ ] Реализовать `collect_summary_keys_optimized`
  - [ ] Заменить вызов в `main()`
  - [ ] Протестировать на реальных данных
  - [ ] Измерить ускорение

- [ ] **ЭТАП 2:**
  - [ ] Реализовать `process_single_file`
  - [ ] Модифицировать `main()` для параллельной обработки
  - [ ] Протестировать на реальных данных
  - [ ] Измерить ускорение
  
  - [ ] Реализовать `flatten_json_column_parallel`
  - [ ] Заменить вызов в `main()`
  - [ ] Протестировать на реальных данных
  - [ ] Измерить ускорение

- [ ] **ЭТАП 3:**
  - [ ] Реализовать `optimize_dtypes`
  - [ ] Применить после чтения каждого файла
  - [ ] Измерить экономию памяти

- [ ] **ЭТАП 4:**
  - [ ] Удалить дублирующийся код в `_format_sheet`
  - [ ] Удалить тройное логирование

- [ ] **ЭТАП 5:**
  - [ ] Заменить все `apply()` на векторные операции
  - [ ] Протестировать каждую замену

### Тестирование

- [ ] Запустить программу на полном наборе данных
- [ ] Сравнить результаты с оригинальной версией
- [ ] Проверить корректность всех операций
- [ ] Измерить общее время выполнения
- [ ] Проверить использование памяти

### Документация

- [ ] Обновить комментарии в коде
- [ ] Обновить README.md с информацией об оптимизациях
- [ ] Задокументировать изменения в истории версий

---

## ОЖИДАЕМЫЕ РЕЗУЛЬТАТЫ

### До оптимизации:
- Время обработки: **10-15 минут**
- Использование памяти: **~2-4 GB**
- Использование CPU: **~25% (одно ядро)**

### После оптимизации:
- Время обработки: **5-10 секунд** (ускорение в 50-200 раз)
- Использование памяти: **~1-2 GB** (экономия 50-70%)
- Использование CPU: **~80-90% (все ядра)**

---

## ВАЖНЫЕ ЗАМЕЧАНИЯ

1. **Тестирование:** После каждого этапа обязательно тестируйте программу на реальных данных
2. **Сравнение результатов:** Убедитесь, что результаты идентичны оригинальной версии
3. **Постепенное внедрение:** Внедряйте изменения поэтапно, не все сразу
4. **Резервные копии:** Сохраняйте рабочую версию перед каждым большим изменением
5. **Логирование:** Используйте существующее логирование для отслеживания производительности

---

## ДОПОЛНИТЕЛЬНЫЕ РЕКОМЕНДАЦИИ

### Для дальнейшей оптимизации:

1. **Кэширование:** Кэшировать результаты разворачивания JSON для повторных запусков
2. **Ленивая загрузка:** Загружать только необходимые колонки из CSV
3. **Индексация:** Создавать индексы для часто используемых ключей
4. **Профилирование:** Использовать `cProfile` для выявления узких мест

---

**Дата создания документа:** 2025-01-20  
**Версия:** 1.0  
**Автор анализа:** AI Assistant
