# 🔧 Исправление проблемы с pandas

## ❌ Проблема
```
ModuleNotFoundError: No module named 'pandas'
```

## ✅ Решение

### Вариант 1: Автоматический скрипт
```bash
# В вашей консоли выполните:
./activate_env.sh
```

### Вариант 2: Ручная активация
```bash
# 1. Убедитесь, что вы в правильной директории:
pwd
# Должно показать: /Users/orionflash/Desktop/MyProject/SPOD_PROM/SPOD-cursor-Parce/SPOD_PARCE_LOAD

# 2. Активируйте виртуальное окружение:
source venv/bin/activate

# 3. Проверьте активацию:
which python
# Должно показать путь к venv/bin/python

# 4. Проверьте pandas:
python -c "import pandas; print('pandas OK')"
```

### Вариант 3: Если ничего не помогает
```bash
# Переустановите пакеты в виртуальное окружение:
source venv/bin/activate
pip install --upgrade pandas openpyxl
```

## ✅ После исправления

Запустите программу:
```bash
python main.py
```

## 🔍 Диагностика

Если проблема остается, проверьте:

1. **Правильная директория:**
   ```bash
   ls -la venv/
   ```

2. **Активация окружения:**
   ```bash
   echo $VIRTUAL_ENV
   # Должно показать путь к venv
   ```

3. **Установленные пакеты:**
   ```bash
   pip list | grep pandas
   ```
