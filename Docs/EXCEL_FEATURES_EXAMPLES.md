# Примеры работы с Excel: выпадающие списки и формулы

## Дата создания
2025-12-25

## Возможности openpyxl

Библиотека `openpyxl`, которая уже используется в проекте, поддерживает:

1. ✅ **Выпадающие списки (Data Validation)**
2. ✅ **Формулы в ячейках** (автоматический пересчет при открытии)
3. ✅ **Условное форматирование**
4. ✅ **Гиперссылки**
5. ✅ **Комментарии к ячейкам**

---

## 1. Выпадающие списки (Data Validation)

### Пример создания выпадающего списка:

```python
from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation

wb = Workbook()
ws = wb.active

# Создаем выпадающий список
dv = DataValidation(type="list", formula1='"Да,Нет,Не определено"', allow_blank=True)
dv.error = 'Неверное значение'
dv.errorTitle = 'Ошибка ввода'
dv.prompt = 'Выберите значение из списка'
dv.promptTitle = 'Выбор значения'

# Применяем к диапазону ячеек
ws.add_data_validation(dv)
dv.add("A1:A10")  # Применяем к ячейкам A1:A10

# Или можно использовать список из другой области листа
dv2 = DataValidation(type="list", formula1='$E$1:$E$5', allow_blank=False)
ws.add_data_validation(dv2)
dv2.add("B1:B10")  # Список берется из ячеек E1:E5
```

### Пример с динамическим списком из другого листа:

```python
from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation

wb = Workbook()
ws1 = wb.active
ws1.title = "Данные"

# Создаем второй лист со списком значений
ws2 = wb.create_sheet("Справочник")
ws2["A1"] = "Значение 1"
ws2["A2"] = "Значение 2"
ws2["A3"] = "Значение 3"

# Создаем выпадающий список, ссылающийся на другой лист
dv = DataValidation(
    type="list",
    formula1="Справочник!$A$1:$A$3",  # Ссылка на другой лист
    allow_blank=True
)
ws1.add_data_validation(dv)
dv.add("B1:B100")  # Применяем к столбцу B
```

---

## 2. Формулы в ячейках

### Пример создания формул:

```python
from openpyxl import Workbook

wb = Workbook()
ws = wb.active

# Простые формулы
ws["A1"] = 10
ws["B1"] = 20
ws["C1"] = "=A1+B1"  # Сумма
ws["D1"] = "=SUM(A1:B1)"  # Функция SUM
ws["E1"] = "=IF(A1>B1, \"Больше\", \"Меньше\")"  # Условная формула

# Формулы с ссылками на другие листы
ws["F1"] = "=SUM(Лист2!A1:A10)"  # Сумма из другого листа

# Формулы с именованными диапазонами
ws["G1"] = "=SUM(Данные)"  # Если есть именованный диапазон "Данные"

# Массивы формул (для Excel 365)
ws["H1"] = "=SUM(A1:A10*B1:B10)"  # Массивное умножение
```

### Пример сложных формул:

```python
# Формула с условием
ws["A2"] = "=IF(SUM(A1:A10)>100, \"Превышен лимит\", \"В норме\")"

# Формула с поиском (VLOOKUP)
ws["B2"] = "=VLOOKUP(A2, Справочник!A:B, 2, FALSE)"

# Формула с подсчетом
ws["C2"] = "=COUNTIF(A1:A10, \">50\")"

# Формула с текстом
ws["D2"] = "=CONCATENATE(A1, \" - \", B1)"

# Формула с датой
ws["E2"] = "=TODAY()"
ws["F2"] = "=NOW()"
```

---

## 3. Комбинированный пример

### Создание листа с выпадающими списками и формулами:

```python
from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.styles import Font, Alignment

wb = Workbook()
ws = wb.active
ws.title = "Расчеты"

# Заголовки
ws["A1"] = "Количество"
ws["B1"] = "Цена"
ws["C1"] = "Статус"
ws["D1"] = "Сумма"
ws["E1"] = "Скидка"
ws["F1"] = "Итого"

# Стили для заголовков
header_font = Font(bold=True)
for cell in ws[1]:
    cell.font = header_font
    cell.alignment = Alignment(horizontal="center")

# Данные
ws["A2"] = 10
ws["B2"] = 100
ws["A3"] = 5
ws["B3"] = 200

# Выпадающий список для статуса
status_dv = DataValidation(
    type="list",
    formula1='"Оплачено,В обработке,Отменено"',
    allow_blank=False
)
ws.add_data_validation(status_dv)
status_dv.add("C2:C100")  # Применяем к столбцу C

# Формулы
ws["D2"] = "=A2*B2"  # Сумма = Количество * Цена
ws["D3"] = "=A3*B3"

ws["E2"] = "=IF(D2>1000, D2*0.1, 0)"  # Скидка 10% если сумма > 1000
ws["E3"] = "=IF(D3>1000, D3*0.1, 0)"

ws["F2"] = "=D2-E2"  # Итого = Сумма - Скидка
ws["F3"] = "=D3-E3"

# Итоговая строка
ws["A4"] = "ИТОГО:"
ws["D4"] = "=SUM(D2:D3)"
ws["E4"] = "=SUM(E2:E3)"
ws["F4"] = "=SUM(F2:F3)"

# Сохраняем
wb.save("example_with_formulas.xlsx")
```

---

## 4. Интеграция в существующий код

### Модификация функции write_to_excel:

```python
from openpyxl.worksheet.datavalidation import DataValidation

def write_to_excel(sheets_data, output_path):
    """
    Записывает данные в Excel с поддержкой формул и выпадающих списков
    """
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, (df, conf) in sheets_data.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            
            # Получаем объект листа для дополнительных настроек
            ws = writer.sheets[sheet_name]
            
            # Добавляем выпадающий список (пример)
            if sheet_name == "SUMMARY":
                # Создаем выпадающий список для колонки "Статус"
                if "Статус" in df.columns:
                    status_col_idx = df.columns.get_loc("Статус") + 1  # +1 т.к. Excel считает с 1
                    status_col_letter = get_column_letter(status_col_idx)
                    
                    dv = DataValidation(
                        type="list",
                        formula1='"Активен,Неактивен,В архиве"',
                        allow_blank=True
                    )
                    ws.add_data_validation(dv)
                    # Применяем ко всем строкам данных (начиная со 2-й, т.к. 1-я - заголовок)
                    dv.add(f"{status_col_letter}2:{status_col_letter}{len(df)+1}")
            
            # Добавляем формулы (пример)
            if sheet_name == "SUMMARY" and "Количество" in df.columns and "Цена" in df.columns:
                qty_col = df.columns.get_loc("Количество") + 1
                price_col = df.columns.get_loc("Цена") + 1
                total_col = len(df.columns) + 1  # Новая колонка для итога
                
                # Добавляем заголовок
                ws.cell(row=1, column=total_col, value="Сумма")
                
                # Добавляем формулу в каждую строку
                for row_idx in range(2, len(df) + 2):
                    qty_letter = get_column_letter(qty_col)
                    price_letter = get_column_letter(price_col)
                    total_letter = get_column_letter(total_col)
                    ws[f"{total_letter}{row_idx}"] = f"={qty_letter}{row_idx}*{price_letter}{row_idx}"
```

---

## 5. Полезные функции для работы с формулами

### Создание именованных диапазонов:

```python
from openpyxl.workbook.defined_name import DefinedName

# Создаем именованный диапазон
wb = Workbook()
ws = wb.active

# Заполняем данные
for i in range(1, 11):
    ws[f"A{i}"] = i * 10

# Создаем именованный диапазон
wb.defined_names.add(DefinedName('Данные', attr_text='Лист1!$A$1:$A$10'))

# Теперь можно использовать в формулах
ws["B1"] = "=SUM(Данные)"
```

### Условное форматирование с формулами:

```python
from openpyxl.styles import PatternFill
from openpyxl.formatting.rule import FormulaRule

# Закрашиваем ячейки, где сумма > 1000
red_fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")

rule = FormulaRule(formula=['$D2>1000'], fill=red_fill)
ws.conditional_formatting.add("D2:D100", rule)
```

---

## 6. Ограничения и особенности

### Формулы:
- ✅ Формулы пересчитываются автоматически при открытии Excel
- ✅ Поддерживаются все стандартные функции Excel (SUM, IF, VLOOKUP и т.д.)
- ✅ Можно ссылаться на другие листы
- ⚠️ Массивные формулы (CSE) требуют Excel 365

### Выпадающие списки:
- ✅ Можно использовать статический список значений
- ✅ Можно ссылаться на диапазон ячеек в том же листе
- ✅ Можно ссылаться на диапазон в другом листе
- ⚠️ Ссылка на другой лист должна быть в формате "Лист!$A$1:$A$10"
- ⚠️ Максимальная длина списка значений в строке - около 255 символов

---

## 7. Рекомендации

1. **Для больших списков** лучше использовать ссылку на диапазон ячеек, а не строку
2. **Для формул** используйте абсолютные ссылки ($A$1) когда нужно, чтобы формула не менялась при копировании
3. **Тестируйте** формулы в Excel после создания файла
4. **Именованные диапазоны** упрощают работу с формулами и делают их более читаемыми

---

## Пример для вашего проекта

Если нужно добавить выпадающие списки или формулы в Summary лист, можно модифицировать функцию `_format_sheet` или `write_to_excel` в main.py.
