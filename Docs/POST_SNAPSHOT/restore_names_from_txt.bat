@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

rem Каталог скрипта = каталог с файлами *.txt (распаковка POST). Права администратора не нужны.
cd /d "%~dp0"

echo Снятие суффикса .txt: main.py.txt -^> main.py, config.json.txt -^> config.json, src\*.py.txt -^> src\*.py
echo Пропуск: имена без точки перед последним .txt (справки вроде КУДА_ПОЛОЖИТЬ_ФАЙЛЫ.txt).
echo.

set /a COUNT=0
set /a SKIP=0
set /a ERR=0

for /r %%F in (*.txt) do (
  set "NM=%%~nF"
  set "NMNoDot=!NM:.=!"
  if "!NMNoDot!"=="!NM!" (
    set /a SKIP+=1
  ) else (
    if exist "%%~dpF!NM!" (
      set /a ERR+=1
      echo ОШИБКА: уже существует: "%%~dpF!NM!"
    ) else (
      ren "%%~fF" "!NM!" 2>nul
      set "RC=!errorlevel!"
      if !RC! equ 0 (
        set /a COUNT+=1
        echo OK: %%~nxF  -^>  !NM!
      ) else (
        set /a ERR+=1
        echo ОШИБКА: не удалось переименовать "%%~nxF" -^> "!NM!"
      )
    )
  )
)

echo.
echo Готово. Переименовано: !COUNT!  Пропущено: !SKIP!  Ошибок: !ERR!
pause
