@echo off
chcp 65001 >nul
setlocal

rem Запуск SPOD (main.py) на Windows из cmd или PowerShell: .\run_main.bat
rem Только Python/Anaconda 3.12 и его пакеты; ничего не устанавливает (pip install запрещён).
rem Интерпретатор: переменная SPOD_PYTHON (полный путь к python.exe Anaconda 3.12)
rem или python из PATH (Anaconda Prompt / активированное conda-окружение).
rem Аргументы передаются в main.py как есть. SPOD_NO_PAUSE=1 — не ждать нажатия клавиши.
rem Коды возврата: как у main.py; 3 — не найден подходящий Python или пакеты.

cd /d "%~dp0"

if defined SPOD_PYTHON (set "PY=%SPOD_PYTHON%") else (set "PY=python")

"%PY%" -c "import sys" >nul 2>&1
if errorlevel 1 (
    echo ОШИБКА: не удалось запустить Python: %PY%
    echo Запустите из Anaconda Prompt или укажите путь, например:
    echo   set SPOD_PYTHON=C:\ProgramData\anaconda3\python.exe
    set "RC=3"
    goto :done
)

"%PY%" -m src.runtime_env
if errorlevel 1 (
    set "RC=3"
    goto :done
)

rem Вывод в UTF-8 и при перенаправлении в файл (символы ✓ ⚠ █ в консольном выводе)
set "PYTHONIOENCODING=utf-8"
echo.
"%PY%" main.py %*
set "RC=%ERRORLEVEL%"

:done
echo.
echo Код завершения: %RC%
rem Пауза только при запуске двойным щелчком (окно иначе сразу закроется)
if not defined SPOD_NO_PAUSE (
    echo %cmdcmdline% | findstr /i /c:"%~nx0" >nul && pause
)
endlocal & exit /b %RC%
