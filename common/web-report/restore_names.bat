@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
echo Восстановление имён .script -^> .js / .mjs
if exist "common\web-report\report_app.script" ren "common\web-report\report_app.script" "report_app.js"
if exist "common\web-report\report_core.script" ren "common\web-report\report_core.script" "report_core.js"
if exist "common\web-report\report_io.script" ren "common\web-report\report_io.script" "report_io.js"
if exist "common\web-report\xlsx.full.min.script" ren "common\web-report\xlsx.full.min.script" "xlsx.full.min.js"
if exist "src\Tests\test_web_report_core.script" ren "src\Tests\test_web_report_core.script" "test_web_report_core.mjs"
echo Готово. См. ПРАВИЛЬНЫЕ_ИМЕНА.txt
pause
