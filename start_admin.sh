#!/bin/bash
cd "$(dirname "$0")"
cd admin_panel
python3 app.py > admin_panel.log 2>&1 &
echo $! > admin_panel.pid
sleep 3
if ps -p $(cat admin_panel.pid 2>/dev/null) > /dev/null 2>&1; then
    echo "✅ Сервер запущен (PID: $(cat admin_panel.pid))"
    curl -s http://localhost:5001/api/sessions | head -5
else
    echo "❌ Сервер не запустился"
    tail -20 admin_panel.log
fi
