#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
rename_one() {
  local src="$1" dst="$2"
  if [[ -f "$src" ]]; then
    mv -f "$src" "$dst"
    echo "OK: $src -> $dst"
  fi
}
rename_one "common/web-report/report_app.script" "common/web-report/report_app.js"
rename_one "common/web-report/report_core.script" "common/web-report/report_core.js"
rename_one "common/web-report/report_io.script" "common/web-report/report_io.js"
rename_one "common/web-report/xlsx.full.min.script" "common/web-report/xlsx.full.min.js"
rename_one "src/Tests/test_web_report_core.script" "src/Tests/test_web_report_core.mjs"
echo "Готово. См. ПРАВИЛЬНЫЕ_ИМЕНА.txt"
