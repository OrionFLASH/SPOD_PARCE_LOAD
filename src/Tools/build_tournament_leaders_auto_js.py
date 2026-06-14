# -*- coding: utf-8 -*-
"""
CLI: сборка Tournament_LeadersForAdmin_AutoRun.js в OUT/YYYY/DD-MM (рядом с Excel).

Запуск из корня проекта:
  python src/Tools/build_tournament_leaders_auto_js.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.leaders_for_admin_auto_js import (  # noqa: E402
    get_run_output_dir,
    manager_stats_only_in_run_outputs,
    write_tournament_leaders_auto_js,
)

CONFIG_PATH = ROOT / "config.json"


def main() -> None:
    with CONFIG_PATH.open(encoding="utf-8") as fh:
        cfg = json.load(fh)
    if not manager_stats_only_in_run_outputs(cfg):
        raise SystemExit(
            "JS не создан: в config.json run_outputs должен содержать manager_stats_only."
        )
    base_out = str((cfg.get("paths") or {}).get("output") or "OUT")
    output_dir = get_run_output_dir(base_out)
    out_path = write_tournament_leaders_auto_js(
        output_dir,
        full_cfg=cfg,
    )
    if not out_path:
        raise SystemExit("Файл leadersForAdmin JS не создан (нет кодов или отключено в config).")
    print(f"Записано: {out_path}")


if __name__ == "__main__":
    main()
