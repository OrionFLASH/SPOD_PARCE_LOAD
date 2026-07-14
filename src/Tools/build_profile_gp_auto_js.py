# -*- coding: utf-8 -*-
"""
CLI: сборка Profile_GP_LOAD_AutoRun.js в OUT/YYYY/DD-MM (рядом с Excel).

Требует уже собранный MANAGER_STATS Excel в том же каталоге или прогон main с manager_stats_only.

Запуск из корня проекта (после прогона MANAGER_STATS):
  python src/Tools/build_profile_gp_auto_js.py

Либо с явным путём к Excel:
  python src/Tools/build_profile_gp_auto_js.py --excel "OUT/2026/12-06/MANAGER_STATS 20260612_120000.xlsx"
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.leaders_for_admin_auto_js import (  # noqa: E402
    get_run_output_dir,
    manager_stats_only_in_run_outputs,
)
from src.manager_stats import merge_manager_stats_config  # noqa: E402
from src.profile_gp_auto_js import write_profile_gp_auto_js  # noqa: E402

CONFIG_PATH = ROOT / "config" / "config.json"


def _find_latest_manager_stats_xlsx(output_dir: Path) -> Path | None:
    candidates = sorted(
        output_dir.glob("MANAGER_STATS *.xlsx"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _load_tabs_from_excel(excel_path: Path, tab_sheet: str) -> pd.DataFrame:
    return pd.read_excel(excel_path, sheet_name=tab_sheet, dtype=str).fillna("")


def main() -> None:
    parser = argparse.ArgumentParser(description="Сборка Profile_GP_LOAD_AutoRun.js")
    parser.add_argument(
        "--excel",
        help="Путь к MANAGER_STATS *.xlsx (иначе — последний в OUT/YYYY/DD-MM)",
    )
    parser.add_argument(
        "--output-dir",
        help="Каталог OUT/YYYY/DD-MM (иначе — текущий по дате)",
    )
    args = parser.parse_args()

    from src.config_loader import load_config_dict

    cfg = load_config_dict(str(CONFIG_PATH))

    if not manager_stats_only_in_run_outputs(cfg):
        raise SystemExit(
            "JS не создан: в config/ (run_outputs) должен быть токен manager_stats_only."
        )

    mcfg = merge_manager_stats_config(cfg.get("manager_stats"))
    tab_sheet = str(mcfg.get("output_sheet") or "TAB_NUMBERS")

    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        base_out = str((cfg.get("paths") or {}).get("output") or "OUT")
        output_dir = Path(get_run_output_dir(base_out))

    df_tabs: pd.DataFrame | None = None
    if args.excel:
        excel_path = Path(args.excel)
        if not excel_path.is_file():
            raise SystemExit(f"Excel не найден: {excel_path}")
        df_tabs = _load_tabs_from_excel(excel_path, tab_sheet)
    else:
        latest = _find_latest_manager_stats_xlsx(output_dir)
        if latest is not None:
            df_tabs = _load_tabs_from_excel(latest, tab_sheet)
            print(f"TAB_NUMBERS из: {latest}")

    mcfg_with_paths = {**mcfg, "_paths": {"input": str((cfg.get("paths") or {}).get("input") or "IN")}}
    out_path = write_profile_gp_auto_js(
        str(output_dir),
        df_tabs=df_tabs,
        manager_stats_cfg=mcfg_with_paths,
        full_cfg=cfg,
    )
    if not out_path:
        raise SystemExit(
            "Файл profile GP JS не создан (отключено в config, нет ТН или нет эталона IN/JS)."
        )
    print(f"Записано: {out_path}")


if __name__ == "__main__":
    main()
